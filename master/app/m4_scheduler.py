"""
master/app/m4_scheduler.py  —  M4 Task Scheduler
==================================================
Owner: Person A.

Worker-pull scheduling:
  assign_task()   — pick a compatible pending Task, create an Attempt with a
                    lease, all under ONE lock (this is what proves "no task was
                    handed to two workers at once")
  renew_lease()   — extend a running attempt
  commit_result() — accept or discard a result; discard is how a late result
                    from a node that already lost its lease is prevented from
                    overwriting a confirmed one
  expiry loop     — an attempt past its lease becomes EXPIRED and the task is
                    retried

Only *succeeded* attempts ever reach M6.
"""

from __future__ import annotations

import threading
import time
from dataclasses import asdict
from typing import Callable, Dict, List, Optional

from shared.contracts import (
    Attempt,
    AttemptStatus,
    LEASE_DURATION,
    TaskStatus,
)

# Hooks installed by the composition root to avoid import cycles.
_on_success: Optional[Callable[[str, str, str, int], None]] = None   # task, job, worker, units
_on_attempt_created: Optional[Callable[[str], None]] = None
_validate = None            # callable(output_ref, task_meta) -> units
_resolve_output = None      # callable(output_ref) -> local path or None


def bind_hooks(on_success=None, on_attempt_created=None, validate=None,
               resolve_output=None) -> None:
    global _on_success, _on_attempt_created, _validate, _resolve_output
    if on_success is not None:
        _on_success = on_success
    if on_attempt_created is not None:
        _on_attempt_created = on_attempt_created
    if validate is not None:
        _validate = validate
    if resolve_output is not None:
        _resolve_output = resolve_output


class TaskScheduler:
    def __init__(self, jobs, registry):
        self._jobs = jobs
        self._registry = registry
        # attempt_id -> {task_id, worker_id, status, lease_expires, started_at, ...}
        self._attempts: Dict[str, dict] = {}
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # ── lifecycle ────────────────────────────────────────────────────────────

    def start(self) -> None:
        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._lease_loop, name="m4-lease", daemon=True
            )
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    # ── assign ───────────────────────────────────────────────────────────────

    def assign_task(self, worker_id: str, free_slots: int) -> Optional[dict]:
        """Atomically pick, assign and lease one task for this worker."""
        if free_slots < 1:
            return None
        worker = self._registry.get_worker(worker_id)
        if worker is None:
            return None

        payload = None
        created_attempt_id = None

        with self._lock:
            # Re-check willingness inside the lock: the heartbeat could have
            # paused this node between the HTTP hop and here. Capacity comes from
            # the claim request itself, which is live — the cached free_slots is
            # only as fresh as the last heartbeat.
            if not self._registry.can_accept_work(worker_id):
                return None

            for task in self._jobs.pending_tasks():
                if not self._worker_matches(worker, task):
                    continue

                attempt = Attempt(
                    task_id=task.task_id,
                    worker_id=worker_id,
                    lease_expires=time.time() + LEASE_DURATION,
                )
                task.status = TaskStatus.ASSIGNED
                task.assigned_worker = worker_id

                record = asdict(attempt)
                record["status"] = attempt.status.value
                self._attempts[attempt.attempt_id] = record
                self._jobs.add_attempt(task.task_id, record)

                payload = {
                    "task": self._task_payload(task),
                    "attempt": dict(record),
                }
                created_attempt_id = attempt.attempt_id
                break                      # one task per claim request

        if payload is None:
            return None
        if _on_attempt_created and created_attempt_id:
            _safe(_on_attempt_created, created_attempt_id)
        return payload

    @staticmethod
    def _task_payload(task) -> dict:
        payload = asdict(task)
        payload["status"] = task.status.value
        return payload

    @staticmethod
    def _worker_matches(worker, task) -> bool:
        supported = worker.capability.supported_task_types
        if task.task_type not in (supported if supported is not None else ["image_thumb"]):
            return False
        req = task.resource_req or {}
        if req.get("gpu") and not worker.capability.gpu_name:
            return False
        return True

    # ── lease ────────────────────────────────────────────────────────────────

    def set_job_paused(self, job_id: str, paused: bool) -> dict:
        with self._lock:
            return self._jobs.set_paused(job_id, paused)

    def delete_job(self, job_id: str) -> None:
        with self._lock:
            if self._jobs.get_job(job_id) is None:
                raise KeyError(job_id)
            task_ids = {task.task_id for task in self._jobs.tasks_of(job_id)}
            for record in self._attempts.values():
                if record["task_id"] in task_ids and record["status"] == AttemptStatus.RUNNING.value:
                    record.update(status=AttemptStatus.EXPIRED.value,
                                  finished_at=time.time(), units_done=0,
                                  error_msg="job deleted")
            self._jobs.delete_job(job_id)

    def renew_lease(self, attempt_id: str) -> dict:
        with self._lock:
            record = self._attempts.get(attempt_id)
            if record is None or record["status"] != AttemptStatus.RUNNING.value:
                return {"error": "attempt not found or not running"}
            record["lease_expires"] = time.time() + LEASE_DURATION
            self._jobs.update_attempt(
                record["task_id"], attempt_id,
                lease_expires=record["lease_expires"],
            )
            return {"lease_expires": record["lease_expires"]}

    def mark_running(self, attempt_id: str) -> None:
        """Optional explicit RUNNING signal; assignment already implies it."""
        with self._lock:
            record = self._attempts.get(attempt_id)
            if record and record["status"] == AttemptStatus.RUNNING.value:
                self._jobs.mark_task_running(record["task_id"], record["worker_id"])

    # ── commit ───────────────────────────────────────────────────────────────

    def commit_result(self, attempt_id: str, output_ref: str,
                      units_done: int, error_msg: Optional[str]) -> dict:
        """Accept or discard a result. Never overwrites a confirmed outcome."""
        accepted_units = 0

        with self._lock:
            record = self._attempts.get(attempt_id)
            if record is None:
                return {"error": "unknown attempt"}

            if record["status"] != AttemptStatus.RUNNING.value:
                # Late arrival after expiry / after a sibling already won.
                return {"discarded": True, "reason": "attempt already closed"}

            task_id = record["task_id"]
            worker_id = record["worker_id"]
            task = self._jobs.get_task(task_id)
            if task is None:
                return {"error": "unknown task"}

            if task.status == TaskStatus.SUCCEEDED:
                # Another attempt already produced the confirmed result.
                record["status"] = AttemptStatus.EXPIRED.value
                self._jobs.update_attempt(task_id, attempt_id,
                                          status=AttemptStatus.EXPIRED.value)
                return {"discarded": True, "reason": "task already confirmed"}

            now = time.time()

            if error_msg:
                record["status"] = AttemptStatus.FAILED.value
                record["finished_at"] = now
                record["error_msg"] = str(error_msg)[:1000]
                record["units_done"] = 0
                self._jobs.update_attempt(
                    task_id, attempt_id,
                    status=AttemptStatus.FAILED.value,
                    finished_at=now,
                    error_msg=record["error_msg"],
                    units_done=0,
                )
                job_id = record.get("job_id")
                self._jobs.mark_task_failed(task_id, record["error_msg"])
                return {"committed": False, "failed": True, "task_id": task_id}

            # ── verify before trusting the number the worker reported ────────
            verified_units = int(units_done)
            if _validate is not None:
                meta = {
                    "task_id": task_id,
                    "task_type": task.task_type,
                    "input_ref": task.input_ref,
                }
                path = None
                if _resolve_output is not None and output_ref:
                    try:
                        path = _resolve_output(output_ref)
                    except Exception:
                        path = None
                if path:
                    meta["input_abs_path"] = self._input_abs_path(task.input_ref)
                    try:
                        verified_units = int(_validate(path, meta))
                    except Exception as exc:
                        record["status"] = AttemptStatus.FAILED.value
                        record["finished_at"] = now
                        record["error_msg"] = f"validation failed: {exc}"
                        record["units_done"] = 0
                        self._jobs.update_attempt(
                            task_id, attempt_id,
                            status=AttemptStatus.FAILED.value,
                            finished_at=now,
                            error_msg=record["error_msg"],
                            units_done=0,
                        )
                        self._jobs.mark_task_failed(task_id, record["error_msg"])
                        return {
                            "committed": False, "failed": True,
                            "validation_error": str(exc)[:400],
                        }

            record["status"] = AttemptStatus.SUCCEEDED.value
            record["finished_at"] = now
            record["output_ref"] = output_ref
            record["units_done"] = verified_units
            accepted_units = verified_units

            self._jobs.update_attempt(
                task_id, attempt_id,
                status=AttemptStatus.SUCCEEDED.value,
                finished_at=now,
                output_ref=output_ref,
                units_done=verified_units,
            )
            self._jobs.mark_task_done(task_id, output_ref)

        # Record only on the success path, once per attempt (technical point J1).
        if _on_success and accepted_units >= 0:
            _safe(_on_success, task_id, task.job_id, worker_id, accepted_units)

        return {"committed": True, "units_done": accepted_units, "task_id": task_id}

    def _input_abs_path(self, input_ref: str) -> Optional[str]:
        resolver = _resolve_output
        if resolver is None or not input_ref:
            return None
        try:
            return resolver(input_ref)
        except Exception:
            return None

    # ── reclaim ──────────────────────────────────────────────────────────────

    def reclaim_worker(self, worker_id: str) -> List[str]:
        """A node went OFFLINE: close its open attempts and requeue their tasks."""
        reclaimed: List[str] = []
        with self._lock:
            for attempt_id, record in self._attempts.items():
                if record["worker_id"] != worker_id:
                    continue
                if record["status"] != AttemptStatus.RUNNING.value:
                    continue
                now = time.time()
                record["status"] = AttemptStatus.EXPIRED.value
                record["finished_at"] = now
                record["units_done"] = 0
                self._jobs.update_attempt(
                    record["task_id"], attempt_id,
                    status=AttemptStatus.EXPIRED.value,
                    finished_at=now,
                    units_done=0,
                )
                self._jobs.requeue_task(record["task_id"], "worker offline")
                reclaimed.append(record["task_id"])
        return reclaimed

    # ── lease expiry loop ────────────────────────────────────────────────────

    def _lease_loop(self) -> None:
        while not self._stop.wait(10):
            self.scan_expired_leases()

    def scan_expired_leases(self) -> List[str]:
        now = time.time()
        expired: List[str] = []
        with self._lock:
            for attempt_id, record in self._attempts.items():
                if record["status"] != AttemptStatus.RUNNING.value:
                    continue
                if float(record.get("lease_expires") or 0) >= now:
                    continue
                record["status"] = AttemptStatus.EXPIRED.value
                record["finished_at"] = now
                record["units_done"] = 0
                self._jobs.update_attempt(
                    record["task_id"], attempt_id,
                    status=AttemptStatus.EXPIRED.value,
                    finished_at=now,
                    units_done=0,
                )
                self._jobs.mark_task_failed(record["task_id"], "lease expired")
                expired.append(attempt_id)
        return expired

    # ── introspection (used by tests) ────────────────────────────────────────

    def attempt(self, attempt_id: str) -> Optional[dict]:
        with self._lock:
            record = self._attempts.get(attempt_id)
            return dict(record) if record else None

    def open_attempts(self) -> List[dict]:
        with self._lock:
            return [
                dict(r) for r in self._attempts.values()
                if r["status"] == AttemptStatus.RUNNING.value
            ]


def _safe(fn, *args) -> None:
    try:
        fn(*args)
    except Exception:  # pragma: no cover
        pass
