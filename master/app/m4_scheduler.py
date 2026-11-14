"""
master/app/m4_scheduler.py  —  M4 Task Scheduler
==================================================
Person A owns this file.

Responsibility:
  - Match pending Tasks to available Workers (capability check)
  - Create Attempt records with execution leases
  - Expire overdue leases and re-queue their Tasks
  - Commit results and delegate to M3/M5
"""

import threading, time
from dataclasses import asdict
from typing import Dict, Optional
from shared.contracts import (
    Task, Attempt, TaskStatus, AttemptStatus,
    LEASE_DURATION, WORKER_TIMEOUT,
)
from master.app.m2_registry import registry
from master.app.m3_job import job_manager


class TaskScheduler:
    def __init__(self):
        self._attempts: Dict[str, Attempt] = {}
        self._lock = threading.Lock()
        t = threading.Thread(target=self._lease_expiry_loop, daemon=True)
        t.start()

    # ── Worker pulls a task ───────────────────────────────────────────────────

    def assign_task(self, worker_id: str, free_slots: int) -> Optional[dict]:
        """
        Find a pending Task compatible with this Worker and assign it.
        Returns the Task dict + Attempt dict, or None if nothing is available.
        """
        worker = registry.get_worker(worker_id)
        if not worker or free_slots < 1:
            return None

        with self._lock:
            for task in job_manager.pending_tasks():
                if self._worker_matches(worker, task):
                    attempt = Attempt(
                        task_id=task.task_id,
                        worker_id=worker_id,
                        lease_expires=time.time() + LEASE_DURATION,
                    )
                    task.status          = TaskStatus.ASSIGNED
                    task.assigned_worker = worker_id
                    self._attempts[attempt.attempt_id] = attempt
                    return {"task": asdict(task), "attempt": asdict(attempt)}
        return None

    # ── Lease management ──────────────────────────────────────────────────────

    def renew_lease(self, attempt_id: str) -> dict:
        with self._lock:
            att = self._attempts.get(attempt_id)
            if not att or att.status != AttemptStatus.RUNNING:
                return {"error": "attempt not found or not running"}
            att.lease_expires = time.time() + LEASE_DURATION
        return {"lease_expires": att.lease_expires}

    # ── Result commit ─────────────────────────────────────────────────────────

    def commit_result(self, attempt_id: str, output_ref: str,
                      units_done: int, error_msg: Optional[str]) -> dict:
        with self._lock:
            att = self._attempts.get(attempt_id)
            if not att:
                return {"error": "unknown attempt"}
            if att.status != AttemptStatus.RUNNING:
                # Late arrival after expiry — discard; do not overwrite confirmed result
                return {"error": "attempt already closed", "discarded": True}
            att.finished_at = time.time()
            if error_msg:
                att.status    = AttemptStatus.FAILED
                att.error_msg = error_msg
                job_manager.mark_task_failed(att.task_id, error_msg)
            else:
                att.status     = AttemptStatus.SUCCEEDED
                att.output_ref = output_ref
                att.units_done = units_done
                job_manager.mark_task_done(att.task_id, output_ref)
                # TODO (Person A): trigger M6 metric recording here
        return {"committed": True}

    # ── Lease expiry background loop ──────────────────────────────────────────

    def _lease_expiry_loop(self):
        while True:
            time.sleep(10)
            now = time.time()
            with self._lock:
                for att in list(self._attempts.values()):
                    if att.status == AttemptStatus.RUNNING and att.lease_expires < now:
                        att.status = AttemptStatus.EXPIRED
                        job_manager.mark_task_failed(att.task_id, "lease expired")

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _worker_matches(worker, task: Task) -> bool:
        req = task.resource_req
        if req.get("gpu") and not worker.capability.gpu_name:
            return False
        return True


scheduler = TaskScheduler()
