"""
master/app/m3_job.py  —  M3 Job Manager
========================================
Owner: Person A.

Owns the Job/Task lifecycle:
  * create_job()   — build a Job and split it into shards-sized Tasks
  * pending_tasks()— what M4 may hand out (PENDING + RETRY_PENDING)
  * mark_task_done / mark_task_failed — with the retry policy
  * Job终态: every task succeeded -> COMPLETED, any task permanently failed -> FAILED

Sharding is driven by SHARD_SIZE (env, default 3). Each Task carries
input_ref = "inputs/<job_id>/shard_<i>.zip" which M5 produced.

The response view (docs/验收门禁.md §0.6 item 3) nests attempts inside each task,
so G3 can prove "no task was handed to two workers at once".
"""

from __future__ import annotations

import os
import threading
import time
import uuid
from dataclasses import asdict
from typing import Callable, Dict, List, Optional

from shared.contracts import Job, JobStatus, Task, TaskStatus, AttemptStatus

DEFAULT_SHARD_SIZE = 3


def shard_size() -> int:
    """SHARD_SIZE from the environment, clamped to a sane range."""
    raw = os.environ.get("SHARD_SIZE", str(DEFAULT_SHARD_SIZE))
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return DEFAULT_SHARD_SIZE
    return max(1, value)


# Hook installed by the composition root: called once when a Job completes,
# so M5 can aggregate and M6 can freeze the measurement window.
_on_job_completed: Optional[Callable[[str], None]] = None


def bind_hooks(on_job_completed=None) -> None:
    global _on_job_completed
    if on_job_completed is not None:
        _on_job_completed = on_job_completed


class JobManager:
    def __init__(self):
        self._jobs: Dict[str, Job] = {}
        self._tasks: Dict[str, List[Task]] = {}          # job_id -> tasks in shard order
        self._task_index: Dict[str, Task] = {}           # task_id -> Task
        self._attempts: Dict[str, List[dict]] = {}       # task_id -> attempt dicts
        self._lock = threading.RLock()

    # ── creation ─────────────────────────────────────────────────────────────

    def create_job(self, task_type: str, input_refs: List[str],
                   input_zip_ref: str = "", job_id: Optional[str] = None,
                   measurement_mode: str = "pool", target_worker_id: Optional[str] = None,
                   measurement_profile: Optional[dict] = None) -> Job:
        """Create a Job whose Tasks are one per shard reference.

        input_refs is produced by M5 when it splits the uploaded archive.
        `job_id` lets the caller pre-allocate the id so M5 can write the upload
        under a folder that already carries it.
        """
        total = len(input_refs)
        job = Job(
            job_id=job_id or ("job-" + uuid.uuid4().hex[:8]),
            task_type=task_type,
            input_ref=input_zip_ref or (input_refs[0] if input_refs else ""),
            status=JobStatus.QUEUED,
            total_tasks=total,
            measurement_mode=measurement_mode,
            target_worker_id=target_worker_id,
            measurement_profile=measurement_profile or {},
        )
        tasks = [
            Task(
                job_id=job.job_id,
                task_type=task_type,
                input_ref=ref,
                resource_req={"gpu": task_type in ("image_thumb_gpu", "convnext_infer")},
            )
            for ref in input_refs
        ]
        with self._lock:
            self._jobs[job.job_id] = job
            self._tasks[job.job_id] = tasks
            for task in tasks:
                self._task_index[task.task_id] = task
                self._attempts[task.task_id] = []
        return job

    def record_measurement_assignment(self, job_id: str, workers: dict) -> None:
        with self._lock:
            job = self._jobs[job_id]
            if job.started_at is None:
                job.started_at = time.time()
            for worker_id, config in workers.items():
                old = job.measurement_workers.get(worker_id)
                if old is not None and old != config and "configuration_changed" not in job.measurement_issues:
                    job.measurement_issues.append("configuration_changed")
                job.measurement_workers[worker_id] = config

    # ── queries ──────────────────────────────────────────────────────────────

    def get_job(self, job_id: str) -> Optional[Job]:
        with self._lock:
            return self._jobs.get(job_id)

    def get_task(self, task_id: str) -> Optional[Task]:
        with self._lock:
            return self._task_index.get(task_id)

    def tasks_of(self, job_id: str) -> List[Task]:
        with self._lock:
            return list(self._tasks.get(job_id, []))

    def pending_tasks(self) -> List[Task]:
        """Tasks M4 may assign, oldest first."""
        with self._lock:
            result = [
                t for t in self._task_index.values()
                if t.status in (TaskStatus.PENDING, TaskStatus.RETRY_PENDING)
                and self._jobs[t.job_id].status in (JobStatus.QUEUED, JobStatus.RUNNING)
            ]
        result.sort(key=lambda t: t.input_ref)
        return result

    def job_view(self, job_id: str) -> Optional[dict]:
        """Job payload with nested tasks[] and attempts[] (gate response shape)."""
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return None
            payload = asdict(job)
            payload["status"] = job.status.value
            payload["tasks"] = [
                self._task_view(t) for t in self._tasks.get(job_id, [])
            ]
            return payload

    def _task_view(self, task: Task) -> dict:
        view = asdict(task)
        view["status"] = task.status.value
        view["attempts"] = [
            dict(a) for a in self._attempts.get(task.task_id, [])
        ]
        return view

    def list_jobs(self) -> List[dict]:
        with self._lock:
            out = []
            for job in self._jobs.values():
                item = asdict(job)
                item["status"] = job.status.value
                out.append(item)
            return out

    def ledger(self, job_id: str) -> dict:
        """Measurement inputs M6 needs for a finished job."""
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return {}
            units_total = 0
            per_worker_units: Dict[str, float] = {}
            per_worker_busy: Dict[str, float] = {}
            succeeded = 0
            for task in self._tasks.get(job_id, []):
                for attempt in self._attempts.get(task.task_id, []):
                    if attempt.get("status") != AttemptStatus.SUCCEEDED.value:
                        continue
                    units = int(attempt.get("units_done") or 0)
                    worker = attempt.get("worker_id") or "unknown"
                    units_total += units
                    per_worker_units[worker] = per_worker_units.get(worker, 0.0) + units
                    started = attempt.get("started_at") or 0.0
                    finished = attempt.get("finished_at") or started
                    busy = max(0.0, float(finished) - float(started))
                    per_worker_busy[worker] = per_worker_busy.get(worker, 0.0) + busy
                    succeeded += 1
            return {
                "job_id": job_id,
                "status": job.status.value,
                "created_at": job.created_at,
                "finished_at": job.finished_at,
                "units_total": units_total,
                "succeeded_attempts": succeeded,
                "per_worker_units": per_worker_units,
                "per_worker_busy": per_worker_busy,
            }

    def succeeded_task_ids(self, job_id: str) -> List[str]:
        with self._lock:
            return [
                t.task_id for t in self._tasks.get(job_id, [])
                if t.status == TaskStatus.SUCCEEDED
            ]

    # ── attempt bookkeeping (called by M4) ───────────────────────────────────

    def set_paused(self, job_id: str, paused: bool) -> dict:
        """Called under the scheduler lock so pause is atomic with assignment."""
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise KeyError(job_id)
            if job.status in (JobStatus.COMPLETED, JobStatus.FAILED):
                raise ValueError("completed or failed jobs cannot be paused or resumed")
            if paused:
                job.status = JobStatus.PAUSED
                if "paused" not in job.measurement_issues:
                    job.measurement_issues.append("paused")
            elif job.status == JobStatus.PAUSED:
                started = any(self._attempts.get(t.task_id) for t in self._tasks[job_id])
                job.status = JobStatus.RUNNING if started else JobStatus.QUEUED
            return self.job_view(job_id)

    def delete_job(self, job_id: str) -> None:
        """Remove a job and its task indexes after M4 closes its attempts."""
        with self._lock:
            if job_id not in self._jobs:
                raise KeyError(job_id)
            for task in self._tasks.pop(job_id, []):
                self._task_index.pop(task.task_id, None)
                self._attempts.pop(task.task_id, None)
            del self._jobs[job_id]

    def add_attempt(self, task_id: str, attempt: dict) -> None:
        with self._lock:
            self._attempts.setdefault(task_id, []).append(attempt)
            job = self._jobs[self._task_index[task_id].job_id]
            if job.status == JobStatus.QUEUED:
                job.status = JobStatus.RUNNING

    def update_attempt(self, task_id: str, attempt_id: str, **fields) -> Optional[dict]:
        with self._lock:
            for attempt in self._attempts.get(task_id, []):
                if attempt.get("attempt_id") == attempt_id:
                    attempt.update(fields)
                    return attempt
        return None

    def attempts_of(self, task_id: str) -> List[dict]:
        with self._lock:
            return [dict(a) for a in self._attempts.get(task_id, [])]

    # ── transitions ──────────────────────────────────────────────────────────

    def mark_task_running(self, task_id: str, worker_id: str) -> None:
        with self._lock:
            task = self._task_index.get(task_id)
            if task is None:
                return
            if task.status == TaskStatus.ASSIGNED:
                task.status = TaskStatus.RUNNING
            task.assigned_worker = worker_id

    def mark_task_done(self, task_id: str, output_ref: str) -> Optional[str]:
        """Mark a task SUCCEEDED. Returns the job_id when the job just completed."""
        with self._lock:
            task = self._task_index.get(task_id)
            if task is None:
                return None
            task.status = TaskStatus.SUCCEEDED
            task.output_ref = output_ref

            job = self._jobs.get(task.job_id)
            if job is None:
                return None
            job.done_tasks = sum(
                1 for t in self._tasks.get(job.job_id, [])
                if t.status == TaskStatus.SUCCEEDED
            )
            if job.status == JobStatus.QUEUED:
                job.status = JobStatus.RUNNING

            all_done = all(
                t.status == TaskStatus.SUCCEEDED
                for t in self._tasks.get(job.job_id, [])
            )
            just_completed = False
            if all_done and job.status != JobStatus.COMPLETED:
                job.status = JobStatus.COMPLETED
                job.finished_at = time.time()
                just_completed = True

        if just_completed and _on_job_completed:
            try:
                _on_job_completed(task.job_id)
            except Exception:  # pragma: no cover - aggregation is best-effort here
                pass
        return task.job_id if just_completed else None

    def mark_task_failed(self, task_id: str, error: str) -> Optional[str]:
        """Apply the retry policy. Returns the job_id if the job just failed."""
        with self._lock:
            task = self._task_index.get(task_id)
            if task is None:
                return None
            if task.status == TaskStatus.SUCCEEDED:
                return None                    # late failure must never undo success

            job = self._jobs.get(task.job_id)
            if task.retry_count < task.max_retries:
                task.retry_count += 1
                task.status = TaskStatus.RETRY_PENDING
                task.assigned_worker = None
                return None

            task.status = TaskStatus.FAILED
            if job is not None and job.status != JobStatus.COMPLETED:
                job.status = JobStatus.FAILED
                if job.finished_at is None:
                    job.finished_at = time.time()
                return job.job_id
            return None

    def requeue_task(self, task_id: str, reason: str) -> None:
        """Put a task back without consuming a retry (used when a node vanishes).

        The worker never got to report, so the attempt was never confirmed either
        way; the task must become available again promptly rather than wait for
        the full lease to lapse.
        """
        with self._lock:
            task = self._task_index.get(task_id)
            if task is None or task.status in (TaskStatus.SUCCEEDED, TaskStatus.FAILED):
                return
            task.status = TaskStatus.RETRY_PENDING
            task.assigned_worker = None


job_manager = JobManager()
