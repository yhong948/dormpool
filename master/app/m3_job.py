"""
master/app/m3_job.py  —  M3 Job Manager
========================================
Person A owns this file.

Responsibility:
  - Create a Job from a user submission
  - Split Job into Tasks (sharding logic lives here)
  - Track Job progress as Tasks complete
  - Determine Job terminal state (COMPLETED / FAILED)
"""

import threading
from dataclasses import asdict
from typing import Dict, List, Optional
from shared.contracts import Job, Task, JobStatus, TaskStatus


class JobManager:
    def __init__(self):
        self._jobs:  Dict[str, Job]        = {}
        self._tasks: Dict[str, List[Task]] = {}  # job_id → tasks
        self._lock   = threading.Lock()

    # ── Job creation ──────────────────────────────────────────────────────────

    def create_job(self, task_type: str, input_ref: str, num_shards: int = 1) -> Job:
        """
        Create a Job and split it into `num_shards` Tasks.
        Each Task gets an input_ref of the form "<input_ref>/<shard_index>".
        Person A: replace shard naming once M5 defines actual file layout.
        """
        job = Job(task_type=task_type, input_ref=input_ref, total_tasks=num_shards)
        tasks = [
            Task(
                job_id=job.job_id,
                task_type=task_type,
                input_ref=f"{input_ref}/shard_{i}",
            )
            for i in range(num_shards)
        ]
        with self._lock:
            self._jobs[job.job_id]  = job
            self._tasks[job.job_id] = tasks
        return job

    # ── Queries ───────────────────────────────────────────────────────────────

    def get_job(self, job_id: str) -> Optional[Job]:
        return self._jobs.get(job_id)

    def get_tasks(self, job_id: str) -> List[Task]:
        return self._tasks.get(job_id, [])

    def list_jobs(self) -> List[dict]:
        with self._lock:
            return [asdict(j) for j in self._jobs.values()]

    def pending_tasks(self) -> List[Task]:
        """Return all tasks that are ready to be scheduled (PENDING or RETRY_PENDING)."""
        result = []
        with self._lock:
            for tasks in self._tasks.values():
                for t in tasks:
                    if t.status in (TaskStatus.PENDING, TaskStatus.RETRY_PENDING):
                        result.append(t)
        return result

    # ── Progress updates (called by M4 Scheduler) ─────────────────────────────

    def mark_task_done(self, task_id: str, output_ref: str) -> None:
        with self._lock:
            task = self._find_task(task_id)
            if not task:
                return
            task.status     = TaskStatus.SUCCEEDED
            task.output_ref = output_ref
            job = self._jobs[task.job_id]
            job.done_tasks += 1
            job.status = JobStatus.RUNNING
            # check completion
            all_tasks = self._tasks[task.job_id]
            if all(t.status == TaskStatus.SUCCEEDED for t in all_tasks):
                import time
                job.status      = JobStatus.COMPLETED
                job.finished_at = time.time()

    def mark_task_failed(self, task_id: str, error: str) -> None:
        with self._lock:
            task = self._find_task(task_id)
            if not task:
                return
            if task.retry_count < task.max_retries:
                task.retry_count += 1
                task.status = TaskStatus.RETRY_PENDING
                task.assigned_worker = None
            else:
                task.status = TaskStatus.FAILED
                # propagate to Job if any task permanently fails
                job = self._jobs.get(task.job_id)
                if job:
                    job.status = JobStatus.FAILED

    def _find_task(self, task_id: str) -> Optional[Task]:
        for tasks in self._tasks.values():
            for t in tasks:
                if t.task_id == task_id:
                    return t
        return None


job_manager = JobManager()
