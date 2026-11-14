"""W5: one resident Executor, attempt leases, uploads and bounded cleanup."""
from __future__ import annotations

import logging
import os
import threading
import time

from shared.contracts import LEASE_DURATION, LEASE_RENEW_BEFORE
from worker.agent.executor_client import ResidentExecutor, ExecutorFailure

log = logging.getLogger("w5")


class ExecutionManager:
    def __init__(self, config):
        self.cfg = config
        self.client = ResidentExecutor(startup_timeout=float(os.environ.get("DORMPOOL_EXECUTOR_STARTUP_TIMEOUT", "180")))
        self.task_timeout = float(os.environ.get("DORMPOOL_TASK_TIMEOUT", "90"))
        self._running = {}       # attempt_id -> task_id (includes download/upload)
        self._threads = set()
        self._lock = threading.RLock()
        self._closing = threading.Event()

    def prepare(self):
        return self.client.ensure_ready()

    def status(self):
        with self._lock:
            active = list(self._running)
        return {**self.client.status(), "active_attempts": active}

    def run_task(self, task: dict, attempt: dict, agent=None):
        with self._lock:
            if self._closing.is_set():
                raise ExecutorFailure("execution manager is closing")
            thread = threading.Thread(target=self._execute, args=(task, attempt, agent),
                                      daemon=True, name=f"task-{task.get('task_id', 'unknown')}")
            if agent is not None:
                agent.monitor.slot_acquire()
            self._running[attempt["attempt_id"]] = task["task_id"]
            self._threads.add(thread)
            try:
                thread.start()
            except Exception:
                self._running.pop(attempt["attempt_id"], None)
                self._threads.discard(thread)
                if agent is not None:
                    agent.monitor.slot_release()
                    agent._work_event.set()
                raise

    def running_task_ids(self):
        with self._lock:
            return list(self._running.values())

    def active_count(self):
        with self._lock:
            return len(self._running)

    def _execute(self, task, attempt, agent):
        stop_renew = threading.Event()
        renew_thread = None
        try:
            if agent is not None:
                renew_thread = threading.Thread(target=self._renew_loop,
                    args=(agent, attempt["attempt_id"],
                          float(attempt.get("lease_expires") or time.time() + LEASE_DURATION), stop_renew),
                    daemon=True, name=f"renew-{attempt['attempt_id']}")
                renew_thread.start()
            work_dir = self.cfg.data_dir / task["task_id"] / attempt["attempt_id"]
            work_dir.mkdir(parents=True, exist_ok=True)
            self._run_guarded(task, attempt, agent, work_dir / "input.zip", work_dir / "output.zip")
        except Exception as exc:
            log.warning("task %s failed: %s", task["task_id"], exc)
            if agent is not None:
                agent.connector.commit_result(attempt["attempt_id"], "", 0, str(exc)[:1500])
        finally:
            stop_renew.set()
            if renew_thread:
                renew_thread.join(timeout=1)
            with self._lock:
                self._running.pop(attempt["attempt_id"], None)
                self._threads.discard(threading.current_thread())
            if agent is not None:
                agent.monitor.slot_release()
                agent._work_event.set()

    def _run_guarded(self, task, attempt, agent, input_path, output_path):
        if agent is None:
            raise ExecutorFailure("no agent bound")
        started = time.perf_counter()
        if not agent.connector.fetch_input(task.get("input_ref") or "", str(input_path)):
            raise ExecutorFailure("input fetch failed")
        downloaded = time.perf_counter()
        result = self.client.execute(task, attempt, input_path, output_path, self.task_timeout)
        if not result["ok"]:
            raise ExecutorFailure(result.get("detail") or "executor task failed")
        units = int(result.get("units_done") or 0)
        if units <= 0 or not output_path.is_file():
            self.client.abort()
            raise ExecutorFailure("executor reported success without valid output/work units")
        output_ref = agent.connector.upload_output(task.get("job_id") or "", task["task_id"], str(output_path))
        if not output_ref:
            raise ExecutorFailure("result upload failed")
        reply = agent.connector.commit_result(attempt["attempt_id"], output_ref, units, None)
        if not reply or not reply.get("committed"):
            log.warning("task %s result not confirmed by Master: %s", task["task_id"], reply)
        log.info("task %s finished: units=%s executor_pid=%s download_s=%.4f compute_s=%s total_s=%.4f",
                 task["task_id"], units, result["pid"], downloaded-started,
                 result.get("elapsed_s"), time.perf_counter()-started)

    def _renew_loop(self, agent, attempt_id, lease_expires, stop_event):
        while not stop_event.is_set():
            if lease_expires - time.time() < LEASE_RENEW_BEFORE:
                if agent.connector.renew_lease(attempt_id):
                    lease_expires = time.time() + LEASE_DURATION
            stop_event.wait(5)

    def close(self, timeout=None):
        with self._lock:
            self._closing.set()
        timeout = float(os.environ.get("DORMPOOL_SHUTDOWN_TIMEOUT", "30")) if timeout is None else timeout
        deadline = time.monotonic() + max(0, timeout)
        while True:
            with self._lock:
                threads = list(self._threads)
            if not threads or time.monotonic() >= deadline:
                break
            threads[0].join(timeout=min(0.1, max(0, deadline-time.monotonic())))
        if threads:
            log.warning("shutdown deadline reached; terminating executor with %s attempts pending", len(threads))
        self.client.close()
        for thread in threads:
            thread.join(timeout=1)

    def stop_claims(self):
        """Cancel idle/startup work immediately; accepted attempts may drain."""
        with self._lock:
            self._closing.set()
            idle = not self._running
        if idle:
            self.client.close()
