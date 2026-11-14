"""
worker/agent/w5_exec.py  —  W5 Execution Manager
==================================================
Person B owns this file.

Responsibility:
  - Receive a Task + Attempt dict from W1
  - Fetch input from Master via W2
  - Spawn executor subprocess (contracts.py CLI shape)
  - Stream lease renewals while executor runs
  - Collect exit code + JSON stdout, upload output, commit result
  - Isolate task-level failures from Agent lifecycle
"""

import json, logging, os, subprocess, threading, time
from pathlib import Path
from typing import Optional

log = logging.getLogger("w5")


class ExecutionManager:
    def __init__(self, config):
        self.cfg      = config
        self._running: dict[str, subprocess.Popen] = {}  # attempt_id → process
        self._lock    = threading.Lock()

    # ── Public ────────────────────────────────────────────────────────────────

    def run_task(self, task: dict, attempt: dict):
        """Launch a task in a background thread; returns immediately."""
        t = threading.Thread(
            target=self._execute,
            args=(task, attempt),
            daemon=True,
            name=f"task-{task['task_id']}",
        )
        t.start()

    def running_task_ids(self) -> list[str]:
        with self._lock:
            return list(self._running.keys())

    # ── Internal ──────────────────────────────────────────────────────────────

    def _execute(self, task: dict, attempt: dict):
        from worker.agent.w1_runtime import AgentRuntime   # imported lazily to avoid circular
        # Acquire resource slot
        agent = _agent_singleton()
        agent.monitor.slot_acquire()
        attempt_id = attempt["attempt_id"]
        task_id    = task["task_id"]
        try:
            self._do_execute(task, attempt, agent)
        except Exception as exc:
            log.exception("Unexpected error in task %s", task_id)
            agent.connector.commit_result(attempt_id, "", 0, str(exc))
        finally:
            agent.monitor.slot_release()
            with self._lock:
                self._running.pop(attempt_id, None)

    def _do_execute(self, task: dict, attempt: dict, agent):
        from shared.contracts import EXECUTOR_SUCCESS, LEASE_DURATION, LEASE_RENEW_BEFORE

        attempt_id = attempt["attempt_id"]
        task_id    = task["task_id"]
        task_type  = task["task_type"]
        input_ref  = task["input_ref"]

        # 1. Fetch input
        work_dir   = self.cfg.data_dir / task_id
        work_dir.mkdir(parents=True, exist_ok=True)
        input_path  = work_dir / "input"
        output_path = work_dir / "output"

        ok = agent.connector.fetch_input(input_ref, str(input_path))
        if not ok:
            agent.connector.commit_result(attempt_id, "", 0, "input fetch failed")
            return

        # 2. Spawn executor
        cmd = [
            "python", "executor/run.py",
            task_type,
            str(input_path),
            str(output_path),
            "--worker-id", self.cfg.worker_id,
        ]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        with self._lock:
            self._running[attempt_id] = proc

        # 3. Lease renewal thread
        lease_expires = attempt["lease_expires"]
        renew_stop    = threading.Event()

        def _renew():
            while not renew_stop.is_set():
                remaining = lease_expires - time.time()
                if remaining < LEASE_RENEW_BEFORE:
                    agent.connector.renew_lease(attempt_id)
                time.sleep(5)

        renew_thread = threading.Thread(target=_renew, daemon=True)
        renew_thread.start()

        # 4. Wait for completion
        stdout, stderr = proc.communicate()
        renew_stop.set()

        # 5. Parse result
        exit_code  = proc.returncode
        units_done = 0
        error_msg: Optional[str] = None

        if exit_code == EXECUTOR_SUCCESS:
            try:
                last_line = stdout.strip().splitlines()[-1]
                meta      = json.loads(last_line)
                units_done = meta.get("units_done", 0)
            except Exception:
                error_msg = "executor stdout parse error"
        else:
            error_msg = stderr.decode(errors="replace")[:500] or f"exit {exit_code}"

        # 6. Upload output and commit
        output_ref = ""
        if not error_msg and output_path.exists():
            try:
                output_ref = agent.connector.upload_output(task["job_id"], task_id, str(output_path))
            except NotImplementedError:
                # Fallback: encode local path as ref until M5 upload endpoint exists
                output_ref = str(output_path)

        agent.connector.commit_result(attempt_id, output_ref, units_done, error_msg)
        log.info("Task %s finished: units=%d err=%s", task_id, units_done, error_msg)


_agent_ref = None

def _agent_singleton():
    """Return the running AgentRuntime instance (set by w1_runtime.start)."""
    return _agent_ref
