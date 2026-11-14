"""
worker/agent/w5_exec.py  —  W5 Execution Manager
==================================================
Owner: Person B.

One Task = one thread = one executor subprocess:

  acquire a slot -> fetch the shard -> spawn `python executor/run.py ...`
  -> renew the lease in the background while it runs
  -> parse the last stdout line -> upload the result -> commit
  -> release the slot (always, in `finally`)

A task failure (exit 1) or a crash cannot take the agent down: every exception
is caught here and reported to the Master as an attempt failure. That separation
is what lets one bad image cost one task instead of the whole node.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import threading
import time
from typing import Dict, List, Optional

from shared.contracts import (
    EXECUTOR_FATAL,
    EXECUTOR_SUCCESS,
    LEASE_DURATION,
    LEASE_RENEW_BEFORE,
)

log = logging.getLogger("w5")

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
EXECUTOR_PATH = os.path.join(REPO_ROOT, "executor", "run.py")


class ExecutionManager:
    def __init__(self, config):
        self.cfg = config
        self._running: Dict[str, subprocess.Popen] = {}
        self._lock = threading.RLock()

    # ── public ───────────────────────────────────────────────────────────────

    def run_task(self, task: dict, attempt: dict, agent=None) -> None:
        """Start the task in a background thread; returns immediately."""
        thread = threading.Thread(
            target=self._execute,
            args=(task, attempt, agent),
            daemon=True,
            name=f"task-{task.get('task_id', 'unknown')}",
        )
        # Reserve synchronously: the pull loop can immediately claim the next
        # free slot without sleeping for the execution thread to catch up.
        if agent is not None:
            agent.monitor.slot_acquire()
        try:
            thread.start()
        except Exception:
            if agent is not None:
                agent.monitor.slot_release()
                agent._work_event.set()
            raise

    def running_task_ids(self) -> List[str]:
        with self._lock:
            return list(self._running.keys())

    def active_count(self) -> int:
        with self._lock:
            return len(self._running)

    # ── internal ─────────────────────────────────────────────────────────────

    def _execute(self, task: dict, attempt: dict, agent) -> None:
        task_id = str(task.get("task_id") or "unknown")
        attempt_id = str(attempt.get("attempt_id") or "unknown")

        try:
            self.cfg.data_dir.mkdir(parents=True, exist_ok=True)
            work_dir = self.cfg.data_dir / task_id
            work_dir.mkdir(parents=True, exist_ok=True)
            input_path = work_dir / "input.zip"
            output_path = work_dir / "output.zip"
            self._run_guarded(task, attempt, agent, input_path, output_path)
        except Exception as exc:                       # pragma: no cover - belt and braces
            log.exception("unhandled error in task %s", task_id)
            try:
                if agent is not None:
                    agent.connector.commit_result(attempt_id, "", 0, f"internal error: {exc}")
            except Exception:
                pass
        finally:
            with self._lock:
                self._running.pop(attempt_id, None)
            if agent is not None:
                agent.monitor.slot_release()
                agent._work_event.set()

    def _run_guarded(self, task: dict, attempt: dict, agent,
                     input_path, output_path) -> None:
        task_id = str(task["task_id"])
        attempt_id = str(attempt["attempt_id"])
        task_type = str(task.get("task_type") or "convnext_infer")
        input_ref = str(task.get("input_ref") or "")
        job_id = str(task.get("job_id") or "")

        if agent is None:
            log.error("no agent bound; cannot execute task %s", task_id)
            return

        # 1. fetch the shard
        if not agent.connector.fetch_input(input_ref, str(input_path)):
            agent.connector.commit_result(attempt_id, "", 0, "input fetch failed")
            return

        # 2. spawn the executor
        cmd = [
            sys.executable or "python",
            EXECUTOR_PATH,
            task_type,
            str(input_path),
            str(output_path),
            "--worker-id", self.cfg.worker_id,
        ]
        log.info("starting executor for task %s (%s)", task_id, task_type)
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=REPO_ROOT,
            )
        except Exception as exc:
            agent.connector.commit_result(attempt_id, "", 0, f"spawn failed: {exc}")
            return

        with self._lock:
            self._running[attempt_id] = proc

        # 3. keep the lease alive while it works
        lease_expires = float(attempt.get("lease_expires") or (time.time() + LEASE_DURATION))
        stop_renew = threading.Event()
        renew_thread = threading.Thread(
            target=self._renew_loop,
            args=(agent, attempt_id, lease_expires, stop_renew),
            daemon=True,
            name=f"renew-{attempt_id}",
        )
        renew_thread.start()

        # 4. wait, with a hard ceiling so a hung child cannot wedge the slot
        timeout_s = max(30.0, LEASE_DURATION - 30.0)
        try:
            stdout, stderr = proc.communicate(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            log.warning("executor timeout for task %s — killing", task_id)
            proc.kill()
            try:
                proc.communicate(timeout=10)
            except Exception:
                pass
            stop_renew.set()
            agent.connector.commit_result(attempt_id, "", 0, "executor timeout")
            return
        finally:
            stop_renew.set()

        exit_code = proc.returncode
        if exit_code == EXECUTOR_SUCCESS and stderr:
            log.info("executor %s: %s", task_id, stderr.decode("utf-8", "replace").strip())
        units_done, detail, error_msg = self._parse_result(exit_code, stdout, stderr, task_id)

        # 5. upload + commit
        output_ref: Optional[str] = None
        if error_msg is None and output_path.exists():
            output_ref = agent.connector.upload_output(job_id, task_id, str(output_path))
            if output_ref is None:
                error_msg = "result upload failed"

        if error_msg is None:
            log.info("task %s finished: units=%d", task_id, units_done)
            agent.connector.commit_result(attempt_id, output_ref or "", units_done, None)
        else:
            log.warning("task %s failed: %s", task_id, error_msg)
            agent.connector.commit_result(attempt_id, "", 0, error_msg)

    # ── helpers ──────────────────────────────────────────────────────────────

    def _renew_loop(self, agent, attempt_id: str, lease_expires: float,
                    stop_event: threading.Event) -> None:
        while not stop_event.wait(5):
            remaining = lease_expires - time.time()
            if remaining < LEASE_RENEW_BEFORE:
                if agent.connector.renew_lease(attempt_id):
                    lease_expires = time.time() + LEASE_DURATION
                else:
                    # Keep trying; the Master may simply be restarting.
                    lease_expires = time.time() + LEASE_RENEW_BEFORE

    @staticmethod
    def _parse_result(exit_code: Optional[int], stdout: bytes, stderr: bytes,
                      task_id: str):
        """Return (units_done, detail, error_msg). error_msg None means success."""
        text = (stdout or b"").decode("utf-8", "replace").strip()
        err_text = (stderr or b"").decode("utf-8", "replace").strip()

        units_done = 0
        detail = ""
        for line in reversed(text.splitlines()):
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except ValueError:
                continue
            units_done = int(payload.get("units_done") or 0)
            detail = str(payload.get("detail") or "")
            break

        if exit_code == EXECUTOR_SUCCESS:
            if units_done <= 0:
                return 0, detail, f"executor reported no work units: {detail[:200]}"
            return units_done, detail, None

        if exit_code == EXECUTOR_FATAL:
            return 0, detail, f"executor fatal: {detail or err_text[:300]}"

        return 0, detail, (detail or err_text or f"executor exit {exit_code}")[:400]
