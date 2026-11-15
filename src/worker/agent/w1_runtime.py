"""
worker/agent/w1_runtime.py  —  W1 Agent Runtime
================================================
Owner: Person B.

Boot order (each step is observable in the log, which the gates read):

  ping Master (backoff until reachable)
  -> register the REAL capability of this machine
  -> start heartbeat thread
  -> start control surface (REPL, stdin reader, or localhost HTTP)
  -> run the task loop until shutdown

The task loop is the only place that decides "may I take more work":
  paused            -> idle, slower poll
  no free slot      -> idle
  claim returned 204-> back off briefly
  claim returned task-> hand to W5 and immediately try again
"""

from __future__ import annotations

import argparse
import logging
import os
import platform
import signal
import sys
import threading
import uuid
from pathlib import Path

from dormpool.src.shared.contracts import WorkerContribState

log = logging.getLogger("w1")

POLL_IDLE = 0.25  # only back off when the Master has no queued work
POLL_PAUSED = 10.0
POLL_ERROR = 5.0


class AgentConfig:
    """Environment-driven configuration. Every value has a working default."""

    def __init__(self, master_url=None, worker_id=None,
                 max_slots=None, data_dir=None, control_port=None):
        self.master_url = master_url or os.environ.get("MASTER_URL", "http://127.0.0.1:8000")
        self.max_slots = int(max_slots if max_slots is not None
                             else os.environ.get("MAX_SLOTS", "1"))
        self.data_dir = Path(data_dir or os.environ.get("WORKER_DATA", "worker_data"))

        explicit_id = worker_id or os.environ.get("WORKER_ID", "")
        if explicit_id:
            self.worker_id = explicit_id
        else:
            try:
                host = platform.node() or "worker"
            except Exception:
                host = "worker"
            self.worker_id = f"{host}-{uuid.uuid4().hex[:6]}"

        port_raw = control_port if control_port is not None else os.environ.get("WORKER_CONTROL_PORT", "0")
        try:
            self.control_port = int(port_raw or 0)
        except (TypeError, ValueError):
            self.control_port = 0

        self.headless = bool(int(os.environ.get("WORKER_NO_REPL", "0") or 0))

    def prepare(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)


class AgentRuntime:
    def __init__(self, config: AgentConfig):
        self.config = config
        self._running = True
        self._draining = False
        self._stop_event = threading.Event()
        self._work_event = threading.Event()
        self._heartbeat_stop = threading.Event()

        self.monitor = None
        self.contrib = None
        self.exec_mgr = None
        self.connector = None
        self.control = None

    # ── lifecycle ────────────────────────────────────────────────────────────

    def start(self) -> None:
        from dormpool.src.worker.agent.w3_monitor import ResourceMonitor
        from dormpool.src.worker.agent.w4_contrib import ContributionController
        from dormpool.src.worker.agent.w5_exec import ExecutionManager
        from dormpool.src.worker.agent.w2_connector import MasterConnector
        from dormpool.src.worker.agent.w6_control import LocalControl

        cfg = self.config
        cfg.prepare()
        # if cfg.max_slots != 1:
        #     log.warning("resident Executor capacity is 1; requested max_slots=%s, using 1", cfg.max_slots)
        #     cfg.max_slots = 1

        self.monitor = ResourceMonitor(cfg.max_slots)
        self.contrib = ContributionController(self.monitor)
        self.exec_mgr = ExecutionManager(cfg)
        self.connector = MasterConnector(cfg, self)
        self.control = LocalControl(self)

        log.info("worker_id=%s master=%s max_slots=%d data=%s",
                 cfg.worker_id, cfg.master_url, cfg.max_slots, cfg.data_dir)

        try:
            if not self.connector.wait_for_master():
                log.error("Master never became reachable at %s — exiting", cfg.master_url)
                return
            self.connector.register()
            threading.Thread(target=self.connector.heartbeat_loop,
                             name="heartbeat", daemon=True).start()
            self.control.start()
            self._task_loop()
        finally:
            self.shutdown()
            self.exec_mgr.close()
            self._heartbeat_stop.set()
            self.monitor.stop_sampling()
            self.control.stop()
            log.info("agent stopped; Executor reaped")

    def pause(self) -> None:
        self._draining = True
        self.contrib.pause()
        self._work_event.set()
        log.info("contrib_state -> draining/paused")
        self._push_state_now()

    def resume(self) -> None:
        self._draining = False
        self.contrib.resume()
        log.info("contrib_state -> available")
        self._push_state_now(wake=True)

    def _push_state_now(self, wake=False) -> None:
        """Send one heartbeat immediately so the Master sees the new state at once.

        Without this the registry keeps the previous contribution state until the
        next scheduled beat — up to HEARTBEAT_INTERVAL of stale data, during which
        a paused node would still look available.
        """
        connector = self.connector
        if connector is None:
            return

        def _send():
            try:
                connector.send_heartbeat()
            except Exception as exc:
                log.warning("immediate heartbeat failed: %s", exc)
            finally:
                if wake:
                    self._work_event.set()

        threading.Thread(target=_send, name="heartbeat-now", daemon=True).start()

    def shutdown(self) -> None:
        if not self._running:
            return
        log.info("shutdown requested — finishing in-flight work")
        self._running = False
        self._stop_event.set()
        self._work_event.set()
        if self.exec_mgr is not None:
            self.exec_mgr.stop_claims()

    def is_running(self) -> bool:
        return self._running

    # ── task loop ────────────────────────────────────────────────────────────

    def _task_loop(self) -> None:
        while self._running:
            # Clear before checking capacity so a concurrent completion cannot
            # be lost between the capacity check and wait().
            self._work_event.clear()
            try:
                if not self.contrib.can_accept():
                    state = self.contrib.state()
                    self._work_event.wait(
                        POLL_IDLE if state == WorkerContribState.AVAILABLE else POLL_PAUSED
                    )
                    continue

                claimed = self.connector.claim_task()
                if not claimed:
                    self._work_event.wait(POLL_IDLE)
                    continue

                task = claimed.get("task") or {}
                attempt = claimed.get("attempt") or {}
                if not self._running or not self.contrib.can_accept():
                    self.connector.commit_result(attempt["attempt_id"], "", 0, "worker paused/stopping during claim")
                    continue
                log.info("claimed %s (%s)", task.get("task_id"), task.get("task_type"))
                self.exec_mgr.run_task(task, attempt, agent=self)
            except Exception as exc:
                log.warning("task loop error: %s", exc)
                self._stop_event.wait(POLL_ERROR)


def _install_signal_handlers(agent: AgentRuntime) -> None:
    def handler(signum, _frame):
        log.info("signal %s received", signum)
        agent.shutdown()

    for sig in (getattr(signal, "SIGINT", None), getattr(signal, "SIGTERM", None)):
        if sig is not None:
            try:
                signal.signal(sig, handler)
            except (ValueError, OSError):        # not on the main thread
                pass


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="DormPool worker agent")
    parser.add_argument("--headless", action="store_true",
                        help="skip the interactive REPL (required under CI / gates)")
    parser.add_argument("--master", default=None)
    parser.add_argument("--worker-id", default=None)
    parser.add_argument("--max-slots", type=int, default=None)
    parser.add_argument("--control-port", type=int, default=None)
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
    )

    if args.headless:
        os.environ["WORKER_NO_REPL"] = "1"

    config = AgentConfig(
        master_url=args.master,
        worker_id=args.worker_id,
        max_slots=args.max_slots,
        control_port=args.control_port,
    )
    agent = AgentRuntime(config)
    _install_signal_handlers(agent)

    try:
        agent.start()
    except KeyboardInterrupt:
        agent.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
