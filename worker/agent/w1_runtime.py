"""
worker/agent/w1_runtime.py  —  W1 Agent Runtime
================================================
Person B owns this file.

Responsibility:
  - Main entry point for the Worker process
  - Load config (master URL, worker_id, token, max_slots)
  - Manage Agent lifecycle: start → register → heartbeat loop → task loop → shutdown
  - Catch signals for graceful pause/drain before exit
"""

import os, signal, sys, time, threading, logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [W1] %(message)s")
log = logging.getLogger("w1")


class AgentConfig:
    master_url:   str  = os.environ.get("MASTER_URL",  "http://localhost:8000")
    token:        str  = os.environ.get("DORMPOOL_TOKEN", "dev-token")
    worker_id:    str  = os.environ.get("WORKER_ID",   "")
    max_slots:    int  = int(os.environ.get("MAX_SLOTS", "2"))
    data_dir:     Path = Path(os.environ.get("WORKER_DATA", "worker_data"))

    def __post_init__(self):
        if not self.worker_id:
            import socket, uuid
            self.worker_id = socket.gethostname() + "-" + uuid.uuid4().hex[:6]
        self.data_dir.mkdir(parents=True, exist_ok=True)


class AgentRuntime:
    """W1: boots all other worker modules and runs the main loops."""

    def __init__(self, config: AgentConfig):
        self.config   = config
        self._running = True
        self._pausing = False   # graceful drain requested

        # Modules wired in after construction (avoid circular imports)
        self.connector  = None   # W2 MasterConnector
        self.monitor    = None   # W3 ResourceMonitor
        self.contrib    = None   # W4 ContributionController
        self.exec_mgr   = None   # W5 ExecutionManager
        self.local_ctrl = None   # W6 LocalControl

    def start(self):
        from worker.agent.w2_connector  import MasterConnector
        from worker.agent.w3_monitor    import ResourceMonitor
        from worker.agent.w4_contrib    import ContributionController
        from worker.agent.w5_exec       import ExecutionManager
        from worker.agent.w6_control    import LocalControl

        cfg = self.config
        self.monitor    = ResourceMonitor(cfg.max_slots)
        self.contrib    = ContributionController(self.monitor)
        self.exec_mgr   = ExecutionManager(cfg)
        self.connector  = MasterConnector(cfg, self)
        self.local_ctrl = LocalControl(self)

        # Register with Master
        self.connector.register()

        # Launch loops
        threading.Thread(target=self.connector.heartbeat_loop, daemon=True).start()
        threading.Thread(target=self._task_loop,               daemon=True).start()
        self.local_ctrl.start()   # blocks (serves CLI / localhost)

    def _task_loop(self):
        while self._running:
            if self.contrib.can_accept():
                result = self.connector.claim_task()
                if result:
                    self.exec_mgr.run_task(result["task"], result["attempt"])
                else:
                    time.sleep(5)
            else:
                time.sleep(10)

    def pause(self):
        log.info("Pause requested — entering DRAINING state")
        self._pausing = True
        self.contrib.pause()

    def resume(self):
        log.info("Resume requested")
        self._pausing = False
        self.contrib.resume()

    def shutdown(self):
        log.info("Shutdown requested")
        self._running = False


def main():
    cfg = AgentConfig()
    agent = AgentRuntime(cfg)

    def _sig(sig, _frame):
        log.info("Signal %s received — graceful shutdown", sig)
        agent.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT,  _sig)
    signal.signal(signal.SIGTERM, _sig)
    agent.start()


if __name__ == "__main__":
    main()
