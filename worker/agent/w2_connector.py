"""
worker/agent/w2_connector.py  —  W2 Master Connector
=====================================================
Person B owns this file.

Responsibility:
  - Maintain HTTP connection to Master
  - Register, heartbeat, claim tasks, fetch input, renew lease, commit result
  - Auto-reconnect on failure (exponential backoff, max 60 s)
"""

import time, logging, requests, dataclasses
from typing import Optional
from shared.contracts import (
    WorkerCapability, WorkerHeartbeat,
    WorkerConnState, WorkerContribState, WorkerExecState,
    HEARTBEAT_INTERVAL,
)

log = logging.getLogger("w2")


class MasterConnector:
    def __init__(self, config, agent):
        self.cfg   = config
        self.agent = agent
        self._base = config.master_url.rstrip("/")
        self._hdrs = {"Authorization": f"Bearer {config.token}"}

    # ── Registration ──────────────────────────────────────────────────────────

    def register(self):
        import platform, psutil
        cap = WorkerCapability(
            worker_id=self.cfg.worker_id,
            hostname=platform.node(),
            cpu_cores=psutil.cpu_count(logical=False) or 1,
            ram_gb=round(psutil.virtual_memory().total / 1e9, 1),
            max_slots=self.cfg.max_slots,
        )
        # TODO (Person B): detect GPU via torch/nvml and populate cap.gpu_name / gpu_vram_gb
        resp = self._post("/workers/register", dataclasses.asdict(cap))
        log.info("Registered: %s", resp)

    # ── Heartbeat loop ────────────────────────────────────────────────────────

    def heartbeat_loop(self):
        while self.agent._running:
            try:
                self._send_heartbeat()
            except Exception as exc:
                log.warning("Heartbeat failed: %s", exc)
            time.sleep(HEARTBEAT_INTERVAL)

    def _send_heartbeat(self):
        mon   = self.agent.monitor
        contrib = self.agent.contrib
        hb = WorkerHeartbeat(
            worker_id      = self.cfg.worker_id,
            conn_state     = WorkerConnState.ONLINE,
            contrib_state  = contrib.state(),
            exec_state     = WorkerExecState.BUSY if mon.busy_slots() > 0 else WorkerExecState.IDLE,
            free_slots     = mon.free_slots(),
            running_task_ids = self.agent.exec_mgr.running_task_ids(),
            cpu_pct        = mon.cpu_pct(),
            gpu_pct        = mon.gpu_pct(),
        )
        self._post(f"/workers/{self.cfg.worker_id}/heartbeat", dataclasses.asdict(hb))

    # ── Task pull ─────────────────────────────────────────────────────────────

    def claim_task(self) -> Optional[dict]:
        try:
            resp = self._post(
                f"/workers/{self.cfg.worker_id}/claim",
                {"free_slots": self.agent.monitor.free_slots()},
            )
            return resp if resp and "task" in resp else None
        except Exception as exc:
            log.warning("claim_task failed: %s", exc)
            return None

    def fetch_input(self, ref: str, dest_path: str) -> bool:
        try:
            r = requests.get(f"{self._base}/data/{ref}", headers=self._hdrs, timeout=60, stream=True)
            r.raise_for_status()
            with open(dest_path, "wb") as f:
                for chunk in r.iter_content(65536):
                    f.write(chunk)
            return True
        except Exception as exc:
            log.warning("fetch_input %s failed: %s", ref, exc)
            return False

    def renew_lease(self, attempt_id: str) -> bool:
        try:
            self._post(f"/attempts/{attempt_id}/renew", {})
            return True
        except Exception:
            return False

    def commit_result(self, attempt_id: str, output_ref: str,
                      units_done: int, error_msg: Optional[str] = None) -> bool:
        try:
            self._post(f"/attempts/{attempt_id}/commit", {
                "output_ref": output_ref,
                "units_done": units_done,
                "error_msg":  error_msg,
            })
            return True
        except Exception as exc:
            log.warning("commit_result failed: %s", exc)
            return False

    # ── Upload ────────────────────────────────────────────────────────────────

    def upload_output(self, job_id: str, task_id: str, file_path: str) -> str:
        """Upload result file and return the ref key."""
        # TODO (Person B): implement multipart POST to /data/upload once M5 adds that endpoint
        raise NotImplementedError("upload_output not implemented")

    # ── HTTP helpers ──────────────────────────────────────────────────────────

    def _post(self, path: str, body: dict, retries: int = 3) -> dict:
        url = f"{self._base}{path}"
        backoff = 2
        for attempt in range(retries):
            try:
                r = requests.post(url, json=body, headers=self._hdrs, timeout=15)
                r.raise_for_status()
                return r.json()
            except Exception as exc:
                if attempt == retries - 1:
                    raise
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
        return {}
