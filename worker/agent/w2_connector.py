"""
worker/agent/w2_connector.py  —  W2 Master Connector
=====================================================
Owner: Person B.

The only place that speaks HTTP to the Master. Everything above it (W1/W4/W5)
calls plain methods and never sees requests, retries or JSON.

Rules
  * 4xx never retries — a rejected request means the request was wrong,
    and repeating it just burns the schedule.
  * 5xx / timeout / connection refused retries with 2,4,8,16,32,60s backoff.
  * heartbeat_loop never dies: a dead Master must not kill the agent.
"""

from __future__ import annotations

import dataclasses
import logging
import os
import time
from typing import Optional

import requests

from shared.contracts import (
    WorkerCapability,
    WorkerHeartbeat,
    WorkerConnState,
    WorkerContribState,
    WorkerExecState,
    HEARTBEAT_INTERVAL,
)

log = logging.getLogger("w2")

CONNECT_TIMEOUT = 5.0
READ_TIMEOUT = 30.0
COMMIT_TIMEOUT = 60.0
MAX_RETRIES = 3
BACKOFF = (2, 4, 8, 16, 32, 60)


class MasterConnector:
    def __init__(self, config, agent):
        self.cfg = config
        self.agent = agent
        self._base = str(config.master_url).rstrip("/")
        # NOTE: Content-Type is NOT set here. Setting it globally breaks
        # multipart uploads (requests would keep "application/json" and never
        # generate a boundary, so FastAPI answers 422). JSON calls set it
        # explicitly in _request().
        self._headers = {"Authorization": f"Bearer {config.token}"}
        self._session = requests.Session()
        self._session.headers.update(self._headers)

    # ── transport ────────────────────────────────────────────────────────────

    def _post(self, path: str, body: Optional[dict] = None,
              timeout: Optional[float] = None) -> Optional[dict]:
        return self._request("POST", path, json_body=body or {}, timeout=timeout)

    def _get(self, path: str, timeout: Optional[float] = None) -> Optional[dict]:
        return self._request("GET", path, timeout=timeout)

    def _request(self, method: str, path: str, json_body: Optional[dict] = None,
                 timeout: Optional[float] = None) -> Optional[dict]:
        url = f"{self._base}{path}"
        read_timeout = timeout or READ_TIMEOUT
        last_exc: Optional[Exception] = None
        # JSON content type is applied per request so multipart calls elsewhere
        # are free to let requests generate their own boundary.
        request_headers = {"Content-Type": "application/json"} if json_body is not None else {}

        for attempt in range(MAX_RETRIES):
            try:
                response = self._session.request(
                    method,
                    url,
                    json=json_body,
                    headers=request_headers or None,
                    timeout=(CONNECT_TIMEOUT, read_timeout),
                )
                if response.status_code == 204:
                    return None
                if 400 <= response.status_code < 500:
                    # Client error: retrying cannot help.
                    log.warning("%s %s -> HTTP %s (%s)", method, path,
                                response.status_code, response.text[:200])
                    response.raise_for_status()
                if response.status_code >= 500:
                    raise requests.HTTPError(f"HTTP {response.status_code}")
                if not response.content:
                    return {}
                try:
                    return response.json()
                except ValueError:
                    return {}
            except requests.HTTPError as exc:
                status = getattr(getattr(exc, "response", None), "status_code", None)
                if status is not None and 400 <= status < 500:
                    raise                        # already logged; do not retry
                last_exc = exc
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_exc = exc
            except Exception as exc:             # pragma: no cover - defensive
                last_exc = exc

            if attempt < MAX_RETRIES - 1:
                delay = BACKOFF[min(attempt, len(BACKOFF) - 1)]
                time.sleep(delay)

        if last_exc is not None:
            raise last_exc
        return None

    # ── registration ─────────────────────────────────────────────────────────

    def ping(self) -> bool:
        try:
            result = self._request("GET", "/ping", timeout=5.0)
            return bool(result and result.get("ok"))
        except Exception:
            return False

    def wait_for_master(self, max_wait_s: float = 600.0) -> bool:
        """Poll /ping with growing backoff until the Master answers."""
        deadline = time.time() + max_wait_s
        delay = 2
        while time.time() < deadline and self.agent._running:
            if self.ping():
                return True
            log.warning("Cannot reach Master at %s — retrying in %ss", self._base, delay)
            time.sleep(delay)
            delay = min(delay * 2, 60)
        return False

    def register(self, monitor=None) -> dict:
        """Send this machine's REAL capability. Returns the Master's reply."""
        monitor = monitor or self.agent.monitor
        # ResourceMonitor caches the sniffed capability per process.
        capability = monitor.capability(self.cfg.worker_id)
        body = dataclasses.asdict(capability)
        reply = self._post("/workers/register", body) or {}
        log.info(
            "Registered worker_id=%s cpu=%d ram=%sGB gpu=%s score=%s",
            capability.worker_id, capability.cpu_cores, capability.ram_gb,
            capability.gpu_name or "none", capability.executor_version,
        )
        return reply

    # ── heartbeat ────────────────────────────────────────────────────────────

    def heartbeat_loop(self) -> None:
        while self.agent._running:
            try:
                self.send_heartbeat()
            except Exception as exc:              # never let the loop die
                log.warning("heartbeat failed: %s", exc)
            time.sleep(HEARTBEAT_INTERVAL)

    def send_heartbeat(self) -> None:
        monitor = self.agent.monitor
        controller = self.agent.contrib
        executor = self.agent.exec_mgr

        busy = monitor.busy_slots()
        contrib_state = controller.state()
        # A paused/draining node accepts no new work, so it must not advertise
        # capacity: free_slots is the pool's supply signal and reporting 1 while
        # refusing claims would make the Master plan around a slot that never
        # materialises.
        advertised_slots = monitor.free_slots() if contrib_state == WorkerContribState.AVAILABLE else 0

        heartbeat = WorkerHeartbeat(
            worker_id=self.cfg.worker_id,
            conn_state=WorkerConnState.ONLINE,
            contrib_state=contrib_state,
            exec_state=WorkerExecState.BUSY if busy > 0 else WorkerExecState.IDLE,
            free_slots=advertised_slots,
            running_task_ids=executor.running_task_ids() if executor else [],
            cpu_pct=monitor.cpu_pct(),
            gpu_pct=monitor.gpu_pct(),
        )
        try:
            self._post(
                f"/workers/{self.cfg.worker_id}/heartbeat",
                dataclasses.asdict(heartbeat),
                timeout=10.0,
            )
        except requests.HTTPError as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if status == 404:
                # Master restarted and lost our registration — re-register now.
                log.warning("heartbeat 404: Master does not know us, re-registering")
                self.register()
            else:
                raise

    # ── tasks ────────────────────────────────────────────────────────────────

    def claim_task(self) -> Optional[dict]:
        monitor = self.agent.monitor
        result = self._post(
            f"/workers/{self.cfg.worker_id}/claim",
            {"free_slots": monitor.free_slots()},
            timeout=15.0,
        )
        if result and "task" in result:
            return result
        return None

    def fetch_input(self, ref: str, dest_path: str) -> bool:
        url = f"{self._base}/data/{ref}"
        try:
            with self._session.get(url, timeout=(CONNECT_TIMEOUT, 60.0), stream=True) as response:
                response.raise_for_status()
                os.makedirs(os.path.dirname(os.path.abspath(dest_path)), exist_ok=True)
                with open(dest_path, "wb") as fh:
                    for chunk in response.iter_content(65536):
                        if chunk:
                            fh.write(chunk)
            return True
        except Exception as exc:
            log.warning("fetch_input %s failed: %s", ref, exc)
            return False

    def upload_output(self, job_id: str, task_id: str, path: str) -> Optional[str]:
        url = f"{self._base}/data/upload/{job_id}/{task_id}"
        try:
            with open(path, "rb") as fh:
                response = self._session.post(
                    url,
                    files={"file": (os.path.basename(path), fh, "application/zip")},
                    timeout=(CONNECT_TIMEOUT, 60.0),
                )
            response.raise_for_status()
            payload = response.json() if response.content else {}
            return payload.get("output_ref")
        except Exception as exc:
            log.warning("upload_output %s failed: %s", task_id, exc)
            return None

    def renew_lease(self, attempt_id: str) -> bool:
        try:
            self._post(f"/attempts/{attempt_id}/renew", {}, timeout=10.0)
            return True
        except Exception as exc:
            log.warning("renew_lease %s failed: %s", attempt_id, exc)
            return False

    def commit_result(self, attempt_id: str, output_ref: str, units_done: int,
                      error_msg: Optional[str] = None) -> Optional[dict]:
        try:
            return self._post(
                f"/attempts/{attempt_id}/commit",
                {
                    "output_ref": output_ref or "",
                    "units_done": int(units_done),
                    "error_msg": error_msg,
                },
                timeout=COMMIT_TIMEOUT,
            )
        except Exception as exc:
            log.warning("commit_result %s failed: %s", attempt_id, exc)
            return None
