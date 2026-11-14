"""
master/app/m2_registry.py  —  M2 Worker Registry
==================================================
Person A owns this file.

Responsibility:
  - Register workers and store capability
  - Accept heartbeats; update three-dimension state
    (conn_state / contrib_state / exec_state)
  - Expire workers that miss heartbeats (WORKER_TIMEOUT)
  - Return available workers to M4 Scheduler

State is kept in memory for the 18-hour sprint.
Replace with SQLite-backed store if persistence is needed.
"""

import time, threading
from typing import Dict, Optional
from shared.contracts import (
    WorkerCapability, WorkerHeartbeat,
    WorkerConnState, WorkerContribState, WorkerExecState,
    WORKER_TIMEOUT,
)


class WorkerEntry:
    def __init__(self, cap: WorkerCapability):
        self.capability  = cap
        self.conn_state  = WorkerConnState.ONLINE
        self.contrib     = WorkerContribState.AVAILABLE
        self.exec_state  = WorkerExecState.IDLE
        self.free_slots  = cap.max_slots
        self.running_task_ids: list[str] = []
        self.cpu_pct: Optional[float] = None
        self.gpu_pct: Optional[float] = None
        self.last_seen   = time.time()

    def to_dict(self) -> dict:
        return {
            "worker_id":   self.capability.worker_id,
            "hostname":    self.capability.hostname,
            "conn_state":  self.conn_state,
            "contrib":     self.contrib,
            "exec_state":  self.exec_state,
            "free_slots":  self.free_slots,
            "cpu_pct":     self.cpu_pct,
            "gpu_pct":     self.gpu_pct,
            "last_seen":   self.last_seen,
        }


class WorkerRegistry:
    def __init__(self):
        self._workers: Dict[str, WorkerEntry] = {}
        self._lock = threading.Lock()
        # start background expiry thread
        t = threading.Thread(target=self._expire_loop, daemon=True)
        t.start()

    def register(self, cap: WorkerCapability) -> dict:
        with self._lock:
            if cap.worker_id in self._workers:
                # reconnect: reset state, keep history
                entry = self._workers[cap.worker_id]
                entry.capability = cap
                entry.conn_state = WorkerConnState.ONLINE
                entry.last_seen  = time.time()
            else:
                self._workers[cap.worker_id] = WorkerEntry(cap)
        return {"registered": cap.worker_id}

    def heartbeat(self, hb: WorkerHeartbeat) -> dict:
        with self._lock:
            entry = self._workers.get(hb.worker_id)
            if not entry:
                return {"error": "unknown worker"}
            entry.conn_state        = hb.conn_state
            entry.contrib           = hb.contrib_state
            entry.exec_state        = hb.exec_state
            entry.free_slots        = hb.free_slots
            entry.running_task_ids  = hb.running_task_ids
            entry.cpu_pct           = hb.cpu_pct
            entry.gpu_pct           = hb.gpu_pct
            entry.last_seen         = time.time()
        return {"ok": True}

    def available_workers(self) -> list[WorkerEntry]:
        """Workers that can accept at least one new task right now."""
        with self._lock:
            return [
                e for e in self._workers.values()
                if e.conn_state  == WorkerConnState.ONLINE
                and e.contrib    == WorkerContribState.AVAILABLE
                and e.free_slots  > 0
            ]

    def get_worker(self, worker_id: str) -> Optional[WorkerEntry]:
        with self._lock:
            return self._workers.get(worker_id)

    def list_workers(self) -> list[dict]:
        with self._lock:
            return [e.to_dict() for e in self._workers.values()]

    def _expire_loop(self):
        while True:
            time.sleep(WORKER_TIMEOUT // 2)
            deadline = time.time() - WORKER_TIMEOUT
            with self._lock:
                for entry in self._workers.values():
                    if entry.last_seen < deadline:
                        entry.conn_state = WorkerConnState.OFFLINE
                        entry.free_slots = 0


# Module-level singleton used by m1_api and m4_scheduler
registry = WorkerRegistry()
