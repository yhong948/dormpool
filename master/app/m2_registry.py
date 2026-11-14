"""
master/app/m2_registry.py  —  M2 Worker Registry
==================================================
Owner: Person A.

Tracks every Worker as three independent state dimensions:
  conn_state    ONLINE / OFFLINE
  contrib_state AVAILABLE / PAUSED / DRAINING
  exec_state    IDLE / BUSY

Responsibilities
  * register()      — new node vs reconnect, keyed on worker_id
  * heartbeat()     — refresh state, record pause/resume events
  * expire loop     — mark a node OFFLINE after WORKER_TIMEOUT without a beat
  * available_workers() — the pool M4 draws from

Public shape of a worker (docs/验收门禁.md §0.6):
  {"worker_id","hostname","conn_state","contrib_state","exec_state","free_slots",
   "max_slots","running_task_ids","cpu_cores","ram_gb","gpu_name","gpu_vram_gb",
   "executor_version","cpu_pct","gpu_pct","last_seen"}
"""

from __future__ import annotations

import threading
import time
from typing import Dict, List, Optional

from shared.contracts import (
    WorkerCapability,
    WorkerHeartbeat,
    WorkerConnState,
    WorkerContribState,
    WorkerExecState,
    WORKER_TIMEOUT,
)

# Optional hooks installed by the composition root so M2 stays free of imports
# from M4/M6 (which would create a cycle).
_on_offline = None      # callable(worker_id)
_on_node_event = None   # callable(worker_id, event_name)


def bind_hooks(on_offline=None, on_node_event=None) -> None:
    global _on_offline, _on_node_event
    if on_offline is not None:
        _on_offline = on_offline
    if on_node_event is not None:
        _on_node_event = on_node_event


class WorkerEntry:
    """One registered node plus its live state."""

    __slots__ = (
        "capability", "conn_state", "contrib", "exec_state", "free_slots",
        "running_task_ids", "cpu_pct", "gpu_pct", "last_seen", "registered_at",
    )

    def __init__(self, cap: WorkerCapability):
        self.capability = cap
        self.conn_state = WorkerConnState.ONLINE
        self.contrib = WorkerContribState.AVAILABLE
        self.exec_state = WorkerExecState.IDLE
        self.free_slots = int(cap.max_slots)
        self.running_task_ids: List[str] = []
        self.cpu_pct: Optional[float] = None
        self.gpu_pct: Optional[float] = None
        self.last_seen = time.time()
        self.registered_at = time.time()

    # ── serialisation ────────────────────────────────────────────────────────

    def to_dict(self) -> dict:
        """JSON-safe view. Field names are frozen by the acceptance gates."""
        cap = self.capability
        return {
            "worker_id": cap.worker_id,
            "hostname": cap.hostname,
            "conn_state": _enum_value(self.conn_state),
            "contrib_state": _enum_value(self.contrib),
            "exec_state": _enum_value(self.exec_state),
            "free_slots": int(self.free_slots),
            "max_slots": int(cap.max_slots),
            "running_task_ids": list(self.running_task_ids),
            # capability fields — required so G2 can prove real sniffing
            "cpu_cores": int(cap.cpu_cores),
            "ram_gb": float(cap.ram_gb),
            "gpu_name": cap.gpu_name,          # None -> JSON null, never "None"
            "gpu_vram_gb": cap.gpu_vram_gb,
            "executor_version": cap.executor_version,
            "cpu_pct": self.cpu_pct,
            "gpu_pct": self.gpu_pct,
            "last_seen": self.last_seen,
            "registered_at": self.registered_at,
        }


def _enum_value(value):
    """Accept both Enum members and plain strings (heartbeats may carry either)."""
    return value.value if hasattr(value, "value") else value


def _as_enum(enum_cls, value, default):
    """Coerce an incoming string/enum into the enum type."""
    if value is None:
        return default
    if isinstance(value, enum_cls):
        return value
    try:
        return enum_cls(str(value))
    except ValueError:
        return default


class WorkerRegistry:
    def __init__(self):
        self._workers: Dict[str, WorkerEntry] = {}
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # ── lifecycle ────────────────────────────────────────────────────────────

    def start(self) -> None:
        """Start the expiry loop. Safe to call twice."""
        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._expire_loop, name="m2-expire", daemon=True
            )
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    # ── registration ─────────────────────────────────────────────────────────

    def register(self, cap: WorkerCapability) -> dict:
        """Create or refresh a node. Returns {"registered": worker_id, "reconnect": bool}."""
        with self._lock:
            existing = self._workers.get(cap.worker_id)
            if existing is None:
                entry = WorkerEntry(cap)
                self._workers[cap.worker_id] = entry
                reconnect = False
                event = "join"
            else:
                # Reconnect: keep identity and history, refresh capability + state.
                was_offline = existing.conn_state == WorkerConnState.OFFLINE
                existing.capability = cap
                existing.conn_state = WorkerConnState.ONLINE
                existing.last_seen = time.time()
                # A node that reconnects starts accepting work again; its owner can
                # pause it immediately afterwards if that is what they want.
                if existing.contrib in (WorkerContribState.PAUSED, WorkerContribState.DRAINING):
                    existing.contrib = WorkerContribState.AVAILABLE
                existing.free_slots = max(0, int(cap.max_slots) - len(existing.running_task_ids))
                reconnect = True
                event = "join" if was_offline else None
                entry = existing

        if event and _on_node_event:
            _safe(_on_node_event, cap.worker_id, event)
        return {"registered": cap.worker_id, "reconnect": reconnect}

    # ── heartbeat ────────────────────────────────────────────────────────────

    def heartbeat(self, hb: WorkerHeartbeat) -> dict:
        with self._lock:
            entry = self._workers.get(hb.worker_id)
            if entry is None:
                return {"error": "unknown worker", "worker_id": hb.worker_id}

            previous_contrib = entry.contrib
            new_contrib = _as_enum(WorkerContribState, hb.contrib_state, entry.contrib)

            entry.conn_state = _as_enum(WorkerConnState, hb.conn_state, WorkerConnState.ONLINE)
            entry.contrib = new_contrib
            entry.exec_state = _as_enum(WorkerExecState, hb.exec_state, entry.exec_state)
            entry.free_slots = max(0, int(hb.free_slots))
            entry.running_task_ids = list(hb.running_task_ids or [])
            entry.cpu_pct = hb.cpu_pct
            entry.gpu_pct = hb.gpu_pct
            entry.last_seen = time.time()

        if previous_contrib != new_contrib and _on_node_event:
            if new_contrib == WorkerContribState.PAUSED:
                _safe(_on_node_event, hb.worker_id, "pause")
            elif new_contrib == WorkerContribState.AVAILABLE:
                _safe(_on_node_event, hb.worker_id, "resume")
        return {"ok": True, "contrib_state": _enum_value(entry.contrib)}

    # ── queries ──────────────────────────────────────────────────────────────

    def get_worker(self, worker_id: str) -> Optional[WorkerEntry]:
        with self._lock:
            return self._workers.get(worker_id)

    def list_workers(self) -> List[dict]:
        with self._lock:
            return [e.to_dict() for e in self._workers.values()]

    def snapshot_entries(self) -> List[dict]:
        return self.list_workers()

    def available_workers(self) -> List[dict]:
        """Nodes that may take at least one new task right now."""
        with self._lock:
            return [
                e.to_dict() for e in self._workers.values()
                if e.conn_state == WorkerConnState.ONLINE
                and e.contrib == WorkerContribState.AVAILABLE
                and e.free_slots > 0
            ]

    def is_available(self, worker_id: str) -> bool:
        with self._lock:
            e = self._workers.get(worker_id)
            return bool(
                e
                and e.conn_state == WorkerConnState.ONLINE
                and e.contrib == WorkerContribState.AVAILABLE
                and e.free_slots > 0
            )

    def can_accept_work(self, worker_id: str) -> bool:
        """Online AND willing to take work — ignores the *cached* slot count.

        The cached free_slots is only as fresh as the last heartbeat (up to
        HEARTBEAT_INTERVAL stale). A claim request carries the worker's live slot
        count, so that value must be used for the capacity decision; using the
        cached one stalls every worker for up to a full heartbeat interval after
        each finished task.
        """
        with self._lock:
            e = self._workers.get(worker_id)
            return bool(
                e
                and e.conn_state == WorkerConnState.ONLINE
                and e.contrib == WorkerContribState.AVAILABLE
            )

    # ── expiry ───────────────────────────────────────────────────────────────

    def _expire_loop(self) -> None:
        # Worst-case detection latency is WORKER_TIMEOUT + interval. With the
        # 2s timeout that has to stay near the timeout itself, so the scan runs
        # at half the timeout (1s) and a dead node is marked offline within 3s.
        interval = max(0.25, min(WORKER_TIMEOUT / 2.0, 10.0))
        while not self._stop.wait(interval):
            self.scan_expired()

    def scan_expired(self) -> List[str]:
        """Mark stale nodes OFFLINE. Returns the ids that just went offline."""
        now = time.time()
        expired: List[str] = []
        with self._lock:
            for entry in self._workers.values():
                if entry.conn_state != WorkerConnState.ONLINE:
                    continue
                if (now - entry.last_seen) > WORKER_TIMEOUT:
                    entry.conn_state = WorkerConnState.OFFLINE
                    entry.free_slots = 0
                    entry.exec_state = WorkerExecState.IDLE
                    expired.append(entry.capability.worker_id)

        for worker_id in expired:
            if _on_node_event:
                _safe(_on_node_event, worker_id, "timeout")
            if _on_offline:
                _safe(_on_offline, worker_id)
        return expired


def _safe(fn, *args) -> None:
    """Call a hook without letting it break the registry."""
    try:
        fn(*args)
    except Exception:  # pragma: no cover - hooks are advisory
        pass


# Module-level singleton (the composition root may also build its own)
registry = WorkerRegistry()
