"""
worker/agent/w4_contrib.py  —  W4 Contribution Controller
==========================================================
Person B owns this file.

Responsibility:
  - Honour manual pause / resume from W6 Local Control
  - Enforce max-slot concurrency limit (via W3 ResourceMonitor)
  - Optionally gate task acceptance on CPU/GPU load thresholds
  - Prevent flapping: require load to stay low for N consecutive checks
"""

import threading
from shared.contracts import WorkerContribState


# Load thresholds (P1 feature — set to None to disable automatic gate)
CPU_PAUSE_THRESHOLD  = None   # e.g. 85.0  — pause if CPU exceeds this %
CPU_RESUME_THRESHOLD = None   # e.g. 60.0  — resume if CPU drops below this %


class ContributionController:
    def __init__(self, monitor):
        self.monitor = monitor
        self._manual_paused = False
        self._lock = threading.Lock()

    # ── State ─────────────────────────────────────────────────────────────────

    def state(self) -> WorkerContribState:
        with self._lock:
            if self._manual_paused:
                if self.monitor.busy_slots() > 0:
                    return WorkerContribState.DRAINING
                return WorkerContribState.PAUSED
        return WorkerContribState.AVAILABLE

    def can_accept(self) -> bool:
        """True when the agent should request a new task right now."""
        with self._lock:
            if self._manual_paused:
                return False
        if self.monitor.free_slots() < 1:
            return False
        # Optional CPU load gate (P1)
        if CPU_PAUSE_THRESHOLD is not None:
            cpu = self.monitor.cpu_pct()
            if cpu is not None and cpu >= CPU_PAUSE_THRESHOLD:
                return False
        return True

    # ── Manual control (called by W6) ─────────────────────────────────────────

    def pause(self):
        with self._lock:
            self._manual_paused = True

    def resume(self):
        with self._lock:
            self._manual_paused = False
