"""
worker/agent/w4_contrib.py  —  W4 Contribution Controller
==========================================================
Owner: Person B.

Decides whether this machine may accept another task. Three inputs:
  * the owner's manual pause/resume (always wins)
  * free execution slots
  * an optional CPU ceiling (disabled unless configured)

Graceful pause is the contract: pausing stops NEW claims immediately, but a task
already running is allowed to finish. That is why state() distinguishes
DRAINING (paused, still busy) from PAUSED (paused, idle) — the gates check the
order draining -> paused and reject a direct jump to paused.
"""

from __future__ import annotations

import threading
from typing import Optional

from shared.contracts import WorkerContribState

# Automatic admission (P1). Set both to enable; leave None to disable.
CPU_PAUSE_THRESHOLD: Optional[float] = None
CPU_RESUME_THRESHOLD: Optional[float] = None
CONSECUTIVE_SAMPLES = 3          # anti-flapping: require N samples in a row


class ContributionController:
    def __init__(self, monitor):
        self.monitor = monitor
        self._manual_paused = False
        self._auto_paused = False
        self._high_streak = 0
        self._low_streak = 0
        self._lock = threading.RLock()

    # ── state ────────────────────────────────────────────────────────────────

    def state(self) -> WorkerContribState:
        with self._lock:
            paused = self._manual_paused or self._auto_paused
            if paused:
                # Still finishing in-flight work -> DRAINING, not PAUSED.
                if self.monitor.busy_slots() > 0:
                    return WorkerContribState.DRAINING
                return WorkerContribState.PAUSED
        return WorkerContribState.AVAILABLE

    def can_accept(self) -> bool:
        """True when the agent may request another task right now."""
        with self._lock:
            if self._manual_paused:
                return False
            if self.monitor.free_slots() < 1:
                return False
            automated = self._auto_gate_allows()
            if not automated:
                return False
        return True

    def _auto_gate_allows(self) -> bool:
        """CPU-threshold admission with anti-flapping. No-op when disabled."""
        if CPU_PAUSE_THRESHOLD is None or CPU_RESUME_THRESHOLD is None:
            return True
        cpu = self.monitor.cpu_pct()
        if cpu is None:
            return True

        if cpu >= CPU_PAUSE_THRESHOLD:
            self._high_streak += 1
            self._low_streak = 0
            if self._high_streak >= CONSECUTIVE_SAMPLES:
                self._auto_paused = True
        elif cpu <= CPU_RESUME_THRESHOLD:
            self._low_streak += 1
            self._high_streak = 0
            if self._low_streak >= CONSECUTIVE_SAMPLES:
                self._auto_paused = False
        else:
            self._high_streak = 0
            self._low_streak = 0

        return not self._auto_paused

    # ── owner control ────────────────────────────────────────────────────────

    def pause(self) -> None:
        with self._lock:
            self._manual_paused = True

    def resume(self) -> None:
        with self._lock:
            self._manual_paused = False
            self._auto_paused = False
            self._high_streak = 0
            self._low_streak = 0

    def is_manually_paused(self) -> bool:
        with self._lock:
            return self._manual_paused
