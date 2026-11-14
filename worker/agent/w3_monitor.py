"""
worker/agent/w3_monitor.py  —  W3 Resource Monitor
====================================================
Person B owns this file.

Responsibility:
  - Track free/busy execution slots
  - Sample CPU and GPU utilisation
  - Provide instantaneous snapshot for W4 and heartbeat
"""

import threading
from typing import Optional

try:
    import psutil
    _PSUTIL = True
except ImportError:
    _PSUTIL = False


class ResourceMonitor:
    def __init__(self, max_slots: int):
        self._max    = max_slots
        self._active = 0         # tasks currently running
        self._lock   = threading.Lock()

    # ── Slot accounting (called by W5) ────────────────────────────────────────

    def slot_acquire(self):
        with self._lock:
            self._active += 1

    def slot_release(self):
        with self._lock:
            self._active = max(0, self._active - 1)

    def free_slots(self) -> int:
        with self._lock:
            return max(0, self._max - self._active)

    def busy_slots(self) -> int:
        with self._lock:
            return self._active

    # ── Resource sampling ─────────────────────────────────────────────────────

    def cpu_pct(self) -> Optional[float]:
        if not _PSUTIL:
            return None
        try:
            return psutil.cpu_percent(interval=0.5)
        except Exception:
            return None

    def gpu_pct(self) -> Optional[float]:
        """
        Return GPU utilisation if available.
        Person B: implement via pynvml or torch.cuda.utilization().
        """
        # TODO (Person B): pynvml.nvmlDeviceGetUtilizationRates()
        return None
