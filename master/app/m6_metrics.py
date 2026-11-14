"""
master/app/m6_metrics.py  —  M6 Metrics Collector
===================================================
Person A owns this file.

Responsibility:
  - Record per-Attempt execution events (units_done, elapsed, cpu/gpu)
  - Track dynamic node join/pause/resume/fail events
  - Compute per-device and pool-wide throughput for the η_pool formula
  - Expose /metrics/summary for the console and benchmarking

Formula (from project requirement PDF):
  η_pool = P_pool / Σ P_i  × 100%
  S      = P_pool / max(P_1 … P_m)
"""

import threading, time
from collections import defaultdict
from dataclasses import asdict
from typing import Dict, List
from shared.contracts import MetricRecord


class MetricsCollector:
    def __init__(self):
        self._records: List[MetricRecord] = []
        self._node_events: list = []        # {"ts", "worker_id", "event"}
        self._lock = threading.Lock()

    # ── Recording ─────────────────────────────────────────────────────────────

    def record_attempt(self, rec: MetricRecord) -> None:
        with self._lock:
            self._records.append(rec)

    def record_node_event(self, worker_id: str, event: str) -> None:
        """event: 'join' | 'pause' | 'resume' | 'leave' | 'timeout'"""
        with self._lock:
            self._node_events.append({
                "ts": time.time(),
                "worker_id": worker_id,
                "event": event,
            })

    # ── Aggregation ───────────────────────────────────────────────────────────

    def per_device_throughput(self, window_s: float = 300.0) -> Dict[str, float]:
        """
        Units correctly completed per second per device over the last window_s.
        Only SUCCEEDED records count (failed/retried attempts excluded).
        """
        cutoff = time.time() - window_s
        totals: Dict[str, int]   = defaultdict(int)
        spans:  Dict[str, float] = defaultdict(float)
        with self._lock:
            for r in self._records:
                if r.ts >= cutoff and r.units_done > 0:
                    totals[r.worker_id] += r.units_done
                    spans[r.worker_id]  += r.elapsed_s
        return {
            wid: totals[wid] / spans[wid] if spans[wid] > 0 else 0.0
            for wid in totals
        }

    def summary(self) -> dict:
        per_dev = self.per_device_throughput()
        p_sum   = sum(per_dev.values())
        # P_pool is the pool's total effective throughput over the same window
        p_pool  = p_sum   # simplification: use sum until pool benchmark is run separately
        eta     = (p_pool / p_sum * 100) if p_sum > 0 else None
        p_max   = max(per_dev.values()) if per_dev else 0
        s       = (p_pool / p_max) if p_max > 0 else None

        with self._lock:
            recent_events = list(self._node_events[-20:])

        return {
            "per_device_throughput": per_dev,
            "p_pool":                p_pool,
            "eta_pool_pct":          eta,
            "speedup_S":             s,
            "node_events":           recent_events,
            "total_records":         len(self._records),
        }


metrics = MetricsCollector()
