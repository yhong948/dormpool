"""
master/app/m6_metrics.py  —  M6 Metrics Collector
===================================================
Owner: Person A.

The one rule that matters (G6-4 / G6-5, risk R7):
    P_pool is measured from the POOL's wall clock, never derived as Σ P_i.

    P_i    = units succeeded on worker i / that worker's busy seconds
    P_pool = units completed by the pool's last job / that job's wall clock
    η_pool = P_pool / Σ P_i × 100 %      (may legitimately exceed 100 when the
                                          per-device baselines were taken
                                          separately and serialised)
    S      = P_pool / max(P_i)

With one worker alive, P_pool ≈ that worker's own rate, so S ≈ 1.0 — which is
exactly the control experiment G6-5 requires. If P_pool were Σ P_i, S would
always equal the node count and the control would fail.

Only AttemptStatus.SUCCEEDED attempts are ever recorded, and each attempt is
recorded at most once.
"""

from __future__ import annotations

import threading
import time
from dataclasses import asdict
from typing import Dict, List, Optional

from shared.contracts import MetricRecord


class MetricsCollector:
    def __init__(self):
        self._records: List[MetricRecord] = []
        self._seen_attempts: set = set()
        self._node_events: List[dict] = []
        self._pool_jobs: Dict[str, dict] = {}    # job_id -> frozen measurement
        self._lock = threading.RLock()

    # ── recording ────────────────────────────────────────────────────────────

    def record_attempt(self, worker_id: str, task_id: str, attempt_id: str,
                       units_done: int, elapsed_s: float,
                       cpu_pct: Optional[float] = None,
                       gpu_pct: Optional[float] = None) -> bool:
        """Record one succeeded attempt. Returns False if already recorded."""
        if units_done <= 0:
            return False
        with self._lock:
            if attempt_id in self._seen_attempts:
                return False
            self._seen_attempts.add(attempt_id)
            self._records.append(
                MetricRecord(
                    worker_id=worker_id,
                    task_id=task_id,
                    attempt_id=attempt_id,
                    units_done=int(units_done),
                    elapsed_s=float(max(0.0, elapsed_s)),
                    cpu_pct=cpu_pct,
                    gpu_pct=gpu_pct,
                )
            )
            return True

    def record_node_event(self, worker_id: str, event: str) -> None:
        """event: join | pause | resume | leave | timeout"""
        with self._lock:
            self._node_events.append(
                {"ts": time.time(), "worker_id": worker_id, "event": event}
            )

    def record_pool_job(self, job_id: str, created_at: float, finished_at: float,
                        units_total: int) -> None:
        """Freeze a completed job's wall-clock measurement for P_pool."""
        if units_total <= 0 or finished_at is None:
            return
        wall = float(finished_at) - float(created_at)
        if wall <= 0:
            return
        with self._lock:
            self._pool_jobs[job_id] = {
                "job_id": job_id,
                "created_at": float(created_at),
                "finished_at": float(finished_at),
                "wall_s": wall,
                "units_total": int(units_total),
                "p_pool": int(units_total) / wall,
            }

    # ── aggregation ──────────────────────────────────────────────────────────

    def per_device_throughput(self) -> Dict[str, float]:
        """P_i = succeeded units / occupied seconds, per worker.

        The denominator is the worker's OCCUPIED SPAN — from its first attempt
        start to its last attempt finish — not the sum of individual execution
        times. Summing only execution time hides the fetch/upload/commit gaps
        that the pool's wall clock does include, which would make P_pool and ΣP_i
        incomparable and η_pool meaningless.

        The span is also floored at the summed execution time, so a worker whose
        records somehow span less than its own work cannot report an absurd rate.
        """
        with self._lock:
            units: Dict[str, float] = {}
            busy: Dict[str, float] = {}
            first_start: Dict[str, float] = {}
            last_finish: Dict[str, float] = {}

            for rec in self._records:
                wid = rec.worker_id
                units[wid] = units.get(wid, 0.0) + rec.units_done
                busy[wid] = busy.get(wid, 0.0) + rec.elapsed_s
                start = rec.ts - rec.elapsed_s
                first_start[wid] = min(first_start.get(wid, start), start)
                last_finish[wid] = max(last_finish.get(wid, rec.ts), rec.ts)

        out: Dict[str, float] = {}
        for wid, total_units in units.items():
            if total_units <= 0:
                continue
            span = max(0.0, last_finish.get(wid, 0.0) - first_start.get(wid, 0.0))
            span = max(span, busy.get(wid, 0.0))
            if span > 0:
                out[wid] = round(total_units / span, 4)
        return out

    def averages(self) -> Dict[str, dict]:
        """Average CPU/GPU utilisation per worker over its succeeded attempts."""
        with self._lock:
            cpu: Dict[str, List[float]] = {}
            gpu: Dict[str, List[float]] = {}
            for rec in self._records:
                if rec.cpu_pct is not None:
                    cpu.setdefault(rec.worker_id, []).append(float(rec.cpu_pct))
                if rec.gpu_pct is not None:
                    gpu.setdefault(rec.worker_id, []).append(float(rec.gpu_pct))
        out: Dict[str, dict] = {}
        for worker_id in set(cpu) | set(gpu):
            out[worker_id] = {
                "avg_cpu_pct": round(sum(cpu.get(worker_id, [0.0])) / len(cpu[worker_id]), 2)
                if cpu.get(worker_id) else None,
                "avg_gpu_pct": round(sum(gpu.get(worker_id, [0.0])) / len(gpu[worker_id]), 2)
                if gpu.get(worker_id) else None,
            }
        return out

    def latest_pool_measurement(self) -> Optional[dict]:
        with self._lock:
            if not self._pool_jobs:
                return None
            latest = max(self._pool_jobs.values(), key=lambda m: m["finished_at"])
            return dict(latest)

    def summary(self) -> dict:
        per_dev = self.per_device_throughput()
        pool = self.latest_pool_measurement()

        p_pool = float(pool["p_pool"]) if pool else 0.0
        p_sum = sum(per_dev.values())
        p_max = max(per_dev.values()) if per_dev else 0.0

        eta = round(p_pool / p_sum * 100.0, 2) if p_sum > 0 else None
        speedup = round(p_pool / p_max, 4) if p_max > 0 else None

        with self._lock:
            events = list(self._node_events[-20:])
            total_records = len(self._records)

        return {
            "per_device_throughput": per_dev,
            "p_pool": round(p_pool, 4),
            "p_pool_source": "pool_wall_clock" if pool else "no_completed_job",
            "pool_measurement": pool,
            "p_sum": round(p_sum, 4),
            "p_max": round(p_max, 4),
            "eta_pool_pct": eta,
            "speedup_S": speedup,
            "node_events": events,
            "total_records": total_records,
            "avg_cpu_pct": {k: v["avg_cpu_pct"] for k, v in self.averages().items()},
            "avg_gpu_pct": {k: v["avg_gpu_pct"] for k, v in self.averages().items()},
        }


metrics = MetricsCollector()
