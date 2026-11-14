"""M6 measurements using independent per-device baselines."""
from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional

from shared.contracts import MetricRecord


class MetricsCollector:
    """Compare an independently measured pool job with matching baselines."""

    def __init__(self, storage_path: Optional[Path] = None):
        self._records: List[MetricRecord] = []
        self._seen_attempts: set = set()
        self._node_events: List[dict] = []
        self._measurements: Dict[str, dict] = {}
        self._storage_path = Path(storage_path) if storage_path else None
        self._lock = threading.RLock()
        self._load()

    def _load(self) -> None:
        if not self._storage_path or not self._storage_path.is_file():
            return
        try:
            data = json.loads(self._storage_path.read_text(encoding="utf-8"))
            self._measurements = {str(x["job_id"]): x for x in data if x.get("job_id")}
        except (OSError, ValueError, TypeError, KeyError):
            self._measurements = {}

    def _save(self) -> None:
        if not self._storage_path:
            return
        self._storage_path.parent.mkdir(parents=True, exist_ok=True)
        temp = self._storage_path.with_suffix(".tmp")
        temp.write_text(json.dumps(list(self._measurements.values()), ensure_ascii=False,
                                   indent=2), encoding="utf-8")
        temp.replace(self._storage_path)

    def record_attempt(self, worker_id: str, task_id: str, attempt_id: str,
                       units_done: int, elapsed_s: float,
                       cpu_pct: Optional[float] = None,
                       gpu_pct: Optional[float] = None) -> bool:
        if units_done <= 0:
            return False
        with self._lock:
            if attempt_id in self._seen_attempts:
                return False
            self._seen_attempts.add(attempt_id)
            self._records.append(MetricRecord(
                worker_id=worker_id, task_id=task_id, attempt_id=attempt_id,
                units_done=int(units_done), elapsed_s=float(max(0.0, elapsed_s)),
                cpu_pct=cpu_pct, gpu_pct=gpu_pct))
            return True

    def record_node_event(self, worker_id: str, event: str) -> None:
        with self._lock:
            self._node_events.append({"ts": time.time(), "worker_id": worker_id, "event": event})

    def record_measurement(self, job_id: str, mode: str, task_type: str,
                           started_at: Optional[float], finished_at: Optional[float],
                           units_total: int, successful_tasks: int,
                           profile: dict, workers: dict, issues: list) -> None:
        if not started_at or not finished_at or units_total <= 0:
            return
        wall = float(finished_at) - float(started_at)
        if wall <= 0:
            return
        item = {
            "job_id": job_id, "mode": mode, "task_type": task_type,
            "created_at": float(started_at), "finished_at": float(finished_at),
            "wall_s": wall, "units_total": int(units_total),
            "successful_tasks": int(successful_tasks), "throughput": int(units_total) / wall,
            "profile": dict(profile or {}), "workers": dict(workers or {}),
            "worker_ids": list(workers or {}), "issues": list(issues or []),
        }
        with self._lock:
            self._measurements[job_id] = item
            self._save()

    def _latest_pool(self, job_id: Optional[str] = None) -> Optional[dict]:
        pools = [x for x in self._measurements.values() if x.get("mode") == "pool"]
        if job_id:
            return next((x for x in pools if x.get("job_id") == job_id), None)
        return max(pools, key=lambda x: x.get("finished_at", 0), default=None)

    def _latest_measurement(self, job_id: Optional[str] = None) -> Optional[dict]:
        if job_id:
            return self._measurements.get(job_id)
        return max(self._measurements.values(), key=lambda x: x.get("finished_at", 0), default=None)

    def _matching_baselines(self, pool: dict) -> tuple[Dict[str, float], List[str]]:
        rates: Dict[str, float] = {}
        issues: List[str] = []
        profile = pool.get("profile") or {}
        for worker_id in pool.get("worker_ids", []):
            candidates = [x for x in self._measurements.values()
                          if x.get("mode") == "baseline"
                          and x.get("task_type") == pool.get("task_type")
                          and worker_id in x.get("worker_ids", [])
                          and x.get("profile") == profile]
            baseline = max(candidates, key=lambda x: x.get("finished_at", 0), default=None)
            if baseline is None:
                issues.append(f"missing_baseline:{worker_id}")
            elif baseline.get("workers", {}).get(worker_id) != pool.get("workers", {}).get(worker_id):
                issues.append(f"configuration_mismatch:{worker_id}")
            elif baseline.get("issues"):
                issues.extend(f"baseline:{worker_id}:{issue}" for issue in baseline["issues"])
            else:
                rates[worker_id] = float(baseline["throughput"])
        return rates, issues

    def summary(self, job_id: Optional[str] = None) -> dict:
        with self._lock:
            run = self._latest_measurement(job_id)
            pool = run if run and run.get("mode") == "pool" else None
            if run and run.get("mode") == "baseline":
                baseline_rates = {wid: float(run["throughput"]) for wid in run.get("worker_ids", [])}
                issues = list(run.get("issues") or [])
                comparable = len(baseline_rates) == 1 and not issues
            else:
                baseline_rates, issues = self._matching_baselines(pool) if pool else ({}, [])
                if pool:
                    issues = list(dict.fromkeys((pool.get("issues") or []) + issues))
                comparable = bool(pool and baseline_rates and not issues
                                  and set(baseline_rates) == set(pool.get("worker_ids", [])))
            p_pool = float(run["throughput"]) if run else 0.0
            p_sum = sum(baseline_rates.values())
            p_max = max(baseline_rates.values(), default=0.0)
            events = list(self._node_events[-20:])
            total_records = len(self._records)
            measurements = list(self._measurements.values())
        return {
            "per_device_throughput": {k: round(v, 4) for k, v in baseline_rates.items()},
            "per_device_source": "independent_baseline" if comparable else "missing_or_incompatible_baseline",
            "p_pool": round(p_pool, 4),
            "p_pool_source": ("pool_wall_clock" if pool else
                              "baseline_wall_clock" if run else "no_completed_measurement"),
            "pool_measurement": dict(run) if run else None,
            "baseline_comparable": comparable, "baseline_issues": issues,
            "p_sum": round(p_sum, 4) if comparable else None,
            "p_max": round(p_max, 4) if comparable else None,
            "eta_pool_pct": round(p_pool / p_sum * 100.0, 2) if comparable and p_sum else None,
            "speedup_S": round(p_pool / p_max, 4) if comparable and p_max else None,
            "node_events": events, "total_records": total_records,
            "measurements": measurements,
        }


metrics = MetricsCollector()
