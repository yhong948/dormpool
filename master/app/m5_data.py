"""
master/app/m5_data.py  —  M5 Data & Result Manager
====================================================
Person A owns this file.

Responsibility:
  - Save uploaded job input files, return a ref key
  - Serve input shards to Workers on demand
  - Accept and store result uploads from Workers
  - Aggregate per-Task outputs into a final Job output
"""

import os, shutil, threading
from pathlib import Path
from typing import Optional

DATA_ROOT = Path(os.environ.get("DORMPOOL_DATA", "data"))
INPUTS_DIR  = DATA_ROOT / "inputs"
OUTPUTS_DIR = DATA_ROOT / "outputs"

for _d in (INPUTS_DIR, OUTPUTS_DIR):
    _d.mkdir(parents=True, exist_ok=True)


class DataManager:
    def __init__(self):
        self._lock = threading.Lock()

    def save_input(self, job_id: str, filename: str, data: bytes) -> str:
        """Persist uploaded job input. Returns ref key."""
        dest = INPUTS_DIR / job_id
        dest.mkdir(parents=True, exist_ok=True)
        path = dest / filename
        path.write_bytes(data)
        return f"inputs/{job_id}/{filename}"

    def resolve_path(self, ref: str) -> Optional[Path]:
        """Return absolute path for a ref key, or None if not found."""
        path = DATA_ROOT / ref
        return path if path.exists() else None

    def save_task_output(self, job_id: str, task_id: str, data: bytes) -> str:
        """Store a task result. Returns ref key."""
        dest = OUTPUTS_DIR / job_id
        dest.mkdir(parents=True, exist_ok=True)
        path = dest / f"{task_id}.out"
        path.write_bytes(data)
        return f"outputs/{job_id}/{task_id}.out"

    def aggregate_job_output(self, job_id: str, task_ids: list[str]) -> str:
        """
        Concatenate task outputs into a single job result file.
        Person A: replace with format-aware aggregation once workload is defined.
        """
        dest_dir = OUTPUTS_DIR / job_id
        dest_dir.mkdir(parents=True, exist_ok=True)
        out_path = dest_dir / "result.out"
        with open(out_path, "wb") as out_f:
            for task_id in task_ids:
                shard = dest_dir / f"{task_id}.out"
                if shard.exists():
                    out_f.write(shard.read_bytes())
        return f"outputs/{job_id}/result.out"

    def get_job_output_path(self, job_id: str) -> Optional[Path]:
        path = OUTPUTS_DIR / job_id / "result.out"
        return path if path.exists() else None


data_manager = DataManager()
