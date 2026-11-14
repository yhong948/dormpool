"""
executor/benchmark.py  —  single-node baseline (P_i)
====================================================
Owner: Person C.

Runs the workload N times on one machine and prints the throughput that the
pool's aggregate efficiency is computed against:

  python executor/benchmark.py --task-type image_thumb --input sample.zip --repeat 3

Output is one JSON object on stdout:
  {"task_type","total_tasks","units_done","elapsed_s","throughput", ...}

`total_tasks` counts the shard executions (`--repeat`); `units_done` is the sum
of the images processed, so `units_done == total_tasks * <images per shard>`.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import time
import zipfile

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from executor.run import TASK_REGISTRY  # noqa: E402
from executor.workload.image_thumb import is_image_member  # noqa: E402


def run_benchmark(task_type: str, input_path: str, repeat: int) -> dict:
    handler = TASK_REGISTRY.get(task_type)
    if handler is None:
        raise SystemExit(f"unknown task_type: {task_type}")

    if not zipfile.is_zipfile(input_path):
        raise SystemExit(f"input is not a zip archive: {input_path}")

    with zipfile.ZipFile(input_path) as zf:
        images_per_shard = sum(1 for n in zf.namelist() if is_image_member(n))
    if images_per_shard == 0:
        raise SystemExit(f"no images in {input_path}")

    total_units = 0
    per_run = []
    with tempfile.TemporaryDirectory(prefix="dormpool-bench-") as tmp:
        for i in range(repeat):
            out_path = os.path.join(tmp, f"out_{i}.zip")
            start = time.perf_counter()
            units = handler(input_path, out_path)
            elapsed = time.perf_counter() - start
            total_units += int(units)
            per_run.append({"run": i, "units_done": int(units), "elapsed_s": round(elapsed, 4)})

    wall = sum(r["elapsed_s"] for r in per_run)
    throughput = (total_units / wall) if wall > 0 else 0.0

    return {
        "task_type": task_type,
        "total_tasks": repeat,
        "units_done": total_units,
        "images_per_shard": images_per_shard,
        "elapsed_s": round(wall, 4),
        "throughput": round(throughput, 4),
        "per_run": per_run,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Single-node throughput baseline")
    parser.add_argument("--task-type", default="image_thumb")
    parser.add_argument("--input", required=True, help="sample zip of images")
    parser.add_argument("--repeat", type=int, default=3, help="shard executions")
    args = parser.parse_args(argv)

    if args.repeat < 1:
        parser.error("--repeat must be >= 1")

    result = run_benchmark(args.task_type, args.input, args.repeat)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
