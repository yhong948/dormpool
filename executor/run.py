"""
executor/run.py  —  C2 Workload Adapter entry point
====================================================
Person C owns this file and executor/workload/.

Subprocess contract (from shared/contracts.py):
  python executor/run.py <task_type> <input_path> <output_path> [--worker-id X]

Exit codes:  0 = success,  1 = task failure,  2 = fatal error
Last stdout line must be JSON: {"units_done": <int>, "detail": "..."}

Steps to implement (Person C):
  1. Register supported task types in TASK_REGISTRY below.
  2. Implement each handler in executor/workload/<type>.py.
  3. Each handler receives (input_path, output_path) and returns units_done.
"""

import sys, json, argparse, traceback

# ── Task registry ──────────────────────────────────────────────────────────────
# Map task_type string → callable(input_path: str, output_path: str) -> int
#
# Example entry once workload is decided:
#   from executor.workload.image_resize import run as image_resize_run
#   TASK_REGISTRY = {"image_resize": image_resize_run}

TASK_REGISTRY: dict = {}   # TODO (Person C): populate with real workload handler(s)


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("task_type")
    parser.add_argument("input_path")
    parser.add_argument("output_path")
    parser.add_argument("--worker-id", default="unknown")
    args = parser.parse_args()

    handler = TASK_REGISTRY.get(args.task_type)
    if handler is None:
        print(json.dumps({"units_done": 0, "detail": f"unknown task_type: {args.task_type}"}))
        sys.exit(1)

    try:
        units_done = handler(args.input_path, args.output_path)
        print(json.dumps({"units_done": units_done, "detail": "ok"}))
        sys.exit(0)
    except Exception as exc:
        print(json.dumps({"units_done": 0, "detail": traceback.format_exc(limit=5)}))
        sys.exit(1)


if __name__ == "__main__":
    main()
