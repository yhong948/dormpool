"""
executor/run.py  —  C2 Workload Adapter entry point
====================================================
Owner: Person C.

Subprocess contract (frozen — shared/contracts.py and docs/验收门禁.md):
  python executor/run.py <task_type> <input_path> <output_path> [--worker-id X]

Exit codes:  0 = success,  1 = task-level failure,  2 = fatal error
The LAST stdout line must be JSON: {"units_done": <int>, "detail": "..."}

Task detail goes to stderr; stdout carries only the final JSON line, so the
worker's "parse the last stdout line" rule stays unambiguous.
"""

import argparse
import json
import os
import sys
import traceback

# Make the repository root importable when this file is launched as a script
# from an arbitrary working directory (the worker always runs from the repo
# root, but a gate or a human may not).
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from executor.workload.convnext_infer import run as convnext_run  # noqa: E402
from executor.workload.image_thumb import run as image_thumb_run  # noqa: E402
from executor.workload.image_thumb_gpu import (  # noqa: E402
    GpuUnavailable,
    run as image_thumb_gpu_run,
)

# ── Task registry ────────────────────────────────────────────────────────────
# Map task_type string → callable(input_path: str, output_path: str) -> int
#
# image_thumb_gpu is a separate type on purpose: its GPU resize cannot be
# byte-identical to Pillow's LANCZOS, so it must not replace the frozen
# image_thumb implementation (that would break the G1 determinism gate). Both
# emit the same manifest schema and are checked by the same authoritative
# validator; only the pixels differ.

TASK_REGISTRY = {
    "image_thumb": image_thumb_run,
    "image_thumb_gpu": image_thumb_gpu_run,
    "convnext_infer": convnext_run,
}


def _emit(units_done: int, detail: str) -> None:
    """Print the contract line. Only ever called once, as the last stdout write."""
    sys.stdout.write(json.dumps({"units_done": int(units_done), "detail": detail}) + "\n")
    sys.stdout.flush()


def main(argv=None) -> int:
    if (argv if argv is not None else sys.argv[1:]) == ["--serve"]:
        from executor.service import serve
        return serve(TASK_REGISTRY)
    parser = argparse.ArgumentParser(description="DormPool task executor")
    parser.add_argument("task_type")
    parser.add_argument("input_path")
    parser.add_argument("output_path")
    parser.add_argument("--worker-id", default="unknown")
    args = parser.parse_args(argv)

    handler = TASK_REGISTRY.get(args.task_type)
    if handler is None:
        # Unknown type is a task-level failure: the Master may retry elsewhere
        # but should not blame this node.
        _emit(0, f"unknown task_type: {args.task_type}")
        return 1

    try:
        units_done = handler(args.input_path, args.output_path)
    except Exception:
        detail = traceback.format_exc(limit=5)[-800:]
        sys.stderr.write(detail)
        _emit(0, detail.replace("\n", " | "))
        return 1

    _emit(units_done, "ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
