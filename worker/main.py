"""
worker/main.py  —  Worker Agent entry point
===========================================
Owner: Person B.

    cd f:/dormpool/dormpool
    MASTER_URL=http://127.0.0.1:8000 DORMPOOL_TOKEN=dev-token \
    WORKER_ID=worker-A WORKER_DATA=data/worker_data/worker-A MAX_SLOTS=1 \
    python worker/main.py --headless

Launching a file inside a package puts that directory on sys.path[0], not the
repository root — so `import worker.agent...` would fail. Insert the repo root
explicitly; this keeps `python worker/main.py` working from any cwd.
"""

from __future__ import annotations

import os
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from worker.agent.w1_runtime import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
