"""
master/main.py  —  Master Server entry point
=============================================
Owner: Person A.

Start on the loopback interface only — this is a trusted-team prototype running
on one host, and exposing it on 0.0.0.0 is both unnecessary and a gate failure:

    python -m uvicorn master.main:app --host 127.0.0.1 --port 8000

Do not pass --reload: it forks a second process and the worker tests then see
two schedulers competing over the same state.
"""

from __future__ import annotations

import os
from pathlib import Path

from fastapi.responses import FileResponse

from master.app.m1_api import app  # noqa: F401 — uvicorn imports this symbol

CONSOLE_PATH = Path(__file__).resolve().parents[1] / "console" / "index.html"


@app.get("/")
def index():
    """Landing page: hand people straight to the console when it exists."""
    if CONSOLE_PATH.is_file():
        return FileResponse(CONSOLE_PATH, media_type="text/html")
    return {
        "service": "DormPool Master",
        "console": "/console",
        "ping": "/ping",
    }


@app.get("/health")
def health():
    """Alias of /ping for simple uptime checks."""
    import time
    return {"ok": True, "ts": time.time()}


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("MASTER_PORT", "8000"))
    uvicorn.run(app, host="127.0.0.1", port=port)
