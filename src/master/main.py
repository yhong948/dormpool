"""
master/main.py  —  Master Server entry point
=============================================
Owner: Person A.

Bind address comes from MASTER_BIND and defaults to loopback:

    # single-host demo / all gates
    MASTER_BIND=127.0.0.1 python -m uvicorn master.main:app --port 8000

    # LAN demo — bind the machine's own LAN address, never a wildcard
    MASTER_BIND=10.4.139.66 python -m uvicorn master.main:app --port 8000

Binding 0.0.0.0 is deliberately not supported: it exposes the service on every
interface (including public ones) and it fails acceptance gate G0-5. Name the
one interface that other team machines should actually reach.

Do not pass --reload: it forks a second process and two schedulers then compete
over the same in-memory state.
"""

from __future__ import annotations

import os
from pathlib import Path

from fastapi.responses import FileResponse

from dormpool.src.master.app.m1_api import app  # noqa: F401 — uvicorn imports this symbol

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


DEFAULT_BIND = "127.0.0.1"


def bind_host() -> str:
    """Resolve the listen address from MASTER_BIND.

    A wildcard is rejected loudly rather than silently honoured, because binding
    every interface is both unnecessary for a team pool and a gate failure.
    """
    host = (os.environ.get("MASTER_BIND") or DEFAULT_BIND).strip()
    if host in ("0.0.0.0", "::", "*"):
        raise SystemExit(
            "MASTER_BIND must be a specific address (e.g. 192.168.1.20), "
            f"not {host!r}: a wildcard bind exposes the pool on every interface."
        )
    return host


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("MASTER_PORT", "8000"))
    uvicorn.run(app, host=bind_host(), port=port)
