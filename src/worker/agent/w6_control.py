"""
worker/agent/w6_control.py  —  W6 Local Control Interface
==========================================================
Owner: Person B.

Three ways the device owner can look after their own machine, all of which end
up calling the same W4 methods:

  1. REPL          interactive terminal (`status`, `pause`, `resume`, `quit`)
  2. stdin reader  line commands fed on stdin — what the gates drive with
                   `echo pause > /tmp/worker-A.stdin`
  3. local HTTP    127.0.0.1 only, opt-in via WORKER_CONTROL_PORT
                   GET /status, POST /pause, POST /resume

Nothing here ever binds a non-loopback address: the owner's control channel is
not part of the team network.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Optional

log = logging.getLogger("w6")

STATUS_PAUSED = "paused"


class LocalControl:
    def __init__(self, agent):
        self.agent = agent
        self._server: Optional[HTTPServer] = None
        self._threads = []

    # ── startup ──────────────────────────────────────────────────────────────

    def start(self) -> None:
        port = int(getattr(self.agent.config, "control_port", 0) or 0)
        if port > 0:
            self._start_http(port)

        no_repl = bool(int(os.environ.get("WORKER_NO_REPL", "0") or 0))
        if not no_repl and sys.stdin is not None and sys.stdin.isatty():
            thread = threading.Thread(target=self._repl, name="repl", daemon=True)
            thread.start()
            self._threads.append(thread)
        elif not no_repl:
            # Non-interactive stdin (pipe / file): accept line commands.
            thread = threading.Thread(target=self._stdin_reader, name="stdin", daemon=True)
            thread.start()
            self._threads.append(thread)

    # ── command handling ─────────────────────────────────────────────────────

    def handle(self, command: str) -> dict:
        command = (command or "").strip().lower()
        if command == "pause":
            self.agent.pause()
            return {"ok": True, "command": "pause", "status": self.status()}
        if command == "resume":
            self.agent.resume()
            return {"ok": True, "command": "resume", "status": self.status()}
        if command == "status":
            return {"ok": True, "command": "status", "status": self.status()}
        if command == "quit":
            self.agent.shutdown()
            return {"ok": True, "command": "quit"}
        return {"ok": False, "error": f"unknown command: {command!r}"}

    def status(self) -> dict:
        monitor = self.agent.monitor
        contrib = self.agent.contrib
        executor = self.agent.exec_mgr
        return {
            "worker_id": self.agent.config.worker_id,
            "running": self.agent.is_running(),
            "contrib_state": contrib.state().value if hasattr(contrib.state(), "value")
                             else str(contrib.state()),
            "busy_slots": monitor.busy_slots() if monitor else 0,
            "free_slots": monitor.free_slots() if monitor else 0,
            "cpu_pct": monitor.cpu_pct() if monitor else None,
            "gpu_pct": monitor.gpu_pct() if monitor else None,
            "running_tasks": executor.running_task_ids() if executor else [],
            "executor": executor.status() if executor else None,
        }

    # ── REPL / stdin ─────────────────────────────────────────────────────────

    def _repl(self) -> None:
        print("\nDormPool worker — local control")
        print("commands: status | pause | resume | quit\n")
        while self.agent.is_running():
            try:
                line = input("dormpool> ")
            except (EOFError, KeyboardInterrupt):
                self.agent.shutdown()
                break
            result = self.handle(line)
            if result["ok"]:
                print(json.dumps(result.get("status", result), indent=2, ensure_ascii=False))
            else:
                print(result["error"])

    def _stdin_reader(self) -> None:
        """Read line commands until EOF or shutdown (gate-driven mode)."""
        stdin = sys.stdin
        if stdin is None:
            return
        while self.agent.is_running():
            try:
                line = stdin.readline()
            except Exception:
                return
            if not line:
                return
            command = line.strip()
            if command:
                log.info("control command: %s", command)
                self.handle(command)

    # ── localhost HTTP ───────────────────────────────────────────────────────

    def _start_http(self, port: int) -> None:
        control = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):        # keep the log readable
                return

            def _send(self, code: int, payload: dict) -> None:
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(code)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):                    # noqa: N802
                if self.path.split("?")[0] != "/status":
                    self._send(404, {"error": "not found"})
                    return
                self._send(200, control.status())

            def do_POST(self):                   # noqa: N802
                path = self.path.split("?")[0]
                if path == "/pause":
                    self._send(200, control.handle("pause"))
                elif path == "/resume":
                    self._send(200, control.handle("resume"))
                elif path == "/quit":
                    self._send(200, control.handle("quit"))
                else:
                    self._send(404, {"error": "not found"})

        try:
            server = HTTPServer(("127.0.0.1", port), Handler)
        except OSError as exc:
            log.warning("control port %s unavailable: %s", port, exc)
            return

        self._server = server
        thread = threading.Thread(target=server.serve_forever,
                                  name="control-http", daemon=True)
        thread.start()
        self._threads.append(thread)
        log.info("local control listening on http://127.0.0.1:%s", port)

    def stop(self) -> None:
        if self._server is not None:
            try:
                self._server.shutdown()
            except Exception:
                pass
