"""Own one local Executor process. No torch/CuPy imports in the Agent."""
from collections import deque
import json
import logging
import os
from pathlib import Path
import queue
import subprocess
import sys
import threading
import uuid
import psutil

log = logging.getLogger("executor")
ROOT = Path(__file__).resolve().parents[2]


class ExecutorFailure(RuntimeError):
    pass


class ResidentExecutor:
    def __init__(self, command=None, startup_timeout=180.0):
        self.command = command or [sys.executable, str(ROOT / "executor/run.py"), "--serve"]
        self.startup_timeout = startup_timeout
        self._rpc = threading.RLock()
        self._state = threading.RLock()
        self._proc = None
        self._ready = None
        self._readers = []
        self._stopping = False
        self._generation = 0
        self._tail = deque(maxlen=20)

    def status(self):
        with self._state:
            proc = self._proc
            return {"pid": self._ready["pid"] if proc and proc.poll() is None and self._ready else None,
                    "launcher_pid": proc.pid if proc and proc.poll() is None else None,
                    "generation": self._generation, "stopping": self._stopping,
                    "ready": self._ready}

    def ensure_ready(self):
        with self._rpc:
            with self._state:
                if self._stopping:
                    raise ExecutorFailure("executor is closing")
                if self._proc and self._proc.poll() is None and self._ready:
                    return self._ready
            self.abort()
            messages = queue.Queue()
            environment = dict(os.environ, PYTHONUNBUFFERED="1")
            with self._state:
                if self._stopping:
                    raise ExecutorFailure("executor is closing")
                proc = subprocess.Popen(self.command, cwd=ROOT, env=environment,
                                        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, text=True, encoding="utf-8",
                                        errors="replace", bufsize=1,
                                        creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
                                        start_new_session=os.name != "nt")
                self._proc = proc
                self._generation += 1
                self._messages = messages
                self._tail.clear()
            def stdout():
                try:
                    for line in proc.stdout:
                        try:
                            messages.put(json.loads(line))
                        except ValueError:
                            messages.put(ExecutorFailure("invalid JSON from executor"))
                            return
                finally:
                    messages.put(ExecutorFailure("executor exited / protocol pipe closed"))
            def stderr():
                for line in proc.stderr:
                    self._tail.append(line.strip()[:2000])
                    log.info("pid=%s %s", proc.pid, line.rstrip())
            with self._state:
                if self._stopping or self._proc is not proc:
                    raise ExecutorFailure("executor startup cancelled")
                self._readers = [threading.Thread(target=stdout, name="executor-stdout", daemon=True),
                                 threading.Thread(target=stderr, name="executor-stderr", daemon=True)]
                for thread in self._readers:
                    thread.start()
            try:
                ready = self._receive(self.startup_timeout)
                if (not isinstance(ready, dict) or ready.get("type") != "ready"
                        or ready.get("protocol") != 1 or not isinstance(ready.get("pid"), int)
                        or not isinstance(ready.get("supported_task_types"), list)):
                    raise ExecutorFailure("invalid ready response")
                with self._state:
                    if self._stopping or self._proc is not proc:
                        raise ExecutorFailure("executor startup cancelled")
                    self._ready = ready
                log.info("ready pid=%s generation=%s supported=%s initialization_s=%s",
                         ready["pid"], self._generation, ready["supported_task_types"], ready.get("initialization_s"))
                return ready
            except Exception:
                self.abort()
                raise

    def _receive(self, timeout):
        try:
            message = self._messages.get(timeout=max(0.001, timeout))
        except queue.Empty:
            raise ExecutorFailure("executor response timeout") from None
        if isinstance(message, Exception):
            raise ExecutorFailure(f"{message}; stderr: {' | '.join(self._tail)}")
        return message

    def execute(self, task, attempt, input_path, output_path, timeout):
        with self._rpc:
            self.ensure_ready()
            identity = {"request_id": uuid.uuid4().hex, "task_id": task["task_id"],
                        "attempt_id": attempt["attempt_id"]}
            request = {"type": "execute", **identity,
                       "task_type": task.get("task_type") or "convnext_infer",
                       "input_path": str(Path(input_path).resolve()),
                       "output_path": str(Path(output_path).resolve())}
            # Remove stale partial output before dispatch; never accept it after failure.
            Path(output_path).unlink(missing_ok=True)
            try:
                proc = self._proc
                ready_pid = self._ready["pid"]
                log.info("request pid=%s task=%s attempt=%s", ready_pid, identity["task_id"], identity["attempt_id"])
                proc.stdin.write(json.dumps(request) + "\n")
                proc.stdin.flush()
                result = self._receive(timeout)
                if (not isinstance(result, dict) or result.get("type") != "result"
                        or any(result.get(key) != value for key, value in identity.items())
                        or result.get("pid") != ready_pid or not isinstance(result.get("ok"), bool)):
                    raise ExecutorFailure("executor response identity/protocol mismatch")
                if result.get("fatal"):
                    self.abort()
                return result
            except Exception as exc:
                self.abort()
                raise ExecutorFailure(str(exc)) from exc

    def abort(self):
        """Interrupt even an in-flight RPC; its reader will observe EOF."""
        with self._state:
            proc, self._proc = self._proc, None
            self._ready = None
        if proc is None:
            return
        # Windows venv launchers can spawn the actual Python as a child. Kill
        # the entire owned process tree, not only the launcher PID.
        try:
            children = psutil.Process(proc.pid).children(recursive=True)
        except psutil.NoSuchProcess:
            children = []
        for child in reversed(children):
            try:
                child.kill()
            except psutil.NoSuchProcess:
                pass
        if proc.poll() is None:
            proc.kill()
        proc.wait(timeout=10)
        psutil.wait_procs(children, timeout=5)
        for thread in self._readers:
            if thread is not threading.current_thread():
                thread.join(timeout=2)
        for stream in (proc.stdin, proc.stdout, proc.stderr):
            if stream:
                stream.close()

    def close(self):
        with self._state:
            self._stopping = True
        # Graceful shutdown only when no execute() is in flight.
        if self._rpc.acquire(blocking=False):
            try:
                proc = self._proc
                if proc and proc.poll() is None:
                    try:
                        proc.stdin.write('{"type":"shutdown"}\n')
                        proc.stdin.flush()
                        proc.wait(timeout=5)
                    except (OSError, subprocess.TimeoutExpired):
                        pass
            finally:
                self.abort()
                self._rpc.release()
        else:
            self.abort()
