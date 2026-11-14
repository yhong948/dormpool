"""
tests/g5_dynamic.py  —  dynamic autonomy checks (G5)
====================================================
Proves the pool survives the owner changing their mind and a machine vanishing:

  G5-A  pause  -> contrib_state goes draining (while busy) then paused,
                  the in-flight task still finishes, no new claims
  G5-B  resume -> contrib_state returns to available, work advances again
  G5-C  hot-join -> a worker started mid-job receives work
  G5-D  kill   -> the killed worker's task is reassigned and completed by the
                  other worker, and total units are NOT double counted

Everything is driven over HTTP on 127.0.0.1. The worker's control surface is the
localhost HTTP endpoint (WORKER_CONTROL_PORT), so no terminal plumbing is needed.

    python tests/g5_dynamic.py --port 8200
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

TOKEN = os.environ.get("DORMPOOL_TOKEN", "dev-token")
HEADERS = {"Authorization": f"Bearer {TOKEN}"}


def call(url, method="GET", body=None, headers=None, timeout=15.0):
    req = urllib.request.Request(url, data=body, method=method)
    for k, v in (headers or HEADERS).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            return json.loads(raw.decode()) if raw else None
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"{method} {url} -> {exc.code}: {exc.read()[:200]}")


def multipart(fields, file_field, filename, blob):
    boundary = "----g5" + os.urandom(6).hex()
    out = bytearray()
    for name, value in fields.items():
        out += f"--{boundary}\r\n".encode()
        out += f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode()
        out += f"{value}\r\n".encode()
    out += f"--{boundary}\r\n".encode()
    out += (f'Content-Disposition: form-data; name="{file_field}"; '
            f'filename="{filename}"\r\n').encode()
    out += b"Content-Type: application/zip\r\n\r\n" + blob
    out += f"\r\n--{boundary}--\r\n".encode()
    headers = dict(HEADERS)
    headers["Content-Type"] = f"multipart/form-data; boundary={boundary}"
    return bytes(out), headers


def wait_port(port, timeout=40):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(0.4)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(0.4)
    return False


class Env:
    def __init__(self, port, work_dir, images, delay_ms):
        self.port, self.work_dir = port, work_dir
        self.images, self.delay_ms = images, delay_ms
        self.base = f"http://127.0.0.1:{port}"
        self.procs = {}
        self.data = work_dir / "data"

    def start_master(self):
        env = dict(os.environ, DORMPOOL_TOKEN=TOKEN,
                   DORMPOOL_DATA=str(self.data), PYTHONUNBUFFERED="1",
                   SHARD_SIZE="1")
        log = open(self.work_dir / "master.log", "wb")
        self.procs["master"] = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "master.main:app",
             "--host", "127.0.0.1", "--port", str(self.port)],
            cwd=str(REPO_ROOT), env=env, stdout=log, stderr=subprocess.STDOUT)
        if not wait_port(self.port):
            raise RuntimeError("master failed to start")

    def start_worker(self, name, control_port=None):
        env = dict(os.environ, MASTER_URL=self.base, DORMPOOL_TOKEN=TOKEN,
                   WORKER_ID=name, WORKER_DATA=str(self.work_dir / name),
                   MAX_SLOTS="1", WORKER_NO_REPL="1", PYTHONUNBUFFERED="1",
                   DORMPOOL_EXECUTOR_DELAY_MS=str(self.delay_ms))
        if control_port:
            env["WORKER_CONTROL_PORT"] = str(control_port)
        log = open(self.work_dir / f"{name}.log", "ab")
        proc = subprocess.Popen([sys.executable, "worker/main.py", "--headless"],
                                cwd=str(REPO_ROOT), env=env, stdout=log,
                                stderr=subprocess.STDOUT)
        self.procs[name] = proc
        return proc

    def workers(self):
        return call(f"{self.base}/workers")["workers"]

    def worker(self, name):
        for w in self.workers():
            if w["worker_id"] == name:
                return w
        return None

    def submit(self, zip_path):
        blob = zip_path.read_bytes()
        body, headers = multipart({"task_type": "image_thumb"}, "file", zip_path.name, blob)
        return call(f"{self.base}/jobs", "POST", body, headers)["job_id"]

    def job(self, job_id):
        return call(f"{self.base}/jobs/{job_id}")

    def wait_status(self, job_id, statuses, timeout=180):
        end = time.time() + timeout
        while time.time() < end:
            job = self.job(job_id)
            if job["status"] in statuses:
                return job
            time.sleep(1)
        raise RuntimeError(f"job did not reach {statuses}")

    def wait_busy(self, name, timeout=30):
        """Wait until the registry reports the node as busy."""
        end = time.time() + timeout
        while time.time() < end:
            w = self.worker(name)
            if w and w["exec_state"] == "busy":
                return w
            time.sleep(0.5)
        raise RuntimeError(f"{name} never became busy")

    def wait_open_attempt(self, name, job_id, timeout=45):
        """Wait until this worker currently holds an attempt on the given job.

        exec_state only refreshes on heartbeat (every HEARTBEAT_INTERVAL), while a
        single-shard task may run for well under a second — so sampling the
        registry can miss the busy window entirely. The job payload is
        authoritative and updates on every claim, so poll that instead.
        """
        end = time.time() + timeout
        while time.time() < end:
            job = self.job(job_id)
            for task in job.get("tasks", []):
                for att in task.get("attempts", []):
                    if att.get("worker_id") == name and att.get("status") == "running":
                        return att
            if job["status"] in ("completed", "failed"):
                raise RuntimeError(f"job finished before {name} held an attempt")
            time.sleep(0.3)
        raise RuntimeError(f"{name} never held an open attempt on {job_id}")

    def wait_online(self, names, timeout=45):
        end = time.time() + timeout
        while time.time() < end:
            online = {w["worker_id"] for w in self.workers() if w["conn_state"] == "online"}
            if names.issubset(online):
                return True
            time.sleep(1)
        raise RuntimeError(f"workers {names} did not come online")

    def stop_all(self):
        for name, proc in self.procs.items():
            if proc.poll() is None:
                proc.terminate()
        for name, proc in self.procs.items():
            try:
                proc.wait(timeout=10)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass


def units_succeeded(job):
    total = 0
    for task in job.get("tasks", []):
        for att in task.get("attempts", []):
            if att["status"] == "succeeded":
                total += int(att.get("units_done") or 0)
    return total


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8200)
    ap.add_argument("--images", type=int, default=12)
    ap.add_argument("--delay-ms", type=int, default=800)
    ap.add_argument("--work-dir", default=None)
    args = ap.parse_args(argv)

    work = Path(args.work_dir) if args.work_dir else Path(tempfile.mkdtemp(prefix="dormpool-g5-"))
    work.mkdir(parents=True, exist_ok=True)
    env = Env(args.port, work, args.images, args.delay_ms)
    ev = work / "evidence"
    ev.mkdir(exist_ok=True)

    from executor.gen_sample import build
    input_zip = work / "input.zip"
    build(args.images, str(input_zip))

    results = {}
    try:
        print(f"work dir {work}")
        env.start_master()
        print("[setup] master up")

        env.start_worker("worker-A", control_port=args.port + 1)
        env.wait_online({"worker-A"})
        print("[setup] worker-A online")

        job_id = env.submit(input_zip)
        print(f"[setup] job {job_id} submitted")

        # ── G5-A pause ────────────────────────────────────────────────────────
        print("\n[G5-A] pause")
        env.wait_busy("worker-A")
        before = env.job(job_id)
        print(f"    busy at done={before['done_tasks']}")

        call(f"http://127.0.0.1:{args.port + 1}/pause", "POST")
        states = []
        seen_draining = False
        for _ in range(25):
            w = env.worker("worker-A")
            states.append((w["contrib_state"], w["exec_state"], w["free_slots"]))
            if w["contrib_state"] == "draining":
                seen_draining = True
            if w["contrib_state"] == "paused":
                break
            time.sleep(1)
        (ev / "01-pause.txt").write_text(
            "\n".join(f"{a} {b} {c}" for a, b, c in states), encoding="utf-8")
        print("    trace:", states[:8])
        assert seen_draining, "never observed draining"
        assert states[-1][0] == "paused", f"never reached paused: {states[-1]}"
        assert states[-1][2] == 0, f"free_slots not 0 when paused: {states[-1]}"
        after_pause = env.job(job_id)
        assert after_pause["done_tasks"] - before["done_tasks"] <= 1, \
            "more than the in-flight task completed after pause"
        results["G5-A"] = f"draining->paused, done {before['done_tasks']}->{after_pause['done_tasks']}"

        # ── G5-B resume ───────────────────────────────────────────────────────
        print("\n[G5-B] resume")
        resumed_at = env.job(job_id)["done_tasks"]
        call(f"http://127.0.0.1:{args.port + 1}/resume", "POST")
        time.sleep(2)
        w = env.worker("worker-A")
        assert w["contrib_state"] == "available", w["contrib_state"]
        end = time.time() + 40
        grew = False
        while time.time() < end:
            if env.job(job_id)["done_tasks"] > resumed_at:
                grew = True
                break
            time.sleep(1)
        assert grew, "no progress after resume"
        results["G5-B"] = f"available again, done advanced from {resumed_at}"

        # ── G5-C hot join ─────────────────────────────────────────────────────
        print("\n[G5-C] hot join")
        current = env.job(job_id)
        if current["status"] == "completed":
            print("    job already finished — starting a second job for the hot-join test")
            job_id = env.submit(input_zip)
            env.wait_busy("worker-A")

        before_join = env.job(job_id)
        assert before_join["done_tasks"] < before_join["total_tasks"], \
            "need remaining work for a meaningful hot-join test"
        env.start_worker("worker-B")
        env.wait_online({"worker-B"})
        print(f"    worker-B joined at done={before_join['done_tasks']}")
        final = env.wait_status(job_id, ("completed", "failed"), timeout=300)
        assert final["status"] == "completed", final["status"]

        b_units = 0
        for task in final["tasks"]:
            for att in task.get("attempts", []):
                if att["status"] == "succeeded" and att["worker_id"] == "worker-B":
                    b_units += int(att.get("units_done") or 0)
        assert b_units > 0, "worker-B received no work after joining"
        (ev / "03-hotjoin.txt").write_text(
            f"joined at done={before_join['done_tasks']}, "
            f"worker-B units={b_units}\n", encoding="utf-8")
        results["G5-C"] = f"worker-B did {b_units} units"

        # ── G5-D kill ─────────────────────────────────────────────────────────
        print("\n[G5-D] kill a worker mid-task")
        job_id = env.submit(input_zip)
        env.wait_open_attempt("worker-A", job_id)
        env.wait_open_attempt("worker-B", job_id)
        killed_at = time.time()
        env.procs["worker-A"].kill()
        print(f"    killed worker-A at t={killed_at:.0f}")

        offline_at = None
        end = time.time() + 60
        while time.time() < end:
            w = env.worker("worker-A")
            if w and w["conn_state"] == "offline":
                offline_at = time.time() - killed_at
                break
            time.sleep(1)
        assert offline_at is not None, "worker-A never went offline"
        assert offline_at <= 45, f"offline detection took {offline_at:.1f}s (limit 45s)"
        print(f"    offline after {offline_at:.0f}s")

        final = env.wait_status(job_id, ("completed", "failed"), timeout=420)
        assert final["status"] == "completed", final["status"]
        total = units_succeeded(final)
        assert total == args.images, f"units {total} != {args.images} (double counting)"
        expired = sum(1 for t in final["tasks"] for a in t.get("attempts", [])
                      if a["status"] in ("expired", "failed"))
        (ev / "04-kill.txt").write_text(
            f"killed at t=0, offline after {offline_at:.1f}s\n"
            f"status={final['status']} succeeded_units={total} "
            f"(expected {args.images}), non_succeeded_attempts={expired}\n",
            encoding="utf-8")
        print(f"    recovered: {total} units, {expired} discarded attempts")
        results["G5-D"] = f"offline in {offline_at:.0f}s, {total} units, no double count"

        (ev / "summary.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
        print("\nG5 ALL PASSED")
        print(json.dumps(results, indent=2))
        return 0

    except AssertionError as exc:
        print(f"\nFAILED: {exc}")
        return 1
    except Exception as exc:
        print(f"\nERROR: {exc!r}")
        return 2
    finally:
        env.stop_all()


if __name__ == "__main__":
    raise SystemExit(main())
