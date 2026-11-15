"""
tests/e2e_local.py  —  local end-to-end harness (G3 / G4 / G6 in one run)
=========================================================================
Everything runs on 127.0.0.1 inside this one machine. Two worker processes with
separate data directories stand in for two physical machines; that is enough to
prove scheduling, isolation and result closure, but it is NOT a claim about
multi-machine speedup (see docs/阶段规划书.md, the honesty boundary).

    python tests/e2e_local.py                     # 7 images, 2 workers, shard=1
    python tests/e2e_local.py --images 12 --keep  # leave processes running
    python tests/e2e_local.py --port 8100

Exit code 0 means every assertion passed. The report on stdout is the evidence:
it contains the poll trace, the per-task claim table, the manifest check and the
metrics summary.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
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

HEADERS = {}


# ── tiny HTTP helpers (stdlib only, so the harness has no deps) ──────────────

def http_json(url: str, method: str = "GET", body: bytes = None,
              headers: dict = None, timeout: float = 15.0):
    req = urllib.request.Request(url, data=body, method=method)
    for key, value in (headers or HEADERS).items():
        req.add_header(key, value)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            payload = response.read()
            if response.status == 204 or not payload:
                return None
            return json.loads(payload.decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")[:300]
        raise RuntimeError(f"{method} {url} -> HTTP {exc.code}: {detail}")


def http_bytes(url: str, timeout: float = 60.0) -> bytes:
    req = urllib.request.Request(url)
    for key, value in HEADERS.items():
        req.add_header(key, value)
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return response.read()


def multipart(fields: dict, file_field: str, filename: str, blob: bytes):
    boundary = "----dormpool" + os.urandom(8).hex()
    out = bytearray()
    for name, value in fields.items():
        out += f"--{boundary}\r\n".encode()
        out += f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode()
        out += f"{value}\r\n".encode()
    out += f"--{boundary}\r\n".encode()
    out += (f'Content-Disposition: form-data; name="{file_field}"; '
            f'filename="{filename}"\r\n').encode()
    out += b"Content-Type: application/zip\r\n\r\n"
    out += blob
    out += f"\r\n--{boundary}--\r\n".encode()
    headers = dict(HEADERS)
    headers["Content-Type"] = f"multipart/form-data; boundary={boundary}"
    return bytes(out), headers


# ── process management ──────────────────────────────────────────────────────

def wait_for_port(port: int, timeout: float = 40.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.5)
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(0.4)
    return False


class Harness:
    def __init__(self, port: int, workers: int, images: int, shard_size: int,
                 delay_ms: int, work_dir: Path):
        self.port = port
        self.workers = workers
        self.images = images
        self.shard_size = shard_size
        self.delay_ms = delay_ms
        self.work_dir = work_dir
        self.base = f"http://127.0.0.1:{port}"
        self.children = []
        self.data_dir = work_dir / "data"
        self.worker_dirs = work_dir / "workers"

    # ── start / stop ─────────────────────────────────────────────────────────

    def start_master(self) -> None:
        env = dict(os.environ)
        env.update({

            "DORMPOOL_DATA": str(self.data_dir),
            "PYTHONUNBUFFERED": "1",
        })
        log = open(self.work_dir / "master.log", "wb")
        proc = subprocess.Popen(
            [os.environ.get("DORMPOOL_MASTER_PYTHON", sys.executable), "-m", "uvicorn", "master.main:app",
             "--host", "127.0.0.1", "--port", str(self.port)],
            cwd=str(REPO_ROOT), env=env, stdout=log, stderr=subprocess.STDOUT,
        )
        self.children.append(proc)
        if not wait_for_port(self.port):
            log.flush()
            raise RuntimeError("master did not start; see " + str(self.work_dir / "master.log"))

    def start_worker(self, name: str, control_port: int = 0) -> subprocess.Popen:
        env = dict(os.environ)
        env.update({
            "MASTER_URL": self.base,

            "WORKER_ID": name,
            "WORKER_DATA": str(self.worker_dirs / name),
            "MAX_SLOTS": "1",
            "WORKER_NO_REPL": "1",
            "DORMPOOL_EXECUTOR_DELAY_MS": str(self.delay_ms),
            "PYTHONUNBUFFERED": "1",
        })
        if control_port:
            env["WORKER_CONTROL_PORT"] = str(control_port)
        log = open(self.work_dir / f"{name}.log", "wb")
        proc = subprocess.Popen(
            [sys.executable, "worker/main.py", "--headless"],
            cwd=str(REPO_ROOT), env=env, stdout=log, stderr=subprocess.STDOUT,
        )
        self.children.append(proc)
        return proc

    def restart_worker(self, name: str) -> subprocess.Popen:
        return self.start_worker(name)

    def wait_registered(self, expected: int, timeout: float = 45.0) -> list:
        deadline = time.time() + timeout
        while time.time() < deadline:
            workers = http_json(f"{self.base}/workers")["workers"]
            online = [w for w in workers if w["conn_state"] == "online"]
            if len(online) >= expected:
                return online
            time.sleep(1)
        raise RuntimeError(f"only {len(online)} workers online, expected {expected}")

    # ── job helpers ──────────────────────────────────────────────────────────

    def build_input(self, path: Path) -> Path:
        from dormpool.src.executor.gen_sample import build
        build(self.images, str(path))
        return path

    def submit(self, zip_path: Path) -> str:
        blob = zip_path.read_bytes()
        body, headers = multipart({"task_type": "image_thumb"}, "file", zip_path.name, blob)
        reply = http_json(f"{self.base}/jobs", method="POST", body=body, headers=headers)
        return reply["job_id"]

    def poll_job(self, job_id: str, timeout: float = 300.0, trace_path=None):
        deadline = time.time() + timeout
        trace = []
        last = None
        while time.time() < deadline:
            job = http_json(f"{self.base}/jobs/{job_id}")
            line = f"{job['status']} {job['done_tasks']}/{job['total_tasks']}"
            if line != last:
                trace.append(line)
                last = line
                print(f"    {line}")
                if trace_path:
                    trace_path.write_text("\n".join(trace), encoding="utf-8")
            if job["status"] in ("completed", "failed"):
                return job, trace
            time.sleep(1.5)
        raise RuntimeError(f"job {job_id} did not finish in {timeout}s (last: {last})")

    def stop(self) -> None:
        for proc in self.children:
            if proc.poll() is None:
                try:
                    proc.terminate()
                except Exception:
                    pass
        deadline = time.time() + 15
        for proc in self.children:
            remaining = max(0.5, deadline - time.time())
            try:
                proc.wait(timeout=remaining)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass


# ── assertions ──────────────────────────────────────────────────────────────

def check_claims(job: dict, expected_workers: int) -> dict:
    tasks = job.get("tasks") or []
    assert tasks, "job carries no tasks[] — the response shape is wrong"
    workers = set()
    for task in tasks:
        succeeded = [a for a in task.get("attempts", []) if a["status"] == "succeeded"]
        assert len(succeeded) == 1, f"{task['task_id']} has {len(succeeded)} succeeded attempts"
        assert task["status"] == "succeeded", f"{task['task_id']} is {task['status']}"
        workers.add(succeeded[0]["worker_id"])
    assert len(workers) == expected_workers, f"expected {expected_workers} workers, saw {sorted(workers)}"
    return {"tasks": len(tasks), "workers": sorted(workers)}


def check_result(zip_bytes: bytes, source_zip: Path) -> dict:
    import io
    import zipfile

    result = zipfile.ZipFile(io.BytesIO(zip_bytes))
    names = result.namelist()
    assert "manifest.json" in names, "result zip has no manifest"
    manifest = json.loads(result.read("manifest.json"))

    source = zipfile.ZipFile(source_zip)
    want = sorted(n for n in source.namelist() if n.lower().endswith((".png", ".jpg", ".jpeg")))
    got = sorted(entry["input_name"] for entry in manifest)
    assert got == want, f"manifest mismatch: {set(want) ^ set(got)}"
    assert len(got) == len(set(got)), "duplicate input_name in result manifest"
    thumbs = [n for n in names if n.startswith("thumbs/")]
    return {"units": len(manifest), "thumbs": len(thumbs), "unique": len(set(got))}


def unit_sum(job: dict) -> int:
    total = 0
    for task in job.get("tasks") or []:
        for attempt in task.get("attempts", []):
            if attempt["status"] == "succeeded":
                total += int(attempt.get("units_done") or 0)
    return total


# ── main ────────────────────────────────────────────────────────────────────

def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="DormPool local end-to-end harness")
    parser.add_argument("--port", type=int, default=8100)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--images", type=int, default=7)
    parser.add_argument("--shard-size", type=int, default=1)
    parser.add_argument("--delay-ms", type=int, default=0)
    parser.add_argument("--keep", action="store_true", help="leave processes running")
    parser.add_argument("--work-dir", default=None)
    args = parser.parse_args(argv)

    os.environ["SHARD_SIZE"] = str(args.shard_size)

    work_dir = Path(args.work_dir) if args.work_dir else Path(tempfile.mkdtemp(prefix="dormpool-e2e-"))
    work_dir.mkdir(parents=True, exist_ok=True)
    evidence = work_dir / "evidence"
    evidence.mkdir(exist_ok=True)

    print(f"work dir : {work_dir}")
    print(f"master   : http://127.0.0.1:{args.port}   shard_size={args.shard_size}")

    harness = Harness(args.port, args.workers, args.images, args.shard_size,
                      args.delay_ms, work_dir)
    failures = []
    try:
        # 1. master
        print("\n[1] starting master")
        harness.start_master()
        ping = http_json(f"{harness.base}/ping", headers={})
        assert ping.get("ok") is True, ping
        print(f"    ping ok, ts={ping['ts']:.0f}")

        # 2. workers
        print(f"\n[2] starting {args.workers} workers")
        names = [f"worker-{chr(ord('A') + i)}" for i in range(args.workers)]
        procs = {}
        for name in names:
            procs[name] = harness.start_worker(name)
            print(f"    {name} pid={procs[name].pid}")
        online = harness.wait_registered(args.workers)
        (evidence / "workers.json").write_text(json.dumps(online, indent=2), encoding="utf-8")
        for worker in online:
            print(f"    {worker['worker_id']}: {worker['cpu_cores']}c "
                  f"{worker['ram_gb']}GB gpu={worker['gpu_name']} "
                  f"version={worker['executor_version']}")

        # 3. submit
        print(f"\n[3] submitting a {args.images}-image job")
        input_zip = harness.build_input(work_dir / "input.zip")
        job_id = harness.submit(input_zip)
        print(f"    job_id={job_id}")

        # 4. poll
        print("\n[4] polling")
        job, trace = harness.poll_job(job_id, trace_path=evidence / "progress.txt")
        (evidence / "job.json").write_text(json.dumps(job, indent=2), encoding="utf-8")
        assert job["status"] == "completed", f"job ended {job['status']}"
        assert job["done_tasks"] == job["total_tasks"], job["done_tasks"]
        expected_tasks = -(-args.images // args.shard_size)
        assert job["total_tasks"] == expected_tasks, \
            f"{job['total_tasks']} tasks, expected {expected_tasks}"

        # 5. claims
        print("\n[5] claim accounting")
        claims = check_claims(job, args.workers)
        (evidence / "claims.json").write_text(json.dumps(claims, indent=2), encoding="utf-8")
        print(f"    {claims['tasks']} tasks, workers {claims['workers']}")
        assert unit_sum(job) == args.images, \
            f"units sum {unit_sum(job)} != {args.images} (double counting?)"
        print(f"    total units {unit_sum(job)} == {args.images} (no double counting)")

        # 6. result
        print("\n[6] fetching and verifying the result")
        blob = http_bytes(f"{harness.base}/jobs/{job_id}/result")
        (evidence / "result.zip").write_bytes(blob)
        summary = check_result(blob, input_zip)
        print(f"    result: {len(blob)} bytes, units={summary['units']} "
              f"thumbs={summary['thumbs']} unique={summary['unique']}")

        from dormpool.src.master.app.c3_validation import validate_result
        validated = validate_result(str(evidence / "result.zip"),
                                    {"task_type": "image_thumb"})
        assert validated == summary["units"], (validated, summary["units"])
        print(f"    authoritative validator agrees: {validated} units")

        # 7. metrics
        print("\n[7] metrics")
        metrics = http_json(f"{harness.base}/metrics/summary")
        (evidence / "metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")
        print(f"    per device: {metrics['per_device_throughput']}")
        print(f"    p_pool={metrics['p_pool']} ({metrics['p_pool_source']}) "
              f"eta={metrics['eta_pool_pct']} S={metrics['speedup_S']}")
        assert metrics["p_pool"] > 0, "P_pool must be measured from the job wall clock"
        # Every completed run publishes device timings and observed rates.
        report = metrics['benchmark']
        assert report['mode'] == 'automatic_run', report
        assert report['eta_pool_pct'] is not None and report['speedup_S'] is not None, report
        assert set(report['device_metrics']) == set(claims['workers']), report
        assert sum(d['units_done'] for d in report['device_metrics'].values()) == args.images, report
        for device in report['device_metrics'].values():
            assert device['processing_s'] > 0 and device['time_source'] == 'worker_window', device
            assert abs(device['p_i'] - device['units_done'] / device['processing_s']) < 1e-9, device
        if metrics["per_device_throughput"]:
            assert set(metrics["per_device_throughput"]).issubset(set(claims["workers"])), \
                "throughput reported for a worker that never succeeded"
        if metrics["speedup_S"] is not None:
            assert 0.2 <= metrics["speedup_S"] <= 2.5, metrics["speedup_S"]

        # 8. console
        console = urllib.request.urlopen(f"{harness.base}/console", timeout=10).read()
        assert b"DormPool" in console, "console page not served"
        print(f"\n[8] console served ({len(console)} bytes)")

        print("\nALL CHECKS PASSED")
        (evidence / "PASSED.txt").write_text("all checks passed\n", encoding="utf-8")
        return 0

    except AssertionError as exc:
        failures.append(str(exc))
        print(f"\nFAILED: {exc}")
        return 1
    except Exception as exc:
        failures.append(repr(exc))
        print(f"\nERROR: {exc}")
        return 2
    finally:
        if args.keep:
            print(f"\nkeeping processes alive; logs in {work_dir}")
        else:
            harness.stop()
            print("processes stopped")


if __name__ == "__main__":
    raise SystemExit(main())
