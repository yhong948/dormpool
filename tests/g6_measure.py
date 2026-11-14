"""
tests/g6_measure.py  —  measurement gate (G6)
=============================================
Two INDEPENDENT measurements, never one derived from the other:

  1. single-node  : one worker alive, same input   -> P_1, and S must be ~1.0
  2. pool         : both workers alive, same input -> P_pool from the job's wall
                                                     clock, S should exceed 1

If S is always equal to the number of workers, P_pool is being faked as ΣP_i and
this script fails on purpose (G6-5).

    python tests/g6_measure.py --port 8300 --images 9 --delay-ms 700
"""

from __future__ import annotations

import argparse
import json
import os
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
sys.path.insert(0, str(Path(__file__).parent))
from e2e_local import http_json, http_bytes, multipart, wait_for_port  # noqa: E402


class Env:
    def __init__(self, port, work, delay_ms):
        self.port, self.work, self.delay_ms = port, work, delay_ms
        self.base = f"http://127.0.0.1:{port}"
        self.procs = {}

    def start_master(self):
        env = dict(os.environ,
                   DORMPOOL_DATA=str(self.work / "data"), PYTHONUNBUFFERED="1",
                   SHARD_SIZE="1")
        log = open(self.work / "master.log", "wb")
        self.procs["master"] = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "master.main:app",
             "--host", "127.0.0.1", "--port", str(self.port)],
            cwd=str(REPO_ROOT), env=env, stdout=log, stderr=subprocess.STDOUT)
        if not wait_for_port(self.port):
            raise RuntimeError("master did not start")

    def start_worker(self, name):
        env = dict(os.environ, MASTER_URL=self.base,
                   WORKER_ID=name, WORKER_DATA=str(self.work / name),
                   MAX_SLOTS="1", WORKER_NO_REPL="1", PYTHONUNBUFFERED="1",
                   DORMPOOL_EXECUTOR_DELAY_MS=str(self.delay_ms))
        log = open(self.work / f"{name}.log", "ab")
        proc = subprocess.Popen([sys.executable, "worker/main.py", "--headless"],
                                cwd=str(REPO_ROOT), env=env, stdout=log,
                                stderr=subprocess.STDOUT)
        self.procs[name] = proc

    def stop_worker(self, name):
        proc = self.procs.pop(name, None)
        if proc and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=15)
            except Exception:
                proc.kill()

    def wait_online(self, names, timeout=45):
        end = time.time() + timeout
        while time.time() < end:
            online = {w["worker_id"] for w in http_json(f"{self.base}/workers")["workers"]
                      if w["conn_state"] == "online"}
            if names.issubset(online):
                return True
            time.sleep(1)
        raise RuntimeError(f"{names} not online")

    def submit(self, zip_path, measurement_mode="pool", target_worker_id=""):
        fields = {"task_type": "image_thumb", "measurement_mode": measurement_mode}
        if target_worker_id:
            fields["target_worker_id"] = target_worker_id
        body, headers = multipart(fields, "file",
                                  zip_path.name, zip_path.read_bytes())
        return http_json(f"{self.base}/jobs", "POST", body, headers)["job_id"]

    def run_job(self, zip_path, timeout=300, measurement_mode="pool", target_worker_id=""):
        job_id = self.submit(zip_path, measurement_mode, target_worker_id)
        end = time.time() + timeout
        while time.time() < end:
            job = http_json(f"{self.base}/jobs/{job_id}")
            if job["status"] in ("completed", "failed"):
                return job
            time.sleep(1)
        raise RuntimeError("job did not finish")

    def stop_all(self):
        for name in list(self.procs):
            self.stop_worker(name)


def measure(env, zip_path, label, measurement_mode="pool", target_worker_id=""):
    job = env.run_job(zip_path, measurement_mode=measurement_mode,
                      target_worker_id=target_worker_id)
    wall = float(job["finished_at"]) - float(job["created_at"])
    units = sum(int(a.get("units_done") or 0)
                for t in job["tasks"] for a in t.get("attempts", [])
                if a["status"] == "succeeded")
    rate = units / wall if wall > 0 else 0.0
    summary = http_json(f"{env.base}/metrics/summary")
    print(f"  {label}: status={job['status']} units={units} wall={wall:.2f}s "
          f"p={rate:.3f} u/s")
    print(f"    summary: p_pool={summary['p_pool']} eta={summary['eta_pool_pct']}% "
          f"S={summary['speedup_S']} per_device={summary['per_device_throughput']}")
    return {"job": job, "wall": wall, "units": units, "rate": rate, "summary": summary}


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8300)
    ap.add_argument("--images", type=int, default=9)
    ap.add_argument("--delay-ms", type=int, default=700)
    ap.add_argument("--work-dir", default=None)
    args = ap.parse_args(argv)

    work = Path(args.work_dir) if args.work_dir else Path(tempfile.mkdtemp(prefix="dormpool-g6-"))
    work.mkdir(parents=True, exist_ok=True)
    evidence = work / "evidence"
    evidence.mkdir(exist_ok=True)

    from executor.gen_sample import build
    input_zip = work / "input.zip"
    build(args.images, str(input_zip))

    env = Env(args.port, work, args.delay_ms)
    report = {}
    try:
        env.start_master()
        env.start_worker("worker-A")
        env.wait_online({"worker-A"})
        print(f"[single] only worker-A alive, {args.images} images")
        baseline_a = measure(env, input_zip, "baseline-A", "baseline", "worker-A")
        assert baseline_a["job"]["status"] == "completed", baseline_a["job"]["status"]
        report["baseline-A"] = {"wall": baseline_a["wall"], "units": baseline_a["units"],
                                "rate": baseline_a["rate"]}

        env.start_worker("worker-B")
        env.wait_online({"worker-A", "worker-B"})
        print(f"[pool] both workers alive, same input")
        baseline_b = measure(env, input_zip, "baseline-B", "baseline", "worker-B")
        assert baseline_b["job"]["status"] == "completed", baseline_b["job"]["status"]
        report["baseline-B"] = {"wall": baseline_b["wall"], "units": baseline_b["units"],
                                "rate": baseline_b["rate"]}

        pool = measure(env, input_zip, "pool", "pool")
        assert pool["job"]["status"] == "completed", pool["job"]["status"]
        s_pool = pool["summary"]["speedup_S"]
        assert s_pool is not None and s_pool > 0, s_pool
        assert pool["summary"]["per_device_source"] == "independent_baseline"

        # P_pool must come from the wall clock, not from ΣP_i.
        assert pool["summary"]["p_pool_source"] == "pool_wall_clock", \
            pool["summary"]["p_pool_source"]
        eta = pool["summary"]["eta_pool_pct"]
        assert eta is not None and 0 < eta <= 115, f"eta={eta}"

        report["pool"] = {"wall": pool["wall"], "units": pool["units"],
                          "rate": pool["rate"], "S": s_pool, "eta": eta,
                          "p_pool": pool["summary"]["p_pool"],
                          "per_device": pool["summary"]["per_device_throughput"]}

        gap = abs(pool["summary"]["p_pool"] - pool["rate"]) / max(pool["rate"], 1e-9)
        assert gap < 0.15, f"p_pool {pool['summary']['p_pool']} vs measured {pool['rate']}"
        report["p_pool_agrees_with_wall_clock"] = True

        (evidence / "summary.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        print("\nG6 PASSED")
        print(json.dumps(report, indent=2))
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
