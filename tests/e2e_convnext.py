"""Run a real local Master -> Worker -> CUDA ConvNeXt -> result round trip.

Run with .venv-gpu/Scripts/python.exe tests/e2e_convnext.py.
Uses a temporary loopback server, not the team's running Master.
"""
import io
import argparse
import json
import os
from pathlib import Path
import socket
import tempfile
import zipfile

from e2e_local import Harness, http_json, http_bytes, multipart, REPO_ROOT


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--master-python", default=str(REPO_ROOT / ".venv" / "Scripts" / "python.exe"))
    args = parser.parse_args()
    os.environ["DORMPOOL_MASTER_PYTHON"] = str(Path(args.master_python).resolve())
    cache = REPO_ROOT / "._tmp" / "cupy-cache"
    os.environ.setdefault("CUPY_CACHE_DIR", str(cache))
    os.environ["DORMPOOL_CONVNEXT_DEVICE"] = "cuda"
    os.environ["SHARD_SIZE"] = "1"
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    root = REPO_ROOT / "._tmp"
    root.mkdir(exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix="convnext-e2e-", dir=root))
    harness = Harness(port, 1, 2, 1, 0, work)
    print("Evidence:", work, flush=True)
    try:
        harness.start_master()
        harness.start_worker("convnext-gpu")
        workers = harness.wait_registered(1, timeout=120)
        assert "convnext_infer" in workers[0]["supported_task_types"], workers
        # Two shards with equal stems exercise aggregation filename collisions.
        from PIL import Image
        image = io.BytesIO()
        Image.new("RGB", (320, 240), (20, 90, 150)).save(image, format="PNG")
        source = work / "input.zip"
        with zipfile.ZipFile(source, "w") as archive:
            for name in ("a/same.png", "b/same.png"):
                archive.writestr(name, image.getvalue())
        body, headers = multipart({"task_type": "convnext_infer"}, "file", "input.zip", source.read_bytes())
        job_id = http_json(harness.base + "/jobs", "POST", body, headers)["job_id"]
        job, _ = harness.poll_job(job_id)
        (work / "job.json").write_text(json.dumps(job, indent=2), encoding="utf-8")
        assert job["status"] == "completed", job
        assert job["done_tasks"] == job["total_tasks"] == 2, job
        for task in job["tasks"]:
            assert task["resource_req"]["gpu"] is True
            assert sum(a["status"] == "succeeded" for a in task["attempts"]) == 1
        result = work / "result.zip"
        result.write_bytes(http_bytes(harness.base + f"/jobs/{job_id}/result"))
        from master.app.c3_validation import validate_convnext
        assert validate_convnext(str(source), str(result)) == 2
        metrics = http_json(harness.base + "/metrics/summary")
        (work / "metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")
        assert metrics["p_pool"] > 0, metrics
        log = (work / "convnext-gpu.log").read_text(encoding="utf-8", errors="replace")
        assert "device=cuda" in log, log
        print("PASS: 2 GPU tasks, validated combined predictions and logits, metrics recorded.")
    finally:
        harness.stop()


if __name__ == "__main__":
    main()
