"""
master/app/m1_api.py  —  M1 API Gateway
========================================
Person A owns this file.

Responsibility: unified HTTP entry point for users and Worker Agents.
  - Input validation and routing
  - Basic auth token check (Bearer token from env DORMPOOL_TOKEN)
  - Delegates to M3 Job Manager (jobs) and M2 Worker Registry (workers)

Stub: replace pass/TODO sections with real logic.
"""

import os
from fastapi import FastAPI, Depends, HTTPException, Header, UploadFile, File
from fastapi.responses import FileResponse
from typing import Optional

app = FastAPI(title="DormPool Master")

# ── Auth ──────────────────────────────────────────────────────────────────────

_TOKEN = os.environ.get("DORMPOOL_TOKEN", "dev-token")

def verify_token(authorization: Optional[str] = Header(None)):
    if _TOKEN and authorization != f"Bearer {_TOKEN}":
        raise HTTPException(status_code=401, detail="Unauthorized")

# ── Job routes (M3) ───────────────────────────────────────────────────────────

@app.post("/jobs", dependencies=[Depends(verify_token)])
async def submit_job(task_type: str, file: UploadFile = File(...)):
    """Accept a job submission. Calls M3 job_manager.create_job()."""
    # TODO (Person A): save file via M5, call m3_job.create_job(task_type, input_ref)
    raise HTTPException(status_code=501, detail="Not implemented")

@app.get("/jobs/{job_id}")
async def job_status(job_id: str):
    """Return Job status and progress."""
    # TODO (Person A): call m3_job.get_job(job_id)
    raise HTTPException(status_code=501, detail="Not implemented")

@app.get("/jobs/{job_id}/result")
async def job_result(job_id: str):
    """Return or redirect to the aggregated output for a completed Job."""
    # TODO (Person A): call m5_data.get_job_output(job_id)
    raise HTTPException(status_code=501, detail="Not implemented")

@app.get("/jobs")
async def list_jobs():
    """List all Jobs with summary status."""
    # TODO (Person A): call m3_job.list_jobs()
    raise HTTPException(status_code=501, detail="Not implemented")

# ── Worker routes (M2) ────────────────────────────────────────────────────────

@app.post("/workers/register")
async def register_worker(body: dict):
    """Register a new Worker. Body: WorkerCapability as dict."""
    # TODO (Person A): call m2_registry.register(WorkerCapability(**body))
    raise HTTPException(status_code=501, detail="Not implemented")

@app.post("/workers/{worker_id}/heartbeat")
async def heartbeat(worker_id: str, body: dict):
    """Receive heartbeat. Body: WorkerHeartbeat as dict."""
    # TODO (Person A): call m2_registry.heartbeat(WorkerHeartbeat(**body))
    raise HTTPException(status_code=501, detail="Not implemented")

@app.post("/workers/{worker_id}/claim")
async def claim_task(worker_id: str, body: dict):
    """Worker pulls next Task. Body: {"free_slots": int}. Returns Task dict or 204."""
    # TODO (Person A): call m4_scheduler.assign_task(worker_id, body["free_slots"])
    raise HTTPException(status_code=501, detail="Not implemented")

@app.get("/workers")
async def list_workers():
    """List all registered Workers and their state."""
    # TODO (Person A): call m2_registry.list_workers()
    raise HTTPException(status_code=501, detail="Not implemented")

# ── Data routes (M5) ──────────────────────────────────────────────────────────

@app.get("/data/{ref:path}")
async def fetch_data(ref: str):
    """Serve an input shard or result file by ref key."""
    # TODO (Person A): call m5_data.resolve_path(ref) → FileResponse
    raise HTTPException(status_code=501, detail="Not implemented")

# ── Attempt routes (M4 + M5) ─────────────────────────────────────────────────

@app.post("/attempts/{attempt_id}/renew")
async def renew_lease(attempt_id: str):
    """Extend execution lease for a running Attempt."""
    # TODO (Person A): call m4_scheduler.renew_lease(attempt_id)
    raise HTTPException(status_code=501, detail="Not implemented")

@app.post("/attempts/{attempt_id}/commit")
async def commit_result(attempt_id: str, body: dict):
    """Worker commits result. Body: {output_ref, units_done, error_msg}."""
    # TODO (Person A): call m4_scheduler.commit_result(attempt_id, body)
    raise HTTPException(status_code=501, detail="Not implemented")

# ── Metrics ───────────────────────────────────────────────────────────────────

@app.get("/metrics/summary")
async def metrics_summary():
    """Return aggregated throughput, per-device stats, and dynamic node events."""
    # TODO (Person A): call m6_metrics.summary()
    raise HTTPException(status_code=501, detail="Not implemented")
