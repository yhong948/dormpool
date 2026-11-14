"""
master/app/m1_api.py  —  M1 API Gateway
========================================
Owner: Person A.

The HTTP surface for Workers and users. Requests do not require credentials.

Endpoint map (paths come from shared.contracts.API):
  GET  /ping                                  no auth
  POST /jobs                                  multipart: task_type, file
  GET  /jobs, /jobs/{id}, /jobs/{id}/result
  POST /workers/register
  POST /workers/{id}/heartbeat
  POST /workers/{id}/claim                    204 when nothing is queued
  GET  /workers                               {"workers": [...]}
  GET  /data/{ref}                            traversal-safe
  POST /data/upload/{job_id}/{task_id}        multipart field "file"
  POST /attempts/{id}/renew, /attempts/{id}/commit
  GET  /metrics/summary
  GET  /console
"""

from __future__ import annotations

import os
import time
import uuid
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, Form, HTTPException, Response, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from starlette.concurrency import run_in_threadpool

from shared.contracts import (
    WorkerCapability,
    WorkerHeartbeat,
    WorkerConnState,
    WorkerContribState,
    WorkerExecState,
    LEASE_DURATION,
)
from master.app.c3_validation import validate_result, ValidationError, VALIDATORS
from master.app.m2_registry import WorkerRegistry, bind_hooks as bind_registry_hooks
from master.app.m3_job import JobManager, bind_hooks as bind_job_hooks, shard_size
from master.app.m4_scheduler import TaskScheduler, bind_hooks as bind_scheduler_hooks
from master.app.m5_data import DataManager
from master.app.m6_metrics import MetricsCollector

# ── Composition root ─────────────────────────────────────────────────────────
# One instance of each module, wired once, shared by every request handler.

REGISTRY = WorkerRegistry()
JOBS = JobManager()
DATA = DataManager()
METRICS = MetricsCollector()
SCHED = TaskScheduler(JOBS, REGISTRY)


def _on_success(task_id: str, job_id: str, worker_id: str, units: int) -> None:
    attempt = _current_attempt_for(task_id, worker_id)
    elapsed = 0.0
    if attempt:
        elapsed = max(0.0, float(attempt.get("finished_at") or 0)
                      - float(attempt.get("started_at") or 0))
        METRICS.record_attempt(
            worker_id=worker_id,
            task_id=task_id,
            attempt_id=attempt["attempt_id"],
            units_done=units,
            elapsed_s=elapsed,
            cpu_pct=attempt.get("cpu_pct"),
            gpu_pct=attempt.get("gpu_pct"),
        )
    if not attempt:
        # Defensive: still account the unit rather than silently dropping it.
        METRICS.record_attempt(
            worker_id=worker_id,
            task_id=task_id,
            attempt_id=f"fallback-{task_id}-{worker_id}",
            units_done=units,
            elapsed_s=elapsed,
        )


def _current_attempt_for(task_id: str, worker_id: str) -> Optional[dict]:
    for attempt in JOBS.attempts_of(task_id):
        if attempt.get("worker_id") == worker_id and attempt.get("status") == "succeeded":
            return attempt
    return None


def _on_job_completed(job_id: str) -> None:
    ledger = JOBS.ledger(job_id)
    if not ledger:
        return
    task_ids = JOBS.succeeded_task_ids(job_id)
    ref = DATA.aggregate_job_output(job_id, task_ids)
    if ref:
        job = JOBS.get_job(job_id)
        if job is not None:
            job.output_ref = ref
    METRICS.record_pool_job(
        job_id=job_id,
        created_at=ledger["created_at"],
        finished_at=ledger["finished_at"] or time.time(),
        units_total=ledger["units_total"],
    )


REGISTRY_EVENTS = METRICS
bind_registry_hooks(
    on_offline=SCHED.reclaim_worker,
    on_node_event=lambda wid, ev: METRICS.record_node_event(wid, ev),
)
bind_job_hooks(on_job_completed=_on_job_completed)
bind_scheduler_hooks(
    on_success=_on_success,
    validate=validate_result,
    resolve_output=lambda ref: DATA.resolve_path(ref),
)

REGISTRY.start()
SCHED.start()

app = FastAPI(title="DormPool Master", version="0.1.0")

# ── Health ───────────────────────────────────────────────────────────────────

@app.get("/ping")
def ping() -> dict:
    return {"ok": True, "ts": time.time()}


# ── Jobs ─────────────────────────────────────────────────────────────────────

@app.post("/jobs")
async def submit_job(task_type: str = Form(...), file: UploadFile = File(...)):
    try:
        payload = await file.read()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"could not read upload: {exc}")

    if not payload:
        raise HTTPException(status_code=400, detail="uploaded file is empty")
    if not payload.startswith(b"PK"):
        raise HTTPException(status_code=400, detail="input must be a zip archive")

    if task_type not in _known_task_types():
        raise HTTPException(status_code=400, detail=f"unknown task_type: {task_type}")

    # Allocate the id up front so the upload and its shards live under one folder.
    job_id = "job-" + uuid.uuid4().hex[:8]
    # Writing the archive and splitting it are blocking disk work. This handler
    # is `async def`, so running them inline would occupy the event loop for the
    # whole split — every heartbeat and console poll queues behind it and a
    # dead-looking node is marked OFFLINE. Push them to the threadpool.
    input_ref = await run_in_threadpool(
        DATA.save_input, job_id, file.filename or "input.zip", payload
    )
    refs = await run_in_threadpool(DATA.split_job_input, job_id, input_ref, shard_size())
    if not refs:
        raise HTTPException(status_code=400, detail="no images found in the uploaded archive")

    job = JOBS.create_job(task_type, refs, input_zip_ref=input_ref, job_id=job_id)

    return {
        "job_id": job.job_id,
        "total_tasks": job.total_tasks,
        "status": job.status.value,
        "task_type": job.task_type,
    }


def _known_task_types() -> set:
    # The Master validates results; it does not import Worker execution code.
    return set(VALIDATORS)


@app.get("/jobs")
def list_jobs():
    return {"jobs": JOBS.list_jobs()}


@app.get("/jobs/{job_id}")
def job_status(job_id: str):
    view = JOBS.job_view(job_id)
    if view is None:
        raise HTTPException(status_code=404, detail="job not found")
    return view


@app.get("/jobs/{job_id}/result")
def job_result(job_id: str):
    job = JOBS.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    path = DATA.get_job_output_path(job_id)
    if path is None:
        raise HTTPException(status_code=404, detail="result not ready")
    return FileResponse(
        path,
        media_type="application/zip",
        filename=f"{job_id}-result.zip",
    )


# ── Workers ──────────────────────────────────────────────────────────────────

@app.post("/workers/register")
def register_worker(body: dict):
    try:
        cap = WorkerCapability(
            worker_id=str(body["worker_id"]),
            hostname=str(body.get("hostname") or "unknown"),
            cpu_cores=int(body.get("cpu_cores") or 1),
            ram_gb=float(body.get("ram_gb") or 0.0),
            gpu_name=body.get("gpu_name"),
            gpu_vram_gb=body.get("gpu_vram_gb"),
            max_slots=int(body.get("max_slots") or 1),
            executor_version=str(body.get("executor_version") or "1"),
            supported_task_types=body.get("supported_task_types"),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=f"invalid capability: {exc}")
    return REGISTRY.register(cap)


@app.post("/workers/{worker_id}/heartbeat")
def heartbeat(worker_id: str, body: dict):
    hb = WorkerHeartbeat(
        worker_id=worker_id,
        conn_state=_enum(WorkerConnState, body.get("conn_state"), WorkerConnState.ONLINE),
        contrib_state=_enum(WorkerContribState, body.get("contrib_state"),
                            WorkerContribState.AVAILABLE),
        exec_state=_enum(WorkerExecState, body.get("exec_state"), WorkerExecState.IDLE),
        free_slots=int(body.get("free_slots") or 0),
        running_task_ids=list(body.get("running_task_ids") or []),
        cpu_pct=body.get("cpu_pct"),
        gpu_pct=body.get("gpu_pct"),
    )
    result = REGISTRY.heartbeat(hb)
    if result.get("error"):
        raise HTTPException(status_code=404, detail=result["error"])
    return result


def _enum(enum_cls, value, default):
    if value is None:
        return default
    if isinstance(value, enum_cls):
        return value
    try:
        return enum_cls(str(value))
    except ValueError:
        return default


@app.post("/workers/{worker_id}/claim")
def claim_task(worker_id: str, body: dict = None):
    free_slots = int((body or {}).get("free_slots") or 1)
    assigned = SCHED.assign_task(worker_id, free_slots)
    if assigned is None:
        return Response(status_code=204)
    return assigned


@app.get("/workers")
def list_workers():
    return {"workers": REGISTRY.list_workers()}


# ── Data ─────────────────────────────────────────────────────────────────────

@app.get("/data/{ref:path}")
def fetch_data(ref: str):
    path = DATA.resolve_path(ref)
    if path is None or not os.path.isfile(path):
        raise HTTPException(status_code=404, detail="not found")
    return FileResponse(path, media_type="application/octet-stream")


@app.post("/data/upload/{job_id}/{task_id}")
async def upload_output(job_id: str, task_id: str, file: UploadFile = File(...)):
    payload = await file.read()
    if not payload:
        raise HTTPException(status_code=400, detail="empty upload")
    if not payload.startswith(b"PK"):
        raise HTTPException(status_code=400, detail="result must be a zip archive")
    # Blocking write — keep it off the event loop (see submit_job for why).
    ref = await run_in_threadpool(DATA.save_task_output, job_id, task_id, payload)
    return {"output_ref": ref}


# ── Attempts ─────────────────────────────────────────────────────────────────

@app.post("/attempts/{attempt_id}/renew")
def renew_lease(attempt_id: str):
    return SCHED.renew_lease(attempt_id)


@app.post("/attempts/{attempt_id}/commit")
def commit_result(attempt_id: str, body: dict):
    return SCHED.commit_result(
        attempt_id=attempt_id,
        output_ref=str(body.get("output_ref") or ""),
        units_done=int(body.get("units_done") or 0),
        error_msg=body.get("error_msg"),
    )


# ── Metrics ──────────────────────────────────────────────────────────────────

@app.get("/metrics/summary")
def metrics_summary():
    return METRICS.summary()


# ── Console ──────────────────────────────────────────────────────────────────

CONSOLE_PATH = Path(__file__).resolve().parents[2] / "console" / "index.html"


@app.get("/console")
def console():
    if not CONSOLE_PATH.is_file():
        raise HTTPException(status_code=404, detail="console not built yet")
    return FileResponse(CONSOLE_PATH, media_type="text/html")
