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
import hashlib
import time
import uuid
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, Form, HTTPException, Response, UploadFile, Request
from fastapi.responses import FileResponse, JSONResponse
from starlette.concurrency import run_in_threadpool

from dormpool.src.shared.contracts import (
    WorkerCapability,
    WorkerHeartbeat,
    WorkerConnState,
    WorkerContribState,
    WorkerExecState,
    LEASE_DURATION,
)
from dormpool.src.master.app.c3_validation import validate_result, ValidationError, VALIDATORS
from dormpool.src.master.app.m2_registry import WorkerRegistry, bind_hooks as bind_registry_hooks
from dormpool.src.master.app.m3_job import JobManager, bind_hooks as bind_job_hooks, shard_size
from dormpool.src.master.app.m4_scheduler import TaskScheduler, bind_hooks as bind_scheduler_hooks
from dormpool.src.master.app.m5_data import DataManager
from dormpool.src.master.app.m6_metrics import MetricsCollector

# ── Composition root ─────────────────────────────────────────────────────────
# One instance of each module, wired once, shared by every request handler.

REGISTRY = WorkerRegistry()
JOBS = JobManager()
DATA = DataManager()
METRICS = MetricsCollector(report_path=Path(os.environ.get('DORMPOOL_DATA','data'))/'measurements/benchmarks.json')
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
            job.finished_at = time.time()  # include verified final result assembly
    else:
        job = JOBS.get_job(job_id)
        if job is not None:
            job.status = type(job.status).FAILED
            job.finished_at = time.time()
        return
    METRICS.record_pool_job(
        job_id=job_id,
        created_at=ledger["created_at"],
        finished_at=job.finished_at,
        units_total=ledger["units_total"],
        workers=ledger['workers'], execution_profiles=ledger['execution_profiles'],
        worker_devices=ledger['worker_devices'], failed_attempts=ledger['failed_attempts'],
        profile_changed=ledger['profile_changed'], per_worker_units=ledger['per_worker_units'],
        per_worker_windows=ledger['per_worker_windows'],
        succeeded_attempts=ledger['succeeded_attempts'],
    )
    # Every Job gets its own observed timing report. Independent benchmark
    # reports from /metrics/benchmarks retain their separate provenance.
    METRICS.record_automatic_benchmark(job_id)


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


@app.middleware('http')
async def measurement_clock(request: Request, call_next):
    request.state.received_at = time.time()
    return await call_next(request)

# ── Health ───────────────────────────────────────────────────────────────────

@app.get("/ping")
def ping() -> dict:
    return {"ok": True, "ts": time.time()}


# ── Jobs ─────────────────────────────────────────────────────────────────────

@app.post("/jobs")
async def submit_job(request: Request, task_type: str = Form("convnext_infer"), file: UploadFile = File(...),
                     worker_ids: Optional[str] = Form(None)):
    submitted_at = request.state.received_at
    allowed = [value.strip() for value in worker_ids.split(',') if value.strip()] if worker_ids else None
    if worker_ids is not None and not allowed:
        raise HTTPException(status_code=400,detail='worker_ids must contain at least one Worker ID')
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
    total_units = await run_in_threadpool(DATA.count_images, input_ref)

    input_hash = await run_in_threadpool(lambda: hashlib.sha256(payload).hexdigest())
    validator_hash = hashlib.sha256((Path(__file__).parent/'c3_validation.py').read_bytes()).hexdigest()
    METRICS.register_job(job_id, task_type=task_type, input_sha256=input_hash,
                         shard_size=shard_size(), correctness_version=validator_hash)
    job = JOBS.create_job(task_type, refs, input_zip_ref=input_ref, job_id=job_id,
                          worker_ids=allowed, created_at=submitted_at,
                          total_units=total_units)

    return {
        "job_id": job.job_id,
        "total_tasks": job.total_tasks,
        "total_units": job.total_units,
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


def _set_job_paused(job_id: str, paused: bool):
    try:
        return SCHED.set_job_paused(job_id, paused)
    except KeyError:
        raise HTTPException(status_code=404, detail="job not found")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@app.post("/jobs/{job_id}/pause")
def pause_job(job_id: str):
    return _set_job_paused(job_id, True)


@app.post("/jobs/{job_id}/resume")
def resume_job(job_id: str):
    return _set_job_paused(job_id, False)


@app.delete("/jobs/{job_id}")
def delete_job(job_id: str):
    try:
        SCHED.delete_job(job_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="job not found")
    DATA.delete_job(job_id)
    return {"deleted": True, "job_id": job_id}


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
            execution_profile=body.get("execution_profile") or {},
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
        resource_sample=body.get("resource_sample") or {},
    )
    result = REGISTRY.heartbeat(hb)
    if result.get("error"):
        raise HTTPException(status_code=404, detail=result["error"])
    METRICS.record_sample(worker_id, hb.resource_sample)
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
    task = JOBS.get_task(task_id)
    if task is None or task.job_id != job_id:
        raise HTTPException(status_code=404, detail="job or task not found")
    payload = await file.read()
    if not payload:
        raise HTTPException(status_code=400, detail="empty upload")
    if not payload.startswith(b"PK"):
        raise HTTPException(status_code=400, detail="result must be a zip archive")
    # Blocking write — keep it off the event loop (see submit_job for why).
    try:
        ref = await run_in_threadpool(DATA.save_task_output, job_id, task_id, payload)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
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
def metrics_summary(job_id: Optional[str] = None, benchmark_id: Optional[str] = None):
    jobs = JOBS.list_jobs()
    if job_id is None and jobs:
        job_id = max(jobs,key=lambda j:j['created_at'])['job_id']
    live = JOBS.ledger(job_id) if job_id else None
    if job_id and not live:
        raise HTTPException(status_code=404,detail='unknown Job')
    return METRICS.summary(live, benchmark_id)


@app.get('/metrics/benchmarks')
def list_benchmarks():
    return {'benchmarks':METRICS.benchmarks()}


@app.post('/metrics/benchmarks')
def create_benchmark(body: dict):
    try:
        return METRICS.create_benchmark(participants=body['participants'],
            baseline_jobs=body['baseline_jobs'], pool_job=body['pool_job'],
            warmup_jobs=body['warmup_jobs'], name=body.get('name',''),
            observed_jobs=[JOBS.ledger(j['job_id']) for j in JOBS.list_jobs()])
    except (ValueError, KeyError, TypeError) as exc:
        raise HTTPException(status_code=400,detail=str(exc)) from exc


# ── Console ──────────────────────────────────────────────────────────────────

CONSOLE_PATH = Path(__file__).resolve().parents[2] / "console" / "index.html"


@app.get("/console")
def console():
    if not CONSOLE_PATH.is_file():
        raise HTTPException(status_code=404, detail="console not built yet")
    return FileResponse(CONSOLE_PATH, media_type="text/html")
