"""
shared/contracts.py  —  C1 Task Contract  —  FROZEN SHARED CONTRACT
====================================================================
Team rule: change this file only by unanimous agreement.
Commit the change to main before anyone creates a new branch.

Covers:
  C1  Job / Task / Attempt / Worker data models
  REST endpoint paths Master exposes and Worker calls
  Worker → Executor subprocess CLI shape
  Metric record format
  Timing constants
"""

from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
import time, uuid


# ── Enumerations ──────────────────────────────────────────────────────────────

class JobStatus(str, Enum):
    QUEUED    = "queued"
    RUNNING   = "running"
    PAUSED    = "paused"
    COMPLETED = "completed"
    FAILED    = "failed"

class TaskStatus(str, Enum):
    PENDING       = "pending"
    ASSIGNED      = "assigned"
    RUNNING       = "running"
    SUCCEEDED     = "succeeded"
    RETRY_PENDING = "retry_pending"
    FAILED        = "failed"

class AttemptStatus(str, Enum):
    RUNNING   = "running"
    SUCCEEDED = "succeeded"
    FAILED    = "failed"
    EXPIRED   = "expired"      # lease timed out before result arrived

class WorkerConnState(str, Enum):
    ONLINE  = "online"
    OFFLINE = "offline"

class WorkerContribState(str, Enum):
    AVAILABLE = "available"
    PAUSED    = "paused"
    DRAINING  = "draining"    # paused, finishing tasks already in progress

class WorkerExecState(str, Enum):
    IDLE = "idle"
    BUSY = "busy"


# ── Data models ───────────────────────────────────────────────────────────────
# Plain dataclasses; convert to/from dict for HTTP (use dataclasses.asdict).

@dataclass
class WorkerCapability:
    worker_id:        str
    hostname:         str
    cpu_cores:        int
    ram_gb:           float
    gpu_name:         Optional[str]   = None
    gpu_vram_gb:      Optional[float] = None
    max_slots:        int             = 1    # max concurrent tasks this node accepts
    executor_version: str             = "1"
    supported_task_types: Optional[list[str]] = None
    execution_profile: dict = field(default_factory=dict)

@dataclass
class WorkerHeartbeat:
    worker_id:        str
    conn_state:       WorkerConnState
    contrib_state:    WorkerContribState
    exec_state:       WorkerExecState
    free_slots:       int
    running_task_ids: list[str]       = field(default_factory=list)
    cpu_pct:          Optional[float] = None
    gpu_pct:          Optional[float] = None
    ts:               float           = field(default_factory=time.time)
    resource_sample: dict = field(default_factory=dict)

@dataclass
class Job:
    job_id:      str            = field(default_factory=lambda: "job-" + uuid.uuid4().hex[:8])
    task_type:   str            = ""
    input_ref:   str            = ""   # filename/key on Master data store
    status:      JobStatus      = JobStatus.QUEUED
    total_tasks: int            = 0
    done_tasks:  int            = 0
    total_units: int            = 0   # validated input units, e.g. images
    done_units:  int            = 0   # validated output units
    created_at:  float          = field(default_factory=time.time)
    finished_at: Optional[float]= None
    output_ref:  Optional[str]  = None

@dataclass
class Task:
    task_id:         str           = field(default_factory=lambda: "task-" + uuid.uuid4().hex[:8])
    job_id:          str           = ""
    task_type:       str           = ""
    input_ref:       str           = ""   # shard reference within job input
    resource_req:    dict          = field(default_factory=dict)  # e.g. {"gpu": False}
    status:          TaskStatus    = TaskStatus.PENDING
    retry_count:     int           = 0
    max_retries:     int           = 2
    output_ref:      Optional[str] = None
    assigned_worker: Optional[str] = None

@dataclass
class Attempt:
    attempt_id:   str            = field(default_factory=lambda: "att-" + uuid.uuid4().hex[:8])
    task_id:      str            = ""
    worker_id:    str            = ""
    status:       AttemptStatus  = AttemptStatus.RUNNING
    lease_expires: float         = 0.0   # epoch seconds
    started_at:   float          = field(default_factory=time.time)
    finished_at:  Optional[float]= None
    output_ref:   Optional[str]  = None
    error_msg:    Optional[str]  = None
    units_done:   int            = 0     # e.g. images processed

@dataclass
class MetricRecord:
    worker_id:  str
    task_id:    str
    attempt_id: str
    units_done: int
    elapsed_s:  float
    cpu_pct:    Optional[float]
    gpu_pct:    Optional[float]
    ts:         float = field(default_factory=time.time)


# ── REST API surface ──────────────────────────────────────────────────────────
# Master listens on these paths; Worker calls them.
# All relative to http://<master_host>:<MASTER_PORT>

class API:
    # Worker lifecycle
    REGISTER     = "POST /workers/register"               # body: WorkerCapability (as dict)
    HEARTBEAT    = "POST /workers/{worker_id}/heartbeat"  # body: WorkerHeartbeat (as dict)

    # Task pull loop (Worker-pull model)
    CLAIM_TASK   = "POST /workers/{worker_id}/claim"      # body: {"free_slots": int}
    FETCH_INPUT  = "GET  /data/{ref}"
    UPLOAD_OUTPUT= "POST /data/upload/{job_id}/{task_id}" # multipart, field name "file"
    RENEW_LEASE  = "POST /attempts/{attempt_id}/renew"
    COMMIT_RESULT= "POST /attempts/{attempt_id}/commit"   # body: {"output_ref","units_done","error_msg"}

    # Health
    PING         = "GET  /ping"

    # User / job surface
    SUBMIT_JOB   = "POST /jobs"                           # body: multipart or {"task_type","input_ref"}
    JOB_STATUS   = "GET  /jobs/{job_id}"
    JOB_RESULT   = "GET  /jobs/{job_id}/result"
    LIST_JOBS    = "GET  /jobs"
    LIST_WORKERS = "GET  /workers"

    # Metrics / benchmark
    METRICS_SUMMARY = "GET /metrics/summary"


# ── Worker → Executor subprocess contract ────────────────────────────────────
# The executor is called as a subprocess by the Worker Agent:
#
#   python executor/run.py <task_type> <input_path> <output_path> [--worker-id X]
#
# Exit codes:
#   0  success — output written to <output_path>
#   1  task-level failure (bad input, computation error) — no output
#   2  fatal executor error — do not retry on this node
#
# The executor's LAST stdout line must be JSON:
#   {"units_done": <int>, "detail": "<optional string>"}

EXECUTOR_SUCCESS    = 0
EXECUTOR_TASK_FAIL  = 1
EXECUTOR_FATAL      = 2


# ── Timing constants (seconds) ────────────────────────────────────────────────

MASTER_PORT         = 8000
HEARTBEAT_INTERVAL  = 0.5   # Worker sends heartbeat every N seconds
LEASE_DURATION      = 120   # Initial task lease granted on claim
LEASE_RENEW_BEFORE  = 20    # Worker renews when ≤ this many seconds remain
WORKER_TIMEOUT      = 2.0   # Master marks OFFLINE after N seconds without heartbeat
