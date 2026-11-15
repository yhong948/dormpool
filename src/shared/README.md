# src/shared/ — 跨模块冻结契约

> **维护规则：🔒 修改 `contracts.py` 须全员同意，单独提交，其他人先同步再继续。**
> 当前版本已在 `proto` 分支上被五个模块共同依赖，任何改动都会同时影响 Master / Worker / Executor。

## 功能

定义整个系统"说同一种语言"的部分：数据模型、状态枚举、REST 路径常量、子进程契约、时序常量。业务代码不硬编码任何状态字符串或端点路径，一律从这里取。

## 结构

```
src/shared/
├── __init__.py
├── contracts.py     唯一需要关注的实现文件
└── README.md
```

## contracts.py 内容清单

### 1. 状态枚举（6 个）

| 枚举 | 取值 | 用途 |
|------|------|------|
| `JobStatus` | queued / running / completed / failed | 用户可见的作业状态 |
| `TaskStatus` | pending / assigned / running / succeeded / retry_pending / failed | 调度单元状态 |
| `AttemptStatus` | running / succeeded / failed / expired | 一次真实执行；`expired` 是"租约到期未交付" |
| `WorkerConnState` | online / offline | 连接维度 |
| `WorkerContribState` | available / paused / draining | 贡献维度，三者语义见下 |
| `WorkerExecState` | idle / busy | 执行维度 |

三组 Worker 状态**互相独立**：一个节点可以同时是 `online + draining + busy`（人已按暂停、但手上的活还没干完）。这是整个动态自治设计的基础，不能合并成单一状态字段。

### 2. 数据模型（6 个 dataclass）

| 类 | 关键字段 | 说明 |
|----|---------|------|
| `WorkerCapability` | worker_id, hostname, cpu_cores, ram_gb, gpu_name, gpu_vram_gb, max_slots, executor_version | 注册时一次性上报的**静态能力** |
| `WorkerHeartbeat` | conn_state, contrib_state, exec_state, free_slots, running_task_ids, cpu_pct, gpu_pct | 周期性上报的**动态状态**，默认值即 `time.time()` |
| `Job` | job_id, task_type, input_ref, status, total_tasks, done_tasks, created_at, finished_at, output_ref | 用户一次提交 |
| `Task` | task_id, job_id, input_ref, resource_req, status, retry_count, max_retries, assigned_worker | 最小调度单元 |
| `Attempt` | attempt_id, task_id, worker_id, status, lease_expires, started_at, finished_at, units_done | 一次真实执行尝试 |
| `MetricRecord` | worker_id, task_id, attempt_id, units_done, elapsed_s, cpu_pct, gpu_pct | 仅成功路径写入 |

所有模型都用 `dataclasses.asdict()` 序列化为 HTTP body，因此字段名同时是线上协议的一部分。

### 3. REST 路径常量（`API` 类）

只声明，不实现；Master 实现、Worker 调用。

| 常量 | 路径 | 发起方 |
|------|------|--------|
| `PING` | `GET /ping` | 任意（免鉴权） |
| `REGISTER` | `POST /workers/register` | Worker |
| `HEARTBEAT` | `POST /workers/{worker_id}/heartbeat` | Worker |
| `CLAIM_TASK` | `POST /workers/{worker_id}/claim` | Worker |
| `FETCH_INPUT` | `GET /data/{ref}` | Worker |
| `UPLOAD_OUTPUT` | `POST /data/upload/{job_id}/{task_id}` | Worker，multipart 字段名固定 `file` |
| `RENEW_LEASE` | `POST /attempts/{attempt_id}/renew` | Worker |
| `COMMIT_RESULT` | `POST /attempts/{attempt_id}/commit` | Worker |
| `SUBMIT_JOB` | `POST /jobs` | 用户，multipart：`task_type` + `file` |
| `JOB_STATUS` | `GET /jobs/{job_id}` | 用户 / Console |
| `JOB_RESULT` | `GET /jobs/{job_id}/result` | 用户 / Console |
| `LIST_JOBS` | `GET /jobs` | Console |
| `LIST_WORKERS` | `GET /workers` | Console |
| `METRICS_SUMMARY` | `GET /metrics/summary` | Console |

### 4. 执行器子进程契约

```
python src/src/executor/run.py <task_type> <input_path> <output_path> [--worker-id X]
```

| 退出码 | 常量 | 含义 | Worker 的处理 |
|--------|------|------|--------------|
| 0 | `EXECUTOR_SUCCESS` | 成功，结果已写入 output_path | 上传并 commit |
| 1 | `EXECUTOR_TASK_FAIL` | 任务级失败（坏输入、计算异常） | commit 失败，Master 可能重试 |
| 2 | `EXECUTOR_FATAL` | 执行器致命错误 | commit 失败，本节点不再重试 |

stdout **最后一行**必须是 JSON：`{"units_done": <int>, "detail": "..."}`。中间行允许任意日志，Worker 从后往前找第一个可解析的 JSON 行。

### 5. 时序常量

| 常量 | 值 | 含义 | 关联影响 |
|------|-----|------|---------|
| `MASTER_PORT` | 8000 | Master 默认端口 | — |
| `HEARTBEAT_INTERVAL` | 0.5s | 心跳周期 | 必须远小于 `WORKER_TIMEOUT`，否则正常节点会误判离线 |
| `LEASE_DURATION` | 120s | 执行租约 | 太长则失联回收慢，太短则长任务被误判 |
| `LEASE_RENEW_BEFORE` | 20s | 剩余多久时续租 | 需小于租约的 1/3 |
| `WORKER_TIMEOUT` | 2s | 心跳超时判定离线 | 扫描间隔 `WORKER_TIMEOUT/2`，最坏检测约 3s |

## 使用方式

```python
from shared.contracts import (
    Job, Task, Attempt, WorkerCapability, WorkerHeartbeat,
    JobStatus, TaskStatus, AttemptStatus,
    WorkerConnState, WorkerContribState, WorkerExecState,
    LEASE_DURATION, HEARTBEAT_INTERVAL, WORKER_TIMEOUT,
)
```

## 一致性约束（改动前必读）

1. **字段名即协议**：`contrib_state` 不能写成 `contrib`，`free_slots` 必须是整数。
2. **`gpu_name` 保持 `Optional[str]`**：无卡时序列化为 JSON `null`，绝不能是字符串 `"None"`（验收门禁明确规定）。
3. **`units_done` 只统计有效工作量**：失败/过期/重试的 attempt 一律记 0，否则聚合效率会失真。
4. **`executor_version` 被复用**：当前格式为 `"1|<compute_score>"`，Worker 用它携带本机真实算力得分；这不是扩展字段，而是复用已有字段，所以不影响冻结。
