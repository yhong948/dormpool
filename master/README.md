# master/ — 中心协调服务

**负责人：Person A · 阶段：S2–S5 · 对应门禁：G2 / G3 / G4 / G6**

## 功能

部署在团队可访问的主机上，是整个系统的唯一权威：持有 Job/Task/Attempt 的全局状态、决定每个任务分给谁、确认结果、汇总输出、记录指标。Worker 不做任何状态决策，只上报和领取。

所有接口只监听 `127.0.0.1`，在局域网演示时由同一主机上的两个 Worker 进程访问。

## 结构

```
master/
├── main.py              入口 + uvicorn 调用 + / 与 /health
├── README.md
└── app/
    ├── __init__.py
    ├── m1_api.py        M1 API Gateway — 路由、鉴权、请求编排、组合根
    ├── m2_registry.py   M2 Worker Registry — 注册、心跳、三维状态、离线判定
    ├── m3_job.py        M3 Job Manager — Job 创建、分片、进度、终态、账本
    ├── m4_scheduler.py  M4 Task Scheduler — 原子分配、租约、校验提交、回收
    ├── m5_data.py       M5 Data & Result Manager — 分片落盘、结果聚合、路径防护
    ├── m6_metrics.py    M6 Metrics Collector — P_i / P_pool / η_pool / S / 节点事件
    └── c3_validation.py C3 结果校验（权威侧，见 executor/README.md）
```

## 模块依赖与组合根

实际依赖方向（都在 `m1_api.py` 里装配，避免循环导入）：

```
m1_api (组合根)
  ├── m2_registry ──(hook)──> m4_scheduler.reclaim_worker   (节点离线时回收任务)
  ├── m3_job ───────(hook)──> m1_api._on_job_completed       (Job 完成时触发聚合+冻结测量)
  ├── m4_scheduler ─(hook)──> m1_api._on_success             (成功路径写 MetricRecord)
  ├── m5_data
  └── m6_metrics
```

M2/M3/M4 内部不 import 彼此之外的其他 M 模块；跨模块协作通过 `bind_hooks()` 注入回调完成。

## 请求流程

### 提交作业

```
POST /jobs (multipart: task_type, file)
  -> 校验 zip 魔数 + task_type 在白名单内
  -> 预分配 job_id
  -> M5.save_input()            data/inputs/<job_id>/input.zip
  -> M5.split_job_input()       data/inputs/<job_id>/shard_<i>.zip   (SHARD_SIZE 张/片)
  -> M3.create_job()            为每个 shard 建一个 Task
  -> 返回 {job_id, total_tasks, status}
```

### Worker 领取

```
POST /workers/{id}/claim {free_slots}
  -> M2.can_accept_work()       在线且未被暂停（不看缓存槽位）
  -> M4.assign_task()           持锁：选任务 + 改状态 + 建 Attempt 三件事一起做
     -> 无可用任务返回 204
  -> 返回 {task, attempt{lease_expires}}
```

### 结果提交

```
POST /attempts/{id}/commit {output_ref, units_done, error_msg}
  -> Attempt 已非 RUNNING          -> {"discarded": true}   迟到结果直接丢弃
  -> 有 error_msg                  -> Attempt FAILED, Task 进入重试
  -> 否则 C3 校验 output_ref 指向的 zip
       校验失败                   -> Attempt FAILED, Task 进入重试
       校验通过                   -> Attempt SUCCEEDED（units 以校验值为准）
                                   -> M3.mark_task_done()
                                   -> 全部成功时 Job COMPLETED
                                   -> on_job_completed: M5 聚合 + M6 冻结 P_pool
```

## 关键语义

| 语义 | 实现位置 | 说明 |
|------|---------|------|
| **唯一确认** | `m4.commit_result` | 同一 Task 只有第一个成功的 Attempt 被计入；后续 commit 返回 `discarded` |
| **迟到结果丢弃** | `m4.commit_result` | Attempt 状态不是 RUNNING 时一律丢弃，绝不覆盖已确认结果 |
| **租约** | `m4._lease_loop` | 每 10s 扫描，过期 Attempt → EXPIRED，Task 重新排队 |
| **节点失联回收** | `m2.scan_expired` → `m4.reclaim_worker` | 不消耗重试次数就把 Task 放回队列，避免等满 120s 租约 |
| **只统计成功** | `m1._on_success` | MetricRecord 只在成功分支写入，每个 attempt 只写一次 |
| **P_pool 独立测量** | `m6.record_pool_job` | 用 Job 的 `created_at → finished_at` 墙钟，**绝不**用 ΣP_i |
| **P_i 口径** | `m6.per_device_throughput` | 分子为该设备成功单位数，分母为其**占用跨度**（首 attempt 开始到最后结束），下限为累加执行时间 |

## 接口清单

| 方法 | 路径 | 鉴权 | 说明 |
|------|------|------|------|
| GET | `/ping` | 否 | `{"ok": true, "ts": ...}` |
| POST | `/jobs` | 是 | multipart：`task_type`、`file` |
| GET | `/jobs` | 是 | `{"jobs": [...]}` |
| GET | `/jobs/{id}` | 是 | Job 字段 + `tasks[]`，每个 task 内含 `attempts[]` |
| GET | `/jobs/{id}/result` | 是 | 聚合后的 zip |
| POST | `/workers/register` | 是 | WorkerCapability |
| POST | `/workers/{id}/heartbeat` | 是 | WorkerHeartbeat |
| POST | `/workers/{id}/claim` | 是 | 有任务返回 200 + 载荷，无任务 204 |
| GET | `/workers` | 是 | `{"workers": [...]}`，含能力字段 |
| GET | `/data/{ref}` | 是 | 分片/结果文件，拒绝路径穿越 |
| POST | `/data/upload/{job_id}/{task_id}` | 是 | multipart 字段名 `file` |
| POST | `/attempts/{id}/renew` | 是 | 续租 |
| POST | `/attempts/{id}/commit` | 是 | 提交结果 |
| GET | `/metrics/summary` | 是 | 指标汇总 |
| GET | `/console` | 否 | Team Console 页面 |

## 启动

**Windows（PowerShell）**

```powershell
$env:DORMPOOL_TOKEN = "dev-token"
$env:DORMPOOL_DATA  = "data"       # 数据根目录
$env:SHARD_SIZE     = "3"          # 每个 Task 处理几张图（门禁用 1）
python -m uvicorn master.main:app --host 127.0.0.1 --port 8000
```

**Ubuntu / Linux（bash）**

```bash
export DORMPOOL_TOKEN=dev-token
export DORMPOOL_DATA=data          # 数据根目录
export SHARD_SIZE=3                # 每个 Task 处理几张图（门禁用 1）
python -m uvicorn master.main:app --host 127.0.0.1 --port 8000
```

**不要加 `--reload`**：它会 fork 出第二个进程，两个调度器会争抢同一份内存状态。

## 数据目录布局

```
data/
├── inputs/<job_id>/
│   ├── input.zip          用户上传原件
│   └── shard_0.zip ...    按 SHARD_SIZE 切分的分片（input_ref 指向这里）
└── outputs/<job_id>/
    ├── <task_id>.zip      单个 Task 的结果
    └── result.zip         聚合后的最终结果（manifest 无重复 input_name）
```

## 常见问题

| 现象 | 原因 | 处置 |
|------|------|------|
| claim 返回 500 | `assign_task` 内的局部变量未在空队列路径上初始化 | 已修复：`payload = None` 前置 |
| 每个任务之间卡 5–10 秒 | 用的是注册表里缓存的心跳 `free_slots` | 已修复：容量以 claim 请求体为准（`can_accept_work`） |
| 节点被杀 50 秒才变 offline | 扫描间隔 17.5s + 超时 35s > 45s 上限 | 已修复：扫描间隔 `min(WORKER_TIMEOUT/4, 10)` |
| 结果 zip 少图 | 聚合时按 task 顺序而非 manifest 合并 | `aggregate_job_output` 按 `input_name` 去重后统一合并 |
