# DormPool — 弹性团队算力池

> Campus Edge Fabric 黑客马拉松 · 方案二 · 18h 有效开发

把宿舍/工作室里的设备连起来，对外提供一个统一计算服务。

---

## 目录结构与人员分工

```
dormpool/
├── shared/            ← 🔒 共享契约 (三人共同维护，改动须全员同意后先合并主分支)
│   └── contracts.py      C1: Job/Task/Attempt 数据模型、REST 接口路径、时序常量
│
├── master/            ← 👤 Person A 负责
│   ├── main.py           启动入口 (uvicorn)
│   └── app/
│       ├── m1_api.py     M1 API Gateway (FastAPI 路由)
│       ├── m2_registry.py M2 Worker Registry
│       ├── m3_job.py     M3 Job Manager (Job 创建、拆分、进度)
│       ├── m4_scheduler.py M4 Task Scheduler (分配、租约、重试)
│       ├── m5_data.py    M5 Data & Result Manager
│       └── m6_metrics.py  M6 Metrics Collector (η_pool, S)
│
├── worker/            ← 👤 Person B 负责
│   ├── main.py           启动入口
│   └── agent/
│       ├── w1_runtime.py  W1 Agent Runtime (生命周期、主循环)
│       ├── w2_connector.py W2 Master Connector (HTTP 通信、重连)
│       ├── w3_monitor.py  W3 Resource Monitor (CPU/GPU、槽位)
│       ├── w4_contrib.py  W4 Contribution Controller (暂停/恢复/准入)
│       ├── w5_exec.py     W5 Execution Manager (子进程、租约续租)
│       └── w6_control.py  W6 Local Control (CLI REPL)
│
├── executor/          ← 👤 Person C 负责
│   ├── run.py            C2 子进程入口 (task_type input output)
│   └── workload/
│       ├── c3_validation.py  C3 结果验证与有效工作量统计
│       └── example_handler.py 新 workload 实现模板
│
├── console/           ← 👤 Person C 负责
│   └── index.html        Team Console (提交任务/查看进度/Worker状态/η_pool)
│
├── requirements.txt
└── README.md
```

---

## 并行开发规则（无冲突合并）

| 规则 | 说明 |
|------|------|
| **目录隔离** | 每人只改自己目录内的文件；不改别人目录 |
| **shared/ 变更** | 修改 `shared/contracts.py` 须三人同意，单独 commit 推 main，各人 rebase 后再继续 |
| **接口先于实现** | M1 API stub 先上线（返回 501），B/C 才能联调；C 的 executor 子进程接口已固定 |
| **分支命名** | `feat/A-<name>` / `feat/B-<name>` / `feat/C-<name>` |
| **合并顺序** | shared → master (A) → worker (B) → executor+console (C) |

---

## 快速启动

### 1. 安装依赖（三台机器各跑一次）

```bash
pip install -r requirements.txt
```

### 2. 启动 Master（Person A 的机器）

```bash
cd f:/dormpool/dormpool
set DORMPOOL_TOKEN=yourtoken   # Windows
uvicorn master.main:app --host 0.0.0.0 --port 8000
```

打开 `http://<master-ip>:8000/console` 查看 Team Console。

### 3. 启动 Worker（每台从机）

```bash
cd f:/dormpool/dormpool
set MASTER_URL=http://<master-ip>:8000
set DORMPOOL_TOKEN=yourtoken
set WORKER_ID=worker-A       # 每台不同
set MAX_SLOTS=2
python worker/main.py
```

Worker 本地 REPL 命令：`status` / `pause` / `resume` / `quit`

---

## 实现顺序（T1 → T7）

| 阶段 | 目标 | 验收 |
|------|------|------|
| T1 | Person C: 实现 executor/workload 真实计算 | 单机单任务出正确结果 |
| T2 | Person A: M1/M2 注册+心跳; Person B: W1/W2 连接 | 两台 Worker 出现在 /workers |
| T3 | Person A: M3/M4 调度; Person B: W2/W5 领取执行 | 两台设备各执行不同 Task |
| T4 | Person A: M5 结果聚合; Console 显示下载链接 | 一个 Job 完整返回结果 |
| T5 | Person B: W4/W6 暂停恢复; Person A: M2 状态 | 热加入、暂停、恢复演示 |
| T6 | Person A: M4 租约超时重试 | 断连节点任务被重新调度 |
| T7 | Person A: M6 指标; Console 显示 η_pool / S | 单机/多机对照数据输出 |

---

## 效率指标定义（来自题目 PDF）

```
η_pool = P_pool / Σ P_i × 100%
S      = P_pool / max(P_1 … P_m)
```

- `P_i`：第 i 台设备独立运行时的有效任务吞吐量（相同任务类型、相同正确性标准）
- `P_pool`：统一服务入口在足够负载下的有效总吞吐量
- 失败请求和重复计算**不计入**有效吞吐

---

## 安全边界

- Worker 主动连 Master，不要求对外开放 Worker 端口
- 所有请求须携带 `Authorization: Bearer <token>`
- Executor 只执行 `TASK_REGISTRY` 中注册的预定义任务类型；不开放任意 shell
- 设备所有者通过 W6 本地 REPL 控制是否贡献算力
