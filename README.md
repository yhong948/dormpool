# DormPool — 弹性团队算力池

> Campus Edge Fabric 黑客马拉松 · 方案二
> 默认工作负载：GPU 图像分类（`convnext_infer`）；也可选择 CPU/GPU 缩略图。

把宿舍/工作室里分散的设备连成一个统一的计算服务：用户提交一个 zip 包，系统自动切分、分发到多台设备并行处理，最后把结果打包还回去。

**当前状态：原型已跑通。** 单机计算、节点接入、任务分发、结果闭环、动态暂停/恢复/热加入/失联回收、性能测量七道门禁全部通过，四条可复现脚本在 `tests/` 下。

---

## 一分钟跑起来

### Windows（PowerShell）

```powershell
# 0. 依赖（切到项目根目录后执行）
pip install -r requirements.txt

# 1. 启动 Master（终端 1）
$env:DORMPOOL_DATA  = "data"
$env:SHARD_SIZE     = "1"
python -m uvicorn master.main:app --host 127.0.0.1 --port 8000

# 2. 启动 Worker A（终端 2）
$env:MASTER_URL     = "http://127.0.0.1:8000"
$env:MAX_SLOTS      = "1"
$env:WORKER_ID      = "worker-A"
$env:WORKER_DATA    = "data/worker_data/worker-A"
python worker/main.py --headless

# 3. 启动 Worker B（终端 3）
$env:WORKER_ID   = "worker-B"
$env:WORKER_DATA = "data/worker_data/worker-B"
python worker/main.py --headless

# 4. 打开界面
#    http://127.0.0.1:8000/console
```

### Ubuntu / Linux（bash）

```bash
# 0. 依赖（切到项目根目录后执行）
pip install -r requirements.txt

# 1. 启动 Master（终端 1）
export DORMPOOL_DATA=data SHARD_SIZE=1
python -m uvicorn master.main:app --host 127.0.0.1 --port 8000

# 2. 启动两个 Worker（终端 2、3）
export MASTER_URL=http://127.0.0.1:8000 MAX_SLOTS=1
# 终端 2:
WORKER_ID=worker-A WORKER_DATA=data/worker_data/worker-A python worker/main.py --headless
# 终端 3:
WORKER_ID=worker-B WORKER_DATA=data/worker_data/worker-B python worker/main.py --headless

# 3. 打开界面
#    http://127.0.0.1:8000/console
```

界面默认选择 `convnext_infer`（GPU 图像分类），上传一个含图片的 zip 即可创建 Job，完成后下载分类结果。Worker 需要 GPU 环境和本地 ConvNeXt 权重，配置见 [GPU 部署说明](docs/ConvNeXt_GPU部署.md)。

### 不开浏览器也能验证

以下命令在 Windows 和 Ubuntu 上写法相同：

```
python tests/e2e_local.py --port 8100 --workers 2 --images 9 --shard-size 1
python tests/g5_dynamic.py --port 8200 --images 12 --delay-ms 800   # 暂停/恢复/热加入/杀进程
python tests/g6_measure.py --port 8300 --images 9 --delay-ms 700    # P_i / P_pool / η_pool / S
```

每个脚本自己起 Master 和 Worker、自己清理，跑完打印证据。退出码 0 = 全通过。

---

## 目录结构

```
dormpool/
├── shared/                    🔒 冻结契约：数据模型、状态枚举、REST 路径、时序常量
│   └── contracts.py
│
├── master/                    中心协调服务（M1–M6）
│   ├── main.py                入口
│   ├── README.md
│   └── app/
│       ├── m1_api.py          M1 API Gateway：路由、请求编排、组合根
│       ├── m2_registry.py     M2 Worker Registry：注册、心跳、三维状态、离线判定
│       ├── m3_job.py          M3 Job Manager：创建、分片、进度、终态、账本
│       ├── m4_scheduler.py    M4 Task Scheduler：原子分配、租约、校验提交、回收
│       ├── m5_data.py         M5 Data & Result：分片落盘、结果聚合、路径防护
│       ├── m6_metrics.py      M6 Metrics：P_i、P_pool、η_pool、S、节点事件
│       └── c3_validation.py   C3 权威结果校验
│
├── worker/                    本地资源代理（W1–W6）
│   ├── main.py                入口
│   ├── README.md
│   └── agent/
│       ├── w1_runtime.py      W1 生命周期、配置、任务主循环
│       ├── w2_connector.py    W2 HTTP 客户端：注册/心跳/领取/下载/上传/续租/提交
│       ├── w3_monitor.py      W3 真实硬件嗅探 + 槽位 + 利用率 + 算力微基准
│       ├── w4_contrib.py      W4 贡献策略：暂停/恢复/准入/防抖动
│       ├── w5_exec.py         W5 子进程执行、租约续租、结果解析
│       └── w6_control.py      W6 本地控制：REPL / stdin / 127.0.0.1 API
│
├── executor/                  任务执行器（C2–C3）
│   ├── run.py                 子进程入口 + TASK_REGISTRY
│   ├── gen_sample.py          确定性测试样本生成
│   ├── benchmark.py           单机吞吐基准 P_i
│   ├── README.md
│   └── workload/
│       ├── image_thumb.py     C2 真实计算：128px 缩略图 + SHA-256 manifest
│       └── example_handler.py 新工作负载模板
│
├── console/                   统一界面
│   ├── index.html             单文件 SPA
│   └── README.md
│
├── tests/                     可复现验证脚本
│   ├── e2e_local.py           端到端：分片→双 Worker→唯一领取→结果闭环→指标
│   ├── g5_dynamic.py          暂停/恢复/热加入/杀进程，含不双计断言
│   └── g6_measure.py          单机与池化两次独立测量
│
├── docs/
│   ├── 阶段规划书.md           S0–S5 六阶段
│   ├── 验收门禁.md             G0–G6 七道门禁，含真实命令与判据
│   └── 进度日志.md             逐步记录：做了什么、跑了什么、修了什么
│
├── requirements.txt
└── README.md
```

---

## 系统如何工作

```
用户 ──zip──> Master
                │  M3 按 SHARD_SIZE 切成 shard_0.zip…shard_N.zip
                │  M4 等 Worker 来领
                ▼
        ┌───────┴───────┐
   Worker A           Worker B          ← 各自主动 claim，不开放端口
   W5 起子进程         W5 起子进程
   executor/run.py    executor/run.py
   128px 缩略图        128px 缩略图
        │                 │
        └──结果 zip────────┘
                │
        M4 校验 + 唯一确认（迟到的丢弃）
        M5 按 manifest 去重聚合
                ▼
           result.zip ──> 用户下载
```

### 三个关键设计

**1. Worker 主动领取，Master 不推送。**
Worker 不需要开放任何网络端口，天然穿过 NAT 和防火墙；空闲才来要活，因此也不需要复杂的过载控制。

**2. Job / Task / Attempt 三级模型。**
一次用户提交是 `Job`，切成多个独立的 `Task`，每个 Task 在某台机器上的每次真实执行是 `Attempt`。节点失联后 Task 可以被重新分配，产生第二个 Attempt，但**有效工作量只按最终确认的那一个 Attempt 计一次**。这是"不重复计量"的制度基础。

**3. 三维 Worker 状态。**
连接（online/offline）、贡献（available/paused/draining）、执行（idle/busy）互相独立。所以"人已经按了暂停，但手上的活还在干"能被准确表达为 `online + draining + busy`，而不是含糊的"离线"。

---

## 性能指标

题目定义：

```
η_pool = P_pool / Σ P_i × 100%        聚合效率
S      = P_pool / max(P_i)            相对最强单机的加速比
```

**P_pool 必须独立测量**——用池化作业的墙钟（`result_ready_at - created_at`），而不是把各设备吞吐加起来。P_i 只能来自相同任务类型、输入指纹、分片大小、校验标准和运行配置下的独立“单机基准”作业；没有匹配基准时 η_pool 和 S 显示为空，不伪造比较结果。代码里对 P_pool 的来源保留显式标记（`p_pool_source == "pool_wall_clock"`）。

本机实测（同机双进程，`delay=700ms`，9 张图）：

| 场景 | 墙钟 | 吞吐 | S | η_pool |
|------|------|------|---|--------|
| 单 Worker | 46.91s | 0.192 u/s | 0.91 | 90.8% |
| 双 Worker | 24.18s | 0.372 u/s | 1.54 | 83.4% |

单 Worker 时 S≈1.0，这正是"P_pool 不是 ΣP_i"的证据。

**诚实说明**：以上是同一台机器上两个进程的结果，证明了调度、隔离、故障恢复有效，但**不构成**跨物理机的加速证据——两者共享同一颗 CPU。真实多机需要在两台设备上各跑一个 Worker 指向同一 Master。

---

## 动态行为

| 场景 | 行为 | 观测 |
|------|------|------|
| 所有者暂停 | 立即停止领取；在跑的任务跑完；`draining` → `paused`；`free_slots` 上报 0 | 进度最多 +1 |
| 所有者恢复 | 立即补发心跳；恢复领取 | 进度继续增长 |
| 新节点加入 | 中途加入即分到剩余任务 | 实测新节点完成 5 个单位 |
| 节点失联 | ≤45s 判离线；手上任务放回队列且**不消耗重试次数**；由其他节点完成 | 实测 42s |
| 迟到结果 | Attempt 已非 RUNNING → 丢弃，不覆盖已确认结果 | 返回 `discarded: true` |

---

## 配置

### Master

| 变量 | 默认 | 说明 |
|------|------|------|
| `DORMPOOL_DATA` | `data` | 数据根目录 |
| `SHARD_SIZE` | `3` | 每个 Task 处理几张图 |

### Worker

| 变量 | 默认 | 说明 |
|------|------|------|
| `MASTER_URL` | `http://127.0.0.1:8000` | Master 地址 |
| `WORKER_ID` | 主机名 + uuid 短码 | 固定后重连被识别为同一节点 |
| `MAX_SLOTS` | `2` | 并发任务上限 |
| `WORKER_DATA` | `worker_data` | 本地目录，每个 Worker 必须不同 |
| `WORKER_CONTROL_PORT` | `0` | 开启本机 `/status` `/pause` `/resume` |
| `WORKER_NO_REPL` | `0` | `1` 跳过交互式 REPL |
| `DORMPOOL_EXECUTOR_DELAY_MS` | `0` | 每张图后额外 sleep，仅拉长时间窗口，不改输出字节 |

---

## 算力嗅探

Worker 注册时上报的是**这台机器此刻的真实硬件**，不是写死的值。级联探测：

1. `pynvml`（NVML）→ GPU 名称 + 显存精确值
2. Windows PowerShell `Get-CimInstance Win32_VideoController` → 名称 + AdapterRAM（>4GB 会截断，且会给虚拟适配器排序靠后）
3. 都没有 → `(None, None)`，不报错

另外跑一个 20 万次 SHA-256 的微基准测本机算力，结果编码进 `executor_version`（格式 `"1|<score>"`）。

本机实测：

```
cpu=16 ram=31.78GB gpu=NVIDIA GeForce RTX 4060 Laptop GPU score=1|1252217.207
```

---

## 安全边界

- Worker 只主动出站连接 Master，不开放任何入站端口
- 所有接口直接访问，无需凭证。
- Executor 只执行 `TASK_REGISTRY` 里注册的预定义类型，**没有**任意代码执行入口
- `/data/{ref}` 拒绝绝对路径与 `..`，任何 ref 都无法逃出数据根目录
- 所有进程只监听 `127.0.0.1`，不绑 `0.0.0.0`
- 数据校验在 Master 侧权威执行：重算哈希、比对尺寸，不信任 Worker 自报的数字

---

## 进一步阅读

| 文档 | 内容 |
|------|------|
| [docs/阶段规划书.md](docs/阶段规划书.md) | S0–S5 六阶段、模块归属、风险登记、同机模拟的诚实边界 |
| [docs/验收门禁.md](docs/验收门禁.md) | G0–G6 每道门禁的命令、判据、证据清单、常见红灯 |
| [docs/进度日志.md](docs/进度日志.md) | 真实开发记录：每个阶段跑了什么、8 个联调缺陷的根因与修复 |
| [shared/README.md](shared/README.md) | 冻结契约逐项说明 |
| [master/README.md](master/README.md) | M1–M6 职责、请求流程、接口表、关键语义 |
| [worker/README.md](worker/README.md) | W1–W6 职责、嗅探级联、暂停语义、执行时序 |
| [executor/README.md](executor/README.md) | 子进程契约、工作负载规格、确定性规则、校验清单 |
| [console/README.md](console/README.md) | 界面分区、轮询周期、降级行为 |

---

## 已知局限

1. **同机模拟**：双 Worker 共享一颗 CPU，聚合效率受单机上限约束。真实多机需在两台设备上各跑一个 Worker。
2. **无持久化**：Master 重启后内存中的 Job/Worker 状态丢失（磁盘数据仍在）。
3. **单 Master**：无高可用，符合方案二既定边界。
4. **延时开关**：动态实验中的部分数字在 `DORMPOOL_EXECUTOR_DELAY_MS>0` 下测得，目的是让单任务时长足以观察暂停/热加入；真实吞吐以 `delay=0` 的基准为准。
