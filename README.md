# DormPool — 团队算力池

> Campus Edge Fabric 黑客马拉松 · 评委复现指南

把宿舍/实验室里分散的设备连成一个统一计算服务：用户提交一个 zip，系统自动切分并分发到多台设备并行处理，最后把结果打包还回去。

---

## 目录

1. [项目介绍](#1-项目介绍)
2. [系统架构](#2-系统架构)
3. [技术栈](#3-技术栈)
4. [目录结构](#4-目录结构)
5. [环境准备](#5-环境准备)
6. [安装依赖](#6-安装依赖)
7. [Windows 一键启动](#7-windows-一键启动)
8. [Linux 命令行启动](#8-linux-命令行启动)
9. [自动化测试与验证](#9-自动化测试与验证)
10. [Console 界面操作](#10-console-界面操作)
11. [性能指标说明](#11-性能指标说明)
12. [安全边界](#12-安全边界)

---

## 1. 项目介绍

DormPool 是一个轻量级分布式算力池，面向高校实验室、课程项目团队和设计工作室。核心理念是：**用身边已经开着的设备，组成一个数据不出门、随时能停、结果可逐条核验的团队算力服务。**

### 解决的问题

| 场景 | 痛点 | DormPool 的解法 |
|------|------|----------------|
| 实验室批量图像推理 | 单机跑一晚跑不完；云 GPU 要申请预算 | 把组里闲置机联成池 |
| 课程项目批处理 | 学生没有云预算 | 每人贡献一台笔记本 |
| 摄影工作室格式转换 | 设计师的机器白天要用 | 晚上闲置时自动贡献 |

### 本次演示的工作负载

- **`convnext_infer`** — GPU 图像分类（ConvNeXt-Small，ImageNet-1k 1000 类）
- **`image_thumb`** — CPU 批量缩略图（128px，含 SHA-256 manifest，无 GPU 也能跑）
- **`image_thumb_gpu`** — GPU 加速缩略图

---

## 2. 系统架构

```
用户浏览器
    │  POST /jobs  (zip 上传)
    ▼
┌─────────────────────────────────────────────┐
│                   Master                    │
│  M1 API Gateway  ←→  M3 Job Manager         │
│  M2 Worker Registry  ←→  M4 Task Scheduler  │
│  M5 Data & Result    ←→  M6 Metrics         │
└──────────────┬──────────────────────────────┘
               │  HTTP (Worker 主动出站)
       ┌───────┴────────┐
  Worker A          Worker B          ...更多节点
  W1 生命周期        W1 生命周期
  W2 HTTP 客户端     W2 HTTP 客户端
  W3 硬件嗅探        W3 硬件嗅探
  W5 子进程执行      W5 子进程执行
       │                   │
  executor/run.py    executor/run.py
  (image_thumb /     (image_thumb /
   convnext_infer)    convnext_infer)
       │                   │
       └────────┬──────────┘
                │  POST /workers/{id}/commit
                ▼
          Master 聚合结果
                │
                ▼
          GET /jobs/{id}/result  →  用户下载 result.zip
```

### 三个关键设计决策

**1. Worker 主动拉取（Pull），Master 不推送。**
Worker 不开放任何入站端口，天然穿越 NAT 和校园防火墙。空闲时才来领活，无需复杂的过载控制。

**2. Job / Task / Attempt 三级模型。**
一次用户提交是 `Job`，自动切分为多个独立 `Task`，每个 Task 在某台机器上的一次真实执行是 `Attempt`。节点失联后 Task 被重新分配，产生新 Attempt，但**有效工作量只按最终确认的那一个计一次**，不会重复计量。

**3. 三维 Worker 状态。**
连接（online/offline）、贡献（available/paused/draining）、执行（idle/busy）互相独立。设备所有者按下暂停后，系统进入 `draining`（收尾手上的活）→ `paused`（完全空闲），不会强行中断正在运行的任务。

---

## 3. 技术栈

| 层 | 技术 | 说明 |
|----|------|------|
| Master 服务框架 | FastAPI + uvicorn | 异步 HTTP，`APIRouter` 分模块挂载 |
| Worker HTTP 客户端 | requests | 指数退避重试，连接/读超时分离 |
| 并发控制 | threading.Lock / threading.Event | 原子任务分配、租约续租线程 |
| 硬件嗅探 | psutil + pynvml / torch.cuda | 三级降级：NVML → PyTorch → 默认值 |
| GPU 推理 | PyTorch + torchvision (ConvNeXt-Small) | CUDA 推理，模型权重本地加载 |
| 图像处理 | Pillow | 缩略图生成，确定性输出 |
| 结果校验 | SHA-256 manifest | 每张图逐条核验，改任意字节即失败 |
| 进程隔离 | subprocess.Popen | Executor 以子进程运行，崩溃不影响 Agent |
| 前端 Console | 原生 HTML/JS（无框架） | 单文件 SPA，setInterval 轮询，无构建步骤 |
| 测试框架 | Python unittest / 自定义 e2e 脚本 | 退出码 0 = 全通过，可在 CI 中运行 |

---

## 4. 目录结构

```
dormpool/
├── src/shared/                 冻结契约：数据模型、状态枚举、REST 路径、时序常量
│   └── contracts.py
│
├── src/master/                 中心协调服务
│   ├── main.py             uvicorn 入口，/health, /console 路由
│   └── app/
│       ├── m1_api.py       API Gateway：路由注册、请求编排
│       ├── m2_registry.py  Worker Registry：注册、心跳、三维状态、离线判定
│       ├── m3_job.py       Job Manager：创建、分片、进度、终态
│       ├── m4_scheduler.py Task Scheduler：原子分配、租约、提交确认
│       ├── m5_data.py      Data & Result：分片落盘、聚合、路径防护
│       ├── m6_metrics.py   Metrics：P_i、P_pool、η_pool、S、节点事件
│       └── c3_validation.py 权威结果校验（重算哈希）
│
├── src/worker/                 本地资源代理
│   ├── main.py             Worker 入口
│   └── agent/
│       ├── w1_runtime.py   生命周期、配置、任务主循环
│       ├── w2_connector.py HTTP 客户端：注册/心跳/领取/下载/上传/提交
│       ├── w3_monitor.py   硬件嗅探、槽位计数、利用率采样
│       ├── w4_contrib.py   贡献策略：暂停/恢复/准入/防抖
│       ├── w5_exec.py      子进程执行、租约续租、结果解析
│       └── w6_control.py   本地控制 REPL / loopback API
│
├── src/executor/               任务执行器（子进程入口）
│   ├── run.py              TASK_REGISTRY 分发
│   ├── benchmark.py        单机吞吐基准 P_i
│   ├── gen_sample.py       确定性测试样本生成
│   └── src/executor/workload/
│       ├── image_thumb.py  128px 缩略图 + SHA-256 manifest
│       └── example_handler.py  新工作负载模板
│
├── src/console/
│   └── index.html          单文件 SPA 控制台
│
├── src/tests/                  可复现验证脚本（退出码 0 = 全通过）
│   ├── e2e_local.py        端到端：分片→双 Worker→唯一领取→结果闭环→指标
│   ├── g5_dynamic.py       暂停/恢复/热加入/杀进程，含不双计断言
│   └── g6_measure.py       单机与池化两次独立吞吐测量
│
├── start/                Windows PowerShell 启动脚本
│   ├── start_master.ps1
│   ├── start_worker.ps1
│   └── stop.ps1
│
├── docs/                   设计文档与进度日志
├── requirements.txt        CPU 依赖（无 GPU 也可运行）
└── requirements-gpu.txt    GPU 依赖（CUDA 13.2 + PyTorch 2.15）
```

---

## 5. 环境准备

### 最低要求

| 项 | 要求 |
|----|------|
| Python | 3.11 或 3.12（推荐 3.12） |
| 操作系统 | Windows 10/11 或 Linux（Ubuntu 20.04+） |
| 节点数量 | 至少 2 台设备（或同机双 Worker 进程验证调度逻辑） |
| 网络 | 同一局域网，或 Worker 能出站访问 Master 的 IP:Port |
| GPU（可选） | NVIDIA GPU，CUDA 13.x 驱动，用于 `convnext_infer` |

### 多机网络说明

- Master 机器需要固定的局域网 IP（如 `192.168.1.10`）
- Worker 机器只需要能访问 Master 的 IP 和端口（默认 8000）
- **Worker 不需要开放任何入站端口**
- 如果 Master 在 Windows 上，需要在 Windows 防火墙中放行入站 TCP 8000：

```powershell
# 在 Master 机器上以管理员身份执行（一次性）
New-NetFirewallRule -DisplayName "DormPool Master" -Direction Inbound `
  -Protocol TCP -LocalPort 8000 -Action Allow
```

---

## 6. 安装依赖

> 以下命令在项目根目录（`dormpool/` 下，包含 `requirements.txt` 的那一层）执行。

### 仅 CPU（无 GPU，运行 `image_thumb`）

```bash
# 建议先创建虚拟环境
python -m venv .venv

# Windows 激活
.venv\Scripts\activate

# Linux 激活
source .venv/bin/activate

# 安装
pip install -r requirements.txt
```

### GPU（运行 `convnext_infer`，需要 NVIDIA CUDA 13.x）

```bash
python -m venv .venv-gpu

# Windows 激活
.venv-gpu\Scripts\activate

# Linux 激活
source .venv-gpu/bin/activate

# 安装（会下载约 2.5GB 的 PyTorch CUDA 包，请在网络良好时执行）
pip install -r requirements-gpu.txt
```

### 验证安装

```bash
python -c "import fastapi, uvicorn, requests, psutil, PIL; print('CPU deps OK')"

# GPU 额外验证
python -c "import torch; print('CUDA available:', torch.cuda.is_available())"
```

### ConvNeXt 模型权重

`convnext_infer` 工作负载需要模型权重文件，放置路径：

```
dormpool/models/convnext_small-0c510722.pth
```

Worker 不会在任务执行时下载模型。文件缺失时，Worker 仍可注册并执行 CPU 工作负载，
但会拒绝 `convnext_infer` 任务；请在启动前将权重放到上述路径，或设置
`DORMPOOL_CONVNEXT_WEIGHTS` 指向绝对路径。

---

## 7. Windows 一键启动

所有脚本均在 `start/` 目录下，使用 PowerShell 5.1+。在项目根目录打开 PowerShell 窗口执行。

Worker 启动命令均显式指定 Master 地址。请将 `你的master ip` 替换为实际地址：单机使用 `127.0.0.1`；多机使用 Master 监听的局域网 IP，并按实际情况调整端口。

> **注意**：如果提示"脚本执行被禁止"，以管理员身份执行一次：
> ```powershell
> Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
> ```

### 场景 A：单机双 Worker（验证调度逻辑，无需第二台设备）

```powershell
# 终端 1：启动 Master（前台运行，Ctrl+C 停止）
.\start\start_master.ps1 -Bind "127.0.0.1" -Foreground

# 终端 2：启动 Worker A
.\start\start_worker.ps1 -Master "http://你的master ip:8000" -WorkerId "worker-A" -MaxSlots 10 -Foreground

# 终端 3：启动 Worker B
.\start\start_worker.ps1 -Master "http://你的master ip:8000" -WorkerId "worker-B" -MaxSlots 10 -Foreground

# 打开浏览器
start http://127.0.0.1:8000/console
```

### 场景 B：真实多机（两台设备，推荐）

**在 Master 机器上（将 `192.168.1.10` 替换为本机实际局域网 IP）：**

```powershell
.\start\start_master.ps1 -Bind "192.168.1.10" -Foreground
# 启动后输出类似：
#   Master  : http://192.168.1.10:8000
#   Console : http://192.168.1.10:8000/console
```

**在每台 Worker 机器上（替换 Master IP 和 Worker ID）：**

```powershell
# 第一台 Worker 机器
.\start\start_worker.ps1 -Master "http://你的master ip:8000" -WorkerId "worker-A" -MaxSlots 10 -Foreground

# 第二台 Worker 机器
.\start\start_worker.ps1 -Master "http://你的master ip:8000" -WorkerId "worker-B" -MaxSlots 10 -Foreground
```

### 启动参数速查

**start_master.ps1**

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-Bind` | 自动检测局域网 IP | Master 监听地址，不允许 `0.0.0.0` |
| `-Port` | `8000` | 监听端口 |
| `-ShardSize` | `50` | 每个 Task 处理几张图 |
| `-Foreground` | 否 | 在当前窗口前台运行（Ctrl+C 停止） |
| `-KillIfBusy` | 否 | 端口被占用时自动结束占用进程 |

**start_worker.ps1**

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-Master` | `http://127.0.0.1:8000` | Master 地址 |
| `-WorkerId` | `主机名-随机4字符` | 节点 ID，重连时保持相同 ID |
| `-MaxSlots` | `100` | 最大并发任务数 |
| `-DelayMs` | `0` | 每张图后额外 sleep（用于演示暂停/热加入） |
| `-ControlPort` | `0` | 开启本机 HTTP 控制 API（`/status` `/pause` `/resume`） |
| `-Foreground` | 否 | 前台运行，可使用交互式 REPL |

### 停止所有进程

```powershell
# 停止全部
.\start\stop.ps1

# 只停止某个 Worker
.\start\stop.ps1 -What worker -WorkerId worker-A

# 只停止 Master
.\start\stop.ps1 -What master
```

---

## 8. Linux 命令行启动

以下 Worker 命令中的 `你的master ip` 也需替换为实际地址：单机使用 `127.0.0.1`，多机使用 Master 监听的局域网 IP。

### 场景 A：单机双 Worker

```bash
# 终端 1：启动 Master
export DORMPOOL_DATA=data
export SHARD_SIZE=50
export MASTER_BIND=127.0.0.1
python -m uvicorn src.master.main:app --host 127.0.0.1 --port 8000 &
echo $! > logs/master.pid
sleep 2 && curl -s http://127.0.0.1:8000/ping   # 应返回 {"ok":true,...}

# 终端 2：启动 Worker A
export WORKER_ID=worker-A
export WORKER_DATA=src/data/worker_data/worker-A
export MAX_SLOTS=4
mkdir -p $WORKER_DATA
python src/worker/main.py --master "http://你的master ip:8000" --headless &
echo $! > logs/worker-A.pid

# 终端 3：启动 Worker B
export WORKER_ID=worker-B
export WORKER_DATA=src/data/worker_data/worker-B
export MAX_SLOTS=4
mkdir -p $WORKER_DATA
python src/worker/main.py --master "http://你的master ip:8000" --headless &
echo $! > logs/worker-B.pid
```

### 场景 B：真实多机

**Master 机器（替换 `192.168.1.10` 为实际 IP）：**

```bash
mkdir -p logs data
export DORMPOOL_DATA=data
export SHARD_SIZE=50
export MASTER_BIND=192.168.1.10
python -m uvicorn src.master.main:app --host 192.168.1.10 --port 8000 &
echo $! > logs/master.pid
```

**每台 Worker 机器：**

```bash
# 在各自机器上执行，WORKER_ID 保持唯一
mkdir -p logs src/data/worker_data/worker-A
export WORKER_ID=worker-A
export WORKER_DATA=src/data/worker_data/worker-A
export MAX_SLOTS=4
python src/worker/main.py --master "http://你的master ip:8000" --headless &
echo $! > logs/worker-A.pid
```

### 停止（Linux）

```bash
# 读取 pid 文件停止
kill $(cat logs/master.pid) 2>/dev/null
kill $(cat logs/worker-A.pid) 2>/dev/null
kill $(cat logs/worker-B.pid) 2>/dev/null
```

### 防火墙（如需跨机器访问，在 Master 机器上执行）

```bash
# Ubuntu / Debian（ufw）
sudo ufw allow 8000/tcp

# CentOS / RHEL（firewalld）
sudo firewall-cmd --add-port=8000/tcp --permanent && sudo firewall-cmd --reload
```

---

## 9. 自动化测试与验证

所有测试脚本自己起 Master 和 Worker、自己清理，退出码 0 = 全通过。**不需要提前启动任何服务。**

### G1 · 端到端基础流程

验证：分片 → 双 Worker 领取 → 唯一确认 → 结果闭环 → 指标一致。

```bash
python src/tests/e2e_local.py --port 8100 --workers 2 --images 9 --shard-size 1
```

预期输出（末尾）：
```
e2e_local ALL PASSED
```

### G5 · 动态自治

验证：暂停/恢复语义、热加入、节点失联后任务回收、不双计。

```bash
python src/tests/g5_dynamic.py --port 8200 --images 12 --delay-ms 800
```

预期输出（末尾）：
```
G5 ALL PASSED
{"G5-A": "draining->paused, done 1->2", "G5-B": "...", "G5-C": "worker-B did 5 units", "G5-D": "offline in 42s, 12 units, no double count"}
```

### G6 · 性能测量

验证：单机基准 P_i 和池化 P_pool 分两次独立测量，计算 η_pool 和 S。

```bash
python src/tests/g6_measure.py --port 8300 --images 9 --delay-ms 700
```

预期输出（末尾）：
```
G6 PASSED
single: p=0.19 u/s   pool: p=0.37 u/s   eta=83.4%   S=1.54
```

### 单机基准（`src/executor/benchmark.py`）

在每台设备上单独运行，记录该设备的 P_i：

```bash
# 生成测试样本（如果 datasets/ 为空）
python src/executor/gen_sample.py --count 20 --out src/datasets/sample.zip

# 运行基准（image_thumb，CPU）
python src/executor/benchmark.py --task-type image_thumb --input src/datasets/sample.zip --tasks 10

# 运行基准（convnext_infer，GPU）
python src/executor/benchmark.py --task-type convnext_infer --input src/datasets/sample.zip --tasks 5
```

输出示例：
```
task_type    : image_thumb
total_tasks  : 10
units_done   : 90
elapsed_s    : 4.73
throughput   : 19.03 units/s   ← 这是 P_i
```

---

## 10. Console 界面操作

Master 启动后，浏览器打开 `http://<Master-IP>:8000/console`。

### 提交任务

1. 选择任务类型（`image_thumb` 无需 GPU，适合演示）
2. 点击"输入文件"，上传一个包含 `.jpg` / `.png` 图片的 zip 文件
3. 点击"↑ 提交任务"
4. 页面自动刷新，可以看到 Job ID、分片数量、进度条

### 查看节点状态

**节点**面板显示每台 Worker 的：
- 连接状态（online / offline）
- 贡献状态（available / paused / draining）
- 执行状态（idle / busy）
- CPU% / GPU% 实时采样值
- 硬件配置（CPU 核数、内存、GPU 型号）

### 下载结果

Job 完成后，状态变为绿色 `completed`，点击"下载"获取 `result.zip`，内含：
- 每张图对应的缩略图（`image_thumb`）或分类结果 JSON（`convnext_infer`）
- `manifest.json`：每条记录包含输入文件名、SHA-256、处理结果

### Worker 本地控制（前台模式）

用 `-Foreground` 启动 Worker 后，终端出现交互式 REPL：

```
dormpool> status        # 查看当前状态
dormpool> pause         # 暂停接收新任务（已在跑的会完成）
dormpool> resume        # 恢复
dormpool> quit          # 退出
```

---

## 11. 性能指标说明

赛题定义的两个核心指标：

```
η_pool = P_pool / Σ P_i × 100%     聚合效率
S      = P_pool / max(P_i)          相对最强单机的加速比
```

**P_i**：每台设备在单独承担相同输入时的有效任务吞吐量（已验证图片数 ÷ 墙钟时间）。  
**P_pool**：所有设备在池化状态下，从 Job 提交到结果就绪的端到端吞吐量（同样用墙钟时间，不是各 P_i 相加）。

Console 的"测评汇总"面板实时显示这两个指标。没有独立基线数据时显示"—"，不伪造比较结果。

### 关键规则

- P_pool **必须独立测量**，不能用 ΣP_i 代替
- 失败或重复的 Attempt 不计入有效工作量
- η_pool > 100% 时系统自动提示复核
- 每台设备的处理记录绑定 Job ID、输入 SHA-256 和时间窗口，可逐条核验

---

## 12. 安全边界

| 条目 | 说明 |
|------|------|
| Worker 不开放入站端口 | 只主动出站连接 Master，天然穿越 NAT |
| 无任意代码执行 | Executor 只运行 `TASK_REGISTRY` 注册的预定义任务类型 |
| 路径防护 | `/data/{ref}` 拒绝绝对路径和 `..`，无法逃出数据根目录 |
| 拒绝通配符绑定 | Master 明确拒绝 `0.0.0.0`，必须指定具体 IP |
| 权威校验在 Master 侧 | 重算 SHA-256，不信任 Worker 自报的数字 |
| 所有进程只监听本机必要接口 | 不绑定不需要的网络接口 |

---

## 常见问题

**Q: Worker 启动后立即退出，日志说"Cannot reach Master"**  
A: 检查 Master 是否已启动，以及 `MASTER_URL` 是否填写了正确的 IP 和端口。跨机器时确认防火墙已放行 TCP 8000。

**Q: `convnext_infer` 任务一直 PENDING**  
A: 该任务类型需要 GPU Worker。确认 Worker 机器安装了 `requirements-gpu.txt`，且 `torch.cuda.is_available()` 返回 `True`。

**Q: η_pool 低于预期（低于 70%）**  
A: 常见原因：分片太小（调度开销占比高）、GPU 模型首次推理有加载延迟（需预热）、Worker 网络延迟大。尝试增大 `ShardSize`（`-ShardSize 20`）或先跑一次预热 Job。

**Q: 端口 8000 被占用**  
A: Windows 上运行 `.\start\start_master.ps1 -KillIfBusy` 自动清理；或手动 `Stop-Process -Id <PID> -Force`。

**Q: 评委想现场停掉一台 Worker 测试容错**  
A: 直接关闭 Worker 窗口或运行 `.\start\stop.ps1 -What worker -WorkerId worker-A`。Master 在约 2–45 秒内检测到离线，将该节点手上的 Task 重新放回队列，由其他 Worker 领取，Console 节点事件面板会显示 `timeout` 事件。

