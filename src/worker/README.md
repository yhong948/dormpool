# src/worker/ — 本地资源代理

**负责人：Person B · 阶段：S1–S5 · 对应门禁：G2 / G3 / G5**

任务领取不设固定间隔：有空闲槽位就连续领取，执行完成释放槽位后立即唤醒领取循环。
Master 暂无任务时每 0.25 秒再查询，网络错误时保留退避；暂停期间不领取新任务。

GPU 环境通过 `requirements-gpu.txt` 安装 `nvidia-ml-py`（导入名为 `pynvml`）。
Agent 按实际执行器报告的 UUID/PCI 选择设备，每秒在后台读取一次整卡 GPU 繁忙度与显存；
心跳传送缓存样本、时间、来源和错误，不在请求路径运行阻塞探测。
缺少采样包或驱动不支持时保留 null 并上报原因，0% 才表示真实读到的零值。安装后需重启 Worker。

## 功能

每台成员电脑上运行一个 Agent。它主动连接 Master（不开放任何监听端口给网络），上报本机真实硬件能力，按所有者意愿决定是否领取任务，下载分片、起子进程算、传回结果，并在整个过程中保护本机的日常使用。

## 结构

```
src/worker/
├── main.py              入口（插入仓库根到 sys.path 后调用 W1）
├── README.md
└── agent/
    ├── __init__.py
    ├── w1_runtime.py    W1 生命周期、配置、任务主循环、状态变更即时心跳
    ├── w2_connector.py  W2 HTTP 客户端：注册/心跳/领取/下载/上传/续租/提交
    ├── w3_monitor.py    W3 真实硬件嗅探 + 槽位计数 + 利用率采样 + 算力微基准
    ├── w4_contrib.py    W4 贡献策略：手动暂停/恢复、资源准入、防抖动
    ├── w5_exec.py       W5 子进程执行、租约续租线程、结果解析与回传
    └── w6_control.py    W6 本地控制：REPL / stdin 命令 / 127.0.0.1 控制 API
```

## 启动顺序

```
加载配置（环境变量 + 参数）
  ↓
GET /ping 探活，失败则指数退避重试（2→4→8…→60s，不退出进程）
  ↓
注册：上报 W3 嗅探到的真实能力
  ↓
启动心跳线程（每 10s）
  ↓
启动控制面（按配置三选一）→ 阻塞
  ↓
任务主循环
```

## 配置

| 环境变量 | 默认 | 说明 |
|---------|------|------|
| `MASTER_URL` | `http://127.0.0.1:8000` | Master 地址 |
| `WORKER_ID` | 主机名 + uuid 短码 | 固定后重连会被识别为同一节点 |
| `MAX_SLOTS` | `2` | 并发任务上限 |
| `WORKER_DATA` | `worker_data` | 本地工作目录（每个 Worker 必须不同） |
| `WORKER_CONTROL_PORT` | `0`（关闭） | 开启后在本机端口提供 `/status` `/pause` `/resume` |
| `WORKER_NO_REPL` | `0` | `1` 跳过交互式 REPL（CI / 门禁必须开） |
| `DORMPOOL_EXECUTOR_DELAY_MS` | `0` | 每处理一张图后额外 sleep，仅用于拉长任务窗口，不影响输出字节 |

命令行：`python src/worker/main.py [--headless] [--master URL] [--worker-id ID] [--max-slots N] [--control-port P]`

## 算力嗅探（W3）

这是本模块最需要如实实现的部分：**上报的必须是这台机器此刻的真实硬件**，不允许写死。

### 探测级联

| 层 | 手段 | 取到的值 | 失败时 |
|----|------|---------|--------|
| 1 | `pynvml`（NVML） | GPU 名称 + 显存精确值 | 降级到层 2 |
| 2 | Windows PowerShell `Get-CimInstance Win32_VideoController` | GPU 名称 + AdapterRAM（uint32，>4GB 会截断） | 降级到层 3 |
| 3 | — | `(None, None)` | 无 GPU，不报错 |

层 2 会按「厂商关键字 → 有显存 → 非虚拟适配器 → 显存大小」给候选排序，避免把虚拟显示适配器当成显卡。

CPU 与内存走 `psutil`：`cpu_count(logical=False)`（物理核）、`virtual_memory().total`。

### 运行时采样

- `cpu_pct()` → `psutil.cpu_percent(interval=0.3)`
- `gpu_pct()` → NVML `nvmlDeviceGetUtilizationRates().gpu`，无卡返回 `None`

### 算力微基准

`compute_score()` 跑 20 万次 `sha256(1KiB)` 并测吞吐（ops/s），结果缓存。它被编码进 `WorkerCapability.executor_version`，格式为 `"1|<score>"`——这是复用已有字段，不扩展冻结契约。

本机实测输出：

```
cpu=16 ram=31.78GB gpu=NVIDIA GeForce RTX 4060 Laptop GPU score=1|1252217.207
```

## 暂停语义（W4）

| 状态 | 含义 | free_slots 上报 |
|------|------|----------------|
| `available` | 可领取新任务 | 真实空闲槽位 |
| `draining` | 已暂停，但手上还有任务在跑 | 0 |
| `paused` | 已暂停且空闲 | 0 |

关键约束：

1. **顺序必须是 draining → paused**，不能直接跳 paused。
2. 暂停只停**领取**，不杀在跑的任务。
3. 暂停状态下 `free_slots` 上报 0——不要一边拒绝领取一边宣称有空槽。
4. 状态变化时**立即补发一次心跳**，否则 Master 最多滞后 10 秒才看到。

## 一次任务的完整执行（W5）

```
claim 成功
  ↓ 新线程
同步 slot_acquire()，启动覆盖下载/执行/上传的续租线程
  ↓
GET  /data/{input_ref}          下载到 WORKER_DATA/<task_id>/<attempt_id>/input.zip
  ↓
向常驻 Executor 发送 execute 请求（Task/Attempt/Request ID 与输入输出路径）
  ↓ 每 Worker 一个串行执行器，模型预加载并复用
等待匹配 ID 的 result 响应（默认 90 秒上限）
  ↓
校验响应身份及成功状态 → units_done
  ↓
POST /data/upload/{job_id}/{task_id}      上传结果 zip，拿到 output_ref
  ↓
POST /attempts/{id}/commit {output_ref, units_done, error_msg}
  ↓ finally
slot_release()
```

异常隔离：普通输入错误不重建模型；超时、协议错配、进程崩溃或 CUDA 错误会回收执行器进程树，
当前 Attempt 按失败提交，下一个 Attempt 重建并预热。重试次数仍由 Master 控制，Worker 不内部重放任务。
有效执行容量为 1；退出先停止领取，等待在途任务至期限后回收进程，详情见 [工作流与验收门禁](../docs/常驻Executor工作流与验收门禁.md)。

## 本地控制（W6）

三种入口，最终都调用同一组 W4 方法：

```bash
# 1. 交互式
dormpool> status | pause | resume | quit

# 2. stdin 行命令（无 tty 时自动启用）
# Ubuntu/Linux：
echo pause > /proc/<pid>/fd/0
# Windows（PowerShell）：向进程 stdin 发送命令，或用 --control-file 文件方式触发

# 3. 本机 HTTP（WORKER_CONTROL_PORT 开启时，Windows 和 Linux 相同）
curl 127.0.0.1:8122/status
curl -X POST 127.0.0.1:8122/pause
curl -X POST 127.0.0.1:8122/resume
```

## 通信细节（W2）

| 项 | 值 |
|----|----|
| 超时 | connect 5s / read 30s / commit 60s |
| 重试 | 5xx、超时、连接失败 → 2,4,8,16,32,60s 退避，最多 3 次 |
| **不重试** | 4xx（请求本身就是错的，重发无意义） |
| Content-Type | JSON 请求逐次设置；**不在 Session 上设全局头**，否则 multipart 上传会丢 boundary 导致 422 |
| 心跳循环 | 捕获所有异常，永不退出进程 |

## 常见问题

| 现象 | 原因 | 处置 |
|------|------|------|
| `ModuleNotFoundError: worker` | 直接运行 `src/worker/main.py` 时 sys.path[0] 是 worker/ | 入口已插入仓库根 |
| 结果上传 422 | Session 全局 `Content-Type: application/json` 覆盖 multipart | 已修复：JSON 头逐请求设置 |
| Worker 起来即退出 | 非 tty 环境下 REPL 的 `input()` 立即 EOF | 用 `--headless` 或 `WORKER_NO_REPL=1` |
| `gpu_name` 是 `"None"` 字符串 | 序列化时 `str(None)` | 保持 `Optional[str]`，JSON 里是 `null` |
| 暂停后 Master 仍派活 | 心跳滞后 | 状态变化后立即补发心跳（已实现） |
