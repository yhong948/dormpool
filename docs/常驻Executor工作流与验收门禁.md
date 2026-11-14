# 常驻 Executor 工作流与验收门禁

目标：模型由实际执行任务的常驻子进程加载、预热和复用；Agent 管理协议、租约与结果提交。
一个 Worker 管理一个串行 Executor，容量为 1；多 GPU 可以分别启动 Worker。
保留原单次执行 CLI。协议不开放 Shell 或动态代码执行。

## 工作流

1. G1：新增 JSON Lines 协议及客户端。stdout 为结构化消息，stderr 持续读取；请求和响应匹配 ID。
2. G2：由常驻子进程预热并报告能力，Agent 注册时不导入 torch/CuPy 或加载模型。
3. G3：W5 每次等待任务响应，覆盖下载、执行、上传的续租；模型跨 Task 复用。
4. G4：普通任务错误继续服务；超时、协议错误、CUDA 错误或崩溃回收进程，由 Master 有限重试。
5. G5：暂停停止新领取，在途任务完成；正常退出等待在途任务并关闭子进程，超时退出强制回收。
6. G6：运行故障注入、CPU 回归和真实 GPU 双端联调，记录证据后完成目标。

## 必须通过的门禁

| 门禁 | 判据 | 自动验证 |
|---|---|---|
| G1 协议 | 标识匹配、输出即时 flush、大量 stderr 不死锁、未知响应失败 | 客户端/协议测试 |
| G2 初始化 | ready 在预热后发出；Agent 不加载模型；失败能力不宣称可用 | 实进程探测及 GPU 日志 |
| G3 复用 | 至少 10 个 GPU Task 共用一个 PID，期间只有一次权重加载；结果和有效计数正确 | GPU HTTP 全链路 |
| G4 恢复 | 崩溃与超时使当前任务失败、旧进程被回收、后续任务用新进程；永久错误受 Master 重试上限限制 | 故障注入及 HTTP 全链路 |
| G5 自治 | 暂停期间无新领取；恢复复用模型；退出后无 Executor 遗留 | 本地控制与进程存活检查 |
| G6 回归 | CPU 图片结果仍正确，单次 CLI 可用，原领取唤醒/槽位测试通过 | CPU E2E 与单元测试 |

## 协议约束

启动使用 `executor/run.py --serve`，初始化后发送 `ready`（协议版本、PID、可用任务类型和后端信息）。
Agent 发 `execute`（请求 ID、Task/Attempt ID、固定任务类型、本地输入输出路径）。
响应 `result` 必须回显全部 ID，包含成功状态、工作量、错误及耗时。每次只允许一个在途请求。
收到 `shutdown` 或 stdin EOF 后退出。重启从全新子进程初始化，无内部任务自动重放。

任务协议失败时不得把旧输出当作成功；输出路径按 Attempt 隔离。
stderr 与 stdout 独立持续读取。读写超时、响应错配均视为进程不可信并回收。
冷启动只计入启动/恢复，不人为增加热任务等待；端到端延迟不等同于 GPU 推理时间。

## 运行记录

G1–G6 已完成。验收命令（仓库根目录）：

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -p 'test_*.py'
.\.venv\Scripts\python.exe tests/e2e_local.py --images 4 --workers 2 --port 8146
.\.venv-gpu\Scripts\python.exe tests/e2e_resident.py
```

- 单元及进程协议测试：12 项通过，包括超时、崩溃、非法 JSON、响应错配、stderr 排空、
  CUDA fatal、下载期间续租、停止在途请求、正常 drain 和槽位释放。
- CPU 回归：两个 Worker 完成四个分片，结果校验、唯一计量、指标和控制台检查通过。
- GPU 验收：10 个 Task 复用同一进程、只加载一次权重；Agent 中没有导入 torch/CuPy。
  暂停期间没有新领取，恢复不重载模型；合并结果通过验证，类别与同批大小的原单次 CLI 一致，
  logits 在 `rtol=1e-4, atol=1e-4` 范围内一致。
- 崩溃注入：当前 Attempt 失败，Master 创建第二个 Attempt，由新 PID 执行成功。
- 永久坏输入：恰好三次尝试、有效计数为零、Job 最终 failed；健康执行器不重建，后续有效任务成功。
- 真实响应超时：将任务响应上限设为 50ms，注入每图 150ms 延时；Master 恰好三次尝试后失败，
  三代执行器均被回收。生产默认无人工延时。
- 在途退出：当前任务完成并确认后，Worker 正常退出；测试发现的所有 Executor PID 均不存在。

最近一次完整 GPU 原始证据：`._tmp/resident-gates-ak4wh1cb/`，包括 `report.json`、
`PASSED.txt`、Worker/Master 日志、原始输入、结果和单次 CLI 对照输出。
测试中的 150ms 人工延时用于稳定暂停/杀进程的注入窗口，不能把该结果作为真实吞吐基准。

## 使用与边界

正常启动命令不变；升级后重启 Worker 即可使用常驻协议，Master 仍负责有限重试和唯一确认。
一个 Worker 只有一个实际执行槽位，`MaxSlots` 大于 1 会警告并按 1 注册。
首次启动和崩溃后重建仍有冷启动，后续稳定 Task 复用模型。
普通暂停保留模型以便快速恢复；退出才卸载并回收整个执行器进程树。

`DORMPOOL_EXECUTOR_STARTUP_TIMEOUT=180` 为 ready 等待上限，`DORMPOOL_TASK_TIMEOUT=90` 为任务响应上限，
`DORMPOOL_SHUTDOWN_TIMEOUT=30` 为正常退出等待在途任务的期限；超期后还有终止与回收进程的清理时间。
后台 HTTP 传输使用原有网络超时；若退出期限内网络仍未返回，退出不再等待该守护任务线程，
Executor 仍会回收，未确认 Attempt 由 Master 的租约处理。
直接强杀 Agent 无法执行其 finally；本次门禁覆盖正常退出和 Agent 主动回收执行器，
不宣称实现操作系统崩溃或断电后的进程生命周期保证。
