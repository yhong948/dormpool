# worker/ — Worker Agent（本地自治服务）

**负责人：👤 Person B**

## 功能

运行在每台成员电脑上，主动连接 Master，注册设备能力，按本地贡献策略领取任务，调用本机 Executor 执行，回传结果。对应架构文档的 W1–W6 模块。Worker 不对外开放端口，所有通信由 Worker 主动发起。

## 结构

```
worker/
├── main.py            启动入口（调用 w1_runtime.main）
└── agent/
    ├── __init__.py
    ├── w1_runtime.py  W1 Agent Runtime — 生命周期、主循环、信号处理
    ├── w2_connector.py W2 Master Connector — 注册、心跳、领任务、续租、上传
    ├── w3_monitor.py  W3 Resource Monitor — CPU/GPU 采样、执行槽位计数
    ├── w4_contrib.py  W4 Contribution Controller — 暂停/恢复/资源准入、防抖动
    ├── w5_exec.py     W5 Execution Manager — 子进程启动、租约续租、结果收集
    └── w6_control.py  W6 Local Control — 本地 CLI REPL（status/pause/resume/quit）
```

## 模块依赖

```
W1 Runtime ─┬─ W2 Connector     (对外通信)
            ├─ W3 Monitor → W4 Contrib → W5 Exec  (资源→准入→执行)
            └─ W6 Control → W4                   (所有者控制)
```

W2 调用 W3/W4/W5 的状态拼装心跳；W5 调用 W2 下载输入和提交结果；W6 调用 W4 改变贡献状态。

## 启动

```bash
cd f:/dormpool/dormpool
set MASTER_URL=http://<master-ip>:8000
set DORMPOOL_TOKEN=yourtoken
set WORKER_ID=worker-A        # 每台机器不同
set MAX_SLOTS=2
python worker/main.py
```

本地 REPL 命令：`status` / `pause` / `resume` / `quit`

## 实现顺序

1. **T2**：w2_connector 的 register 和 heartbeat_loop 真正发 HTTP（配合 Person A 的 M1/M2）
2. **T3**：w1_runtime 的 `_task_loop` 接通 claim_task；w5_exec 跑通子进程
3. **T5**：w4_contrib 的暂停/恢复 + w6_control 的 REPL
4. **T6**：w5_exec 的租约续租线程 + 子进程超时处理

## 关键设计点

- **Worker-Pull 模型**：Worker 主动请求任务，Master 不主动推送。空闲时每 5s 轮询一次。
- **Executor 隔离**：每个 Task 在独立子进程中执行，崩溃不影响 Agent。
- **Graceful Pause**：暂停只停止领取新任务，已开始的任务继续完成（DRAINING → PAUSED）。
- **防抖动**：CPU 负载准入用滑动均值，避免短时波动触发频繁状态切换。
