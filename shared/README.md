# shared/ — 共享契约（FROZEN）

**维护规则：🔒 三人共同维护。修改 `contracts.py` 须三人同意，单独 commit 推 main，其他人 rebase 后再继续。不绑定固定分支名，各人自行命名开发分支。**

## 功能

存放整个系统跨模块共享的数据模型、接口定义和常量。所有 Master / Worker / Executor 之间的"语言"都在这里定义，避免各人各自实现导致接口不一致。

## 结构

```
shared/
├── __init__.py
└── contracts.py      ← 唯一需要关注的文件
```

### contracts.py 包含

| 内容 | 说明 |
|------|------|
| 枚举类 | `JobStatus` / `TaskStatus` / `AttemptStatus` / `WorkerConnState` / `WorkerContribState` / `WorkerExecState` |
| 数据模型 (C1) | `WorkerCapability` / `WorkerHeartbeat` / `Job` / `Task` / `Attempt` / `MetricRecord`，均为 `@dataclass` |
| REST 接口路径 | `API` 类列出 Master 暴露的所有 HTTP 端点（Worker 按此调用） |
| 子进程契约 | Worker→Executor 的命令行格式与退出码（`EXECUTOR_SUCCESS` / `EXECUTOR_TASK_FAIL` / `EXECUTOR_FATAL`） |
| 时序常量 | `HEARTBEAT_INTERVAL` / `LEASE_DURATION` / `LEASE_RENEW_BEFORE` / `WORKER_TIMEOUT` / `MASTER_PORT` |

## 使用方式

```python
from shared.contracts import Job, Task, Attempt, API, LEASE_DURATION
```

Master 用 `API` 类实现路由；Worker 用 `API` 类拼请求 URL；Executor 用退出码约定。三方都不应硬编码端点路径或状态字符串。
