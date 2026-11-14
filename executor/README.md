# executor/ — Task Executor（计算执行器）

**负责人：👤 Person C**

## 功能

封装实际业务计算为标准化执行器。由 Worker Agent 通过子进程方式调用，接收任务输入，执行预定义计算，输出可验证结果。对应架构文档的 C2 Workload Adapter 和 C3 Validation & Accounting。

## 结构

```
executor/
├── run.py                    子进程入口（命令行契约见 shared/contracts.py）
└── workload/
    ├── __init__.py
    ├── c3_validation.py      C3 结果验证与有效工作量统计
    └── example_handler.py    新 workload 实现模板（复制后改名使用）
```

## 子进程契约

```bash
python executor/run.py <task_type> <input_path> <output_path> [--worker-id X]
```

| 退出码 | 含义 |
|--------|------|
| `0` | 成功，结果写入 `<output_path>` |
| `1` | 任务级失败（输入错误、计算异常），无输出 |
| `2` | 执行器致命错误，不应在本节点重试 |

stdout 最后一行必须是 JSON：`{"units_done": <int>, "detail": "..."}`

## 添加新 workload

1. 复制 [example_handler.py](workload/example_handler.py)，改名为 `<task_type>.py`
2. 实现 `run(input_path, output_path) -> int`，返回有效工作量
3. 在 [run.py](run.py) 的 `TASK_REGISTRY` 注册：
   ```python
   from executor.workload.image_resize import run as image_resize_run
   TASK_REGISTRY = {"image_resize": image_resize_run}
   ```
4. （可选）在 [c3_validation.py](workload/c3_validation.py) 的 `VALIDATORS` 注册对应的验证器

## 实现顺序

1. **T1**：选定真实计算任务，实现 handler，单机跑通 `python executor/run.py <type> <in> <out>` 出正确结果
2. **T3**：与 Person B 联调，确认 W5 能正确启动子进程并解析 stdout
3. **T4**：实现 c3_validation 的正确性校验，确保 Master 能验证结果

## 安全边界

- 只执行 `TASK_REGISTRY` 中注册的预定义任务类型，不开放任意 shell 执行
- 输入来自 Master 分发，Executor 不直接访问网络
- 计算结果应确定性可验证（相同输入 → 相同输出），便于 Master 校验
