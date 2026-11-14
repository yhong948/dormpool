# console/ — Team Console（统一用户界面）

**负责人：👤 Person C · 分支：`feat/C-executor-console`**

## 功能

面向任务使用者和团队管理者的单页 Web 界面。直接调用 Master REST API，提供任务提交、进度查看、Worker 状态监控、结果下载和性能指标展示。

## 结构

```
console/
└── index.html    单文件 SPA（HTML + CSS + JS，无构建步骤）
```

无框架依赖，原生 `fetch` 调用 Master API。Master 通过 `GET /console` 返回此文件。

## 页面分区

| 区域 | 对应 API | 功能 |
|------|---------|------|
| Submit Job | `POST /jobs` | 选择任务类型 + 上传输入文件 |
| Jobs 列表 | `GET /jobs` · `GET /jobs/{id}` | 显示 Job 进度和状态，完成后提供下载链接 |
| Workers 列表 | `GET /workers` | 显示每台 Worker 的连接/贡献/执行状态、CPU/GPU 利用率 |
| Pool Metrics | `GET /metrics/summary` | 显示 η_pool、加速比 S、各设备吞吐量 |

## 接入方式

1. Master 启动后访问 `http://<master-ip>:8000/console`
2. 在浏览器 Console 中设置 token（如需）：`localStorage.setItem("dormpool_token", "yourtoken")`
3. 修改 [index.html](index.html) 中的 `MASTER_URL` 为实际 Master 地址（或留空使用相对路径，由 Master 同源服务）

## 实现顺序

1. **T2**：连上 Master 后先接通 `GET /workers`，确认节点注册成功
2. **T4**：接通 `POST /jobs` 和结果下载链接
3. **T7**：接通 `GET /metrics/summary`，展示 η_pool 和加速比 S

## 设计说明

单文件无构建链路是为了 18h 开发周期内可快速迭代。如后续需要框架化（React/Vue），可在 `console/` 下引入构建工具，但 `index.html` 作为入口保持不变。
