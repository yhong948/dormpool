# src/console/ — 统一用户界面

**负责人：Person C · 阶段：S4 · 对应门禁：G4（UI 部分）/ G6（指标展示）**

## 功能

单文件 Web 页面，由 Master 在 `GET /console` 提供。任务使用者在这里提交作业、看进度、下载结果；团队管理者在这里看节点状态和池化指标。无构建步骤、无外部 CDN——离线场地也能用。

## 结构

```
src/console/
├── index.html     单文件 SPA（HTML + CSS + 原生 JS）
└── README.md
```

## 页面分区

| 分区 | 数据来源 | 轮询 | 内容 |
|------|---------|------|------|
| 提交任务 | `POST /jobs` | — | 任务类型 + zip 文件选择；Master 根据可用设备自动选择单机基准或池化测试 |
| 任务列表 | `GET /jobs` | 1.5s | Job ID、类型、进度条（done/total）、状态徽章、下载、暂停/恢复和删除 |
| 节点 | `GET /workers` | 1s | ID、主机、连接/贡献/执行三态、槽位、CPU%、GPU%、真实能力 |
| 池化指标 | `GET /metrics/summary` | 5s | η_pool、加速比 S、P_pool、成功任务数、各设备吞吐条形图 |
| 节点事件 | 同上 `node_events` | 5s | 最近 20 条 join/pause/resume/timeout，按时间倒序 |

## 配置

任务操作：`POST /jobs/{id}/pause` 暂停后续分片分配，正在执行的分片正常完成并保留进度；
`POST /jobs/{id}/resume` 恢复分配。最后一个在途分片完成时，作业仍可正常进入完成状态。
已完成或失败的任务不可暂停/恢复，接口返回 409。
`DELETE /jobs/{id}` 移除列表记录及 Master 上该作业的输入和结果文件，页面操作前会确认。
已下发分片会自然结束，迟到上传和提交不再写回该作业。历史成功工作量指标保留。
这些操作仅影响所选作业，其他作业和 Worker 继续运行。

指标测量必须分两步：先让只有一台设备处于 available，使用相同任务类型、输入 ZIP、分片大小和校验标准运行一次；
Master 会自动记录“单机基准（P_i）”。然后恢复其他设备并再次提交同一个 ZIP，Master 自动记录“池化测试（P_pool）”。
只有匹配的独立基准全部存在时，页面才计算 η_pool 和 S；否则显示 `—` 并在指标提示中说明缺少基准。
这样单机与多机比较不会把同一轮池化任务的执行时间当成单机基准。
基准记录持久化在 Master 数据目录的 `metrics/measurements.json`，重启 Master 后仍可用于比较。

- **API 基地址**：默认 `window.location.origin`（即由 Master 同源提供）。也可用查询参数覆盖并持久化：
  `http://127.0.0.1:8000/console?master=http://127.0.0.1:8000`

## 状态徽章语义

| 徽章 | 含义 |
|------|------|
| 绿 `online` / `available` / `completed` | 正常 |
| 黄 `draining` / `paused` / `running` | 过渡或进行中 |
| 红 `offline` / `failed` | 异常 |
| 灰 `idle` / `queued` | 空闲或等待 |

颜色之外每个徽章都带文字，不依赖颜色传达信息。

## 降级行为

- 任一接口失败：顶部出现"与 Master 的连接中断，正在重试…"横幅，页面不清空、不抛未处理异常
- 标签页切到后台：暂停所有轮询；切回时立即刷新并恢复
- 指标为空：显示 `—` 而不是 `NaN` 或 `null`
- 结果未就绪：下载列显示 `—`

## 常见问题

| 现象 | 原因 | 处置 |
|------|------|------|
| `/console` 404 | Master 从错误 cwd 解析相对路径 | Master 用 `Path(__file__).resolve().parents[2] / "console/index.html"` |
| 打开是静态文件但请求失败 | 用 `file://` 打开，跨域到 Master | 通过 `http://<master>/console` 访问 |
