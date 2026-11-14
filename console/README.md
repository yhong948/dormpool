# console/ — 统一用户界面

**负责人：Person C · 阶段：S4 · 对应门禁：G4（UI 部分）/ G6（指标展示）**

## 功能

单文件 Web 页面，由 Master 在 `GET /console` 提供。任务使用者在这里提交作业、看进度、下载结果；团队管理者在这里看节点状态和池化指标。无构建步骤、无外部 CDN——离线场地也能用。

## 结构

```
console/
├── index.html     单文件 SPA（HTML + CSS + 原生 JS）
└── README.md
```

## 页面分区

| 分区 | 数据来源 | 轮询 | 内容 |
|------|---------|------|------|
| 提交任务 | `POST /jobs` | — | 任务类型下拉 + zip 文件选择，返回 job_id 与任务数 |
| 任务列表 | `GET /jobs` | 3s | Job ID、类型、进度条（done/total）、状态徽章、完成后下载链接 |
| 节点 | `GET /workers` | 3s | ID、主机、连接/贡献/执行三态、槽位、CPU%、GPU%、真实能力 |
| 池化指标 | `GET /metrics/summary` | 5s | η_pool、加速比 S、P_pool、成功任务数、各设备吞吐条形图 |
| 节点事件 | 同上 `node_events` | 5s | 最近 20 条 join/pause/resume/timeout，按时间倒序 |

## 配置

- **API 基地址**：默认 `window.location.origin`（即由 Master 同源提供）。也可用查询参数覆盖并持久化：
  `http://127.0.0.1:8000/console?master=http://127.0.0.1:8000`
- **Token**：`localStorage.setItem("dormpool_token", "dev-token")`，请求时带 `Authorization: Bearer`

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
| 页面能开但全空 | token 与 Master 的 `DORMPOOL_TOKEN` 不一致 → 401 | 在浏览器 localStorage 里设置同一个 token |
| 打开是静态文件但请求失败 | 用 `file://` 打开，跨域到 Master | 通过 `http://<master>/console` 访问 |
