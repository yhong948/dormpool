# master/ — Master Server（中心协调服务）

**负责人：👤 Person A**

## 功能

部署在团队可访问的主机上，提供统一任务入口，管理所有 Worker 的注册与状态、全局任务队列、结果汇总、基础监控与指标。对应架构文档的 M1–M6 模块。

## 结构

```
master/
├── main.py            启动入口（uvicorn master.main:app）
└── app/
    ├── __init__.py
    ├── m1_api.py      M1 API Gateway — FastAPI 路由、鉴权、请求路由
    ├── m2_registry.py M2 Worker Registry — 节点登记、心跳、失联检测、重连
    ├── m3_job.py      M3 Job Manager — Job 创建、拆分、进度追踪、终态判定
    ├── m4_scheduler.py M4 Task Scheduler — 任务分配、能力匹配、租约、超时重试
    ├── m5_data.py     M5 Data & Result Manager — 输入存储、分片分发、结果聚合
    └── m6_metrics.py  M6 Metrics Collector — 有效吞吐、η_pool、S、节点事件
```

## 模块依赖

```
M1 API → M3 Job → M4 Scheduler → M5 Data
M2 Registry → M4 Scheduler        (提供可用 Worker)
M6 Metrics ← 横跨 M2/M3/M4       (采集执行记录)
```

任务主链：`M1 → M3 → M4 → M5`；资源链：`M2 → M4`；观测链：`M6` 横跨各模块。

## 启动

```bash
cd f:/dormpool/dormpool
set DORMPOOL_TOKEN=yourtoken
uvicorn master.main:app --host 0.0.0.0 --port 8000
```

打开 `http://<master-ip>:8000/console` 查看 Team Console。

## 实现顺序

1. **T2**：m1_api 的 register / heartbeat 路由接通 m2_registry（目前返回 501）
2. **T3**：m3_job 的 `create_job` 拆分逻辑 + m4_scheduler 的 `assign_task`
3. **T4**：m5_data 的上传/下载/聚合 + m1_api 的 commit_result 路由
4. **T6**：m4_scheduler 的租约超时后台线程
5. **T7**：m6_metrics 的 `/metrics/summary` 聚合计算
