# DormPool 部署到学校 AI+ 应用社区

本文对应入口：<https://gpunion.hkust-gz.edu.cn/community>。社区应用运行在
Docker Web 容器中，DormPool 的 Master 提供网页和 HTTP API；真正执行图像识别的
Worker 继续运行在带 GPU 的电脑上。

## 一、准备代码

在 GitHub 上确认代码已经推送到目标分支，然后在社区工作台创建开发沙盒。通过
WebSSH 或 SSH 进入沙盒，把项目放到平台要求的应用目录：

```bash
mkdir -p /mydata/apps/dormpool
cd /mydata/apps/dormpool
git clone -b yhong2 https://github.com/yhong948/dormpool.git .
```

如果平台不能访问 GitHub，也可以在本地打包后上传项目文件。必须同时上传
`Dockerfile`、`requirements.txt`、`src/` 和 `start/`；`models/*.pth` 被 Git 忽略，
不需要放进 Master 镜像，模型由 GPU Worker 单独保存。

## 二、构建并运行 Master

项目根目录已经包含适配社区的 `Dockerfile`。它安装 CPU 侧 API 依赖，监听平台要求的
`0.0.0.0:3000`，并把 Master 数据写入容器的 `/app/data`：

```bash
cd /mydata/apps/dormpool
docker build -t dormpool-master:latest .
docker rm -f dormpool-master 2>/dev/null || true
docker run -d --name dormpool-master \
  -p 3000:3000 \
  -v /mydata/apps/dormpool/runtime-data:/app/data \
  dormpool-master:latest
```

先在沙盒内检查服务：

```bash
curl -fsS http://127.0.0.1:3000/ping
curl -fsS http://127.0.0.1:3000/console | head
```

然后在社区的“我的应用”中选择这个镜像或运行实例，填写应用说明和责任确认，
按平台流程发布并等待审核。发布后平台会给出应用地址，形式通常为：

```text
https://<社区域名>/apps/dormpool/
```

网页会自动识别 `/apps/dormpool` 子路径。首次访问也可以显式指定 Master 地址：

```text
https://<社区域名>/apps/dormpool/?master=/apps/dormpool
```

如果浏览器打开页面但 API 请求失败，检查“应用路径”和 `master` 参数是否一致，
以及反向代理是否把 `/apps/dormpool/` 转发到容器的 3000 端口。

## 三、在 GPU 电脑上启动 Worker

每台 GPU 电脑都需要完整的 Python GPU 环境、模型文件和项目代码。模型放在项目根目录
的 `models/convnext_small-0c510722.pth`；也可以用环境变量指定绝对路径。

Windows PowerShell：

```powershell
cd F:\dormpool\dormpool
$env:DORMPOOL_CONVNEXT_WEIGHTS = (Resolve-Path .\models\convnext_small-0c510722.pth)
.\start\start_worker.ps1 `
  -Master https://<社区域名>/apps/dormpool `
  -WorkerId gpu-01 -MaxSlots 1 -Foreground
```

Linux：

```bash
cd /path/to/dormpool
export PYTHONPATH="$(dirname "$PWD")"
export MASTER_URL="https://<社区域名>/apps/dormpool"
export WORKER_ID="gpu-01"
export WORKER_DATA="$PWD/data/worker_data/$WORKER_ID"
export DORMPOOL_CONVNEXT_WEIGHTS="$PWD/models/convnext_small-0c510722.pth"
python src/worker/main.py --headless
```

启动后在网页的设备列表确认 Worker 为在线，并确认支持
`convnext_infer`。不同电脑使用不同 `WorkerId`，不要共用同一个 Worker 数据目录。

## 四、提交一次图像识别任务

可以直接使用网页上传 ZIP。命令行示例：

```bash
curl -F task_type=convnext_infer \
     -F file=@images.zip \
     https://<社区域名>/apps/dormpool/jobs
```

结果和指标分别通过以下接口查看：

```text
GET /apps/dormpool/jobs
GET /apps/dormpool/jobs/<job_id>
GET /apps/dormpool/jobs/<job_id>/result
GET /apps/dormpool/metrics/summary
```

单机和多机都会记录每台 Worker 的 `p_i`、处理时间、`p_pool`，并汇总池化效率
`η_pool` 和加速比 `S`。正式对比时必须使用相同输入、任务类型、分片大小、模型权重
和正确性版本；本地多个 Worker 共用一台物理机只能作为调度/容错测试，不能宣称真实
多机加速。

## 五、常见问题

* `Python environment missing`：先在项目目录创建 `.venv`，或用 `-Python` 指定现有
  Python。启动脚本现在位于 `start/`，不是 `scripts/`。
* Worker 一直 `queued`：检查 Worker 是否在线、是否声明了任务类型，以及 Master URL
  是否带正确的 `/apps/dormpool` 前缀。
* ConvNeXt 不可用：检查 `torch`、CUDA 驱动和模型绝对路径；模型缺失时 Worker 仍可
  注册，但不会领取 `convnext_infer` 任务。
* 公开发布前请限制应用可见范围或在反向代理增加访问令牌和限流。当前 Master API
  本身没有用户认证，公开 URL 不应承载敏感数据。

## 六、本地发布前检查

在推送或构建镜像前，从仓库父目录运行：

```powershell
$env:PYTHONPATH = (Get-Location).Path
python -m pytest -q dormpool/src/tests
python -m dormpool.src.tests.e2e_local --port 8100 --workers 2 --images 7 --shard-size 1
python -m dormpool.src.tests.g5_dynamic --port 8200
```

端到端测试会自动清理临时 Master 和 Worker。GPU ConvNeXt 测试还需要本机 CUDA、
torchvision 和模型文件；没有这些条件时应明确标记为环境跳过，不要把 CPU 缩略图测试
当作 GPU 识别测试结果。
