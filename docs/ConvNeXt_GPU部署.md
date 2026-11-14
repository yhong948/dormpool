# ConvNeXt GPU 全链路

本机使用 `.venv-gpu`（Python 3.14、PyTorch CUDA 13.2、torchvision 和 CuPy）。
GPU 依赖清单为 `requirements-gpu.txt`；普通 Master 使用 `requirements.txt` 即可，
无需模型或 GPU。Master 和 Worker 都需要更新本次源码，才能协商可运行任务类型。

权重放在每台推理 Worker 的 `data/convnext_small-0c510722.pth`。
可用 `DORMPOOL_CONVNEXT_WEIGHTS` 覆盖路径。默认设备为 CUDA，初始化失败会记录原因，
节点不会宣称支持 `convnext_infer`。权重不会在领取任务时从网络下载。

在仓库根目录启动（将 Master 地址换成实际地址）：

```powershell
.\scripts\start_worker.ps1 -Master http://10.4.139.66:8000 -WorkerId worker-A -MaxSlots 1
```

启动脚本优先使用 `.venv-gpu`，也可用 `-Python` 明确指定解释器。
它会设置 CUDA 设备、权重路径和工作区 CuPy 缓存；GPU 推理默认单槽位。
后台日志在 `logs/worker-worker-A.err.log`，应出现 `supported` 包含 `convnext_infer`，
以及 `ConvNeXt loaded ... device=cuda`。Worker 和执行器使用相同解释器。

在 Master 的 `/console` 选择 **convnext_infer — GPU 图像分类**，上传图片 ZIP。
`image_thumb` 仍执行 CPU 缩略图；任务类型不会自动转换。GPU 分类结果为
`predictions.json` 与 `logits/*.npy`，Master 验证后汇总为可下载的 ZIP，
不会因不同分片含同名文件而覆盖结果。

仅上报显卡名称不足以领取 GPU 任务。Worker 注册时会实际初始化后端并上报
`supported_task_types`；没有该字段的旧 Worker 只领取 `image_thumb`。
库、权重或设备设置变化后，需要重启 Worker 重新探测能力。

运行本地隔离联调：

```powershell
.\.venv-gpu\Scripts\python.exe tests\e2e_convnext.py
```

该测试默认用 `.venv` 启动临时 Master，用当前 `.venv-gpu` 解释器启动 Worker；
可传 `--master-python <解释器路径>` 覆盖 Master 的解释器。
经 HTTP 提交两个分片，验证 CUDA 日志、
结果汇总、有效工作量和指标，并停止测试进程；证据保存在 `._tmp/convnext-e2e-*`。
它验证单机链路，不代表多物理设备性能或多机加速效果。

当前推理每个 Task 启动一个子进程并加载一次权重。小分片的模型加载开销可能明显；
吞吐评测时应记录分片大小及端到端耗时。

## 本地两个终端启动

两个终端都先进入仓库根目录。终端 A：

```powershell
.\scripts\start_master.ps1 -Bind 127.0.0.1 -Port 8000 -DataDir data/master-local -ShardSize 1 -Foreground
```

终端 B：

```powershell
.\scripts\start_worker.ps1 -Master http://127.0.0.1:8000 -WorkerId worker-local-gpu -MaxSlots 1 -Foreground
```

终端 C 生成样本并提交（API 返回 `job_id`）：

```powershell
.\.venv-gpu\Scripts\python.exe executor/gen_sample.py --count 2 --out data/convnext-input.zip
curl.exe -sS -F "task_type=convnext_infer" -F "file=@data/convnext-input.zip" http://127.0.0.1:8000/jobs
```

可在 `http://127.0.0.1:8000/console` 查询 Job 状态并下载结果。
终端 B 应记录 `supported` 包含 `convnext_infer`，每个执行器应记录 `device=cuda`。
Master 不需要权重文件，也无需加载 PyTorch。
