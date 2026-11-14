# executor/ — 任务执行器

**负责人：Person C · 阶段：S1 · 对应门禁：G1**

## 功能

把"业务计算"封装成 Worker 可以用子进程调用的标准组件。执行器完全不知道 Master 的存在，只认三个参数：任务类型、输入文件、输出文件。

当前实现一种工作负载：**`image_thumb` — 批量缩略图**。

## 结构

```
executor/
├── run.py                      子进程入口 + TASK_REGISTRY
├── gen_sample.py               生成确定性测试 zip
├── benchmark.py                单机吞吐基准（P_i）
├── README.md
└── workload/
    ├── __init__.py
    ├── image_thumb.py          C2 真实计算实现
    └── example_handler.py      新工作负载模板
```

## 子进程契约

```bash
python executor/run.py <task_type> <input_path> <output_path> [--worker-id X]
```

| 退出码 | 含义 | Worker 行为 |
|--------|------|------------|
| `0` | 成功，结果在 `output_path` | 上传并提交 |
| `1` | 任务级失败（坏输入/未知类型/计算异常） | 提交失败，Master 可重试到别处 |
| `2` | 致命错误 | 提交失败，本节点不再重试 |

stdout 的**最后一行**必须是 JSON：

```json
{"units_done": 12, "detail": "ok"}
```

诊断信息一律写 stderr，不要污染 stdout——Worker 从后往前找第一个可解析的 JSON 行，中间行写什么都不影响。

## image_thumb 工作负载规格

### 输入

一个平铺 zip，内含图片（`.png/.jpg/.jpeg/.bmp/.gif/.webp/.tif`）。Master 会把它切成 `shard_<i>.zip`，每个分片就是一个 Task 的输入。

### 输出

```
out.zip
├── thumbs/<stem>.png      每张图一个，最长边 ≤128
└── manifest.json          条目数组，按 input_name 排序
```

manifest 每条字段固定六个：

```json
{
  "input_name":    "img_000.png",
  "input_sha256":  "<64 hex>",
  "output_name":   "thumbs/img_000.png",
  "output_sha256": "<64 hex>",
  "out_w": 128,
  "out_h": 96
}
```

`units_done = len(manifest)` = 处理的图片张数。

### 确定性（硬性要求）

同一份输入必须产出**逐字节相同**的输出。实现上必须同时满足：

| 要求 | 做法 | 不做的后果 |
|------|------|-----------|
| 成员顺序稳定 | `sorted(zipfile.namelist())` | 每次 zip 内部顺序不同 |
| 不写源 mtime | `ZipInfo(date_time=(1980,1,1,0,0,0))` | `ZipFile.write()` 会把文件时间写进归档，跨时区不一致 |
| 不写 PNG 时间块 | `save(..., pnginfo=None)` 且不带 EXIF | 归档里出现 `tIME` 块，输出字节随运行时刻变化 |
| manifest 键序稳定 | `json.dumps(..., sort_keys=True)` | 字典序不确定 |
| 固定文件权限 | `external_attr = 0o600 << 16`，`create_system=0` | 不同系统写出不同元数据 |

实测：4 次运行（含换时区 `TZ=Asia/Shanghai`、含 `DORMPOOL_EXECUTOR_DELAY_MS=50`）sha256 全部相同。

### 延时开关

`DORMPOOL_EXECUTOR_DELAY_MS`（默认 0）在**每张图处理完之后** sleep 该毫秒数。它只拉长执行时间，让动态节点实验有操作窗口，**不得影响输出字节**——门禁会对比开/关延时后的 sha256。

## 结果校验（C3）

权威校验器在 [master/app/c3_validation.py](../master/app/c3_validation.py)，由 Master 在提交结果时执行。它**不信任执行器自报的任何数字**，全部重算：

| 检查 | 作用 |
|------|------|
| `manifest.json` 存在且是合法 JSON 数组 | 拒绝空壳输出 |
| 每条恰好六个键、类型正确 | 拒绝字段增删 |
| 输出文件在归档内且是能解码的 PNG | 拒绝伪造路径 |
| 实际尺寸 ≤128×128 | 拒绝未缩放的图 |
| **记录的 out_w/out_h 等于真实尺寸** | 拦住被篡改的 manifest |
| 重算 `output_sha256` | 拦住被替换的图 |
| `output_name` / `input_name` 不重复 | 拦住重复计量 |
| 源分片可达时重算 `input_sha256` 并比对集合 | 拦住漏图/换图 |

实测：把 manifest 里一条 `out_w` 改成 999，校验器立即抛 `ValidationError: manifest[0] dimensions out of range: 999x96 (max 128)`。

## 添加新工作负载

1. 复制 `workload/example_handler.py` → `workload/<new_type>.py`
2. 实现 `run(input_path: str, output_path: str) -> int`，返回有效工作量
3. 在 `run.py` 的 `TASK_REGISTRY` 里注册
4. 在 `master/app/c3_validation.py` 的 `VALIDATORS` 里注册对应的校验器（没有校验器时 Master 会拒绝该类型，这是有意为之）

## 常用命令

```bash
# 生成 12 张确定性测试图
python executor/gen_sample.py --count 12 --out _tmp/sample12.zip

# 跑一个分片
python executor/run.py image_thumb _tmp/sample12.zip _tmp/out.zip --worker-id test

# 单机吞吐基准（P_i）
python executor/benchmark.py --task-type image_thumb --input _tmp/sample12.zip --repeat 3
```

`benchmark.py` 输出：

```json
{"task_type":"image_thumb","total_tasks":3,"units_done":36,"images_per_shard":12,
 "elapsed_s":0.0672,"throughput":535.71,"per_run":[...]}
```

`units_done == total_tasks × images_per_shard` 是自检条件。

## 安全边界

- 只执行 `TASK_REGISTRY` 里注册的类型，未注册一律退出 1；不提供任意代码执行入口
- 不访问网络、不读写 `output_path` 以外的路径
- 不 import torch 或任何重型框架；依赖只有 Pillow 与标准库
