"""Private local JSON Lines service. Only predefined workload handlers run here."""
from contextlib import redirect_stdout
import json
import hashlib
import importlib.metadata
from pathlib import Path
import os
import sys
import time
import traceback

PROTOCOL_VERSION = 1


def emit(message):
    print(json.dumps(message, ensure_ascii=True), flush=True)


def initialize():
    start = time.perf_counter()
    from executor.workload.image_thumb_gpu import gpu_summary
    gpu = gpu_summary()
    supported = ["image_thumb"]
    if gpu.get("available"):
        supported.append("image_thumb_gpu")
    try:
        import torch
        torch.set_num_threads(max(1, int(os.environ.get("DORMPOOL_TORCH_THREADS", "2"))))
        from executor.workload.convnext_infer import _load_model, _weights_path
        model, device, _ = _load_model()
        if device.type != "cuda":
            raise RuntimeError("resident classification requires CUDA; CPU fallback disabled")
        with torch.inference_mode():
            model(torch.zeros(1, 3, 224, 224, device=device))
        torch.cuda.synchronize(device)
        convnext = {"available": True, "device": device.type, "weights": _weights_path(),
                    "gpu_uuid": str(torch.cuda.get_device_properties(device).uuid),
                    "gpu": torch.cuda.get_device_name(device), "torch": torch.__version__}
        with open(_weights_path(), "rb") as weights:
            convnext["weights_sha256"] = hashlib.file_digest(weights, "sha256").hexdigest()
        supported.append("convnext_infer")
    except Exception as exc:
        convnext = {"available": False, "reason": str(exc)}
    implementation = hashlib.sha256()
    for path in sorted((Path(__file__).parent / "workload").glob("*.py")):
        implementation.update(path.name.encode())
        implementation.update(path.read_bytes())
    profile = {"implementation_sha256": implementation.hexdigest(), "torch_threads": os.environ.get("DORMPOOL_TORCH_THREADS", "2"),
               "weights_sha256": convnext.get("weights_sha256"), "torch": convnext.get("torch")}
    profile['libraries'] = {}
    for package in ('Pillow','numpy','torch','torchvision','cupy-cuda13x'):
        try:
            profile['libraries'][package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            pass
    return {"type": "ready", "protocol": PROTOCOL_VERSION, "pid": os.getpid(), "execution_profile": profile,
            "supported_task_types": supported, "gpu": gpu, "convnext": convnext,
            "initialization_s": round(time.perf_counter() - start, 6)}


def fatal_error(exc):
    # Broken CUDA contexts must not be reused. Ordinary bad inputs can recover.
    message = str(exc).lower()
    if isinstance(exc, MemoryError) or any(name in type(exc).__module__.lower()
                                          for name in ("cupy", "cuda", "nvrtc")):
        return True
    return isinstance(exc, RuntimeError) and any(
        name in message for name in ("cuda", "cudnn", "cublas", "nvrtc", "out of memory"))


def serve(handlers):
    with redirect_stdout(sys.stderr):
        ready = initialize()
    emit(ready)
    for line in sys.stdin:
        try:
            request = json.loads(line)
            if request.get("type") == "shutdown":
                emit({"type": "stopped", "pid": os.getpid()})
                return 0
            if request.get("type") != "execute":
                raise ValueError("expected execute request")
            identity = {key: request[key] for key in ("request_id", "task_id", "attempt_id")}
            if not all(isinstance(value, str) and value for value in identity.values()):
                raise ValueError("request identity must contain nonempty strings")
        except (ValueError, KeyError, TypeError, AttributeError) as exc:
            emit({"type": "protocol_error", "detail": str(exc)})
            return 2

        result = {"type": "result", **identity, "pid": os.getpid(), "ok": False,
                  "units_done": 0, "fatal": False}
        start = time.perf_counter()
        try:
            task_type = request["task_type"]
            if task_type not in ready["supported_task_types"]:
                raise ValueError(f"task type not available: {task_type}")
            with redirect_stdout(sys.stderr):
                units = handlers[task_type](request["input_path"], request["output_path"])
            if int(units) <= 0:
                raise ValueError("executor produced no work units")
            result.update(ok=True, units_done=int(units), detail="ok")
        except Exception as exc:
            result.update(detail=traceback.format_exc(limit=5)[-1500:], fatal=fatal_error(exc))
            print(result["detail"], file=sys.stderr, flush=True)
        result["elapsed_s"] = round(time.perf_counter() - start, 6)
        emit(result)
        if result["fatal"]:
            return 2
    return 0
