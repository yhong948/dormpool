"""
executor/workload/convnext_infer.py  —  ConvNeXt-Small inference workload
=========================================================================
Owner: Person C.

Why this workload exists
------------------------
image_thumb turned out to be GPU-negative: at 128 px the cost is dominated by
JPEG decode plus PNG encode, both of which stay on the CPU, so a 128px resize
moves too few FLOPs to pay for the transfers. ConvNeXt-Small does ~8.68 GFLOPs
per 224x224 image, which is the compute/move ratio a GPU actually wins on.

Contract
--------
  input  : zip containing images (same extensions as image_thumb)
  output : zip containing
             predictions.json   [{"input_name","input_sha256","top1_index",
                                  "top1_label","top1_score","logits_sha256"}, ...]
             logits/<stem>.npy  float32[1000] raw logits per image
  units_done = number of images classified

Determinism
-----------
Per-device determinism only. A different GPU (or CPU vs GPU) produces
different logits in the last digits, so the validator never compares logits
across machines — it recomputes the SHA-256 of whatever logits file this node
produced and checks the shape and score ranges. That is the same principle
image_thumb_gpu uses: verify integrity, not cross-implementation equality.

Weights
-------
Loaded from DORMPOOL_CONVNEXT_WEIGHTS (default models/convnext_small-0c510722.pth).
The file is the official Torchvision checkpoint, so the architecture is built
with convnext_small(weights=None) and the state dict loaded from disk — never
downloaded at task time, which would make an offline demo hang.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import time
import zipfile
from typing import Dict, List, Optional, Tuple

PRED_NAME = "predictions.json"
LOGIT_DIR = "logits/"
INPUT_SIZE = (224, 224)

WEIGHTS_ENV = "DORMPOOL_CONVNEXT_WEIGHTS"
DEFAULT_WEIGHTS = "models/convnext_small-0c510722.pth"
DEVICE_ENV = "DORMPOOL_CONVNEXT_DEVICE"      # auto | cpu | cuda
DELAY_ENV = "DORMPOOL_EXECUTOR_DELAY_MS"

# Same fixed-zip rule as the other workloads (G1 determinism).
FIXED_DATE_TIME = (1980, 1, 1, 0, 0, 0)
IMAGE_EXTS = (".png", ".jpg", ".jpeg", ".bmp", ".gif", ".webp", ".tif", ".tiff")

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TorchUnavailable(RuntimeError):
    """Raised when this node has no usable torch/CUDA stack."""


# ── Lazy model loading ───────────────────────────────────────────────────────

_model = None
_device = None
_categories: Optional[List[str]] = None


def _weights_path() -> str:
    raw = os.environ.get(WEIGHTS_ENV) or DEFAULT_WEIGHTS
    return raw if os.path.isabs(raw) else os.path.join(REPO_ROOT, raw)


def _load_model():
    """Build ConvNeXt-Small once per process and load local weights."""
    global _model, _device, _categories
    if _model is not None:
        return _model, _device, _categories

    try:
        import torch
        from torchvision.models import convnext_small
    except Exception as exc:
        raise TorchUnavailable(f"torch/torchvision unavailable: {exc}") from exc

    path = _weights_path()
    if not os.path.isfile(path):
        raise TorchUnavailable(
            f"ConvNeXt weights not found at {path!r}; set {WEIGHTS_ENV} or place "
            f"the official convnext_small-0c510722.pth there"
        )

    want = (os.environ.get(DEVICE_ENV) or "auto").strip().lower()
    if want == "cuda" and not torch.cuda.is_available():
        raise TorchUnavailable("DORMPOOL_CONVNEXT_DEVICE=cuda but CUDA is not available")
    if want == "cpu":
        device = torch.device("cpu")
    elif want == "cuda":
        device = torch.device("cuda")
    else:
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    model = convnext_small(weights=None)          # never downloads
    state = torch.load(path, map_location="cpu", weights_only=True)
    model.load_state_dict(state)
    model = model.to(device).eval()

    try:
        from torchvision.models import ConvNeXt_Small_Weights
        _categories = list(ConvNeXt_Small_Weights.IMAGENET1K_V1.meta["categories"])
    except Exception:
        _categories = None

    _model, _device = model, device
    return _model, _device, _categories


def backend_summary() -> dict:
    """Describe the inference backend without raising. Used for logs/README."""
    try:
        model, device, _ = _load_model()
        info = {"available": True, "device": device.type,
                "weights": _weights_path()}
        if device.type == "cuda":
            import torch
            info["gpu"] = torch.cuda.get_device_name(0)
            info["torch"] = torch.__version__
        return info
    except Exception as exc:
        return {"available": False, "reason": str(exc)}


# ── Preprocessing ────────────────────────────────────────────────────────────

def _preprocess(im):
    """Shortest side -> 232, centre-crop 224, tensor in [0,1], ImageNet norm.

    Done by hand rather than through weights.transforms() so the exact numbers
    are visible here and the same on every node.
    """
    from PIL import Image
    import numpy as np

    short, long_ = 232, 224
    w, h = im.size
    if w <= h:
        new_w, new_h = short, max(short, int(round(h * short / w)))
    else:
        new_h, new_w = short, max(short, int(round(w * short / h)))
    im = im.resize((new_w, new_h), Image.BILINEAR)

    left = (new_w - long_) // 2
    top = (new_h - long_) // 2
    im = im.crop((left, top, left + long_, top + long_))

    arr = np.asarray(im, dtype=np.float32) / 255.0
    mean = np.array([0.485, 0.456, 0.406], dtype=np.float32)
    std = np.array([0.229, 0.224, 0.225], dtype=np.float32)
    arr = (arr - mean) / std
    return arr.transpose(2, 0, 1)                 # HWC -> CHW


def _delay_seconds() -> float:
    try:
        return max(0.0, float(os.environ.get(DELAY_ENV, "0")) / 1000.0)
    except (TypeError, ValueError):
        return 0.0


# ── Workload entry point ─────────────────────────────────────────────────────

def run(input_path: str, output_path: str) -> int:
    import torch

    model, device, categories = _load_model()
    delay = _delay_seconds()

    with zipfile.ZipFile(input_path, "r") as zin:
        names = sorted(
            n for n in zin.namelist()
            if not n.endswith("/") and n.lower().endswith(IMAGE_EXTS)
        )
        payloads = {n: zin.read(n) for n in names}

    if not payloads:
        return 0

    from PIL import Image
    import numpy as np

    arrays = []
    for name in names:
        with Image.open(io.BytesIO(payloads[name])) as im:
            arrays.append(_preprocess(im.convert("RGB")))

    batch = torch.from_numpy(np.stack(arrays)).to(device)

    with torch.inference_mode():
        logits = model(batch)
        probs = logits.softmax(dim=1)
        scores, indices = probs.max(dim=1)

    logits = logits.detach().cpu().numpy().astype("float32")
    scores = scores.detach().cpu().numpy()
    indices = indices.detach().cpu().numpy()

    entries: List[Dict[str, object]] = []
    blobs: Dict[str, bytes] = {}

    for i, name in enumerate(names):
        stem = os.path.splitext(os.path.basename(name))[0]
        logit_name = f"{LOGIT_DIR}{stem}.npy"
        suffix = 1
        while logit_name in blobs:
            logit_name = f"{LOGIT_DIR}{stem}_{suffix}.npy"
            suffix += 1

        buf = io.BytesIO()
        np.save(buf, logits[i])
        blob = buf.getvalue()
        blobs[logit_name] = blob

        idx = int(indices[i])
        entries.append({
            "input_name": name,
            "input_sha256": hashlib.sha256(payloads[name]).hexdigest(),
            "logits_name": logit_name,
            "logits_sha256": hashlib.sha256(blob).hexdigest(),
            "top1_index": idx,
            "top1_label": (categories[idx] if categories and idx < len(categories) else str(idx)),
            "top1_score": round(float(scores[i]), 6),
        })

        if delay > 0:
            time.sleep(delay)

    manifest = json.dumps(entries, sort_keys=True, separators=(",", ":")).encode()

    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zout:
        for name in sorted(blobs):
            info = zipfile.ZipInfo(name, date_time=FIXED_DATE_TIME)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o600 << 16
            info.create_system = 0
            zout.writestr(info, blobs[name])

        info = zipfile.ZipInfo(PRED_NAME, date_time=FIXED_DATE_TIME)
        info.compress_type = zipfile.ZIP_DEFLATED
        info.external_attr = 0o600 << 16
        info.create_system = 0
        zout.writestr(info, manifest)

    return len(entries)
