"""
executor/workload/image_thumb_gpu.py  —  GPU batch-thumbnail workload
=====================================================================
Owner: Person C.

Same archive contract as image_thumb (thumbs/<stem>.png + manifest.json), but the
resize is computed on an NVIDIA GPU with CuPy.

Why this is a SEPARATE task_type rather than a flag on image_thumb
---------------------------------------------------------------
G1-5 requires image_thumb to be byte-identical across runs, and G1-6 compares
sha256 across machines. A GPU resize is a different algorithm (box filter in
float32 vs Pillow's LANCZOS in fixed-point), so its pixels can never match the
CPU output. Silently swapping the implementation would fail the determinism
gate and make the frozen manifest unverifiable. Instead:

  image_thumb       CPU, Pillow LANCZOS, byte-stable     <- the frozen workload
  image_thumb_gpu   GPU, CuPy box filter, byte-stable *on the same device*

Both emit the identical manifest schema, so the same authoritative validator
(master/app/c3_validation.py::ImageThumbValidator) verifies both. The validator
checks dimensions and recomputed hashes, not "matches CPU pixels" — which is
exactly what lets a second implementation pass honestly.

Determinism notes for the GPU path
----------------------------------
* No atomics, no reductions across threads: every output pixel is computed by
  one thread from a fixed set of input pixels, so the result is reproducible.
* Box-filter weights are computed in float64 on the host and cast once, so the
  same input always yields the same float32 coefficients.
* The manifest keeps sort_keys=True and fixed zip timestamps, same as the CPU
  workload.

If CuPy or a CUDA device is missing, run() raises GpuUnavailable. run.py maps
that to exit code 1 (task-level failure) so the Master can retry the task on a
node that has a GPU.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import time
import zipfile
from typing import Dict, List, Optional, Tuple

from PIL import Image

from executor.workload.image_thumb import (
    DELAY_ENV,
    FIXED_DATE_TIME,
    IMAGE_EXTS,
    MANIFEST_NAME,
    THUMB_DIR,
    THUMB_SIZE,
    is_image_member,
)

MAX_SIDE = THUMB_SIZE[0]

# ── GPU availability probe ───────────────────────────────────────────────────

_gpu_state: Optional[bool] = None
_cp = None


class GpuUnavailable(RuntimeError):
    """Raised when this machine cannot run the GPU workload."""


def _cupy():
    """Return the CuPy module with a working device, or raise GpuUnavailable."""
    global _gpu_state, _cp
    if _gpu_state is True and _cp is not None:
        return _cp
    if _gpu_state is False:
        raise GpuUnavailable("CuPy/CUDA unavailable (probed earlier)")
    try:
        import cupy as cp  # noqa: WPS433 — lazy by design
        if int(cp.cuda.runtime.getDeviceCount()) < 1:
            raise GpuUnavailable("no CUDA device")
        # Touch the device so a broken driver fails here, not mid-shard.
        cp.zeros(1, dtype=cp.float32).sum()
        _cp = cp
        _gpu_state = True
        return cp
    except GpuUnavailable:
        _gpu_state = False
        raise
    except Exception as exc:              # ImportError, CUDARuntimeError, ...
        _gpu_state = False
        raise GpuUnavailable(f"CuPy/CUDA unavailable: {exc}") from exc


def gpu_summary() -> dict:
    """Describe the device for logs and the README. Never raises."""
    try:
        cp = _cupy()
        props = cp.cuda.runtime.getDeviceProperties(0)
        name = props["name"]
        if isinstance(name, bytes):
            name = name.decode("utf-8", "replace")
        return {"available": True, "name": str(name),
                "cupy": cp.__version__,
                "vram_gb": round(int(props["totalGlobalMem"]) / 1024 ** 3, 2)}
    except Exception as exc:
        return {"available": False, "reason": str(exc)}


# ── GPU resize ───────────────────────────────────────────────────────────────

def _box_resize(cp, arr, out_h: int, out_w: int):
    """Area-average downsample of a float32 HxWxC array, deterministic.

    Each output pixel is the mean of the source box that maps onto it. Box
    average is the standard choice for thumbnails: it is alias-free when
    shrinking and needs no prefilter kernel.

    Weights are built in float64 and cast to float32 once, so repeated runs on
    the same device produce identical bits.
    """
    h, w = int(arr.shape[0]), int(arr.shape[1])
    ys = cp.asarray(_axis_weights(h, out_h), dtype=cp.float32)   # (out_h, h)
    xs = cp.asarray(_axis_weights(w, out_w), dtype=cp.float32)   # (out_w, w)

    # Separable: contract height first, then width. Two matmuls, no atomics.
    tmp = cp.tensordot(ys, arr, axes=(1, 0))        # (out_h, w, C)
    out = cp.tensordot(tmp, xs, axes=(1, 1))        # (out_h, C, out_w)
    return cp.transpose(out, (0, 2, 1))             # (out_h, out_w, C)


def _axis_weights(n_in: int, n_out: int) -> "list[list[float]]":
    """Normalised box-filter matrix mapping n_in samples onto n_out samples.

    Row r covers source indices [r*scale, (r+1)*scale); the weights sum to 1 so
    the output stays in range and no per-pixel renormalisation is needed on GPU.
    """
    scale = n_in / float(n_out)
    rows: List[List[float]] = []
    for r in range(n_out):
        lo = r * scale
        hi = (r + 1) * scale
        row = [0.0] * n_in
        first = int(lo)
        last = min(n_in - 1, int(hi - 1e-12))
        for i in range(first, last + 1):
            left = max(lo, float(i))
            right = min(hi, float(i + 1))
            if right > left:
                row[i] = (right - left) / scale
        total = sum(row)
        if total > 0:
            row = [v / total for v in row]
        rows.append(row)
    return rows


def _thumb_gpu(cp, raw: bytes) -> Tuple[bytes, int, int]:
    """Decode on CPU (Pillow), resize on GPU, encode on CPU.

    Returns (png_bytes, out_w, out_h). Box resize only shrinks; images already
    within MAX_SIDE are passed through untouched, matching image_thumb's
    "thumbnail never enlarges" behaviour.
    """
    with Image.open(io.BytesIO(raw)) as im:
        im = im.convert("RGB")
        w, h = im.size
        if w <= MAX_SIDE and h <= MAX_SIDE:
            out_w, out_h = w, h
            buf = io.BytesIO()
            im.save(buf, "PNG", pnginfo=None)
            return buf.getvalue(), out_w, out_h

        scale = MAX_SIDE / float(max(w, h))
        out_w = max(1, int(round(w * scale)))
        out_h = max(1, int(round(h * scale)))

        # Upload as float32 straight from the host. Doing the uint8->float32
        # conversion with a CuPy elementwise op would compile a kernel through
        # NVRTC, which needs CUDA toolkit headers (not just the driver) and
        # fails with "Failed to find CUDA headers" on driver-only machines.
        # tensordot uses cuBLAS, which ships precompiled, so this path needs
        # nothing but the driver.
        import numpy as np
        arr = cp.asarray(np.asarray(im, dtype=np.float32))

    resized = _box_resize(cp, arr, out_h, out_w)
    # Bring the float result back and finish on the host: no device-side
    # elementwise kernel, hence no NVRTC dependency. np.rint is
    # round-half-to-even and deterministic, matching CuPy's rint.
    host = cp.asnumpy(resized)                     # (out_h, out_w, 3) float32
    pixels = np.clip(np.rint(host), 0, 255).astype(np.uint8)

    buf = io.BytesIO()
    Image.fromarray(pixels, "RGB").save(buf, "PNG", pnginfo=None)
    return buf.getvalue(), out_w, out_h


# ── Workload entry point ─────────────────────────────────────────────────────

def _delay_seconds() -> float:
    try:
        return max(0.0, float(os.environ.get(DELAY_ENV, "0")) / 1000.0)
    except (TypeError, ValueError):
        return 0.0


def run(input_path: str, output_path: str) -> int:
    """Process every image in input_path, write thumbs + manifest to output_path.

    Raises GpuUnavailable when this node has no usable CUDA device — run.py
    turns that into a task-level failure so the Master can retry elsewhere.
    """
    cp = _cupy()                                  # raises GpuUnavailable
    delay = _delay_seconds()

    with zipfile.ZipFile(input_path, "r") as zin:
        names = sorted(n for n in zin.namelist() if is_image_member(n))

    entries: List[Dict[str, object]] = []
    thumbs: Dict[str, bytes] = {}

    with zipfile.ZipFile(input_path, "r") as zin:
        for name in names:
            raw = zin.read(name)
            png, out_w, out_h = _thumb_gpu(cp, raw)

            stem = os.path.splitext(os.path.basename(name))[0]
            out_name = f"{THUMB_DIR}{stem}.png"
            suffix = 1
            while out_name in thumbs:             # duplicate stems in one shard
                out_name = f"{THUMB_DIR}{stem}_{suffix}.png"
                suffix += 1

            thumbs[out_name] = png
            entries.append({
                "input_name": name,
                "input_sha256": hashlib.sha256(raw).hexdigest(),
                "output_name": out_name,
                "output_sha256": hashlib.sha256(png).hexdigest(),
                "out_w": out_w,
                "out_h": out_h,
            })

            if delay > 0:
                time.sleep(delay)

    entries.sort(key=lambda e: str(e["input_name"]))
    manifest = json.dumps(entries, sort_keys=True, separators=(",", ":")).encode()

    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zout:
        for out_name in sorted(thumbs):
            info = zipfile.ZipInfo(out_name, date_time=FIXED_DATE_TIME)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o600 << 16
            info.create_system = 0
            zout.writestr(info, thumbs[out_name])

        info = zipfile.ZipInfo(MANIFEST_NAME, date_time=FIXED_DATE_TIME)
        info.compress_type = zipfile.ZIP_DEFLATED
        info.external_attr = 0o600 << 16
        info.create_system = 0
        zout.writestr(info, manifest)

    return len(entries)
