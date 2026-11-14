"""
executor/workload/image_thumb.py  —  C2 workload: batch thumbnail generation
============================================================================
Owner: Person C.

Frozen workload contract (docs/验收门禁.md):
  input  : a flat zip containing images (png/jpg/jpeg/bmp/gif/webp)
  output : a zip containing
             thumbs/<stem>.png   one 128x128-bounded PNG per input image
             manifest.json       [{"input_name","input_sha256","output_name",
                                   "output_sha256","out_w","out_h"}, ...]
  units_done = number of images processed

Determinism rules (hard requirements from G1-5 / G1-6):
  * iterate members with sorted(zipfile.namelist())
  * every archive entry written via ZipInfo with a fixed date_time (1980-01-01)
  * manifest serialised with sort_keys=True and no time-dependent fields
  * PNG saved without pnginfo / EXIF, so no tIME chunk is embedded
  * no reliance on the local timezone, locale or filesystem timestamps

DORMPOOL_EXECUTOR_DELAY_MS (env, default 0):
  sleeps that many milliseconds AFTER each image. It only slows the task down so
  the dynamic-node experiments (G5) have a real time window. It must never change
  the output bytes.
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

# Pillow >= 9.1 exposes Image.Resampling; older versions expose the constants
# directly. Support both without importing torch or anything heavy.
try:  # pragma: no cover - version dependent
    _LANCZOS = Image.Resampling.LANCZOS
except AttributeError:  # pragma: no cover
    _LANCZOS = Image.LANCZOS  # type: ignore[attr-defined]

# ── Constants ────────────────────────────────────────────────────────────────

THUMB_SIZE: Tuple[int, int] = (128, 128)
IMAGE_EXTS = (".png", ".jpg", ".jpeg", ".bmp", ".gif", ".webp", ".tif", ".tiff")
MANIFEST_NAME = "manifest.json"
THUMB_DIR = "thumbs/"
FIXED_DATE_TIME = (1980, 1, 1, 0, 0, 0)   # zip has no sub-1980 timestamps
DELAY_ENV = "DORMPOOL_EXECUTOR_DELAY_MS"

MANIFEST_KEYS = frozenset(
    {"input_name", "input_sha256", "output_name", "output_sha256", "out_w", "out_h"}
)


# ── Helpers ──────────────────────────────────────────────────────────────────

def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def is_image_member(name: str) -> bool:
    """True for a real image entry (not a directory, not a manifest)."""
    if name.endswith("/"):
        return False
    lower = name.lower()
    if lower.endswith("/" + MANIFEST_NAME) or lower == MANIFEST_NAME:
        return False
    return lower.endswith(IMAGE_EXTS)


def _delay_seconds() -> float:
    raw = os.environ.get(DELAY_ENV, "0")
    try:
        ms = float(raw)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, ms / 1000.0)


def _zip_info(name: str) -> zipfile.ZipInfo:
    """Deterministic ZipInfo: fixed timestamp, fixed mode, fixed compression."""
    info = zipfile.ZipInfo(name, date_time=FIXED_DATE_TIME)
    info.compress_type = zipfile.ZIP_DEFLATED
    info.external_attr = 0o600 << 16
    info.create_system = 0          # MS-DOS: never leaks a POSIX uid/gid
    return info


def _thumb_name(stem: str, index: int, used: set) -> str:
    """thumbs/<stem>.png, disambiguated deterministically on collision."""
    candidate = f"{THUMB_DIR}{stem}.png"
    if candidate not in used:
        return candidate
    candidate = f"{THUMB_DIR}{stem}__{index}.png"
    if candidate not in used:
        return candidate
    # Pathological: still colliding — keep appending until free (deterministic).
    n = 2
    while f"{THUMB_DIR}{stem}__{index}_{n}.png" in used:
        n += 1
    return f"{THUMB_DIR}{stem}__{index}_{n}.png"


def make_thumbnail_png(raw: bytes) -> Tuple[bytes, int, int]:
    """Decode image bytes, bound to 128x128, re-encode as a bare PNG.

    Returns (png_bytes, width, height). Raises on undecodable input.
    """
    with Image.open(io.BytesIO(raw)) as img:
        rgb = img.convert("RGB")
        rgb.thumbnail(THUMB_SIZE, _LANCZOS)   # only ever shrinks
        width, height = rgb.size
        buf = io.BytesIO()
        # pnginfo=None + no exif => no tIME / eXIf chunk, so output is time-free
        rgb.save(buf, format="PNG", optimize=False, pnginfo=None)
        return buf.getvalue(), width, height


# ── Main handler ─────────────────────────────────────────────────────────────

def run(input_path: str, output_path: str) -> int:
    """Process one shard zip. Returns units_done (number of images).

    Raises RuntimeError when the shard contains no image at all, so run.py exits 1.
    """
    delay = _delay_seconds()
    manifest: List[Dict[str, object]] = []
    used_names: set = set()

    if not zipfile.is_zipfile(input_path):
        raise RuntimeError(f"input is not a zip archive: {input_path}")

    with zipfile.ZipFile(input_path, "r") as zin:
        members = sorted(zin.namelist())
        images = [n for n in members if is_image_member(n)]
        if not images:
            raise RuntimeError(f"no image members in shard: {input_path}")

        # Write straight to the requested output path; the worker gives us a
        # private temp name, so no partial file is ever visible as a result.
        with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zout:
            for index, name in enumerate(images):
                raw = zin.read(name)
                input_sha = sha256_hex(raw)

                png_bytes, width, height = make_thumbnail_png(raw)

                stem = os.path.splitext(os.path.basename(name))[0]
                out_name = _thumb_name(stem, index, used_names)
                used_names.add(out_name)

                zout.writestr(_zip_info(out_name), png_bytes)

                manifest.append(
                    {
                        "input_name": name,
                        "input_sha256": input_sha,
                        "output_name": out_name,
                        "output_sha256": sha256_hex(png_bytes),
                        "out_w": width,
                        "out_h": height,
                    }
                )

                if delay:
                    time.sleep(delay)

            payload = json.dumps(
                manifest, sort_keys=True, ensure_ascii=False, separators=(",", ":")
            ).encode("utf-8")
            zout.writestr(_zip_info(MANIFEST_NAME), payload)

    return len(manifest)
