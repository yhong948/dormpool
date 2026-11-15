"""
master/app/c3_validation.py  —  C3 Validation & Accounting (authoritative)
===========================================================================
Owner: Person C, executed by Master (M4/M5).

This is the AUTHORITATIVE validator: the Master confirms a Task only after this
passes. It deliberately does NOT trust anything the executor self-reports — every
number is recomputed from the bytes in the result archive.

Checks performed on an image_thumb output zip:
  1. manifest.json exists, is valid JSON, and is a list
  2. every entry has exactly the six contract keys, with the right types
  3. every listed output file exists in the archive, is a decodable PNG,
     and is at most 128x128
  4. the recorded out_w/out_h EQUAL the real dimensions of the PNG
     (this is what catches a tampered manifest)
  5. every output_sha256 equals the sha256 recomputed from the archive bytes
  6. output_name is unique across the manifest (no duplicate accounting)
  7. when the source shard is reachable, input_sha256 is re-verified against it
     and the manifest covers exactly the shard's image set
"""

from __future__ import annotations

import ast
import hashlib
import io
import json
import os
import zipfile
from typing import Any, Dict, List, Optional

MANIFEST_NAME = "manifest.json"
THUMB_DIR = "thumbs/"
MAX_DIM = 128
IMAGE_EXTS = (".png", ".jpg", ".jpeg", ".bmp", ".gif", ".webp", ".tif", ".tiff")
MANIFEST_KEYS = frozenset(
    {"input_name", "input_sha256", "output_name", "output_sha256", "out_w", "out_h"}
)


class ValidationError(Exception):
    """Raised when an executor output cannot be trusted."""


# ── Base + registry ──────────────────────────────────────────────────────────

class ResultValidator:
    """Base class for workload-specific validators."""

    def validate(self, output_path: str, task_meta: dict) -> int:
        raise NotImplementedError


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _is_image_member(name: str) -> bool:
    if name.endswith("/"):
        return False
    lower = name.lower()
    if lower == MANIFEST_NAME or lower.endswith("/" + MANIFEST_NAME):
        return False
    return lower.endswith(IMAGE_EXTS)


def _resolve_source_zip(task_meta: dict) -> Optional[str]:
    """Best-effort lookup of the source shard on the Master's data root.

    The result is advisory: validation still runs (and still rejects a tampered
    manifest) when the shard cannot be found. Only the cross-check of
    input_sha256 and the input-set equality are skipped in that case.
    """
    for key in ("input_abs_path", "input_path"):
        p = task_meta.get(key)
        if p and os.path.isfile(str(p)):
            return str(p)

    ref = task_meta.get("input_ref")
    if not ref:
        return None
    root = os.environ.get("DORMPOOL_DATA", "data")
    candidate = os.path.abspath(os.path.join(root, str(ref).replace("/", os.sep)))
    if os.path.isfile(candidate):
        return candidate
    return None


class ImageThumbValidator(ResultValidator):
    """Authoritative validator for the frozen image_thumb workload."""

    def validate(self, output_path: str, task_meta: dict) -> int:
        units = validate_shard(_resolve_source_zip(task_meta or {}), output_path)
        return units


class ConvNextValidator(ResultValidator):
    """Validator for convnext_infer output archives.

    It re-derives every hash from the archive bytes and checks that each entry
    names a logits file with the right dtype and shape. It deliberately does NOT
    compare logits against another device's output: CPU and GPU differ in the
    last float bits, and demanding equality would wrongly fail an honest node.
    """

    def validate(self, output_path: str, task_meta: dict) -> int:
        return validate_convnext(_resolve_source_zip(task_meta or {}), output_path)


def validate_shard(source_zip: Optional[str], output_zip: str) -> int:
    """Validate one result archive. Returns units_done. Raises ValidationError."""
    if not output_zip or not os.path.isfile(output_zip):
        raise ValidationError(f"result archive missing: {output_zip!r}")
    if not zipfile.is_zipfile(output_zip):
        raise ValidationError(f"result is not a zip archive: {output_zip!r}")

    try:
        with zipfile.ZipFile(output_zip, "r") as zout:
            names = set(zout.namelist())
            if MANIFEST_NAME not in names:
                raise ValidationError("manifest.json missing from result archive")
            raw_manifest = zout.read(MANIFEST_NAME)

            try:
                manifest = json.loads(raw_manifest.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValidationError(f"manifest.json is not valid JSON: {exc}") from exc

            if not isinstance(manifest, list):
                raise ValidationError("manifest.json must be a JSON list")
            if not manifest:
                raise ValidationError("manifest.json is empty (zero work units)")

            seen_outputs: set = set()
            seen_inputs: set = set()

            for index, entry in enumerate(manifest):
                where = f"manifest[{index}]"
                if not isinstance(entry, dict):
                    raise ValidationError(f"{where} is not an object")

                keys = set(entry.keys())
                if keys != MANIFEST_KEYS:
                    missing = MANIFEST_KEYS - keys
                    extra = keys - MANIFEST_KEYS
                    raise ValidationError(
                        f"{where} key mismatch: missing={sorted(missing)} extra={sorted(extra)}"
                    )

                input_name = entry["input_name"]
                output_name = entry["output_name"]
                out_w = entry["out_w"]
                out_h = entry["out_h"]
                out_sha = entry["output_sha256"]
                in_sha = entry["input_sha256"]

                if not isinstance(output_name, str) or not output_name.startswith(THUMB_DIR):
                    raise ValidationError(f"{where} output_name must start with {THUMB_DIR!r}")
                if not isinstance(out_w, int) or not isinstance(out_h, int):
                    raise ValidationError(f"{where} out_w/out_h must be integers")
                if not (1 <= out_w <= MAX_DIM and 1 <= out_h <= MAX_DIM):
                    raise ValidationError(
                        f"{where} dimensions out of range: {out_w}x{out_h} (max {MAX_DIM})"
                    )
                for label, digest in (("output_sha256", out_sha), ("input_sha256", in_sha)):
                    if not isinstance(digest, str) or len(digest) != 64:
                        raise ValidationError(f"{where} {label} is not a 64-char hex digest")

                if output_name in seen_outputs:
                    raise ValidationError(f"{where} duplicate output_name {output_name!r}")
                seen_outputs.add(output_name)

                if input_name in seen_inputs:
                    raise ValidationError(f"{where} duplicate input_name {input_name!r}")
                seen_inputs.add(input_name)

                if output_name not in names:
                    raise ValidationError(f"{where} output file absent from archive: {output_name!r}")

                data = zout.read(output_name)
                actual_sha = _sha256_hex(data)
                if actual_sha != out_sha:
                    raise ValidationError(
                        f"{where} output_sha256 mismatch for {output_name!r}: "
                        f"recorded {out_sha[:12]}… actual {actual_sha[:12]}…"
                    )

                # Decode and confirm the manifest's dimensions are the real ones.
                real_w, real_h = _png_dimensions(data, output_name)
                if (real_w, real_h) != (out_w, out_h):
                    raise ValidationError(
                        f"{where} dimension mismatch for {output_name!r}: "
                        f"recorded {out_w}x{out_h}, actual {real_w}x{real_h}"
                    )
                if real_w > MAX_DIM or real_h > MAX_DIM:
                    raise ValidationError(
                        f"{where} thumbnail exceeds {MAX_DIM}px: {real_w}x{real_h}"
                    )
    except zipfile.BadZipFile as exc:
        raise ValidationError(f"corrupt result archive: {exc}") from exc

    # ── Cross-check against the source shard when it is reachable ────────────
    if source_zip and os.path.isfile(source_zip):
        try:
            with zipfile.ZipFile(source_zip, "r") as zsrc:
                src_images = {n: zsrc.read(n) for n in zsrc.namelist() if _is_image_member(n)}
        except (zipfile.BadZipFile, OSError):
            src_images = {}

        if src_images:
            if seen_inputs != set(src_images):
                missing = sorted(set(src_images) - seen_inputs)
                extra = sorted(seen_inputs - set(src_images))
                raise ValidationError(
                    f"manifest does not cover the shard exactly: missing={missing} extra={extra}"
                )
            for entry in manifest:
                expected = _sha256_hex(src_images[entry["input_name"]])
                if entry["input_sha256"] != expected:
                    raise ValidationError(
                        f"input_sha256 mismatch for {entry['input_name']!r}: "
                        f"recorded {entry['input_sha256'][:12]}… actual {expected[:12]}…"
                    )

    return len(manifest)


CONVNEXT_PRED = "predictions.json"
CONVNEXT_LOGIT_DIR = "logits/"
CONVNEXT_KEYS = frozenset(
    {"input_name", "input_sha256", "logits_name", "logits_sha256",
     "top1_index", "top1_label", "top1_score"}
)
IMAGENET_CLASSES = 1000


def _npy_shape(data: bytes) -> tuple:
    """Parse the shape out of a .npy header without importing numpy.

    Only the fields this validator needs are read. A malformed header is a
    ValidationError, never a crash.
    """
    if len(data) < 10 or data[:6] != b"\x93NUMPY":
        raise ValidationError("logits file is not a .npy array")
    major = data[6]
    header_len = int.from_bytes(data[8:10], "little") if major == 1 else int.from_bytes(data[8:12], "little")
    start = 10 if major == 1 else 12
    header = data[start:start + header_len].decode("latin-1", "replace")
    try:
        spec = ast.literal_eval(header.strip())
    except (ValueError, SyntaxError) as exc:
        raise ValidationError(f"unreadable .npy header: {exc}") from exc
    return tuple(spec.get("shape") or ()), str(spec.get("descr") or "")


def validate_convnext(source_zip: Optional[str], output_zip: str) -> int:
    """Validate a convnext_infer result archive. Returns units_done."""
    if not output_zip or not os.path.isfile(output_zip):
        raise ValidationError(f"result archive missing: {output_zip!r}")
    if not zipfile.is_zipfile(output_zip):
        raise ValidationError(f"result is not a zip archive: {output_zip!r}")

    with zipfile.ZipFile(output_zip, "r") as zout:
        names = set(zout.namelist())
        if CONVNEXT_PRED not in names:
            raise ValidationError(f"{CONVNEXT_PRED} missing from result archive")

        try:
            entries = json.loads(zout.read(CONVNEXT_PRED).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValidationError(f"{CONVNEXT_PRED} is not valid JSON: {exc}") from exc
        if not isinstance(entries, list):
            raise ValidationError(f"{CONVNEXT_PRED} must be a JSON list")
        if not entries:
            raise ValidationError(f"{CONVNEXT_PRED} is empty (zero work units)")

        seen_inputs: set = set()
        seen_logits: set = set()

        for index, entry in enumerate(entries):
            where = f"predictions[{index}]"
            if not isinstance(entry, dict):
                raise ValidationError(f"{where} is not an object")
            if set(entry.keys()) != CONVNEXT_KEYS:
                raise ValidationError(
                    f"{where} key mismatch: missing={sorted(CONVNEXT_KEYS - set(entry))} "
                    f"extra={sorted(set(entry) - CONVNEXT_KEYS)}"
                )

            input_name = entry["input_name"]
            logits_name = entry["logits_name"]
            if not isinstance(input_name, str) or not input_name:
                raise ValidationError(f"{where} input_name must be a non-empty string")
            if not isinstance(logits_name, str) or not logits_name.startswith(CONVNEXT_LOGIT_DIR):
                raise ValidationError(f"{where} logits_name must start with {CONVNEXT_LOGIT_DIR!r}")

            for label in ("input_sha256", "logits_sha256"):
                digest = entry[label]
                if not isinstance(digest, str) or len(digest) != 64:
                    raise ValidationError(f"{where} {label} is not a 64-char hex digest")

            top1 = entry["top1_index"]
            if not isinstance(top1, int) or not (0 <= top1 < IMAGENET_CLASSES):
                raise ValidationError(f"{where} top1_index out of range: {top1!r}")
            score = entry["top1_score"]
            if not isinstance(score, (int, float)) or not (0.0 <= float(score) <= 1.0):
                raise ValidationError(f"{where} top1_score out of range: {score!r}")
            if not isinstance(entry["top1_label"], str):
                raise ValidationError(f"{where} top1_label must be a string")

            if input_name in seen_inputs:
                raise ValidationError(f"{where} duplicate input_name {input_name!r}")
            seen_inputs.add(input_name)
            if logits_name in seen_logits:
                raise ValidationError(f"{where} duplicate logits_name {logits_name!r}")
            seen_logits.add(logits_name)

            if logits_name not in names:
                raise ValidationError(f"{where} logits file absent from archive: {logits_name!r}")

            blob = zout.read(logits_name)
            actual = _sha256_hex(blob)
            if actual != entry["logits_sha256"]:
                raise ValidationError(
                    f"{where} logits_sha256 mismatch: recorded "
                    f"{entry['logits_sha256'][:12]}… actual {actual[:12]}…"
                )

            shape, descr = _npy_shape(blob)
            if shape != (IMAGENET_CLASSES,):
                raise ValidationError(
                    f"{where} logits shape {shape} != ({IMAGENET_CLASSES},)"
                )
            if not descr.startswith("<f4") and not descr.startswith("|f4"):
                raise ValidationError(f"{where} logits dtype {descr!r} is not float32")

    # ── Cross-check against the source shard when it is reachable ────────────
    src_images: Dict[str, bytes] = {}
    if source_zip and os.path.isfile(source_zip):
        try:
            with zipfile.ZipFile(source_zip, "r") as zsrc:
                src_images = {
                    n: zsrc.read(n) for n in zsrc.namelist() if _is_image_member(n)
                }
        except (zipfile.BadZipFile, OSError):
            src_images = {}

    if src_images:
        if seen_inputs != set(src_images):
            raise ValidationError(
                "predictions do not cover the shard exactly: "
                f"missing={sorted(set(src_images) - seen_inputs)} "
                f"extra={sorted(seen_inputs - set(src_images))}"
            )
        for entry in entries:
            expected = _sha256_hex(src_images[entry["input_name"]])
            if entry["input_sha256"] != expected:
                raise ValidationError(
                    f"input_sha256 mismatch for {entry['input_name']!r}"
                )

    return len(entries)


def _png_dimensions(data: bytes, label: str):
    """Return (width, height) of PNG bytes, raising ValidationError if unreadable."""
    try:
        from PIL import Image
    except ImportError as exc:  # pragma: no cover - Pillow is a hard dependency
        raise ValidationError(f"Pillow unavailable, cannot verify {label!r}: {exc}") from exc

    try:
        with Image.open(io.BytesIO(data)) as img:
            img.verify()                      # structural check
        with Image.open(io.BytesIO(data)) as img:
            return img.size
    except Exception as exc:
        raise ValidationError(f"{label!r} is not a decodable image: {exc}") from exc


# ── Registry ──────────────────────────────────────────────────────────────────

VALIDATORS: Dict[str, ResultValidator] = {
    "image_thumb": ImageThumbValidator(),
    # Same archive contract and same checks. The GPU path produces different
    # pixels, and that is fine: this validator recomputes hashes and verifies
    # dimensions, it never demands the pixels match the CPU implementation.
    "image_thumb_gpu": ImageThumbValidator(),
    # ConvNeXt inference: hashes and logits shape are re-derived from the
    # archive; logits are never compared across devices (CPU/GPU differ in the
    # last bits and an honest node must not be failed for it).
    "convnext_infer": ConvNextValidator(),
}


def validate_result(output_path: str, task_meta: dict) -> int:
    """Validate a task result. Returns units_done or raises ValidationError."""
    task_type = (task_meta or {}).get("task_type", "")
    validator = VALIDATORS.get(task_type)
    if validator is None:
        # No validator for this type: the Master cannot verify it. Refuse rather
        # than silently trusting a self-reported count (G1-4 forbids that).
        raise ValidationError(f"no validator registered for task_type {task_type!r}")
    return validator.validate(output_path, task_meta)
