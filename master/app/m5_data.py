"""
master/app/m5_data.py  —  M5 Data & Result Manager
====================================================
Owner: Person A.

On-disk layout (root from DORMPOOL_DATA, default "data"):

  data/inputs/<job_id>/input.zip            the uploaded original
  data/inputs/<job_id>/shard_<i>.zip        one shard per Task; input_ref = "inputs/<job_id>/shard_<i>.zip"
  data/outputs/<job_id>/<task_id>.zip       a worker's per-task result
  data/outputs/<job_id>/result.zip          the aggregated final result

Every ref that crosses the HTTP boundary is resolved through resolve_path(),
which refuses absolute paths and any ".." escape so a ref can never read or
write outside the data root.
"""

from __future__ import annotations

import json
import os
import shutil
import threading
import zipfile
from typing import Dict, List, Optional

IMAGE_EXTS = (".png", ".jpg", ".jpeg", ".bmp", ".gif", ".webp", ".tif", ".tiff")
MANIFEST_NAME = "manifest.json"
FIXED_DATE_TIME = (1980, 1, 1, 0, 0, 0)


def _data_root() -> str:
    root = os.environ.get("DORMPOOL_DATA", "data")
    return os.path.abspath(root)


def _is_image_member(name: str) -> bool:
    if name.endswith("/"):
        return False
    lower = name.lower()
    if lower == MANIFEST_NAME or lower.endswith("/" + MANIFEST_NAME):
        return False
    return lower.endswith(IMAGE_EXTS)


def _zip_info(name: str) -> zipfile.ZipInfo:
    info = zipfile.ZipInfo(name, date_time=FIXED_DATE_TIME)
    info.compress_type = zipfile.ZIP_DEFLATED
    info.external_attr = 0o600 << 16
    info.create_system = 0
    return info


class DataManager:
    def __init__(self):
        self._lock = threading.RLock()
        os.makedirs(os.path.join(_data_root(), "inputs"), exist_ok=True)
        os.makedirs(os.path.join(_data_root(), "outputs"), exist_ok=True)

    # ── paths ────────────────────────────────────────────────────────────────

    @staticmethod
    def root() -> str:
        return _data_root()

    def resolve_path(self, ref: str) -> Optional[str]:
        """Map a ref to an absolute path inside the data root, or None.

        Rejects absolute paths, drive letters and any '..' segment. This is the
        single choke point for both reads and writes.
        """
        if not ref:
            return None
        normalised = str(ref).replace("\\", "/").strip()
        if not normalised or normalised.startswith("/"):
            return None
        if ":" in normalised.split("/")[0]:
            return None
        if any(part == ".." for part in normalised.split("/")):
            return None

        root = _data_root()
        candidate = os.path.abspath(os.path.join(root, normalised.replace("/", os.sep)))
        # Belt and braces: the resolved path must live under the root.
        try:
            if os.path.commonpath([root, candidate]) != root:
                return None
        except ValueError:
            return None
        return candidate

    # ── inputs ───────────────────────────────────────────────────────────────

    def save_input(self, job_id: str, filename: str, data: bytes) -> str:
        """Persist the uploaded archive. Returns the ref key."""
        with self._lock:
            folder = os.path.join(_data_root(), "inputs", job_id)
            os.makedirs(folder, exist_ok=True)
            safe_name = os.path.basename(filename or "input.zip")
            path = os.path.join(folder, safe_name)
            with open(path, "wb") as fh:
                fh.write(data)
            return f"inputs/{job_id}/{safe_name}"

    def split_job_input(self, job_id: str, input_ref: str, shard_size: int) -> List[str]:
        """Split the uploaded archive into shard zips. Returns the input_refs.

        Members are taken in sorted order so the shard composition is stable for
        a given upload. Each shard is a valid zip carrying only its own images,
        which is what the executor consumes.
        """
        source = self.resolve_path(input_ref)
        if source is None or not os.path.isfile(source):
            return []

        shard_size = max(1, int(shard_size))
        with self._lock:
            try:
                with zipfile.ZipFile(source, "r") as zin:
                    members = sorted(n for n in zin.namelist() if _is_image_member(n))
                    blobs = {n: zin.read(n) for n in members}
            except (zipfile.BadZipFile, OSError):
                return []

            if not blobs:
                return []

            refs: List[str] = []
            for index, start in enumerate(range(0, len(members), shard_size)):
                chunk = members[start:start + shard_size]
                shard_name = f"shard_{index}.zip"
                shard_path = os.path.join(_data_root(), "inputs", job_id, shard_name)
                with zipfile.ZipFile(shard_path, "w", zipfile.ZIP_DEFLATED) as zout:
                    for name in chunk:
                        zout.writestr(_zip_info(name), blobs[name])
                refs.append(f"inputs/{job_id}/{shard_name}")
            return refs

    def count_images(self, input_ref: str) -> int:
        path = self.resolve_path(input_ref)
        if path is None or not os.path.isfile(path):
            return 0
        try:
            with zipfile.ZipFile(path, "r") as zin:
                return sum(1 for n in zin.namelist() if _is_image_member(n))
        except (zipfile.BadZipFile, OSError):
            return 0

    # ── outputs ──────────────────────────────────────────────────────────────

    def save_task_output(self, job_id: str, task_id: str, data: bytes) -> str:
        """Store one task's result archive. Returns the ref key."""
        with self._lock:
            folder = os.path.join(_data_root(), "outputs", job_id)
            os.makedirs(folder, exist_ok=True)
            path = os.path.join(folder, f"{task_id}.zip")
            with open(path, "wb") as fh:
                fh.write(data)
            return f"outputs/{job_id}/{task_id}.zip"

    def save_task_output_from_file(self, job_id: str, task_id: str, src_path: str) -> str:
        with self._lock:
            folder = os.path.join(_data_root(), "outputs", job_id)
            os.makedirs(folder, exist_ok=True)
            path = os.path.join(folder, f"{task_id}.zip")
            shutil.copyfile(src_path, path)
            return f"outputs/{job_id}/{task_id}.zip"

    # ── aggregation ──────────────────────────────────────────────────────────

    def aggregate_job_output(self, job_id: str, task_ids: List[str]) -> Optional[str]:
        """Merge every succeeded task's manifest and thumbs into result.zip.

        Deduplicates on input_name so a retried task never contributes twice, and
        raises nothing: a broken task archive is skipped and the rest still merge.
        """
        with self._lock:
            out_folder = os.path.join(_data_root(), "outputs", job_id)
            os.makedirs(out_folder, exist_ok=True)
            result_path = os.path.join(out_folder, "result.zip")

            merged: Dict[str, dict] = {}
            blobs: Dict[str, bytes] = {}
            seen_thumb_names: set = set()

            for task_id in task_ids:
                task_zip = os.path.join(out_folder, f"{task_id}.zip")
                if not os.path.isfile(task_zip):
                    continue
                try:
                    with zipfile.ZipFile(task_zip, "r") as zin:
                        if MANIFEST_NAME not in zin.namelist():
                            continue
                        manifest = json.loads(zin.read(MANIFEST_NAME).decode("utf-8"))
                        for entry in manifest:
                            input_name = entry.get("input_name")
                            if not input_name or input_name in merged:
                                continue          # first success wins, no double count
                            out_name = entry.get("output_name")
                            if not out_name or out_name not in zin.namelist():
                                continue
                            merged[input_name] = entry
                            blobs[out_name] = zin.read(out_name)
                            seen_thumb_names.add(out_name)
                except (zipfile.BadZipFile, OSError, ValueError, json.JSONDecodeError):
                    continue

            if not merged:
                return None

            ordered = [merged[k] for k in sorted(merged.keys())]
            with zipfile.ZipFile(result_path, "w", zipfile.ZIP_DEFLATED) as zout:
                for out_name in sorted(blobs.keys()):
                    zout.writestr(_zip_info(out_name), blobs[out_name])
                payload = json.dumps(
                    ordered, sort_keys=True, ensure_ascii=False, separators=(",", ":")
                ).encode("utf-8")
                zout.writestr(_zip_info(MANIFEST_NAME), payload)

            return f"outputs/{job_id}/result.zip"

    def get_job_output_path(self, job_id: str) -> Optional[str]:
        path = os.path.join(_data_root(), "outputs", job_id, "result.zip")
        return path if os.path.isfile(path) else None

    def task_output_path(self, job_id: str, task_id: str) -> Optional[str]:
        path = os.path.join(_data_root(), "outputs", job_id, f"{task_id}.zip")
        return path if os.path.isfile(path) else None


data_manager = DataManager()
