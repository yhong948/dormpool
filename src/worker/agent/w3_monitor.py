"""
worker/agent/w3_monitor.py  —  W3 Resource Monitor
==================================================
Person B owns this file.

Responsibility:
  - Detect the REAL hardware of this machine once (CPU / RAM / GPU), no placeholders
  - Track free/busy execution slots (lock-protected)
  - Sample CPU and GPU utilisation for the heartbeat
  - Provide a real compute micro-benchmark (sha256 ops/s) that is carried to the
    Master inside the frozen WorkerCapability.executor_version field as "1|<score>"

GPU detection cascade (degrades silently, never raises):
  1) pynvml / nvidia-ml-py  -> nvmlDeviceGetName + nvmlDeviceGetMemoryInfo().total
  2) Windows PowerShell     -> Get-CimInstance Win32_VideoController (Name, AdapterRAM)
  3) (None, None)

Note on Win32_VideoController.AdapterRAM: it is a uint32 and is truncated to
~4 GiB for cards with more VRAM, and virtual display adapters report 0/absent.
The PowerShell layer therefore ranks real GPUs above virtual adapters and only
uses it when NVML is unavailable. NVML (layer 1) always reports the true size.
"""

from __future__ import annotations

import hashlib
import json
import logging
import platform
import subprocess
import sys
import threading
import time
from typing import Optional, Tuple

from dormpool.src.shared.contracts import WorkerCapability

log = logging.getLogger("w3")

try:                                    # psutil is a hard requirement in practice
    import psutil
    _PSUTIL = True
except Exception:                       # pragma: no cover - defensive
    psutil = None                       # type: ignore
    _PSUTIL = False

# ── Constants ────────────────────────────────────────────────────────────────

BENCH_ITERATIONS = 200_000              # sha256() calls per compute score sample
BENCH_BUFFER = bytes(range(256)) * 4    # exactly 1024 bytes
EXECUTOR_VERSION_PREFIX = "1"           # executor_version == f"1|<score>"

_PS_TIMEOUT = 15.0
_PS_COMMAND = (
    "Get-CimInstance Win32_VideoController | "
    "Select-Object Name,AdapterRAM | ConvertTo-Json -Compress"
)

# Vendor substrings used to prefer a real GPU over virtual display adapters
_GPU_VENDOR_HINTS = (
    "nvidia", "geforce", "rtx", "gtx", "quadro", "tesla",
    "amd", "radeon", "rx ", "instinct",
    "intel", "arc ", "iris", "uhd graphics",
)


# ── NVML lazy singleton ──────────────────────────────────────────────────────

_nvml_lock = threading.Lock()
_nvml_state: Optional[bool] = None      # None = not tried, True = usable, False = unusable
_nvml_mod = None
_nvml_error = None


def _nvml():
    """Return the pynvml module with NVML initialised, or None.

    Thread-safe, idempotent, and swallows every failure (missing lib, no driver,
    deprecated package shim, ...). The result is cached for the process lifetime.
    """
    global _nvml_state, _nvml_mod, _nvml_error
    with _nvml_lock:
        if _nvml_state is not None:
            return _nvml_mod if _nvml_state else None
        try:
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                import pynvml as _m       # noqa: WPS433 (lazy by design)
            _m.nvmlInit()
            _nvml_mod = _m
            _nvml_state = True
            _nvml_error = None
        except Exception as exc:          # ImportError, NVMLError, OSError, ...
            log.debug("pynvml unavailable: %s", exc)
            _nvml_mod = None
            _nvml_state = False
            _nvml_error = str(exc)
        return _nvml_mod if _nvml_state else None


# ── GPU probe cascade ────────────────────────────────────────────────────────

def _gpu_via_nvml() -> Tuple[Optional[str], Optional[float]]:
    """Layer 1: NVIDIA Management Library. Returns (name, vram_gb) or (None, None)."""
    nv = _nvml()
    if nv is None:
        return None, None
    try:
        count = int(nv.nvmlDeviceGetCount())
        if count <= 0:
            return None, None
        handle = nv.nvmlDeviceGetHandleByIndex(0)
        raw_name = nv.nvmlDeviceGetName(handle)
        if isinstance(raw_name, bytes):
            raw_name = raw_name.decode("utf-8", "replace")
        name = str(raw_name).strip() or None
        vram_gb = None
        try:
            mem = nv.nvmlDeviceGetMemoryInfo(handle)
            if mem.total > 0:
                vram_gb = round(mem.total / 1024 ** 3, 2)
        except Exception as exc:
            log.debug("nvml memory query failed: %s", exc)
        return name, vram_gb
    except Exception as exc:
        log.debug("nvml device query failed: %s", exc)
        return None, None


def _rank_adapter(item: dict) -> tuple:
    """Score a Win32_VideoController entry so real GPUs beat virtual adapters."""
    name = str(item.get("Name") or "").strip()
    raw_ram = item.get("AdapterRAM")
    try:
        ram = int(raw_ram) if raw_ram not in (None, "") else 0
    except (TypeError, ValueError):
        ram = 0
    low = name.lower()
    vendor = any(h in low for h in _GPU_VENDOR_HINTS)
    virtual = any(w in low for w in ("virtual", "remote", "basic display", "indirect"))
    return (
        1 if vendor else 0,
        1 if ram > 0 else 0,
        0 if virtual else 1,
        ram,
    )


def _gpu_via_powershell() -> Tuple[Optional[str], Optional[float]]:
    """Layer 2 (Windows only): query Win32_VideoController via PowerShell."""
    if not sys.platform.startswith("win"):
        return None, None
    try:
        kwargs = {}
        if hasattr(subprocess, "CREATE_NO_WINDOW"):
            kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW
        proc = subprocess.run(
            ["powershell", "-NoProfile", "-NonInteractive", "-Command", _PS_COMMAND],
            capture_output=True, text=True, timeout=_PS_TIMEOUT, **kwargs
        )
        if proc.returncode != 0 or not (proc.stdout or "").strip():
            log.debug("powershell gpu probe rc=%s stderr=%s",
                      proc.returncode, (proc.stderr or "")[:200])
            return None, None
        data = json.loads(proc.stdout)
    except Exception as exc:
        log.debug("powershell gpu probe failed: %s", exc)
        return None, None

    if isinstance(data, dict):
        data = [data]
    if not isinstance(data, list):
        return None, None

    items = [d for d in data if isinstance(d, dict) and str(d.get("Name") or "").strip()]
    if not items:
        return None, None

    best = max(items, key=_rank_adapter)
    name = str(best.get("Name") or "").strip() or None
    vram_gb = None
    try:
        raw_ram = best.get("AdapterRAM")
        ram = int(raw_ram) if raw_ram not in (None, "") else 0
        if ram > 0:
            vram_gb = round(ram / 1024 ** 3, 2)
    except (TypeError, ValueError):
        vram_gb = None
    return name, vram_gb


def detect_gpu() -> Tuple[Optional[str], Optional[float]]:
    """Full cascade. Always returns a 2-tuple; never raises."""
    try:
        name, vram = _gpu_via_nvml()
        if name or vram:
            return name, vram
    except Exception as exc:              # belt and braces
        log.debug("nvml layer crashed: %s", exc)
    try:
        name, vram = _gpu_via_powershell()
        if name or vram:
            return name, vram
    except Exception as exc:
        log.debug("powershell layer crashed: %s", exc)
    return None, None


# ── Compute micro-benchmark ──────────────────────────────────────────────────

_score_lock = threading.Lock()
_score_cache: Optional[float] = None


def compute_score(force: bool = False) -> float:
    """Time a fixed sha256 loop and return iterations/second (cached after first call).

    The benchmark is deliberately IO-free and allocation-light (one 1 KiB buffer,
    digest discarded) so the number reflects raw CPU hashing throughput and is
    comparable between machines. It is reported to the Master as "1|<score>".
    """
    global _score_cache
    with _score_lock:
        if _score_cache is not None and not force:
            return _score_cache
        digest = hashlib.sha256
        buf = BENCH_BUFFER
        n = BENCH_ITERATIONS
        t0 = time.perf_counter()
        for _ in range(n):
            digest(buf).digest()
        elapsed = time.perf_counter() - t0
        _score_cache = n / elapsed if elapsed > 0 else 0.0
        return _score_cache


def executor_version_tag() -> str:
    """The exact string every worker must put in WorkerCapability.executor_version."""
    try:
        score = compute_score()
    except Exception:
        score = 0.0
    return f"{EXECUTOR_VERSION_PREFIX}|{score:.3f}"


# ── Static capability detection ──────────────────────────────────────────────

def _cpu_cores() -> int:
    if _PSUTIL:
        try:
            physical = psutil.cpu_count(logical=False)
            if physical:
                return int(physical)
            logical = psutil.cpu_count(logical=True)
            if logical:
                return int(logical)
        except Exception as exc:
            log.debug("psutil cpu_count failed: %s", exc)
    try:
        return int(__import__("os").cpu_count() or 1)
    except Exception:
        return 1


def _ram_gb() -> float:
    if _PSUTIL:
        try:
            return round(psutil.virtual_memory().total / 1024 ** 3, 2)
        except Exception as exc:
            log.debug("psutil virtual_memory failed: %s", exc)
    return 0.0


def detect_capabilities(worker_id: str, max_slots: int) -> WorkerCapability:
    """Build a WorkerCapability from the REAL hardware of this machine.

    Never raises: every probe has a fallback, the GPU cascade ends in (None, None).
    Safe to call from any thread; GPU detection is cached inside _nvml().
    """
    cpu = 1
    ram = 0.0
    gpu_name: Optional[str] = None
    gpu_vram: Optional[float] = None
    try:
        cpu = _cpu_cores()
    except Exception:
        pass
    try:
        ram = _ram_gb()
    except Exception:
        pass
    try:
        gpu_name, gpu_vram = detect_gpu()
    except Exception:
        gpu_name, gpu_vram = None, None

    try:
        hostname = platform.node() or "unknown-host"
    except Exception:
        hostname = "unknown-host"

    try:
        version = executor_version_tag()
    except Exception:
        version = EXECUTOR_VERSION_PREFIX

    return WorkerCapability(
        worker_id=str(worker_id),
        hostname=hostname,
        cpu_cores=int(cpu),
        ram_gb=float(ram),
        gpu_name=gpu_name if gpu_name else None,
        gpu_vram_gb=float(gpu_vram) if gpu_vram is not None else None,
        max_slots=max(1, int(max_slots)),
        executor_version=version,
    )


# ── Runtime monitor ──────────────────────────────────────────────────────────

class ResourceMonitor:
    """Slot accounting + live utilisation sampling for one Worker process."""

    def __init__(self, max_slots: int):
        self._max = max(1, int(max_slots))
        self._active = 0
        self._lock = threading.Lock()
        self._cap_lock = threading.Lock()
        self._cap: Optional[WorkerCapability] = None
        self._sample_lock = threading.Lock()
        self._sample_stop = threading.Event()
        self._sampler = None
        self._gpu_identity = {}
        self._sample = {"gpu_pct": None, "cpu_pct": None, "status": "initializing",
                        "error": "resource sampler not started", "sampled_at": None}

    def start_sampling(self, uuid=None, pci_bus_id=None):
        self._gpu_identity = {"uuid": uuid, "pci_bus_id": pci_bus_id}
        if self._sampler is not None:
            return
        def sample_loop():
            if _PSUTIL:
                psutil.cpu_percent(interval=None)  # prime this sampling thread
            while not self._sample_stop.is_set():
                start = time.monotonic()
                try:
                    cpu = float(psutil.cpu_percent(interval=None)) if _PSUTIL else None
                except Exception:
                    cpu = None
                sample = {"cpu_pct": cpu,
                          "gpu_pct": None, "sampled_at": time.time(), "interval_s": 1.0,
                          "source": "nvml", "scope": "whole_device", "status": "unavailable"}
                try:
                    nv = _nvml()
                    if nv is None:
                        raise RuntimeError(_nvml_error or "NVML unavailable; install nvidia-ml-py")
                    if uuid:
                        handle = nv.nvmlDeviceGetHandleByUUID(str(uuid))
                    elif pci_bus_id:
                        handle = nv.nvmlDeviceGetHandleByPciBusId(str(pci_bus_id))
                    else:
                        handle = nv.nvmlDeviceGetHandleByIndex(0)
                    value = float(nv.nvmlDeviceGetUtilizationRates(handle).gpu)
                    if not 0 <= value <= 100:
                        raise RuntimeError("invalid NVML utilization sample")
                    gpu_uuid = nv.nvmlDeviceGetUUID(handle)
                    if isinstance(gpu_uuid, bytes):
                        gpu_uuid = gpu_uuid.decode()
                    sample.update(gpu_pct=value, gpu_uuid=gpu_uuid, status="ok", error=None)
                    mem = nv.nvmlDeviceGetMemoryInfo(handle)
                    sample.update(memory_used_bytes=int(mem.used), memory_total_bytes=int(mem.total))
                except Exception as exc:
                    sample["error"] = str(exc)
                with self._sample_lock:
                    self._sample = sample
                self._sample_stop.wait(max(0.01, 1.0-(time.monotonic()-start)))
        self._sampler = threading.Thread(target=sample_loop, name="resource-sampler", daemon=True)
        self._sampler.start()

    def resource_sample(self):
        with self._sample_lock:
            return dict(self._sample)

    def stop_sampling(self):
        self._sample_stop.set()
        if self._sampler:
            self._sampler.join(timeout=2)

    # ── Slot accounting (called by W5) ────────────────────────────────────────

    def slot_acquire(self) -> int:
        """Reserve one slot. Returns the number of busy slots after the increment."""
        with self._lock:
            self._active += 1
            return self._active

    def slot_release(self) -> int:
        with self._lock:
            self._active = max(0, self._active - 1)
            return self._active

    def free_slots(self) -> int:
        with self._lock:
            return max(0, self._max - self._active)

    def busy_slots(self) -> int:
        with self._lock:
            return self._active

    @property
    def max_slots(self) -> int:
        return self._max

    # ── Static capability (cached per process) ────────────────────────────────

    def capability(self, worker_id: str) -> WorkerCapability:
        with self._cap_lock:
            if self._cap is None or self._cap.worker_id != worker_id:
                self._cap = detect_capabilities(worker_id, self._max)
            return self._cap

    def compute_score(self) -> float:
        return compute_score()

    # ── Resource sampling ─────────────────────────────────────────────────────

    def cpu_pct(self) -> Optional[float]:
        if not _PSUTIL:
            return None
        try:
            # interval=None returns immediately, comparing against the previous
            # call. A blocking 0.3s sample would eat 60% of the 0.5s heartbeat
            # period and stall the beat, which now defines offline detection.
            return float(psutil.cpu_percent(interval=None))
        except Exception:
            return None

    def gpu_pct(self) -> Optional[float]:
        """GPU utilisation in percent, or None when no usable NVML device exists."""
        nv = _nvml()
        if nv is None:
            return None
        try:
            if int(nv.nvmlDeviceGetCount()) <= 0:
                return None
            handle = nv.nvmlDeviceGetHandleByIndex(0)
            return float(nv.nvmlDeviceGetUtilizationRates(handle).gpu)
        except Exception:
            return None
