"""Keep Windows platform detection independent of the WMI service."""

import sys


def use_native_windows_platform() -> None:
    """Use platform's native Windows fallback before importing GPU libraries.

    Python 3.12+ tries WMI even for platform.node() and platform.machine().
    Its WMI call has no timeout and can block startup (including torch imports)
    indefinitely. Raising OSError selects the standard library's existing
    sys.getwindowsversion/registry fallback without changing the host system.
    """
    if sys.platform != "win32":
        return
    import platform

    if hasattr(platform, "_wmi_query"):
        platform._wmi_query = _wmi_unavailable


def _wmi_unavailable(*_args):
    raise OSError("DormPool uses native Windows platform detection")
