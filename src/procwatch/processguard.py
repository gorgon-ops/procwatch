"""Make the OS kill our children if the supervisor dies without doing it itself.

Windows: a Job Object with KILL_ON_JOB_CLOSE, whose only handle is ours.
Linux: ``prctl(PR_SET_PDEATHSIG, SIGKILL)`` in each child.
Elsewhere: no equivalent, so ``ProcessGuard.available`` is False.
"""

from __future__ import annotations

import os
import signal
import sys
from collections.abc import Callable
from typing import Any

_IS_WINDOWS = sys.platform == "win32"
_IS_LINUX = sys.platform.startswith("linux")

#: signal.SIGKILL does not exist on Windows, where this module is still imported.
_SIGKILL = getattr(signal, "SIGKILL", 9)


class ProcessGuard:
    available = False

    description = "none (children may outlive a hard-killed supervisor)"

    def adopt(self, pid: int) -> bool:
        return False

    def child_setup(self) -> Callable[[], None] | None:
        return None

    def close(self) -> None:
        pass

    def __enter__(self) -> ProcessGuard:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


def create() -> ProcessGuard:
    if _IS_WINDOWS:
        try:
            return _JobObjectGuard()
        except OSError:
            return ProcessGuard()

    if _IS_LINUX:
        return _PdeathsigGuard()

    return ProcessGuard()


# --- Windows ---------------------------------------------------------------


class _JobObjectGuard(ProcessGuard):
    available = True
    description = "Windows Job Object (KILL_ON_JOB_CLOSE)"

    # winnt.h
    _EXTENDED_LIMIT_INFORMATION = 9
    _LIMIT_KILL_ON_JOB_CLOSE = 0x2000
    _PROCESS_SET_QUOTA = 0x0100
    _PROCESS_TERMINATE = 0x0001

    def __init__(self) -> None:
        import ctypes
        from ctypes import wintypes

        self._ctypes = ctypes
        self._kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

        class IO_COUNTERS(ctypes.Structure):
            _fields_ = [
                ("ReadOperationCount", ctypes.c_ulonglong),
                ("WriteOperationCount", ctypes.c_ulonglong),
                ("OtherOperationCount", ctypes.c_ulonglong),
                ("ReadTransferCount", ctypes.c_ulonglong),
                ("WriteTransferCount", ctypes.c_ulonglong),
                ("OtherTransferCount", ctypes.c_ulonglong),
            ]

        class BASIC_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("PerProcessUserTimeLimit", wintypes.LARGE_INTEGER),
                ("PerJobUserTimeLimit", wintypes.LARGE_INTEGER),
                ("LimitFlags", wintypes.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", wintypes.DWORD),
                ("Affinity", ctypes.POINTER(ctypes.c_ulong)),
                ("PriorityClass", wintypes.DWORD),
                ("SchedulingClass", wintypes.DWORD),
            ]

        class EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("BasicLimitInformation", BASIC_LIMIT_INFORMATION),
                ("IoInfo", IO_COUNTERS),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t),
            ]

        self._job = self._kernel32.CreateJobObjectW(None, None)
        if not self._job:
            raise ctypes.WinError(ctypes.get_last_error())

        limits = EXTENDED_LIMIT_INFORMATION()
        limits.BasicLimitInformation.LimitFlags = self._LIMIT_KILL_ON_JOB_CLOSE

        ok = self._kernel32.SetInformationJobObject(
            self._job,
            self._EXTENDED_LIMIT_INFORMATION,
            ctypes.byref(limits),
            ctypes.sizeof(limits),
        )
        if not ok:
            error = ctypes.get_last_error()
            self._kernel32.CloseHandle(self._job)
            self._job = None
            raise ctypes.WinError(error)

    def adopt(self, pid: int) -> bool:
        """The child is already running when its PID comes back, so a grandchild spawned
        in that sub-millisecond window could escape the job.
        """
        if self._job is None:
            return False

        handle = self._kernel32.OpenProcess(
            self._PROCESS_SET_QUOTA | self._PROCESS_TERMINATE, False, pid
        )
        if not handle:
            return False

        try:
            return bool(self._kernel32.AssignProcessToJobObject(self._job, handle))
        finally:
            self._kernel32.CloseHandle(handle)

    def close(self) -> None:
        if self._job is not None:
            self._kernel32.CloseHandle(self._job)
            self._job = None


# --- Linux -----------------------------------------------------------------


class _PdeathsigGuard(ProcessGuard):
    available = True
    description = "Linux prctl(PR_SET_PDEATHSIG, SIGKILL)"

    _PR_SET_PDEATHSIG = 1

    def child_setup(self) -> Callable[[], None] | None:
        def setup() -> None:
            try:
                import ctypes

                libc = ctypes.CDLL("libc.so.6", use_errno=True)
                libc.prctl(self._PR_SET_PDEATHSIG, _SIGKILL, 0, 0, 0)

                # The parent may have died during the fork; the signal will never arrive.
                if os.getppid() == 1:
                    os._exit(1)
            except Exception:
                pass

        return setup
