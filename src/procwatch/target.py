from __future__ import annotations

import collections
import enum
import os
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Any

import psutil

from . import events
from .heartbeat import ENV_ADDRESS, ENV_TARGET, ENV_TOKEN
from .processguard import ProcessGuard


class HangAction(enum.StrEnum):
    RESTART = "restart"
    KILL = "kill"
    LOG = "log"


class State(enum.StrEnum):
    PENDING = "pending"      # not started yet
    STARTING = "starting"    # inside the startup grace period
    RUNNING = "running"      # heartbeats arriving
    HUNG = "hung"            # heartbeat deadline missed
    RESTARTING = "restarting"
    EXITED = "exited"        # finished on its own
    FAILED = "failed"        # restart budget exhausted; no longer supervised


@dataclass(slots=True)
class TargetSpec:
    name: str

    # Exactly one: launch a command, or attach to a running PID.
    cmd: list[str] | None = None
    pid: int | None = None

    cwd: str | None = None
    env: dict[str, str] = field(default_factory=dict)

    # Liveness
    heartbeat_timeout: float = 15.0
    startup_grace: float = 20.0
    require_heartbeat: bool = True
    #: Seconds without tick()/progress() before the target counts as stalled, even
    #: while heartbeats arrive. None checks liveness only.
    progress_timeout: float | None = None

    # Resource ceilings; None disables the check.
    max_cpu: float | None = None
    max_memory_mb: float | None = None
    #: Consecutive breaching samples, so one spike is ignored.
    breach_samples: int = 3
    on_breach: HangAction = HangAction.LOG

    # Failure handling
    on_hang: HangAction = HangAction.RESTART
    max_restarts: int = 5
    restart_window: float = 300.0
    backoff: float = 2.0
    max_backoff: float = 60.0
    stop_timeout: float = 10.0
    #: For services that should never stop.
    restart_on_exit: bool = False

    # Output
    #: Per-target file, instead of unattributed in the supervisor's console.
    capture_output: bool = True
    output_max_bytes: int = 5_000_000

    @property
    def managed(self) -> bool:
        return self.cmd is not None


@dataclass(slots=True)
class LaunchContext:
    """Everything a target needs from its supervisor in order to start."""

    address: tuple[str, int]
    token: str
    guard: ProcessGuard | None = None
    output_dir: Path | None = None

    @property
    def child_setup(self) -> Any:
        return self.guard.child_setup() if self.guard is not None else None


class Target:
    def __init__(self, spec: TargetSpec) -> None:
        self.spec = spec
        self.state = State.PENDING
        self.pid: int | None = spec.pid
        self.process: subprocess.Popen[bytes] | None = None

        self.last_heartbeat: float | None = None
        self.started_at: float | None = None
        self.last_sample: Any = None
        self.progress: dict[str, Any] = {}
        #: Seconds since the last forward progress, as of the latest heartbeat.
        self.stalled_for: float | None = None

        self.restart_count = 0
        self.consecutive_restarts = 0
        self._restart_times: collections.deque[float] = collections.deque()
        self.retry_at: float | None = None
        self.cpu_breaches = 0
        self.memory_breaches = 0

        #: Hang detected, waiting for stacks before acting. Deferred rather than blocking,
        #: so one unresponsive target cannot delay the others.
        self.hang_deadline: float | None = None

        self.output_path: Path | None = None
        self._output: Any = None

    @property
    def name(self) -> str:
        return self.spec.name

    @property
    def supervised(self) -> bool:
        return self.state not in (State.FAILED, State.EXITED)

    # --- lifecycle ---------------------------------------------------------

    def start(self, launch: LaunchContext, now: float) -> bool:
        if not self.spec.managed:
            return self._attach(now)

        env = os.environ.copy()
        env.update(self.spec.env)
        env[ENV_ADDRESS] = f"{launch.address[0]}:{launch.address[1]}"
        env[ENV_TARGET] = self.spec.name
        env[ENV_TOKEN] = launch.token

        cwd = self.spec.cwd
        if cwd is not None and not Path(cwd).is_dir():
            events.emit(
                "ERROR",
                "target_start_failed",
                f"Working directory does not exist: {cwd}",
                target=self.name,
            )
            self.state = State.FAILED
            return False

        try:
            output = self._open_output(launch.output_dir)
        except OSError as exc:
            events.emit(
                "WARNING",
                "output_capture_failed",
                f"Could not open the output file, inheriting the console: {exc}",
                target=self.name,
            )
            output = None

        try:
            self.process = subprocess.Popen(
                self.spec.cmd or [],
                cwd=cwd,
                env=env,
                stdin=subprocess.DEVNULL,
                # stderr merged in to keep the interleaving. A file, not a pipe, so a chatty
                # child can never block on a full pipe buffer.
                stdout=output,
                stderr=subprocess.STDOUT if output is not None else None,
                preexec_fn=launch.child_setup,
            )
        except (OSError, ValueError) as exc:
            events.emit(
                "ERROR",
                "target_start_failed",
                f"Could not launch: {exc}",
                target=self.name,
                cmd=self.spec.cmd,
            )
            self._close_output()
            self.state = State.FAILED
            return False

        self.pid = self.process.pid

        if launch.guard is not None and not launch.guard.adopt(self.pid):
            events.emit(
                "DEBUG",
                "guard_adopt_failed",
                "Could not bring the process under the process guard",
                target=self.name,
                pid=self.pid,
            )
        self.started_at = now
        self.last_heartbeat = None
        self.stalled_for = None
        self.hang_deadline = None
        self.cpu_breaches = self.memory_breaches = 0
        self.state = State.STARTING

        events.emit(
            "NOTIFY",
            "target_started",
            f"Started (pid {self.pid})",
            target=self.name,
            pid=self.pid,
            cmd=self.spec.cmd,
            cwd=cwd,
        )
        return True

    def _attach(self, now: float) -> bool:
        if self.pid is None or not psutil.pid_exists(self.pid):
            events.emit(
                "ERROR",
                "target_attach_failed",
                f"No such process: {self.pid}",
                target=self.name,
                pid=self.pid,
            )
            self.state = State.FAILED
            return False

        self.started_at = now
        self.state = State.STARTING
        events.emit(
            "NOTIFY",
            "target_attached",
            f"Attached to pid {self.pid}",
            target=self.name,
            pid=self.pid,
        )
        return True

    def release(self) -> None:
        """Safe to call more than once, and on a target that will run again."""
        self._close_output()

    def poll_exit(self) -> int | None:
        if self.process is None:
            return None
        return self.process.poll()

    def is_alive(self) -> bool:
        if self.process is not None:
            return self.process.poll() is None
        return self.pid is not None and psutil.pid_exists(self.pid)

    def terminate(self, *, timeout: float | None = None) -> None:
        """SIGTERM first, SIGKILL if needed, children included."""
        if self.pid is None:
            return

        grace = self.spec.stop_timeout if timeout is None else timeout

        try:
            parent = psutil.Process(self.pid)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            self._reap()
            return

        try:
            family = [*parent.children(recursive=True), parent]
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            family = [parent]

        for proc in family:
            try:
                proc.terminate()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        _, alive = psutil.wait_procs(family, timeout=grace)

        for proc in alive:
            try:
                proc.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        if alive:
            psutil.wait_procs(alive, timeout=3)
            events.emit(
                "WARNING",
                "target_killed",
                f"Did not exit within {grace}s; killed {len(alive)} process(es)",
                target=self.name,
                pid=self.pid,
            )

        self._reap()

    def _reap(self) -> None:
        if self.process is not None:
            try:
                self.process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                pass
        self._close_output()

    # --- output capture ----------------------------------------------------

    def _open_output(self, output_dir: Path | None) -> IO[bytes] | None:
        if not self.spec.capture_output or output_dir is None:
            return None

        output_dir.mkdir(parents=True, exist_ok=True)
        # Names come from config: flatten anything path-like.
        safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in self.name)
        path = output_dir / f"{safe}.out"

        if path.exists() and path.stat().st_size > self.spec.output_max_bytes:
            previous = path.with_suffix(".out.1")
            previous.unlink(missing_ok=True)
            path.rename(previous)

        self._close_output()
        self.output_path = path
        self._output = path.open("ab")
        return self._output

    def _close_output(self) -> None:
        if self._output is not None:
            try:
                self._output.close()
            except OSError:
                pass
            self._output = None

    def output_tail(self, lines: int = 20) -> list[str]:
        """The last lines the process wrote: usually why it died."""
        if self.output_path is None or not self.output_path.exists():
            return []

        try:
            with self.output_path.open("rb") as handle:
                # Seek: a crash loop can leave megabytes behind.
                handle.seek(0, os.SEEK_END)
                size = handle.tell()
                window = min(size, 8192)
                handle.seek(size - window)
                text = handle.read().decode("utf-8", errors="replace")
        except OSError:
            return []

        return [line for line in text.splitlines() if line.strip()][-lines:]

    # --- restart policy ----------------------------------------------------

    def restart_budget_left(self, now: float) -> bool:
        window_start = now - self.spec.restart_window
        while self._restart_times and self._restart_times[0] < window_start:
            self._restart_times.popleft()
        return len(self._restart_times) < self.spec.max_restarts

    def record_restart(self, now: float) -> float:
        self._restart_times.append(now)
        self.restart_count += 1
        self.consecutive_restarts += 1

        delay = min(
            self.spec.backoff * (2 ** (self.consecutive_restarts - 1)),
            self.spec.max_backoff,
        )
        self.retry_at = now + delay
        self.state = State.RESTARTING
        return delay

    def note_healthy(self, now: float) -> None:
        if (
            self.consecutive_restarts
            and self.started_at is not None
            and now - self.started_at > self.spec.restart_window / 2
        ):
            self.consecutive_restarts = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "state": self.state.value,
            "pid": self.pid,
            "restarts": self.restart_count,
            # time.monotonic() throughout: an NTP step mid-run would otherwise fake a hang.
            "uptime_seconds": (
                round(time.monotonic() - self.started_at, 1) if self.started_at else None
            ),
            "cpu_percent": getattr(self.last_sample, "cpu_percent", None),
            "memory_mb": getattr(self.last_sample, "memory_mb", None),
            "threads": getattr(self.last_sample, "num_threads", None),
            "last_heartbeat_age": (
                round(time.monotonic() - self.last_heartbeat, 2)
                if self.last_heartbeat
                else None
            ),
            "stalled_for": self.stalled_for,
            "progress": dict(self.progress),
        }
