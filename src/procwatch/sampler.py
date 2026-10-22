"""Non-blocking resource sampling: ``cpu_percent(interval=None)`` on cached handles,
with the supervisor's tick as the measurement window. The prototype's
``interval=1`` slept a second per target.
"""

from __future__ import annotations

from dataclasses import dataclass

import psutil


@dataclass(slots=True, frozen=True)
class Sample:
    cpu_percent: float
    memory_mb: float
    num_threads: int
    num_children: int
    alive: bool = True

    @classmethod
    def dead(cls) -> Sample:
        return cls(0.0, 0.0, 0, 0, alive=False)


class ResourceSampler:
    def __init__(self, *, include_children: bool = True) -> None:
        self.include_children = include_children
        self._processes: dict[int, psutil.Process] = {}

    def prime(self, pid: int) -> None:
        """The first reading is always 0.0; take it when a target starts."""
        process = self._handle(pid)
        if process is None:
            return
        try:
            process.cpu_percent(interval=None)
            if self.include_children:
                for child in process.children(recursive=True):
                    child.cpu_percent(interval=None)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

    def sample(self, pid: int) -> Sample:
        process = self._handle(pid)
        if process is None:
            return Sample.dead()

        try:
            with process.oneshot():
                cpu = process.cpu_percent(interval=None)
                memory = process.memory_info().rss
                threads = process.num_threads()

            children = 0
            if self.include_children:
                for child in process.children(recursive=True):
                    try:
                        with child.oneshot():
                            cpu += child.cpu_percent(interval=None)
                            memory += child.memory_info().rss
                        children += 1
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue

        except (psutil.NoSuchProcess, psutil.ZombieProcess):
            self.forget(pid)
            return Sample.dead()
        except psutil.AccessDenied:
            # Visible but not inspectable.
            return Sample(0.0, 0.0, 0, 0, alive=True)

        return Sample(
            cpu_percent=round(cpu, 2),
            memory_mb=round(memory / (1024 * 1024), 2),
            num_threads=threads,
            num_children=children,
        )

    def forget(self, pid: int) -> None:
        """After a restart gives a new PID."""
        self._processes.pop(pid, None)

    def _handle(self, pid: int) -> psutil.Process | None:
        process = self._processes.get(pid)
        if process is not None:
            return process if process.is_running() else None

        try:
            process = psutil.Process(pid)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return None

        self._processes[pid] = process
        return process
