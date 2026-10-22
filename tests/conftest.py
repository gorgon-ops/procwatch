from __future__ import annotations

import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from procwatch import Config, Supervisor, events

#: Reports progress for a while, then stops while its heartbeat thread keeps
#: beating: alive but stuck.
STALLING_WORKER = """\
import sys, time
from procwatch import Heartbeat

with Heartbeat.from_env(interval=0.2) as hb:
    for i in range(3):
        hb.progress(done=i)
        time.sleep(0.2)
    # Stop ticking, keep breathing.
    time.sleep(300)
"""

#: A worker that exits with a failure code almost immediately.
CRASHING_WORKER = """\
import sys, time
time.sleep(0.2)
sys.exit(3)
"""

CLEAN_WORKER = """\
import time
time.sleep(0.3)
"""


@pytest.fixture(autouse=True)
def quiet_logging():
    events.setup(None, console_level="CRITICAL")
    yield
    logger = events.get_logger()
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()


@pytest.fixture
def worker_script(tmp_path: Path) -> Callable[[str, str], Path]:
    def write(source: str, name: str = "worker.py") -> Path:
        path = tmp_path / name
        path.write_text(source, encoding="utf-8")
        return path

    return write


class RunningSupervisor:
    """A supervisor running on a background thread, for end-to-end tests."""

    def __init__(self, config: Config) -> None:
        self.supervisor = Supervisor(config)
        self._thread = threading.Thread(
            target=self.supervisor.run,
            kwargs={"install_signal_handlers": False},
            daemon=True,
        )

    def __enter__(self) -> Supervisor:
        self._thread.start()
        return self.supervisor

    def __exit__(self, *exc: Any) -> None:
        self.supervisor.stop()
        self._thread.join(timeout=20)


def wait_until(
    predicate: Callable[[], bool], *, timeout: float = 20.0, interval: float = 0.1
) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def base_config(**overrides: Any) -> Config:
    settings: dict[str, Any] = {
        "port": 0,  # OS-picked, so tests never collide
        "interval": 0.2,
        "log_file": None,
        "metrics_file": None,
        "metrics_history": None,
        "metrics_interval": 3600.0,
        "dump_timeout": 1.0,
        "output_dir": None,  # tests that want capture pass a tmp_path
    }
    settings.update(overrides)
    return Config(**settings)
