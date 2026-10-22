"""Thread-safe counters and periodic snapshots. The lock is for the alert workers."""

from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any


class Metrics:
    def __init__(
        self,
        snapshot_path: str | os.PathLike[str] | None = None,
        history_path: str | os.PathLike[str] | None = None,
        *,
        history_max_lines: int = 10_000,
    ) -> None:
        self._lock = threading.Lock()
        self._counters: dict[str, int] = {}
        self._gauges: dict[str, dict[str, Any]] = {}
        self._started = time.time()
        self._snapshot_path = Path(snapshot_path) if snapshot_path else None
        self._history_path = Path(history_path) if history_path else None
        self._history_max_lines = history_max_lines
        self._history_lines = 0

        for path in (self._snapshot_path, self._history_path):
            if path is not None:
                path.parent.mkdir(parents=True, exist_ok=True)

    # --- recording ---------------------------------------------------------

    def inc(self, name: str, value: int = 1) -> None:
        with self._lock:
            self._counters[name] = self._counters.get(name, 0) + value

    def set_gauge(self, target: str, **values: Any) -> None:
        with self._lock:
            self._gauges.setdefault(target, {}).update(values)

    def drop_gauge(self, target: str) -> None:
        with self._lock:
            self._gauges.pop(target, None)

    # --- reading -----------------------------------------------------------

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "ts": time.time(),
                "uptime_seconds": round(time.time() - self._started, 3),
                "counters": dict(self._counters),
                "targets": {k: dict(v) for k, v in self._gauges.items()},
            }

    def get(self, name: str) -> int:
        with self._lock:
            return self._counters.get(name, 0)

    # --- persistence -------------------------------------------------------

    def flush(self) -> dict[str, Any]:
        snapshot = self.snapshot()

        if self._snapshot_path is not None:
            # Write-then-rename, so a reader never sees a half-written file.
            tmp = self._snapshot_path.with_suffix(self._snapshot_path.suffix + ".tmp")
            tmp.write_text(json.dumps(snapshot, indent=2, default=str), encoding="utf-8")
            tmp.replace(self._snapshot_path)

        if self._history_path is not None:
            self._append_history(snapshot)

        return snapshot

    def _append_history(self, snapshot: dict[str, Any]) -> None:
        assert self._history_path is not None
        line = json.dumps(snapshot, separators=(",", ":"), default=str)
        with self._history_path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")

        self._history_lines += 1
        if self._history_lines >= self._history_max_lines:
            self._trim_history()

    def _trim_history(self) -> None:
        assert self._history_path is not None
        keep = max(1, self._history_max_lines // 2)
        lines = self._history_path.read_text(encoding="utf-8").splitlines()[-keep:]
        self._history_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        self._history_lines = len(lines)
