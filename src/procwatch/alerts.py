"""Alerts are delivered on a worker thread, so a slow webhook never stalls the loop
that is noticing hangs, and rate limited per (target, kind) against flapping.
"""

from __future__ import annotations

import json
import queue
import subprocess
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any

from . import events


@dataclass(slots=True)
class Alert:
    kind: str
    target: str
    message: str
    level: str = "WARNING"
    data: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "target": self.target,
            "level": self.level,
            "message": self.message,
            "ts": time.time(),
            "data": self.data,
        }


class Sink:
    def __init__(self, *, levels: list[str] | None = None, min_interval: float = 0.0):
        self.levels = {lvl.upper() for lvl in levels} if levels else None
        self.min_interval = min_interval
        self._last_sent: dict[tuple[str, str], float] = {}

    def wants(self, alert: Alert) -> bool:
        if self.levels is not None and alert.level.upper() not in self.levels:
            return False

        if self.min_interval > 0:
            key = (alert.target, alert.kind)
            now = time.monotonic()
            previous = self._last_sent.get(key)
            if previous is not None and now - previous < self.min_interval:
                return False
            self._last_sent[key] = now

        return True

    def deliver(self, alert: Alert) -> None:  # pragma: no cover - interface
        raise NotImplementedError


class LogSink(Sink):
    """Always available, never fails."""

    def deliver(self, alert: Alert) -> None:
        events.emit(
            alert.level,
            f"alert_{alert.kind}",
            alert.message,
            target=alert.target,
            **alert.data,
        )


class WebhookSink(Sink):
    def __init__(self, url: str, *, timeout: float = 10.0, **kwargs: Any):
        super().__init__(**kwargs)
        self.url = url
        self.timeout = timeout

    def deliver(self, alert: Alert) -> None:
        payload = json.dumps(alert.as_dict()).encode("utf-8")
        request = urllib.request.Request(
            self.url,
            data=payload,
            headers={"Content-Type": "application/json", "User-Agent": "procwatch/1.0"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=self.timeout) as response:
            response.read(1024)


class CommandSink(Sink):
    """Runs a local command with the alert as JSON on stdin."""

    def __init__(self, command: list[str], *, timeout: float = 30.0, **kwargs: Any):
        super().__init__(**kwargs)
        self.command = command
        self.timeout = timeout

    def deliver(self, alert: Alert) -> None:
        subprocess.run(
            self.command,
            input=json.dumps(alert.as_dict()),
            text=True,
            timeout=self.timeout,
            capture_output=True,
            check=True,
        )


class AlertManager:
    def __init__(self, sinks: list[Sink], *, queue_size: int = 256) -> None:
        self._sinks = sinks
        self._queue: queue.Queue[Alert | None] = queue.Queue(maxsize=queue_size)
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if not self._sinks or self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._worker, name="procwatch-alerts", daemon=True
        )
        self._thread.start()

    def send(self, alert: Alert) -> None:
        """Never blocks and never raises."""
        if not self._sinks:
            return
        try:
            self._queue.put_nowait(alert)
        except queue.Full:
            events.emit(
                "WARNING",
                "alert_dropped",
                "Alert queue full, dropping notification",
                target=alert.target,
                kind=alert.kind,
            )

    def stop(self, timeout: float = 5.0) -> None:
        if self._thread is None:
            return
        try:
            self._queue.put_nowait(None)
        except queue.Full:
            pass
        self._thread.join(timeout)
        self._thread = None

    def _worker(self) -> None:
        while True:
            alert = self._queue.get()
            if alert is None:
                return

            for sink in self._sinks:
                if not sink.wants(alert):
                    continue
                try:
                    sink.deliver(alert)
                except (urllib.error.URLError, OSError, subprocess.SubprocessError) as exc:
                    events.emit(
                        "ERROR",
                        "alert_failed",
                        f"{type(sink).__name__} could not deliver: {exc}",
                        target=alert.target,
                        kind=alert.kind,
                    )
                except Exception as exc:
                    events.emit(
                        "ERROR",
                        "alert_failed",
                        f"{type(sink).__name__} raised {type(exc).__name__}: {exc}",
                        target=alert.target,
                        kind=alert.kind,
                    )
