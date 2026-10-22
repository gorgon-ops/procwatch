"""The client half, imported by the watched process.

The stack dump is produced inside the watched process, so it is the hung
program's own stack (the prototype dumped the watchdog's). A thread holding the
GIL in a C call blocks this too; the missing heartbeats still give it away.
Everything here is best-effort: monitoring must never crash what it monitors.
"""

from __future__ import annotations

import os
import select
import socket
import sys
import threading
import time
import traceback
from types import TracebackType
from typing import Any

from . import protocol

#: Set by the supervisor for processes it launches.
ENV_ADDRESS = "PROCWATCH_ADDR"
ENV_TARGET = "PROCWATCH_TARGET"
#: Tells a launched process from any local process that knows the port and name.
ENV_TOKEN = "PROCWATCH_TOKEN"

DEFAULT_INTERVAL = 1.0


def collect_stacks() -> dict[str, list[str]]:
    names = {thread.ident: thread.name for thread in threading.enumerate()}
    stacks: dict[str, list[str]] = {}
    for ident, frame in sys._current_frames().items():
        label = f"{names.get(ident, 'unknown')} ({ident})"
        stacks[label] = [line.rstrip() for line in traceback.format_stack(frame)]
    return stacks


class Heartbeat:
    def __init__(
        self,
        address: tuple[str, int] | None,
        name: str,
        *,
        token: str = "",
        interval: float = DEFAULT_INTERVAL,
        reconnect_backoff: float = 1.0,
        max_backoff: float = 30.0,
    ) -> None:
        self.address = address
        self.name = name
        self.token = token
        self.interval = interval
        self.reconnect_backoff = reconnect_backoff
        self.max_backoff = max_backoff

        self._sock: socket.socket | None = None
        self._decoder = protocol.FrameDecoder()
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._send_lock = threading.Lock()
        self._sequence = 0
        self._progress: dict[str, Any] = {}
        self._last_tick = time.monotonic()

    # --- construction ---------------------------------------------------------

    @classmethod
    def from_env(cls, **kwargs: Any) -> Heartbeat:
        """Inert when the variables are absent (started by hand), so call sites need no
        branching.
        """
        raw = os.environ.get(ENV_ADDRESS)
        name = os.environ.get(ENV_TARGET, f"pid-{os.getpid()}")
        token = os.environ.get(ENV_TOKEN, "")

        address = None
        if raw:
            host, _, port = raw.rpartition(":")
            if host and port.isdigit():
                address = (host, int(port))

        kwargs.setdefault("token", token)
        return cls(address, name, **kwargs)

    @property
    def enabled(self) -> bool:
        return self.address is not None

    # --- lifecycle ---------------------------------------------------------

    def start(self) -> Heartbeat:
        if not self.enabled or self._thread is not None:
            return self

        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="procwatch-heartbeat", daemon=True
        )
        self._thread.start()
        return self

    def stop(self, timeout: float = 2.0) -> None:
        if self._thread is None:
            return

        self._stop.set()
        self._send({"type": protocol.BYE, "name": self.name, "pid": os.getpid()})
        self._thread.join(timeout)
        self._thread = None
        self._close()

    def __enter__(self) -> Heartbeat:
        return self.start()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.stop()

    # --- application-facing ------------------------------------------------

    def tick(self) -> None:
        """Mark forward progress from the working thread.

        Heartbeats come from a background thread and keep arriving while the worker is
        wedged; the time since the last tick travels with them, separating dead from
        stuck.
        """
        self._last_tick = time.monotonic()

    def progress(self, **values: Any) -> None:
        """``hb.progress(done=n, current=path)``: reported in metrics, and counts as a tick."""
        self._progress.update(values)
        self.tick()

    def beat(self) -> None:
        if not self.enabled:
            return
        self._sequence += 1
        self._send(
            {
                "type": protocol.HEARTBEAT,
                "name": self.name,
                "pid": os.getpid(),
                "seq": self._sequence,
                "progress": dict(self._progress),
                "stalled_for": round(time.monotonic() - self._last_tick, 3),
            }
        )

    # --- internals ---------------------------------------------------------

    def _run(self) -> None:
        backoff = self.reconnect_backoff
        next_beat = 0.0

        while not self._stop.is_set():
            if self._sock is None:
                if not self._connect():
                    # Connection refused: the supervisor may not be up yet.
                    if self._stop.wait(backoff):
                        return
                    backoff = min(backoff * 2, self.max_backoff)
                    continue
                backoff = self.reconnect_backoff
                next_beat = 0.0

            now = time.monotonic()
            if now >= next_beat:
                self.beat()
                next_beat = now + self.interval

            self._poll(timeout=max(0.0, next_beat - time.monotonic()))

    def _connect(self) -> bool:
        assert self.address is not None
        try:
            sock = socket.create_connection(self.address, timeout=5.0)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.settimeout(None)
        except OSError:
            return False

        self._sock = sock
        self._decoder = protocol.FrameDecoder()
        self._send(
            {
                "type": protocol.HELLO,
                "name": self.name,
                "token": self.token,
                "pid": os.getpid(),
                "interval": self.interval,
                "executable": sys.executable,
            }
        )
        return True

    def _send(self, message: dict[str, Any]) -> None:
        sock = self._sock
        if sock is None:
            return
        try:
            with self._send_lock:
                sock.sendall(protocol.encode(message))
        except (OSError, protocol.ProtocolError):
            self._close()

    def _poll(self, timeout: float) -> None:
        sock = self._sock
        if sock is None:
            return

        try:
            readable, _, _ = select.select([sock], [], [], timeout)
        except (OSError, ValueError):
            self._close()
            return

        if not readable:
            return

        try:
            data = sock.recv(4096)
        except OSError:
            self._close()
            return

        if not data:
            self._close()
            return

        try:
            for message in self._decoder.feed(data):
                self._handle(message)
        except protocol.ProtocolError:
            self._close()

    def _handle(self, message: dict[str, Any]) -> None:
        if message.get("type") == protocol.DUMP_REQUEST:
            self._send(
                {
                    "type": protocol.STACKS,
                    "name": self.name,
                    "pid": os.getpid(),
                    "stacks": collect_stacks(),
                }
            )

    def _close(self) -> None:
        sock, self._sock = self._sock, None
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass
