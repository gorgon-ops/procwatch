"""The supervisor: one ``selectors`` loop for every heartbeat connection and the tick."""

from __future__ import annotations

import hmac
import secrets
import selectors
import signal
import socket
import threading
import time
from pathlib import Path
from typing import Any

from . import events, processguard, protocol
from .alerts import Alert, AlertManager
from .config import Config
from .metrics import Metrics
from .sampler import ResourceSampler
from .target import HangAction, LaunchContext, State, Target


class _Connection:
    __slots__ = ("decoder", "peer", "sock", "target")

    def __init__(self, sock: socket.socket, peer: Any) -> None:
        self.sock = sock
        self.decoder = protocol.FrameDecoder()
        self.target: str | None = None
        self.peer = peer


class Supervisor:
    def __init__(
        self,
        config: Config,
        *,
        metrics: Metrics | None = None,
        alerts: AlertManager | None = None,
    ) -> None:
        self.config = config
        self.metrics = metrics or Metrics(
            config.metrics_file, config.metrics_history
        )
        self.alerts = alerts or AlertManager(config.sinks)

        self.targets: dict[str, Target] = {
            spec.name: Target(spec) for spec in config.targets
        }

        self._sampler = ResourceSampler()
        self._selector = selectors.DefaultSelector()
        self._listener: socket.socket | None = None
        self._connections: dict[int, _Connection] = {}
        self._by_target: dict[str, _Connection] = {}

        self._stop = threading.Event()
        self._next_tick = 0.0
        self._next_metrics_flush = 0.0
        self.bound_address: tuple[str, int] = config.address

        # A per-run secret, or any local process could beat on a hung worker's behalf.
        self.token = config.token or secrets.token_urlsafe(32)
        self._guard = processguard.create()
        self._launch = LaunchContext(
            address=self.bound_address,
            token=self.token,
            guard=self._guard,
            output_dir=Path(config.output_dir) if config.output_dir else None,
        )

    # --- public API --------------------------------------------------------

    def run(self, *, install_signal_handlers: bool = True) -> None:
        if install_signal_handlers:
            self._install_signal_handlers()

        self._listen()
        self.alerts.start()

        now = time.monotonic()
        self._next_tick = now
        self._next_metrics_flush = now + self.config.metrics_interval

        self._launch.address = self.bound_address

        for target in self.targets.values():
            if target.start(self._launch, now) and target.pid:
                self._sampler.prime(target.pid)

        events.emit(
            "NOTIFY",
            "supervisor_started",
            f"Supervising {len(self.targets)} target(s) on "
            f"{self.bound_address[0]}:{self.bound_address[1]}",
            targets=sorted(self.targets),
            orphan_guard=self._guard.description,
        )

        try:
            self._loop()
        finally:
            self.shutdown()

    def stop(self) -> None:
        """Safe to call from a signal handler."""
        self._stop.set()

    def shutdown(self) -> None:
        for target in self.targets.values():
            if target.spec.managed and target.is_alive():
                events.emit(
                    "INFO", "target_stopping", "Stopping", target=target.name
                )
                target.terminate()
            target.release()

        for connection in list(self._connections.values()):
            self._drop(connection)

        if self._listener is not None:
            try:
                self._selector.unregister(self._listener)
            except (KeyError, ValueError):
                pass
            self._listener.close()
            self._listener = None

        self._selector.close()
        self.metrics.flush()
        self.alerts.stop()
        # Last, so children are terminated on our terms, not by the job handle closing.
        self._guard.close()
        events.emit("NOTIFY", "supervisor_stopped", "Supervisor stopped")

    def status(self) -> dict[str, Any]:
        return {name: target.as_dict() for name, target in self.targets.items()}

    # --- event loop --------------------------------------------------------

    def _loop(self) -> None:
        while not self._stop.is_set():
            timeout = max(0.0, self._next_tick - time.monotonic())
            for key, _ in self._selector.select(timeout):
                key.data(key.fileobj)

            now = time.monotonic()
            if now >= self._next_tick:
                self._tick(now)
                self._next_tick = now + self.config.interval

            if now >= self._next_metrics_flush:
                self.metrics.flush()
                self._next_metrics_flush = now + self.config.metrics_interval

            if not any(t.supervised for t in self.targets.values()):
                events.emit(
                    "NOTIFY",
                    "all_targets_finished",
                    "No targets left to supervise",
                )
                return

    def _tick(self, now: float) -> None:
        for target in self.targets.values():
            try:
                self._check(target, now)
            except Exception as exc:
                # One misbehaving target must not take down the supervisor.
                events.emit(
                    "ERROR",
                    "check_failed",
                    f"Error while checking target: {type(exc).__name__}: {exc}",
                    target=target.name,
                )

    def _check(self, target: Target, now: float) -> None:
        if target.state is State.RESTARTING:
            if target.retry_at is not None and now >= target.retry_at:
                self._relaunch(target, now)
            return

        if not target.supervised:
            return

        if not target.is_alive():
            self._handle_exit(target, now)
            return

        self._sample(target, now)

        if target.hang_deadline is not None:
            if now >= target.hang_deadline:
                # The stack dump never came, which is itself informative.
                events.emit(
                    "WARNING",
                    "stack_dump_timeout",
                    "Target did not answer the stack-dump request",
                    target=target.name,
                )
                self._act_on_hang(target, now)
            return

        self._check_heartbeat(target, now)
        target.note_healthy(now)

    # --- checks ------------------------------------------------------------

    def _sample(self, target: Target, now: float) -> None:
        if target.pid is None:
            return

        sample = self._sampler.sample(target.pid)
        target.last_sample = sample
        if not sample.alive:
            return

        self.metrics.set_gauge(target.name, **target.as_dict())

        spec = target.spec
        if spec.max_cpu is not None and sample.cpu_percent > spec.max_cpu:
            target.cpu_breaches += 1
            if target.cpu_breaches == spec.breach_samples:
                self._breach(target, "cpu", sample.cpu_percent, spec.max_cpu, now)
        else:
            target.cpu_breaches = 0

        if spec.max_memory_mb is not None and sample.memory_mb > spec.max_memory_mb:
            target.memory_breaches += 1
            if target.memory_breaches == spec.breach_samples:
                self._breach(
                    target, "memory", sample.memory_mb, spec.max_memory_mb, now
                )
        else:
            target.memory_breaches = 0

    def _breach(
        self, target: Target, kind: str, value: float, limit: float, now: float
    ) -> None:
        unit = "%" if kind == "cpu" else " MB"
        message = f"{kind.upper()} at {value}{unit}, over the {limit}{unit} limit"

        self.metrics.inc(f"{kind}_breaches")
        events.emit(
            "WARNING",
            f"high_{kind}",
            message,
            target=target.name,
            value=value,
            limit=limit,
            samples=target.spec.breach_samples,
        )
        self.alerts.send(
            Alert(
                kind=f"high_{kind}",
                target=target.name,
                message=message,
                data={"value": value, "limit": limit, "pid": target.pid},
            )
        )

        if target.spec.on_breach is HangAction.KILL:
            self._terminate(target, reason=f"{kind} limit exceeded")
        elif target.spec.on_breach is HangAction.RESTART:
            self._restart(target, now, reason=f"{kind} limit exceeded")

    def _check_heartbeat(self, target: Target, now: float) -> None:
        spec = target.spec
        if not spec.require_heartbeat:
            target.state = State.RUNNING
            return

        if target.last_heartbeat is None:
            # Still inside the startup grace period.
            if target.started_at is not None and now - target.started_at > spec.startup_grace:
                self._hang_detected(
                    target,
                    now,
                    f"No heartbeat within {spec.startup_grace}s of starting",
                )
            return

        age = now - target.last_heartbeat
        if age > spec.heartbeat_timeout:
            self._hang_detected(target, now, f"No heartbeat for {age:.1f}s")
            return

        # Heartbeats come from a background thread, so they prove life, not progress.
        if (
            spec.progress_timeout is not None
            and target.stalled_for is not None
            and target.stalled_for > spec.progress_timeout
        ):
            self._hang_detected(
                target,
                now,
                f"Heartbeats fine but no progress for {target.stalled_for:.1f}s",
            )

    # --- failure handling --------------------------------------------------

    def _hang_detected(self, target: Target, now: float, reason: str) -> None:
        if target.state is State.HUNG:
            return

        target.state = State.HUNG
        self.metrics.inc("hangs_detected")

        events.emit(
            "WARNING",
            "hang_detected",
            reason,
            target=target.name,
            pid=target.pid,
            action=target.spec.on_hang.value,
        )
        self.alerts.send(
            Alert(
                kind="hang",
                target=target.name,
                message=f"{target.name}: {reason}",
                data={"pid": target.pid, "action": target.spec.on_hang.value},
            )
        )

        # The heartbeat thread answers while the main thread is stuck; if the GIL is
        # held nothing replies, and the deadline lets us act anyway.
        connection = self._by_target.get(target.name)
        if (
            self.config.dump_stacks_on_hang
            and connection is not None
            and self._send(connection, {"type": protocol.DUMP_REQUEST})
        ):
            target.hang_deadline = now + self.config.dump_timeout
            return

        self._act_on_hang(target, now)

    def _act_on_hang(self, target: Target, now: float) -> None:
        target.hang_deadline = None
        action = target.spec.on_hang

        if action is HangAction.LOG:
            return
        if action is HangAction.KILL:
            self._terminate(target, reason="hang")
        else:
            self._restart(target, now, reason="hang")

    def _handle_exit(self, target: Target, now: float) -> None:
        code = target.poll_exit()
        self.metrics.inc("target_exits")
        clean = code == 0
        # The tail is read below.

        events.emit(
            "INFO" if clean else "WARNING",
            "target_exited",
            f"Exited with code {code}",
            target=target.name,
            pid=target.pid,
            exit_code=code,
        )

        if not target.spec.managed:
            target.state = State.EXITED
            target.release()
            self.metrics.drop_gauge(target.name)
            return

        if clean and not target.spec.restart_on_exit:
            target.state = State.EXITED
            target.release()
            self.metrics.drop_gauge(target.name)
            return

        if not clean:
            # The traceback printed on the way out explains more than an exit code.
            tail = target.output_tail()
            if tail:
                events.emit(
                    "ERROR",
                    "target_output",
                    "Last output before exit",
                    target=target.name,
                    output=tail,
                )
            self.alerts.send(
                Alert(
                    kind="crash",
                    target=target.name,
                    message=f"{target.name} exited with code {code}",
                    data={"exit_code": code, "pid": target.pid, "output": tail},
                )
            )

        self._restart(target, now, reason=f"exit code {code}")

    def _restart(self, target: Target, now: float, *, reason: str) -> None:
        if not target.spec.managed:
            events.emit(
                "WARNING",
                "restart_impossible",
                "Cannot restart a target procwatch did not launch",
                target=target.name,
            )
            target.state = State.HUNG
            return

        if not target.restart_budget_left(now):
            message = (
                f"Restart budget exhausted: {target.spec.max_restarts} restarts "
                f"in {target.spec.restart_window}s. Giving up."
            )
            events.emit(
                "CRITICAL", "target_failed", message, target=target.name
            )
            self.alerts.send(
                Alert(
                    kind="gave_up",
                    target=target.name,
                    level="CRITICAL",
                    message=f"{target.name}: {message}",
                    data={"restarts": target.restart_count},
                )
            )
            target.state = State.FAILED
            self._terminate(target, reason="gave up", final_state=State.FAILED)
            return

        self._terminate(target, reason=reason, final_state=None)
        delay = target.record_restart(now)

        events.emit(
            "NOTIFY",
            "restart_scheduled",
            f"Restarting in {delay:.1f}s ({reason})",
            target=target.name,
            delay=round(delay, 2),
            attempt=target.restart_count,
        )
        self.metrics.inc("restarts")

    def _relaunch(self, target: Target, now: float) -> None:
        old_pid = target.pid
        if old_pid is not None:
            self._sampler.forget(old_pid)
        self._by_target.pop(target.name, None)

        if target.start(self._launch, now) and target.pid:
            self._sampler.prime(target.pid)

    def _terminate(
        self,
        target: Target,
        *,
        reason: str,
        final_state: State | None = State.EXITED,
    ) -> None:
        events.emit(
            "NOTIFY",
            "target_terminating",
            f"Terminating ({reason})",
            target=target.name,
            pid=target.pid,
        )
        target.terminate()
        if final_state is not None:
            target.state = final_state
            self.metrics.drop_gauge(target.name)

    # --- socket plumbing ---------------------------------------------------

    def _listen(self) -> None:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            listener.bind(self.config.address)
        except OSError as exc:
            listener.close()
            host, port = self.config.address
            raise RuntimeError(
                f"Cannot listen on {host}:{port}: {exc}. "
                f"Another procwatch may already be running."
            ) from exc

        listener.listen(64)
        listener.setblocking(False)
        self.bound_address = listener.getsockname()
        self._listener = listener
        self._selector.register(listener, selectors.EVENT_READ, self._accept)

    def _accept(self, listener: Any) -> None:
        try:
            sock, peer = listener.accept()
        except OSError:
            return

        sock.setblocking(False)
        connection = _Connection(sock, peer)
        self._connections[sock.fileno()] = connection
        self._selector.register(sock, selectors.EVENT_READ, self._readable)

    def _readable(self, sock: Any) -> None:
        connection = self._connections.get(sock.fileno())
        if connection is None:
            return

        try:
            data = sock.recv(8192)
        except (BlockingIOError, InterruptedError):
            return
        except OSError:
            self._drop(connection)
            return

        if not data:
            self._drop(connection)
            return

        try:
            for message in connection.decoder.feed(data):
                self._dispatch(connection, message)
        except protocol.ProtocolError as exc:
            events.emit(
                "WARNING",
                "protocol_error",
                str(exc),
                target=connection.target,
                peer=str(connection.peer),
            )
            self._drop(connection)

    def _dispatch(self, connection: _Connection, message: dict[str, Any]) -> None:
        kind = message.get("type")
        name = message.get("name")

        if kind == protocol.HELLO:
            self._on_hello(connection, message, name)
            return

        if connection.target is None:
            events.emit(
                "WARNING",
                "unidentified_message",
                f"Message of type {kind!r} before hello; ignoring",
                peer=str(connection.peer),
            )
            return

        target = self.targets.get(connection.target)
        if target is None:
            return

        if kind == protocol.HEARTBEAT:
            self._on_heartbeat(target, message)
        elif kind == protocol.STACKS:
            self._on_stacks(target, message)
        elif kind == protocol.BYE:
            events.emit("INFO", "target_goodbye", "Clean shutdown", target=target.name)
        else:
            events.emit(
                "DEBUG",
                "unknown_message",
                f"Ignoring message of type {kind!r}",
                target=target.name,
            )

    def _on_hello(
        self, connection: _Connection, message: dict[str, Any], name: Any
    ) -> None:
        if name not in self.targets:
            events.emit(
                "WARNING",
                "unknown_target",
                f"Connection claimed to be target {name!r}, which is not configured",
                peer=str(connection.peer),
            )
            self._drop(connection)
            return

        # compare_digest, so timing does not leak the token.
        presented = message.get("token")
        if not isinstance(presented, str) or not hmac.compare_digest(
            presented, self.token
        ):
            events.emit(
                "WARNING",
                "authentication_failed",
                f"A connection claiming to be {name!r} presented no valid token; "
                f"refusing it. A process procwatch did not launch cannot vouch "
                f"for that target's liveness.",
                target=name,
                peer=str(connection.peer),
            )
            self.metrics.inc("rejected_connections")
            self._drop(connection)
            return

        connection.target = name
        previous = self._by_target.get(name)
        if previous is not None and previous is not connection:
            self._drop(previous)
        self._by_target[name] = connection

        target = self.targets[name]
        target.last_heartbeat = time.monotonic()
        if target.state in (State.STARTING, State.HUNG):
            target.state = State.RUNNING

        events.emit(
            "INFO",
            "target_connected",
            f"Heartbeat channel open (pid {message.get('pid')})",
            target=name,
            pid=message.get("pid"),
        )

    def _on_heartbeat(self, target: Target, message: dict[str, Any]) -> None:
        target.last_heartbeat = time.monotonic()
        self.metrics.inc("heartbeats_received")

        progress = message.get("progress")
        if isinstance(progress, dict) and progress:
            target.progress = progress

        stalled = message.get("stalled_for")
        if isinstance(stalled, (int, float)):
            target.stalled_for = float(stalled)

        if target.state is State.HUNG and self._still_stalled(target):
            # A stalled target keeps heartbeating; only progress clears a stall.
            return

        if target.state is State.HUNG:
            # Recovered before we finished reacting.
            target.hang_deadline = None
            target.state = State.RUNNING
            events.emit(
                "NOTIFY",
                "hang_recovered",
                "Heartbeats resumed",
                target=target.name,
            )
            self.metrics.inc("hangs_recovered")
        elif target.state is State.STARTING:
            target.state = State.RUNNING

    @staticmethod
    def _still_stalled(target: Target) -> bool:
        timeout = target.spec.progress_timeout
        return (
            timeout is not None
            and target.stalled_for is not None
            and target.stalled_for > timeout
        )

    def _on_stacks(self, target: Target, message: dict[str, Any]) -> None:
        stacks = message.get("stacks", {})
        self.metrics.inc("stack_dumps")

        events.emit(
            "WARNING",
            "stack_dump",
            f"Stack traces from {target.name} ({len(stacks)} thread(s))",
            target=target.name,
            pid=message.get("pid"),
            threads=stacks,
        )

        if target.hang_deadline is not None:
            self._act_on_hang(target, time.monotonic())

    def _send(self, connection: _Connection, message: dict[str, Any]) -> bool:
        try:
            connection.sock.sendall(protocol.encode(message))
            return True
        except (OSError, protocol.ProtocolError):
            self._drop(connection)
            return False

    def _drop(self, connection: _Connection) -> None:
        fileno = connection.sock.fileno()
        try:
            self._selector.unregister(connection.sock)
        except (KeyError, ValueError):
            pass

        self._connections.pop(fileno, None)
        if connection.target and self._by_target.get(connection.target) is connection:
            self._by_target.pop(connection.target)

        try:
            connection.sock.close()
        except OSError:
            pass

    # --- signals -----------------------------------------------------------

    def _install_signal_handlers(self) -> None:
        def handler(signum: int, _frame: Any) -> None:
            events.emit(
                "NOTIFY",
                "signal_received",
                f"Received signal {signum}, shutting down",
            )
            self.stop()

        for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
            signum = getattr(signal, name, None)
            if signum is None:
                continue
            try:
                signal.signal(signum, handler)
            except (ValueError, OSError):
                # Not the main thread, or unsupported on this platform.
                pass
