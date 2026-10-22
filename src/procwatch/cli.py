from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from . import __version__, events
from .config import Config, ConfigError, load
from .supervisor import Supervisor
from .target import HangAction, TargetSpec

STARTER_CONFIG = '''\
# procwatch configuration.
# Paths are resolved relative to this file.

[watch]
host = "127.0.0.1"
port = 5001
interval = 2.0              # seconds between resource samples
console_level = "INFO"
log_file = "logs/procwatch.log"
metrics_file = "logs/metrics.json"
output_dir = "logs/output"  # each target's stdout/stderr, stderr merged in
# token = "..."             # only if a process procwatch does not launch
#                           # needs to report heartbeats; otherwise a fresh
#                           # secret is generated for every run

# A single service that reports heartbeats.
[[targets]]
name = "api"
cmd = ["python", "-m", "myapp.server"]
heartbeat_timeout = 15.0    # no heartbeat for this long => unresponsive
progress_timeout = 120.0    # heartbeats fine but no tick() => stalled
startup_grace = 30.0        # allow this long for the first heartbeat
on_hang = "restart"         # restart | kill | log
max_memory_mb = 1024
max_restarts = 5
restart_window = 300.0

# One worker per data directory, expanded automatically at startup.
# {dir} is the matched path, {dirname} its last component.
# [[targets]]
# name = "ingest-{dirname}"
# for_each_dir = "data/*"
# cmd = ["python", "worker.py", "--input", "{dir}"]
# heartbeat_timeout = 60.0
# on_hang = "restart"

[[alerts]]
type = "log"

# [[alerts]]
# type = "webhook"
# url = "https://hooks.example.com/procwatch"
# levels = ["WARNING", "ERROR", "CRITICAL"]
# min_interval = 60.0       # at most one alert per target+kind per minute
'''


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        return args.handler(args)
    except ConfigError as exc:
        print(f"procwatch: configuration error: {exc}", file=sys.stderr)
        return 2
    except RuntimeError as exc:
        print(f"procwatch: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="procwatch",
        description="Watch processes for hangs, crashes and runaway resource use.",
    )
    parser.add_argument("--version", action="version", version=f"procwatch {__version__}")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- run ---------------------------------------------------------------
    run = subparsers.add_parser(
        "run",
        help="Supervise targets from a config file, or one ad-hoc command.",
        epilog=(
            "Examples:\n"
            "  procwatch run -c procwatch.toml\n"
            "  procwatch run --restart -- python worker.py --input data/\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    run.add_argument("-c", "--config", help="Path to a procwatch TOML file.")
    run.add_argument(
        "command_args",
        nargs=argparse.REMAINDER,
        metavar="-- COMMAND [ARGS...]",
        help="A command to launch and watch, instead of a config file.",
    )
    _add_target_flags(run)
    run.set_defaults(handler=_cmd_run)

    # --- attach ------------------------------------------------------------
    attach = subparsers.add_parser(
        "attach",
        help="Monitor a process that is already running, by PID.",
    )
    attach.add_argument("pid", type=int, help="PID to watch.")
    attach.add_argument("--name", help="Label for logs (default: pid-<PID>).")
    attach.add_argument(
        "--on-hang",
        choices=["kill", "log"],
        default="log",
        help="procwatch cannot restart what it did not launch (default: log).",
    )
    _add_limit_flags(attach)
    attach.set_defaults(handler=_cmd_attach)

    # --- status ------------------------------------------------------------
    status = subparsers.add_parser(
        "status", help="Print the latest metrics snapshot."
    )
    status.add_argument("-c", "--config", help="Config file, to locate metrics.json.")
    status.add_argument(
        "-f", "--file", help="Read this metrics file directly."
    )
    status.add_argument("--json", action="store_true", help="Raw JSON output.")
    status.set_defaults(handler=_cmd_status)

    # --- init --------------------------------------------------------------
    init = subparsers.add_parser("init", help="Write a starter config file.")
    init.add_argument(
        "path", nargs="?", default="procwatch.toml", help="Where to write it."
    )
    init.set_defaults(handler=_cmd_init)

    return parser


def _add_limit_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--max-cpu", type=float, metavar="PCT", help="CPU ceiling.")
    parser.add_argument(
        "--max-memory", type=float, metavar="MB", help="Memory ceiling in MB."
    )
    parser.add_argument(
        "--interval", type=float, default=2.0, help="Seconds between samples."
    )
    parser.add_argument("--log-file", help="Write JSON events here.")
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Log debug events to the console."
    )


def _add_target_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--name", default="main", help="Label for logs.")
    parser.add_argument(
        "--heartbeat",
        action="store_true",
        help="Require heartbeats; the command must use procwatch.Heartbeat.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=15.0,
        help="Heartbeat timeout in seconds (with --heartbeat).",
    )
    parser.add_argument(
        "--restart",
        action="store_true",
        help="Relaunch the command when it hangs, crashes or exits.",
    )
    parser.add_argument(
        "--max-restarts", type=int, default=5, help="Restart budget per window."
    )
    parser.add_argument("--port", type=int, default=5001, help="Heartbeat port.")
    parser.add_argument(
        "--capture-output",
        metavar="DIR",
        help="Write the command's output to DIR/<name>.out instead of the console.",
    )
    _add_limit_flags(parser)


# --- handlers --------------------------------------------------------------


def _cmd_run(args: argparse.Namespace) -> int:
    command = _strip_separator(args.command_args)

    if args.config and command:
        print(
            "procwatch: give either --config or a -- COMMAND, not both.",
            file=sys.stderr,
        )
        return 2

    if args.config:
        config = load(args.config)
    elif command:
        config = _adhoc_config(args, command)
    else:
        default = Path("procwatch.toml")
        if not default.is_file():
            print(
                "procwatch: nothing to run. Pass -c CONFIG, or a command after --,\n"
                "           or create procwatch.toml with: procwatch init",
                file=sys.stderr,
            )
            return 2
        config = load(default)

    _setup_logging(config)
    Supervisor(config).run()
    return 0


def _cmd_attach(args: argparse.Namespace) -> int:
    spec = TargetSpec(
        name=args.name or f"pid-{args.pid}",
        pid=args.pid,
        require_heartbeat=False,
        max_cpu=args.max_cpu,
        max_memory_mb=args.max_memory,
        on_hang=HangAction(args.on_hang),
        on_breach=HangAction(args.on_hang),
    )
    config = Config(
        interval=args.interval,
        log_file=args.log_file,
        console_level="DEBUG" if args.verbose else "INFO",
        metrics_file=None,
        metrics_history=None,
        # Nothing was launched here, so there is no stdio to redirect.
        output_dir=None,
        targets=[spec],
        # An attached process has no heartbeat channel to answer on.
        dump_stacks_on_hang=False,
    )
    _setup_logging(config)
    Supervisor(config).run()
    return 0


def _cmd_status(args: argparse.Namespace) -> int:
    if args.file:
        path = Path(args.file)
    elif args.config:
        metrics_file = load(args.config).metrics_file
        if not metrics_file:
            print("procwatch: that config has no metrics_file.", file=sys.stderr)
            return 2
        path = Path(metrics_file)
    else:
        path = Path("logs/metrics.json")

    if not path.is_file():
        print(
            f"procwatch: no metrics at {path}. Is the supervisor running?",
            file=sys.stderr,
        )
        return 1

    try:
        snapshot = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        print(f"procwatch: {path} is not valid JSON: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(snapshot, indent=2))
        return 0

    _print_status(snapshot)
    return 0


def _cmd_init(args: argparse.Namespace) -> int:
    path = Path(args.path)
    if path.exists():
        print(f"procwatch: {path} already exists; not overwriting.", file=sys.stderr)
        return 1

    path.write_text(STARTER_CONFIG, encoding="utf-8")
    print(f"Wrote {path}. Edit the [[targets]] block, then: procwatch run -c {path}")
    return 0


# --- helpers ---------------------------------------------------------------


def _strip_separator(command_args: list[str]) -> list[str]:
    """argparse.REMAINDER keeps the leading ``--``; drop it."""
    if command_args and command_args[0] == "--":
        return command_args[1:]
    return command_args


def _adhoc_config(args: argparse.Namespace, command: list[str]) -> Config:
    on_hang = HangAction.RESTART if args.restart else HangAction.LOG
    spec = TargetSpec(
        name=args.name,
        cmd=command,
        require_heartbeat=args.heartbeat,
        heartbeat_timeout=args.timeout,
        max_cpu=args.max_cpu,
        max_memory_mb=args.max_memory,
        on_hang=on_hang,
        max_restarts=args.max_restarts,
        restart_on_exit=args.restart,
    )
    return Config(
        port=args.port,
        interval=args.interval,
        log_file=args.log_file,
        console_level="DEBUG" if args.verbose else "INFO",
        metrics_file=None,
        metrics_history=None,
        # An ad-hoc run is watched in a terminal, so output goes there; config-driven
        # runs capture to per-target files.
        output_dir=str(args.capture_output) if args.capture_output else None,
        targets=[spec],
    )


def _setup_logging(config: Config) -> None:
    events.setup(
        config.log_file,
        console_level=config.console_level,
        file_level=config.file_level,
        max_bytes=config.log_max_bytes,
        backups=config.log_backups,
    )


def _print_status(snapshot: dict[str, Any]) -> None:
    uptime = snapshot.get("uptime_seconds", 0)
    print(f"procwatch  up {_duration(uptime)}")

    counters = snapshot.get("counters", {})
    if counters:
        print("\ncounters")
        for name in sorted(counters):
            print(f"  {name:<22} {counters[name]}")

    targets = snapshot.get("targets", {})
    if not targets:
        print("\nno active targets")
        return

    print(f"\n{'target':<20} {'state':<11} {'pid':>7} {'cpu%':>7} {'mem MB':>9} {'restarts':>9}")
    print("-" * 68)
    for name in sorted(targets):
        row = targets[name]
        print(
            f"{name[:20]:<20} "
            f"{row.get('state', '?')!s:<11} "
            f"{_num(row.get('pid')):>7} "
            f"{_num(row.get('cpu_percent')):>7} "
            f"{_num(row.get('memory_mb')):>9} "
            f"{_num(row.get('restarts')):>9}"
        )

        progress = row.get("progress")
        if progress:
            fields = "  ".join(f"{k}={v}" for k, v in progress.items())
            print(f"{'':<20} {fields}")


def _num(value: Any) -> str:
    return "-" if value is None else str(value)


def _duration(seconds: float) -> str:
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s"
    return f"{seconds // 3600}h {(seconds % 3600) // 60}m"
