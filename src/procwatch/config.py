from __future__ import annotations

import glob
import os
import tomllib
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any

from .alerts import CommandSink, LogSink, Sink, WebhookSink
from .target import HangAction, TargetSpec


class ConfigError(Exception):
    pass


@dataclass(slots=True)
class Config:
    host: str = "127.0.0.1"
    port: int = 5001
    #: Seconds between samples and deadline checks.
    interval: float = 2.0

    log_file: str | None = "logs/procwatch.log"
    console_level: str = "INFO"
    file_level: str = "DEBUG"
    log_max_bytes: int = 5_000_000
    log_backups: int = 3

    metrics_file: str | None = "logs/metrics.json"
    metrics_history: str | None = "logs/metrics_history.jsonl"
    metrics_interval: float = 10.0

    #: None inherits the supervisor's console, like a plain shell launch.
    output_dir: str | None = "logs/output"

    #: Empty generates a fresh one per run, injected into each child. Pin it only
    #: if a process procwatch did not launch must report heartbeats.
    token: str = ""

    dump_stacks_on_hang: bool = True
    dump_timeout: float = 3.0

    targets: list[TargetSpec] = field(default_factory=list)
    sinks: list[Sink] = field(default_factory=list)

    @property
    def address(self) -> tuple[str, int]:
        return (self.host, self.port)


def load(path: str | os.PathLike[str]) -> Config:
    config_path = Path(path)
    if not config_path.is_file():
        raise ConfigError(f"No configuration file at {config_path}")

    try:
        with config_path.open("rb") as handle:
            raw = tomllib.load(handle)
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"{config_path} is not valid TOML: {exc}") from exc

    # Relative to the config file, not the shell's cwd.
    base = config_path.parent.resolve()
    return from_dict(raw, base=base)


def from_dict(raw: dict[str, Any], *, base: Path | None = None) -> Config:
    base = base or Path.cwd()
    section = raw.get("watch", {})
    if not isinstance(section, dict):
        raise ConfigError("[watch] must be a table")

    known = {f.name for f in fields(Config)} - {"targets", "sinks"}
    unknown = set(section) - known
    if unknown:
        raise ConfigError(
            f"Unknown key(s) in [watch]: {', '.join(sorted(unknown))}. "
            f"Valid keys: {', '.join(sorted(known))}"
        )

    config = Config(**section)
    _validate_globals(config)

    for path_attr in ("log_file", "metrics_file", "metrics_history", "output_dir"):
        value = getattr(config, path_attr)
        if value:
            setattr(config, path_attr, str(_resolve(base, value)))

    raw_targets = raw.get("targets", [])
    if not isinstance(raw_targets, list):
        raise ConfigError("[[targets]] must be an array of tables")

    for entry in raw_targets:
        config.targets.extend(_build_targets(entry, base))

    if not config.targets:
        raise ConfigError("No targets configured; add at least one [[targets]] block")

    _check_unique_names(config.targets)

    for entry in raw.get("alerts", []):
        config.sinks.append(_build_sink(entry))

    return config


def _validate_globals(config: Config) -> None:
    # Port 0 lets the OS pick; the supervisor reports what it bound.
    if not 0 <= config.port < 65536:
        raise ConfigError(f"port must be between 0 and 65535, got {config.port}")
    if config.interval <= 0:
        raise ConfigError(f"interval must be positive, got {config.interval}")
    if config.metrics_interval <= 0:
        raise ConfigError(
            f"metrics_interval must be positive, got {config.metrics_interval}"
        )


def _check_unique_names(targets: list[TargetSpec]) -> None:
    seen: set[str] = set()
    for target in targets:
        if target.name in seen:
            raise ConfigError(
                f"Duplicate target name {target.name!r}. Names identify targets "
                f"in logs and heartbeats, so they must be unique."
            )
        seen.add(target.name)


def _build_targets(entry: Any, base: Path) -> list[TargetSpec]:
    if not isinstance(entry, dict):
        raise ConfigError("Each [[targets]] entry must be a table")

    entry = dict(entry)
    pattern = entry.pop("for_each_dir", None)
    if pattern is None:
        return [_build_target(entry, base)]

    # One target per matching directory. glob.glob, because Path.glob refuses an
    # absolute pattern.
    matches = sorted(
        path
        for path in glob.glob(str(_resolve(base, pattern)))  # noqa: PTH207
        if Path(path).is_dir()
    )
    if not matches:
        raise ConfigError(
            f"for_each_dir pattern {pattern!r} matched no directories "
            f"(resolved against {base})"
        )

    specs = []
    for directory in matches:
        resolved = dict(entry)
        substitutions = {
            "dir": directory,
            "dirname": Path(directory).name,
        }
        resolved["name"] = _substitute(entry.get("name", "{dirname}"), substitutions)
        resolved.setdefault("cwd", directory)
        if "cwd" in entry:
            resolved["cwd"] = _substitute(entry["cwd"], substitutions)
        if "cmd" in entry:
            resolved["cmd"] = [_substitute(arg, substitutions) for arg in entry["cmd"]]
        specs.append(_build_target(resolved, base))

    return specs


def _substitute(value: Any, mapping: dict[str, str]) -> Any:
    if not isinstance(value, str):
        return value
    for key, replacement in mapping.items():
        value = value.replace("{" + key + "}", replacement)
    return value


def _build_target(entry: dict[str, Any], base: Path) -> TargetSpec:
    entry = dict(entry)

    known = {f.name for f in fields(TargetSpec)}
    unknown = set(entry) - known - {"for_each_dir"}
    if unknown:
        raise ConfigError(
            f"Unknown key(s) in [[targets]]: {', '.join(sorted(unknown))}. "
            f"Valid keys: {', '.join(sorted(known))}, for_each_dir"
        )

    if "name" not in entry:
        raise ConfigError("Every target needs a name")
    name = entry["name"]

    has_cmd = bool(entry.get("cmd"))
    has_pid = entry.get("pid") is not None
    if has_cmd == has_pid:
        raise ConfigError(
            f"Target {name!r} must set exactly one of cmd (launch it) or "
            f"pid (attach to it)"
        )

    if has_cmd and not isinstance(entry["cmd"], list):
        raise ConfigError(
            f"Target {name!r}: cmd must be a list of arguments, "
            f'e.g. ["python", "worker.py"]'
        )

    for key in ("on_hang", "on_breach"):
        if key in entry:
            entry[key] = _parse_action(entry[key], name, key)

    if entry.get("cwd"):
        entry["cwd"] = str(_resolve(base, entry["cwd"]))

    spec = TargetSpec(**entry)

    if not spec.managed and spec.on_hang is HangAction.RESTART:
        raise ConfigError(
            f"Target {name!r} attaches to an existing pid, so procwatch cannot "
            f'restart it. Use on_hang = "kill" or "log", or give it a cmd.'
        )

    if spec.heartbeat_timeout <= 0:
        raise ConfigError(f"Target {name!r}: heartbeat_timeout must be positive")
    if spec.progress_timeout is not None and spec.progress_timeout <= 0:
        raise ConfigError(
            f"Target {name!r}: progress_timeout must be positive, or omitted "
            f"to disable stall detection"
        )
    if spec.max_restarts < 0:
        raise ConfigError(f"Target {name!r}: max_restarts cannot be negative")

    return spec


def _parse_action(value: Any, target: str, key: str) -> HangAction:
    try:
        return HangAction(str(value).lower())
    except ValueError:
        valid = ", ".join(action.value for action in HangAction)
        raise ConfigError(
            f"Target {target!r}: {key} must be one of {valid}, got {value!r}"
        ) from None


def _build_sink(entry: Any) -> Sink:
    if not isinstance(entry, dict):
        raise ConfigError("Each [[alerts]] entry must be a table")

    entry = dict(entry)
    kind = entry.pop("type", None)
    common = {
        "levels": entry.pop("levels", None),
        "min_interval": entry.pop("min_interval", 0.0),
    }

    if kind == "log":
        _reject_extra(entry, "log")
        return LogSink(**common)

    if kind == "webhook":
        url = entry.pop("url", None)
        if not url:
            raise ConfigError('A webhook alert needs a url')
        timeout = entry.pop("timeout", 10.0)
        _reject_extra(entry, "webhook")
        return WebhookSink(url, timeout=timeout, **common)

    if kind == "command":
        command = entry.pop("command", None)
        if not isinstance(command, list) or not command:
            raise ConfigError(
                'A command alert needs command = ["executable", "args"...]'
            )
        timeout = entry.pop("timeout", 30.0)
        _reject_extra(entry, "command")
        return CommandSink(command, timeout=timeout, **common)

    raise ConfigError(
        f"Unknown alert type {kind!r}; expected one of: log, webhook, command"
    )


def _reject_extra(entry: dict[str, Any], kind: str) -> None:
    if entry:
        raise ConfigError(
            f"Unknown key(s) for a {kind} alert: {', '.join(sorted(entry))}"
        )


def _resolve(base: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else base / path
