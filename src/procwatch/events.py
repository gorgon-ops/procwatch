"""Structured logging: one JSON object per line, rotated so it cannot fill the disk."""

from __future__ import annotations

import json
import logging
import logging.handlers
import os
import sys
import time
from pathlib import Path
from typing import Any

#: Between INFO and WARNING: notable, but not a problem.
NOTIFY = 25
logging.addLevelName(NOTIFY, "NOTIFY")

LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "NOTIFY": NOTIFY,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}

_LOGGER_NAME = "procwatch"

#: Attributes LogRecord always carries; anything else is ours.
_RESERVED = frozenset(
    logging.LogRecord("", 0, "", 0, "", None, None).__dict__
) | {"message", "asctime", "taskName"}


class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, Any] = {
            "ts": round(record.created, 6),
            "time": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(record.created)),
            "level": record.levelname,
            "event": getattr(record, "event", record.name),
            "pid": record.process,
            "message": record.getMessage(),
        }

        extras = {k: v for k, v in record.__dict__.items() if k not in _RESERVED}
        extras.pop("event", None)
        if extras:
            entry["data"] = extras

        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(entry, separators=(",", ":"), default=str)


class ConsoleFormatter(logging.Formatter):
    """``12:03:41 WARNING  target=x  message``."""

    def format(self, record: logging.LogRecord) -> str:
        stamp = time.strftime("%H:%M:%S", time.localtime(record.created))
        target = getattr(record, "target", None)
        prefix = f"[{target}] " if target else ""
        line = f"{stamp} {record.levelname:<8} {prefix}{record.getMessage()}"
        if record.exc_info:
            line += "\n" + self.formatException(record.exc_info)
        return line


def get_logger() -> logging.Logger:
    return logging.getLogger(_LOGGER_NAME)


def setup(
    log_file: str | os.PathLike[str] | None = None,
    *,
    console_level: str = "INFO",
    file_level: str = "DEBUG",
    max_bytes: int = 5_000_000,
    backups: int = 3,
) -> logging.Logger:
    logger = get_logger()
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()

    console = logging.StreamHandler(sys.stderr)
    console.setLevel(_resolve(console_level))
    console.setFormatter(ConsoleFormatter())
    logger.addHandler(console)

    levels = [console.level]

    if log_file is not None:
        path = Path(log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        rotating = logging.handlers.RotatingFileHandler(
            path, maxBytes=max_bytes, backupCount=backups, encoding="utf-8"
        )
        rotating.setLevel(_resolve(file_level))
        rotating.setFormatter(JSONFormatter())
        logger.addHandler(rotating)
        levels.append(rotating.level)

    # The logger must pass anything a handler might want.
    logger.setLevel(min(levels))
    logger.propagate = False
    return logger


def _resolve(level: str | int) -> int:
    if isinstance(level, int):
        return level
    try:
        return LEVELS[level.upper()]
    except KeyError:
        raise ValueError(
            f"unknown log level {level!r}; expected one of {', '.join(LEVELS)}"
        ) from None


def emit(level: str | int, event: str, message: str = "", **data: Any) -> None:
    """``event`` is the stable machine-readable name; keyword arguments become ``data``."""
    get_logger().log(_resolve(level), message, extra={"event": event, **data})
