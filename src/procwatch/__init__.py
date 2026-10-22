"""procwatch: a process watchdog.

    from procwatch import Heartbeat

    with Heartbeat.from_env():
        main()

Then ``procwatch run -c procwatch.toml``.
"""

from __future__ import annotations

from .alerts import Alert, AlertManager, CommandSink, LogSink, Sink, WebhookSink
from .config import Config, ConfigError, load
from .heartbeat import Heartbeat, collect_stacks
from .metrics import Metrics
from .sampler import ResourceSampler, Sample
from .supervisor import Supervisor
from .target import HangAction, State, Target, TargetSpec

__version__ = "1.0.0"

__all__ = [
    "Alert",
    "AlertManager",
    "CommandSink",
    "Config",
    "ConfigError",
    "HangAction",
    "Heartbeat",
    "LogSink",
    "Metrics",
    "ResourceSampler",
    "Sample",
    "Sink",
    "State",
    "Supervisor",
    "Target",
    "TargetSpec",
    "WebhookSink",
    "__version__",
    "collect_stacks",
    "load",
]
