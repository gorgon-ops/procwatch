from __future__ import annotations

from pathlib import Path

import pytest

from procwatch.config import ConfigError, from_dict, load
from procwatch.target import HangAction

MINIMAL = {"targets": [{"name": "w", "cmd": ["python", "-c", "pass"]}]}


def test_defaults_applied():
    config = from_dict(MINIMAL)

    assert config.host == "127.0.0.1"
    assert config.port == 5001
    assert len(config.targets) == 1
    assert config.targets[0].on_hang is HangAction.RESTART


def test_load_from_file(tmp_path: Path):
    config_file = tmp_path / "procwatch.toml"
    config_file.write_text(
        """
[watch]
port = 5099

[[targets]]
name = "api"
cmd = ["python", "-c", "pass"]
on_hang = "kill"
""",
        encoding="utf-8",
    )

    config = load(config_file)

    assert config.port == 5099
    assert config.targets[0].on_hang is HangAction.KILL


def test_relative_paths_resolve_against_the_config_file(tmp_path: Path):
    """So the same config behaves identically from any working directory."""
    config_file = tmp_path / "nested" / "procwatch.toml"
    config_file.parent.mkdir()
    config_file.write_text(
        """
[watch]
log_file = "logs/events.log"

[[targets]]
name = "w"
cmd = ["python", "-c", "pass"]
""",
        encoding="utf-8",
    )

    config = load(config_file)

    assert Path(config.log_file).parent.parent == tmp_path / "nested"


def test_missing_file_names_the_path(tmp_path: Path):
    with pytest.raises(ConfigError, match="No configuration file at"):
        load(tmp_path / "absent.toml")


def test_invalid_toml_is_reported_as_such(tmp_path: Path):
    config_file = tmp_path / "bad.toml"
    config_file.write_text("[watch\nport = ", encoding="utf-8")

    with pytest.raises(ConfigError, match="not valid TOML"):
        load(config_file)


# --- fan-out ---------------------------------------------------------------


@pytest.fixture
def pipeline_dirs(tmp_path: Path) -> Path:
    for name in ("north", "south", "west"):
        (tmp_path / "data" / name).mkdir(parents=True)
    (tmp_path / "data" / "notes.txt").write_text("not a directory", encoding="utf-8")
    return tmp_path


def test_for_each_dir_creates_one_target_per_directory(pipeline_dirs: Path):
    config = from_dict(
        {
            "targets": [
                {
                    "name": "ingest-{dirname}",
                    "for_each_dir": "data/*",
                    "cmd": ["python", "worker.py", "--input", "{dir}"],
                }
            ]
        },
        base=pipeline_dirs,
    )

    assert [t.name for t in config.targets] == [
        "ingest-north",
        "ingest-south",
        "ingest-west",
    ]


def test_fan_out_substitutes_the_directory_into_the_command(pipeline_dirs: Path):
    config = from_dict(
        {
            "targets": [
                {
                    "name": "ingest-{dirname}",
                    "for_each_dir": "data/*",
                    "cmd": ["python", "worker.py", "--input", "{dir}"],
                }
            ]
        },
        base=pipeline_dirs,
    )

    north = config.targets[0]
    assert north.cmd[-1].endswith("north")
    assert Path(north.cmd[-1]).is_dir()


def test_fan_out_defaults_cwd_to_each_directory(pipeline_dirs: Path):
    config = from_dict(
        {
            "targets": [
                {
                    "name": "ingest-{dirname}",
                    "for_each_dir": "data/*",
                    "cmd": ["python", "-c", "pass"],
                }
            ]
        },
        base=pipeline_dirs,
    )

    assert Path(config.targets[0].cwd).name == "north"


def test_fan_out_ignores_files(pipeline_dirs: Path):
    """The glob matches notes.txt; only directories become targets."""
    config = from_dict(
        {
            "targets": [
                {
                    "name": "{dirname}",
                    "for_each_dir": "data/*",
                    "cmd": ["python", "-c", "pass"],
                }
            ]
        },
        base=pipeline_dirs,
    )

    assert "notes.txt" not in [t.name for t in config.targets]
    assert len(config.targets) == 3


def test_fan_out_with_no_matches_is_an_error(tmp_path: Path):
    with pytest.raises(ConfigError, match="matched no directories"):
        from_dict(
            {
                "targets": [
                    {
                        "name": "{dirname}",
                        "for_each_dir": "nothing/*",
                        "cmd": ["python", "-c", "pass"],
                    }
                ]
            },
            base=tmp_path,
        )


# --- validation ------------------------------------------------------------


def test_no_targets_is_an_error():
    with pytest.raises(ConfigError, match="No targets configured"):
        from_dict({"watch": {"port": 5001}})


def test_duplicate_names_are_rejected():
    with pytest.raises(ConfigError, match="Duplicate target name"):
        from_dict(
            {
                "targets": [
                    {"name": "w", "cmd": ["python", "-c", "pass"]},
                    {"name": "w", "cmd": ["python", "-c", "pass"]},
                ]
            }
        )


def test_cmd_and_pid_together_are_rejected():
    with pytest.raises(ConfigError, match="exactly one of cmd"):
        from_dict({"targets": [{"name": "w", "cmd": ["x"], "pid": 1}]})


def test_neither_cmd_nor_pid_is_rejected():
    with pytest.raises(ConfigError, match="exactly one of cmd"):
        from_dict({"targets": [{"name": "w"}]})


def test_attached_target_cannot_be_set_to_restart():
    """An adopted process has no command to relaunch with: caught at load time."""
    with pytest.raises(ConfigError, match=r"cannot\s+restart it"):
        from_dict({"targets": [{"name": "w", "pid": 1234, "on_hang": "restart"}]})


def test_attached_target_may_be_killed_or_logged():
    config = from_dict({"targets": [{"name": "w", "pid": 1234, "on_hang": "kill"}]})

    assert config.targets[0].on_hang is HangAction.KILL
    assert not config.targets[0].managed


def test_cmd_must_be_a_list():
    with pytest.raises(ConfigError, match="must be a list"):
        from_dict({"targets": [{"name": "w", "cmd": "python worker.py"}]})


def test_unknown_target_key_lists_the_valid_ones():
    with pytest.raises(ConfigError, match="Unknown key\\(s\\) in \\[\\[targets\\]\\]"):
        from_dict(
            {"targets": [{"name": "w", "cmd": ["x"], "heartbaet_timeout": 5}]}
        )


def test_unknown_watch_key_is_rejected():
    with pytest.raises(ConfigError, match="Unknown key\\(s\\) in \\[watch\\]"):
        from_dict({"watch": {"prot": 1}, **MINIMAL})


def test_unknown_hang_action_lists_the_choices():
    with pytest.raises(ConfigError, match="must be one of restart, kill, log"):
        from_dict({"targets": [{"name": "w", "cmd": ["x"], "on_hang": "panic"}]})


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"port": 70000}, "port must be between"),
        ({"interval": 0}, "interval must be positive"),
        ({"metrics_interval": -1}, "metrics_interval must be positive"),
    ],
)
def test_global_bounds(overrides, message):
    with pytest.raises(ConfigError, match=message):
        from_dict({"watch": overrides, **MINIMAL})


def test_port_zero_is_allowed_for_an_ephemeral_port():
    assert from_dict({"watch": {"port": 0}, **MINIMAL}).port == 0


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"heartbeat_timeout": 0}, "heartbeat_timeout must be positive"),
        ({"progress_timeout": 0}, "progress_timeout must be positive"),
        ({"max_restarts": -1}, "max_restarts cannot be negative"),
    ],
)
def test_target_bounds(overrides, message):
    with pytest.raises(ConfigError, match=message):
        from_dict({"targets": [{"name": "w", "cmd": ["x"], **overrides}]})


# --- alerts ----------------------------------------------------------------


def test_webhook_sink_is_built():
    config = from_dict(
        {**MINIMAL, "alerts": [{"type": "webhook", "url": "https://example.com/h"}]}
    )

    assert config.sinks[0].url == "https://example.com/h"


def test_webhook_without_url_is_rejected():
    with pytest.raises(ConfigError, match="needs a url"):
        from_dict({**MINIMAL, "alerts": [{"type": "webhook"}]})


def test_command_sink_needs_a_list():
    with pytest.raises(ConfigError, match="needs command"):
        from_dict({**MINIMAL, "alerts": [{"type": "command", "command": "notify"}]})


def test_unknown_alert_type_is_rejected():
    with pytest.raises(ConfigError, match="Unknown alert type"):
        from_dict({**MINIMAL, "alerts": [{"type": "carrier-pigeon"}]})


def test_alert_level_filter_is_parsed():
    config = from_dict(
        {**MINIMAL, "alerts": [{"type": "log", "levels": ["warning", "critical"]}]}
    )

    assert config.sinks[0].levels == {"WARNING", "CRITICAL"}
