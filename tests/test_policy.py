from __future__ import annotations

import json
from pathlib import Path

from procwatch.alerts import Alert, AlertManager, Sink
from procwatch.metrics import Metrics
from procwatch.target import State, Target, TargetSpec

# --- restart policy --------------------------------------------------------


def make_target(**overrides) -> Target:
    settings = {
        "name": "w",
        "cmd": ["python", "-c", "pass"],
        "max_restarts": 3,
        "restart_window": 100.0,
        "backoff": 2.0,
        "max_backoff": 10.0,
    }
    settings.update(overrides)
    return Target(TargetSpec(**settings))


def test_budget_allows_up_to_max_restarts():
    target = make_target()

    for index in range(3):
        assert target.restart_budget_left(now=index), f"restart {index} refused"
        target.record_restart(now=index)

    assert not target.restart_budget_left(now=3)


def test_budget_is_a_sliding_window():
    """Rare failures restart forever; only a tight cluster exhausts the budget."""
    target = make_target()

    for index in range(3):
        target.record_restart(now=index)
    assert not target.restart_budget_left(now=3)

    # Far enough on that the old restarts have aged out.
    assert target.restart_budget_left(now=200)


def test_backoff_grows_exponentially():
    target = make_target()

    delays = [target.record_restart(now=index) for index in range(4)]

    assert delays == [2.0, 4.0, 8.0, 10.0]  # last one capped at max_backoff


def test_backoff_is_capped():
    target = make_target(max_restarts=20, max_backoff=5.0)

    delays = [target.record_restart(now=index) for index in range(8)]

    assert max(delays) == 5.0


def test_record_restart_schedules_the_relaunch():
    target = make_target()

    delay = target.record_restart(now=100.0)

    assert target.state is State.RESTARTING
    assert target.retry_at == 100.0 + delay


def test_healthy_run_clears_the_backoff():
    """A target that recovers should not stay penalised."""
    target = make_target()
    target.record_restart(now=0)
    target.started_at = 0.0

    target.note_healthy(now=target.spec.restart_window)

    assert target.consecutive_restarts == 0


def test_brief_run_does_not_clear_the_backoff():
    target = make_target()
    target.record_restart(now=0)
    target.started_at = 0.0

    target.note_healthy(now=1.0)

    assert target.consecutive_restarts == 1


def test_failed_target_is_no_longer_supervised():
    target = make_target()
    assert target.supervised

    target.state = State.FAILED

    assert not target.supervised


# --- metrics ---------------------------------------------------------------


def test_counters_start_absent_and_accumulate():
    metrics = Metrics()

    assert metrics.get("heartbeats_received") == 0
    metrics.inc("heartbeats_received")
    metrics.inc("heartbeats_received", 4)

    assert metrics.get("heartbeats_received") == 5


def test_snapshot_includes_counters_and_gauges():
    metrics = Metrics()
    metrics.inc("restarts", 2)
    metrics.set_gauge("w", cpu_percent=12.5)

    snapshot = metrics.snapshot()

    assert snapshot["counters"]["restarts"] == 2
    assert snapshot["targets"]["w"]["cpu_percent"] == 12.5


def test_gauges_merge_rather_than_replace():
    metrics = Metrics()
    metrics.set_gauge("w", cpu_percent=1.0)
    metrics.set_gauge("w", memory_mb=50.0)

    assert metrics.snapshot()["targets"]["w"] == {
        "cpu_percent": 1.0,
        "memory_mb": 50.0,
    }


def test_dropped_gauge_disappears():
    metrics = Metrics()
    metrics.set_gauge("w", cpu_percent=1.0)
    metrics.drop_gauge("w")

    assert metrics.snapshot()["targets"] == {}


def test_flush_writes_readable_json(tmp_path: Path):
    """The old counters lived in another process, so metrics.json read zero forever."""
    snapshot_file = tmp_path / "metrics.json"
    metrics = Metrics(snapshot_file)
    metrics.inc("hangs_detected", 7)

    metrics.flush()

    assert json.loads(snapshot_file.read_text())["counters"]["hangs_detected"] == 7


def test_flush_leaves_no_temporary_file(tmp_path: Path):
    metrics = Metrics(tmp_path / "metrics.json")
    metrics.flush()

    assert [p.name for p in tmp_path.iterdir()] == ["metrics.json"]


def test_history_appends_one_line_per_flush(tmp_path: Path):
    history = tmp_path / "history.jsonl"
    metrics = Metrics(history_path=history)

    for _ in range(3):
        metrics.flush()

    assert len(history.read_text().strip().splitlines()) == 3


def test_history_trims_itself(tmp_path: Path):
    """Unbounded growth was one of the original problems."""
    history = tmp_path / "history.jsonl"
    metrics = Metrics(history_path=history, history_max_lines=10)

    for _ in range(12):
        metrics.flush()

    assert len(history.read_text().strip().splitlines()) < 10


def test_metrics_creates_missing_directories(tmp_path: Path):
    metrics = Metrics(tmp_path / "deep" / "nested" / "metrics.json")
    metrics.flush()

    assert (tmp_path / "deep" / "nested" / "metrics.json").is_file()


# --- alerts ----------------------------------------------------------------


class RecordingSink(Sink):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.received: list[Alert] = []

    def deliver(self, alert: Alert) -> None:
        self.received.append(alert)


def alert(kind: str = "hang", level: str = "WARNING", target: str = "w") -> Alert:
    return Alert(kind=kind, target=target, message="m", level=level)


def test_level_filter_excludes_lower_levels():
    sink = RecordingSink(levels=["CRITICAL"])

    assert not sink.wants(alert(level="WARNING"))
    assert sink.wants(alert(level="CRITICAL"))


def test_no_level_filter_accepts_everything():
    sink = RecordingSink()

    assert sink.wants(alert(level="INFO"))
    assert sink.wants(alert(level="CRITICAL"))


def test_rate_limit_suppresses_a_repeat():
    sink = RecordingSink(min_interval=60.0)

    assert sink.wants(alert())
    assert not sink.wants(alert())


def test_rate_limit_is_per_target_and_kind():
    """A flapping target must not silence alerts about a different one."""
    sink = RecordingSink(min_interval=60.0)

    assert sink.wants(alert(target="a"))
    assert sink.wants(alert(target="b"))
    assert sink.wants(alert(target="a", kind="crash"))
    assert not sink.wants(alert(target="a"))


def test_manager_delivers_to_every_sink():
    first, second = RecordingSink(), RecordingSink()
    manager = AlertManager([first, second])
    manager.start()

    manager.send(alert())
    manager.stop()

    assert len(first.received) == 1
    assert len(second.received) == 1


def test_a_failing_sink_does_not_stop_the_others():
    class Exploding(Sink):
        def deliver(self, alert: Alert) -> None:
            raise RuntimeError("boom")

    survivor = RecordingSink()
    manager = AlertManager([Exploding(), survivor])
    manager.start()

    manager.send(alert())
    manager.stop()

    assert len(survivor.received) == 1


def test_manager_with_no_sinks_is_inert():
    manager = AlertManager([])
    manager.start()

    manager.send(alert())  # must not raise
    manager.stop()
