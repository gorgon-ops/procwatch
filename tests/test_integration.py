from __future__ import annotations

import sys
from pathlib import Path

import pytest

from procwatch.target import HangAction, State, TargetSpec

from .conftest import (
    CLEAN_WORKER,
    CRASHING_WORKER,
    STALLING_WORKER,
    RunningSupervisor,
    base_config,
    wait_until,
)

pytestmark = pytest.mark.timeout(90)


def spec(script: Path, **overrides) -> TargetSpec:
    settings = {
        "name": "worker",
        "cmd": [sys.executable, str(script)],
        "heartbeat_timeout": 3.0,
        "startup_grace": 10.0,
        "backoff": 0.2,
        "stop_timeout": 3.0,
    }
    settings.update(overrides)
    return TargetSpec(**settings)


def test_heartbeats_reach_the_supervisor(worker_script):
    script = worker_script(STALLING_WORKER)
    config = base_config(targets=[spec(script, require_heartbeat=True)])

    with RunningSupervisor(config) as supervisor:
        connected = wait_until(
            lambda: supervisor.targets["worker"].last_heartbeat is not None
        )

    assert connected, "the worker never reported a heartbeat"
    assert supervisor.metrics.get("heartbeats_received") > 0


def test_progress_fields_reach_the_supervisor(worker_script):
    script = worker_script(STALLING_WORKER)
    config = base_config(targets=[spec(script, require_heartbeat=True)])

    with RunningSupervisor(config) as supervisor:
        wait_until(lambda: supervisor.targets["worker"].progress.get("done") == 2)

    assert supervisor.targets["worker"].progress["done"] == 2


def test_a_stalled_worker_is_detected_although_it_keeps_beating(worker_script):
    """The heartbeat thread is fine; the working thread is not."""
    script = worker_script(STALLING_WORKER)
    config = base_config(
        targets=[
            spec(
                script,
                require_heartbeat=True,
                progress_timeout=1.5,
                on_hang=HangAction.LOG,
            )
        ]
    )

    with RunningSupervisor(config) as supervisor:
        detected = wait_until(
            lambda: supervisor.metrics.get("hangs_detected") > 0, timeout=25
        )

    assert detected, "a stalled worker went unnoticed"
    assert supervisor.targets["worker"].state is State.HUNG
    # Heartbeats kept arriving throughout: liveness was never the signal.
    assert supervisor.metrics.get("heartbeats_received") > 0


def test_the_hung_process_reports_its_own_stacks(worker_script):
    """The traceback must come from the monitored process, not the watchdog."""
    script = worker_script(STALLING_WORKER)
    config = base_config(
        targets=[
            spec(
                script,
                require_heartbeat=True,
                progress_timeout=1.5,
                on_hang=HangAction.LOG,
            )
        ],
        dump_stacks_on_hang=True,
    )

    with RunningSupervisor(config) as supervisor:
        dumped = wait_until(
            lambda: supervisor.metrics.get("stack_dumps") > 0, timeout=25
        )

    assert dumped, "no stack dump was collected from the hung worker"


def test_a_stalled_worker_is_restarted(worker_script):
    script = worker_script(STALLING_WORKER)
    config = base_config(
        targets=[
            spec(
                script,
                require_heartbeat=True,
                progress_timeout=1.5,
                on_hang=HangAction.RESTART,
                max_restarts=5,
            )
        ]
    )

    with RunningSupervisor(config) as supervisor:
        restarted = wait_until(
            lambda: supervisor.targets["worker"].restart_count > 0, timeout=30
        )

    assert restarted, "the stalled worker was never restarted"


def test_the_restart_gives_the_target_a_new_pid(worker_script):
    script = worker_script(STALLING_WORKER)
    config = base_config(
        targets=[
            spec(
                script,
                require_heartbeat=True,
                progress_timeout=1.5,
                on_hang=HangAction.RESTART,
            )
        ]
    )

    with RunningSupervisor(config) as supervisor:
        wait_until(lambda: supervisor.targets["worker"].pid is not None)
        first_pid = supervisor.targets["worker"].pid
        wait_until(
            lambda: supervisor.targets["worker"].pid not in (None, first_pid),
            timeout=30,
        )
        second_pid = supervisor.targets["worker"].pid

    assert second_pid != first_pid


def test_a_crashing_worker_is_restarted(worker_script):
    script = worker_script(CRASHING_WORKER)
    config = base_config(
        targets=[spec(script, require_heartbeat=False, on_hang=HangAction.RESTART)]
    )

    with RunningSupervisor(config) as supervisor:
        wait_until(lambda: supervisor.targets["worker"].restart_count >= 2, timeout=30)

    assert supervisor.targets["worker"].restart_count >= 2


def test_the_restart_budget_is_eventually_exhausted(worker_script):
    """A crash loop must stop, not spin forever."""
    script = worker_script(CRASHING_WORKER)
    config = base_config(
        targets=[
            spec(
                script,
                require_heartbeat=False,
                on_hang=HangAction.RESTART,
                max_restarts=2,
                restart_window=300.0,
            )
        ]
    )

    with RunningSupervisor(config) as supervisor:
        gave_up = wait_until(
            lambda: supervisor.targets["worker"].state is State.FAILED, timeout=40
        )

    assert gave_up, "the crash loop never ended"
    assert supervisor.targets["worker"].restart_count == 2


def test_a_clean_exit_is_left_alone(worker_script):
    script = worker_script(CLEAN_WORKER)
    config = base_config(
        targets=[spec(script, require_heartbeat=False, on_hang=HangAction.RESTART)]
    )

    with RunningSupervisor(config) as supervisor:
        finished = wait_until(
            lambda: supervisor.targets["worker"].state is State.EXITED, timeout=25
        )

    assert finished
    assert supervisor.targets["worker"].restart_count == 0


def test_restart_on_exit_relaunches_a_clean_exit(worker_script):
    script = worker_script(CLEAN_WORKER)
    config = base_config(
        targets=[
            spec(
                script,
                require_heartbeat=False,
                on_hang=HangAction.RESTART,
                restart_on_exit=True,
            )
        ]
    )

    with RunningSupervisor(config) as supervisor:
        relaunched = wait_until(
            lambda: supervisor.targets["worker"].restart_count > 0, timeout=25
        )

    assert relaunched


def test_resource_samples_are_collected(worker_script):
    script = worker_script(STALLING_WORKER)
    config = base_config(targets=[spec(script, require_heartbeat=True)])

    with RunningSupervisor(config) as supervisor:
        sampled = wait_until(
            lambda: supervisor.targets["worker"].last_sample is not None
        )

    assert sampled
    assert supervisor.targets["worker"].last_sample.memory_mb > 0


def test_a_memory_limit_breach_is_reported(worker_script):
    """max_memory_mb of zero guarantees a breach from any live process."""
    script = worker_script(STALLING_WORKER)
    config = base_config(
        targets=[
            spec(
                script,
                require_heartbeat=True,
                max_memory_mb=0.0,
                breach_samples=2,
                on_breach=HangAction.LOG,
            )
        ]
    )

    with RunningSupervisor(config) as supervisor:
        breached = wait_until(
            lambda: supervisor.metrics.get("memory_breaches") > 0, timeout=25
        )

    assert breached


def test_supervisor_terminates_its_children_on_shutdown(worker_script):
    script = worker_script(STALLING_WORKER)
    config = base_config(targets=[spec(script, require_heartbeat=True)])

    with RunningSupervisor(config) as supervisor:
        wait_until(lambda: supervisor.targets["worker"].pid is not None)
        target = supervisor.targets["worker"]

    assert not target.is_alive(), "a child process outlived the supervisor"


def test_many_targets_run_under_one_supervisor(worker_script):
    """The fleet case: one thread, one socket, many processes."""
    script = worker_script(STALLING_WORKER)
    config = base_config(
        targets=[
            spec(script, name=f"worker-{index}", require_heartbeat=True)
            for index in range(6)
        ]
    )

    with RunningSupervisor(config) as supervisor:
        all_up = wait_until(
            lambda: all(
                t.last_heartbeat is not None for t in supervisor.targets.values()
            ),
            timeout=30,
        )

    assert all_up, "not every target connected"
    assert len(supervisor.targets) == 6


def test_status_reports_every_target(worker_script):
    script = worker_script(STALLING_WORKER)
    config = base_config(targets=[spec(script, require_heartbeat=True)])

    with RunningSupervisor(config) as supervisor:
        wait_until(lambda: supervisor.targets["worker"].last_heartbeat is not None)
        status = supervisor.status()

    assert set(status) == {"worker"}
    assert status["worker"]["pid"] is not None


def test_attaching_to_a_live_process(worker_script):
    """Monitoring something procwatch did not launch."""
    import subprocess

    script = worker_script(CLEAN_WORKER)
    child = subprocess.Popen([sys.executable, str(script)])
    try:
        config = base_config(
            targets=[
                TargetSpec(
                    name="adopted",
                    pid=child.pid,
                    require_heartbeat=False,
                    on_hang=HangAction.LOG,
                )
            ]
        )

        with RunningSupervisor(config) as supervisor:
            sampled = wait_until(
                lambda: supervisor.targets["adopted"].last_sample is not None
            )

        assert sampled
        assert supervisor.targets["adopted"].pid == child.pid
    finally:
        child.kill()
        child.wait(timeout=10)
