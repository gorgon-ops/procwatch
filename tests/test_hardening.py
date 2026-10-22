from __future__ import annotations

import os
import socket
import subprocess
import sys
from pathlib import Path

import psutil
import pytest

from procwatch import processguard, protocol
from procwatch.target import HangAction, TargetSpec

from .conftest import RunningSupervisor, base_config, wait_until

pytestmark = pytest.mark.timeout(90)

SLEEPER = "import time\ntime.sleep(300)\n"

NOISY_WORKER = """\
import sys
print("hello from stdout", flush=True)
print("and from stderr", file=sys.stderr, flush=True)
"""

CRASHING_WITH_TRACEBACK = """\
import sys
print("about to fail", flush=True)
print("Traceback (most recent call last):", file=sys.stderr, flush=True)
print("ValueError: the database went away", file=sys.stderr, flush=True)
sys.exit(1)
"""


# --- orphan prevention -----------------------------------------------------


def test_the_platform_offers_a_guard():
    guard = processguard.create()
    try:
        if sys.platform == "win32" or sys.platform.startswith("linux"):
            assert guard.available, f"no orphan guard on {sys.platform}"
        else:
            pytest.skip(f"no guard primitive exists on {sys.platform}")
    finally:
        guard.close()


def test_children_die_with_a_hard_killed_supervisor(tmp_path: Path):
    """Before the guard, SIGKILLing the supervisor left every worker running, unseen."""
    guard = processguard.create()
    available = guard.available
    guard.close()
    if not available:
        pytest.skip(f"no orphan guard on {sys.platform}")

    sleeper = tmp_path / "sleeper.py"
    sleeper.write_text(SLEEPER, encoding="utf-8")

    config = tmp_path / "procwatch.toml"
    config.write_text(
        f"""
[watch]
port = 0
interval = 0.5
console_level = "CRITICAL"

[[targets]]
name = "sleeper"
cmd = ["{Path(sys.executable).as_posix()}", "{sleeper.as_posix()}"]
require_heartbeat = false
on_hang = "log"
""",
        encoding="utf-8",
    )

    supervisor = subprocess.Popen(
        [sys.executable, "-m", "procwatch", "run", "-c", str(config)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    try:
        parent = psutil.Process(supervisor.pid)
        assert wait_until(lambda: len(parent.children(recursive=True)) > 0), (
            "the supervisor never launched its child"
        )
        children = parent.children(recursive=True)

        # SIGKILL equivalent: no shutdown(), no cleanup, no chance to react.
        parent.kill()
        supervisor.wait(timeout=15)

        gone, alive = psutil.wait_procs(children, timeout=15)
    finally:
        if supervisor.poll() is None:
            supervisor.kill()

    assert not alive, f"orphaned processes survived: {[p.pid for p in alive]}"
    assert len(gone) == len(children)


# --- authentication --------------------------------------------------------


def _impostor(address: tuple[str, int], name: str, token: str) -> socket.socket:
    sock = socket.create_connection(address, timeout=5)
    sock.sendall(
        protocol.encode(
            {
                "type": protocol.HELLO,
                "name": name,
                "token": token,
                "pid": os.getpid(),
            }
        )
    )
    return sock


@pytest.fixture
def sleeping_supervisor(worker_script):
    script = worker_script(SLEEPER)
    config = base_config(
        targets=[
            TargetSpec(
                name="victim",
                cmd=[sys.executable, str(script)],
                require_heartbeat=False,
                on_hang=HangAction.LOG,
            )
        ]
    )
    with RunningSupervisor(config) as supervisor:
        assert wait_until(lambda: supervisor.bound_address[1] != 0)
        yield supervisor


def test_a_valid_token_is_accepted(sleeping_supervisor):
    sock = _impostor(
        sleeping_supervisor.bound_address, "victim", sleeping_supervisor.token
    )
    try:
        accepted = wait_until(
            lambda: sleeping_supervisor.targets["victim"].last_heartbeat is not None
        )
    finally:
        sock.close()

    assert accepted, "a correctly authenticated connection was refused"


def test_a_wrong_token_is_refused(sleeping_supervisor):
    """Otherwise any local process could vouch for a target that is hung."""
    sock = _impostor(sleeping_supervisor.bound_address, "victim", "not-the-token")
    try:
        refused = wait_until(
            lambda: sleeping_supervisor.metrics.get("rejected_connections") > 0
        )
    finally:
        sock.close()

    assert refused, "an unauthenticated connection was accepted"
    assert sleeping_supervisor.targets["victim"].last_heartbeat is None


def test_a_missing_token_is_refused(sleeping_supervisor):
    sock = socket.create_connection(sleeping_supervisor.bound_address, timeout=5)
    try:
        sock.sendall(
            protocol.encode({"type": protocol.HELLO, "name": "victim", "pid": 1})
        )
        refused = wait_until(
            lambda: sleeping_supervisor.metrics.get("rejected_connections") > 0
        )
    finally:
        sock.close()

    assert refused


def test_each_run_gets_a_fresh_token():
    """So a token learned from one run is useless against the next."""
    spec = TargetSpec(name="w", cmd=["python", "-c", "pass"])
    from procwatch import Supervisor

    first = Supervisor(base_config(targets=[spec])).token
    second = Supervisor(base_config(targets=[spec])).token

    assert first != second
    assert len(first) > 20


def test_a_pinned_token_is_used_as_given():
    """For processes procwatch does not launch and so cannot hand a secret."""
    from procwatch import Supervisor

    config = base_config(
        token="a-pinned-secret",
        targets=[TargetSpec(name="w", cmd=["python", "-c", "pass"])],
    )

    assert Supervisor(config).token == "a-pinned-secret"


# --- output capture --------------------------------------------------------


def test_child_output_lands_in_a_per_target_file(worker_script, tmp_path: Path):
    script = worker_script(NOISY_WORKER)
    output_dir = tmp_path / "output"
    config = base_config(
        output_dir=str(output_dir),
        targets=[
            TargetSpec(
                name="noisy",
                cmd=[sys.executable, str(script)],
                require_heartbeat=False,
                on_hang=HangAction.LOG,
            )
        ],
    )

    with RunningSupervisor(config) as supervisor:
        wait_until(lambda: (output_dir / "noisy.out").exists())
        wait_until(
            lambda: "and from stderr" in (output_dir / "noisy.out").read_text(
                encoding="utf-8", errors="replace"
            )
        )
        del supervisor

    captured = (output_dir / "noisy.out").read_text(encoding="utf-8")
    assert "hello from stdout" in captured
    assert "and from stderr" in captured, "stderr was not merged into the file"


def test_a_crash_carries_its_last_output(worker_script, tmp_path: Path):
    """An exit code alone does not explain anything; the traceback does."""
    script = worker_script(CRASHING_WITH_TRACEBACK)
    config = base_config(
        output_dir=str(tmp_path / "output"),
        targets=[
            TargetSpec(
                name="crasher",
                cmd=[sys.executable, str(script)],
                require_heartbeat=False,
                on_hang=HangAction.LOG,
            )
        ],
    )

    with RunningSupervisor(config) as supervisor:
        wait_until(lambda: supervisor.metrics.get("target_exits") > 0, timeout=25)
        tail = supervisor.targets["crasher"].output_tail()

    assert any("the database went away" in line for line in tail), tail


def test_capture_can_be_turned_off(worker_script, tmp_path: Path):
    script = worker_script(NOISY_WORKER)
    output_dir = tmp_path / "output"
    config = base_config(
        output_dir=str(output_dir),
        targets=[
            TargetSpec(
                name="noisy",
                cmd=[sys.executable, str(script)],
                require_heartbeat=False,
                capture_output=False,
                on_hang=HangAction.LOG,
            )
        ],
    )

    with RunningSupervisor(config) as supervisor:
        wait_until(lambda: supervisor.metrics.get("target_exits") > 0, timeout=25)

    assert not (output_dir / "noisy.out").exists()


def test_a_path_like_target_name_cannot_escape_the_output_directory(
    worker_script, tmp_path: Path
):
    """Target names reach the filesystem, so they are flattened rather than trusted."""
    script = worker_script(NOISY_WORKER)
    output_dir = tmp_path / "output"
    config = base_config(
        output_dir=str(output_dir),
        targets=[
            TargetSpec(
                name="../escaped",
                cmd=[sys.executable, str(script)],
                require_heartbeat=False,
                on_hang=HangAction.LOG,
            )
        ],
    )

    with RunningSupervisor(config) as supervisor:
        wait_until(lambda: supervisor.metrics.get("target_exits") > 0, timeout=25)

    assert not (tmp_path / "escaped.out").exists()
    assert list(output_dir.glob("*.out")), "no output file was written at all"


def test_the_output_file_is_rotated_when_it_grows(worker_script, tmp_path: Path):
    script = worker_script(NOISY_WORKER)
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    stale = output_dir / "noisy.out"
    stale.write_text("x" * 5000, encoding="utf-8")

    config = base_config(
        output_dir=str(output_dir),
        targets=[
            TargetSpec(
                name="noisy",
                cmd=[sys.executable, str(script)],
                require_heartbeat=False,
                output_max_bytes=1000,
                on_hang=HangAction.LOG,
            )
        ],
    )

    with RunningSupervisor(config) as supervisor:
        wait_until(lambda: supervisor.metrics.get("target_exits") > 0, timeout=25)

    assert (output_dir / "noisy.out.1").exists(), "the old output was not kept"
    assert "x" * 100 not in (output_dir / "noisy.out").read_text(encoding="utf-8")
