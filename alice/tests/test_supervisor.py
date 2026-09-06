"""Real owned process/Unix-socket transitions with synthetic candidate behavior.

Artifact authenticity and independent environment imports are tested separately
through ReleaseManager's actual installed-wheel tests. No model or native service
is started by these scheduling/lifecycle fixtures.
"""

import asyncio
import json
import os
import signal
import subprocess
import sys
import time

import pytest

from alice_codex.config import RuntimeConfig
from alice_codex.control import request
from alice_codex.releases import ReleaseError
from alice_codex.service import process_birth
from alice_codex.supervisor import Supervisor

DAEMON = """
import argparse, asyncio, json, os, signal
from pathlib import Path
p=argparse.ArgumentParser()
p.add_argument('--home'); p.add_argument('--mode'); p.add_argument('--socket')
a=p.parse_args()
root=Path(a.home)
def record(kind):
    with (root/'process-events.jsonl').open('a') as stream:
        stream.write(json.dumps({'kind':kind,'pid':os.getpid(),'mode':a.mode})+'\\n')
async def main():
    stop=asyncio.Event()
    loop=asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig,stop.set)
    record('started')
    if a.mode in {'bad','empty'}:
        record('exited')
        return 71 if a.mode=='bad' else 0
    if a.mode=='slow':
        await stop.wait()
        record('exited')
        return 0
    async def control(reader,writer):
        message=json.loads(await reader.readline())
        result={'ready':True,'pid':os.getpid(),'autonomy_paused':True}
        writer.write((json.dumps({'ok':True,'result':result})+'\\n').encode())
        await writer.drain()
        writer.close()
        if message['action']=='shutdown':
            stop.set()
    Path(a.socket).unlink(missing_ok=True)
    server=await asyncio.start_unix_server(control,path=a.socket)
    async with server:
        await stop.wait()
    record('exited')
    return 0
raise SystemExit(asyncio.run(main()))
"""


class Candidates:
    def __init__(self, modes, *, previous="A", current="B"):
        self.pointer = {"current": current, "previous": previous, "activation_epoch": "explicit-1"}
        self.modes = modes
        self.fallbacks = []

    def current(self):
        return dict(self.pointer)

    def checked_current(self):
        if self.modes[self.pointer["current"]] == "changed":
            raise ReleaseError("installed candidate environment changed")
        return {**self.pointer, "python": sys.executable}

    def automatic_rollback(self, expected_current, failed_candidates, *, expected_epoch):
        assert expected_epoch == self.pointer["activation_epoch"]
        assert expected_current == self.pointer["current"]
        previous = self.pointer["previous"]
        if not previous or previous in failed_candidates:
            raise ReleaseError("no unfailed previous candidate is available")
        self.fallbacks.append((expected_current, previous))
        self.pointer.update(current=previous, previous=expected_current)
        return self.current()


class ProcessSupervisor(Supervisor):
    def launch_command(self, pointer):
        return [
            sys.executable,
            "-I",
            str(self.config.root / "alice_codex_fake_daemon.py"),
            "--home",
            self.config.home,
            "--mode",
            self.manager.modes[pointer["current"]],
            "--socket",
            str(self.config.control_socket),
        ]


@pytest.fixture
def runtime(tmp_path):
    config = RuntimeConfig(str(tmp_path), "/tmp/codex-fixture", "fixture", "fixture")
    config.prepare_directories()
    (tmp_path / "alice_codex_fake_daemon.py").write_text(DAEMON)
    yield config
    config.control_socket.unlink(missing_ok=True)
    config.codex_socket.unlink(missing_ok=True)
    config.socket_dir.rmdir()


def supervisor(config, modes, **manager_options):
    value = ProcessSupervisor(
        config,
        startup_timeout=0.35,
        healthy_seconds=0.4,
        stop_timeout=0.5,
        retry_delay=0.01,
    )
    value.manager = Candidates(modes, **manager_options)
    return value


async def eventually(predicate, *, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = await predicate()
        if result:
            return result
        await asyncio.sleep(0.02)
    raise TimeoutError("synthetic lifecycle transition was not observed")


def records(config):
    path = config.root / "process-events.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


async def stop_ready(value):
    state = await value.status()
    assert state and state["ready"]
    await request(value.config.control_socket, "shutdown")
    # Installed CLI stop also signals the supervisor; a daemon can complete its
    # shutdown before the supervisor first observes its ready socket.
    value.stop_event.set()


@pytest.mark.asyncio
async def test_two_actual_startup_failures_fall_back_once_without_replacing_data(runtime):
    value = supervisor(runtime, {"A": "good", "B": "bad"})
    marker = runtime.root / "new-evidence.json"
    marker.write_text('{"after_promotion":true}')
    task = asyncio.create_task(value.run())
    try:
        await eventually(lambda: value.status())
        assert value.manager.current()["current"] == "A"
        assert value.manager.fallbacks == [("B", "A")]
        await stop_ready(value)
        assert await asyncio.wait_for(task, 5) == 0
    finally:
        value.stop_event.set()
        await asyncio.gather(task, return_exceptions=True)
    assert marker.read_text() == '{"after_promotion":true}'
    assert [row["mode"] for row in records(runtime) if row["kind"] == "started"] == [
        "bad",
        "bad",
        "good",
    ]
    assert value.state["lifecycle"] == "stopped"
    assert not any(process_birth(row["pid"]) for row in records(runtime))


@pytest.mark.asyncio
async def test_failed_previous_blocks_and_restart_cannot_bounce_back(runtime):
    value = supervisor(runtime, {"A": "bad", "B": "bad"})
    assert await asyncio.wait_for(value.run(), 10) == 0
    assert value.state["lifecycle"] == "blocked"
    assert value.manager.fallbacks == [("B", "A")]
    before = records(runtime)
    assert len([row for row in before if row["kind"] == "started"]) == 4
    restarted = supervisor(runtime, {"A": "bad", "B": "bad"})
    restarted.manager = value.manager
    assert await restarted.run() == 0
    assert records(runtime) == before


@pytest.mark.asyncio
async def test_no_previous_stops_after_finite_real_failures(runtime):
    value = supervisor(runtime, {"B": "bad"}, previous=None)
    assert await asyncio.wait_for(value.run(), 10) == 0
    assert value.state["lifecycle"] == "blocked"
    assert len([row for row in records(runtime) if row["kind"] == "started"]) == 2


@pytest.mark.asyncio
async def test_changed_candidate_never_executes_and_previous_can_run(runtime):
    value = supervisor(runtime, {"A": "good", "B": "changed"})
    task = asyncio.create_task(value.run())
    try:
        await eventually(lambda: value.status())
        await stop_ready(value)
        assert await asyncio.wait_for(task, 5) == 0
    finally:
        value.stop_event.set()
        await asyncio.gather(task, return_exceptions=True)
    assert [row["mode"] for row in records(runtime) if row["kind"] == "started"] == ["good"]


@pytest.mark.asyncio
async def test_startup_timeout_exits_owned_process_before_fallback(runtime):
    value = supervisor(runtime, {"A": "good", "B": "slow"})
    task = asyncio.create_task(value.run())
    try:
        await eventually(lambda: value.status())
        await stop_ready(value)
        assert await asyncio.wait_for(task, 5) == 0
    finally:
        value.stop_event.set()
        await asyncio.gather(task, return_exceptions=True)
    events = records(runtime)
    assert [row["kind"] for row in events] == ["started", "exited"] * 3
    assert [row["mode"] for row in events if row["kind"] == "started"] == ["slow", "slow", "good"]


@pytest.mark.asyncio
async def test_explicit_stop_during_startup_never_restarts(runtime):
    value = supervisor(runtime, {"A": "good", "B": "slow"})
    task = asyncio.create_task(value.run())

    async def child_exists():
        return value.state.get("child")

    await eventually(child_exists)
    value.stop_event.set()
    assert await asyncio.wait_for(task, 5) == 0
    assert value.manager.fallbacks == []
    assert value.state["lifecycle"] == "stopped"
    assert len([row for row in records(runtime) if row["kind"] == "started"]) <= 1


@pytest.mark.asyncio
@pytest.mark.parametrize("explicit", [False, True])
async def test_three_short_normal_stops_do_not_consume_startup_failure_budget(runtime, explicit):
    manager = Candidates({"A": "good", "B": "good"})
    for _ in range(3):
        value = supervisor(runtime, manager.modes)
        value.manager = manager
        value.healthy_seconds = 60
        task = asyncio.create_task(value.run())
        try:

            async def running():
                return value.state.get("lifecycle") == "running"

            await eventually(running)
            await request(runtime.control_socket, "shutdown")
            if explicit:
                value.stop_event.set()
            assert await asyncio.wait_for(task, 5) == 0
        finally:
            value.stop_event.set()
            await asyncio.gather(task, return_exceptions=True)
        assert value.state["attempts"]["B"] == 0
        assert manager.current()["current"] == "B"
        assert manager.fallbacks == []
    assert len([row for row in records(runtime) if row["kind"] == "started"]) == 3


@pytest.mark.asyncio
async def test_explicit_stop_during_failed_attempt_cleanup_resets_counter(runtime, monkeypatch):
    value = supervisor(runtime, {"A": "good", "B": "bad"})
    original = value.clean_orphan
    calls = 0

    async def cleanup():
        nonlocal calls
        calls += 1
        await original()
        if calls == 2:
            value.stop_event.set()

    monkeypatch.setattr(value, "clean_orphan", cleanup)
    assert await asyncio.wait_for(value.run(), 5) == 0
    assert value.state["attempts"]["B"] == 0
    assert value.manager.fallbacks == []
    assert len([row for row in records(runtime) if row["kind"] == "started"]) == 1


@pytest.mark.asyncio
async def test_healthy_runtime_resets_old_failure_count_before_a_later_crash(runtime):
    value = supervisor(runtime, {"A": "bad", "B": "good"})
    value.state.update(activation_epoch="explicit-1", attempts={"B": 1})
    task = asyncio.create_task(value.run())
    try:

        async def healthy():
            return value.state.get("lifecycle") == "healthy"

        await eventually(healthy)
        old = dict(value.state["child"])
        assert process_birth(old["pid"]) == old["birth"]
        os.kill(old["pid"], signal.SIGKILL)

        async def recovered():
            state = await value.status()
            return state if state and state["pid"] != old["pid"] else None

        await eventually(recovered)
        assert value.manager.fallbacks == []
        await stop_ready(value)
        assert await asyncio.wait_for(task, 5) == 0
    finally:
        value.stop_event.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_cleanup_failure_prevents_starting_another_candidate(runtime, monkeypatch):
    value = supervisor(runtime, {"A": "good", "B": "bad"})
    original = value.clean_orphan
    calls = 0

    async def cleanup():
        nonlocal calls
        calls += 1
        if calls > 1:
            raise RuntimeError("Recorded server identity changed")
        await original()

    monkeypatch.setattr(value, "clean_orphan", cleanup)
    with pytest.raises(RuntimeError, match="identity changed"):
        await value.run()
    assert value.manager.fallbacks == []
    assert len([row for row in records(runtime) if row["kind"] == "started"]) == 1


@pytest.mark.asyncio
async def test_second_supervisor_cannot_interrupt_first_owned_daemon(runtime):
    value = supervisor(runtime, {"A": "good", "B": "good"})
    task = asyncio.create_task(value.run())
    try:
        await eventually(lambda: value.status())
        child = dict(value.state["child"])
        other = supervisor(runtime, {"A": "good", "B": "good"})
        with pytest.raises(RuntimeError, match="owns this data directory"):
            await other.run()
        assert process_birth(child["pid"]) == child["birth"]
        await stop_ready(value)
        assert await asyncio.wait_for(task, 5) == 0
    finally:
        value.stop_event.set()
        await asyncio.gather(task, return_exceptions=True)


def test_unreaped_exited_child_is_not_mistaken_for_a_changed_live_identity(runtime):
    from alice_codex.service import process_identity

    # Keep a real exited child deliberately unreaped to make the Linux child
    # watcher race deterministic. The child executes only async-signal-safe OS
    # calls after fork; no model, service, or Python worker is started there.
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(write_fd)
        os.setsid()
        os.read(read_fd, 1)
        os._exit(0)
    os.close(read_fd)
    try:
        child = {"pid": pid, "birth": process_birth(pid), "identity": process_identity(pid)}
        assert child["birth"]
        os.write(write_fd, b"x")
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            status = subprocess.run(
                ["ps", "-p", str(pid), "-o", "stat="], capture_output=True, text=True, check=True
            ).stdout.strip()
            if status.startswith("Z"):
                break
            time.sleep(0.01)
        assert status.startswith("Z"), "fixture child was not observed as an unreaped zombie"
        assert process_birth(pid) == child["birth"]
        value = supervisor(runtime, {"A": "good", "B": "good"})
        assert value._signalable_child(child) is False
    finally:
        os.close(write_fd)
        os.waitpid(pid, 0)


def test_changed_live_child_identity_is_still_rejected(runtime):
    from alice_codex.service import process_identity

    child = subprocess.Popen(
        [sys.executable, "-I", "-c", "import time; time.sleep(30)"], start_new_session=True
    )
    try:
        record = {
            "pid": child.pid,
            "birth": process_birth(child.pid),
            "identity": process_identity(child.pid),
        }
        value = supervisor(runtime, {"A": "good", "B": "good"})
        with pytest.raises(RuntimeError, match="identity changed"):
            value._signalable_child(record)
        assert child.poll() is None
    finally:
        child.terminate()
        child.wait(timeout=5)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["corrupt", "future"])
async def test_owned_native_is_stopped_before_rejecting_damaged_business_store(runtime, failure):
    import sqlite3
    from alice_codex.files import write_json
    from alice_codex.service import process_identity

    native = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        "import time; time.sleep(30)",
        str(runtime.codex_socket),
        start_new_session=True,
    )
    state = {
        "version": 1,
        "tasks": {},
        "intents": {},
        "lifecycle": "running",
        "server": {
            "pid": native.pid,
            "birth": process_birth(native.pid),
            "identity": process_identity(native.pid),
        },
    }
    write_json(runtime.root / "state/runtime.json", state)
    database = runtime.root / "memory-state/sources.sqlite3"
    database.parent.mkdir()
    if failure == "corrupt":
        database.write_bytes(b"synthetic corrupt database; preserve these bytes")
    else:
        with sqlite3.connect(database) as db:
            db.execute("PRAGMA user_version=2")
            db.execute("CREATE TABLE evidence(value TEXT)")
            db.execute("INSERT INTO evidence VALUES ('newer data')")
    before = database.read_bytes()
    value = supervisor(runtime, {"A": "good", "B": "good"})
    try:
        with pytest.raises(ReleaseError, match="memory"):
            await value.run()
        assert await asyncio.wait_for(native.wait(), 5) != 0
        assert database.read_bytes() == before
        assert value.manager.fallbacks == []
        assert records(runtime) == []
        assert json.loads((runtime.root / "state/runtime.json").read_text()) == state
    finally:
        if native.returncode is None:
            native.kill()
            await native.wait()


@pytest.mark.asyncio
async def test_exit_zero_before_readiness_is_a_startup_failure_not_an_explicit_stop(runtime):
    value = supervisor(runtime, {"A": "good", "B": "empty"})
    task = asyncio.create_task(value.run())
    try:
        await eventually(lambda: value.status())
        assert value.manager.fallbacks == [("B", "A")]
        await stop_ready(value)
        assert await asyncio.wait_for(task, 5) == 0
    finally:
        value.stop_event.set()
        await asyncio.gather(task, return_exceptions=True)
    assert [row["mode"] for row in records(runtime) if row["kind"] == "started"] == [
        "empty",
        "empty",
        "good",
    ]


@pytest.mark.asyncio
async def test_clean_exit_after_observed_readiness_stays_stopped(runtime):
    value = supervisor(runtime, {"A": "good", "B": "good"})
    task = asyncio.create_task(value.run())
    try:

        async def observed_ready():
            return value.state.get("lifecycle") in {"running", "healthy"}

        await eventually(observed_ready)
        await request(runtime.control_socket, "shutdown")
        assert await asyncio.wait_for(task, 5) == 0
        assert value.manager.fallbacks == []
    finally:
        value.stop_event.set()
        await asyncio.gather(task, return_exceptions=True)
