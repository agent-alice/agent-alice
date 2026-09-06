"""Real owned process/Unix-socket transitions with synthetic candidate behavior.

Artifact authenticity and independent environment imports are tested separately
through ReleaseManager's actual installed-wheel tests. No model or native service
is started by these scheduling/lifecycle fixtures.
"""

import asyncio
import json
import os
import signal
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
    if a.mode=='bad':
        record('exited')
        return 71
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
