"""CLI lifecycle entry points with actual local control messages."""

import argparse
import asyncio
from contextlib import asynccontextmanager
import json
import subprocess
import sys
import threading

import pytest

from alice_codex import cli, launchd
from alice_codex.config import RuntimeConfig
from alice_codex.files import write_json
from alice_codex.releases import ReleaseManager


def fixture_config(tmp_path):
    config = RuntimeConfig(str(tmp_path / "alice"), "/usr/bin/true", "fixture", "unused")
    config.prepare_directories()
    config.save()
    return config


@asynccontextmanager
async def control(config, respond):
    async def handle(reader, writer):
        try:
            message = json.loads(await reader.readline())
            result = respond(message)
            if result is not None:
                writer.write(json.dumps(result).encode() + b"\n")
                await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_unix_server(handle, str(config.control_socket))
    try:
        yield
    finally:
        server.close()
        await server.wait_closed()
        config.control_socket.unlink(missing_ok=True)


async def test_chat_start_waits_for_existing_daemon_initialization(tmp_path, monkeypatch):
    config = fixture_config(tmp_path)
    calls = []

    def respond(message):
        calls.append(message["action"])
        return {"ok": True, "result": {"ready": len(calls) > 1}}

    def unexpected_child(*args, **kwargs):
        pytest.fail("an initializing service must not start another daemon")

    monkeypatch.setattr(cli.subprocess, "Popen", unexpected_child)
    async with control(config, respond):
        assert (await cli.start(config))["ready"] is True
    assert calls == ["status", "status"]


async def test_supervisor_can_recover_before_cli_resolves_damaged_current(tmp_path, monkeypatch):
    config = fixture_config(tmp_path)
    write_json(config.root / "state/supervisor.json", {"installed": True})
    write_json(config.root / "releases/current.json", {"current": "damaged-candidate"})
    started = []
    monkeypatch.setattr(launchd, "start", lambda value: started.append(value.home))

    def respond(message):
        return {"ok": True, "result": {"ready": bool(started)}}

    async with control(config, respond):
        assert (await cli.start(config))["ready"] is True
    assert started == [config.home]


async def test_supervised_start_recovers_read_only_status_disconnect(tmp_path, monkeypatch):
    config = fixture_config(tmp_path)
    write_json(config.root / "state/supervisor.json", {"installed": True})
    started = []
    monkeypatch.setattr(launchd, "start", lambda value: started.append(value.home))

    def respond(message):
        if not started:
            return None
        return {"ok": True, "result": {"ready": True}}

    async with control(config, respond):
        assert (await cli.start(config))["ready"] is True
    assert started == [config.home]


async def test_status_disconnect_remains_an_error_for_offline_write_guards(tmp_path):
    from alice_codex.control import ControlError

    config = fixture_config(tmp_path)
    async with control(config, lambda message: None):
        with pytest.raises(ControlError, match="disconnected"):
            await cli.running(config)
        with pytest.raises(ControlError, match="disconnected"):
            await cli.start(config)


async def test_supervisor_blocked_is_reported_without_waiting_for_timeout(tmp_path, monkeypatch):
    config = fixture_config(tmp_path)
    write_json(config.root / "state/supervisor.json", {"installed": True})
    monkeypatch.setattr(launchd, "start", lambda value: None)
    monkeypatch.setattr(launchd, "status", lambda value: {"supervisor": {"lifecycle": "blocked"}})
    with pytest.raises(RuntimeError, match="compatible verified release"):
        await cli.start(config)


@pytest.mark.parametrize("confirmed", [True, False])
async def test_stop_requires_supervisor_confirmation_when_control_is_unavailable(
    tmp_path, monkeypatch, confirmed
):
    config = fixture_config(tmp_path)
    write_json(config.root / "state/supervisor.json", {"installed": True})
    write_json(config.root / "state/runtime.json", {"server": {"pid": 1}})
    called = []

    def stop(value):
        called.append(value.home)
        return {"stopped": confirmed}

    monkeypatch.setattr(launchd, "stop", stop, raising=False)
    args = argparse.Namespace(command="stop", home=config.root)
    if confirmed:
        assert await cli.execute(args) == {"stopped": True}
    else:
        with pytest.raises(RuntimeError, match="has not confirmed"):
            await cli.execute(args)
    assert called == [config.home]


@asynccontextmanager
async def lock_owner(config, name):
    """An actual other process owns a lifecycle lock, with no control socket."""
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        "-c",
        "import fcntl,sys,time; f=open(sys.argv[1],'a'); "
        "fcntl.flock(f,fcntl.LOCK_EX); print('locked',flush=True); time.sleep(60)",
        str(config.root / "state" / name),
        stdout=asyncio.subprocess.PIPE,
    )
    try:
        assert await asyncio.wait_for(child.stdout.readline(), 5) == b"locked\n"
        yield child
    finally:
        if child.returncode is None:
            child.terminate()
        await child.wait()


def release_args(config, operation):
    return argparse.Namespace(
        command="release", operation=operation, home=config.root, candidate_id="candidate-fixture"
    )


@pytest.mark.parametrize("operation", ["activate", "rollback"])
@pytest.mark.parametrize("name", ["lifecycle.lock", "bootstrap.lock", "service.lock"])
async def test_release_switch_rejects_actual_starting_owner_without_control(
    tmp_path, monkeypatch, operation, name
):
    config = fixture_config(tmp_path)

    def unexpected(*args, **kwargs):
        pytest.fail("release pointer must not change while a startup owner exists")

    monkeypatch.setattr(ReleaseManager, operation, unexpected)
    async with lock_owner(config, name):
        assert await cli.running(config) is None
        with pytest.raises(RuntimeError, match="owns this data directory"):
            await cli.execute(release_args(config, operation))


@pytest.mark.parametrize("operation", ["activate", "rollback"])
async def test_loaded_supervisor_blocks_switch_before_it_has_a_child_or_socket(
    tmp_path, monkeypatch, operation
):
    config = fixture_config(tmp_path)
    write_json(
        config.root / "state/supervisor.json",
        {"label": launchd.label(config), "plist": "/synthetic/own.plist"},
    )
    monkeypatch.setattr(
        launchd,
        "_command",
        lambda *args, **kwargs: subprocess.CompletedProcess(args, 0, "state = waiting\n", ""),
    )
    assert await cli.running(config) is None
    with pytest.raises(ValueError, match="Stop the installed supervisor"):
        await cli.execute(release_args(config, operation))


async def test_supervisor_start_and_release_switch_share_real_transition_lock(tmp_path, monkeypatch):
    config = fixture_config(tmp_path)
    write_json(
        config.root / "state/supervisor.json",
        {"version": 2, "label": launchd.label(config), "plist": "/synthetic/own.plist"},
    )
    entered, finish = threading.Event(), threading.Event()
    monkeypatch.setattr(launchd, "checked_runtime", lambda home: {})

    def command(operation, *args, **kwargs):
        if operation == "print":
            return subprocess.CompletedProcess(args, 1, "", "")
        assert operation == "bootstrap"
        entered.set()
        if not finish.wait(5):
            raise TimeoutError("fixture launch transition was not released")
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(launchd, "_command", command)
    task = asyncio.create_task(asyncio.to_thread(launchd.start, config))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        assert await cli.running(config) is None
        with pytest.raises(RuntimeError, match="owns this data directory"):
            await cli.execute(release_args(config, "activate"))
    finally:
        finish.set()
        await task


@pytest.mark.parametrize("operation", ["activate", "rollback"])
async def test_unloaded_quiescent_supervisor_allows_explicit_repair_release(
    tmp_path, monkeypatch, operation
):
    config = fixture_config(tmp_path)
    write_json(
        config.root / "state/supervisor.json",
        {"label": launchd.label(config), "plist": "/synthetic/own.plist"},
    )
    write_json(config.root / "state/bootstrap-state.json", {"lifecycle": "blocked", "child": None})
    monkeypatch.setattr(
        launchd,
        "_command",
        lambda *args, **kwargs: subprocess.CompletedProcess(args, 1, "", ""),
    )
    changed = []
    monkeypatch.setattr(
        ReleaseManager, operation, lambda *args: changed.append(operation) or {"current": "next"}
    )
    assert await cli.execute(release_args(config, operation)) == {"current": "next"}
    assert changed == [operation]


async def test_direct_start_cannot_cross_an_active_release_transition(tmp_path, monkeypatch):
    config = fixture_config(tmp_path)

    def unexpected(*args, **kwargs):
        pytest.fail("an active release transition must prevent process creation")

    # Start the lock fixture before monkeypatching subprocess creation.
    async with lock_owner(config, "lifecycle.lock"):
        monkeypatch.setattr(cli.subprocess, "Popen", unexpected)
        with pytest.raises(RuntimeError, match="owns this data directory"):
            await cli.start(config)


async def test_live_recorded_orphan_blocks_release_even_without_owner_lock_or_socket(tmp_path):
    from alice_codex.service import process_birth

    config = fixture_config(tmp_path)
    async with lock_owner(config, "orphan-fixture.lock") as child:
        write_json(
            config.root / "state/runtime.json",
            {"server": {"pid": child.pid, "birth": process_birth(child.pid)}},
        )
        with pytest.raises(RuntimeError, match="recorded owned process remains"):
            await cli.execute(release_args(config, "activate"))


async def test_release_control_disconnect_does_not_authorize_an_offline_switch(tmp_path):
    from alice_codex.control import ControlError

    config = fixture_config(tmp_path)
    async with control(config, lambda message: None):
        with pytest.raises(ControlError, match="disconnected"):
            await cli.execute(release_args(config, "activate"))
