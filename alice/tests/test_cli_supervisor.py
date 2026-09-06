"""CLI lifecycle entry points with actual local control messages."""

import argparse
import asyncio
from contextlib import asynccontextmanager
import json

import pytest

from alice_codex import cli, launchd
from alice_codex.config import RuntimeConfig
from alice_codex.files import write_json


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
