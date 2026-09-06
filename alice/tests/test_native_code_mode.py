"""Installed native pair executes functions.exec through its real Code Mode host.

The Responses endpoint is an owned localhost fixture, without authentication or
model inference. This test does not use Desktop, a personal home or a live site.
"""

import asyncio
import json
import os
from pathlib import Path
import signal
import subprocess

import pytest
import tomlkit

from alice_codex.config import initialize_config
from alice_codex.rpc import RpcClient
from alice_codex.runtime_bundle import verify_runtime_bundle


pytestmark = pytest.mark.native


def _processes():
    result = subprocess.run(
        ["ps", "-axo", "pid=,ppid=,pgid=,lstart=,comm="],
        capture_output=True,
        text=True,
        check=True,
        timeout=5,
    )
    rows = {}
    for line in result.stdout.splitlines():
        parts = line.strip().split(None, 8)
        if len(parts) == 9:
            rows[int(parts[0])] = {
                "pid": int(parts[0]),
                "ppid": int(parts[1]),
                "pgid": int(parts[2]),
                "birth": " ".join(parts[3:8]),
                "command": parts[8],
            }
    return rows


async def test_native_pinned_pair_executes_code_mode_read(tmp_path):
    binary = Path(
        os.environ.get(
            "ALICE_TEST_CODEX_BINARY", "/Applications/ChatGPT.app/Contents/Resources/codex"
        )
    )
    if not binary.is_file():
        if "ALICE_TEST_CODEX_BINARY" in os.environ:
            pytest.fail("Required native Codex executable is unavailable")
        pytest.skip("Native Codex executable is unavailable")
    config = initialize_config(tmp_path / "alice", binary)
    pair = verify_runtime_bundle(config, require=True)
    assert Path(config.codex_binary) != binary.resolve(), "Exercise the installed copy"
    process_home = tmp_path / "process-home"
    process_home.mkdir(mode=0o700)
    marker = "owned-code-mode-fixture-" + tmp_path.name
    (config.workspace / "input.txt").write_text(marker + "\n")
    requests, fixture_errors, events, peers, handlers, owned = [], [], [], set(), set(), {}
    finished = asyncio.Event()
    process = rpc = None

    def remember():
        snapshot = _processes()
        selected = {process.pid} if process else set()
        while True:
            expanded = selected | {pid for pid, row in snapshot.items() if row["ppid"] in selected}
            if expanded == selected:
                break
            selected = expanded
        owned.update({pid: row for pid, row in snapshot.items() if pid in selected})
        return snapshot

    async def responses(reader, writer):
        peers.add(writer)
        handler = asyncio.current_task()
        handlers.add(handler)
        try:
            header = (await reader.readuntil(b"\r\n\r\n")).decode().split("\r\n")
            headers = {
                row.split(":", 1)[0].lower(): row.split(":", 1)[1].strip()
                for row in header[1:]
                if ":" in row
            }
            assert header[0] == "POST /responses HTTP/1.1"
            assert "authorization" not in headers
            body = json.loads(await reader.readexactly(int(headers["content-length"])))
            requests.append(body)
            number = len(requests)
            if number == 1:
                advertised = body.get("tools", []) + [
                    tool
                    for item in body["input"]
                    if item.get("type") == "additional_tools"
                    for tool in item.get("tools", [])
                ]
                matches = []
                for tool in advertised:
                    if tool.get("type") == "namespace":
                        matches.extend(
                            {"namespace": tool["name"], "name": nested["name"]}
                            for nested in tool.get("tools", [])
                            if nested.get("name") == "exec" and nested.get("type") == "custom"
                        )
                    elif tool.get("name") == "exec" and tool.get("type") == "custom":
                        matches.append({"name": "exec"})
                assert len(matches) == 1, "Native request must advertise one Code Mode exec"
                command = {
                    "cmd": "cat input.txt",
                    "login": False,
                    "workdir": str(config.workspace),
                    "max_output_tokens": 100,
                }
                item = {
                    "type": "custom_tool_call",
                    "call_id": "read-through-code-mode",
                    **matches[0],
                    "input": "text(await tools.exec_command(" + json.dumps(command) + "));",
                }
            elif number == 2:
                outputs = [
                    item
                    for item in body["input"]
                    if item.get("call_id") == "read-through-code-mode"
                    and item.get("type") == "custom_tool_call_output"
                ]
                assert marker in json.dumps(outputs), (
                    "Actual Code Mode output must contain file data"
                )
                remember()
                assert any(
                    row["command"] == pair["codex_code_mode_host"] and row["ppid"] == process.pid
                    for row in owned.values()
                ), "The pinned native host must actually run as Codex's child"
                item = {
                    "type": "message",
                    "role": "assistant",
                    "id": "recorded-complete",
                    "content": [{"type": "output_text", "text": "Recorded fixture complete."}],
                }
            else:
                raise AssertionError("Unexpected extra recorded request")
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n"
            )
            for event in (
                {"type": "response.created", "response": {"id": str(number)}},
                {"type": "response.output_item.done", "item": item},
                {
                    "type": "response.completed",
                    "response": {
                        "id": str(number),
                        "usage": {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
                    },
                },
            ):
                writer.write(("data: " + json.dumps(event) + "\n\n").encode())
            await writer.drain()
        except Exception as error:
            fixture_errors.append(repr(error))
            finished.set()
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass
            peers.discard(writer)
            handlers.discard(handler)

    http = await asyncio.start_server(responses, "127.0.0.1", 0)
    port = http.sockets[0].getsockname()[1]
    document = tomlkit.parse((config.codex_home / "config.toml").read_text())
    document.update(
        model="gpt-6-astra",
        model_provider="recorded",
        approval_policy="never",
        check_for_update_on_startup=False,
        web_search="disabled",
    )
    document["features"].update(
        apps=False,
        plugins=False,
        memories=False,
        code_mode=True,
        code_mode_host={"enabled": True, "disable_in_process_fallback": True},
    )
    document["mcp_servers"]["alice"].update(enabled=False, required=False)
    document["model_providers"] = {
        "recorded": {
            "name": "Owned recorded Responses",
            "base_url": f"http://127.0.0.1:{port}",
            "wire_api": "responses",
            "supports_websockets": False,
            "requires_openai_auth": False,
            "request_max_retries": 0,
            "stream_max_retries": 0,
        }
    }
    (config.codex_home / "config.toml").write_text(tomlkit.dumps(document))
    env = {
        "HOME": str(process_home),
        "CODEX_HOME": str(config.codex_home),
        "PATH": "/usr/bin:/bin",
        "RUST_LOG": "error",
        "NO_PROXY": "127.0.0.1,localhost",
        "no_proxy": "127.0.0.1,localhost",
    }
    log = (tmp_path / "native-code-mode.log").open("wb")
    try:
        async with asyncio.timeout(60):
            process = await asyncio.create_subprocess_exec(
                config.codex_binary,
                "app-server",
                "--listen",
                f"unix://{config.codex_socket}",
                cwd=config.workspace,
                env=env,
                stdout=log,
                stderr=log,
                start_new_session=True,
            )
            for _ in range(200):
                if config.codex_socket.exists():
                    break
                assert process.returncode is None, "Native App Server exited before socket creation"
                await asyncio.sleep(0.05)
            rpc = await RpcClient.connect_unix(config.codex_socket)
            await rpc.initialize(name="alice_native_code_mode_test")

            def event(message):
                events.append(message)
                if message.get("method") == "turn/completed":
                    finished.set()

            rpc.add_listener(event)
            started = await rpc.request("thread/start", {"cwd": str(config.workspace)})
            await rpc.request(
                "turn/start",
                {
                    "threadId": started["thread"]["id"],
                    "input": [{"type": "text", "text": "Execute the owned recorded fixture."}],
                },
            )
            await finished.wait()
            assert not fixture_errors, fixture_errors
            assert len(requests) == 2
            turns = [item for item in events if item.get("method") == "turn/completed"]
            assert turns[-1]["params"]["turn"]["status"] == "completed"
    finally:
        if process:
            remember()
        if rpc:
            await rpc.close()
        if process and process.returncode is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                await asyncio.wait_for(process.wait(), 8)
            except asyncio.TimeoutError:
                os.killpg(process.pid, signal.SIGKILL)
                await process.wait()
        log.close()
        http.close()
        await http.wait_closed()
        for writer in peers.copy():
            writer.close()
        if handlers:
            await asyncio.wait_for(asyncio.gather(*handlers, return_exceptions=True), 5)
        await asyncio.sleep(0.1)
        snapshot = _processes()
        remaining = [
            row
            for pid, row in owned.items()
            if pid in snapshot and snapshot[pid]["birth"] == row["birth"]
        ]
        for row in remaining:
            try:
                os.kill(row["pid"], signal.SIGKILL)
            except ProcessLookupError:
                pass
        assert not remaining, "Owned Code Mode processes outlived native shutdown"
        assert process is None or process.returncode == 0
