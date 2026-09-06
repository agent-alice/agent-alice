"""Real Alice CLI + native Codex + recorded localhost Responses + actual MCP.

The model endpoint is a test-owned HTTP server with no credentials. This is
native execution/lifecycle evidence, not a model-capability or paid API test.
"""

import asyncio
from contextlib import suppress
import json
import os
from pathlib import Path
import signal
import subprocess
import sys

import pytest
import tomlkit

from alice_codex.config import load_config
from alice_codex.control import request
from alice_codex.rpc import RpcClient

pytestmark = pytest.mark.native


def process_is_alive(pid):
    result = subprocess.run(
        ["ps", "-p", str(pid), "-o", "stat="], capture_output=True, text=True, timeout=5
    )
    return bool(result.stdout.strip()) and not result.stdout.strip().startswith("Z")


def process_identity(pid):
    return subprocess.run(
        ["ps", "-p", str(pid), "-o", "lstart=", "-o", "command="],
        capture_output=True,
        text=True,
        timeout=5,
    ).stdout.strip()


async def until(operation, timeout=15):
    async def wait():
        while True:
            value = await operation()
            if value:
                return value
            await asyncio.sleep(0.03)

    return await asyncio.wait_for(wait(), timeout)


async def test_native_service_mcp_interrupt_crash_recovery_and_required_mcp_failure(tmp_path):
    binary = Path(
        os.environ.get(
            "ALICE_TEST_CODEX_BINARY", "/Applications/ChatGPT.app/Contents/Resources/codex"
        )
    )
    if not binary.is_file():
        if "ALICE_TEST_CODEX_BINARY" in os.environ:
            pytest.fail("Required native Codex binary is unavailable")
        pytest.skip("Native Codex binary unavailable")
    version = subprocess.run(
        [str(binary), "--version"], capture_output=True, text=True, check=True, timeout=10
    ).stdout.strip()
    assert version == "codex-cli 0.153.4", (
        "native integration is pinned to its verified protocol version"
    )
    home, process_home = tmp_path / "alice", tmp_path / "process-home"
    process_home.mkdir()
    runtime_python = os.environ.get("ALICE_ARTIFACT_PYTHON", sys.executable)
    env = {
        "PATH": str(Path(runtime_python).parent)
        + os.pathsep
        + os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": str(process_home),
        "LANG": "en_US.UTF-8",
        "RUST_LOG": "error",
        "PYTHONDONTWRITEBYTECODE": "1",
        "NO_PROXY": "127.0.0.1,localhost,::1",
        "no_proxy": "127.0.0.1,localhost,::1",
    }
    peers, handlers, model_requests, errors = set(), set(), [], []
    held, release_held, held_closed = asyncio.Event(), asyncio.Event(), asyncio.Event()
    crash_held, release_crash, crash_closed = asyncio.Event(), asyncio.Event(), asyncio.Event()
    tool_identity = {}

    async def model(reader, writer):
        peers.add(writer)
        task = asyncio.current_task()
        handlers.add(task)
        held_stream = False
        crash_stream = False
        try:
            header = await reader.readuntil(b"\r\n\r\n")
            lines = header.decode().split("\r\n")
            headers = {
                line.split(":", 1)[0].lower(): line.split(":", 1)[1].strip()
                for line in lines[1:]
                if ":" in line
            }
            assert lines[0] == "POST /responses HTTP/1.1", lines[0]
            assert headers["host"].startswith("127.0.0.1:")
            assert "authorization" not in headers
            assert "content-encoding" not in headers, "recorded requests must remain inspectable"
            body = json.loads(await reader.readexactly(int(headers["content-length"])))
            model_requests.append(body)
            number = len(model_requests)
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n"
            )
            if number == 1:
                assert any(tool.get("type") == "tool_search" for tool in body["tools"])
                item = {
                    "type": "tool_search_call",
                    "call_id": "discover-alice-cron",
                    "execution": "client",
                    "arguments": {"query": "alice cron_create", "limit": 1},
                }
            elif number == 2:
                # Match the actual exported native tool specification, rather than
                # assuming namespace flattening is unchanged between versions.
                matches = []
                discovered = [
                    tool
                    for row in body["input"]
                    if row.get("type") == "tool_search_output"
                    for tool in row.get("tools", [])
                ]
                for tool in [*body["tools"], *discovered]:
                    if tool.get("type") == "namespace":
                        matches.extend(
                            {"namespace": tool["name"], "name": item["name"]}
                            for item in tool.get("tools", [])
                            if "cron_create" in item.get("name", "")
                        )
                    elif "cron_create" in tool.get("name", ""):
                        matches.append({"name": tool["name"]})
                assert len(matches) == 1, (
                    f"Alice cron tool absent/ambiguous after tool search: {matches}; discovered={discovered}"
                )
                tool_identity.update(matches[0])
                item = {
                    "type": "function_call",
                    "call_id": "native-create-schedule",
                    **tool_identity,
                    "arguments": json.dumps(
                        {
                            "name": "created by native MCP execution",
                            "schedule_type": "every",
                            "schedule_value": 3600,
                            "prompt": "test only",
                            "enabled": False,
                        }
                    ),
                }
            elif number == 3:
                outputs = [
                    row
                    for row in body["input"]
                    if row.get("call_id") == "native-create-schedule"
                    and row.get("type") == "function_call_output"
                ]
                assert len(outputs) == 1, body["input"]
                assert "created by native MCP execution" in json.dumps(outputs), outputs
                item = {
                    "type": "message",
                    "role": "assistant",
                    "id": "native-done",
                    "content": [
                        {"type": "output_text", "text": "Recorded native execution completed."}
                    ],
                }
            elif number == 4:
                held_stream = True
                writer.write(
                    b'data: {"type":"response.created","response":{"id":"held-native-turn"}}\n\n'
                )
                await writer.drain()
                held.set()
                await release_held.wait()
                item = {
                    "type": "function_call",
                    "call_id": "late-native-schedule",
                    **tool_identity,
                    "arguments": json.dumps(
                        {
                            "name": "late forbidden schedule",
                            "schedule_type": "every",
                            "schedule_value": 3600,
                            "prompt": "must not execute",
                            "enabled": False,
                        }
                    ),
                }
            else:
                assert number == 5, f"unexpected additional model request {number}"
                crash_stream = True
                writer.write(
                    b'data: {"type":"response.created","response":{"id":"crashed-native-turn"}}\n\n'
                )
                await writer.drain()
                crash_held.set()
                await release_crash.wait()
                item = {
                    "type": "message",
                    "role": "assistant",
                    "id": "must-not-complete-after-crash",
                    "content": [{"type": "output_text", "text": "Late crash result."}],
                }
            events = [
                {"type": "response.created", "response": {"id": f"native-{number}"}},
                {"type": "response.output_item.done", "item": item},
                {
                    "type": "response.completed",
                    "response": {
                        "id": f"native-{number}",
                        "usage": {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
                    },
                },
            ]
            for event in events:
                writer.write(("data: " + json.dumps(event) + "\n\n").encode())
            await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        except Exception as exc:
            errors.append(str(exc))
        finally:
            writer.close()
            with suppress(ConnectionError):
                await writer.wait_closed()
            peers.discard(writer)
            handlers.discard(task)
            if held_stream:
                held_closed.set()
            if crash_stream:
                crash_closed.set()

    http = await asyncio.start_server(model, "127.0.0.1", 0)
    port = http.sockets[0].getsockname()[1]
    service, rpc, config = None, None, None
    owned_codex_pids = {}
    owned_mcp = {}

    def remember_mcp():
        rows = subprocess.run(
            ["ps", "-axo", "pid=,command="], capture_output=True, text=True, check=True, timeout=5
        ).stdout.splitlines()
        matched = [
            int(row.split(None, 1)[0]) for row in rows if f"alice_codex.mcp --home {home}" in row
        ]
        assert matched, "native Codex must have started the real Alice MCP subprocess"
        for pid in matched:
            owned_mcp[pid] = process_identity(pid)

    async def mcp_exited():
        return all(not process_is_alive(pid) for pid in owned_mcp)

    async def cli(*args, timeout=30, expected_code=0):
        process = await asyncio.create_subprocess_exec(
            runtime_python,
            "-I",
            "-m",
            "alice_codex",
            "--home",
            str(home),
            *args,
            cwd=tmp_path,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout)
        except BaseException:
            if process.returncode is None:
                process.kill()
                await process.wait()
            raise
        assert process.returncode == expected_code, (
            f"CLI {args}: {stderr.decode()}\n{stdout.decode()} errors={errors}"
        )
        return json.loads(stdout) if expected_code == 0 else stderr.decode()

    async def launch():
        nonlocal service
        service = await asyncio.create_subprocess_exec(
            runtime_python,
            "-I",
            "-m",
            "alice_codex",
            "--home",
            str(home),
            "serve",
            cwd=tmp_path,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )

        async def ready():
            if service.returncode is not None:
                pytest.fail(
                    f"native integrated service exited: {(await service.stderr.read()).decode()}"
                )
            try:
                status = await request(config.control_socket, "status", timeout=1)
                return status if status["ready"] else None
            except (OSError, TimeoutError):
                return None

        status = await until(ready, timeout=35)
        owned_codex_pids[status["codex_pid"]] = process_identity(status["codex_pid"])
        return status

    try:
        assert (await cli("init", "--codex", str(binary), "--no-pin", "--model", "gpt-5.4"))[
            "autonomy_paused"
        ]
        config = load_config(home)
        config_file = config.codex_home / "config.toml"
        original = config_file.read_text()
        configured = 'model_provider = "recorded"\ncheck_for_update_on_startup = false\n' + original
        configured = configured.replace('web_search = "live"', 'web_search = "disabled"')
        configured = configured.replace(
            "[features]",
            "[features]\napps = false\nplugins = false\nenable_request_compression = false",
        )
        configured += f"""
[model_providers.recorded]
name = "Native service recorded localhost fixture"
base_url = "http://127.0.0.1:{port}"
wire_api = "responses"
supports_websockets = false
requires_openai_auth = false
request_max_retries = 0
stream_max_retries = 0
"""
        config_file.write_text(configured)
        assert not (config.codex_home / "auth.json").exists()
        assert not any("API_KEY" in key or "TOKEN" in key for key in env)
        started = await launch()
        task = await request(config.control_socket, "thread", {"target": "main"})
        thread_id = task["thread_id"]
        rpc = await RpcClient.connect_unix(config.codex_socket)
        await rpc.initialize()

        async def tools_ready():
            result = await rpc.request(
                "mcpServerStatus/list", {"threadId": thread_id, "limit": 100}
            )
            alice = next((row for row in result["data"] if row["name"] == "alice"), None)
            return alice if alice and any("cron_create" in key for key in alice["tools"]) else None

        catalog = await until(tools_ready, timeout=20)
        assert catalog.get("toolsError") is None
        remember_mcp()
        done = await cli(
            "ask",
            "Create the recorded disabled schedule via Alice MCP.",
            "--request-id",
            "native-integrated-first",
            "--wait",
            "20",
            timeout=25,
        )
        assert not errors, "\n".join(errors)
        assert done["intent"]["status"] == "completed", done
        jobs = (await cli("cron", "list"))["jobs"]
        created = [job for job in jobs if job["name"] == "created by native MCP execution"]
        assert len(created) == 1 and not created[0]["enabled"], json.dumps(
            model_requests[2]["input"]
        )
        assert len(model_requests) == 3
        assert "Recorded native execution completed." in json.dumps(done["thread"])
        repeated = await cli(
            "ask",
            "Create the recorded disabled schedule via Alice MCP.",
            "--request-id",
            "native-integrated-first",
        )
        assert repeated == done["intent"]
        assert len(model_requests) == 3, "a repeated request ID must not start a second turn"

        await cli("resume")
        waiting = await cli(
            "ask",
            "Remain active until explicitly paused.",
            "--request-id",
            "native-integrated-held",
        )
        await asyncio.wait_for(held.wait(), 15)
        assert waiting["thread_id"] == thread_id
        # Act through the native transport, as the attached TUI does. Alice must
        # learn about the interruption without a request to its own pause API.
        await rpc.request("turn/interrupt", {"threadId": thread_id, "turnId": waiting["turn_id"]})

        async def native_pause_persisted():
            state = await request(config.control_socket, "status")
            return state["autonomy_paused"] and state["tasks"]["main"]["paused"]

        await until(native_pause_persisted)
        release_held.set()
        await asyncio.wait_for(held_closed.wait(), 5)
        assert not any(
            job["name"] == "late forbidden schedule" for job in (await cli("cron", "list"))["jobs"]
        )
        await rpc.close()
        rpc = None
        assert (await cli("stop", timeout=25))["stopped"]
        await asyncio.wait_for(service.wait(), 10)
        assert service.returncode == 0
        assert not process_is_alive(started["codex_pid"])
        await until(mcp_exited)

        restarted = await launch()
        assert restarted["autonomy_paused"]
        assert restarted["tasks"]["main"]["thread_id"] == thread_id
        # Read/resume the existing thread without starting another model request.
        resumed = await request(config.control_socket, "thread", {"target": "main"})
        assert resumed["thread_id"] == thread_id
        history = await cli("task-status", "--target", "main")
        assert "Recorded native execution completed." in json.dumps(history)
        assert len(model_requests) == 4 and not errors
        rpc = await RpcClient.connect_unix(config.codex_socket)
        await rpc.initialize()
        assert (await until(tools_ready, timeout=20)).get("toolsError") is None
        remember_mcp()
        await rpc.close()
        rpc = None

        # Kill the control process itself; its separate native process group is
        # still live and must be recovered using the recorded process identity.
        service.kill()
        await asyncio.wait_for(service.wait(), 5)
        assert service.returncode == -signal.SIGKILL
        assert process_is_alive(restarted["codex_pid"])
        recovered = await launch()
        assert recovered["codex_pid"] != restarted["codex_pid"]
        assert not process_is_alive(restarted["codex_pid"])
        assert recovered["autonomy_paused"] and recovered["tasks"]["main"]["paused"]
        assert recovered["tasks"]["main"]["thread_id"] == thread_id
        repeated = await cli(
            "ask",
            "Create the recorded disabled schedule via Alice MCP.",
            "--request-id",
            "native-integrated-first",
        )
        assert repeated == done["intent"]
        assert len(model_requests) == 4

        # Kill real Codex during a held turn, after the native receipt and the
        # host intent have both been recorded. Recovery must retain the same
        # association and never turn absence of a completion into success.
        await cli("resume")
        crashed = await cli(
            "ask", "Wait for the isolated Codex crash.", "--request-id", "native-crashed-intent"
        )
        await asyncio.wait_for(crash_held.wait(), 15)
        remember_mcp()
        assert crashed["thread_id"] == thread_id
        assert process_identity(recovered["codex_pid"]) == owned_codex_pids[recovered["codex_pid"]]
        os.kill(recovered["codex_pid"], signal.SIGKILL)
        await asyncio.wait_for(service.wait(), 15)
        assert not process_is_alive(recovered["codex_pid"])
        await until(mcp_exited)
        after_crash = await launch()
        assert after_crash["autonomy_paused"]
        assert after_crash["tasks"]["main"]["thread_id"] == thread_id
        resumed = await request(config.control_socket, "thread", {"target": "main"})
        assert resumed["thread_id"] == thread_id
        remember_mcp()
        replay = await cli(
            "ask", "Wait for the isolated Codex crash.", "--request-id", "native-crashed-intent"
        )
        assert replay["thread_id"] == thread_id and replay["turn_id"] == crashed["turn_id"]
        assert replay["status"] in {"accepted", "unknown", "failed"}
        release_crash.set()
        await asyncio.wait_for(crash_closed.wait(), 5)
        history = await cli("task-status", "--target", "main")
        assert "Late crash result." not in json.dumps(history)
        assert len(model_requests) == 5 and not errors
        assert (await cli("stop", timeout=25))["stopped"]
        await asyncio.wait_for(service.wait(), 10)
        assert service.returncode == 0
        assert not process_is_alive(after_crash["codex_pid"])
        await until(mcp_exited)

        # Required MCP initialization fails at native thread start/resume. The
        # real CLI must expose that failure and preserve existing fixture data.
        document = tomlkit.parse(config_file.read_text())
        document["mcp_servers"]["alice"]["command"] = "/usr/bin/false"
        document["mcp_servers"]["alice"]["args"] = []
        config_file.write_text(tomlkit.dumps(document))
        broken_config = config_file.read_bytes()
        await launch()
        preserved_jobs = (await cli("cron", "list"))["jobs"]
        failure = await cli(
            "ask", "Required MCP must fail.", "--target", "mcp-failure", expected_code=1
        )
        assert "required MCP servers failed to initialize: alice" in failure
        assert config_file.read_bytes() == broken_config
        assert (await cli("cron", "list"))["jobs"] == preserved_jobs
        assert len(model_requests) == 5 and not errors
        assert (await cli("stop", timeout=25))["stopped"]
        await asyncio.wait_for(service.wait(), 10)
        assert service.returncode == 0
    finally:
        release_held.set()
        release_crash.set()
        if rpc is not None:
            await rpc.close()
        if service is not None and service.returncode is None:
            service.terminate()
            try:
                await asyncio.wait_for(service.wait(), 15)
            except TimeoutError:
                os.killpg(service.pid, signal.SIGKILL)
                await service.wait()
        for pid, expected in owned_codex_pids.items():
            if expected and process_is_alive(pid) and process_identity(pid) == expected:
                # These PIDs came from this fixture's private authenticated-by-path control socket.
                with suppress(ProcessLookupError):
                    os.killpg(pid, signal.SIGKILL)
        for pid, expected in owned_mcp.items():
            if expected and process_is_alive(pid) and process_identity(pid) == expected:
                with suppress(ProcessLookupError):
                    os.kill(pid, signal.SIGKILL)
        http.close()
        await http.wait_closed()
        for peer in tuple(peers):
            peer.close()
        for task in tuple(handlers):
            task.cancel()
        await asyncio.gather(*tuple(handlers), return_exceptions=True)
