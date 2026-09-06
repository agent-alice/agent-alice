"""TaskPolicy through the installed CLI and a real, isolated Codex App Server.

Responses come from a fixture-owned localhost endpoint. These are native
protocol/lifecycle checks, not paid-model or independent business-success evidence.
"""

import asyncio
from contextlib import suppress
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import textwrap
import time

import pytest
import tomlkit

from alice_codex.config import load_config
from alice_codex.control import request
from alice_codex.rpc import RpcClient

pytestmark = pytest.mark.native


def process_identity(pid):
    result = subprocess.run(
        ["ps", "-ww", "-p", str(pid), "-o", "lstart=", "-o", "command="],
        capture_output=True,
        text=True,
        timeout=5,
    )
    return result.stdout.strip()


async def until(operation, *, timeout=15):
    async with asyncio.timeout(timeout):
        while True:
            result = await operation()
            if result:
                return result
            await asyncio.sleep(0.03)


def settled_unchanged_heartbeat(snapshot, *, observed_checks, minimum_checks):
    """Accept the unchanged heartbeat only once its dispatch is pending again."""
    if not snapshot or observed_checks < minimum_checks:
        return None
    evidence = snapshot["heartbeat"]
    if not evidence or evidence["comparison"]["state"] != "unchanged":
        return None
    assert evidence["latest"]["state"] == "known"
    assert evidence["waiting_until"] > evidence["latest"]["observed_at"]
    # A host observation can advance while Scheduler.poll is between claim,
    # send and DeferredDispatch. Preserve the pending assertion by waiting for
    # that transition to finish, rather than accepting its in-flight snapshot.
    states = {item["status"] for item in snapshot["events"]}
    if "pending" not in states or states & {"claimed", "sending"}:
        return None
    return snapshot


class NativePolicyRuntime:
    def __init__(self, root):
        self.root = root.resolve()
        self.home = self.root / "alice"
        self.process_home = self.root / "process-home"
        self.process_home.mkdir()
        self.runtime_python = os.environ.get("ALICE_ARTIFACT_PYTHON", sys.executable)
        self.binary = Path(
            os.environ.get(
                "ALICE_TEST_CODEX_BINARY", "/Applications/ChatGPT.app/Contents/Resources/codex"
            )
        )
        self.env = {
            "PATH": str(Path(self.runtime_python).parent)
            + os.pathsep
            + os.environ.get("PATH", "/usr/bin:/bin"),
            "HOME": str(self.process_home),
            "LANG": "en_US.UTF-8",
            "RUST_LOG": "error",
            "PYTHONDONTWRITEBYTECODE": "1",
            "NO_PROXY": "127.0.0.1,localhost,::1",
            "no_proxy": "127.0.0.1,localhost,::1",
        }
        self.base = [self.runtime_python, "-I", "-m", "alice_codex", "--home", str(self.home)]
        self.http = self.service = self.rpc = self.config = None
        self.requests = []
        self.errors = []
        self.peers, self.handlers = set(), set()
        self.held, self.release, self.response_closed = (
            asyncio.Event(),
            asyncio.Event(),
            asyncio.Event(),
        )
        self.response_mode = "held"
        self.max_requests = 1
        self.host_command = None
        self.owned_servers, self.owned_mcp = {}, {}

    async def model(self, reader, writer):
        self.peers.add(writer)
        task = asyncio.current_task()
        self.handlers.add(task)
        try:
            header = await reader.readuntil(b"\r\n\r\n")
            lines = header.decode().split("\r\n")
            headers = {
                line.split(":", 1)[0].lower(): line.split(":", 1)[1].strip()
                for line in lines[1:]
                if ":" in line
            }
            assert lines[0] == "POST /responses HTTP/1.1"
            assert headers["host"].startswith("127.0.0.1:")
            assert "authorization" not in headers and "content-encoding" not in headers
            body = json.loads(await reader.readexactly(int(headers["content-length"])))
            self.requests.append(body)
            assert len(self.requests) <= self.max_requests, "unexpected additional model attempt"
            response_id = f"policy-fixture-{len(self.requests)}"
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n"
            )
            writer.write(
                (
                    "data: "
                    + json.dumps(
                        {
                            "type": "response.created",
                            "response": {"id": response_id},
                        }
                    )
                    + "\n\n"
                ).encode()
            )
            await writer.drain()
            if self.response_mode == "held":
                self.held.set()
                await self.release.wait()
            item = {
                "type": "message",
                "role": "assistant",
                "id": response_id + "-output",
                "content": [
                    {
                        "type": "output_text",
                        "text": "Recorded executor output without business receipt.",
                    }
                ],
            }
            for event in (
                {"type": "response.output_item.done", "item": item},
                {
                    "type": "response.completed",
                    "response": {
                        "id": response_id,
                        "usage": {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
                    },
                },
            ):
                writer.write(("data: " + json.dumps(event) + "\n\n").encode())
            await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        except Exception as error:
            self.errors.append(f"{type(error).__name__}: {error}")
        finally:
            writer.close()
            with suppress(ConnectionError):
                await writer.wait_closed()
            self.peers.discard(writer)
            self.handlers.discard(task)
            self.response_closed.set()

    async def cli(self, *arguments, expected_code=0, timeout=30):
        process = await asyncio.create_subprocess_exec(
            *self.base,
            *arguments,
            cwd=self.root,
            env=self.env,
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
            f"CLI {arguments}: {stderr.decode()}\n{stdout.decode()}; fixture_errors={self.errors}"
        )
        return json.loads(stdout) if expected_code == 0 else stderr.decode()

    async def configure(self):
        assert self.binary.is_file(), "Required real Codex binary is unavailable"
        version = subprocess.run(
            [str(self.binary), "--version"], capture_output=True, text=True, check=True, timeout=10
        ).stdout.strip()
        assert version == "codex-cli 0.153.4", "native protocol evidence is pinned to this version"
        self.http = await asyncio.start_server(self.model, "127.0.0.1", 0)
        port = self.http.sockets[0].getsockname()[1]
        await self.cli("init", "--codex", str(self.binary), "--no-pin", "--model", "gpt-5.6-terra")
        self.config = load_config(self.home)
        path = self.config.codex_home / "config.toml"
        document = tomlkit.parse(path.read_text())
        document["model_provider"] = "recorded"
        document["check_for_update_on_startup"] = False
        document["web_search"] = "disabled"
        document["features"].update(apps=False, plugins=False, enable_request_compression=False)
        document["model_providers"] = {
            "recorded": {
                "name": "TaskPolicy recorded localhost fixture",
                "base_url": f"http://127.0.0.1:{port}",
                "wire_api": "responses",
                "supports_websockets": False,
                "requires_openai_auth": False,
                "request_max_retries": 0,
                "stream_max_retries": 0,
            }
        }
        path.write_text(tomlkit.dumps(document))
        assert not (self.config.codex_home / "auth.json").exists()
        assert not any("API_KEY" in name or "TOKEN" in name for name in self.env)
        await self.launch()

    async def launch(self):
        self.service = await asyncio.create_subprocess_exec(
            *(self.host_command or [*self.base, "serve"]),
            cwd=self.root,
            env=self.env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )

        async def ready():
            if self.service.returncode is not None:
                pytest.fail(f"Native service exited: {(await self.service.stderr.read()).decode()}")
            try:
                status = await self.status()
                return status if status["ready"] else None
            except (OSError, TimeoutError):
                return None

        status = await until(ready, timeout=35)
        pid = status["codex_pid"]
        self.owned_servers[pid] = process_identity(pid)
        self.rpc = await RpcClient.connect_unix(self.config.codex_socket)
        await self.rpc.initialize()
        return status

    async def status(self):
        return await request(self.config.control_socket, "status", timeout=1)

    async def policy(self, target):
        return await self.cli("task-policy", "status", "--target", target)

    async def set_policy(self, target, request_id, *, seconds, attempts, retries, expected_code=0):
        return await self.cli(
            "task-policy",
            "set",
            "--target",
            target,
            "--request-id",
            request_id,
            "--max-elapsed-seconds",
            str(seconds),
            "--max-attempts",
            str(attempts),
            "--max-retries",
            str(retries),
            "--retry-wait-seconds",
            "1",
            "--unchanged-wait-seconds",
            "1",
            expected_code=expected_code,
        )

    def remember_mcp(self):
        rows = subprocess.run(
            ["ps", "-ww", "-axo", "pid=,command="],
            capture_output=True,
            text=True,
            check=True,
            timeout=5,
        ).stdout.splitlines()
        matched = [
            int(row.split(None, 1)[0])
            for row in rows
            if f"alice_codex.mcp --home {self.home}" in row
        ]
        assert matched, "the real native runtime must initialize its required Alice MCP"
        for pid in matched:
            self.owned_mcp[pid] = process_identity(pid)

    async def stop(self):
        if self.rpc:
            await self.rpc.close()
            self.rpc = None
        assert (await self.cli("stop", timeout=25))["stopped"]
        await asyncio.wait_for(self.service.wait(), 10)
        assert self.service.returncode == 0

    async def close(self):
        self.release.set()
        if self.rpc:
            await self.rpc.close()
        if self.service and self.service.returncode is None:
            self.service.terminate()
            try:
                await asyncio.wait_for(self.service.wait(), 15)
            except TimeoutError:
                os.killpg(self.service.pid, signal.SIGKILL)
                await self.service.wait()
        for pid, identity in self.owned_servers.items():
            if identity and process_identity(pid) == identity:
                with suppress(ProcessLookupError):
                    os.killpg(pid, signal.SIGKILL)
        for pid, identity in self.owned_mcp.items():
            if identity and process_identity(pid) == identity:
                with suppress(ProcessLookupError):
                    os.kill(pid, signal.SIGKILL)
        if self.http:
            self.http.close()
            await self.http.wait_closed()
        for peer in tuple(self.peers):
            peer.close()
        for task in tuple(self.handlers):
            task.cancel()
        await asyncio.gather(*tuple(self.handlers), return_exceptions=True)


@pytest.fixture
async def native_policy(tmp_path):
    runtime = NativePolicyRuntime(tmp_path)
    try:
        await runtime.configure()
        yield runtime
    finally:
        await runtime.close()


async def test_native_deadline_interrupts_and_persists_usage_across_restart(native_policy):
    runtime = native_policy
    target, request_id = "research", "research-attempt-1"
    prompt = "Remain active until the explicit task time limit interrupts this fixture."
    await runtime.set_policy(target, "research-policy-1", seconds=2, attempts=1, retries=0)
    initial = await runtime.policy(target)
    assert initial["policy"] == {
        "max_elapsed_seconds": 2,
        "max_attempts": 1,
        "max_retries": 0,
        "retry_wait_seconds": 1,
        "unchanged_wait_seconds": 1,
    }
    accepted = await runtime.cli("ask", prompt, "--target", target, "--request-id", request_id)
    await asyncio.wait_for(runtime.held.wait(), 10)
    runtime.remember_mcp()
    thread_id, turn_id = accepted["thread_id"], accepted["turn_id"]
    # No pause/interrupt request is issued: the background deadline must stop
    # the actual native turn, and its acknowledgement alone is insufficient.

    async def native_interrupted():
        native = await runtime.rpc.request(
            "thread/read", {"threadId": thread_id, "includeTurns": True}
        )
        turns = native["thread"]["turns"]
        turn = next((item for item in turns if item["id"] == turn_id), None)
        return turn if turn and turn["status"] == "interrupted" else None

    completed = await until(native_interrupted)
    completed_at = time.time()
    assert completed["status"] == "interrupted"

    async def durably_paused():
        status = await runtime.status()
        intents = (await request(runtime.config.control_socket, "intents"))["intents"]
        receipt = next(item for item in intents if item["id"] == request_id)
        return (
            status
            if status["tasks"][target]["paused"] and receipt.get("outcome") == "interrupted"
            else None
        )

    assert (await until(durably_paused))["ready"]
    native = await runtime.rpc.request("thread/read", {"threadId": thread_id})
    assert native["thread"]["status"]["type"] in {"idle", "notLoaded"}
    limited = await runtime.policy(target)
    assert limited["usage"]["attempts"] == 1
    assert not limited["decision"]["allowed"]
    started_at = limited["usage"]["started_at"]
    assert completed_at >= started_at + 2, "the final admitted attempt must run until its deadline"
    runtime.release.set()
    await asyncio.wait_for(runtime.response_closed.wait(), 5)
    assert len(runtime.requests) == 1 and not runtime.errors
    repeated = await runtime.cli("ask", prompt, "--target", target, "--request-id", request_id)
    assert repeated["thread_id"] == thread_id and repeated["turn_id"] == turn_id
    await runtime.stop()
    restarted = await runtime.launch()
    assert restarted["tasks"][target]["thread_id"] == thread_id
    assert restarted["tasks"][target]["paused"]
    restored = await runtime.policy(target)
    assert restored["usage"]["attempts"] == 1
    assert restored["usage"]["started_at"] == started_at
    assert not restored["decision"]["allowed"]
    assert (
        await runtime.cli("ask", prompt, "--target", target, "--request-id", request_id) == repeated
    )
    await runtime.cli(
        "ask",
        "A different request ID cannot extend the task.",
        "--target",
        target,
        "--request-id",
        "research-attempt-2",
        expected_code=1,
    )
    # Global resume restores ordinary maintenance without reactivating this
    # exhausted bounded root. Exercise the installed CLI and real native main.
    main = await request(runtime.config.control_socket, "thread", {"target": "main"})
    await runtime.cli("pause", "--target", "main")
    resumed = await runtime.cli("resume")
    assert resumed["resumed"] == "autonomy" and target in resumed["blocked_tasks"]
    status = await runtime.status()
    assert status["tasks"]["main"]["thread_id"] == main["thread_id"]
    assert not status["autonomy_paused"] and not status["tasks"]["main"]["paused"]
    assert status["tasks"][target]["paused"]
    await runtime.cli("resume", "--target", target, expected_code=1)
    # A changed policy must have a new operation ID. Neither a rejected reuse
    # nor a successful extension resets measured attempts or human pause.
    await runtime.set_policy(
        target, "research-policy-1", seconds=60, attempts=3, retries=1, expected_code=1
    )
    assert (await runtime.policy(target))["policy"]["max_elapsed_seconds"] == 2
    await runtime.set_policy(target, "research-policy-2", seconds=60, attempts=3, retries=1)
    extended = await runtime.policy(target)
    assert extended["policy"]["max_elapsed_seconds"] == 60
    assert extended["usage"]["attempts"] == 1
    assert extended["usage"]["started_at"] == started_at
    assert (await runtime.status())["tasks"][target]["paused"]
    await runtime.cli(
        "ask",
        "Policy extension does not resume the task.",
        "--target",
        target,
        "--request-id",
        "research-attempt-3",
        expected_code=1,
    )
    assert len(runtime.requests) == 1 and not runtime.errors
    await runtime.stop()


async def test_native_completion_without_business_receipt_requires_reconciliation(native_policy):
    runtime = native_policy
    runtime.response_mode = "complete"
    target = "unverified"
    await runtime.set_policy(target, "unverified-policy-1", seconds=60, attempts=3, retries=2)
    done = await runtime.cli(
        "ask",
        "Produce the recorded executor output.",
        "--target",
        target,
        "--request-id",
        "unverified-attempt-1",
        "--wait",
        "10",
    )
    runtime.remember_mcp()
    assert done["intent"]["status"] == "completed"
    assert "Recorded executor output without business receipt." in json.dumps(done["thread"])

    async def unknown_outcome():
        status = await runtime.policy(target)
        return status if status["usage"] and status["usage"]["last_outcome"] == "unknown" else None

    unverified = await until(unknown_outcome)
    assert unverified["usage"]["attempts"] == 1
    assert unverified["decision"]["state"] == "reconciliation_required"
    assert not unverified["decision"]["allowed"]
    await runtime.cli(
        "ask",
        "Do not retry without independent evidence.",
        "--target",
        target,
        "--request-id",
        "unverified-attempt-2",
        expected_code=1,
    )
    assert (await runtime.policy(target))["usage"]["attempts"] == 1
    assert len(runtime.requests) == 1 and not runtime.errors
    # A completed native turn can retain an unknown business result across
    # restart. Once a deadline stop is proven it must not monopolize capacity.
    await runtime.stop()
    config_path = runtime.home / "config.json"
    isolated_config = json.loads(config_path.read_text())
    isolated_config["max_active_tasks"] = 1
    config_path.write_text(json.dumps(isolated_config))
    runtime.max_requests = 2
    await runtime.launch()
    await runtime.set_policy(target, "unverified-deadline", seconds=0.01, attempts=3, retries=2)

    async def stopped():
        status = await runtime.status()
        return status if status["tasks"][target].get("policy_deadline_stopped") else None

    await until(stopped)
    await request(runtime.config.control_socket, "thread", {"target": "main"})
    resumed = await runtime.cli("resume")
    assert target in resumed["blocked_tasks"]
    main = await request(
        runtime.config.control_socket,
        "ask",
        {
            "target": "main",
            "text": "Recorded ordinary periodic maintenance.",
            "request_id": "main-after-confirmed-stop",
            "automatic": True,
        },
    )
    assert main["status"] == "accepted"

    async def main_completed():
        intents = (await request(runtime.config.control_socket, "intents"))["intents"]
        return next(
            (
                item
                for item in intents
                if item["id"] == "main-after-confirmed-stop" and item["status"] == "completed"
            ),
            None,
        )

    await until(main_completed)
    still_unknown = await runtime.policy(target)
    assert still_unknown["usage"]["last_outcome"] == "unknown"
    assert still_unknown["usage"]["attempts"] == 1
    assert (await runtime.status())["tasks"][target]["paused"]
    await runtime.cli(
        "ask",
        "Still cannot replay the unknown work.",
        "--target",
        target,
        "--request-id",
        "unverified-attempt-3",
        expected_code=1,
    )
    assert len(runtime.requests) == 2 and not runtime.errors
    await runtime.stop()


async def test_native_registered_heartbeat_waits_for_change_and_preserves_pause(tmp_path):
    runtime = NativePolicyRuntime(tmp_path)
    runtime.response_mode, runtime.max_requests = "complete", 2
    target = "evidence-monitor"
    source = {"votes": 1}
    observations, source_errors = [], []
    source_peers, source_handlers = set(), set()

    async def collection(reader, writer):
        source_peers.add(writer)
        handler = asyncio.current_task()
        source_handlers.add(handler)
        try:
            header = (await reader.readuntil(b"\r\n\r\n")).decode()
            assert header.split("\r\n", 1)[0] == "GET /answers HTTP/1.1"
            assert "authorization:" not in header.lower()
            votes = source["votes"]
            observations.append(votes)
            body = json.dumps(
                {
                    "data": [{"id": "fixture-answer", "voteup_count": votes}],
                    "paging": {"is_end": True, "next": None},
                }
            ).encode()
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                + f"Content-Length: {len(body)}\r\n".encode()
                + b"Cache-Control: no-cache\r\nConnection: close\r\n\r\n"
                + body
            )
            await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        except Exception as error:
            source_errors.append(f"{type(error).__name__}: {error}")
        finally:
            writer.close()
            with suppress(ConnectionError):
                await writer.wait_closed()
            source_peers.discard(writer)
            source_handlers.discard(handler)

    endpoint = await asyncio.start_server(collection, "127.0.0.1", 0)
    url = f"http://127.0.0.1:{endpoint.sockets[0].getsockname()[1]}/answers"
    report_path = runtime.root / "host-heartbeat-report.json"
    host_script = runtime.root / "registered-heartbeat-host.py"
    # This fixture host imports the installed package under -I. Source
    # registration and read-only reports do not add a product control or MCP API.
    host_script.write_text(
        textwrap.dedent("""\
        import asyncio
        from contextlib import suppress
        from dataclasses import asdict
        import json
        from pathlib import Path
        import sys
        from alice_codex.config import load_config
        from alice_codex.heartbeat import CollectionSpec
        from alice_codex.service import Service

        async def main():
            config = load_config(Path(sys.argv[1]))
            assert config.task_policy is None
            config.poll_seconds = 0.05
            target = "evidence-monitor"
            service = Service(config)
            service.register_heartbeat_source(target, CollectionSpec(
                source_id="fixture-answers", url=sys.argv[2],
                subject="fixture-member", collection="answers",
                auth_context_version="fixture-no-auth-v1", max_age_seconds=60,
                required_metrics=("voteup_count",),
                request_timeout=2, total_seconds=3,
            ), wait_seconds=0.15)
            report = Path(sys.argv[3])

            async def report_state():
                while True:
                    snapshot = {
                        "heartbeat": service.store.get_heartbeat_state(target),
                        "policy": service.store.get_task_policy(target),
                        "task": service.state["tasks"].get(target),
                        "consumed": service.state.get("heartbeat_consumed", {}).get(target),
                        "intents": [item for item in service.state["intents"].values()
                                    if item.get("target") == target],
                        "events": [asdict(item) for item in service.store.list_events()
                                   if item.target == target],
                    }
                    temporary = report.with_suffix(".pending")
                    temporary.write_text(json.dumps(snapshot))
                    temporary.replace(report)
                    await asyncio.sleep(0.03)

            reporter = asyncio.create_task(report_state())
            try:
                await service.run()
            finally:
                reporter.cancel()
                with suppress(asyncio.CancelledError):
                    await reporter

        asyncio.run(main())
    """)
    )
    runtime.host_command = [
        runtime.runtime_python,
        "-I",
        str(host_script),
        str(runtime.home),
        url,
        str(report_path),
    ]

    async def report():
        if not report_path.exists():
            return None
        return json.loads(report_path.read_text())

    async def completed(count):
        snapshot = await report()
        if (
            snapshot
            and len(snapshot["intents"]) == count
            and all(item["status"] == "completed" for item in snapshot["intents"])
        ):
            assert snapshot["policy"] is None
            return snapshot
        return None

    async def unchanged_after(minimum_checks):
        return settled_unchanged_heartbeat(
            await report(), observed_checks=len(observations), minimum_checks=minimum_checks
        )

    try:
        await runtime.configure()
        await runtime.cli("resume")
        job = await runtime.cli(
            "cron",
            "create",
            "--name",
            "recorded host heartbeat",
            "--every",
            "0.2",
            "--target",
            target,
            "--heartbeat",
            "--prompt",
            "Review the registered host evidence.",
        )
        assert job["kind"] == "heartbeat" and job["target"] == target
        first = await until(lambda: completed(1))
        runtime.remember_mcp()
        thread_id = first["task"]["thread_id"]
        assert first["consumed"]["state"] == "known"
        assert len(runtime.requests) == 1
        checks = len(observations)
        stable = await until(lambda: unchanged_after(checks + 3))
        assert stable["consumed"] == first["consumed"]
        assert len(stable["intents"]) == 1 and len(runtime.requests) == 1
        assert any(item["status"] == "pending" for item in stable["events"])

        source["votes"] = 2
        second = await until(lambda: completed(2))
        assert second["task"]["thread_id"] == thread_id
        assert second["consumed"]["content_sha256"] != first["consumed"]["content_sha256"]
        assert len({item["id"] for item in second["intents"]}) == 2
        assert len({item["turn_id"] for item in second["intents"]}) == 2
        native = await runtime.rpc.request(
            "thread/read", {"threadId": thread_id, "includeTurns": True}
        )
        assert {turn["id"] for turn in native["thread"]["turns"]} == {
            item["turn_id"] for item in second["intents"]
        }
        assert all(turn["status"] == "completed" for turn in native["thread"]["turns"])
        checks = len(observations)
        stable = await until(lambda: unchanged_after(checks + 3))
        assert stable["consumed"] == second["consumed"]
        assert len(stable["intents"]) == 2 and len(runtime.requests) == 2

        await runtime.cli("pause", "--target", target)
        status = await runtime.status()
        assert not status["autonomy_paused"] and status["tasks"][target]["paused"]
        source["votes"] = 3
        checks = len(observations)
        paused = await until(lambda: unchanged_after(checks + 3))
        assert paused["task"]["paused"]
        assert (
            paused["heartbeat"]["latest"]["content_sha256"] != second["consumed"]["content_sha256"]
        )
        assert paused["consumed"] == second["consumed"]
        assert len(paused["intents"]) == 2 and len(runtime.requests) == 2
        await runtime.stop()
        restarted = await runtime.launch()
        assert restarted["tasks"][target]["paused"]
        assert restarted["tasks"][target]["thread_id"] == thread_id
        checks = len(observations)
        persisted = await until(lambda: unchanged_after(checks + 3))
        assert persisted["task"]["paused"] and persisted["consumed"] == second["consumed"]
        assert len(persisted["intents"]) == 2 and len(runtime.requests) == 2
        assert persisted["policy"] is None
        assert not runtime.errors and not source_errors
        await runtime.stop()
    finally:
        await runtime.close()
        endpoint.close()
        await endpoint.wait_closed()
        for peer in tuple(source_peers):
            peer.close()
        for handler in tuple(source_handlers):
            handler.cancel()
        await asyncio.gather(*tuple(source_handlers), return_exceptions=True)
