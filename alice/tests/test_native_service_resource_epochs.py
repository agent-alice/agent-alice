"""Fixed Codex/host pair, real Service restarts, and localhost usage receipts.

Every case uses four recorded Responses requests and two native process epochs.
The resumed epoch inherits the prior cumulative baseline: its high-water sum is
an observation with overlapping history, not actual usage or a monetary bill.
The provider accepts only a synthetic Bearer credential. No real model or account
is contacted. External observers repeat subscriptions; synthetic Service tests
separately inject duplicate delivery to its accounting listener.
"""

import asyncio
from contextlib import suppress
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import textwrap
from uuid import UUID

import pytest
import tomlkit

from alice_codex.config import load_config
from alice_codex.control import request
from alice_codex.resources import ResourceLedger
from alice_codex.rpc import RpcClient
from test_native_service import process_is_alive
from test_native_task_policy import NativePolicyRuntime, process_identity, until

pytestmark = [pytest.mark.native, pytest.mark.native_resource_epoch]

SYNTHETIC_CREDENTIAL = "alice-localhost-resource-epoch-fixture-not-an-account-key"
RESPONSE_TOTALS = (80, 90, 30, 40)


def file_hash(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def required_environment(name):
    value = os.environ.get(name)
    assert value, f"Native resource epoch verification requires explicit {name}"
    return value


class NativeResourceRuntime(NativePolicyRuntime):
    """Reuse owned lifecycle cleanup, while replacing provider and preflight."""

    def __init__(self, root):
        for name in (
            "ALICE_ARTIFACT_PYTHON",
            "ALICE_TEST_CODEX_BINARY",
            "ALICE_TEST_CODEX_SHA256",
            "ALICE_TEST_CODEX_HOST_BINARY",
            "ALICE_TEST_CODEX_HOST_SHA256",
        ):
            required_environment(name)
        super().__init__(root)
        self.binary = Path(required_environment("ALICE_TEST_CODEX_BINARY")).resolve()
        self.host_binary = Path(required_environment("ALICE_TEST_CODEX_HOST_BINARY")).resolve()
        self.expected_hashes = {
            "codex": required_environment("ALICE_TEST_CODEX_SHA256"),
            "codex-code-mode-host": required_environment("ALICE_TEST_CODEX_HOST_SHA256"),
        }
        self.env["PATH"] = os.pathsep.join(
            [str(Path(self.runtime_python).parent), str(self.binary.parent), "/usr/bin", "/bin"]
        )
        self.env["ALICE_RECORDED_CREDENTIAL"] = SYNTHETIC_CREDENTIAL
        self.extra_rpcs = []
        self.native_events = []
        self.max_requests = len(RESPONSE_TOTALS)

    def verify_pair(self):
        assert self.binary.parent == self.host_binary.parent, "The fixed pair must share its layout"
        assert self.host_binary.name == "codex-code-mode-host", "Preserve the actual host basename"
        for key, path in (("codex", self.binary), ("codex-code-mode-host", self.host_binary)):
            assert path.is_file() and os.access(path, os.X_OK), f"Missing executable {key}"
            expected = self.expected_hashes[key]
            assert len(expected) == 64 and all(c in "0123456789abcdef" for c in expected)
            assert file_hash(path) == expected, f"Fixed {key} hash changed"

    def verify_installed_source(self):
        # -I deliberately ignores PYTHONPATH. Check every installed package .py,
        # so an interpreter pointing to a different editable tree fails preflight.
        probe = textwrap.dedent("""
            import hashlib, importlib.util, json, pathlib, sys
            import alice_codex.service as service
            package = pathlib.Path(importlib.util.find_spec('alice_codex').origin).parent
            print(json.dumps({
                'isolated_environment': sys.prefix != sys.base_prefix,
                'resource_epoch_capability': getattr(service, 'RESOURCE_EPOCH_CAPABILITY', 0),
                'files': {
                    str(path.relative_to(package)): hashlib.sha256(path.read_bytes()).hexdigest()
                    for path in sorted(package.rglob('*.py'))
                },
            }))
        """)
        result = subprocess.run(
            [self.runtime_python, "-I", "-c", probe],
            cwd=self.root,
            env=self.env,
            capture_output=True,
            text=True,
            check=True,
            timeout=15,
        )
        installed = json.loads(result.stdout)
        assert installed["isolated_environment"], "Candidate requires an independent interpreter"
        capability = installed["resource_epoch_capability"]
        assert type(capability) is int and capability >= 1, (
            "Installed Service lacks resource epochs"
        )
        package = Path(__file__).resolve().parents[1] / "src" / "alice_codex"
        expected = {
            str(path.relative_to(package)): file_hash(path)
            for path in sorted(package.rglob("*.py"))
        }
        assert installed["files"] == expected, (
            "Installed candidate differs from the selected source"
        )

    async def configure(self):
        self.verify_pair()
        self.verify_installed_source()
        version = subprocess.run(
            [str(self.binary), "--version"],
            cwd=self.root,
            env=self.env,
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
        ).stdout.strip()
        assert version == "codex-cli 0.153.4"
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
                "name": "Synthetic resource epoch localhost fixture",
                "base_url": f"http://127.0.0.1:{port}",
                "wire_api": "responses",
                "supports_websockets": False,
                "requires_openai_auth": False,
                "env_key": "ALICE_RECORDED_CREDENTIAL",
                "request_max_retries": 0,
                "stream_max_retries": 0,
            }
        }
        path.write_text(tomlkit.dumps(document))
        assert not (self.config.codex_home / "auth.json").exists()
        assert not any(name.startswith("OPENAI_") for name in self.env)
        await self.launch()  # Unmodified installed `alice_codex ... serve` entry point.

    async def model(self, reader, writer):
        self.peers.add(writer)
        task = asyncio.current_task()
        self.handlers.add(task)
        try:
            header = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 10)
            lines = header.decode().split("\r\n")
            headers = {
                line.split(":", 1)[0].lower(): line.split(":", 1)[1].strip()
                for line in lines[1:]
                if ":" in line
            }
            assert lines[0] == "POST /responses HTTP/1.1"
            assert headers["host"].startswith("127.0.0.1:")
            assert headers.get("authorization") == f"Bearer {SYNTHETIC_CREDENTIAL}"
            assert "content-encoding" not in headers
            size = int(headers["content-length"])
            assert 0 < size <= 2 * 1024 * 1024, "Bound fixture request body"
            body = json.loads(await asyncio.wait_for(reader.readexactly(size), 10))
            self.requests.append(body)
            assert len(self.requests) <= self.max_requests, "Unexpected extra provider request"
            sequence = len(self.requests)
            total = RESPONSE_TOTALS[sequence - 1]
            response_id = f"resource-epoch-fixture-{sequence}"
            item = {
                "type": "message",
                "role": "assistant",
                "id": f"{response_id}-output",
                "content": [{"type": "output_text", "text": f"Synthetic receipt {sequence}."}],
            }
            events = (
                {"type": "response.created", "response": {"id": response_id}},
                {"type": "response.output_item.done", "item": item},
                {
                    "type": "response.completed",
                    "response": {
                        "id": response_id,
                        "usage": {
                            "input_tokens": total - 10,
                            "input_tokens_details": {"cached_tokens": 2},
                            "output_tokens": 10,
                            "output_tokens_details": {"reasoning_tokens": 3},
                            "total_tokens": total,
                        },
                    },
                },
            )
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n"
            )
            for event in events:
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

    def epoch_state(self):
        state = json.loads((self.home / "state/runtime.json").read_text())
        server = state["server"]
        epoch = server["resource_epoch_id"]
        assert str(UUID(epoch)) == epoch
        record = state["resource_epochs"][epoch]
        assert record["state"] == "bound"
        assert record["server"] == {key: server[key] for key in ("pid", "birth", "identity")}
        return epoch, state

    async def observe(self, thread_id):
        other = await RpcClient.connect_unix(self.config.codex_socket)
        self.extra_rpcs.append(other)
        await other.initialize()
        observations = [[], []]
        for client, events in zip((self.rpc, other), observations, strict=True):

            def capture(event, destination=events):
                if (
                    event.get("method") == "thread/tokenUsage/updated"
                    and event.get("params", {}).get("threadId") == thread_id
                ):
                    destination.append(json.loads(json.dumps(event["params"])))

            client.add_listener(capture)
            for _ in range(2):
                await client.request("thread/resume", {"threadId": thread_id})
        self.native_events.append(observations)
        return observations

    async def close_observers(self):
        for rpc in self.extra_rpcs:
            await rpc.close()
        self.extra_rpcs.clear()
        if self.rpc:
            await self.rpc.close()
            self.rpc = None

    async def resources(self):
        return await request(self.config.control_socket, "resources_status")

    async def turn(self, thread_id, epoch, number, expected_total):
        accepted = await self.cli(
            "ask",
            f"Return synthetic resource receipt {number}.",
            "--request-id",
            f"epoch-turn-{number}",
        )
        assert accepted["thread_id"] == thread_id

        latest_turns, latest_resources = [], None

        def diagnostics():
            return json.dumps(
                {
                    "turn_id": accepted["turn_id"],
                    "expected_high_water": expected_total,
                    "native_turns": latest_turns,
                    "epoch_record": (
                        latest_resources["tokens"]["epochs"].get(epoch, {}).get(thread_id)
                        if latest_resources
                        else None
                    ),
                    "observer_totals": [
                        [
                            [
                                ((event.get("tokenUsage") or {}).get("total") or {}).get(
                                    "totalTokens"
                                )
                                for event in events
                            ]
                            for events in observers
                        ]
                        for observers in self.native_events
                    ],
                    "fixture_requests": len(self.requests),
                    "fixture_errors": self.errors,
                },
                sort_keys=True,
            )

        async def completed():
            nonlocal latest_turns
            native = await self.rpc.request(
                "thread/read", {"threadId": thread_id, "includeTurns": True}
            )
            latest_turns = [
                {"id": turn["id"], "status": turn["status"]} for turn in native["thread"]["turns"]
            ]
            matching = [turn for turn in latest_turns if turn["id"] == accepted["turn_id"]]
            return matching if matching and matching[0]["status"] == "completed" else None

        try:
            await until(completed, timeout=20)
        except TimeoutError:
            pytest.fail(f"Native turn completion timed out: {diagnostics()}")

        async def accounted():
            nonlocal latest_resources
            resources = latest_resources = await self.resources()
            record = resources["tokens"]["epochs"].get(epoch, {}).get(thread_id, {})
            high = record.get("high_water")
            return resources if high and high["totalTokens"] == expected_total else None

        try:
            result = await until(accounted, timeout=15)
        except TimeoutError:
            pytest.fail(f"Native token accounting timed out: {diagnostics()}")
        self.remember_mcp()
        assert not self.errors
        return result

    async def close(self):
        try:
            await self.close_observers()
        finally:
            await super().close()


@pytest.mark.parametrize("restart", ["ordinary", "service_sigkill", "codex_sigkill"])
async def test_native_service_keeps_process_scoped_usage_across_restart(
    tmp_path, restart, record_property
):
    runtime = NativeResourceRuntime(tmp_path)
    try:
        await runtime.configure()
        started = await runtime.status()
        first_epoch, first_state = runtime.epoch_state()
        assert first_state["server"]["pid"] == started["codex_pid"]
        task = await request(runtime.config.control_socket, "thread", {"target": "main"})
        thread_id = task["thread_id"]
        legacy = {
            "threadId": thread_id,
            "turnId": "legacy-synthetic-history",
            "tokenUsage": {
                "total": {
                    "inputTokens": 989,
                    "cachedInputTokens": 2,
                    "outputTokens": 10,
                    "reasoningOutputTokens": 3,
                    "totalTokens": 999,
                }
            },
        }
        ResourceLedger(runtime.home / "state/resources.sqlite3").record_token_usage(
            legacy, event_id="legacy-synthetic-history"
        )
        first_observers = await runtime.observe(thread_id)
        await runtime.turn(thread_id, first_epoch, 1, 80)
        first_resources = await runtime.turn(thread_id, first_epoch, 2, 170)
        assert len(runtime.requests) == 2
        first_record = first_resources["tokens"]["epochs"][first_epoch][thread_id]
        assert first_record["first_observed"]["totalTokens"] == 80
        assert first_record["high_water"]["totalTokens"] == 170
        assert first_record["observed_increase_after_first"]["totalTokens"] == 90
        assert first_record["event_count"] == 2
        await runtime.close_observers()

        if restart == "ordinary":
            await runtime.stop()
            assert not process_is_alive(started["codex_pid"])
        elif restart == "service_sigkill":
            runtime.service.kill()
            await asyncio.wait_for(runtime.service.wait(), 5)
            assert runtime.service.returncode == -signal.SIGKILL
            assert process_is_alive(started["codex_pid"])
        else:
            assert (
                process_identity(started["codex_pid"])
                == runtime.owned_servers[started["codex_pid"]]
            )
            os.kill(started["codex_pid"], signal.SIGKILL)
            await asyncio.wait_for(runtime.service.wait(), 20)
            assert not process_is_alive(started["codex_pid"])
        restarted = await runtime.launch()
        assert restarted["pid"] != started["pid"]
        assert restarted["codex_pid"] != started["codex_pid"]
        assert not process_is_alive(started["codex_pid"])
        assert restarted["tasks"]["main"]["thread_id"] == thread_id
        second_epoch, second_state = runtime.epoch_state()
        assert second_state["server"]["pid"] == restarted["codex_pid"]
        assert second_epoch != first_epoch
        assert (
            second_state["resource_epochs"][first_epoch]
            == first_state["resource_epochs"][first_epoch]
        )
        assert (await runtime.resources())["tokens"]["epochs"][first_epoch][
            thread_id
        ] == first_record
        await runtime.cli("resume", "--target", "main")
        second_observers = await runtime.observe(thread_id)
        # Real thread resume reports the inherited 170 baseline before new work.
        await runtime.turn(thread_id, second_epoch, 3, 200)
        resources = await runtime.turn(thread_id, second_epoch, 4, 240)
        tokens = resources["tokens"]
        second_record = tokens["epochs"][second_epoch][thread_id]
        assert second_record["first_observed"]["totalTokens"] == 170
        assert second_record["high_water"]["totalTokens"] == 240
        assert second_record["observed_increase_after_first"]["totalTokens"] == 70
        assert second_record["event_count"] == 3
        assert tokens["epochs"][first_epoch][thread_id] == first_record
        assert tokens["threads"][thread_id]["totalTokens"] == 999
        assert tokens["legacy_unscoped"] is True
        assert tokens["sum_epoch_high_water_marks"]["totalTokens"] == 410
        assert tokens["sum_observed_increases_after_first"]["totalTokens"] == 160
        assert tokens["actual_usage_total"] is None and tokens["cost_microusd"] is None
        assert resources["virtual_budget_enabled"] is False and resources["money_receipts"] == {}
        assert tokens["unknown_or_out_of_order_events"] == 0
        for observers, expected in (
            (first_observers, [80, 170]),
            (second_observers, [170, 200, 240]),
        ):
            unique_observers = []
            for events in observers:
                snapshots = {}
                for event in events:
                    counters = event["tokenUsage"]["total"]
                    snapshots.setdefault(json.dumps(counters, sort_keys=True), counters)
                unique = list(snapshots.values())
                assert [counters["totalTokens"] for counters in unique] == expected
                unique_observers.append(unique)
            assert unique_observers[0] == unique_observers[1], "Native observers disagree on usage"
        assert len(runtime.requests) == 4 and not runtime.errors
        await runtime.stop()
        runtime.verify_pair()
        record_property("restart_mode", restart)
        record_property("localhost_fixture_responses", len(runtime.requests))
        record_property("native_epoch_high_water_sum", 410)
        record_property("native_increase_after_first_sum", 160)
        record_property("paid_model_requests", 0)
        record_property("fixed_pair_sha256", json.dumps(runtime.expected_hashes, sort_keys=True))
    finally:
        await runtime.close()
