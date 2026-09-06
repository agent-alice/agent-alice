"""Installed, formal CLI startup restores operator-configured heartbeat sources.

Only the lifecycle case uses real Codex; all HTTP/model responses are owned
localhost fixtures. The two artifact cases use an executable sentinel, not Codex.
No test registers a source in memory or replaces the production Service entry.
"""

import asyncio
from contextlib import suppress
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess

import pytest

from alice_codex.config import load_config
from test_native_task_policy import NativePolicyRuntime, process_identity, until


@pytest.fixture
def installed_python(tmp_path):
    selected = os.environ.get("ALICE_ARTIFACT_PYTHON")
    if not selected:
        pytest.skip("this installed-entry test requires ALICE_ARTIFACT_PYTHON")
    python = str(Path(selected).absolute())
    assert Path(python).is_file(), "candidate interpreter is missing"
    private_home = tmp_path / "probe-home"
    private_home.mkdir()
    probe = subprocess.run(
        [
            python,
            "-I",
            "-c",
            """import importlib.metadata as m,json,sys
from pathlib import Path
import alice_codex
dist = m.distribution('alice-codex')
module = Path(alice_codex.__file__).resolve()
direct = json.loads(dist.read_text('direct_url.json') or '{}')
assert module.is_relative_to(Path(sys.prefix).resolve()), 'candidate imported an external source tree'
assert module == Path(dist.locate_file('alice_codex/__init__.py')).resolve()
assert not direct.get('dir_info', {}).get('editable', False), 'candidate must be an installed wheel'
print(json.dumps({'python':sys.executable, 'module':str(module), 'version':dist.version}))
""",
        ],
        cwd=tmp_path,
        env={"HOME": str(private_home), "PATH": os.environ.get("PATH", "/usr/bin:/bin")},
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert probe.returncode == 0, probe.stderr
    actual_python = Path(json.loads(probe.stdout)["python"])
    expected_python = Path(python)
    # Normalize macOS /tmp aliases without following the venv's executable
    # symlink to a base Python that could be shared by an unrelated candidate.
    assert actual_python.parent.resolve() == expected_python.parent.resolve()
    assert actual_python.name == expected_python.name
    return python


def source_document(url, *, context="scope-v1", header_env=None):
    return {
        "version": 1,
        "sources": [
            {
                "target": "configured-monitor",
                "wait_seconds": 0.15,
                "spec": {
                    "source_id": "fixture-answers",
                    "url": url,
                    "subject": "fixture-member",
                    "collection": "answers",
                    "auth_context_version": context,
                    "max_age_seconds": 60,
                    "required_metrics": ["voteup_count"],
                    "header_env": [] if header_env is None else header_env,
                    "request_timeout": 2,
                    "total_seconds": 3,
                },
            }
        ],
    }


class DiskSourceRuntime(NativePolicyRuntime):
    def __init__(self, root, python, sources):
        super().__init__(root)
        assert self.runtime_python == python
        self.sources = sources
        self.first_boot = True
        self.response_mode, self.max_requests = "complete", 3

    async def cli(self, *arguments, **options):
        # The parent helper's remaining behavior is unchanged, but this test
        # saves the complete binary/companion pair into its own isolated home.
        if arguments and arguments[0] == "init":
            arguments = tuple(argument for argument in arguments if argument != "--no-pin")
        return await super().cli(*arguments, **options)

    def configure_sources(self, sources):
        assert self.service is None or self.service.returncode is not None
        path = self.home / "config.json"
        document = json.loads(path.read_text())
        document["poll_seconds"] = 0.05
        if sources is None:
            document.pop("heartbeat_sources", None)
        else:
            document["heartbeat_sources"] = sources
        path.write_text(json.dumps(document))
        self.config = load_config(self.home)

    async def launch(self):
        assert self.host_command is None, "the formal CLI must load and register disk sources"
        if self.first_boot:
            self.configure_sources(self.sources)
            self.first_boot = False
        return await super().launch()

    def remember_owned_mcp(self):
        rows = subprocess.run(
            ["ps", "-ww", "-axo", "pid=,command="],
            capture_output=True,
            text=True,
            check=True,
            timeout=5,
        ).stdout.splitlines()
        for row in rows:
            if f"alice_codex.mcp --home {self.home}" in row:
                pid = int(row.split(None, 1)[0])
                self.owned_mcp[pid] = process_identity(pid)

    async def stop(self):
        self.remember_owned_mcp()
        await super().stop()

    async def close(self):
        self.remember_owned_mcp()
        await super().close()

    async def snapshot(self):
        # Inspection runs against the same installed wheel. It neither creates
        # a Service nor performs source collection or registration.
        process = await asyncio.create_subprocess_exec(
            self.runtime_python,
            "-I",
            "-c",
            """import json,sys
from pathlib import Path
from alice_codex.config import load_config
from alice_codex.runtime_bundle import verify_runtime_bundle
from alice_codex.store import Store
home=Path(sys.argv[1]); target='configured-monitor'
state=json.loads((home/'state/runtime.json').read_text())
with Store(home/'state/schedules.sqlite3') as store:
    result={'heartbeat':store.get_heartbeat_state(target), 'policy':store.get_task_policy(target)}
result.update(task=state['tasks'].get(target), consumed=state.get('heartbeat_consumed',{}).get(target),
              intents=[item for item in state['intents'].values() if item.get('target')==target],
              pair=verify_runtime_bundle(load_config(home),require=True))
print(json.dumps(result))
""",
            str(self.home),
            cwd=self.root,
            env=self.env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), 10)
        except BaseException:
            if process.returncode is None:
                process.kill()
                await process.wait()
            raise
        assert process.returncode == 0, stderr.decode()
        return json.loads(stdout)


class CollectionEndpoint:
    def __init__(self):
        self.status, self.votes = 200, 1
        self.requests, self.errors = [], []
        self.peers, self.handlers = set(), set()
        self.server = None

    async def start(self):
        self.server = await asyncio.start_server(self.handle, "127.0.0.1", 0)
        return f"http://127.0.0.1:{self.server.sockets[0].getsockname()[1]}"

    async def handle(self, reader, writer):
        handler = asyncio.current_task()
        self.handlers.add(handler)
        self.peers.add(writer)
        try:
            raw = (await reader.readuntil(b"\r\n\r\n")).decode()
            method, path, protocol = raw.split("\r\n", 1)[0].split()
            assert method == "GET" and protocol == "HTTP/1.1"
            assert path in {"/answers", "/changed-scope"}
            assert "authorization:" not in raw.lower()
            self.requests.append({"path": path, "status": self.status, "votes": self.votes})
            body = json.dumps(
                {
                    "data": [{"id": "fixture-answer", "voteup_count": self.votes}],
                    "paging": {"is_end": True, "next": None},
                }
            ).encode()
            writer.write(
                f"HTTP/1.1 {self.status} {'OK' if self.status == 200 else 'Unavailable'}\r\n".encode()
                + b"Content-Type: application/json\r\nCache-Control: no-cache\r\n"
                + f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
                + body
            )
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
            self.handlers.discard(handler)

    async def close(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        for peer in tuple(self.peers):
            peer.close()
        for handler in tuple(self.handlers):
            handler.cancel()
        await asyncio.gather(*tuple(self.handlers), return_exceptions=True)


@pytest.mark.native
async def test_disk_sources_restore_consumption_pause_and_unknown_through_formal_cli(
    tmp_path, installed_python
):
    endpoint = CollectionEndpoint()
    origin = await endpoint.start()
    runtime = DiskSourceRuntime(tmp_path, installed_python, source_document(origin + "/answers"))
    target = "configured-monitor"

    async def completed(count):
        state = await runtime.snapshot()
        if len(state["intents"]) == count and all(
            item["status"] == "completed" for item in state["intents"]
        ):
            assert state["policy"] is None
            return state
        return None

    async def observed(state_name, *, after=None, minimum_fetches=0):
        state = await runtime.snapshot()
        evidence = state["heartbeat"]
        if (
            evidence
            and evidence["latest"]["state"] == state_name
            and (evidence["latest"]["id"] != after and len(endpoint.requests) >= minimum_fetches)
        ):
            return state
        return None

    async def unchanged(minimum_fetches):
        state = await observed("known", minimum_fetches=minimum_fetches)
        if state and state["heartbeat"]["comparison"]["state"] == "unchanged":
            assert state["heartbeat"]["waiting_until"] > state["heartbeat"]["latest"]["observed_at"]
            return state
        return None

    try:
        await runtime.configure()
        assert Path(runtime.config.codex_binary).is_relative_to(runtime.home)
        assert (Path(runtime.config.codex_binary).parent / "codex-code-mode-host").is_file()
        await runtime.cli("resume")
        job = await runtime.cli(
            "cron",
            "create",
            "--name",
            "configured source fixture",
            "--every",
            "0.2",
            "--target",
            target,
            "--heartbeat",
            "--prompt",
            "Review the configured source.",
        )
        first = await until(lambda: completed(1))
        assert first["pair"]["paired"] and first["consumed"]["state"] == "known"
        thread_id = first["task"]["thread_id"]
        assert len(runtime.requests) == 1
        await runtime.stop()
        fetches = len(endpoint.requests)
        restarted = await runtime.launch()
        assert restarted["tasks"][target]["thread_id"] == thread_id
        await runtime.cli("resume")
        same = await until(lambda: unchanged(fetches + 3))
        assert same["consumed"] == first["consumed"]
        assert len(same["intents"]) == 1 and len(runtime.requests) == 1

        await runtime.cli("pause", "--target", target)
        assert not (await runtime.status())["autonomy_paused"]
        endpoint.votes = 2
        fetches = len(endpoint.requests)
        paused = await until(lambda: unchanged(fetches + 3))
        assert paused["task"]["paused"] and paused["consumed"] == first["consumed"]
        assert (
            paused["heartbeat"]["latest"]["content_sha256"] != first["consumed"]["content_sha256"]
        )
        await runtime.stop()
        fetches = len(endpoint.requests)
        await runtime.launch()
        paused = await until(lambda: observed("known", minimum_fetches=fetches + 2))
        assert paused["task"]["paused"] and paused["task"]["thread_id"] == thread_id
        assert len(runtime.requests) == 1

        await runtime.stop()
        endpoint.status = 503
        changed = source_document(origin + "/changed-scope", context="scope-v2")
        runtime.configure_sources(changed)
        fetches = len(endpoint.requests)
        await runtime.launch()
        await runtime.cli("resume")
        unavailable = await until(lambda: observed("unknown", minimum_fetches=fetches + 2))
        evidence = unavailable["heartbeat"]
        assert evidence["latest"]["source_id"] == "fixture-answers"
        assert evidence["latest"]["scope_sha256"] != evidence["last_good"]["scope_sha256"]
        assert all(
            item["path"] == "/changed-scope" and item["status"] == 503
            for item in endpoint.requests[fetches:]
        )
        assert unavailable["consumed"] == first["consumed"] and len(runtime.requests) == 1
        endpoint.status = 200
        second = await until(lambda: completed(2))
        assert second["task"]["thread_id"] == thread_id
        assert second["consumed"]["scope_sha256"] != first["consumed"]["scope_sha256"]
        assert len(runtime.requests) == 2

        await runtime.stop()
        before_credentials = (await runtime.snapshot())["heartbeat"]["last_good"]
        required_environment = "ALICE_TEST_HEARTBEAT_AUTH"
        assert required_environment not in runtime.env
        runtime.configure_sources(
            source_document(
                origin + "/changed-scope",
                context="scope-v2",
                header_env=[["Authorization", required_environment]],
            )
        )
        fetches = len(endpoint.requests)
        await runtime.launch()
        await runtime.cli("resume")
        missing = await until(lambda: observed("unknown"))
        for _ in range(2):
            latest_id = missing["heartbeat"]["latest"]["id"]
            missing = await until(lambda: observed("unknown", after=latest_id))
        assert len(endpoint.requests) == fetches, "missing credentials must fail before HTTP"
        assert missing["heartbeat"]["latest"]["source_id"] == "fixture-answers"
        assert missing["heartbeat"]["last_good"] == before_credentials
        assert missing["consumed"] == second["consumed"]
        assert len(missing["intents"]) == 2 and len(runtime.requests) == 2

        # Removing the operator source restores explicit ordinary self-review,
        # never a borrowed known/unchanged verdict. Use one occurrence only.
        await runtime.cli("cron", "disable", job["id"])
        await runtime.stop()
        runtime.configure_sources(None)
        fetches = len(endpoint.requests)
        await runtime.launch()
        await runtime.cli(
            "cron",
            "create",
            "--name",
            "source removed self-review",
            "--at",
            (datetime.now(timezone.utc) + timedelta(seconds=1)).isoformat(),
            "--target",
            target,
            "--heartbeat",
            "--prompt",
            "Review existing commitments once.",
        )
        await runtime.cli("resume")
        cancelled = await until(lambda: completed(3))
        assert cancelled["heartbeat"]["latest"]["state"] == "unconfigured"
        assert cancelled["heartbeat"]["latest"]["source_id"] is None
        assert cancelled["heartbeat"]["waiting_until"] is None
        assert cancelled["consumed"] == second["consumed"]
        assert len(endpoint.requests) == fetches
        assert "unconfigured" in json.dumps(runtime.requests[-1]["input"])
        native = await runtime.rpc.request(
            "thread/read", {"threadId": thread_id, "includeTurns": True}
        )
        assert {turn["id"] for turn in native["thread"]["turns"]} == {
            item["turn_id"] for item in cancelled["intents"]
        }
        assert len(native["thread"]["turns"]) == 3
        assert all(turn["status"] == "completed" for turn in native["thread"]["turns"])
        assert len(runtime.requests) == 3 and not runtime.errors and not endpoint.errors
        await runtime.stop()
    finally:
        await runtime.close()
        await endpoint.close()


@pytest.mark.artifact
@pytest.mark.parametrize("command,damage", [("start", "future_version"), ("serve", "bad_wait")])
def test_installed_bad_source_config_rejects_before_native_execution(
    tmp_path, installed_python, command, damage
):
    home = (tmp_path / "invalid-runtime").resolve()
    home.mkdir()
    marker = tmp_path / "native-was-invoked"
    binary = tmp_path / "codex-sentinel"
    binary.write_text("#!/bin/sh\ntouch " + shlex.quote(str(marker)) + "\nexit 88\n")
    binary.chmod(0o700)
    sources = source_document("http://127.0.0.1:1/answers")
    if damage == "future_version":
        sources["version"] = 999
    else:
        sources["sources"][0]["wait_seconds"] = 0
    config = {
        "home": str(home),
        "codex_binary": str(binary),
        "codex_version": "codex-cli sentinel",
        "codex_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "heartbeat_sources": sources,
    }
    config_path = home / "config.json"
    config_path.write_text(json.dumps(config))
    before = config_path.read_bytes()
    result = subprocess.run(
        [installed_python, "-I", "-m", "alice_codex", "--home", str(home), command],
        cwd=tmp_path,
        env={"HOME": str(tmp_path / "isolated-home"), "PATH": "/usr/bin:/bin"},
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 1
    assert "heartbeat" in result.stderr.lower()
    assert not marker.exists(), "malformed configuration reached the native executable"
    assert not (home / "state/runtime.json").exists()
    assert config_path.read_bytes() == before
