"""Owned native identity acceptance, with recorded localhost Responses only.

This development check never loads an account login or calls a model. The report
contains synthetic request bodies, native hook metadata and process identities.
Use an installed --hook-python to bind the hook entry point to a candidate wheel.
"""

import argparse
import asyncio
from contextlib import suppress
import errno
import fcntl
import hashlib
import json
import os
from pathlib import Path
import pty
import re
import signal
import shlex
import subprocess
import struct
import sys
import tempfile
import termios
import venv

import tomlkit

from alice_codex.codex import CodexClient
from alice_codex.identity import (
    build_identity_bundle,
    identity_hook_groups,
    validate_identity_hooks,
)
from alice_codex.rpc import RpcClient


PINNED_MAIN = "4ca47945439f9251fe35f4cbe071369192cd9a6c5a3a17b75c7a11ad548a9c7f"
PINNED_HOST = "207984ae6d639c39370fc01ca9b1c79f72487a842b0b70407c92ed0a7c295d6f"


class Terminal:
    """Standalone owned PTY fixture; candidate runtime needs no pytest install."""

    QUERIES = {
        b"\x1b[6n": b"\x1b[1;1R",
        b"\x1b[c": b"\x1b[?1;2c",
        b"\x1b[>c": b"\x1b[>0;0;0c",
        b"\x1b[?u": b"\x1b[?0u",
        b"\x1b]10;?\x1b\\": b"\x1b]10;rgb:ffff/ffff/ffff\x1b\\",
        b"\x1b]11;?\x1b\\": b"\x1b]11;rgb:0000/0000/0000\x1b\\",
        b"\x1b]10;?\x07": b"\x1b]10;rgb:ffff/ffff/ffff\x07",
        b"\x1b]11;?\x07": b"\x1b]11;rgb:0000/0000/0000\x07",
    }

    def __init__(self):
        self.master = self.process = self.reader = None
        self.output, self.query_buffer = bytearray(), b""

    @property
    def text(self):
        value = re.sub(rb"\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)", b"", bytes(self.output))
        return re.sub(rb"\x1b\[[0-?]*[ -/]*[@-~]", b"", value).decode("utf-8", errors="replace")

    async def start(self, arguments, *, cwd, env):
        self.master, slave = pty.openpty()
        try:
            fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 40, 140, 0, 0))
            os.set_blocking(self.master, False)
            self.process = await asyncio.create_subprocess_exec(
                *arguments,
                cwd=cwd,
                env=env,
                stdin=slave,
                stdout=slave,
                stderr=slave,
                start_new_session=True,
            )
        finally:
            os.close(slave)
        self.reader = asyncio.create_task(self._read())

    async def _read(self):
        while True:
            try:
                chunk = os.read(self.master, 65536)
            except BlockingIOError:
                await asyncio.sleep(0.01)
                continue
            except OSError as error:
                if error.errno == errno.EIO:
                    return
                raise
            if not chunk:
                return
            self.output.extend(chunk)
            assert len(self.output) <= 512 * 1024, "Owned terminal exceeded bounded transcript"
            self.query_buffer += chunk
            while True:
                matches = [
                    (self.query_buffer.find(query), query, response)
                    for query, response in self.QUERIES.items()
                    if query in self.query_buffer
                ]
                if not matches:
                    break
                position, query, response = min(matches)
                os.write(self.master, response)
                self.query_buffer = self.query_buffer[position + len(query) :]
            self.query_buffer = self.query_buffer[-48:]

    async def wait_text(self, expected, timeout=20):
        async with asyncio.timeout(timeout):
            while expected not in self.text:
                if self.reader.done():
                    await self.reader
                    raise AssertionError(
                        f"Native TUI exited before {expected!r}: {self.text[-4000:]}"
                    )
                assert self.process.returncode is None, self.text[-4000:]
                await asyncio.sleep(0.02)
        assert (
            "startup failed" not in self.text.lower()
            and "no rollout found" not in self.text.lower()
        )

    async def quit(self):
        os.write(self.master, b"\x15")
        for _ in range(3):
            if self.process.returncode is not None:
                break
            os.write(self.master, b"\x03")
            with suppress(TimeoutError):
                await asyncio.wait_for(asyncio.shield(self.process.wait()), 0.5)
        await asyncio.wait_for(self.process.wait(), 5)
        await asyncio.wait_for(self.reader, 5)
        assert self.process.returncode == 0, self.text[-4000:]

    async def close(self):
        if self.process and self.process.returncode is None:
            self.process.terminate()
            try:
                await asyncio.wait_for(self.process.wait(), 8)
            except TimeoutError:
                self.process.kill()
                await self.process.wait()
        if self.reader:
            self.reader.cancel()
            await asyncio.gather(self.reader, return_exceptions=True)
        if self.master is not None:
            os.close(self.master)
            self.master = None


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def hook_module_digest(python):
    result = subprocess.run(
        [
            str(python),
            "-I",
            "-c",
            "import hashlib,pathlib,alice_codex.identity; "
            "print(hashlib.sha256(pathlib.Path(alice_codex.identity.__file__).read_bytes()).hexdigest())",
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )
    value = result.stdout.strip()
    assert len(value) == 64 and all(char in "0123456789abcdef" for char in value)
    return value


def process_identity(pid):
    result = subprocess.run(
        ["ps", "-p", str(pid), "-o", "lstart=", "-o", "command="],
        capture_output=True,
        text=True,
        timeout=5,
    )
    return result.stdout.strip() or None


def request_text(request, *, role=None):
    return "\n".join(
        part.get("text", "")
        for item in request.get("input", [])
        if item.get("type") == "message" and (role is None or item.get("role") == role)
        for part in item.get("content", [])
        if isinstance(part, dict)
    )


def identity_markers(revision):
    return [f"OWNED_IDENTITY_{revision}_{name}" for name in ("SOUL", "USER", "MEMORY")]


def write_identity(workspace, revision):
    (workspace / "memory").mkdir(exist_ok=True)
    for name, marker in zip(
        ("SOUL.md", "USER.md", "memory/MEMORY.md"), identity_markers(revision), strict=True
    ):
        (workspace / name).write_text(marker + "\nSynthetic fixture; no personal records.\n")
    return build_identity_bundle(workspace)


def assert_snapshot(request, revision, *, absent=None, allow_tool_output=False):
    developer = request_text(request, role="developer")
    for marker in identity_markers(revision):
        assert marker in developer, f"Native developer input is missing {marker}"
    if absent:
        for marker in identity_markers(absent):
            assert marker not in request_text(request), (
                f"Ignored update unexpectedly arrived: {marker}"
            )
    # The fixture never returns a tool call: identity must arrive before tools.
    assert allow_tool_output or not any(
        item.get("type") in {"function_call_output", "custom_tool_call_output"}
        for item in request.get("input", [])
    ), "Identity must not rely on a preceding file-reading tool"


class RecordedIdentityRuntime:
    def __init__(self, root, binary, hook_python):
        self.root, self.binary, self.hook_python = root.resolve(), binary.resolve(), hook_python
        self.home, self.workspace = self.root / "codex", self.root / "workspace"
        self.home.mkdir()
        self.workspace.mkdir()
        # macOS Unix sockets cannot use pytest's potentially long evidence path.
        self.socket_dir = Path(tempfile.mkdtemp(prefix="alice-id-rpc-", dir="/tmp"))
        self.socket = self.socket_dir / "rpc.sock"
        self.requests, self.events, self.errors, self.processes = [], [], [], {}
        self.peers, self.handlers = set(), set()
        self.rpc = self.client = self.process = self.http = self.terminal = None
        self.roots = []
        self.server_sequence = 0
        self.config = {}
        self.usage = 0
        self.fail_next = False
        self.after_request = None
        self.tool_failure = False
        self.request_effects = []
        self.usage_schedule = []
        self.pause_goal_at = None

    async def provider(self, reader, writer):
        self.peers.add(writer)
        self.handlers.add(asyncio.current_task())
        try:
            header = (await reader.readuntil(b"\r\n\r\n")).decode().split("\r\n")
            headers = {
                row.split(":", 1)[0].lower(): row.split(":", 1)[1].strip()
                for row in header[1:]
                if ":" in row
            }
            assert header[0] == "POST /responses HTTP/1.1", header[0]
            assert "authorization" not in headers and "content-encoding" not in headers
            assert int(headers["content-length"]) <= 2 * 1024 * 1024
            body = json.loads(await reader.readexactly(int(headers["content-length"])))
            assert len(self.requests) < 30, "Unexpected native request loop"
            self.requests.append(body)
            number = len(self.requests)
            if self.pause_goal_at and number == self.pause_goal_at[0]:
                await self.rpc.request(
                    "thread/goal/set", {"threadId": self.pause_goal_at[1], "status": "paused"}
                )
            if self.after_request:
                callback, self.after_request = self.after_request, None
                callback()
            if self.request_effects:
                self.request_effects.pop(0)()
            usage = self.usage_schedule.pop(0) if self.usage_schedule else self.usage
            if self.fail_next:
                self.fail_next = False
                writer.write(
                    b"HTTP/1.1 500 Owned response failure\r\nContent-Length: 0\r\n"
                    b"Connection: close\r\n\r\n"
                )
                await writer.drain()
                return
            # Deliberately omit every identity marker from the compaction result.
            item = {
                "id": f"recorded-{number}",
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "Owned recorded summary."}],
            }
            if self.tool_failure and number == 1:
                advertised = body.get("tools", []) + [
                    tool
                    for entry in body["input"]
                    if entry.get("type") == "additional_tools"
                    for tool in entry.get("tools", [])
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
                assert len(matches) == 1, "Exactly one native Code Mode exec must be advertised"
                command = {
                    "cmd": "cat owned-missing-identity-fixture.txt",
                    "login": False,
                    "workdir": str(self.workspace),
                    "max_output_tokens": 300,
                }
                item = {
                    "type": "custom_tool_call",
                    "call_id": "owned-first-tool-fails",
                    **matches[0],
                    "input": "text(await tools.exec_command(" + json.dumps(command) + "));",
                }
            elif self.tool_failure:
                assert number == 2, "The single tool failure needs exactly two fixture requests"
                children = subprocess.run(
                    ["pgrep", "-P", str(self.process.pid)],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                for child in children.stdout.split():
                    self.processes[int(child)] = process_identity(int(child))
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
                        "usage": {
                            "input_tokens": usage,
                            "output_tokens": 0,
                            "total_tokens": usage,
                        },
                    },
                },
            ):
                writer.write(("data: " + json.dumps(event) + "\n\n").encode())
            await writer.drain()
        except Exception as error:
            self.errors.append(repr(error))
        finally:
            writer.close()
            with suppress(ConnectionError):
                await writer.wait_closed()
            self.peers.discard(writer)
            self.handlers.discard(asyncio.current_task())

    async def prepare(self):
        assert digest(self.binary) == PINNED_MAIN, "Select the fixed, verified native main binary"
        assert digest(self.binary.parent / "codex-code-mode-host") == PINNED_HOST
        self.http = await asyncio.start_server(self.provider, "127.0.0.1", 0)
        port = self.http.sockets[0].getsockname()[1]
        self.config = {
            "model": "gpt-6-astra",
            "model_provider": "recorded",
            "approval_policy": "never",
            "sandbox_mode": "read-only",
            "check_for_update_on_startup": False,
            "projects": {str(self.workspace): {"trust_level": "trusted"}},
            "tui": {"animations": False},
            "web_search": "disabled",
            "features": {
                "apps": False,
                "plugins": False,
                "memories": False,
                "hooks": True,
                "enable_request_compression": False,
                "token_budget": False,
            },
            "model_providers": {
                "recorded": {
                    "name": "Owned identity Responses fixture; no model inference",
                    "base_url": f"http://127.0.0.1:{port}",
                    "wire_api": "responses",
                    "supports_websockets": False,
                    "requires_openai_auth": False,
                    "request_max_retries": 0,
                    "stream_max_retries": 0,
                }
            },
        }
        self.write_config()

    def write_config(self):
        (self.home / "config.toml").write_text(tomlkit.dumps(self.config))

    def environment(self):
        return {
            "HOME": str(self.root),
            "CODEX_HOME": str(self.home),
            "PATH": "/usr/bin:/bin",
            "TERM": "xterm-256color",
            "LANG": "en_US.UTF-8",
            "RUST_LOG": "error",
            "NO_PROXY": "127.0.0.1,localhost",
            "no_proxy": "127.0.0.1,localhost",
        }

    async def launch(self):
        assert self.process is None
        self.socket.unlink(missing_ok=True)
        self.server_sequence += 1
        self.log = (self.root / f"server-{self.server_sequence}.log").open("wb")
        self.process = await asyncio.create_subprocess_exec(
            str(self.binary),
            "app-server",
            "--listen",
            "unix://" + str(self.socket),
            cwd=self.workspace,
            env=self.environment(),
            stdout=self.log,
            stderr=self.log,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
        self.processes[self.process.pid] = process_identity(self.process.pid)
        async with asyncio.timeout(20):
            while not self.socket.exists():
                assert self.process.returncode is None, "Owned App Server exited during launch"
                await asyncio.sleep(0.02)
        self.rpc = await RpcClient.connect_unix(self.socket)
        await self.rpc.initialize(name="alice_identity_native_check")
        self.rpc.add_listener(self.events.append)
        self.client = CodexClient(self.rpc, owned_root_ids=self.roots)

    async def stop_server(self):
        if self.client:
            for thread in self.roots:
                with suppress(Exception):
                    await asyncio.wait_for(self.client.stop_tree(thread), 5)
            self.client.close()
            self.client = None
        if self.rpc:
            await self.rpc.close()
            self.rpc = None
        if self.process:
            if self.process.returncode is None:
                self.process.terminate()
                try:
                    await asyncio.wait_for(self.process.wait(), 10)
                except TimeoutError:
                    os.killpg(self.process.pid, signal.SIGKILL)
                    await self.process.wait()
                    raise AssertionError("Owned native server required forced termination")
            assert self.process.returncode == 0
            self.process = None
            self.log.close()

    async def start_thread(self, bundle):
        result = await self.client.thread_start(
            cwd=str(self.workspace),
            model="gpt-6-astra",
            modelProvider="recorded",
            approvalPolicy="never",
            sandbox="read-only",
            developerInstructions=bundle.developer_instructions,
        )
        thread = result["thread"]["id"]
        self.roots.append(thread)
        return thread

    async def completed(self, thread, since):
        async with asyncio.timeout(25):
            while True:
                assert not self.errors, self.errors
                matches = [
                    event
                    for event in self.events[since:]
                    if event.get("method") == "turn/completed"
                    and event.get("params", {}).get("threadId") == thread
                ]
                if matches:
                    return matches[-1]["params"]["turn"]
                await asyncio.sleep(0.02)

    async def turn(self, thread, prompt, *, blocked=False):
        before, event_start = len(self.requests), len(self.events)
        await self.client.turn_start(thread, prompt, approvalPolicy="never")
        result = await self.completed(thread, event_start)
        if blocked:
            assert len(self.requests) == before, "Invalid identity reached model inference"
        else:
            assert result["status"] == "completed", result
            assert len(self.requests) == before + 1, "Expected exactly one recorded request"
        return {"turn": result, "request_index": before, "events": self.events[event_start:]}

    async def compact(self, thread):
        before, event_start = len(self.requests), len(self.events)
        await self.rpc.request("thread/compact/start", {"threadId": thread})
        result = await self.completed(thread, event_start)
        assert result["status"] == "completed", result
        assert len(self.requests) == before + 1, "Compaction must actually call the fixture"
        assert any(
            event.get("params", {}).get("item", {}).get("type") == "contextCompaction"
            for event in self.events[event_start:]
        ), "Native compaction event is required"
        return {"turn": result, "request_index": before, "events": self.events[event_start:]}

    async def close(self):
        if self.terminal:
            await self.terminal.close()
        await self.stop_server()
        if self.http:
            self.http.close()
            await self.http.wait_closed()
        for writer in list(self.peers):
            writer.close()
        for handler in list(self.handlers):
            handler.cancel()
        await asyncio.gather(*self.handlers, return_exceptions=True)
        remaining = {
            pid: identity
            for pid, identity in self.processes.items()
            if identity and process_identity(pid) == identity
        }
        assert not remaining, f"Owned native processes remain: {list(remaining)}"
        self.socket.unlink(missing_ok=True)
        self.socket_dir.rmdir()


def hook_metadata(result):
    assert len(result["data"]) == 1
    entry = result["data"][0]
    assert not entry["warnings"] and not entry["errors"], entry
    return entry["hooks"]


async def check_identity_native(
    root, binary, *, hook_python=None, delivery=False, tool_failure=False
):
    """Run bounded native protocol scenarios and save raw synthetic evidence on failure."""
    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=False)
    report = {"passed": False, "native_model_calls": 0, "checks": {}, "root": str(root)}
    if hook_python is None:
        # A source check uses a private Python environment linked to this exact
        # source tree. Release checks pass the candidate's installed interpreter.
        environment = root / "hook-python"
        venv.EnvBuilder(with_pip=False).create(environment)
        hook_python = environment / "bin/python"
        site = next((environment / "lib").glob("python*/site-packages"))
        import alice_codex
        import websockets

        source = Path(alice_codex.__file__).resolve().parent.parent
        dependencies = Path(websockets.__file__).resolve().parent.parent
        (site / "identity-source.pth").write_text(str(source) + "\n" + str(dependencies) + "\n")
        report["hook_package_mode"] = "isolated_source_link"
    else:
        # Preserve a virtualenv interpreter symlink: resolving it can silently
        # select the base Python and lose the installed candidate package.
        hook_python = Path(hook_python).expanduser().absolute()
        report["hook_package_mode"] = "installed_interpreter"
    runtime = RecordedIdentityRuntime(root, Path(binary), str(hook_python))
    report["identity_module_sha256"] = hook_module_digest(hook_python)
    report["acceptance_script_sha256"] = digest(__file__)
    import alice_codex.identity

    report["checker_python"] = sys.executable
    report["checker_prefix"] = sys.prefix
    report["checker_identity_path"] = alice_codex.identity.__file__
    report["checker_identity_sha256"] = digest(alice_codex.identity.__file__)
    if report["hook_package_mode"] == "installed_interpreter":
        assert report["checker_identity_sha256"] == report["identity_module_sha256"], (
            "Checker and native hook imported different identity implementations"
        )
    try:
        await runtime.prepare()
        if tool_failure:
            await check_first_tool_failure(runtime, report)
            report["passed"] = True
            return report
        if delivery:
            await check_delivery(runtime, report)
            report["passed"] = True
            return report
        baseline = write_identity(runtime.workspace, "A")
        await runtime.launch()
        thread = await runtime.start_thread(baseline)
        phase = await runtime.turn(thread, "Owned first input")
        assert_snapshot(runtime.requests[-1], "A")
        report["checks"]["new_thread_developer"] = phase
        updated = write_identity(runtime.workspace, "B")
        await runtime.client.thread_resume(
            thread, developerInstructions=updated.developer_instructions
        )
        phase = await runtime.turn(thread, "Owned loaded resume negative control")
        assert_snapshot(runtime.requests[-1], "A", absent="B")
        report["checks"]["loaded_resume_override_ignored"] = phase
        await runtime.stop_server()
        await runtime.launch()
        resumed = await runtime.client.thread_resume(
            thread,
            developerInstructions=updated.developer_instructions,
            cwd=str(runtime.workspace),
            model="gpt-6-astra",
            modelProvider="recorded",
        )
        assert resumed["thread"]["id"] == thread
        phase = await runtime.turn(thread, "Owned cold resume input")
        assert_snapshot(runtime.requests[-1], "A", absent="B")
        report["checks"]["same_id_cold_resume_keeps_history_baseline"] = phase
        report["checks"]["native_compaction"] = await runtime.compact(thread)
        phase = await runtime.turn(thread, "Owned input after native compaction")
        assert_snapshot(runtime.requests[-1], "B")
        report["checks"]["baseline_after_compaction"] = phase

        groups = identity_hook_groups(str(hook_python), runtime.workspace)
        trace = root / "trace_hook.py"
        trace.write_text("""import json, pathlib, subprocess, sys
data = sys.stdin.read()
with pathlib.Path(sys.argv[1]).open("a") as out:
    out.write(json.dumps(json.loads(data)) + "\\n")
value = json.loads(data)
transcript = pathlib.Path(value["transcript_path"]) if value.get("transcript_path") else None
if transcript and transcript.is_file():
    with transcript.open("rb") as inp:
        inp.seek(max(0, transcript.stat().st_size - 96000))
        tail = inp.read(96000).decode("utf-8", errors="replace")
    with pathlib.Path(sys.argv[1] + ".transcripts").open("a") as out:
        out.write(json.dumps({"input":value,"transcript_tail":tail}) + "\\n")
if len(sys.argv) > 2:
    result = subprocess.run(sys.argv[2:], input=data, text=True, capture_output=True)
    sys.stdout.write(result.stdout)
    sys.stderr.write(result.stderr)
    raise SystemExit(result.returncode)
print("{}")
""")
        trace_command = shlex.join(
            [str(hook_python), "-I", str(trace), str(root / "hook-inputs.jsonl")]
        )
        for entries in groups.values():
            for group in entries:
                for hook in group["hooks"]:
                    hook["command"] = trace_command + " " + hook["command"]
        groups["Stop"] = [
            {
                "hooks": [
                    {"type": "command", "command": trace_command, "timeout": 10, "async": False}
                ]
            }
        ]
        runtime.config["hooks"] = groups
        runtime.write_config()
        before_trust = hook_metadata(
            await runtime.rpc.request("hooks/list", {"cwds": [str(runtime.workspace)]})
        )
        assert len(before_trust) == 3
        assert all(item["trustStatus"] == "untrusted" for item in before_trust)
        report["checks"]["untrusted_hook_metadata"] = before_trust
        untrusted_thread = await runtime.start_thread(updated)
        write_identity(runtime.workspace, "C")
        await runtime.turn(untrusted_thread, "Owned untrusted hook negative control")
        assert_snapshot(runtime.requests[-1], "B", absent="C")
        groups["state"] = {
            item["key"]: {"trusted_hash": item["currentHash"]} for item in before_trust
        }
        runtime.write_config()
        trusted = hook_metadata(
            await runtime.rpc.request("hooks/list", {"cwds": [str(runtime.workspace)]})
        )
        assert all(item["trustStatus"] == "trusted" for item in trusted)
        report["checks"]["trusted_hook_metadata"] = trusted
        phase = await runtime.turn(
            untrusted_thread, "Owned existing session trust negative control"
        )
        assert_snapshot(runtime.requests[-1], "B", absent="C")
        report["checks"]["existing_session_keeps_untrusted_hook_configuration"] = phase
        hooked_thread = await runtime.start_thread(updated)
        phase = await runtime.turn(hooked_thread, "Owned trusted startup and prompt hooks")
        assert_snapshot(runtime.requests[-1], "C")
        report["checks"]["trusted_hook_same_server_new_thread"] = phase
        write_identity(runtime.workspace, "D")
        phase = await runtime.turn(hooked_thread, "Owned changed prompt identity")
        assert_snapshot(runtime.requests[-1], "D")
        report["checks"]["user_prompt_hook_refresh"] = phase
        report["checks"]["hooked_compaction"] = await runtime.compact(hooked_thread)
        write_identity(runtime.workspace, "E")
        phase = await runtime.turn(hooked_thread, "Owned prompt after compact hook")
        assert_snapshot(runtime.requests[-1], "E")
        assert any("sessionStart" in json.dumps(event) for event in phase["events"]), (
            "SessionStart(compact) must actually run after compaction"
        )
        report["checks"]["compact_hook_refresh"] = phase
        # Missing/oversized source errors must block this native turn before the
        # recorded provider sees input; returning a hook JSON alone is insufficient.
        user = runtime.workspace / "USER.md"
        saved = user.read_bytes()
        user.unlink()
        report["checks"]["missing_identity_blocks"] = await runtime.turn(
            hooked_thread, "Owned missing identity negative control", blocked=True
        )
        user.write_bytes(saved)
        soul = runtime.workspace / "SOUL.md"
        saved = soul.read_bytes()
        soul.write_text("x" * (16 * 1024 + 1))
        report["checks"]["oversized_identity_blocks"] = await runtime.turn(
            hooked_thread, "Owned oversized identity negative control", blocked=True
        )
        soul.write_bytes(saved)

        runtime.terminal = Terminal()
        await runtime.rpc.request(
            "thread/name/set", {"threadId": hooked_thread, "name": "Owned identity fixture"}
        )
        await runtime.terminal.start(
            [
                str(runtime.binary),
                "--remote",
                "unix://" + str(runtime.socket),
                "--cd",
                str(runtime.workspace),
                "resume",
                hooked_thread,
            ],
            cwd=runtime.workspace,
            env=runtime.environment(),
        )
        await runtime.terminal.wait_text("gpt-6-astra")
        runtime.processes[runtime.terminal.process.pid] = process_identity(
            runtime.terminal.process.pid
        )
        write_identity(runtime.workspace, "F")
        before, event_start = len(runtime.requests), len(runtime.events)
        os.write(runtime.terminal.master, b"OwnedIdentityTuiInput")
        await runtime.terminal.wait_text("OwnedIdentityTuiInput")
        os.write(runtime.terminal.master, b"\r")
        phase = await runtime.completed(hooked_thread, event_start)
        assert phase["status"] == "completed" and len(runtime.requests) == before + 1
        assert_snapshot(runtime.requests[-1], "F")
        report["checks"]["remote_tui_submitted_hook_refresh"] = {
            "turn": phase,
            "request_index": before,
            "terminal_contains_submitted_prompt": "OwnedIdentityTuiInput" in runtime.terminal.text,
        }
        await runtime.terminal.quit()
        report["terminal_tail"] = runtime.terminal.text[-4000:]
        await runtime.terminal.close()
        runtime.terminal = None
        # This synthetic Goal is bounded by returned usage, never a paid model.
        write_identity(runtime.workspace, "G")
        before, event_start = len(runtime.requests), len(runtime.events)
        runtime.usage = 2
        await runtime.rpc.request(
            "thread/goal/set",
            {
                "threadId": hooked_thread,
                "objective": "Owned bounded goal hook probe",
                "tokenBudget": 1,
            },
        )
        phase = await runtime.completed(hooked_thread, event_start)
        goal = await runtime.rpc.request("thread/goal/get", {"threadId": hooked_thread})
        assert goal["goal"]["status"] == "budgetLimited", goal
        assert len(runtime.requests) == before + 1
        report["checks"]["goal_continuation_hook_probe"] = {
            "turn": phase,
            "goal": goal,
            "request_index": before,
            "current_revision_delivered": all(
                marker in request_text(runtime.requests[-1], role="developer")
                for marker in identity_markers("G")
            ),
            "events": runtime.events[event_start:],
        }
        report["checks"]["per_request_snapshot_counts"] = [
            request_text(item, role="developer").count("<alice_identity_snapshot>")
            for item in runtime.requests
        ]
        report["passed"] = True
    except BaseException as error:
        report["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        try:
            if runtime.terminal:
                report["terminal_tail"] = runtime.terminal.text[-8000:]
                if runtime.terminal.process:
                    runtime.processes[runtime.terminal.process.pid] = process_identity(
                        runtime.terminal.process.pid
                    )
            trace_log = root / "hook-inputs.jsonl"
            if trace_log.exists():
                report["hook_inputs"] = [
                    json.loads(line) for line in trace_log.read_text().splitlines()
                ]
            transcript_log = root / "hook-inputs.jsonl.transcripts"
            if transcript_log.exists():
                report["hook_transcripts"] = [
                    json.loads(line) for line in transcript_log.read_text().splitlines()
                ]
            await runtime.close()
            report["owned_processes_remaining"] = []
            report["identity_module_sha256_after"] = hook_module_digest(hook_python)
            assert report["identity_module_sha256_after"] == report["identity_module_sha256"], (
                "Identity module changed during native acceptance"
            )
        except BaseException as error:
            report["passed"] = False
            report["cleanup_error"] = f"{type(error).__name__}: {error}"
            raise
        finally:
            report["requests"] = runtime.requests
            report["events"] = runtime.events
            report["provider_errors"] = runtime.errors
            report["process_identities"] = runtime.processes
            report["binary_sha256"] = digest(binary)
            report["host_sha256"] = digest(Path(binary).parent / "codex-code-mode-host")
            (root / "identity-native-report.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


async def check_first_tool_failure(runtime, report):
    runtime.tool_failure = True
    runtime.config["features"].update(
        code_mode=True, code_mode_host={"enabled": True, "disable_in_process_fallback": True}
    )
    runtime.write_config()
    baseline = write_identity(runtime.workspace, "TOOL")
    missing = runtime.workspace / "owned-missing-identity-fixture.txt"
    assert not missing.exists()
    await runtime.launch()
    thread = await runtime.start_thread(baseline)
    event_start = len(runtime.events)
    await runtime.client.turn_start(thread, "Owned first tool fails; preserve identity")
    result = await runtime.completed(thread, event_start)
    assert result["status"] == "completed" and len(runtime.requests) == 2
    assert_snapshot(runtime.requests[0], "TOOL")
    assert_snapshot(runtime.requests[1], "TOOL", allow_tool_output=True)
    outputs = [
        item
        for item in runtime.requests[1]["input"]
        if item.get("type") == "custom_tool_call_output"
        and item.get("call_id") == "owned-first-tool-fails"
    ]
    assert len(outputs) == 1 and "No such file or directory" in json.dumps(outputs)
    failures = [
        event["params"]["item"]
        for event in runtime.events[event_start:]
        if event.get("method") == "item/completed"
        and event.get("params", {}).get("item", {}).get("type") == "commandExecution"
        and event["params"]["item"].get("exitCode") == 1
    ]
    assert failures, "An actual native command must fail, not a fabricated provider tool result"
    host = str(runtime.binary.parent / "codex-code-mode-host")
    assert any(host in identity for identity in runtime.processes.values() if identity), (
        "The matching native Code Mode host must actually execute"
    )
    assert not missing.exists()
    report["checks"]["identity_before_and_after_real_first_tool_failure"] = {
        "request_indexes": [0, 1],
        "turn": result,
        "native_command_failures": failures,
        "tool_outputs": outputs,
    }


async def check_delivery(runtime, report):
    """Exercise the shipped receipt implementation through exact native hooks."""
    state_dir = runtime.root / "delivery"
    baseline = write_identity(runtime.workspace, "H")
    groups = identity_hook_groups(
        runtime.hook_python, runtime.workspace, state_dir=state_dir, socket_path=runtime.socket
    )
    expected_groups = groups.copy()
    runtime.config["hooks"] = groups
    runtime.write_config()
    await runtime.launch()
    listing = await runtime.rpc.request("hooks/list", {"cwds": [str(runtime.workspace)]})
    metadata = hook_metadata(listing)
    assert len(metadata) == 3 and all(item["trustStatus"] == "untrusted" for item in metadata)
    groups["state"] = validate_identity_hooks(
        listing, runtime.home / "config.toml", runtime.workspace, expected_groups
    )
    runtime.write_config()
    listing = await runtime.rpc.request("hooks/list", {"cwds": [str(runtime.workspace)]})
    metadata = hook_metadata(listing)
    validate_identity_hooks(
        listing,
        runtime.home / "config.toml",
        runtime.workspace,
        expected_groups,
        require_trusted=True,
    )
    assert all(item["trustStatus"] == "trusted" for item in metadata)
    report["checks"]["exact_product_hooks_trusted"] = metadata
    thread = await runtime.start_thread(baseline)
    ledger_path = state_dir / (hashlib.sha256(thread.encode()).hexdigest() + ".json")

    def ledger():
        return json.loads(ledger_path.read_text())

    def count():
        return request_text(runtime.requests[-1], role="developer").count(
            "<alice_identity_snapshot>"
        )

    def assert_ack(bundle):
        state = ledger()
        assert state["pending"] is None and state["ack"]["revision"] == bundle.revision, state
        return state

    first = await runtime.turn(thread, "Owned delivery first input")
    assert_snapshot(runtime.requests[-1], "H")
    first_count = count()
    report["checks"]["first_real_stop_ack"] = {
        "turn": first,
        "ledger": assert_ack(baseline),
        "snapshot_count": first_count,
    }
    for number in range(2):
        phase = await runtime.turn(thread, f"Owned unchanged delivery {number}")
        assert_snapshot(runtime.requests[-1], "H")
        assert count() == first_count, "Unchanged memory appended another complete snapshot"
        assert_ack(baseline)
    report["checks"]["two_unchanged_turns_do_not_append"] = phase
    changed = write_identity(runtime.workspace, "I")
    phase = await runtime.turn(thread, "Owned changed delivery")
    assert_snapshot(runtime.requests[-1], "I")
    assert count() == first_count + 1
    assert_ack(changed)
    previous_stop_turn = phase["turn"]["id"]
    stable_count = count()
    await runtime.turn(thread, "Owned changed delivery already acknowledged")
    assert count() == stable_count
    report["checks"]["changed_revision_delivered_once"] = phase

    pending_bundle = write_identity(runtime.workspace, "J")
    before, event_start = len(runtime.requests), len(runtime.events)
    runtime.fail_next = True
    await runtime.client.turn_start(thread, "Owned failed response before receipt")
    failed = await runtime.completed(thread, event_start)
    assert failed["status"] == "failed" and len(runtime.requests) == before + 1
    pending = ledger()
    assert pending["ack"]["revision"] == changed.revision
    assert pending["pending"]["revision"] == pending_bundle.revision
    failed_count = count()
    report["checks"]["failed_provider_does_not_ack"] = {"turn": failed, "ledger": pending}

    # Replay a delayed native Stop through the actual installed hook entry point;
    # this is a synthetic callback replay, not a new native completion event.
    thread_info = await runtime.client.thread_read(thread, include_turns=False)
    delayed = {
        "hook_event_name": "Stop",
        "session_id": thread,
        "turn_id": previous_stop_turn,
        "last_assistant_message": "Owned recorded summary.",
        "transcript_path": thread_info["thread"]["path"],
    }
    result = await asyncio.to_thread(
        subprocess.run,
        [
            runtime.hook_python,
            "-I",
            "-m",
            "alice_codex.identity",
            "--workspace",
            str(runtime.workspace),
            "--state-dir",
            str(state_dir),
            "--hook-event",
            "Stop",
        ],
        input=json.dumps(delayed),
        text=True,
        capture_output=True,
        timeout=10,
    )
    assert result.returncode == 0 and json.loads(result.stdout) == {}
    assert ledger() == pending, "A late Stop acknowledged another turn's pending delivery"
    report["checks"]["late_stop_callback_replay_keeps_pending"] = True
    phase = await runtime.turn(thread, "Owned retry after failed response")
    assert_snapshot(runtime.requests[-1], "J")
    assert count() == failed_count + 1, "Unconfirmed delivery must be retried"
    assert_ack(pending_bundle)
    report["checks"]["retry_ack_requires_actual_response"] = phase

    report["checks"]["native_compact_for_delivery"] = await runtime.compact(thread)
    compacted = write_identity(runtime.workspace, "K")
    phase = await runtime.turn(thread, "Owned delivery after compaction")
    assert_snapshot(runtime.requests[-1], "K")
    assert count() <= first_count, "Compaction did not bound obsolete complete snapshots"
    assert_ack(compacted)
    compact_count = count()
    await runtime.turn(thread, "Owned unchanged post compact delivery")
    assert count() == compact_count
    report["checks"]["native_compact_refresh_then_dedup"] = phase

    await runtime.stop_server()
    resumed = write_identity(runtime.workspace, "L")
    await runtime.launch()
    result = await runtime.client.thread_resume(
        thread,
        developerInstructions=resumed.developer_instructions,
        cwd=str(runtime.workspace),
        model="gpt-6-astra",
        modelProvider="recorded",
    )
    assert result["thread"]["id"] == thread
    phase = await runtime.turn(thread, "Owned cold resume delivery")
    assert_snapshot(runtime.requests[-1], "L")
    assert_ack(resumed)
    report["checks"]["same_id_cold_resume_hook_refresh"] = phase

    runtime.terminal = Terminal()
    await runtime.rpc.request(
        "thread/name/set", {"threadId": thread, "name": "Owned identity delivery"}
    )
    await runtime.terminal.start(
        [
            str(runtime.binary),
            "--remote",
            "unix://" + str(runtime.socket),
            "--cd",
            str(runtime.workspace),
            "resume",
            thread,
        ],
        cwd=runtime.workspace,
        env=runtime.environment(),
    )
    await runtime.terminal.wait_text("gpt-6-astra")
    runtime.processes[runtime.terminal.process.pid] = process_identity(runtime.terminal.process.pid)
    tui_bundle = write_identity(runtime.workspace, "M")
    for number in range(2):
        before, event_start = len(runtime.requests), len(runtime.events)
        prompt = f"OwnedIdentityDeliveryTui{number}"
        os.write(runtime.terminal.master, prompt.encode())
        await runtime.terminal.wait_text(prompt)
        os.write(runtime.terminal.master, b"\r")
        phase = await runtime.completed(thread, event_start)
        assert phase["status"] == "completed" and len(runtime.requests) == before + 1
        assert_snapshot(runtime.requests[-1], "M")
        assert_ack(tui_bundle)
        if number == 0:
            tui_count = count()
        else:
            assert count() == tui_count, "Unchanged direct TUI turn duplicated memory"
    report["checks"]["remote_tui_update_and_dedup"] = {"turn": phase, "snapshot_count": count()}
    await runtime.terminal.quit()
    report["terminal_tail"] = runtime.terminal.text[-4000:]
    await runtime.terminal.close()
    runtime.terminal = None
    # Ordinary preference changes must not create another response at Stop.
    runtime.after_request = lambda: write_identity(runtime.workspace, "N")
    phase = await runtime.turn(thread, "Owned ordinary change during response")
    assert_snapshot(runtime.requests[-1], "M", absent="N")
    assert_ack(tui_bundle)
    assert len([item for item in phase["turn"]["items"] if item["type"] == "agentMessage"]) == 1
    report["checks"]["ordinary_change_does_not_add_sampling"] = phase
    await runtime.turn(thread, "Owned next user input receives changed memory")
    assert_snapshot(runtime.requests[-1], "N")
    assert_ack(build_identity_bundle(runtime.workspace))

    # Public Goal usage can lag until turn completion, so finite goals must
    # conservatively avoid hot injection even while reported status is active.
    before, event_start = len(runtime.requests), len(runtime.events)
    runtime.after_request = lambda: write_identity(runtime.workspace, "O")
    runtime.usage = 2
    await runtime.rpc.request(
        "thread/goal/set",
        {
            "threadId": thread,
            "objective": "Owned bounded product identity refresh",
            "tokenBudget": 3,
        },
    )
    async with asyncio.timeout(20):
        while True:
            goal = await runtime.rpc.request("thread/goal/get", {"threadId": thread})
            completed = [
                event
                for event in runtime.events[event_start:]
                if event.get("method") == "turn/completed"
                and event.get("params", {}).get("threadId") == thread
            ]
            if goal["goal"]["status"] == "budgetLimited" and len(completed) == 2:
                break
            await asyncio.sleep(0.02)
    assert len(runtime.requests) == before + 2, "A finite Goal hot-injected memory"
    assert_snapshot(runtime.requests[before], "N", absent="O")
    assert_snapshot(runtime.requests[before + 1], "N", absent="O")
    report["checks"]["finite_active_goal_defers_memory_refresh"] = {
        "goal": goal,
        "request_indexes": [before, before + 1],
        "events": runtime.events[event_start:],
    }
    runtime.after_request = lambda: write_identity(runtime.workspace, "P")
    phase = await runtime.turn(thread, "Owned exhausted goal cannot append at Stop")
    assert_snapshot(runtime.requests[-1], "O", absent="P")
    report["checks"]["exhausted_goal_does_not_inject"] = phase

    # Only an unbudgeted active Goal may add one sampling per turn. Change files
    # twice in its first turn; pause through public RPC at the third request,
    # after the first turn's cap has already had to prevent another injection.
    before, event_start = len(runtime.requests), len(runtime.events)
    runtime.request_effects = [
        lambda: write_identity(runtime.workspace, "Q"),
        lambda: write_identity(runtime.workspace, "R"),
    ]
    runtime.pause_goal_at = (before + 3, thread)
    await runtime.rpc.request(
        "thread/goal/set",
        {
            "threadId": thread,
            "objective": "Owned unbudgeted Goal with fixture pause",
            "tokenBudget": None,
            "status": "active",
        },
    )
    async with asyncio.timeout(20):
        while True:
            goal = await runtime.rpc.request("thread/goal/get", {"threadId": thread})
            completed = [
                event
                for event in runtime.events[event_start:]
                if event.get("method") == "turn/completed"
                and event.get("params", {}).get("threadId") == thread
            ]
            if goal["goal"]["status"] == "paused" and len(completed) == 2:
                break
            await asyncio.sleep(0.02)
    assert len(runtime.requests) == before + 3, (
        "A second same-turn change bypassed the injection cap"
    )
    assert_snapshot(runtime.requests[before], "O", absent="Q")
    assert_snapshot(runtime.requests[before + 1], "Q", absent="R")
    assert_snapshot(runtime.requests[before + 2], "Q", absent="R")
    starts = [
        event["params"]["turn"]["id"]
        for event in runtime.events[event_start:]
        if event.get("method") == "turn/started"
        and event.get("params", {}).get("threadId") == thread
    ]
    assert len(starts) == len(set(starts)) == 2
    report["checks"]["unbudgeted_goal_allows_one_extra_sampling_per_turn"] = {
        "goal": goal,
        "request_indexes": [before, before + 1, before + 2],
        "ledger": ledger(),
        "events": runtime.events[event_start:],
    }
    runtime.after_request = lambda: write_identity(runtime.workspace, "S")
    phase = await runtime.turn(thread, "Owned paused goal cannot append at Stop")
    assert_snapshot(runtime.requests[-1], "R", absent="S")
    report["checks"]["paused_goal_does_not_inject"] = phase
    await runtime.turn(thread, "Owned acknowledge current files before invalid source checks")
    assert_ack(build_identity_bundle(runtime.workspace))
    for file, content in (("USER.md", None), ("SOUL.md", "x" * (16 * 1024 + 1))):
        target = runtime.workspace / file
        saved = target.read_bytes()
        if content is None:
            target.unlink()
        else:
            target.write_text(content)
        report["checks"]["ack_does_not_bypass_" + file] = await runtime.turn(
            thread, "Owned invalid already acknowledged identity", blocked=True
        )
        target.write_bytes(saved)
    report["checks"]["final_ledger"] = ledger()
    report["scope"] = (
        "Recorded native user turns, cold resume, compaction, TUI and root Goal; no live model"
    )
    report["limits"] = [
        "Goal memory refresh occurs at Stop; no pre-request watcher for idle external changes",
        "An active root Goal without tokenBudget may add one model sampling per turn for a memory change",
        "Finite-budget Goal refresh waits for user input, compact, startup or resume",
        "Public thread/inject_items disallows multi-agent V2 children; root refresh only",
        "Native legacy transcript evidence; unknown formats conservatively resend",
    ]


async def run_cli(args):
    # Cancellation runs each check's owned-process cleanup before exiting.
    async with asyncio.timeout(90):
        return await check_identity_native(
            args.output,
            args.codex,
            hook_python=args.hook_python,
            delivery=args.delivery,
            tool_failure=args.tool_failure,
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--codex", type=Path, required=True)
    parser.add_argument("--hook-python", type=Path)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument(
        "--delivery", action="store_true", help="Run actual receipt module acceptance"
    )
    modes.add_argument(
        "--tool-failure", action="store_true", help="Run actual Code Mode failure acceptance"
    )
    parser.add_argument("--output", type=Path, required=True, help="New private evidence directory")
    args = parser.parse_args()
    report = asyncio.run(run_cli(args))
    print(
        json.dumps(
            {"passed": report["passed"], "report": str(args.output / "identity-native-report.json")}
        )
    )


if __name__ == "__main__":
    main()
