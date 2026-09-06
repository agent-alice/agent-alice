"""Real Alice chat entry point, native TUI, and an empty durable thread.

The terminal is a fixture-owned PTY. A localhost endpoint counts unexpected
model requests; the test only types an unsent draft and never starts a turn.
"""

import asyncio
from contextlib import suppress
import errno
import fcntl
import json
import os
from pathlib import Path
import pty
import re
import signal
import struct
import subprocess
import sys
import termios

import pytest
import tomlkit

from alice_codex.config import load_config
from alice_codex.control import request
from alice_codex.rpc import RpcClient

pytestmark = pytest.mark.native


class Terminal:
    """Answer standard terminal queries and observe text, without screen snapshots."""

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
        self.master = None
        self.process = None
        self.reader = None
        self.output = bytearray()
        self.query_buffer = b""

    @property
    def text(self):
        value = bytes(self.output)
        value = re.sub(rb"\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)", b"", value)
        value = re.sub(rb"\x1b\[[0-?]*[ -/]*[@-~]", b"", value)
        return value.decode("utf-8", errors="replace")

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
                if error.errno == errno.EIO:  # Linux PTY EOF after its slave closes.
                    return
                raise
            if not chunk:
                return
            self.output.extend(chunk)
            if len(self.output) > 512 * 1024:
                raise AssertionError("Native TUI did not settle within its bounded transcript")
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
                    pytest.fail(f"Native TUI exited before {expected!r}: {self.text[-4000:]}")
                assert self.process.returncode is None, self.text[-4000:]
                await asyncio.sleep(0.02)
        assert "startup failed" not in self.text.lower()
        assert "no rollout found" not in self.text.lower()

    async def quit(self):
        # Clear the unsent input; then use the native keyboard exit path.
        os.write(self.master, b"\x15")
        for _ in range(3):
            if self.process.returncode is not None:
                break
            os.write(self.master, b"\x03")
            try:
                await asyncio.wait_for(asyncio.shield(self.process.wait()), 0.5)
            except TimeoutError:
                pass
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


def process_identity(pid):
    return subprocess.run(
        ["ps", "-p", str(pid), "-o", "lstart=", "-o", "command="],
        capture_output=True,
        text=True,
        check=True,
        timeout=5,
    ).stdout.strip()


async def test_native_chat_empty_thread_reopens_same_id_after_service_restart(tmp_path):
    binary = Path(
        os.environ.get(
            "ALICE_TEST_CODEX_BINARY", "/Applications/ChatGPT.app/Contents/Resources/codex"
        )
    )
    assert binary.is_file(), "Required real Codex binary is unavailable"
    version = subprocess.run(
        [str(binary), "--version"], capture_output=True, text=True, check=True, timeout=10
    ).stdout.strip()
    assert version == "codex-cli 0.153.4", "PTY protocol evidence is pinned to this version"
    runtime_python = os.environ.get("ALICE_ARTIFACT_PYTHON", sys.executable)
    root = tmp_path.resolve()
    home, process_home = root / "alice", root / "process-home"
    process_home.mkdir()
    env = {
        "PATH": str(Path(runtime_python).parent)
        + os.pathsep
        + os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": str(process_home),
        "TERM": "xterm-256color",
        "LANG": "en_US.UTF-8",
        "RUST_LOG": "error",
        "PYTHONDONTWRITEBYTECODE": "1",
        "NO_PROXY": "127.0.0.1,localhost,::1",
        "no_proxy": "127.0.0.1,localhost,::1",
    }
    base = [runtime_python, "-I", "-m", "alice_codex", "--home", str(home)]
    model_requests, handlers, writers = [], set(), set()
    service = rpc = terminal = config = None
    owned_servers = {}

    async def unexpected_model(reader, writer):
        handlers.add(asyncio.current_task())
        writers.add(writer)
        model_requests.append(True)
        try:
            writer.write(
                b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            await writer.drain()
        finally:
            writer.close()
            with suppress(ConnectionError):
                await writer.wait_closed()
            writers.discard(writer)
            handlers.discard(asyncio.current_task())

    http = await asyncio.start_server(unexpected_model, "127.0.0.1", 0)
    port = http.sockets[0].getsockname()[1]

    async def cli(*arguments, timeout=30):
        process = await asyncio.create_subprocess_exec(
            *base,
            *arguments,
            cwd=root,
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
        assert process.returncode == 0, stderr.decode()
        return json.loads(stdout)

    async def launch():
        nonlocal service
        service = await asyncio.create_subprocess_exec(
            *base,
            "serve",
            cwd=root,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        async with asyncio.timeout(35):
            while True:
                assert service.returncode is None, (await service.stderr.read()).decode()
                try:
                    status = await request(config.control_socket, "status", timeout=1)
                    if status["ready"]:
                        owned_servers[status["codex_pid"]] = process_identity(status["codex_pid"])
                        return status
                except (OSError, TimeoutError):
                    pass
                await asyncio.sleep(0.03)

    try:
        await cli("init", "--codex", str(binary), "--no-pin", "--model", "gpt-5.6-terra")
        config = load_config(home)
        config_file = config.codex_home / "config.toml"
        document = tomlkit.parse(config_file.read_text())
        document["model_provider"] = "recorded"
        document["check_for_update_on_startup"] = False
        document["web_search"] = "disabled"
        document["features"].update(apps=False, plugins=False, enable_request_compression=False)
        document["model_providers"] = {
            "recorded": {
                "name": "Empty TUI localhost fixture; inference must not occur",
                "base_url": f"http://127.0.0.1:{port}",
                "wire_api": "responses",
                "supports_websockets": False,
                "requires_openai_auth": False,
                "request_max_retries": 0,
                "stream_max_retries": 0,
            }
        }
        config_file.write_text(tomlkit.dumps(document))
        assert not (config.codex_home / "auth.json").exists()
        expected_thread = None
        for phase in ("first", "cold-restart"):
            status = await launch()
            assert status["autonomy_paused"]
            terminal = Terminal()
            await terminal.start([*base, "chat"], cwd=root, env=env)
            await terminal.wait_text("gpt-5.6-terra")
            await terminal.wait_text("Ask Codex to do anything")
            draft = f"AliceUnsentDraft-{phase}"
            os.write(terminal.master, draft.encode())
            await terminal.wait_text(draft)
            state = await request(config.control_socket, "status")
            task = state["tasks"]["main"]
            thread_id = task["thread_id"]
            if expected_thread is None:
                expected_thread = thread_id
            assert thread_id == expected_thread
            assert not task["has_input"] and state["autonomy_paused"]
            assert (await request(config.control_socket, "intents"))["intents"] == []
            rpc = await RpcClient.connect_unix(config.codex_socket)
            await rpc.initialize()
            resumed = await rpc.request("thread/resume", {"threadId": thread_id})
            assert resumed["thread"]["id"] == expected_thread
            turns = await rpc.request("thread/turns/list", {"threadId": thread_id, "limit": 100})
            assert turns["data"] == [] and turns.get("nextCursor") is None
            assert not model_requests
            await terminal.quit()
            assert "startup failed" not in terminal.text.lower()
            assert "no rollout found" not in terminal.text.lower()
            await terminal.close()
            terminal = None
            await rpc.close()
            rpc = None
            assert (await cli("stop", timeout=25))["stopped"]
            await asyncio.wait_for(service.wait(), 10)
            assert service.returncode == 0
            persisted = json.loads((home / "state/runtime.json").read_text())
            assert persisted["tasks"]["main"]["thread_id"] == expected_thread
            assert not persisted["tasks"]["main"]["has_input"]
            assert persisted["intents"] == {}
        assert not model_requests
    finally:
        if terminal:
            await terminal.close()
        if rpc:
            await rpc.close()
        if service and service.returncode is None:
            service.terminate()
            try:
                await asyncio.wait_for(service.wait(), 15)
            except TimeoutError:
                os.killpg(service.pid, signal.SIGKILL)
                await service.wait()
        for pid, identity in owned_servers.items():
            try:
                current = process_identity(pid)
            except subprocess.CalledProcessError:
                continue
            if identity and current == identity:
                with suppress(ProcessLookupError):
                    os.killpg(pid, signal.SIGKILL)
        http.close()
        await http.wait_closed()
        for writer in writers:
            writer.close()
        for task in handlers:
            task.cancel()
        await asyncio.gather(*handlers, return_exceptions=True)
