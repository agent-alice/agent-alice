"""Owned-process recovery stays usable when business state cannot be opened.

These tests use a synthetic native transport. One test terminates an actual
fixture-owned process group; none contacts Codex, a model, or production data.
"""

import asyncio
import copy
import json
import os
import signal
import subprocess
import sys
from unittest.mock import AsyncMock, Mock

import pytest

from alice_codex.config import RuntimeConfig
from alice_codex.rpc import RpcError
import alice_codex.service as service_module


class RecoveryRpc:
    """Small public-protocol fixture with observable native stop effects."""

    def __init__(self, *, root_errors=None, resume_errors=None):
        self.root_errors = dict(root_errors or {})
        self.resume_errors = dict(resume_errors or {})
        self.calls = []
        self.listeners = []
        self.closed = False
        self.main_status = "active"
        self.main_goal = "active"
        self.on_close = None

    def add_listener(self, listener):
        self.listeners.append(listener)
        return lambda: self.listeners.remove(listener)

    async def initialize(self, **kwargs):
        self.calls.append(("initialize", kwargs))
        return {}

    async def close(self):
        self.closed = True
        if self.on_close:
            self.on_close()

    async def request(self, method, params):
        self.calls.append((method, copy.deepcopy(params)))
        thread_id = params.get("threadId")
        if method == "thread/goal/get":
            if thread_id in self.root_errors:
                raise self.root_errors[thread_id]
            return {"goal": {"status": self.main_goal}}
        if method == "thread/goal/set":
            assert thread_id == "main-root"
            self.main_goal = params["status"]
            return {"goal": {"status": self.main_goal}}
        if method == "thread/resume":
            if thread_id in self.resume_errors:
                raise self.resume_errors[thread_id]
            self.root_errors.pop(thread_id, None)
            return {"thread": self._thread(thread_id)}
        if method == "thread/list":
            return {
                "data": [] if params.get("archived") else [self._thread("main-root")],
                "nextCursor": None,
            }
        if method == "thread/loaded/list":
            return {"data": ["main-root"], "nextCursor": None}
        if method == "thread/read":
            return {"thread": self._thread(thread_id)}
        if method == "thread/turns/list":
            return {"data": [{"id": "owned-turn", "status": "inProgress"}]}
        if method == "turn/interrupt":
            assert thread_id == "main-root" and params["turnId"] == "owned-turn"
            self.main_status = "idle"
            return {}
        if method == "thread/backgroundTerminals/clean":
            return {}
        if method == "thread/backgroundTerminals/list":
            return {"data": [], "nextCursor": None}
        raise AssertionError(f"Unexpected native operation: {method}")

    def _thread(self, thread_id):
        return {"id": thread_id, "status": {"type": self.main_status}}


@pytest.fixture
def config(tmp_path):
    value = RuntimeConfig(str(tmp_path / "alice"), "/usr/bin/true", "fixture", "unused")
    value.prepare_directories()
    yield value
    for socket in value.socket_dir.iterdir():
        socket.unlink()
    value.socket_dir.rmdir()


@pytest.fixture(autouse=True)
def forbid_business_construction(monkeypatch):
    constructors = {}
    for name in ("Service", "Store", "MemoryStore", "ResourceLedger"):
        constructors[name] = Mock(side_effect=AssertionError(f"Recovery opened {name}"))
        monkeypatch.setattr(service_module, name, constructors[name])
    yield
    for constructor in constructors.values():
        constructor.assert_not_called()


def recorded_state(config, pid, birth, identity):
    return {
        "version": 1,
        "lifecycle": "running",
        "server": {"pid": pid, "birth": birth, "identity": identity},
        "tasks": {"main": {"thread_id": "main-root", "paused": True, "has_input": True}},
        "intents": {"intent": {"thread_id": "main-root", "status": "unknown"}},
    }


def preserve_fixture_data(config, state):
    documents = {
        config.root / "state/runtime.json": json.dumps(state, ensure_ascii=False).encode(),
        config.database: b"damaged scheduling database\x00synthetic records remain",
        config.root / "state/resources.sqlite3": b"damaged resource database\x00keep this",
        config.workspace / "memory/MEMORY.md": b"Synthetic memory must remain byte-for-byte.\n",
    }
    for path, content in documents.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    return {path: path.read_bytes() for path in config.root.rglob("*") if path.is_file()}


def assert_unchanged(config, state, original_state, original_files):
    assert state == original_state
    assert {path: path.read_bytes() for path in config.root.rglob("*") if path.is_file()} == (
        original_files
    )


def mocked_owned_process(monkeypatch, config):
    pid = 812345
    process = {"alive": True, "birth": "fixture birth", "pgid": pid}
    process["identity"] = f"fixture codex app-server --listen unix://{config.codex_socket}"
    signals = []
    monkeypatch.setattr(
        service_module,
        "process_identity",
        lambda _: process["identity"] if process["alive"] else None,
    )
    monkeypatch.setattr(
        service_module, "process_birth", lambda _: process["birth"] if process["alive"] else None
    )
    monkeypatch.setattr(service_module.os, "getpgid", lambda _: process["pgid"])

    def signal_owned(group, sig):
        assert group == pid
        signals.append((group, sig))
        process["alive"] = False

    monkeypatch.setattr(service_module.os, "killpg", signal_owned)
    return pid, process, signals


def install_rpc(monkeypatch, rpc):
    connect = AsyncMock(return_value=rpc)
    monkeypatch.setattr(service_module.RpcClient, "connect_unix", connect)
    return connect


@pytest.mark.parametrize("startup_placeholder", [False, True], ids=["full-identity", "startup-identity"])
async def test_long_argv_owned_process_cleanup_preserves_corrupt_business_databases(
    config, monkeypatch, startup_placeholder
):
    # A narrow caller environment must not hide the socket at the command tail.
    # Recovery must still inspect the full command before signalling this group.
    monkeypatch.setenv("COLUMNS", "80")
    long_argument = "synthetic-padding-" + "x" * 8192
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        "-c",
        "import time; print('ready', flush=True); time.sleep(60)",
        long_argument,
        str(config.codex_socket),
        start_new_session=True,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        env={"PATH": "/usr/bin:/bin", "HOME": str(config.root)},
    )
    try:
        # Prove the fixture has executed Python before inspecting its full argv;
        # process creation alone can race macOS command-line visibility.
        assert await asyncio.wait_for(process.stdout.readline(), 5) == b"ready\n"
        assert os.getpgid(process.pid) == process.pid
        identity = service_module.process_identity(process.pid)
        assert identity is not None
        assert identity.endswith(str(config.codex_socket))
        birth = service_module.process_birth(process.pid)
        assert birth is not None
        # Simulate the startup observation while keeping the real generation.
        # Recovery must inspect the current full command before signalling it.
        state = recorded_state(
            config,
            process.pid,
            birth,
            "(python-startup-placeholder)" if startup_placeholder else identity,
        )
        original_state = copy.deepcopy(state)
        original_files = preserve_fixture_data(config, state)
        rpc = RecoveryRpc()
        install_rpc(monkeypatch, rpc)
        await service_module.recover_owned_server(config, state)
        await asyncio.wait_for(process.wait(), 5)
        assert process.returncode == -signal.SIGTERM
        assert rpc.main_status == "idle" and rpc.main_goal == "paused"
        assert rpc.closed
        assert_unchanged(config, state, original_state, original_files)
    finally:
        if process.returncode is None:
            process.kill()
            await asyncio.wait_for(process.wait(), 5)


async def test_unrecoverable_first_alias_does_not_skip_other_owned_roots(config, monkeypatch):
    pid, process, signals = mocked_owned_process(monkeypatch, config)
    state = recorded_state(config, pid, process["birth"], process["identity"])
    state["tasks"] = {
        "empty": {"thread_id": "empty-root", "paused": True, "has_input": False},
        **state["tasks"],
    }
    original_state = copy.deepcopy(state)
    original_files = preserve_fixture_data(config, state)
    rpc = RecoveryRpc(
        root_errors={"empty-root": RpcError("thread not found: empty-root", -32600)},
        resume_errors={"empty-root": RpcError("no rollout found for thread id empty-root", -32600)},
    )
    install_rpc(monkeypatch, rpc)
    await service_module.recover_owned_server(config, state)
    methods = [(method, params.get("threadId")) for method, params in rpc.calls]
    assert ("thread/resume", "empty-root") in methods
    assert ("turn/interrupt", "main-root") in methods
    assert methods.index(("thread/resume", "empty-root")) < methods.index(
        ("turn/interrupt", "main-root")
    )
    assert signals == [(pid, signal.SIGTERM)]
    assert rpc.closed
    assert_unchanged(config, state, original_state, original_files)


async def test_future_runtime_with_partial_task_damage_still_cleans_valid_roots(
    config, monkeypatch
):
    pid, process, signals = mocked_owned_process(monkeypatch, config)
    state = recorded_state(config, pid, process["birth"], process["identity"])
    state.update(version=999, intents=["unrecognized future representation"])
    state["tasks"].update(
        absent=None,
        missing_id={"paused": True},
        wrong_id_type={"thread_id": ["not-a-string"]},
        blank_id={"thread_id": ""},
    )
    original_state = copy.deepcopy(state)
    original_files = preserve_fixture_data(config, state)
    rpc = RecoveryRpc()
    install_rpc(monkeypatch, rpc)
    await service_module.recover_owned_server(config, state)
    interrupted = [params["threadId"] for method, params in rpc.calls if method == "turn/interrupt"]
    assert interrupted == ["main-root"]
    assert signals == [(pid, signal.SIGTERM)]
    assert_unchanged(config, state, original_state, original_files)


@pytest.mark.parametrize("failure_phase", ["connect", "close"])
async def test_transport_failure_still_cleans_owned_group_without_touching_data(
    config, monkeypatch, failure_phase
):
    pid, process, signals = mocked_owned_process(monkeypatch, config)
    state = recorded_state(config, pid, process["birth"], process["identity"])
    original_state = copy.deepcopy(state)
    original_files = preserve_fixture_data(config, state)
    rpc = RecoveryRpc()
    connect = install_rpc(monkeypatch, rpc)
    if failure_phase == "connect":
        connect.side_effect = OSError("synthetic unavailable native socket")
    else:

        def fail_close():
            raise OSError("synthetic transport close failure")

        rpc.on_close = fail_close
    await service_module.recover_owned_server(config, state)
    assert signals == [(pid, signal.SIGTERM)]
    if failure_phase == "close":
        assert rpc.main_status == "idle" and rpc.closed
    assert_unchanged(config, state, original_state, original_files)


@pytest.mark.parametrize("message", ["thread not loaded: main-root", "thread not found: main-root"])
async def test_exact_unloaded_root_resumes_the_same_id_before_stop(config, monkeypatch, message):
    pid, process, signals = mocked_owned_process(monkeypatch, config)
    state = recorded_state(config, pid, process["birth"], process["identity"])
    rpc = RecoveryRpc(root_errors={"main-root": RpcError(message, -32600)})
    install_rpc(monkeypatch, rpc)
    await service_module.recover_owned_server(config, state)
    resumes = [params["threadId"] for method, params in rpc.calls if method == "thread/resume"]
    assert resumes == ["main-root"]
    assert rpc.main_status == "idle" and rpc.main_goal == "paused"
    assert signals == [(pid, signal.SIGTERM)]


@pytest.mark.parametrize(
    ("message", "code"),
    [("thread not found: unrelated-root", -32600), ("thread not loaded: main-root", -32000)],
)
async def test_other_root_or_error_code_does_not_authorize_resume(
    config, monkeypatch, message, code
):
    pid, process, signals = mocked_owned_process(monkeypatch, config)
    state = recorded_state(config, pid, process["birth"], process["identity"])
    rpc = RecoveryRpc(root_errors={"main-root": RpcError(message, code)})
    install_rpc(monkeypatch, rpc)
    await service_module.recover_owned_server(config, state)
    assert not any(method in {"thread/resume", "thread/start"} for method, _ in rpc.calls)
    assert signals == [(pid, signal.SIGTERM)]


@pytest.mark.parametrize("mismatch", ["birth", "pgid", "socket"])
async def test_recorded_identity_mismatch_never_signals_process(config, monkeypatch, mismatch):
    pid, process, signals = mocked_owned_process(monkeypatch, config)
    state = recorded_state(config, pid, process["birth"], process["identity"])
    original_state = copy.deepcopy(state)
    original_files = preserve_fixture_data(config, state)
    if mismatch == "birth":
        process["birth"] = "another process generation"
    elif mismatch == "pgid":
        process["pgid"] = pid + 1
    else:
        process["identity"] = "another server with a different socket"
    connect = install_rpc(monkeypatch, RecoveryRpc())
    with pytest.raises(RuntimeError, match="identity"):
        await service_module.recover_owned_server(config, state)
    assert signals == []
    connect.assert_not_awaited()
    assert_unchanged(config, state, original_state, original_files)


async def test_process_generation_change_during_native_cleanup_never_signals_replacement(
    config, monkeypatch
):
    pid, process, signals = mocked_owned_process(monkeypatch, config)
    state = recorded_state(config, pid, process["birth"], process["identity"])
    rpc = RecoveryRpc()
    rpc.on_close = lambda: process.update(birth="replacement generation")
    install_rpc(monkeypatch, rpc)
    with pytest.raises(RuntimeError, match="identity"):
        await service_module.recover_owned_server(config, state)
    assert rpc.main_status == "idle" and rpc.closed
    assert signals == []
