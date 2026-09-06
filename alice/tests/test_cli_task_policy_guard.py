"""CLI entry-point guards over isolated sockets; no Codex or model is invoked."""

import asyncio
from contextlib import asynccontextmanager, suppress
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
from types import SimpleNamespace

import pytest

from alice_codex import cli
from alice_codex.config import RuntimeConfig
from alice_codex.control import request as control_request


POLICY = {
    "max_elapsed_seconds": 120,
    "max_attempts": 4,
    "max_retries": 1,
    "retry_wait_seconds": 5,
    "unchanged_wait_seconds": 30,
}
UNKNOWN = "ValueError: Unknown Alice operation: task_policy_status"
RESERVED = "ValueError: Task policy requires a stable named target, not recurring summary roots"
STALL = object()


class NativeExecObserved(BaseException):
    """An exec replacement cannot return to the remaining CLI command branches."""


@pytest.fixture
def runtime(tmp_path):
    binary = tmp_path / "native-argv-fixture"
    binary.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        "print(json.dumps({'argv': sys.argv[1:], 'codex_home': os.environ['CODEX_HOME']}))\n"
    )
    binary.chmod(0o700)
    config = RuntimeConfig(
        str(tmp_path / "alice"),
        str(binary),
        "synthetic-fixture",
        hashlib.sha256(binary.read_bytes()).hexdigest(),
    )
    config.prepare_directories()
    config.save()
    yield config
    config.control_socket.unlink(missing_ok=True)
    with suppress(OSError):
        config.socket_dir.rmdir()


@pytest.fixture
def exec_spy(monkeypatch):
    observed = []

    def execve(binary, argv, environment):
        observed.append(SimpleNamespace(binary=binary, argv=argv, environment=environment))
        raise NativeExecObserved

    def forbidden_child(*args, **kwargs):
        pytest.fail("the isolated ready control service must not spawn a daemon")

    monkeypatch.setattr(cli.os, "execve", execve)
    monkeypatch.setattr(cli.subprocess, "Popen", forbidden_child)
    return observed


def ok(value):
    return {"ok": True, "result": value}


def error(message):
    return {"ok": False, "error": message}


def no_policy(target):
    return {"target": target, "policy": None, "enforcement_scope": "alice_admission"}


@asynccontextmanager
async def online(config, policy_response, *, on_thread=None):
    calls, handlers = [], set()
    release = asyncio.Event()

    async def handle(reader, writer):
        task = asyncio.current_task()
        handlers.add(task)
        try:
            message = json.loads(await reader.readline())
            calls.append(message)
            action = message["action"]
            if action == "status":
                response = ok({"ready": True})
            elif action == "thread":
                if on_thread is not None:
                    on_thread()
                response = ok({"thread_id": "native-existing-root"})
            elif action == "task_policy_status":
                response = policy_response(message)
            else:
                response = error(f"unexpected fixture action: {action}")
            if response is STALL:
                await release.wait()
            elif response is not None:
                writer.write(json.dumps(response).encode() + b"\n")
                await writer.drain()
        finally:
            writer.close()
            with suppress(ConnectionError):
                await writer.wait_closed()
            handlers.discard(task)

    server = await asyncio.start_unix_server(handle, str(config.control_socket))
    try:
        yield calls
    finally:
        server.close()
        release.set()
        if handlers:
            await asyncio.gather(*list(handlers))
        await server.wait_closed()
        config.control_socket.unlink(missing_ok=True)


async def invoke(config, target="main"):
    try:
        return await asyncio.to_thread(
            cli.main, ["--home", config.home, "chat", "--target", target]
        )
    except NativeExecObserved:
        return 0


async def module_cli(config, target="main"):
    candidate = os.environ.get("ALICE_ARTIFACT_PYTHON")
    python = candidate or sys.executable
    process_home = config.root / "process-home"
    process_home.mkdir(exist_ok=True)
    environment = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": str(process_home),
        "ALICE_HOME": config.home,
        "CODEX_HOME": str(config.codex_home),
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONNOUSERSITE": "1",
    }
    if not candidate:
        environment["PYTHONPATH"] = str(Path(cli.__file__).resolve().parents[1])
    child = await asyncio.create_subprocess_exec(
        python,
        *(["-I"] if candidate else []),
        "-m",
        "alice_codex",
        "--home",
        config.home,
        "chat",
        "--target",
        target,
        cwd=config.root,
        env=environment,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(child.communicate(), 10)
    finally:
        if child.returncode is None:
            child.kill()
            await child.wait()
    return child.returncode, stdout.decode(), stderr.decode()


def assert_denied(result, observed, capsys):
    assert result == 1
    assert observed == []
    message = capsys.readouterr().err
    assert "native chat cannot enforce" in message
    assert "alice ask" in message


def policy_key(target):
    return "task_policy/" + hashlib.sha256(target.encode()).hexdigest()


def seed_settings(config, rows, *, version=1):
    # Deliberately only the fixed-key settings table. A guard must not construct
    # Store, change journal mode, migrate, or reinterpret a business record.
    with sqlite3.connect(config.database) as db:
        db.execute("CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        db.executemany("INSERT INTO settings VALUES (?,?)", list(rows.items()))
        db.execute(f"PRAGMA user_version={version}")
    return config.database.read_bytes()


def assert_database_unchanged(config, before):
    assert config.database.read_bytes() == before
    assert not Path(str(config.database) + "-wal").exists()
    assert not Path(str(config.database) + "-shm").exists()
    assert not Path(str(config.database) + "-journal").exists()


async def test_unmaterialized_config_policy_rejects_named_chat_before_start(
    runtime, exec_spy, capsys
):
    runtime.task_policy = POLICY
    runtime.save()
    async with online(runtime, lambda message: ok(no_policy("worker"))) as calls:
        result = await invoke(runtime, "worker")
    assert_denied(result, exec_spy, capsys)
    assert calls == []
    assert not runtime.database.exists()


@pytest.mark.parametrize("target", ["main", "new", "summary:daily", "scheduled:daily"])
async def test_default_config_policy_does_not_apply_to_excluded_roots(runtime, exec_spy, target):
    runtime.task_policy = POLICY
    runtime.save()
    response = ok(no_policy(target)) if target == "main" else error(RESERVED)
    async with online(runtime, lambda message: response) as calls:
        assert await invoke(runtime, target) == 0
    assert [message["action"] for message in calls] == [
        "status",
        "task_policy_status",
        "thread",
        "task_policy_status",
    ]
    assert len(exec_spy) == 1
    assert not runtime.database.exists()


@pytest.mark.parametrize("target", ["worker", "main", "summary:daily"])
async def test_reported_explicit_policy_rejects_before_native_thread(
    runtime, exec_spy, capsys, target
):
    response = ok({"target": target, "policy": POLICY})
    async with online(runtime, lambda message: response) as calls:
        result = await invoke(runtime, target)
    assert_denied(result, exec_spy, capsys)
    assert [message["action"] for message in calls] == ["status", "task_policy_status"]


async def test_policy_null_with_scope_allows_named_chat(runtime, exec_spy):
    async with online(runtime, lambda message: ok(no_policy("worker"))) as calls:
        assert await invoke(runtime, "worker") == 0
    assert [message["action"] for message in calls] == [
        "status",
        "task_policy_status",
        "thread",
        "task_policy_status",
    ]
    assert [message["params"] for message in calls[1:]] == [{"target": "worker"}] * 3
    assert exec_spy[0].argv[-2:] == ["resume", "native-existing-root"]


@pytest.mark.artifact
@pytest.mark.parametrize("backend", ["supported", "legacy_unknown"])
async def test_module_entry_plain_main_preserves_native_resume_argv(runtime, backend):
    response = ok(no_policy("main")) if backend == "supported" else error(UNKNOWN)
    async with online(runtime, lambda message: response) as calls:
        code, stdout, stderr = await module_cli(runtime)
    assert code == 0, stderr
    assert json.loads(stdout) == {
        "argv": [
            "--remote",
            f"unix://{runtime.codex_socket}",
            "--cd",
            str(runtime.workspace),
            "resume",
            "native-existing-root",
        ],
        "codex_home": str(runtime.codex_home),
    }
    assert [message["action"] for message in calls] == [
        "status",
        "task_policy_status",
        "thread",
        "task_policy_status",
    ]
    assert not runtime.database.exists()


@pytest.mark.artifact
@pytest.mark.parametrize("source", ["configured", "persisted"])
async def test_module_entry_bounded_target_never_executes_native_fixture(runtime, source):
    original = None
    if source == "configured":
        runtime.task_policy = POLICY
        runtime.save()
    else:
        original = seed_settings(runtime, {policy_key("worker"): json.dumps({"policy": POLICY})})
    async with online(runtime, lambda message: error(UNKNOWN)) as calls:
        code, stdout, stderr = await module_cli(runtime, "worker")
    assert code == 1
    assert "native chat cannot enforce" in stderr and "alice ask" in stderr
    assert stdout == ""
    assert calls == []
    if original is not None:
        assert_database_unchanged(runtime, original)
    else:
        assert not runtime.database.exists()


async def test_exact_legacy_unknown_endpoint_keeps_unconfigured_chat_usable(runtime, exec_spy):
    async with online(runtime, lambda message: error(UNKNOWN)) as calls:
        assert await invoke(runtime, "worker") == 0
    assert len(exec_spy) == 1
    assert [message["action"] for message in calls].count("task_policy_status") == 2
    assert not runtime.database.exists()


@pytest.mark.parametrize("target", ["worker", "main", "scheduled:daily"])
async def test_fixed_persisted_policy_key_blocks_legacy_chat_without_writing_sqlite(
    runtime, exec_spy, capsys, target
):
    before = seed_settings(runtime, {policy_key(target): json.dumps({"policy": POLICY})})
    async with online(runtime, lambda message: error(UNKNOWN)) as calls:
        result = await invoke(runtime, target)
    assert_denied(result, exec_spy, capsys)
    assert "thread" not in [message["action"] for message in calls]
    assert_database_unchanged(runtime, before)


@pytest.mark.parametrize("version", [1, 2])
async def test_legacy_lookup_does_not_treat_other_target_or_scope_as_this_policy(
    runtime, exec_spy, version
):
    before = seed_settings(
        runtime,
        {
            policy_key("other"): json.dumps({"policy": POLICY}),
            "task_scope/" + hashlib.sha256(b"worker").hexdigest(): '{"subject":"synthetic"}',
        },
        version=version,
    )
    async with online(runtime, lambda message: error(UNKNOWN)):
        assert await invoke(runtime, "worker") == 0
    assert len(exec_spy) == 1
    assert_database_unchanged(runtime, before)


@pytest.mark.parametrize(
    ("response", "expected_error"),
    [
        (error(UNKNOWN + " (during another operation)"), "during another operation"),
        (error(RESERVED), "requires a stable named target"),
        (error("RuntimeError: policy store unavailable"), "policy store unavailable"),
        (None, "Service disconnected"),
        (ok([]), "invalid service response"),
        (
            ok({"target": "worker", "enforcement_scope": "alice_admission"}),
            "invalid service response",
        ),
        (ok({"target": "different-target", "policy": None}), "invalid service response"),
    ],
    ids=[
        "near-unknown",
        "reserved-error-for-named",
        "rpc-error",
        "disconnect",
        "malformed",
        "missing-policy",
        "wrong-target",
    ],
)
async def test_unknown_or_unbound_policy_status_fails_closed_before_thread(
    runtime, exec_spy, capsys, response, expected_error
):
    async with online(runtime, lambda message: response) as calls:
        result = await invoke(runtime, "worker")
    assert result == 1 and exec_spy == []
    assert expected_error in capsys.readouterr().err
    assert [message["action"] for message in calls] == ["status", "task_policy_status"]


async def test_policy_status_timeout_fails_closed_over_real_control_socket(
    runtime, exec_spy, capsys, monkeypatch
):
    async def short_request(socket, action, params=None, *, timeout=60):
        return await control_request(
            socket, action, params, timeout=0.03 if action == "task_policy_status" else timeout
        )

    monkeypatch.setattr(cli, "request", short_request)
    async with online(runtime, lambda message: STALL) as calls:
        result = await invoke(runtime, "worker")
    assert result == 1 and exec_spy == []
    assert "TimeoutError" in capsys.readouterr().err
    assert [message["action"] for message in calls] == ["status", "task_policy_status"]


async def test_policy_added_during_thread_resolution_blocks_exec(runtime, exec_spy, capsys):
    added = False

    def on_thread():
        nonlocal added
        added = True

    def policy_response(message):
        return ok({"target": "worker", "policy": POLICY if added else None})

    async with online(runtime, policy_response, on_thread=on_thread) as calls:
        result = await invoke(runtime, "worker")
    assert_denied(result, exec_spy, capsys)
    assert [message["action"] for message in calls] == [
        "status",
        "task_policy_status",
        "thread",
        "task_policy_status",
    ]


async def test_legacy_policy_persisted_while_resolving_thread_blocks_exec(
    runtime, exec_spy, capsys
):
    original = None

    def on_thread():
        nonlocal original
        original = seed_settings(runtime, {policy_key("worker"): json.dumps({"policy": POLICY})})

    async with online(runtime, lambda message: error(UNKNOWN), on_thread=on_thread) as calls:
        result = await invoke(runtime, "worker")
    assert_denied(result, exec_spy, capsys)
    assert [message["action"] for message in calls] == ["status", "task_policy_status", "thread"]
    assert_database_unchanged(runtime, original)


async def test_unknown_database_schema_cannot_authorize_legacy_chat(runtime, exec_spy, capsys):
    before = seed_settings(runtime, {}, version=99)
    async with online(runtime, lambda message: error(UNKNOWN)) as calls:
        result = await invoke(runtime, "worker")
    assert result == 1 and exec_spy == []
    assert "unsupported database schema" in capsys.readouterr().err
    assert calls == []
    assert_database_unchanged(runtime, before)
