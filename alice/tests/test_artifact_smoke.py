"""Black-box acceptance of the actual installed wheel, using no model/key.

All Alice calls are subprocesses of ALICE_ARTIFACT_PYTHON in a temporary cwd.
The fake peer is explicitly a mechanism fixture, not native-Codex validation.
"""

import asyncio
from contextlib import suppress
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import time
import tomllib

import pytest

pytestmark = pytest.mark.artifact


def identity(pid):
    result = subprocess.run(
        ["ps", "-p", str(pid), "-o", "lstart=", "-o", "command="],
        capture_output=True,
        text=True,
        timeout=5,
    )
    return result.stdout.strip()


def alive(pid):
    result = subprocess.run(
        ["ps", "-p", str(pid), "-o", "stat="], capture_output=True, text=True, timeout=5
    )
    return bool(result.stdout.strip()) and not result.stdout.strip().startswith("Z")


def wait_until(predicate, *, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.05)
    raise AssertionError("owned artifact process failed to reach the required state")


@pytest.fixture
def runtime(tmp_path):
    python = os.environ.get("ALICE_ARTIFACT_PYTHON")
    if not python:
        pytest.skip("installed-artifact gate requires ALICE_ARTIFACT_PYTHON")
    python = str(Path(python).absolute())
    assert Path(python).is_file(), "candidate interpreter is missing"
    home = tmp_path / "runtime"
    private_home = tmp_path / "process-home"
    private_home.mkdir()
    env = {
        "PATH": str(Path(python).parent) + os.pathsep + os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": str(private_home),
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONNOUSERSITE": "1",
    }
    fake = tmp_path / "fake_codex.py"
    shutil.copyfile(Path(__file__).parent / "fixtures/fake_codex.py", fake)
    fake.chmod(0o700)
    # Preserve the synthetic distribution layout; this companion deliberately
    # fails if executed and does not substitute for the real Code Mode gate.
    fake_host = tmp_path / "codex-code-mode-host"
    shutil.copyfile(Path(__file__).parent / "fixtures/codex-code-mode-host", fake_host)
    fake_host.chmod(0o700)

    def cli(*args, check=True, timeout=30):
        result = subprocess.run(
            [python, "-I", "-m", "alice_codex", "--home", str(home), *args],
            cwd=tmp_path,
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if check:
            if result.returncode:
                log = home / "logs/alice-service.log"
                detail = log.read_text()[-6000:] if log.is_file() else "No service log was created."
                pytest.fail(
                    f"{args}: {result.stderr}\n{result.stdout}\nOwned service log:\n{detail}"
                )
            return json.loads(result.stdout)
        return result

    cli("init", "--codex", str(fake), "--no-pin")
    packaged = subprocess.run(
        [
            python,
            "-I",
            "-c",
            """import json
from importlib.resources import files
root = files('alice_codex').joinpath('templates/.agents/skills')
names = ('verify-outcome', 'learn-from-correction')
print(json.dumps({name: root.joinpath(name, 'SKILL.md').read_text() for name in names}))
""",
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert packaged.returncode == 0, packaged.stderr
    for name, content in json.loads(packaged.stdout).items():
        assert (
            content
            and (home / "workspace/.agents/skills" / name / "SKILL.md").read_text() == content
        )
    owners = {}

    def remember(status):
        for key in ("pid", "codex_pid"):
            pid = status[key]
            owners[pid] = identity(pid)
        return status

    yield {
        "python": python,
        "env": env,
        "home": home,
        "cwd": tmp_path,
        "cli": cli,
        "remember": remember,
        "owners": owners,
    }
    try:
        cli("stop", check=False, timeout=25)
    except (subprocess.TimeoutExpired, OSError):
        pass
    for pid, expected in owners.items():
        if expected and alive(pid) and identity(pid) == expected:
            # Only groups created by this fixture, rechecked against captured identity.
            with suppress(ProcessLookupError):
                if os.getpgid(pid) == pid:
                    os.killpg(pid, signal.SIGKILL)
    for pid in owners:
        wait_until(lambda pid=pid: not alive(pid))


async def mcp_roundtrip(runtime):
    configured = tomllib.loads(
        (runtime["home"] / "codex/config.toml").read_text()
    )["mcp_servers"]["alice"]
    shadow = runtime["cwd"] / "shadow" / "alice_codex"
    shadow.mkdir(parents=True)
    (shadow / "__init__.py").write_text("raise RuntimeError('untrusted cwd package imported')\n")
    process = await asyncio.create_subprocess_exec(
        configured["command"], *configured["args"],
        cwd=shadow.parent,
        env={**runtime["env"], "PYTHONPATH": str(shadow.parent)},
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    async def send(message):
        process.stdin.write(json.dumps(message).encode() + b"\n")
        await process.stdin.drain()

    async def call(number, method, params=None):
        await send({"jsonrpc": "2.0", "id": number, "method": method, "params": params or {}})
        while True:
            raw = await asyncio.wait_for(process.stdout.readline(), 10)
            assert raw, "installed MCP exited before returning a response"
            message = json.loads(raw)
            if message.get("id") == number:
                assert "error" not in message, message
                return message["result"]

    def structured(result):
        assert not result.get("isError"), result
        if "structuredContent" in result:
            return result["structuredContent"]
        return json.loads(
            "".join(item["text"] for item in result["content"] if item["type"] == "text")
        )

    try:
        initialized = await call(
            1,
            "initialize",
            {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "artifact-acceptance", "version": "1"},
            },
        )
        assert "tools" in initialized["capabilities"]
        await send({"jsonrpc": "2.0", "method": "notifications/initialized"})
        tools = await call(2, "tools/list")
        names = {tool["name"] for tool in tools["tools"]}
        assert {"status", "cron_create", "cron_list", "memory_search", "task_start"} <= names
        created = structured(
            await call(
                3,
                "tools/call",
                {
                    "name": "cron_create",
                    "arguments": {
                        "name": "persisted via installed MCP",
                        "schedule_type": "every",
                        "schedule_value": 3600,
                        "prompt": "fixture only",
                        "target": "main",
                        "enabled": False,
                    },
                },
            )
        )
        jobs = structured(await call(4, "tools/call", {"name": "cron_list", "arguments": {}}))
        assert any(job["id"] == created["id"] and not job["enabled"] for job in jobs["jobs"])
        info = structured(await call(5, "tools/call", {"name": "runtime_info", "arguments": {}}))
        verify_runtime_commands(runtime, info)
        return created["id"]
    finally:
        process.stdin.close()
        try:
            await asyncio.wait_for(process.wait(), 10)
        except TimeoutError:
            process.kill()
            await process.wait()
        assert process.returncode == 0, (await process.stderr.read()).decode()


def verify_runtime_commands(runtime, info):
    """Run MCP-discovered installed commands with no source checkout on sys.path."""
    assert info["environment_source"] == "running_mcp_process"
    assert info["python"] == runtime["python"]
    package = Path(info["package_path"])
    assert package.is_relative_to(Path(runtime["python"]).parent.parent)
    assert Path(info["workspace"]) == runtime["home"] / "workspace"
    shadow = runtime["cwd"] / "shadow" / "alice_codex"
    shadow.mkdir(parents=True, exist_ok=True)
    (shadow / "__init__.py").write_text("raise RuntimeError('untrusted cwd package imported')\n")
    env = {**runtime["env"], "PYTHONPATH": str(shadow.parent)}

    def run(name, *args):
        command = info["commands"][name]
        assert command[:3] == [runtime["python"], "-I", "-m"]
        result = subprocess.run(
            [*command, *map(str, args)], cwd=shadow.parent, env=env,
            capture_output=True, text=True, timeout=20,
        )
        assert result.returncode in (0, 2), result.stderr
        return result.returncode, json.loads(result.stdout)

    draft = runtime["cwd"] / "draft with spaces.md"
    draft.write_text("Public text. <!-- private drafting note -->\n")
    code, dirty = run("check-draft", draft)
    assert code == 2 and dirty["passed"] is False
    draft.write_text("A public sentence.\n")
    code, clean = run("check-draft", draft)
    assert code == 0 and clean["passed"] is True
    inputs = info["learning_inputs"]
    assert set(inputs) == {"tasks", "oracle", "correction"}
    assert all(Path(path).is_file() and Path(path).is_relative_to(package) for path in inputs.values())
    report = runtime["cwd"] / "installed learning report.json"
    code, result = run(
        "evaluate", "--tasks", inputs["tasks"], "--oracle", inputs["oracle"], "--report", report
    )
    assert code == 0, result
    actual = json.loads(report.read_text())
    assert actual["counts"] == {"passed": 20, "failed": 0, "error": 0, "not_run": 0}
    assert actual["consumption"]["model_calls"] == 0


def test_installed_cli_mcp_execution_pause_and_process_restart(runtime):
    cli = runtime["cli"]
    started = runtime["remember"](cli("start"))
    assert started["ready"] and started["autonomy_paused"]
    first = cli(
        "ask", "write first independent evidence", "--request-id", "artifact-first", "--wait", "10"
    )
    assert first["intent"]["status"] == "completed", first
    main_thread = first["intent"]["thread_id"]
    evidence_path = runtime["home"] / "workspace/fixture-evidence.jsonl"
    evidence = [json.loads(line) for line in evidence_path.read_text().splitlines()]
    assert [row["client_id"] for row in evidence] == ["artifact-first"]
    assert evidence[0]["text"] == "write first independent evidence"
    # Durable request identity must prevent another invocation of the scripted tool.
    assert (
        cli("ask", "write first independent evidence", "--request-id", "artifact-first")["status"]
        == "completed"
    )
    assert len(evidence_path.read_text().splitlines()) == 1
    job_id = asyncio.run(mcp_roundtrip(runtime))
    assert cli("pause")["paused"]
    assert cli("stop")["stopped"]
    for key in ("pid", "codex_pid"):
        wait_until(lambda key=key: not alive(started[key]))
    stopped_state = json.loads((runtime["home"] / "state/runtime.json").read_text())
    assert stopped_state["lifecycle"] == "stopped" and stopped_state["server"] is None

    restarted = runtime["remember"](cli("start"))
    assert restarted["pid"] != started["pid"]
    assert restarted["autonomy_paused"]
    assert restarted["tasks"]["main"]["thread_id"] == main_thread
    assert any(job["id"] == job_id for job in cli("cron", "list")["jobs"])
    blocked = cli("ask", "must not run while task paused", check=False)
    assert blocked.returncode != 0 and "paused" in blocked.stderr.lower()
    assert len(evidence_path.read_text().splitlines()) == 1
    cli("resume", "--target", "main")
    second = cli(
        "ask", "evidence after process recovery", "--request-id", "artifact-second", "--wait", "10"
    )
    assert second["intent"]["status"] == "completed", second
    assert second["intent"]["thread_id"] == main_thread
    evidence = [json.loads(line) for line in evidence_path.read_text().splitlines()]
    assert [row["client_id"] for row in evidence] == ["artifact-first", "artifact-second"]
    assert all(row["thread_id"] == main_thread for row in evidence)
    assert cli("pause")["paused"]
    # Abrupt daemon death must leave a recoverable owned App Server, rather than
    # requiring the user to find/kill a stale process or lose the main thread.
    assert identity(restarted["pid"]) == runtime["owners"][restarted["pid"]]
    os.kill(restarted["pid"], signal.SIGKILL)
    wait_until(lambda: not alive(restarted["pid"]))
    assert alive(restarted["codex_pid"]), "fixture must exercise actual orphan cleanup"
    recovered = runtime["remember"](cli("start"))
    assert recovered["pid"] != restarted["pid"] and recovered["codex_pid"] != restarted["codex_pid"]
    wait_until(lambda: not alive(restarted["codex_pid"]))
    assert recovered["autonomy_paused"]
    assert recovered["tasks"]["main"]["thread_id"] == main_thread
    assert recovered["tasks"]["main"]["paused"]
    assert "evidence after process recovery" in json.dumps(cli("task-status", "--target", "main"))
    # An already completed request remains idempotent after an ungraceful restart.
    assert (
        cli("ask", "evidence after process recovery", "--request-id", "artifact-second")["status"]
        == "completed"
    )
    assert len(evidence_path.read_text().splitlines()) == 2
    assert any(job["id"] == job_id for job in cli("cron", "list")["jobs"])
    assert cli("stop")["stopped"]
    for key in ("pid", "codex_pid"):
        wait_until(lambda key=key: not alive(recovered[key]))
