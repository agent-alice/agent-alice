"""Opt-in real-model transfer acceptance through owned native Codex App Server.

Select explicitly with -m live and ALICE_RUN_LIVE_LEARNING=1. Provision an empty,
private ALICE_LIVE_CODEX_HOME using Codex's login command, then create the marker
.alice-live-test-only containing 'learning-acceptance'. Do not point at personal
Codex or Alice state. This test never reads/copies credential files. Codex uses
the explicitly provisioned home. Set ALICE_LIVE_MODEL to the authorized model.

Five bounded turns: execute a broken fixture, accept one correction, then three
fresh-thread variants. This measures artifact-assisted reuse, not weight updates
or general intelligence. No real website, publisher, messages, or production
memory is involved. No selected required live test silently skips.
"""

import asyncio
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile
import time

import pytest

from alice_codex.codex import CodexClient
from alice_codex.config import RuntimeConfig
from alice_codex.rpc import RpcClient


pytestmark = [pytest.mark.live, pytest.mark.native]


CONTRACT = """This is an isolated, offline observation task. Never access the network,
credentials, other workspaces, tools outside this workspace, or create subagents.
Raw input: pages in retrieval order, each with cursor, next_cursor, and items.
Each item has an id and possibly count. browser_ids are separately observed IDs.
The output is one JSON object with exactly state ('known' or 'unknown'), value
(integer or null), observed_sum (integer), unknown_ids (sorted list), and
browser_only (sorted list). Write results to the requested output file. Store
reusable instructions in .agents/skills/observation-procedure/SKILL.md and a
reusable executable in summarize.py. Use the Python standard library only.
"""

BROKEN = """import json, sys
d = json.load(open(sys.argv[1]))
total = sum(x.get('count', 0) for x in d['pages'][0]['items'])
json.dump(dict(state='known', value=total, observed_sum=total,
               unknown_ids=[], browser_only=[]), open(sys.argv[2], 'w'))
"""


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


async def process_snapshot():
    """Read identities/ancestry only, never command lines or environment values."""
    ps = await asyncio.create_subprocess_exec(
        "ps",
        "-axo",
        "pid=,ppid=,pgid=,stat=,lstart=",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    output, _ = await asyncio.wait_for(ps.communicate(), 5)
    assert ps.returncode == 0, "Unable to verify owned process identities"
    result = {}
    for line in output.decode().splitlines():
        parts = line.split()
        if len(parts) >= 9:
            result[int(parts[0])] = {
                "parent": int(parts[1]),
                "group": int(parts[2]),
                "state": parts[3],
                "started": " ".join(parts[4:]),
            }
    return result


async def test_one_correction_transfers_to_three_fresh_native_threads(tmp_path):
    assert os.environ.get("ALICE_RUN_LIVE_LEARNING") == "1", (
        "Live inference requires explicit selection and authorization"
    )
    model = os.environ.get("ALICE_LIVE_MODEL")
    home_value = os.environ.get("ALICE_LIVE_CODEX_HOME")
    assert model and home_value, (
        "Set an authorized model and isolated, pre-authenticated live test Codex home"
    )
    home = Path(home_value).expanduser().resolve()
    assert home != Path.home() / ".codex" and home.is_dir(), "Do not use personal Codex state"
    marker = home / ".alice-live-test-only"
    assert marker.is_file() and marker.read_text().strip() == "learning-acceptance", (
        "Dedicated fixture home marker required"
    )
    assert not (home / "config.toml").exists(), (
        "Use an authentication-only fixture home; no personal MCP/plugins/config"
    )
    binary = Path(
        os.environ.get(
            "ALICE_TEST_CODEX_BINARY", "/Applications/ChatGPT.app/Contents/Resources/codex"
        )
    )
    assert binary.is_file(), "Required native binary is missing"
    version = subprocess.run(
        [str(binary), "--version"], capture_output=True, text=True, check=True, timeout=10
    ).stdout.strip()
    assert version == "codex-cli 0.153.4", (
        "Revalidate the protocol before changing the pinned native version"
    )
    # Share the production permission contract, without writing any fixture
    # Codex config or reading credentials. Every workspace is owned by this test.
    permissions = RuntimeConfig(
        home=str(tmp_path / "permission-owner"),
        codex_binary=str(binary),
        codex_version=version,
        codex_sha256="0" * 64,
        network_access=False,
    )
    limit = float(os.environ.get("ALICE_LIVE_MAX_SECONDS", "480"))
    assert 30 <= limit <= 900, "Use an explicit bounded wall-clock inference budget"
    deadline = time.monotonic() + limit
    training = tmp_path / "training"
    training.mkdir()
    (training / "AGENTS.md").write_text(CONTRACT)
    (training / "summarize.py").write_text(BROKEN)
    original_hash = digest(training / "summarize.py")
    write_json(
        training / "training.json",
        {
            "pages": [
                {
                    "cursor": None,
                    "next_cursor": None,
                    "items": [{"id": "training-missing"}, {"id": "training-known", "count": 4}],
                }
            ],
            "browser_ids": [],
        },
    )
    process_home = tmp_path / "process-home"
    process_home.mkdir()
    environment = {
        key: os.environ[key]
        for key in (
            "PATH",
            "LANG",
            "SSL_CERT_FILE",
            "SSL_CERT_DIR",
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "NO_PROXY",
            "http_proxy",
            "https_proxy",
            "all_proxy",
            "no_proxy",
        )
        if key in os.environ
    }
    environment.update(HOME=str(process_home), CODEX_HOME=str(home), RUST_LOG="error")
    completed = {}
    record = {
        "native_version": version,
        "model": model,
        "wall_clock_limit_seconds": limit,
        "status": "started",
        "threads": [],
        "variants": [],
        "token_limit_enforced": False,
        "native_token_usage": {},
        "token_usage_state": "not_received",
        "turn_attempts": [],
        "event_evidence": [],
    }
    record_path = tmp_path / "learning-evidence.json"
    write_json(record_path, record)
    rpc = client = process = None
    owned_processes = {}

    async def remember_owned_processes():
        snapshot = await process_snapshot()
        found = {process.pid} if process else set()
        while True:
            expanded = found | {
                pid
                for pid, info in snapshot.items()
                if info["parent"] in found or process and info["group"] == process.pid
            }
            if expanded == found:
                break
            found = expanded
        for pid in found & snapshot.keys():
            owned_processes[pid] = snapshot[pid]["started"]
        return snapshot

    with tempfile.TemporaryDirectory(prefix="alice-live-", dir="/tmp") as sockets:
        socket = Path(sockets) / "codex.sock"
        try:
            process = await asyncio.create_subprocess_exec(
                str(binary),
                "app-server",
                "--listen",
                f"unix://{socket}",
                "-c",
                "features.apps=false",
                "-c",
                "features.plugins=false",
                "-c",
                "features.memories=false",
                "-c",
                "features.multi_agent=false",
                "-c",
                "features.respect_system_proxy=true",
                "-c",
                'cli_auth_credentials_store="file"',
                "-c",
                'web_search="disabled"',
                "-c",
                "sandbox_workspace_write.network_access=false",
                "-c",
                "check_for_update_on_startup=false",
                cwd=training,
                env=environment,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
                start_new_session=True,
            )
            record["owned_server_pid"] = process.pid
            await remember_owned_processes()

            async def ready():
                while not socket.exists():
                    assert process.returncode is None, "Owned live App Server exited before ready"
                    await asyncio.sleep(0.03)

            await asyncio.wait_for(ready(), 15)
            rpc = await RpcClient.connect_unix(socket)
            await rpc.initialize(name="alice_live_learning")

            def retain_completion(event):
                metadata = {"method": event.get("method")}
                params = event.get("params", {})
                if isinstance(params.get("item"), dict):
                    metadata["item_type"] = params["item"].get("type")
                if event.get("method") == "error":
                    info = params.get("error", {}).get("codexErrorInfo")
                    metadata["error_type"] = info if isinstance(info, str) else type(info).__name__
                record["event_evidence"] = [*record["event_evidence"][-63:], metadata]
                if event.get("method") == "turn/completed":
                    params = event["params"]
                    completed[(params["threadId"], params["turn"]["id"])] = params["turn"]["status"]
                elif event.get("method") == "thread/tokenUsage/updated":
                    params = event["params"]
                    usage = params.get("tokenUsage", {})
                    total = usage.get("total", {})
                    record["native_token_usage"][params["threadId"]] = {
                        key: value
                        for key, value in total.items()
                        if type(value) is int and value >= 0
                    }
                    record["token_usage_state"] = "native_notification_received"

            rpc.add_listener(retain_completion)
            client = CodexClient(rpc)

            async def start(workspace):
                # Codex protects .agents by default. The trusted host creates the
                # exact writable skills root; sibling metadata stays protected.
                (workspace / ".agents/skills").mkdir(parents=True, exist_ok=True)
                result = await client.thread_start(
                    cwd=str(workspace),
                    model=model,
                    approvalPolicy="never",
                    developerInstructions=CONTRACT,
                    **permissions.native_permission_params(workspace=workspace),
                )
                assert result["activePermissionProfile"] == {"id": "alice", "extends": ":read-only"}
                identity = result["thread"]["id"]
                record["threads"].append(identity)
                write_json(record_path, record)
                return identity

            async def turn(thread, text):
                remaining = deadline - time.monotonic()
                assert remaining > 0, "Live inference wall-clock budget exhausted"
                attempt = {"thread_id": thread, "status": "started"}
                record["turn_attempts"].append(attempt)
                started = time.monotonic()
                try:
                    # The bound includes turn/start acknowledgement latency too.
                    async with asyncio.timeout(min(120, remaining)):
                        result = await client.turn_start(thread, text)
                        key = thread, result["turn"]["id"]
                        attempt["turn_id"] = key[1]
                        while key not in completed:
                            assert rpc.connected and process.returncode is None, (
                                "Live transport disconnected"
                            )
                            await asyncio.sleep(0.1)
                        status = completed[key]
                        attempt["status"] = status
                        assert status == "completed", (
                            f"Native turn did not complete successfully: {status}"
                        )
                except BaseException as exc:
                    attempt["status"] = type(exc).__name__
                    raise
                finally:
                    attempt["elapsed_seconds"] = round(time.monotonic() - started, 3)
                    await remember_owned_processes()

            training_thread = await start(training)
            await turn(
                training_thread,
                "Run the existing summarize.py with training.json and write before.json. "
                "Do not change the script yet; we are reproducing its current behavior.",
            )
            assert json.loads((training / "before.json").read_text()) == {
                "state": "known",
                "value": 4,
                "observed_sum": 4,
                "unknown_ids": [],
                "browser_only": [],
            }
            await turn(
                training_thread,
                "Correction: this result incorrectly treats missing data as zero. "
                "A missing/null/non-integer/negative count is unknown; numeric zero is real. "
                "Traverse all supplied pages, checking the cursor chain and explicit final null next_cursor. "
                "An unfinished chain leaves the total unknown. Sum known unique item counts as observed_sum; "
                "list IDs with unknown counts. A separately observed browser ID missing from the API also "
                "makes the total unknown; list browser_only. Value is null whenever incomplete, otherwise the sum. "
                "Fix summarize.py, save this reusable procedure as the specified skill, and rerun training.json "
                "into after.json. This is the only correction; do not edit the input fixture or AGENTS.md.",
            )
            assert json.loads((training / "after.json").read_text()) == {
                "state": "unknown",
                "value": None,
                "observed_sum": 4,
                "unknown_ids": ["training-missing"],
                "browser_only": [],
            }
            script = training / "summarize.py"
            skill = training / ".agents/skills/observation-procedure/SKILL.md"
            assert script.is_file() and digest(script) != original_hash
            assert skill.is_file() and 100 <= skill.stat().st_size <= 16000
            record.update(procedure_sha256=digest(skill), script_sha256=digest(script))
            cases = [
                (
                    {
                        "pages": [
                            {
                                "cursor": None,
                                "next_cursor": None,
                                "items": [
                                    {"id": "new-null", "count": None},
                                    {"id": "new-zero", "count": 0},
                                ],
                            }
                        ],
                        "browser_ids": [],
                    },
                    {
                        "state": "unknown",
                        "value": None,
                        "observed_sum": 0,
                        "unknown_ids": ["new-null"],
                        "browser_only": [],
                    },
                ),
                (
                    {
                        "pages": [
                            {
                                "cursor": None,
                                "next_cursor": "p2",
                                "items": [{"id": "new-z1", "count": 0}],
                            },
                            {
                                "cursor": "p2",
                                "next_cursor": None,
                                "items": [{"id": "new-z2", "count": 0}],
                            },
                        ],
                        "browser_ids": ["new-z2"],
                    },
                    {
                        "state": "known",
                        "value": 0,
                        "observed_sum": 0,
                        "unknown_ids": [],
                        "browser_only": [],
                    },
                ),
                (
                    {
                        "pages": [
                            {"cursor": None, "next_cursor": "p2", "items": []},
                            {
                                "cursor": "p2",
                                "next_cursor": None,
                                "items": [{"id": "page-two", "count": 11}],
                            },
                        ],
                        "browser_ids": ["page-two", "web-only"],
                    },
                    {
                        "state": "unknown",
                        "value": None,
                        "observed_sum": 11,
                        "unknown_ids": [],
                        "browser_only": ["web-only"],
                    },
                ),
            ]
            for number, (raw, expected) in enumerate(cases, 1):
                workspace = tmp_path / f"variant-{number}"
                (workspace / skill.relative_to(training).parent).mkdir(parents=True)
                shutil.copy2(script, workspace / "summarize.py")
                shutil.copy2(skill, workspace / skill.relative_to(training))
                (workspace / "AGENTS.md").write_text(CONTRACT)
                write_json(workspace / "input.json", raw)
                input_hash = digest(workspace / "input.json")
                thread = await start(workspace)
                await turn(
                    thread,
                    "Read the workspace's saved observation-procedure skill. "
                    "Use the learned summarize.py on input.json and write result.json. "
                    "Do not change the skill, script, AGENTS.md, or input. Do not consult previous workspaces.",
                )
                assert digest(workspace / "input.json") == input_hash, "Raw fixture was modified"
                assert digest(workspace / "summarize.py") == record["script_sha256"], (
                    "Learned implementation was replaced"
                )
                assert (
                    digest(workspace / skill.relative_to(training)) == record["procedure_sha256"]
                ), "Learned procedure was replaced"
                assert json.loads((workspace / "result.json").read_text()) == expected
                history = (await client.thread_read(thread, include_turns=True))["thread"]["turns"]
                assert len(history) == 1, (
                    "Transfer used a fresh native thread, not correction history"
                )
                commands = [
                    item
                    for row in history
                    for item in row.get("items", [])
                    if item.get("type") == "commandExecution"
                ]
                assert any(
                    "summarize.py" in item.get("command", "")
                    and "input.json" in item.get("command", "")
                    and item.get("exitCode") == 0
                    for item in commands
                ), "Native execution evidence must show the learned script actually ran"
                record["variants"].append(
                    {
                        "thread_id": thread,
                        "input_sha256": input_hash,
                        "output_sha256": digest(workspace / "result.json"),
                        "passed": True,
                        "execution_item_ids": [item["id"] for item in commands],
                    }
                )
                write_json(record_path, record)
            assert len(set(record["threads"])) == 4
            record["status"] = "passed"
        finally:
            behavior_passed = record["status"] == "passed"
            cleanup_errors = []
            record["stopped_roots"] = {}
            if process:
                try:
                    await remember_owned_processes()
                except BaseException as exc:
                    cleanup_errors.append(
                        {"stage": "process_inventory", "error": type(exc).__name__}
                    )
            # A first SIGTERM enters native restart drain; it does not interrupt
            # active turns. Keep the RPC listener alive until stop is verified,
            # so cancellation/token notifications are still recorded.
            if client and rpc and rpc.connected:
                for identity in sorted(client.owned_root_ids):
                    try:
                        record["stopped_roots"][identity] = await client.stop_tree(
                            identity, timeout=15
                        )
                    except BaseException as exc:
                        cleanup_errors.append(
                            {
                                "stage": "stop_tree",
                                "thread_id": identity,
                                "error": type(exc).__name__,
                            }
                        )
                        break  # Do not let an unknown active turn run during later cleanup.
            elif client and client.owned_root_ids:
                cleanup_errors.append({"stage": "stop_tree", "error": "RpcUnavailable"})
            if process:
                # Only the group created above. Never stop another desktop/runtime.
                try:
                    os.killpg(process.pid, signal.SIGKILL if cleanup_errors else signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    await asyncio.wait_for(process.wait(), 10)
                except asyncio.TimeoutError:
                    os.killpg(process.pid, signal.SIGKILL)
                    await process.wait()
                record["owned_server_exit_code"] = process.returncode
                snapshot = await process_snapshot()
                alive = {
                    pid
                    for pid, started in owned_processes.items()
                    if pid in snapshot
                    and snapshot[pid]["started"] == started
                    and not snapshot[pid]["state"].startswith("Z")
                }
                for pid in alive:
                    # A native helper can have its own group. Birth identity and
                    # previously observed ancestry prevent killing unrelated PIDs.
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                end = time.monotonic() + 5
                while alive and time.monotonic() < end:
                    await asyncio.sleep(0.05)
                    snapshot = await process_snapshot()
                    alive = {
                        pid
                        for pid in alive
                        if pid in snapshot
                        and snapshot[pid]["started"] == owned_processes[pid]
                        and not snapshot[pid]["state"].startswith("Z")
                    }
                record["owned_processes"] = sorted(owned_processes)
                record["remaining_owned_processes"] = sorted(alive)
                if alive:
                    cleanup_errors.append(
                        {"stage": "process_exit", "error": "OwnedProcessesRemain"}
                    )
            if client:
                client.close()
            if rpc:
                await rpc.close()
            record["cleanup_errors"] = cleanup_errors
            record["status"] = "passed" if behavior_passed and not cleanup_errors else "failed"
            write_json(record_path, record)
            assert not cleanup_errors, "Owned native cleanup failed; see learning-evidence.json"
