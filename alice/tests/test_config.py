"""Configuration and durable-write regressions, with no real account or runtime."""

import json
import asyncio
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import tomllib

import pytest

from alice_codex.config import RuntimeConfig, UNATTENDED_TOOLS, initialize_config, load_config
from alice_codex.codex import CodexClient
from alice_codex.files import SingletonLock, atomic_write, read_json
from alice_codex.rpc import RpcClient


@pytest.fixture
def binary(tmp_path):
    path = tmp_path / "codex"
    path.write_text('#!/bin/sh\nprintf "codex-cli 0.test\\n"\n')
    path.chmod(0o700)
    # Synthetic distribution files exercise pinning only; native tool
    # execution is verified with a real pair in test_native_code_mode.py.
    host = tmp_path / "codex-code-mode-host"
    host.write_text("#!/bin/sh\nexit 0\n")
    host.chmod(0o700)
    return path


def test_init_pins_binary_and_preserves_personal_home(tmp_path, binary):
    personal = tmp_path / "personal"
    personal.mkdir()
    (personal / "auth.json").write_text('{"synthetic":true}')
    (personal / "config.toml").write_text('model = "personal"\n')
    config = initialize_config(tmp_path / "alice", binary, login_home=personal)
    assert Path(config.codex_binary).read_bytes() == binary.read_bytes()
    assert config.codex_binary != str(binary)
    assert (personal / "config.toml").read_text() == 'model = "personal"\n'
    assert (config.codex_home / "auth.json").is_symlink()
    settings = tomllib.loads((config.codex_home / "config.toml").read_text())
    assert settings["mcp_servers"]["alice"]["args"][-1] == config.home
    assert settings["mcp_servers"]["alice"]["enabled"] is True
    assert settings["mcp_servers"]["alice"]["required"] is True
    assert load_config(config.root) == config
    config.verify_binary()
    assert config.root.stat().st_mode & 0o777 == 0o700


def test_configuration_never_reinitializes_existing_state(tmp_path, binary):
    home = tmp_path / "data"
    initialize_config(home, binary)
    before = (home / "config.json").read_bytes()
    with pytest.raises(ValueError, match="already initialized"):
        initialize_config(home, binary)
    assert (home / "config.json").read_bytes() == before


def test_corrupt_or_future_config_fails_closed(tmp_path, binary):
    config = initialize_config(tmp_path / "data", binary)
    path = config.root / "config.json"
    value = json.loads(path.read_text())
    value["version"] = 999
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="Unsupported"):
        load_config(config.root)
    path.write_text("{invalid")
    with pytest.raises(ValueError, match="original file preserved"):
        load_config(config.root)
    assert path.read_text() == "{invalid"


def test_binary_mismatch_is_not_silently_accepted(tmp_path, binary):
    config = initialize_config(tmp_path / "data", binary)
    Path(config.codex_binary).write_text("modified")
    with pytest.raises(ValueError, match="changed"):
        config.verify_binary()


def test_failed_replace_preserves_old_state(tmp_path, monkeypatch):
    path = tmp_path / "state.json"
    path.write_text('{"jobs":["original"]}')
    original = path.read_bytes()

    def fail(*args):
        raise OSError("simulated rename failure")

    monkeypatch.setattr("alice_codex.files.os.replace", fail)
    with pytest.raises(OSError, match="simulated"):
        atomic_write(path, b'{"jobs":[]}')
    assert path.read_bytes() == original
    assert list(tmp_path.glob("*.tmp")) == []
    assert read_json(path)["jobs"] == ["original"]


def test_singleton_lock_is_lifetime_owned_and_can_recover(tmp_path):
    path = tmp_path / "service.lock"
    first, second = SingletonLock(path), SingletonLock(path)
    with first:
        with pytest.raises(RuntimeError, match="Another Alice"):
            second.acquire()
    with second:
        assert path.exists()


def test_restart_merge_keeps_plugins_mcp_profiles_and_comments(tmp_path, binary):
    config = initialize_config(tmp_path / "data", binary)
    path = config.codex_home / "config.toml"
    text = """# User's extension configuration
model = "old-model" # preserve this model comment
unknown_root = { version = 7, values = ["a", "b"] } # custom root value

[features] # preserve feature comment
local_experiment = true
memories = true

[mcp_servers.alice]
command = "old-python" # preserve server comment
enabled = false # the control interface is required for Alice threads
required = false
extra_future_option = "keep this field"
env = { EXTRA_SETTING = "keep this local setting" }
default_tools_approval_mode = "approve"

[mcp_servers.alice.tools.autonomy_resume]
approval_mode = "approve" # human resume remains explicit
enabled = true

[mcp_servers.alice.tools.future_tool]
approval_mode = "approve"
enabled = false

[mcp_servers.other] # third-party MCP remains intact
url = "https://example.invalid/mcp"
enabled = false
required = false
default_tools_approval_mode = "approve"

[mcp_servers.other.tools.run]
approval_mode = "approve"

[plugins."example@local"]
enabled = true # plugin installation must survive restart

[profiles.focus]
model = "profile-model"
unknown_profile_value = [1, 2, 3]

[future_section]
date = 1979-05-27
value = "unknown setting"
"""
    path.write_text(text)
    before = tomllib.loads(text)
    config.model = "updated-model"
    config.network_access = False
    interpreter = str(tmp_path / 'python with "quotes"')
    config.write_codex_config(python=interpreter)
    rendered = path.read_text()
    result = tomllib.loads(rendered)
    for name in ("unknown_root", "plugins", "profiles", "future_section"):
        assert result[name] == before[name]
    assert result["mcp_servers"]["other"] == before["mcp_servers"]["other"]
    assert result["features"]["local_experiment"] is True
    assert result["features"]["memories"] is False
    assert result["model"] == "updated-model"
    assert result["permissions"]["alice"]["network"]["enabled"] is False
    assert result["default_permissions"] == "alice"
    server = result["mcp_servers"]["alice"]
    assert server["command"] == interpreter
    assert server["enabled"] is True
    assert server["required"] is True
    assert server["args"] == ["-I", "-m", "alice_codex.mcp", "--home", config.home]
    assert server["env"] == before["mcp_servers"]["alice"]["env"]
    assert server["extra_future_option"] == "keep this field"
    assert server["default_tools_approval_mode"] == "prompt"
    assert {
        name for name, value in server["tools"].items() if value["approval_mode"] == "approve"
    } == set(UNATTENDED_TOOLS)
    assert server["tools"]["autonomy_resume"] == {"approval_mode": "prompt", "enabled": True}
    assert server["tools"]["future_tool"] == {"approval_mode": "prompt", "enabled": False}
    for line in text.splitlines():
        if "#" in line:
            assert line[line.index("#") :] in rendered


def test_matching_config_is_not_rewritten_and_inline_tables_remain_valid(
    tmp_path, binary, monkeypatch
):
    config = initialize_config(tmp_path / "data", binary)
    path = config.codex_home / "config.toml"
    path.write_text(
        'features = { custom = true }\nmcp_servers = { alice = { command = "old" }, extra = { command = "custom", args = ["x"] } }\n'
    )
    config.write_codex_config(python="/tmp/fixed-python")
    before = path.read_bytes()
    parsed = tomllib.loads(before.decode())
    assert parsed["mcp_servers"]["extra"] == {"command": "custom", "args": ["x"]}
    assert parsed["features"]["custom"] is True

    def unexpected_write(*args, **kwargs):
        pytest.fail("Matching configuration should not be rewritten")

    monkeypatch.setattr("alice_codex.config.atomic_write", unexpected_write)
    config.write_codex_config(python="/tmp/fixed-python")
    assert path.read_bytes() == before


@pytest.mark.parametrize("invalid", [b'model = "fixture"\n[features\n', b"\xff\xfeinvalid"])
def test_invalid_codex_toml_is_preserved_byte_for_byte(tmp_path, binary, invalid):
    config = initialize_config(tmp_path / "data", binary)
    path = config.codex_home / "config.toml"
    path.write_bytes(invalid)
    with pytest.raises(ValueError, match="original file preserved"):
        config.write_codex_config()
    assert path.read_bytes() == invalid


@pytest.mark.parametrize(
    "content",
    [
        "features = false\n",
        'mcp_servers = "not a table"\n',
        "[mcp_servers]\nalice = 3\n",
        "[mcp_servers.alice]\ntools = []\n",
        "[mcp_servers.alice.tools]\nstatus = true\n",
        '[mcp_servers.alice]\nurl = "https://example.invalid/other-alice"\n',
    ],
)
def test_invalid_managed_shapes_are_not_silently_reset(tmp_path, binary, content):
    config = initialize_config(tmp_path / "data", binary)
    path = config.codex_home / "config.toml"
    path.write_text(content)
    with pytest.raises(ValueError, match="original file preserved"):
        config.write_codex_config()
    assert path.read_text() == content


def test_merge_preserves_a_concurrent_cli_extension_edit(tmp_path, binary, monkeypatch):
    import tomlkit

    config = initialize_config(tmp_path / "data", binary)
    path = config.codex_home / "config.toml"
    original, real_dumps = path.read_text(), tomlkit.dumps
    concurrent = original + '\n[mcp_servers.concurrent]\ncommand = "newly-added-tool"\n'

    def concurrent_add(document):
        rendered = real_dumps(document)
        path.write_text(concurrent)
        return rendered

    monkeypatch.setattr("alice_codex.config.tomlkit.dumps", concurrent_add)
    config.model = "new-model"
    with pytest.raises(ValueError, match="concurrent edit preserved"):
        config.write_codex_config()
    assert path.read_text() == concurrent


def test_managed_config_symlink_cannot_replace_another_home(tmp_path, binary):
    config = initialize_config(tmp_path / "data", binary)
    path = config.codex_home / "config.toml"
    personal = tmp_path / "personal.toml"
    personal.write_text('model = "personal"\n')
    path.unlink()
    path.symlink_to(personal)
    with pytest.raises(ValueError, match="symlink"):
        config.write_codex_config()
    assert path.is_symlink()
    assert personal.read_text() == 'model = "personal"\n'


def test_native_permissions_migrate_owned_keys_and_preserve_other_profiles(tmp_path, binary):
    config = initialize_config(tmp_path / "data", binary)
    path = config.codex_home / "config.toml"
    path.write_text("""sandbox_mode = "workspace-write"
default_permissions = "custom"
[sandbox_workspace_write]
network_access = true
writable_roots = ["/operator-selected"] # retain for explicitly selected legacy policy
exclude_tmpdir_env_var = true
[permissions.custom]
extends = ":read-only"
description = "Operator profile"
[permissions.custom.filesystem]
"/operator-selected" = "write"
""")
    old = tomllib.loads(path.read_text())
    config.write_codex_config()
    updated = tomllib.loads(path.read_text())
    assert "sandbox_mode" not in updated
    assert updated["sandbox_workspace_write"] == {
        "writable_roots": ["/operator-selected"],
        "exclude_tmpdir_env_var": True,
    }
    assert updated["permissions"]["custom"] == old["permissions"]["custom"]
    assert (
        updated["permissions"]["alice"]
        == config.native_permission_params()["config"]["permissions.alice"]
    )
    assert updated["default_permissions"] == "alice"
    assert (config.workspace / ".agents/skills").is_dir()


def test_read_only_switch_removes_previously_managed_write_roots(tmp_path, binary):
    config = initialize_config(tmp_path / "data", binary)
    assert (
        config.native_permission_params()["config"]["permissions.alice"]["filesystem"][
            str(config.workspace)
        ]
        == "write"
    )
    config.sandbox = "read-only"
    config.network_access = False
    config.write_codex_config()
    expected = {"extends": ":read-only", "filesystem": {"/": "read"}, "network": {"enabled": False}}
    assert config.native_permission_params() == {
        "permissions": "alice",
        "config": {"permissions.alice": expected},
    }
    actual = tomllib.loads((config.codex_home / "config.toml").read_text())
    assert actual["permissions"]["alice"] == expected


@pytest.mark.parametrize(
    "custom",
    [
        '[permissions.alice]\nextends=":workspace"\n',
        '[permissions.alice.filesystem]\n"/unmanaged"="write"\n',
        '[permissions.alice.workspace_roots]\n"/unmanaged"=true\n',
        '[permissions.alice.network]\nproxy_url="https://operator.invalid"\n',
    ],
)
def test_conflicting_alice_profile_is_preserved_instead_of_overwritten(tmp_path, binary, custom):
    config = initialize_config(tmp_path / "data", binary)
    path = config.codex_home / "config.toml"
    path.write_text(custom)
    with pytest.raises(ValueError, match="Conflicting permissions.alice"):
        config.write_codex_config()
    assert path.read_text() == custom


def test_skill_symlink_cannot_expand_native_write_permissions(tmp_path, binary):
    config = initialize_config(tmp_path / "data", binary)
    skills = config.workspace / ".agents/skills"
    skills.rmdir()
    outside = tmp_path / "not-a-skill-directory"
    outside.mkdir()
    skills.symlink_to(outside)
    with pytest.raises(ValueError, match="symbolic links"):
        config.native_permission_params()
    with pytest.raises(ValueError, match="symbolic links"):
        config.prepare_directories()


def native_config(tmp_path):
    binary = Path(
        os.environ.get(
            "ALICE_TEST_CODEX_BINARY", "/Applications/ChatGPT.app/Contents/Resources/codex"
        )
    )
    assert binary.is_file(), "Required native Codex binary unavailable"
    version = subprocess.run(
        [str(binary), "--version"], capture_output=True, text=True, check=True, timeout=10
    ).stdout.strip()
    assert version == "codex-cli 0.153.4", (
        "Revalidate the permission contract for a new native version"
    )
    config = RuntimeConfig(
        home=str(tmp_path / "runtime"),
        codex_binary=str(binary),
        codex_version=version,
        codex_sha256="0" * 64,
        network_access=False,
    )
    config.prepare_directories()
    config.save()
    config.write_codex_config()
    process_home = tmp_path / "process-home"
    process_home.mkdir()
    env = {
        "HOME": str(process_home),
        "CODEX_HOME": str(config.codex_home),
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "RUST_LOG": "error",
        "NO_PROXY": "127.0.0.1,localhost,::1",
        "no_proxy": "127.0.0.1,localhost,::1",
    }
    return config, env


@pytest.mark.native
def test_native_generated_profile_allows_skills_but_protects_metadata_and_release(tmp_path):
    config, env = native_config(tmp_path)
    targets = {
        "ordinary": config.workspace / "result.json",
        "skill": config.workspace / ".agents/skills/new/SKILL.md",
        "other_metadata": config.workspace / ".agents/settings.txt",
        "codex_metadata": config.workspace / ".codex/config.toml",
        "git_metadata": config.workspace / ".git/config",
        "release": config.root / "releases/stable.py",
        "credential_marker": config.codex_home / "protected-sentinel.txt",
    }
    for key, path in targets.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            "# synthetic protected fixture\n" if key == "codex_metadata" else "original"
        )
    probe = """import json,sys
from pathlib import Path
result={}
for key,value in json.loads(sys.argv[1]).items():
 try:
  Path(value).write_text('attempted write')
  result[key]=True
 except OSError as error:
  if error.errno not in (1,13,30): raise
  result[key]=False
print(json.dumps(result))
"""
    # The native CLI loads the same default_permissions used by the TUI.
    parsed = subprocess.run(
        [config.codex_binary, "features", "list"],
        env=env,
        cwd=config.workspace,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert parsed.returncode == 0, "Generated CLI/TUI configuration failed native parsing"
    command = [
        config.codex_binary,
        "sandbox",
        "-C",
        str(config.workspace),
        "-P",
        "alice",
        "--",
        sys.executable,
        "-c",
        probe,
        json.dumps({key: str(path) for key, path in targets.items()}),
    ]
    result = subprocess.run(
        command, env=env, capture_output=True, text=True, check=True, timeout=20
    )
    assert json.loads(result.stdout) == {key: key in {"ordinary", "skill"} for key in targets}
    config.sandbox = "read-only"
    config.write_codex_config()
    result = subprocess.run(
        command, env=env, capture_output=True, text=True, check=True, timeout=20
    )
    assert json.loads(result.stdout) == dict.fromkeys(targets, False)


@pytest.mark.native
async def test_native_start_and_resume_use_the_generated_alice_profile(tmp_path):
    config, env = native_config(tmp_path)
    request_paths = []

    async def recorded_response(reader, writer):
        try:
            header = await reader.readuntil(b"\r\n\r\n")
            request_paths.append(header.split(b"\r\n", 1)[0].decode())
            length = next(
                (
                    int(line.split(b":", 1)[1])
                    for line in header.split(b"\r\n")
                    if line.lower().startswith(b"content-length:")
                ),
                0,
            )
            await reader.readexactly(length)
            events = [
                {"type": "response.created", "response": {"id": "permission-fixture"}},
                {
                    "type": "response.output_item.done",
                    "item": {
                        "type": "message",
                        "id": "permission-result",
                        "role": "assistant",
                        "content": [
                            {"type": "output_text", "text": "Recorded permission fixture."}
                        ],
                    },
                },
                {
                    "type": "response.completed",
                    "response": {
                        "id": "permission-fixture",
                        "usage": {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
                    },
                },
            ]
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n"
            )
            for event in events:
                writer.write(("data: " + json.dumps(event) + "\n\n").encode())
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    http = await asyncio.start_server(recorded_response, "127.0.0.1", 0)
    port = http.sockets[0].getsockname()[1]
    # Preserve harmless legacy customization while the native named profile is
    # active. This checks parsing behavior, not merely tomllib syntax.
    path = config.codex_home / "config.toml"
    path.write_text(
        path.read_text() + '\n[sandbox_workspace_write]\nwritable_roots=["/operator-selected"]\n'
    )
    config.write_codex_config()
    with tempfile.TemporaryDirectory(prefix="alice-config-", dir="/tmp") as sockets:
        socket = Path(sockets) / "rpc.sock"
        process = await asyncio.create_subprocess_exec(
            config.codex_binary,
            "app-server",
            "--listen",
            f"unix://{socket}",
            "-c",
            "mcp_servers.alice.enabled=false",
            "-c",
            "mcp_servers.alice.required=false",
            "-c",
            "features.apps=false",
            "-c",
            "features.plugins=false",
            "-c",
            "features.respect_system_proxy=false",
            "-c",
            'model_provider="recorded"',
            "-c",
            'model_providers.recorded.name="keyless fixture"',
            "-c",
            f'model_providers.recorded.base_url="http://127.0.0.1:{port}"',
            "-c",
            'model_providers.recorded.wire_api="responses"',
            "-c",
            "model_providers.recorded.supports_websockets=false",
            "-c",
            "model_providers.recorded.request_max_retries=0",
            "-c",
            "model_providers.recorded.stream_max_retries=0",
            "-c",
            "model_providers.recorded.requires_openai_auth=false",
            env=env,
            cwd=config.workspace,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        rpc = client = None
        try:

            async def ready():
                while not socket.exists():
                    assert process.returncode is None, "Owned native App Server exited before ready"
                    await asyncio.sleep(0.02)

            await asyncio.wait_for(ready(), 10)
            rpc = await RpcClient.connect_unix(socket)
            await rpc.initialize()
            client = CodexClient(rpc)
            # Empty threads have no durable rollout. A localhost recorded turn
            # creates one, without model inference, credentials or external IO.
            result = await client.thread_start(
                cwd=str(config.workspace), model="gpt-5.4", modelProvider="recorded"
            )
            assert result["activePermissionProfile"] == {"id": "alice", "extends": ":read-only"}
            identity = result["thread"]["id"]
            turn = await client.turn_start(identity, "Record an isolated permission fixture.")
            event = await rpc.wait_event(
                lambda event: (
                    event.get("method") == "turn/completed"
                    and event["params"]["threadId"] == identity
                    and event["params"]["turn"]["id"] == turn["turn"]["id"]
                ),
                timeout=15,
            )
            assert event["params"]["turn"]["status"] == "completed", event["params"]["turn"].get(
                "error"
            )
            resumed = await client.thread_resume(identity, **config.native_permission_params())
            assert resumed["thread"]["id"] == identity
            assert resumed["activePermissionProfile"] == result["activePermissionProfile"]
            assert resumed["thread"]["status"]["type"] == "idle"
            assert request_paths == ["POST /responses HTTP/1.1"]
        finally:
            try:
                if client:
                    for identity in sorted(client.owned_root_ids):
                        await client.stop_tree(identity, timeout=5)
            finally:
                if client:
                    client.close()
                if rpc:
                    await rpc.close()
                if process.returncode is None:
                    process.terminate()
                    try:
                        await asyncio.wait_for(process.wait(), 8)
                    except asyncio.TimeoutError:
                        process.kill()
                        await process.wait()
                http.close()
                await http.wait_closed()
            assert process.returncode == 0


def synthetic_identity_listing(config, python):
    from alice_codex.identity import HOOK_STATUS

    hooks = []
    for event, groups in config.identity_hooks(python=python).items():
        group, handler = groups[0], groups[0]["hooks"][0]
        hooks.append(
            {
                "key": str(config.codex_home / "config.toml") + ":" + event,
                "eventName": event[0].lower() + event[1:],
                "handlerType": "command",
                "command": handler["command"],
                "async": False,
                "matcher": group.get("matcher"),
                "timeoutSec": 10,
                "statusMessage": HOOK_STATUS,
                "additionalContextLimit": handler.get("additionalContextLimit"),
                "source": "user",
                "sourcePath": str(config.codex_home / "config.toml"),
                "enabled": True,
                "currentHash": "sha256:" + "a" * 64,
                "trustStatus": "untrusted",
            }
        )
    return {"data": [{"cwd": str(config.workspace), "warnings": [], "errors": [], "hooks": hooks}]}


def test_identity_hooks_rebind_candidate_and_preserve_extensions(tmp_path, binary):
    config = initialize_config(tmp_path / "data", binary)
    path = config.codex_home / "config.toml"
    path.write_text(
        "# Keep this custom extension\n[[hooks.UserPromptSubmit]]\n"
        '[[hooks.UserPromptSubmit.hooks]]\ntype="command"\ncommand="custom-check"\n'
    )
    original_config = (config.root / "config.json").read_bytes()
    config.write_codex_config(python="/fixture/candidate-a/python")
    listing = synthetic_identity_listing(config, "/fixture/candidate-a/python")
    config.trust_identity_hooks(listing, python="/fixture/candidate-a/python")
    config.write_codex_config(python="/fixture/candidate-b/python")
    body = path.read_text()
    assert "Keep this custom extension" in body and "custom-check" in body
    assert "/fixture/candidate-a/python" not in body
    assert body.count("/fixture/candidate-b/python -I -m alice_codex.identity") == 3
    assert (config.root / "config.json").read_bytes() == original_config
    before = path.read_bytes()
    with pytest.raises(ValueError, match="changed after listing"):
        config.trust_identity_hooks(listing, python="/fixture/candidate-a/python")
    assert path.read_bytes() == before


def test_direct_serve_identity_refresh_preserves_broken_mcp_and_extensions(tmp_path, binary):
    import tomlkit

    from alice_codex.identity import build_identity_bundle
    from alice_codex.memory import MemoryStore

    config = initialize_config(tmp_path / "data", binary)
    MemoryStore(config.root).install_workspace_templates()
    identity = build_identity_bundle(config.workspace)
    path = config.codex_home / "config.toml"
    document = tomlkit.parse(path.read_text())
    document["model"] = "operator-model"
    document["features"]["memories"] = True
    document["mcp_servers"]["alice"]["command"] = "/usr/bin/false"
    document["mcp_servers"]["alice"]["args"] = []
    document["mcp_servers"]["other"] = {"command": "operator-command", "enabled": False}
    document["plugins"] = {"synthetic@local": {"enabled": True}}
    path.write_text(
        "# Operator configuration must survive direct serve\n" + tomlkit.dumps(document)
    )
    before = tomllib.loads(path.read_text())

    config.write_identity_config(identity, python="/fixture/new-candidate/python")

    after = tomllib.loads(path.read_text())
    for key, value in before.items():
        if key not in {"developer_instructions", "features", "hooks"}:
            assert after[key] == value
    assert after["features"] == {**before["features"], "hooks": True}
    assert after["developer_instructions"] == identity.developer_instructions
    assert path.read_text().count("/fixture/new-candidate/python -I -m alice_codex.identity") == 3
    assert "# Operator configuration must survive direct serve" in path.read_text()
    unchanged = path.read_bytes()
    config.write_identity_config(identity, python="/fixture/new-candidate/python")
    assert path.read_bytes() == unchanged


@pytest.mark.parametrize("contents", ["invalid = [", 'developer_instructions = "operator rules"\n'])
def test_identity_refresh_never_resets_conflicting_configuration(tmp_path, binary, contents):
    from alice_codex.identity import build_identity_bundle
    from alice_codex.memory import MemoryStore

    config = initialize_config(tmp_path / "data", binary)
    MemoryStore(config.root).install_workspace_templates()
    identity = build_identity_bundle(config.workspace)
    path = config.codex_home / "config.toml"
    path.write_text(contents)
    before = path.read_bytes()
    with pytest.raises(ValueError, match="original file preserved|original preserved"):
        config.write_identity_config(identity)
    assert path.read_bytes() == before


@pytest.mark.parametrize(
    "field,value",
    [
        ("source", "project"),
        ("eventName", "preToolUse"),
        ("command", "unowned-command"),
        ("enabled", False),
    ],
)
def test_identity_trust_never_authorizes_other_handlers(tmp_path, binary, field, value):
    config = initialize_config(tmp_path / "data", binary)
    listing = synthetic_identity_listing(config, sys.executable)
    listing["data"][0]["hooks"][0][field] = value
    path = config.codex_home / "config.toml"
    before = path.read_bytes()
    with pytest.raises(ValueError):
        config.trust_identity_hooks(listing)
    assert path.read_bytes() == before
