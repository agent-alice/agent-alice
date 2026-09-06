"""Alice's explicit runtime configuration, pinned Codex and private state layout."""

from collections.abc import MutableMapping
from dataclasses import asdict, dataclass, fields
import hashlib
import math
import os
from pathlib import Path
import subprocess
import sys
import tomllib

import tomlkit
from tomlkit.exceptions import ParseError
from tomlkit.items import InlineTable

from .files import SingletonLock, atomic_write, private_dir, read_json, sha256_file, write_json
from .runtime_bundle import inspect_bundle, pin_bundle, verify_runtime_bundle
from .identity import (
    IDENTITY_HEADER,
    IdentityBundle,
    identity_hook_groups,
    is_owned_identity_handler,
    validate_identity_hooks,
)


CONFIG_VERSION = 1

# Only these reviewed, Alice-owned local operations can run unattended. New MCP
# tools keep Codex's approval default; explicit user resume remains a CLI action.
UNATTENDED_TOOLS = (
    "status",
    "cron_create",
    "cron_list",
    "cron_update",
    "cron_delete",
    "task_start",
    "task_status",
    "autonomy_pause",
    "memory_search",
    "memory_read",
    "memory_prepare_summary",
    "memory_commit_summary",
    "resources_status",
    "resources_record_observation",
)


def _table(document: MutableMapping, *keys: str) -> MutableMapping:
    """Update a managed table without replacing its unknown fields or trivia."""
    current = document
    for key in keys:
        if key not in current:
            current[key] = (
                tomlkit.inline_table() if isinstance(current, InlineTable) else tomlkit.table()
            )
        current = current[key]
        if not isinstance(current, MutableMapping):
            raise ValueError(
                "Codex managed configuration requires a table; original file preserved"
            )
    return current


def default_home() -> Path:
    return Path(os.environ.get("ALICE_HOME", "~/.local/share/alice")).expanduser().resolve()


@dataclass
class RuntimeConfig:
    home: str
    codex_binary: str
    codex_version: str
    codex_sha256: str
    source_root: str | None = None
    model: str = "gpt-6-astra"
    reasoning_effort: str = "high"
    timezone: str = "Asia/Shanghai"
    poll_seconds: float = 5.0
    max_active_tasks: int = 2
    sandbox: str = "workspace-write"
    network_access: bool = True
    task_policy: dict | None = None
    version: int = CONFIG_VERSION

    @property
    def root(self) -> Path:
        return Path(self.home)

    @property
    def codex_home(self) -> Path:
        return self.root / "codex"

    @property
    def workspace(self) -> Path:
        return self.root / "workspace"

    @property
    def socket_dir(self) -> Path:
        # macOS sockaddr_un is too short for many worktree/temp directory paths.
        digest = hashlib.sha256(self.home.encode()).hexdigest()[:16]
        return Path("/tmp") / f"alice-{os.getuid()}-{digest}"

    @property
    def control_socket(self) -> Path:
        return self.socket_dir / "alice.sock"

    @property
    def codex_socket(self) -> Path:
        return self.socket_dir / "codex.sock"

    @property
    def database(self) -> Path:
        return self.root / "state" / "schedules.sqlite3"

    def prepare_directories(self) -> None:
        for path in (
            self.root,
            self.codex_home,
            self.workspace,
            self.root / "logs",
            self.root / "state",
            self.root / "releases",
        ):
            if path.is_symlink():
                raise ValueError("Alice runtime directories must not be symbolic links")
            private_dir(path)
        for path in (self.workspace / ".agents", self.workspace / ".agents" / "skills"):
            if path.is_symlink():
                raise ValueError("Alice skill directories must not be symbolic links")
            private_dir(path)
        if self.socket_dir.is_symlink():
            raise ValueError("Runtime socket directory must not be a symbolic link")
        if self.socket_dir.exists() and self.socket_dir.stat().st_uid != os.getuid():
            raise ValueError("Runtime socket directory belongs to another user")
        private_dir(self.socket_dir)

    def save(self) -> None:
        self.validate()
        write_json(self.root / "config.json", asdict(self))

    def validate(self) -> None:
        from zoneinfo import ZoneInfo

        if self.version != CONFIG_VERSION:
            raise ValueError(f"Unsupported Alice config version {self.version}")
        if not Path(self.home).is_absolute() or not Path(self.codex_binary).is_absolute():
            raise ValueError("Alice home and Codex binary must be absolute paths")
        if (
            type(self.poll_seconds) not in (float, int)
            or not math.isfinite(self.poll_seconds)
            or self.poll_seconds <= 0
            or type(self.max_active_tasks) is not int
            or self.max_active_tasks < 1
        ):
            raise ValueError("Polling interval and task limit must be positive")
        if self.sandbox not in {"read-only", "workspace-write"}:
            raise ValueError("Use a bounded read-only or workspace-write Alice runtime")
        if type(self.network_access) is not bool:
            raise ValueError("network_access must be an explicit boolean")
        if self.task_policy is not None:
            from .resources import TaskPolicy

            if not isinstance(self.task_policy, dict):
                raise ValueError("task_policy must be an explicit object or null")
            try:
                TaskPolicy(**self.task_policy)
            except (TypeError, OverflowError) as error:
                raise ValueError("task_policy requires exactly five valid limits") from error
        ZoneInfo(self.timezone)

    def verify_binary(self) -> None:
        binary = Path(self.codex_binary)
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise ValueError("Pinned Codex executable is missing or not executable")
        if sha256_file(binary) != self.codex_sha256:
            raise ValueError("Codex executable changed; validate a new version before using it")
        # Legacy configuration remains readable and usable by earlier Alice
        # releases. New installations record a pair; never ignore its drift.
        verify_runtime_bundle(self)

    def environment(self) -> dict[str, str]:
        env = os.environ.copy()
        env["CODEX_HOME"] = str(self.codex_home)
        env["ALICE_HOME"] = self.home
        return env

    def native_permission_params(self, *, workspace: Path | None = None) -> dict:
        """Native thread/start/resume arguments, without a legacy sandbox override.

        An optional workspace is for a separately host-owned task/test directory.
        The caller must not supply model-selected or unowned paths here. The
        profile retains normal filesystem read access and grants writes only to
        the declared workspace and its skills; other metadata stays protected.
        """
        self.validate()
        workspace = Path(workspace) if workspace is not None else self.workspace
        if not workspace.is_absolute():
            raise ValueError("Native permissions require an absolute workspace")
        if any(
            path.is_symlink()
            for path in (workspace, workspace / ".agents", workspace / ".agents/skills")
        ):
            raise ValueError("Alice permission roots must not be symbolic links")
        workspace = workspace.resolve()
        filesystem = {"/": "read"}
        if self.sandbox == "workspace-write":
            filesystem[str(workspace)] = "write"
            filesystem[str(workspace / ".agents/skills")] = "write"
        profile = {
            "extends": ":read-only",
            "filesystem": filesystem,
            "network": {"enabled": self.network_access},
        }
        return {"permissions": "alice", "config": {"permissions.alice": profile}}

    def write_codex_config(
        self, *, python: str | None = None, identity: IdentityBundle | None = None
    ) -> None:
        """Merge owned settings, preserving CLI-installed plugins/MCP and comments.

        Invalid or concurrently edited input is never reset to defaults. Only
        Alice's own MCP namespace has its unattended approval policy reconciled.
        """
        self.validate()
        python = python or sys.executable
        path = self.codex_home / "config.toml"
        if self.codex_home.is_symlink() or path.is_symlink():
            raise ValueError("Managed Codex configuration cannot be a symlink; original preserved")
        original = path.read_bytes() if path.exists() else None
        try:
            document = (
                tomlkit.parse(original.decode("utf-8"))
                if original is not None
                else tomlkit.document()
            )
        except (UnicodeError, ParseError) as exc:
            raise ValueError("Invalid Codex TOML; original file preserved") from exc
        if original is None:
            document.add(
                tomlkit.comment(
                    "Alice-owned settings; local extensions and comments are preserved."
                )
            )
        for key, value in {
            "model": self.model,
            "model_reasoning_effort": self.reasoning_effort,
            "approval_policy": "on-request",
            "default_permissions": "alice",
            "cli_auth_credentials_store": "file",
            "web_search": "live",
        }.items():
            document[key] = value
        if identity is not None:
            previous = document.get("developer_instructions")
            if previous is not None and not str(previous).startswith(IDENTITY_HEADER):
                raise ValueError(
                    "Custom developer instructions conflict with managed identity; original preserved"
                )
            document["developer_instructions"] = identity.developer_instructions
        # Migrate only settings managed by the former Alice generator. Keep
        # additional legacy fields and other named profiles for explicit use;
        # native default_permissions selects the Alice profile for this runtime.
        document.pop("sandbox_mode", None)
        if "sandbox_workspace_write" in document:
            legacy = _table(document, "sandbox_workspace_write")
            legacy.pop("network_access", None)
            if not legacy:
                document.pop("sandbox_workspace_write")
        profile = self.native_permission_params()["config"]["permissions.alice"]
        managed = _table(document, "permissions", "alice")
        if "extends" in managed and managed["extends"] != ":read-only":
            raise ValueError(
                "Conflicting permissions.alice inheritance; move custom rules to another profile; original file preserved"
            )
        managed["extends"] = profile["extends"]
        filesystem = _table(managed, "filesystem")
        managed_paths = {
            "/",
            str(self.workspace.resolve()),
            str(self.workspace.resolve() / ".agents/skills"),
        }
        if set(filesystem) - managed_paths or "workspace_roots" in managed:
            raise ValueError(
                "Conflicting permissions.alice filesystem roots; move custom rules to another profile; original file preserved"
            )
        for key in list(filesystem):
            if key not in profile["filesystem"]:
                filesystem.pop(key)
        for key, value in profile["filesystem"].items():
            filesystem[key] = value
        network = _table(managed, "network")
        if set(network) - {"enabled"}:
            raise ValueError(
                "Conflicting permissions.alice network rules; move custom rules to another profile; original file preserved"
            )
        network["enabled"] = self.network_access
        features = _table(document, "features")
        for key, value in {
            "goals": True,
            "multi_agent_v2": True,
            "memories": False,
            "respect_system_proxy": True,
        }.items():
            features[key] = value
        features["hooks"] = True
        hooks = _table(document, "hooks")
        for event, groups in self.identity_hooks(python=python).items():
            current_groups = hooks.get(event, [])
            if not isinstance(current_groups, list):
                raise ValueError("Invalid hook configuration; original file preserved")
            retained = []
            for group in current_groups:
                if not isinstance(group, MutableMapping) or not isinstance(
                    group.get("hooks"), list
                ):
                    raise ValueError("Invalid hook group; original file preserved")
                handlers = [
                    handler for handler in group["hooks"] if not is_owned_identity_handler(handler)
                ]
                if handlers:
                    if len(handlers) != len(group["hooks"]):
                        group["hooks"] = handlers
                    retained.append(group)
            if current_groups != retained + groups:
                hooks[event] = retained + groups
        server = _table(document, "mcp_servers", "alice")
        if "url" in server:
            raise ValueError(
                "Alice MCP must use its managed stdio transport; original file preserved"
            )
        for key, value in {
            "command": python,
            "args": ["-m", "alice_codex.mcp", "--home", self.home],
            "enabled": True,
            "required": True,
            "startup_timeout_sec": 15,
            "tool_timeout_sec": 120,
            "default_tools_approval_mode": "prompt",
        }.items():
            server[key] = value
        tools = _table(server, "tools")
        for name in UNATTENDED_TOOLS:
            _table(tools, name)["approval_mode"] = "approve"
        for name in list(tools):
            if name not in UNATTENDED_TOOLS:
                # In particular, an earlier broad rule must not let a model
                # call autonomy_resume without explicit human approval.
                _table(tools, name)["approval_mode"] = "prompt"
        _table(document, "projects", str(self.workspace))["trust_level"] = "trusted"
        rendered = tomlkit.dumps(document).encode("utf-8")
        try:
            tomllib.loads(rendered.decode("utf-8"))
        except tomllib.TOMLDecodeError as exc:
            raise ValueError("Merged Codex TOML is invalid; original file preserved") from exc
        if self.codex_home.is_symlink() or path.is_symlink():
            raise ValueError("Codex configuration changed during update; original preserved")
        current = path.read_bytes() if path.exists() else None
        if current != original:
            raise ValueError("Codex configuration changed during update; concurrent edit preserved")
        if rendered != original:
            atomic_write(path, rendered)

    def identity_hooks(self, *, python: str | None = None) -> dict:
        return identity_hook_groups(
            python or sys.executable,
            self.workspace,
            state_dir=self.root / "state/identity-delivery",
            socket_path=self.codex_socket,
        )

    def trust_identity_hooks(self, listing: dict, *, python: str | None = None) -> None:
        """Trust native hashes for exact Alice commands, preserving other settings.

        Run before loading any native thread. A trusted hooks/list does not reload
        hooks already captured by an existing thread.
        """
        path = self.codex_home / "config.toml"
        states = validate_identity_hooks(
            listing, path, self.workspace, self.identity_hooks(python=python)
        )
        if path.is_symlink():
            raise ValueError("Linked Codex configuration; original preserved")
        original = path.read_bytes()
        try:
            document = tomlkit.parse(original.decode("utf-8"))
        except (UnicodeError, ParseError) as error:
            raise ValueError("Invalid Codex TOML; original preserved") from error
        # Revalidate source commands against the exact configuration still on disk;
        # a listing captured before someone edited the file cannot authorize it.
        hooks = document.get("hooks", {})
        for event, groups in self.identity_hooks(python=python).items():
            commands = [
                handler.get("command")
                for group in hooks.get(event, [])
                for handler in group.get("hooks", [])
                if is_owned_identity_handler(handler)
            ]
            if commands != [group["hooks"][0]["command"] for group in groups]:
                raise ValueError("Identity hooks changed after listing; original preserved")
        target = _table(document, "hooks", "state")
        for key, value in states.items():
            _table(target, key)["trusted_hash"] = value["trusted_hash"]
        if path.is_symlink() or path.read_bytes() != original:
            raise ValueError("Codex configuration changed during trust update; original preserved")
        rendered = tomlkit.dumps(document).encode("utf-8")
        if rendered != original:
            atomic_write(path, rendered)


def load_config(home: Path | str | None = None, *, for_maintenance: bool = False) -> RuntimeConfig:
    """Load executable config strictly; maintenance may inspect known fields only.

    A maintenance view must not be saved over the source JSON. Repinning keeps
    and atomically updates the original object, including unknown fields.
    """
    home = Path(home).expanduser().resolve() if home is not None else default_home()
    path = home / "config.json"
    if not path.is_file():
        raise ValueError(f"Alice is not initialized at {home}; run alice init first")
    value = read_json(path)
    if not isinstance(value, dict):
        raise ValueError("Alice config must be an object")
    try:
        known = {field.name for field in fields(RuntimeConfig)}
        selected = (
            {key: item for key, item in value.items() if key in known} if for_maintenance else value
        )
        config = RuntimeConfig(**selected)
    except TypeError as error:
        raise ValueError("Unsupported or missing Alice configuration fields") from error
    config.validate()
    if config.root.resolve() != home:
        raise ValueError("Alice config belongs to a different data directory")
    return config


def initialize_config(
    home: Path,
    binary: Path,
    *,
    source_root: Path | None = None,
    model: str = "gpt-6-astra",
    pin_binary: bool = True,
    login_home: Path | None = None,
) -> RuntimeConfig:
    """Initialize once; reuse login by a private link without inspecting its contents."""
    home, binary = home.expanduser().resolve(), binary.expanduser().resolve()
    if (home / "config.json").exists():
        raise ValueError("Alice is already initialized; existing state was preserved")
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise ValueError("Provide an existing Codex executable")
    source = inspect_bundle(binary)
    result = subprocess.run(
        [str(binary), "--version"], capture_output=True, text=True, check=True, timeout=15
    )
    version = result.stdout.strip()
    if not version.startswith("codex-cli "):
        raise ValueError("Executable did not identify itself as Codex CLI")
    private_dir(home)
    bundle = pin_bundle(home, source, version, pin=pin_binary)
    config = RuntimeConfig(
        home=str(home),
        codex_binary=str(bundle.binary),
        codex_version=version,
        codex_sha256=bundle.binary_sha256,
        source_root=str(source_root.resolve()) if source_root else None,
        model=model,
    )
    config.prepare_directories()
    if login_home is not None:
        auth_source = login_home.expanduser().resolve() / "auth.json"
        if not auth_source.is_file():
            raise ValueError("No file-based Codex login exists at the selected login home")
        auth_target = config.codex_home / "auth.json"
        if not auth_target.exists():
            auth_target.symlink_to(auth_source)
    config.write_codex_config()
    config.save()
    return config


def repin_codex_bundle(home: Path, source_binary: Path) -> dict:
    """Migrate a stopped runtime, retaining the standalone service-lock contract."""
    home = home.expanduser().resolve()
    lock = home / "state/service.lock"
    if (home / "state").is_symlink() or (home / "config.json").is_symlink():
        raise ValueError("Alice config and state directories must not be symbolic links")
    if lock.is_symlink():
        raise ValueError("Alice service lock must not be a symbolic link")
    with SingletonLock(lock):
        return _repin_codex_bundle_locked(home, source_binary)


def _repin_codex_bundle_locked(home: Path, source_binary: Path) -> dict:
    """Explicit stopped-runtime migration; preserve config fields and previous binaries.

    The source must contain the same primary binary already recorded by Alice.
    The caller must already hold service.lock, normally through offline_maintenance.
    Run this synchronous function off an asyncio event loop. No
    model, auth file, Codex TOML, conversation or schedule is read or changed.
    """
    import asyncio

    from .control import request

    home = home.expanduser().resolve()
    path = home / "config.json"
    if path.is_symlink() or (home / "state").is_symlink():
        raise ValueError("Alice config and state directories must not be symbolic links")
    original = path.read_bytes()
    value = read_json(path)
    if not isinstance(value, dict):
        raise ValueError("Alice config must be an object")
    known = {field.name for field in fields(RuntimeConfig)}
    try:
        config = RuntimeConfig(**{key: item for key, item in value.items() if key in known})
    except TypeError as error:
        raise ValueError("Unsupported or missing Alice configuration fields") from error
    config.validate()
    if config.root.resolve() != home:
        raise ValueError("Alice config belongs to a different data directory")
    if config.control_socket.exists():
        try:
            asyncio.run(request(config.control_socket, "status", timeout=2))
        except Exception as error:
            raise RuntimeError(
                "Alice control state is uncertain; stop/diagnose before repinning"
            ) from error
        raise RuntimeError("Stop Alice before repinning its Codex bundle")
    if config.codex_socket.exists():
        raise RuntimeError("Alice native socket remains; stop/diagnose before repinning")
    source = inspect_bundle(source_binary)
    if source.binary_sha256 != config.codex_sha256:
        raise ValueError(
            "Source Codex does not match the recorded primary hash; original preserved"
        )
    result = subprocess.run(
        [str(source.binary), "--version"],
        capture_output=True,
        text=True,
        check=True,
        timeout=15,
    )
    if result.stdout.strip() != config.codex_version:
        raise ValueError("Source Codex version differs from the recorded version")
    bundle = pin_bundle(home, source, config.codex_version)
    if path.read_bytes() != original:
        raise RuntimeError(
            "Alice configuration changed during repinning; concurrent edit preserved"
        )
    updated = dict(value)
    updated["codex_binary"] = str(bundle.binary)
    # The complete immutable pair and manifest exist before this single
    # configuration switch; a crash cannot pair the old executable with a
    # new companion index. Old binaries remain available for rollback.
    write_json(path, updated)
    config.codex_binary = str(bundle.binary)
    return verify_runtime_bundle(config, require=True)
