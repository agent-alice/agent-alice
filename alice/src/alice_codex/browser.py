"""Install a pinned optional browser MCP without changing Alice's model runtime.

The local manifest detects accidental drift; it is not an attestation against
another process with this user's filesystem permissions. Installation is a
synchronous operation and must run off an asyncio event loop.
"""

import asyncio
from datetime import datetime, timezone
import hashlib
from importlib.resources import files
import os
from pathlib import Path
import signal
import subprocess
import tempfile
from uuid import uuid4

import tomlkit
from tomlkit.exceptions import ParseError

from .browser_verify import verify
from .control import request
from .files import SingletonLock, atomic_write, private_dir, read_json, sha256_file, write_json


SERVER = "alice_browser"
TOOLS = (
    "browser_navigate",
    "browser_snapshot",
    "browser_click",
    "browser_network_requests",
    "browser_close",
)
VERSION = 1


def _directory(path):
    if any(parent.is_symlink() for parent in (path, *path.parents)):
        # /tmp may be a platform alias; all managed roots are canonicalized by
        # RuntimeConfig initialization before reaching this helper.
        raise ValueError("Browser managed directories must not contain symbolic links")
    return private_dir(path)


def _file(path):
    if any(parent.is_symlink() for parent in (path, *path.parents)):
        raise ValueError("Browser state/config file must not be a symbolic link")
    return path.read_bytes() if path.exists() else None


def _config(config):
    path = config.codex_home / "config.toml"
    original = _file(path)
    if original is None:
        raise ValueError("Initialize Alice before installing its browser")
    try:
        document = tomlkit.parse(original.decode())
    except (UnicodeError, ParseError) as error:
        raise ValueError("Invalid Codex config; original preserved") from error
    servers = document.get("mcp_servers", {})
    if not isinstance(servers, dict) and not hasattr(servers, "items"):
        raise ValueError("Invalid MCP configuration; original preserved")
    return original, document, servers.get(SERVER)


def _manifest(config):
    path = config.root / "state/browser.json"
    if _file(path) is None:
        return None
    value = read_json(path)
    if (
        not isinstance(value, dict)
        or value.get("version") != VERSION
        or value.get("server") != SERVER
    ):
        raise ValueError("Unsupported browser installation manifest; original preserved")
    return value


def _artifact_digest(root):
    if root != root.resolve():
        raise ValueError("Browser package root must be canonical")
    digest = hashlib.sha256()
    paths = sorted(
        path
        for name in ("package.json", "package-lock.json", "node_modules")
        for path in ([root / name] if name != "node_modules" else (root / name).rglob("*"))
    )
    if not (root / "node_modules/@playwright/mcp/cli.js").is_file():
        raise ValueError("Installed browser MCP entry point is missing")
    for path in paths:
        relative = str(path.relative_to(root))
        if path.is_symlink():
            if not path.resolve().is_relative_to(root.resolve()):
                raise ValueError("Browser package symlink escapes its installation")
            value = "link:" + os.readlink(path)
        elif path.is_file():
            value = "file:" + sha256_file(path)
        else:
            continue
        digest.update(relative.encode() + b"\0" + value.encode() + b"\0")
    return digest.hexdigest()


def status(config) -> dict:
    """Read installed state and current hashes; never install, repair or execute."""
    result = {"installed": False, "native_verified": False, "server": SERVER}
    try:
        manifest = _manifest(config)
        if manifest is None:
            return result
        _, _, current = _config(config)
        root = Path(manifest["runtime_root"])
        if not root.is_relative_to(config.root / "extensions/browser") or root.is_symlink():
            raise ValueError("Browser installation path is outside its managed root")
        if current != manifest["settings"]:
            raise ValueError("Browser MCP configuration changed since verification")
        for name in ("node", "npm_cli", "browser"):
            entry = manifest[name]
            if sha256_file(Path(entry["path"])) != entry["sha256"]:
                raise ValueError(f"Browser dependency changed: {name}")
        if _artifact_digest(root) != manifest["artifact_sha256"]:
            raise ValueError("Browser package files changed since verification")
        result.update(
            installed=True,
            native_verified=manifest["verification"]["status"] == "passed",
            runtime_root=str(root),
            versions=manifest["versions"],
            verification=manifest["verification"],
            manifest_version=VERSION,
        )
    except (ValueError, OSError, KeyError, TypeError) as error:
        result["error"] = str(error)
    return result


def _settings(config, node, browser, runtime):
    state = config.root / "browser"
    process_home = state / "process-home"
    sockets = config.socket_dir
    if sockets.is_symlink() or (sockets.exists() and sockets.stat().st_uid != os.getuid()):
        raise ValueError("Browser IPC directory is not owned by this runtime user")
    ipc = _directory(sockets.resolve() / "browser")
    for path in (
        state,
        process_home,
        process_home / ".config",
        process_home / ".cache",
        state / "tmp",
        state / "output",
    ):
        _directory(path)
    return {
        "command": str(node),
        "args": [
            str(runtime / "node_modules/@playwright/mcp/cli.js"),
            "--isolated",
            "--headless",
            "--browser",
            "chrome",
            "--executable-path",
            str(browser),
            "--sandbox",
            "--block-service-workers",
            "--output-dir",
            str(state / "output"),
            "--timeout-navigation",
            "10000",
        ],
        "cwd": str(config.workspace),
        "env": {
            "HOME": str(process_home),
            "TMPDIR": str(state / "tmp"),
            "XDG_CONFIG_HOME": str(process_home / ".config"),
            "XDG_CACHE_HOME": str(process_home / ".cache"),
            "NO_PROXY": "127.0.0.1,localhost,::1",
            "no_proxy": "127.0.0.1,localhost,::1",
            "PWTEST_SOCKETS_DIR": str(ipc),
        },
        "env_vars": [],
        "enabled": True,
        "required": False,
        "startup_timeout_sec": 20,
        "tool_timeout_sec": 30,
        "enabled_tools": list(TOOLS),
        "default_tools_approval_mode": "prompt",
    }


def _run(command, *, cwd, env, log, timeout):
    """Own the process group and retain diagnostics privately, without echoing it."""
    with log.open("wb") as output:
        process = subprocess.Popen(
            command,
            cwd=cwd,
            env=env,
            stdout=output,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            code = process.wait(timeout=timeout)
        except BaseException:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
            raise
    if code:
        raise RuntimeError(f"Browser dependency command failed; inspect private log {log}")


def install(config, *, node: Path, npm_cli: Path, browser_executable: Path) -> dict:
    """Install and verify before configuring; refuse running/uncertain services."""
    config.validate()
    config.verify_binary()
    if config.root != config.root.resolve():
        raise ValueError("Browser installation requires a canonical Alice home")
    _directory(config.root / "state")
    _directory(config.codex_home)
    if (config.root / "state/service.lock").is_symlink():
        raise ValueError("Alice service lock must not be a symbolic link")
    with SingletonLock(config.root / "state/service.lock"):
        if config.control_socket.exists():
            try:
                asyncio.run(request(config.control_socket, "status", timeout=2))
            except Exception as error:
                raise RuntimeError(
                    "Alice control state is uncertain; diagnose/stop before installing"
                ) from error
            raise RuntimeError("Stop Alice before installing its browser")
        if config.codex_socket.exists():
            raise RuntimeError("Alice native socket remains; diagnose/stop before installing")
        original, document, current = _config(config)
        previous = _manifest(config)
        if current is not None and (previous is None or current != previous.get("settings")):
            raise ValueError(
                "Existing alice_browser configuration is not owned; original preserved"
            )
        inputs = {
            name: Path(path).expanduser().resolve()
            for name, path in (
                ("node", node),
                ("npm_cli", npm_cli),
                ("browser", browser_executable),
            )
        }
        for name, path in inputs.items():
            if not path.is_file() or (name != "npm_cli" and not os.access(path, os.X_OK)):
                raise ValueError(f"Browser dependency is missing or unusable: {name}")
        dependencies = {
            name: {"path": str(path), "sha256": sha256_file(path)} for name, path in inputs.items()
        }
        if previous and all(previous.get(name) == value for name, value in dependencies.items()):
            existing = status(config)
            if existing["installed"] and existing["native_verified"]:
                return existing
        base = _directory(config.root / "extensions/browser")
        _directory(config.root / "browser/npm-cache")
        candidate = Path(tempfile.mkdtemp(prefix=".candidate-", dir=base))
        environment = {
            "HOME": str(_directory(candidate / "install-home")),
            "PATH": str(inputs["node"].parent) + ":/usr/bin:/bin",
            "PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD": "1",
        }
        try:
            version = subprocess.run(
                [str(inputs["node"]), "--version"],
                env=environment,
                capture_output=True,
                text=True,
                check=True,
                timeout=10,
            ).stdout.strip()
            if int(version.removeprefix("v").split(".")[0]) < 20:
                raise ValueError("Pinned Playwright dependency requires Node >=20")
            browser_version = subprocess.run(
                [str(inputs["browser"]), "--version"],
                env=environment,
                capture_output=True,
                text=True,
                check=True,
                timeout=10,
            ).stdout.strip()
            for name in ("package.json", "package-lock.json"):
                resource = files("alice_codex").joinpath("templates/browser/npm", name)
                atomic_write(candidate / name, resource.read_bytes())
            _run(
                [
                    str(inputs["node"]),
                    str(inputs["npm_cli"]),
                    "ci",
                    "--prefix",
                    str(candidate),
                    "--cache",
                    str(config.root / "browser/npm-cache"),
                    "--ignore-scripts",
                    "--no-audit",
                    "--no-fund",
                ],
                cwd=candidate,
                env=environment,
                log=candidate / "install.log",
                timeout=120,
            )
            artifact = _artifact_digest(candidate)
            final = base / (artifact[:16] + "-" + uuid4().hex[:8])
            candidate.rename(final)
            candidate = final
            settings = _settings(config, inputs["node"], inputs["browser"], final)
            evidence = verify(Path(config.codex_binary), settings)
            write_json(final / "verification.json", evidence)
            if evidence.get("status") != "passed":
                raise RuntimeError(
                    f"Browser native verification failed; inspect {final / 'verification.json'}"
                )
            if _artifact_digest(final) != artifact or any(
                sha256_file(inputs[name]) != value["sha256"] for name, value in dependencies.items()
            ):
                raise RuntimeError(
                    "Browser artifacts changed during verification; original config preserved"
                )
            manifest = {
                "version": VERSION,
                "server": SERVER,
                **dependencies,
                "runtime_root": str(final),
                "artifact_sha256": artifact,
                "settings": settings,
                "verification": evidence,
                "verified_at": datetime.now(timezone.utc).isoformat(),
                "versions": {
                    "node": version,
                    "browser": browser_version,
                    "playwright_mcp": "0.0.80",
                },
            }
            if _file(config.codex_home / "config.toml") != original:
                raise RuntimeError(
                    "Codex config changed during installation; concurrent edit preserved"
                )
            if "mcp_servers" not in document:
                document["mcp_servers"] = tomlkit.table()
            document["mcp_servers"][SERVER] = settings
            rendered = tomlkit.dumps(document).encode()
            atomic_write(config.codex_home / "config.toml", rendered)
            try:
                write_json(config.root / "state/browser.json", manifest)
            except BaseException:
                if _file(config.codex_home / "config.toml") == rendered:
                    atomic_write(config.codex_home / "config.toml", original)
                raise
            return status(config)
        except Exception as error:
            write_json(
                candidate / "failure.json",
                {"status": "failed", "error_type": type(error).__name__, "error": str(error)},
            )
            raise
