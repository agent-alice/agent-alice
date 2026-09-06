"""macOS user-service integration for the verified Alice runtime."""

import hashlib
import os
from pathlib import Path
import plistlib
import subprocess
import sys

from .config import RuntimeConfig
from .files import atomic_write, read_json, write_json
from .releases import ReleaseManager


def label(config: RuntimeConfig) -> str:
    return "io.alice.codex." + hashlib.sha256(config.home.encode()).hexdigest()[:12]


def service_target(config: RuntimeConfig) -> str:
    return f"gui/{os.getuid()}/{label(config)}"


def definition(config: RuntimeConfig, python: str) -> dict:
    if not Path(python).is_absolute():
        raise ValueError("Service interpreter must be an absolute verified path")
    return {
        "Label": label(config),
        "ProgramArguments": [python, "-I", "-m", "alice_codex", "--home", config.home, "serve"],
        "WorkingDirectory": str(config.workspace),
        "EnvironmentVariables": {
            "ALICE_HOME": config.home,
            "CODEX_HOME": str(config.codex_home),
            "PATH": os.environ.get("PATH", "/usr/bin:/bin:/usr/sbin:/sbin"),
        },
        "RunAtLoad": True,
        # An explicit Alice stop exits successfully and stays stopped. A crash
        # restarts with the persisted autonomy pause enforced by Service.run.
        "KeepAlive": {"SuccessfulExit": False},
        "ThrottleInterval": 15,
        "Umask": 0o077,
        "StandardOutPath": str(config.root / "logs/launchd.log"),
        "StandardErrorPath": str(config.root / "logs/launchd.log"),
    }


def _command(*args: str, required: bool = True) -> subprocess.CompletedProcess:
    if sys.platform != "darwin":
        raise ValueError(
            "launchd installation is macOS-only; use alice serve with your Linux supervisor"
        )
    result = subprocess.run(["launchctl", *args], capture_output=True, text=True, timeout=20)
    if required and result.returncode:
        raise RuntimeError(
            f"launchctl {args[0]} failed ({result.returncode}): {result.stderr.strip()}"
        )
    return result


def status(config: RuntimeConfig) -> dict:
    metadata = config.root / "state/supervisor.json"
    if not metadata.exists():
        return {"installed": False, "loaded": False}
    record = read_json(metadata)
    if record.get("label") != label(config):
        raise ValueError("Supervisor metadata does not belong to this runtime")
    result = _command("print", service_target(config), required=False)
    return {
        "installed": True,
        "loaded": result.returncode == 0,
        "label": record["label"],
        "plist": record["plist"],
    }


def install(config: RuntimeConfig, *, directory: Path | None = None) -> dict:
    current = ReleaseManager(config.root).checked_current()
    if not current:
        raise ValueError("Activate a verified release before installing the persistent service")
    if status(config)["loaded"]:
        raise ValueError("Unload the existing Alice user service before reinstalling it")
    directory = directory or Path.home() / "Library/LaunchAgents"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{label(config)}.plist"
    payload = plistlib.dumps(definition(config, current["python"]), sort_keys=True)
    if path.exists():
        prior = plistlib.loads(path.read_bytes())
        if prior.get("Label") != label(config):
            raise ValueError("Existing launch plist belongs to another service")
    atomic_write(path, payload)
    write_json(
        config.root / "state/supervisor.json",
        {
            "version": 1,
            "label": label(config),
            "plist": str(path),
            "installed_release": current["current"],
        },
    )
    _command("bootstrap", f"gui/{os.getuid()}", str(path))
    return status(config)


def start(config: RuntimeConfig) -> None:
    state = status(config)
    if not state["installed"]:
        raise ValueError("Alice has no installed user service")
    if not state["loaded"]:
        _command("bootstrap", f"gui/{os.getuid()}", state["plist"])
    else:
        _command("kickstart", service_target(config))


def uninstall(config: RuntimeConfig) -> dict:
    state = status(config)
    if not state["installed"]:
        return state
    if state["loaded"]:
        _command("bootout", service_target(config))
    path = Path(state["plist"])
    if path.name != f"{label(config)}.plist":
        raise ValueError("Unexpected supervisor plist path; preserved")
    if path.exists() and plistlib.loads(path.read_bytes()).get("Label") == label(config):
        path.unlink()
    (config.root / "state/supervisor.json").unlink()
    return {"installed": False, "loaded": False, "runtime_data_preserved": True}
