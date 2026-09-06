"""macOS user-service integration for the verified Alice runtime."""

import hashlib
import os
from pathlib import Path
import plistlib
import re
import subprocess
import sys
import time

from .bootstrap import checked_runtime, install_runtime
from .config import RuntimeConfig
from .files import atomic_write, read_json, write_json
from .releases import ReleaseManager
from .service import process_birth, process_identity


def label(config: RuntimeConfig) -> str:
    return "io.alice.codex." + hashlib.sha256(config.home.encode()).hexdigest()[:12]


def service_target(config: RuntimeConfig) -> str:
    return f"gui/{os.getuid()}/{label(config)}"


def definition(config: RuntimeConfig, python: str) -> dict:
    if not Path(python).is_absolute():
        raise ValueError("Service interpreter must be an absolute verified path")
    return {
        "Label": label(config),
        "ProgramArguments": [python, "-I", "-m", "alice_codex.supervisor", "--home", config.home],
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
    supervisor = None
    report = config.root / "state/bootstrap-state.json"
    failure = config.root / "state/bootstrap-failure.json"
    if report.exists():
        value = read_json(report)
        supervisor = {
            key: value.get(key) for key in ("lifecycle", "error", "candidate", "observed_at")
        }
    if failure.exists():
        value = read_json(failure)
        if not supervisor or value.get("observed_at", 0) > (supervisor.get("observed_at") or 0):
            supervisor = {"lifecycle": "blocked", **value}
    if supervisor and (supervisor.get("observed_at") or 0) < record.get(
        "last_start_requested_at", 0
    ):
        supervisor = {"lifecycle": "starting", "error": None}
    return {
        "supervisor": supervisor,
        "installed": True,
        "loaded": result.returncode == 0,
        "label": record["label"],
        "plist": record["plist"],
    }


def install(config: RuntimeConfig, *, directory: Path | None = None) -> dict:
    manager = ReleaseManager(config.root)
    current = manager.checked_current()
    if not current:
        raise ValueError("Activate a verified release before installing the persistent service")
    if status(config)["loaded"]:
        raise ValueError("Unload the existing Alice user service before reinstalling it")
    bootstrap = install_runtime(manager)
    directory = directory or Path.home() / "Library/LaunchAgents"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{label(config)}.plist"
    payload = plistlib.dumps(definition(config, bootstrap["python"]), sort_keys=True)
    if path.exists():
        prior = plistlib.loads(path.read_bytes())
        if prior.get("Label") != label(config):
            raise ValueError("Existing launch plist belongs to another service")
    atomic_write(path, payload)
    write_json(
        config.root / "state/supervisor.json",
        {
            "version": 2,
            "bootstrap_generation": bootstrap["generation"],
            "last_start_requested_at": time.time(),
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
    checked_runtime(config.root)
    metadata = read_json(config.root / "state/supervisor.json")
    if metadata.get("version") != 2:
        raise ValueError("Reinstall the user service to enable the stable supervisor")
    metadata["last_start_requested_at"] = time.time()
    write_json(config.root / "state/supervisor.json", metadata)
    if not state["loaded"]:
        _command("bootstrap", f"gui/{os.getuid()}", state["plist"])
    else:
        _command("kickstart", service_target(config))


def _assert_owned_stopped(config: RuntimeConfig) -> None:
    for filename, key in (("bootstrap-state.json", "child"), ("runtime.json", "server")):
        path = config.root / "state" / filename
        value = read_json(path).get(key) if path.exists() else None
        if not value:
            continue
        alive = (
            process_birth(value["pid"]) == value["birth"]
            if value.get("birth")
            else process_identity(value["pid"]) == value.get("identity")
        )
        if alive:
            raise RuntimeError("Supervisor exited but a recorded owned process remains")


def stop(config: RuntimeConfig) -> dict:
    """Stop this exact label, including a supervisor still starting its child.

    The plist and metadata stay installed. Bootout also cancels any already
    scheduled launch after an entry-point failure; explicit start bootstraps it.
    """
    state = status(config)
    if not state["loaded"]:
        _assert_owned_stopped(config)
        return {"stopped": True, "already_stopped": True}
    _command("kill", "SIGTERM", service_target(config), required=False)
    deadline = time.monotonic() + 55
    while time.monotonic() < deadline:
        observed = _command("print", service_target(config), required=False)
        if observed.returncode or not re.search(r"^\s*pid = \d+\s*$", observed.stdout, re.M):
            if observed.returncode == 0:
                _command("bootout", service_target(config))
            _assert_owned_stopped(config)
            return {"stopped": True, "installed": True}
        time.sleep(0.1)
    raise TimeoutError("Supervisor shutdown unconfirmed; own label remains installed")


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
