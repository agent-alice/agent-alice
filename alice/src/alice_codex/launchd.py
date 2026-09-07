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
from .files import SingletonLock, atomic_write, read_json, write_json
from .releases import ReleaseManager
from .service import process_birth, process_identity
from .startup import DEFAULT_STARTUP_TIMEOUT, validate_startup_timeout


def label(config: RuntimeConfig) -> str:
    return "io.alice.codex." + hashlib.sha256(config.home.encode()).hexdigest()[:12]


def service_target(config: RuntimeConfig) -> str:
    return f"gui/{os.getuid()}/{label(config)}"


def definition(
    config: RuntimeConfig, python: str, *, startup_timeout: float = DEFAULT_STARTUP_TIMEOUT
) -> dict:
    if not Path(python).is_absolute():
        raise ValueError("Service interpreter must be an absolute verified path")
    timeout = validate_startup_timeout(startup_timeout)
    arguments = [python, "-I", "-m", "alice_codex.supervisor", "--home", config.home]
    if timeout != DEFAULT_STARTUP_TIMEOUT:
        arguments.extend(["--startup-timeout", str(timeout)])
    return {
        "Label": label(config),
        "ProgramArguments": arguments,
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
    try:
        if report.exists():
            value = read_json(report)
            supervisor = {
                key: value.get(key) for key in ("lifecycle", "error", "candidate", "observed_at")
            }
        if failure.exists():
            value = read_json(failure)
            if not supervisor or value.get("observed_at", 0) > (supervisor.get("observed_at") or 0):
                supervisor = {"lifecycle": "blocked", **value}
    except (OSError, ValueError, AttributeError, TypeError) as error:
        # A damaged report must not prevent stopping the correctly identified label.
        supervisor = {"lifecycle": "blocked", "error": str(error), "observed_at": time.time()}
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


def configured_startup_timeout(config: RuntimeConfig) -> float:
    path = config.root / "state/supervisor.json"
    record = read_json(path) if path.exists() else {}
    return validate_startup_timeout(record.get("startup_timeout_seconds", DEFAULT_STARTUP_TIMEOUT))


def _probe_startup_timeout_support(python: str) -> bool:
    # Probe the exact installed candidate before replacing the independent
    # bootstrap. Importing this module does not initialize data or native work.
    result = subprocess.run(
        [python, "-I", "-B", "-c",
         "import alice_codex.supervisor as s; "
         "print(getattr(s,'SUPERVISOR_STARTUP_OPTIONS_VERSION',0))"],
        capture_output=True, text=True, timeout=15,
    )
    return result.returncode == 0 and result.stdout.strip() == "1"


def install(
    config: RuntimeConfig, *, directory: Path | None = None, startup_timeout: float | None = None
) -> dict:
    if startup_timeout is not None:
        validate_startup_timeout(startup_timeout)
    with SingletonLock(config.root / "state/lifecycle.lock"):
        return _install(config, directory=directory, startup_timeout=startup_timeout)


def _install(
    config: RuntimeConfig, *, directory: Path | None = None, startup_timeout: float | None = None
) -> dict:
    timeout = (
        configured_startup_timeout(config)
        if startup_timeout is None else validate_startup_timeout(startup_timeout)
    )
    manager = ReleaseManager(config.root)
    current = manager.checked_current()
    if not current:
        raise ValueError("Activate a verified release before installing the persistent service")
    if status(config)["loaded"]:
        raise ValueError("Unload the existing Alice user service before reinstalling it")
    if timeout != DEFAULT_STARTUP_TIMEOUT and not _probe_startup_timeout_support(current["python"]):
        raise ValueError("Candidate bootstrap does not support the configured startup timeout")
    bootstrap = install_runtime(manager)
    directory = directory or Path.home() / "Library/LaunchAgents"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{label(config)}.plist"
    payload = plistlib.dumps(
        definition(config, bootstrap["python"], startup_timeout=timeout), sort_keys=True
    )
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
            "startup_timeout_seconds": timeout,
        },
    )
    _command("bootstrap", f"gui/{os.getuid()}", str(path))
    return status(config)


def start(config: RuntimeConfig) -> None:
    with SingletonLock(config.root / "state/lifecycle.lock"):
        _start(config)


def _start(config: RuntimeConfig) -> None:
    state = status(config)
    if not state["installed"]:
        raise ValueError("Alice has no installed user service")
    runtime = checked_runtime(config.root)
    metadata = read_json(config.root / "state/supervisor.json")
    if metadata.get("version") != 2:
        raise ValueError("Reinstall the user service to enable the stable supervisor")
    timeout = configured_startup_timeout(config)
    if "startup_timeout_seconds" in metadata:
        path = Path(state["plist"])
        if path.is_symlink() or path.name != f"{label(config)}.plist":
            raise ValueError("Installed startup settings refer to an unexpected plist")
        actual = plistlib.loads(path.read_bytes()).get("ProgramArguments")
        expected = definition(config, runtime["python"], startup_timeout=timeout)["ProgramArguments"]
        if actual != expected:
            raise ValueError("Installed startup settings disagree with the launch plist; reinstall")
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
        if value.get("birth"):
            alive = process_birth(value["pid"]) == value["birth"]
        else:
            identity = process_identity(value["pid"])
            alive = bool(identity) and identity == value.get("identity")
        if alive:
            raise RuntimeError("Supervisor exited but a recorded owned process remains")


def stop(config: RuntimeConfig) -> dict:
    """Stop this exact label, including a supervisor still starting its child.

    The plist and metadata stay installed. Bootout also cancels any already
    scheduled launch after an entry-point failure; explicit start bootstraps it.
    """
    with SingletonLock(config.root / "state/lifecycle.lock"):
        return _stop(config)


def _stop(config: RuntimeConfig) -> dict:
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
    with SingletonLock(config.root / "state/lifecycle.lock"):
        return _uninstall(config)


def _uninstall(config: RuntimeConfig) -> dict:
    state = status(config)
    if not state["installed"]:
        return state
    _stop(config)
    path = Path(state["plist"])
    if path.name != f"{label(config)}.plist":
        raise ValueError("Unexpected supervisor plist path; preserved")
    if path.exists() and plistlib.loads(path.read_bytes()).get("Label") == label(config):
        path.unlink()
    (config.root / "state/supervisor.json").unlink()
    return {"installed": False, "loaded": False, "runtime_data_preserved": True}
