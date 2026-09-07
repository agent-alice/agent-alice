import asyncio
import json
from pathlib import Path
import plistlib
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from alice_codex import cli, launchd, supervisor
from alice_codex.config import RuntimeConfig
from alice_codex.files import write_json


@pytest.fixture
def installation(tmp_path, monkeypatch):
    config = RuntimeConfig(str(tmp_path / "data"), "/tmp/codex", "fixture", "sha")
    config.prepare_directories()
    manager = Mock()
    manager.checked_current.return_value = {"python": sys.executable, "current": "candidate"}
    monkeypatch.setattr(launchd, "ReleaseManager", lambda home: manager)
    install = Mock(return_value={"python": "/tmp/stable/python", "generation": "bootstrap"})
    monkeypatch.setattr(launchd, "install_runtime", install)
    monkeypatch.setattr(launchd, "checked_runtime", lambda home: {"python": "/tmp/stable/python"})
    command = Mock(return_value=subprocess.CompletedProcess([], 1, "", "not loaded"))
    monkeypatch.setattr(launchd, "_command", command)
    monkeypatch.setattr(
        launchd, "_probe_startup_timeout_support", lambda python: True, raising=False
    )
    try:
        yield config, tmp_path / "agents", command, install
    finally:
        config.socket_dir.rmdir()


def test_reinstall_retains_timeout_and_explicit_override_changes_it(installation):
    config, directory, command, _ = installation
    marker = config.workspace / "retained.txt"
    marker.write_text("synthetic experience")
    launchd.install(config, directory=directory, startup_timeout=120)
    launchd.install(config, directory=directory)
    metadata = json.loads((config.root / "state/supervisor.json").read_text())
    args = plistlib.loads(Path(metadata["plist"]).read_bytes())["ProgramArguments"]
    assert float(args[args.index("--startup-timeout") + 1]) == 120
    assert metadata["startup_timeout_seconds"] == 120
    launchd.install(config, directory=directory, startup_timeout=75)
    metadata = json.loads((config.root / "state/supervisor.json").read_text())
    assert metadata["startup_timeout_seconds"] == 75
    assert marker.read_text() == "synthetic experience"
    assert len([call for call in command.call_args_list if call.args[0] == "bootstrap"]) == 3


@pytest.mark.parametrize("value", [0, -1, True, float("nan"), float("inf"), 601])
def test_invalid_timeout_cannot_change_installation(installation, value):
    config, directory, command, install = installation
    with pytest.raises(ValueError, match="startup timeout"):
        launchd.install(config, directory=directory, startup_timeout=value)
    install.assert_not_called()
    assert not directory.exists()
    assert not (config.root / "state/supervisor.json").exists()
    assert not [call for call in command.call_args_list if call.args[0] == "bootstrap"]


def test_old_bootstrap_cannot_silently_ignore_custom_timeout(installation, monkeypatch):
    config, directory, command, install = installation
    monkeypatch.setattr(
        launchd, "_probe_startup_timeout_support", lambda python: False, raising=False
    )
    with pytest.raises(ValueError, match="bootstrap.*startup timeout"):
        launchd.install(config, directory=directory, startup_timeout=120)
    install.assert_not_called()
    assert not directory.exists()
    assert not [call for call in command.call_args_list if call.args[0] == "bootstrap"]


def test_invalid_saved_timeout_blocks_start_but_not_stop(installation):
    config, directory, command, _ = installation
    launchd.install(config, directory=directory, startup_timeout=120)
    path = config.root / "state/supervisor.json"
    metadata = json.loads(path.read_text())
    metadata["startup_timeout_seconds"] = -1
    write_json(path, metadata)
    original = path.read_bytes()
    command.reset_mock()
    with pytest.raises(ValueError, match="startup timeout"):
        launchd.start(config)
    assert path.read_bytes() == original
    assert not [
        call for call in command.call_args_list if call.args[0] in ("bootstrap", "kickstart")
    ]
    assert launchd.stop(config)["stopped"] is True


def test_plist_drift_does_not_silently_replace_saved_timeout(installation):
    config, directory, command, _ = installation
    launchd.install(config, directory=directory, startup_timeout=120)
    metadata = json.loads((config.root / "state/supervisor.json").read_text())
    path = Path(metadata["plist"])
    payload = plistlib.loads(path.read_bytes())
    payload["ProgramArguments"][-1] = "30"
    path.write_bytes(plistlib.dumps(payload))
    original = path.read_bytes()
    command.reset_mock()
    with pytest.raises(ValueError, match="startup settings"):
        launchd.start(config)
    assert path.read_bytes() == original
    assert not [
        call for call in command.call_args_list if call.args[0] in ("bootstrap", "kickstart")
    ]


@pytest.mark.parametrize("value", ["0", "-1", "nan", "inf", "601"])
def test_invalid_cli_option_is_rejected_during_argument_parsing(value, tmp_path, capsys):
    with pytest.raises(SystemExit) as failure:
        cli.parser().parse_args(
            ["--home", str(tmp_path), "service", "install", "--startup-timeout", value]
        )
    assert failure.value.code == 2
    assert "startup timeout must be" in capsys.readouterr().err


def test_supervisor_entry_passes_custom_timeout_to_existing_lifecycle(tmp_path, monkeypatch):
    seen = []
    config = RuntimeConfig(str(tmp_path), "/tmp/codex", "fixture", "sha")
    monkeypatch.setattr(
        supervisor, "checked_runtime", lambda home: {"python": sys.executable, "manifest": {}}
    )
    monkeypatch.setattr(supervisor, "load_config", lambda home: config)

    class ObservedSupervisor:
        def __init__(self, config, *, bootstrap_manifest, startup_timeout):
            seen.append(startup_timeout)
            self.stop_event = asyncio.Event()

        async def run(self):
            return 0

    monkeypatch.setattr(supervisor, "Supervisor", ObservedSupervisor)
    assert supervisor.main(["--home", str(tmp_path), "--startup-timeout", "120"]) == 0
    assert seen == [120]


@pytest.mark.asyncio
async def test_cli_waits_for_configured_attempts_instead_of_old_180_seconds(tmp_path, monkeypatch):
    config = RuntimeConfig(str(tmp_path), "/tmp/codex", "fixture", "sha")
    write_json(config.root / "state/supervisor.json", {"startup_timeout_seconds": 120})
    ready = {"ready": True}
    monkeypatch.setattr(cli, "running", AsyncMock(side_effect=[None, ready]))
    monkeypatch.setattr(launchd, "start", lambda config: None)
    times = iter([0, 200])
    monkeypatch.setattr(cli, "time", SimpleNamespace(monotonic=lambda: next(times)))
    assert await cli.start(config) == ready


@pytest.mark.asyncio
async def test_invalid_saved_timeout_cannot_stop_service_during_reinstall(tmp_path, monkeypatch):
    config = RuntimeConfig(str(tmp_path), "/tmp/codex", "fixture", "sha")
    write_json(config.root / "state/supervisor.json", {"startup_timeout_seconds": -1})
    monkeypatch.setattr(cli, "load_config", lambda home: config)
    observe = AsyncMock(side_effect=AssertionError("must validate before touching running service"))
    monkeypatch.setattr(cli, "running", observe)
    install = Mock()
    monkeypatch.setattr(launchd, "install", install)
    args = cli.parser().parse_args(["--home", str(tmp_path), "service", "install"])
    with pytest.raises(ValueError, match="startup timeout"):
        await cli.execute(args)
    observe.assert_not_called()
    install.assert_not_called()
