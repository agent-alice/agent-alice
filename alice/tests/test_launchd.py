import plistlib
import subprocess
from unittest.mock import Mock

import pytest

from alice_codex import launchd
from alice_codex.config import RuntimeConfig


def test_service_definition_quotes_paths_and_stops_after_success(tmp_path):
    config = RuntimeConfig(str(tmp_path / "data with spaces"), "/tmp/codex", "fixture", "sha")
    python = str(tmp_path / "release with spaces/bin/python")
    value = plistlib.loads(plistlib.dumps(launchd.definition(config, python)))
    assert value["ProgramArguments"] == [
        python,
        "-I",
        "-m",
        "alice_codex.supervisor",
        "--home",
        config.home,
    ]
    assert value["KeepAlive"] == {"SuccessfulExit": False}
    assert value["EnvironmentVariables"]["CODEX_HOME"] == str(config.codex_home)
    assert value["Umask"] == 0o077


def test_unverified_release_cannot_install_a_persistent_service(tmp_path, monkeypatch):
    config = RuntimeConfig(str(tmp_path), "/tmp/codex", "fixture", "sha")
    config.prepare_directories()
    try:
        command = Mock()
        monkeypatch.setattr(launchd, "_command", command)
        with pytest.raises(ValueError, match="verified release"):
            launchd.install(config, directory=tmp_path / "agents")
        command.assert_not_called()
        assert not (tmp_path / "agents").exists()
    finally:
        config.socket_dir.rmdir()


def test_install_uninstall_keeps_runtime_data(tmp_path, monkeypatch):
    config = RuntimeConfig(str(tmp_path / "data"), "/tmp/codex", "fixture", "sha")
    config.prepare_directories()
    try:
        manager = Mock()
        manager.checked_current.return_value = {
            "python": "/tmp/verified/python",
            "current": "candidate",
        }
        monkeypatch.setattr(launchd, "ReleaseManager", lambda home: manager)
        monkeypatch.setattr(
            launchd,
            "install_runtime",
            lambda manager: {"python": "/tmp/stable/python", "generation": "owned-bootstrap"},
        )
        command = Mock(return_value=subprocess.CompletedProcess([], 0, "", ""))
        monkeypatch.setattr(launchd, "_command", command)
        marker = config.workspace / "kept.txt"
        marker.write_text("new experience")
        installed = launchd.install(config, directory=tmp_path / "agents")
        assert installed["installed"] and installed["loaded"]
        assert command.call_args_list[0].args[0] == "bootstrap"
        assert launchd.uninstall(config) == {
            "installed": False,
            "loaded": False,
            "runtime_data_preserved": True,
        }
        assert marker.read_text() == "new experience"
        assert list((tmp_path / "agents").iterdir()) == []
    finally:
        config.socket_dir.rmdir()


def test_stop_signals_only_exact_label_and_waits_until_no_supervisor_pid(tmp_path, monkeypatch):
    config = RuntimeConfig(str(tmp_path), "/tmp/codex", "fixture", "sha")
    monkeypatch.setattr(launchd, "status", lambda config: {"installed": True, "loaded": True})
    command = Mock(
        side_effect=[
            subprocess.CompletedProcess([], 0, "", ""),
            subprocess.CompletedProcess([], 0, " state = running\n pid = 42\n", ""),
            subprocess.CompletedProcess([], 0, " state = not running\n last exit code = 0\n", ""),
            subprocess.CompletedProcess([], 0, "", ""),
        ]
    )
    monkeypatch.setattr(launchd, "_command", command)
    monkeypatch.setattr(launchd.time, "sleep", lambda duration: None)
    assert launchd.stop(config) == {"stopped": True, "installed": True}
    assert command.call_args_list[0].args == ("kill", "SIGTERM", launchd.service_target(config))
    assert command.call_args_list[-1].args == ("bootout", launchd.service_target(config))
    assert len(command.call_args_list) == 4


def test_stop_cannot_claim_success_with_recorded_native_still_alive(tmp_path, monkeypatch):
    import sys
    from alice_codex.files import write_json
    from alice_codex.service import process_birth

    config = RuntimeConfig(str(tmp_path), "/tmp/codex", "fixture", "sha")
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    try:
        write_json(
            config.root / "state/runtime.json",
            {
                "server": {
                    "pid": child.pid,
                    "birth": process_birth(child.pid),
                }
            },
        )
        monkeypatch.setattr(launchd, "status", lambda config: {"installed": True, "loaded": False})
        with pytest.raises(RuntimeError, match="owned process remains"):
            launchd.stop(config)
        assert child.poll() is None
    finally:
        child.terminate()
        child.wait(timeout=5)
