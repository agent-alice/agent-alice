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
        "alice_codex",
        "--home",
        config.home,
        "serve",
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
