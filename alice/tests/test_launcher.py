from pathlib import Path
import sys
from unittest.mock import Mock

import pytest

from alice_codex import launcher


def test_launcher_forwards_literal_arguments_to_verified_candidate(tmp_path, monkeypatch):
    manager = Mock()
    manager.resolve_current.return_value = {"python": "/verified candidate/bin/python"}
    monkeypatch.setattr(launcher, "ReleaseManager", lambda root: manager)
    monkeypatch.setenv("PYTHONPATH", "/untrusted/imports")
    monkeypatch.setenv("PYTHONHOME", "/untrusted/python")
    execute = Mock(side_effect=SystemExit(0))
    monkeypatch.setattr(launcher.os, "execve", execute)
    monkeypatch.chdir(tmp_path)
    arguments = ["ask", "literal $(touch injected) `whoami`", "--target", "name with spaces"]
    with pytest.raises(SystemExit) as outcome:
        launcher.main(["--home", str(tmp_path), *arguments])
    assert outcome.value.code == 0
    python, argv, environment = execute.call_args.args
    assert python == "/verified candidate/bin/python"
    assert argv == [python, "-I", "-m", "alice_codex", "--home", str(tmp_path), *arguments]
    assert "PYTHONPATH" not in environment and "PYTHONHOME" not in environment
    assert environment["PYTHONNOUSERSITE"] == "1"
    assert Path.cwd() == tmp_path
    manager.resolve_current.assert_called_once_with()
    manager.checked_current.assert_not_called()


def test_launcher_defaults_to_chat_and_fails_without_verified_release(tmp_path, monkeypatch):
    manager = Mock()
    monkeypatch.setattr(launcher, "ReleaseManager", lambda root: manager)
    execute = Mock(side_effect=SystemExit(0))
    monkeypatch.setattr(launcher.os, "execve", execute)
    monkeypatch.chdir(tmp_path)
    manager.resolve_current.return_value = None
    assert launcher.main(["--home", str(tmp_path)]) == 1
    execute.assert_not_called()
    manager.resolve_current.return_value = {"python": sys.executable}
    with pytest.raises(SystemExit):
        launcher.main(["--home", str(tmp_path)])
    assert execute.call_args.args[1][-1] == "chat"
