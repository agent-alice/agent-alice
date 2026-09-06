"""The public maintenance boundary owns real kernel locks for its whole body."""

import subprocess
import sys

import pytest

from alice_codex import launchd
from alice_codex.config import RuntimeConfig
from alice_codex.files import SingletonLock, write_json
from alice_codex.lifecycle import offline_maintenance


@pytest.fixture
def config(tmp_path):
    result = RuntimeConfig(str(tmp_path), "/usr/bin/true", "fixture", "fixture")
    result.prepare_directories()
    return result


def try_lock(path):
    return subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            "import fcntl,sys; f=open(sys.argv[1],'a'); "
            "fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)",
            str(path),
        ],
        capture_output=True,
        timeout=5,
    ).returncode


def test_maintenance_holds_all_owner_locks_until_body_exits(config):
    names = ("lifecycle.lock", "bootstrap.lock", "service.lock")
    with offline_maintenance(config):
        assert all(try_lock(config.root / "state" / name) != 0 for name in names)
    assert all(try_lock(config.root / "state" / name) == 0 for name in names)


@pytest.mark.parametrize("name", ["lifecycle.lock", "bootstrap.lock", "service.lock"])
def test_startup_owner_without_socket_prevents_maintenance(config, name):
    with SingletonLock(config.root / "state" / name):
        with pytest.raises(RuntimeError, match="owns this data directory"):
            with offline_maintenance(config):
                pytest.fail("maintenance must not start")


def test_loaded_supervisor_before_process_lock_or_socket_is_rejected(config, monkeypatch):
    write_json(
        config.root / "state/supervisor.json",
        {"label": launchd.label(config), "plist": "/synthetic/own.plist"},
    )
    monkeypatch.setattr(
        launchd,
        "_command",
        lambda *args, **kwargs: subprocess.CompletedProcess(args, 0, "state = waiting\n", ""),
    )
    with pytest.raises(ValueError, match="Stop the installed supervisor"):
        with offline_maintenance(config):
            pytest.fail("loaded supervision may still start its child")


def test_exception_releases_all_maintenance_locks(config):
    with pytest.raises(ValueError, match="synthetic"):
        with offline_maintenance(config):
            raise ValueError("synthetic maintenance failure")
    with offline_maintenance(config):
        pass


def test_remaining_native_socket_is_not_assumed_offline(config):
    config.codex_socket.touch()
    try:
        with pytest.raises(RuntimeError, match="native socket remains"):
            with offline_maintenance(config):
                pytest.fail("unknown native socket must be diagnosed")
    finally:
        config.codex_socket.unlink()
