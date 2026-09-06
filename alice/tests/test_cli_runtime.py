"""Public runtime commands with synthetic distributions and real file changes."""

import json
from pathlib import Path
import subprocess

import pytest

from alice_codex import cli, launchd
from alice_codex.config import RuntimeConfig, load_config, repin_codex_bundle
from alice_codex.files import SingletonLock, sha256_file, write_json
from alice_codex.runtime_bundle import HOST_NAME


@pytest.fixture
def runtime(tmp_path):
    source = tmp_path / "distribution"
    source.mkdir()
    binary = source / "codex"
    binary.write_text('#!/bin/sh\nprintf "codex-cli fixture\\n"\n')
    host = source / HOST_NAME
    host.write_text("#!/bin/sh\n# Synthetic fixture has no Code Mode implementation.\nexit 78\n")
    binary.chmod(0o700)
    host.chmod(0o700)
    home = tmp_path / "alice"
    home.mkdir()
    legacy = home / "legacy-codex"
    legacy.write_bytes(binary.read_bytes())
    legacy.chmod(0o700)
    config = RuntimeConfig(str(home), str(legacy), "codex-cli fixture", sha256_file(legacy))
    config.prepare_directories()
    config.save()
    value = json.loads((home / "config.json").read_text())
    value["future_preserved_setting"] = {"keep": True}
    write_json(home / "config.json", value)
    return config, binary


def arguments(config, *args):
    return cli.parser().parse_args(["--home", config.home, *args])


async def test_status_reports_legacy_pair_without_adopting_a_nearby_host(runtime):
    config, _ = runtime
    result = await cli.execute(arguments(config, "runtime", "status"))
    assert result == {"status": "unverified_legacy", "paired": False, "native_verified": False}


async def test_repin_uses_shared_guard_without_reentrant_service_lock(runtime):
    config, binary = runtime
    marker = config.root / "memory-state/new-evidence.txt"
    marker.parent.mkdir(exist_ok=True)
    marker.write_text("preserve newer evidence")
    old_binary = config.codex_binary
    result = await cli.execute(arguments(config, "runtime", "repin", "--codex", str(binary)))
    assert result["paired"] and not result["native_verified"]
    current = load_config(config.root, for_maintenance=True)
    assert current.codex_binary != old_binary
    assert Path(old_binary).is_file()
    assert marker.read_text() == "preserve newer evidence"
    assert json.loads((config.root / "config.json").read_text())["future_preserved_setting"] == {
        "keep": True
    }
    assert (await cli.execute(arguments(current, "runtime", "status")))[
        "status"
    ] == "verified_files"
    with pytest.raises(ValueError, match="configuration fields"):
        load_config(config.root)  # Runtime startup still rejects unknown executable semantics.


async def test_repin_rejects_loaded_supervisor_before_control_exists(runtime, monkeypatch):
    config, binary = runtime
    before = (config.root / "config.json").read_bytes()
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
        await cli.execute(arguments(config, "runtime", "repin", "--codex", str(binary)))
    assert (config.root / "config.json").read_bytes() == before


async def test_repin_cannot_cross_an_active_start_transition(runtime):
    config, binary = runtime
    before = (config.root / "config.json").read_bytes()
    with SingletonLock(config.root / "state/lifecycle.lock"):
        with pytest.raises(RuntimeError, match="owns this data directory"):
            await cli.execute(arguments(config, "runtime", "repin", "--codex", str(binary)))
    assert (config.root / "config.json").read_bytes() == before


def test_doctor_reports_unpaired_legacy_and_nonzero_health(runtime, capsys):
    config, _ = runtime
    assert cli.main(["--home", config.home, "doctor"]) == 1
    result = json.loads(capsys.readouterr().out)
    assert result["codex_binary_verified"] is True
    assert result["runtime_bundle"]["status"] == "unverified_legacy"
    assert result["healthy"] is False


def test_doctor_reports_companion_drift_instead_of_a_false_healthy_exit(runtime, capsys):
    config, binary = runtime
    assert cli.main(["--home", config.home, "runtime", "repin", "--codex", str(binary)]) == 0
    result = json.loads(capsys.readouterr().out)
    Path(result["codex_code_mode_host"]).write_text("damaged synthetic host")
    assert cli.main(["--home", config.home, "doctor"]) == 1
    result = json.loads(capsys.readouterr().out)
    assert result["healthy"] is False
    assert result["runtime_bundle"]["status"] == "invalid"


def test_doctor_keeps_unsupported_config_distinct_from_verified_pair(runtime, capsys):
    config, binary = runtime
    assert cli.main(["--home", config.home, "runtime", "repin", "--codex", str(binary)]) == 0
    capsys.readouterr()
    assert cli.main(["--home", config.home, "doctor"]) == 1
    result = json.loads(capsys.readouterr().out)
    assert result["runtime_bundle"]["status"] == "verified_files"
    assert result["config_parsed"] is False
    assert "configuration fields" in result["config_error"]
    assert result["healthy"] is False


def test_standalone_repin_rejects_linked_state_before_touching_external_directory(runtime):
    config, binary = runtime
    state = config.root / "state"
    state.rmdir()
    external = config.root.parent / "unrelated"
    external.mkdir(mode=0o755)
    external.chmod(0o755)
    marker = external / "evidence.txt"
    marker.write_text("unrelated content")
    state.symlink_to(external, target_is_directory=True)
    before = (config.root / "config.json").read_bytes()
    with pytest.raises(ValueError, match="symbolic links"):
        repin_codex_bundle(config.root, binary)
    assert external.stat().st_mode & 0o777 == 0o755
    assert list(external.iterdir()) == [marker]
    assert marker.read_text() == "unrelated content"
    assert (config.root / "config.json").read_bytes() == before
