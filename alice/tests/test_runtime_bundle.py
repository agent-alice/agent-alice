"""Distribution identity and migration failures; synthetic executable fixtures."""

import json
from pathlib import Path
import subprocess

import pytest

from alice_codex.config import initialize_config, load_config, repin_codex_bundle
from alice_codex.files import SingletonLock, sha256_file
from alice_codex.runtime_bundle import (
    BUNDLE_MANIFEST,
    HOST_NAME,
    inspect_bundle,
    pin_bundle,
    runtime_bundle_status,
    verify_runtime_bundle,
)


@pytest.fixture
def distribution(tmp_path):
    root = tmp_path / "distribution"
    root.mkdir()
    binary = root / "codex"
    binary.write_text('#!/bin/sh\nprintf "codex-cli fixture\\n"\n')
    host = root / HOST_NAME
    host.write_text("#!/bin/sh\nexit 0\n")
    for path in (binary, host):
        path.chmod(0o700)
    return binary


def test_init_selects_complete_immutable_pair_without_changing_config_schema(
    tmp_path, distribution
):
    config = initialize_config(tmp_path / "alice", distribution)
    status = verify_runtime_bundle(config, require=True)
    assert status["paired"] and not status["native_verified"]
    assert Path(config.codex_binary).parent == Path(status["codex_code_mode_host"]).parent
    assert (
        sha256_file(Path(status["codex_code_mode_host"])) == status["codex_code_mode_host_sha256"]
    )
    assert "codex_code_mode_host" not in json.loads((config.root / "config.json").read_text())
    distribution.unlink()
    (distribution.parent / HOST_NAME).unlink()
    config.verify_binary()


def test_missing_host_fails_before_creating_runtime(tmp_path, distribution):
    (distribution.parent / HOST_NAME).unlink()
    home = tmp_path / "alice"
    with pytest.raises(ValueError, match="complete matching"):
        initialize_config(home, distribution)
    assert not home.exists()


def test_managed_distribution_prefers_its_resource_host(tmp_path, distribution):
    package = tmp_path / "managed"
    (package / "bin").mkdir(parents=True)
    (package / "codex-resources").mkdir()
    (package / "codex-package.json").write_text('{"version":"fixture"}')
    binary = package / "bin/codex"
    binary.write_bytes(distribution.read_bytes())
    binary.chmod(0o700)
    preferred = package / "codex-resources" / HOST_NAME
    preferred.write_bytes((distribution.parent / HOST_NAME).read_bytes())
    preferred.chmod(0o700)
    (package / "bin" / HOST_NAME).write_text("unrelated fallback")
    assert inspect_bundle(binary).host == preferred


def test_host_symlink_cannot_select_a_different_distribution(tmp_path, distribution):
    host = distribution.parent / HOST_NAME
    foreign = tmp_path / "different-host"
    foreign.write_bytes(host.read_bytes())
    foreign.chmod(0o700)
    host.unlink()
    host.symlink_to(foreign)
    with pytest.raises(ValueError, match="outside the selected distribution"):
        inspect_bundle(distribution)


@pytest.mark.parametrize("change", ["modify", "remove", "nonexecutable", "symlink"])
def test_companion_drift_blocks_startup(tmp_path, distribution, change):
    config = initialize_config(tmp_path / "alice", distribution)
    host = Path(verify_runtime_bundle(config)["codex_code_mode_host"])
    if change == "modify":
        host.write_text("changed")
    elif change == "remove":
        host.unlink()
    elif change == "nonexecutable":
        host.chmod(0o600)
    else:
        host.unlink()
        host.symlink_to(distribution.parent / HOST_NAME)
    with pytest.raises(ValueError):
        config.verify_binary()
    assert runtime_bundle_status(config)["status"] == "invalid"


def test_pair_identity_prevents_different_hosts_sharing_a_directory(tmp_path, distribution):
    first = pin_bundle(tmp_path / "alice", inspect_bundle(distribution), "codex-cli fixture")
    (distribution.parent / HOST_NAME).write_text("#!/bin/sh\nexit 2\n")
    second = pin_bundle(tmp_path / "alice", inspect_bundle(distribution), "codex-cli fixture")
    assert first.binary_sha256 == second.binary_sha256
    assert first.binary.parent != second.binary.parent
    assert first.host.read_text().endswith("exit 0\n")


def test_partial_copy_never_publishes_a_bundle(tmp_path, distribution, monkeypatch):
    from alice_codex import runtime_bundle

    original = runtime_bundle.shutil.copyfile

    def fail_host(source, destination):
        if Path(source).name == HOST_NAME:
            raise OSError("simulated missing companion copy")
        return original(source, destination)

    monkeypatch.setattr(runtime_bundle.shutil, "copyfile", fail_host)
    with pytest.raises(OSError, match="simulated"):
        initialize_config(tmp_path / "alice", distribution)
    assert not (tmp_path / "alice/config.json").exists()
    assert list((tmp_path / "alice/bin").iterdir()) == []


def test_distribution_change_during_version_probe_is_rejected(tmp_path, distribution, monkeypatch):
    from alice_codex import config as module

    original = module.subprocess.run

    def mutate_after_version(*args, **kwargs):
        result = original(*args, **kwargs)
        distribution.write_text('#!/bin/sh\nprintf "codex-cli different-build\\n"\n')
        return result

    monkeypatch.setattr(module.subprocess, "run", mutate_after_version)
    with pytest.raises(ValueError, match="changed"):
        initialize_config(tmp_path / "alice", distribution)
    assert not (tmp_path / "alice/config.json").exists()


def test_missing_pair_manifest_is_invalid_not_legacy(tmp_path, distribution):
    config = initialize_config(tmp_path / "alice", distribution)
    (Path(config.codex_binary).parent / BUNDLE_MANIFEST).unlink()
    assert runtime_bundle_status(config)["status"] == "invalid"
    with pytest.raises(ValueError, match="manifest is missing"):
        config.verify_binary()


def test_existing_different_bundle_is_never_overwritten(tmp_path, distribution):
    source = inspect_bundle(distribution)
    bundle = pin_bundle(tmp_path / "alice", source, "codex-cli fixture")
    bundle.host.write_text("changed")
    with pytest.raises(ValueError, match="changed"):
        pin_bundle(tmp_path / "alice", source, "codex-cli fixture")
    assert bundle.host.read_text() == "changed"


def test_unpinned_pair_is_registered_by_exact_primary_path_and_hash(tmp_path, distribution):
    config = initialize_config(tmp_path / "alice", distribution, pin_binary=False)
    assert config.codex_binary == str(distribution)
    assert verify_runtime_bundle(config, require=True)["paired"]
    assert not (distribution.parent / BUNDLE_MANIFEST).exists()
    assert len(list((config.root / "state/codex-bundles").glob("*.json"))) == 1
    (distribution.parent / HOST_NAME).write_text("changed")
    with pytest.raises(ValueError, match="changed"):
        config.verify_binary()


def legacy_config(tmp_path, distribution):
    config = initialize_config(tmp_path / "alice", distribution)
    # Recreate an actual legacy main-only pin and config without a bundle record.
    legacy = config.root / "bin/codex-legacy"
    legacy.write_bytes(distribution.read_bytes())
    legacy.chmod(0o700)
    config.codex_binary = str(legacy)
    config.save()
    return config


def test_legacy_runtime_is_readable_but_not_claimed_paired(tmp_path, distribution):
    config = legacy_config(tmp_path, distribution)
    assert load_config(config.root) == config
    config.verify_binary()
    assert runtime_bundle_status(config)["status"] == "unverified_legacy"
    with pytest.raises(ValueError, match="explicitly repin"):
        verify_runtime_bundle(config, require=True)


def test_repin_preserves_records_unknown_fields_and_old_executable(tmp_path, distribution):
    config = legacy_config(tmp_path, distribution)
    original_binary = Path(config.codex_binary)
    path = config.root / "config.json"
    value = json.loads(path.read_text())
    value["future_metadata"] = {"preserve": [1, 2]}
    path.write_text(json.dumps(value))
    toml = (config.codex_home / "config.toml").read_bytes()
    history = config.workspace / "history.json"
    history.write_text('{"synthetic":"keep"}')
    result = repin_codex_bundle(config.root, distribution)
    after = json.loads(path.read_text())
    assert result["paired"]
    assert {key: item for key, item in after.items() if key != "codex_binary"} == {
        key: item for key, item in value.items() if key != "codex_binary"
    }
    assert original_binary.exists()
    assert after["codex_binary"] != str(original_binary)
    assert (config.codex_home / "config.toml").read_bytes() == toml
    assert history.read_text() == '{"synthetic":"keep"}'


def test_repin_rejects_another_primary_version(tmp_path, distribution):
    config = legacy_config(tmp_path, distribution)
    original = (config.root / "config.json").read_bytes()
    distribution.write_text('#!/bin/sh\nprintf "codex-cli fixture-2\\n"\n')
    with pytest.raises(ValueError, match="recorded primary hash"):
        repin_codex_bundle(config.root, distribution)
    assert (config.root / "config.json").read_bytes() == original


def test_repin_failure_before_config_switch_keeps_previous_runtime(
    tmp_path, distribution, monkeypatch
):
    from alice_codex import config as module

    config = legacy_config(tmp_path, distribution)
    original = (config.root / "config.json").read_bytes()

    def fail(*args):
        raise OSError("simulated config switch failure")

    monkeypatch.setattr(module, "write_json", fail)
    with pytest.raises(OSError, match="simulated config"):
        repin_codex_bundle(config.root, distribution)
    assert (config.root / "config.json").read_bytes() == original
    assert Path(load_config(config.root).codex_binary).is_file()


def test_repin_refuses_running_service_lock(tmp_path, distribution):
    config = legacy_config(tmp_path, distribution)
    with SingletonLock(config.root / "state/service.lock"):
        with pytest.raises(RuntimeError, match="Another Alice"):
            repin_codex_bundle(config.root, distribution)


def test_shared_protocol_fixture_never_claims_code_mode_execution():
    host = Path(__file__).parent / "fixtures" / HOST_NAME
    result = subprocess.run([str(host)], capture_output=True, text=True, timeout=5)
    assert result.returncode == 78
    assert "not implemented" in result.stderr
