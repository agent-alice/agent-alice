"""Real installed-wheel compatibility probes, using synthetic service packages.

The small native-named fixture below tests gate selection and subprocess imports.
It does not stand in for the production Service's native epoch recovery test.
"""

import json
import sys

import pytest

from alice_codex.files import sha256_file, write_json
from alice_codex.releases import ReleaseError, ReleaseManager
from test_releases import make_wheel, project as project


def prepare_epoch_gate(source):
    (source / "tests/test_native_service_resource_epochs.py").write_text('''import os
import subprocess
from pathlib import Path
import hashlib
import pytest
pytestmark = [pytest.mark.native, pytest.mark.native_resource_epoch]

def test_synthetic_installed_declaration_fixture():
    result = subprocess.run([os.environ["ALICE_ARTIFACT_PYTHON"], "-I", "-c", "from alice_codex.service import RESOURCE_EPOCH_CAPABILITY; print(RESOURCE_EPOCH_CAPABILITY)"], capture_output=True, text=True, check=True)
    assert result.stdout.strip() == "1"
    binary = Path(os.environ["ALICE_TEST_CODEX_BINARY"])
    host = Path(os.environ["ALICE_TEST_CODEX_HOST_BINARY"])
    assert binary.parent == host.parent
    assert hashlib.sha256(binary.read_bytes()).hexdigest() == os.environ["ALICE_TEST_CODEX_SHA256"]
    assert hashlib.sha256(host.read_bytes()).hexdigest() == os.environ["ALICE_TEST_CODEX_HOST_SHA256"]
''')


def stage_epoch(tmp_path, project, *, capability=1, version="0.0.1", manager=None):
    manager = manager or ReleaseManager(tmp_path / "runtime")
    candidate = manager.stage_wheel(
        make_wheel(tmp_path, version, resource_schema=2, epoch_capability=capability),
        source_root=project[0],
        codex_binary=project[1],
        python=sys.executable,
    )
    return manager, candidate


@pytest.mark.parametrize("capability", [None, 0, 1])
def test_capability_is_read_from_the_actual_installed_service(tmp_path, project, capability):
    manager, candidate = stage_epoch(tmp_path, project, capability=capability)
    folder, manifest = manager._manifest(candidate)
    expected = capability or 0
    assert manifest["installed"]["resource_schema"] == 2
    assert manifest["installed"]["resource_epoch_capability"] == expected
    assert manager._probe_epoch_capability(manager._python(folder), folder) == expected


@pytest.mark.parametrize("capability", [True, "1", -1, 2])
def test_invalid_or_unknown_installed_declaration_cannot_stage(tmp_path, project, capability):
    with pytest.raises(ReleaseError, match="invalid resource epoch capability"):
        stage_epoch(tmp_path, project, capability=capability)


@pytest.mark.parametrize("change", ["missing", "lowered", "bool"])
def test_changed_manifest_capability_cannot_inherit_a_verified_report(tmp_path, project, change):
    prepare_epoch_gate(project[0])
    manager, candidate = stage_epoch(tmp_path, project)
    assert manager.verify(candidate, native=True)["promotable"]
    folder, manifest = manager._manifest(candidate)
    if change == "missing":
        del manifest["installed"]["resource_epoch_capability"]
    else:
        manifest["installed"]["resource_epoch_capability"] = 0 if change == "lowered" else True
    write_json(folder / "candidate.json", manifest)
    with pytest.raises(ReleaseError, match="resource epoch capability"):
        manager.activate(candidate)
    assert manager.current() is None


def test_missing_report_declaration_cannot_reuse_new_capability_evidence(tmp_path, project):
    prepare_epoch_gate(project[0])
    manager, candidate = stage_epoch(tmp_path, project)
    assert manager.verify(candidate, native=True)["promotable"]
    folder, manifest = manager._manifest(candidate)
    report = json.loads((folder / "verification.json").read_text())
    del report["resource_epoch_capability"]
    write_json(folder / "verification.json", report)
    manifest["verified_report_sha256"] = sha256_file(folder / "verification.json")
    write_json(folder / "candidate.json", manifest)
    with pytest.raises(ReleaseError, match="required checks"):
        manager.activate(candidate)


def test_capable_candidate_requires_its_explicit_native_service_epoch_gate(tmp_path, project):
    manager, candidate = stage_epoch(tmp_path, project)
    report = manager.verify(candidate, native=True)
    assert report["passed"] is False
    assert "required native Service resource epoch test is missing" in report["checks"][0]["detail"]
    with pytest.raises(ReleaseError, match="verification"):
        manager.activate(candidate)


@pytest.mark.parametrize("tamper_bootstrap", [False, True])
def test_old_bootstrap_cannot_introduce_epochs_even_before_first_journal(
    tmp_path, project, tamper_bootstrap
):
    from alice_codex.bootstrap import install_runtime

    prepare_epoch_gate(project[0])
    manager, old = stage_epoch(tmp_path, project, capability=None)
    assert manager.verify(old, native=True)["promotable"]
    original = manager.activate(old)
    bootstrap = install_runtime(manager)
    if tamper_bootstrap:
        path = manager.home / "bootstrap" / bootstrap["generation"] / "bootstrap.json"
        value = json.loads(path.read_text())
        value["installed"]["resource_epoch_capability"] = 1
        write_json(path, value)
    write_json(manager.home / "state/supervisor.json", {"version": 2})
    manager, new = stage_epoch(tmp_path, project, version="0.0.2", manager=manager)
    assert manager.verify(new, native=True)["promotable"]
    expected = "capability metadata changed" if tamper_bootstrap else "uninstall.*resource epochs"
    with pytest.raises(ReleaseError, match=expected):
        manager.activate(new)
    assert manager.current() == original
    assert not (manager.home / "state/runtime.json").exists()


def epoch_state():
    return {
        "version": 1,
        "tasks": {},
        "intents": {},
        "server": None,
        "resource_epochs": {"synthetic-epoch": {"state": "prepared", "server": None}},
    }


def install_epoch_data(manager, state=None):
    import sqlite3

    database = manager.home / "state/resources.sqlite3"
    database.parent.mkdir(exist_ok=True)
    with sqlite3.connect(database) as connection:
        connection.execute("PRAGMA user_version=2")
        connection.execute("CREATE TABLE retained_evidence(value TEXT)")
        connection.execute("INSERT INTO retained_evidence VALUES('new synthetic observation')")
    write_json(manager.home / "state/runtime.json", state or epoch_state())


def test_schema_two_legacy_candidate_cannot_spawn_over_a_prepared_epoch(tmp_path, project):
    manager, old = stage_epoch(tmp_path, project, capability=None)
    assert manager.verify(old, native=True)["promotable"]
    install_epoch_data(manager)
    before = (manager.home / "state/runtime.json").read_bytes()
    with pytest.raises(ReleaseError, match="resource epoch"):
        manager.activate(old)
    assert manager.current() is None
    assert (manager.home / "state/runtime.json").read_bytes() == before


def test_capable_candidate_accepts_known_journal_and_rejects_corrupt_variants(tmp_path, project):
    import copy

    prepare_epoch_gate(project[0])
    manager, candidate = stage_epoch(tmp_path, project)
    assert manager.verify(candidate, native=True)["promotable"]
    install_epoch_data(manager)
    pointer = manager.activate(candidate)
    assert manager.checked_current() == pointer
    variants = []
    for value in (None, [], {"": {"state": "prepared", "server": None}},
                  {"epoch": {"state": "unknown", "server": None}},
                  {"epoch": {"state": "prepared", "server": {"pid": 123}}},
                  {"epoch": {"state": "bound", "server": {"pid": True, "birth": "born", "identity": "owned"}}}):
        state = epoch_state()
        state["resource_epochs"] = value
        variants.append(state)
    wrong_version = epoch_state()
    wrong_version["version"] = True
    variants.append(wrong_version)
    missing_binding = epoch_state()
    missing_binding["server"] = {
        "resource_epoch_id": "missing", "pid": 123, "birth": "born", "identity": "owned"
    }
    variants.append(missing_binding)
    mismatch = copy.deepcopy(missing_binding)
    mismatch["resource_epochs"]["missing"] = {
        "state": "bound", "server": {"pid": 123, "birth": "other birth", "identity": "owned"}
    }
    variants.append(mismatch)
    path = manager.home / "state/runtime.json"
    for state in variants:
        write_json(path, state)
        before = path.read_bytes()
        with pytest.raises(ReleaseError, match="resource epoch"):
            manager.activate(candidate)
        assert manager.current() == pointer
        assert path.read_bytes() == before


def test_manual_and_automatic_rollback_preserve_prepared_journal_and_new_data(tmp_path, project):
    import sqlite3

    prepare_epoch_gate(project[0])
    manager, old = stage_epoch(tmp_path, project, capability=None)
    assert manager.verify(old, native=True)["promotable"]
    manager.activate(old)
    manager, capable = stage_epoch(tmp_path, project, version="0.0.2", manager=manager)
    assert manager.verify(capable, native=True)["promotable"]
    pointer = manager.activate(capable)
    install_epoch_data(manager)
    path = manager.home / "state/runtime.json"
    before = path.read_bytes()
    with pytest.raises(ReleaseError, match="resource epoch"):
        manager.rollback()
    assert manager.current() == pointer
    with pytest.raises(ReleaseError, match="resource epoch"):
        manager.automatic_rollback(capable, {capable}, expected_epoch=pointer["activation_epoch"])
    assert manager.current() == pointer
    assert manager.checked_current() == pointer
    assert path.read_bytes() == before
    with sqlite3.connect(manager.home / "state/resources.sqlite3") as connection:
        assert connection.execute("SELECT value FROM retained_evidence").fetchone()[0] == "new synthetic observation"


async def test_supervisor_blocks_before_starting_an_epoch_unaware_service(tmp_path, project, monkeypatch):
    from alice_codex.config import RuntimeConfig
    from alice_codex.supervisor import Supervisor

    manager, old = stage_epoch(tmp_path, project, capability=None)
    assert manager.verify(old, native=True)["promotable"]
    manager.activate(old)
    install_epoch_data(manager)
    manifest = manager._manifest(old)[1]
    config = RuntimeConfig(str(manager.home), str(project[1]), "codex-cli test-fixture", sha256_file(project[1]))
    config.prepare_directories()
    path = manager.home / "state/runtime.json"
    before = path.read_bytes()
    supervisor = Supervisor(config, bootstrap_manifest=manifest)

    def unexpected(*args, **kwargs):
        pytest.fail("incompatible journal must be rejected before starting a service")

    monkeypatch.setattr(supervisor, "launch_command", unexpected)
    # run() propagates initial compatibility errors; the installed supervisor
    # main() records the blocked diagnostic. Neither path may reach spawn.
    with pytest.raises(ReleaseError, match="resource epoch"):
        await supervisor.run()
    assert supervisor.process is None
    assert path.read_bytes() == before
