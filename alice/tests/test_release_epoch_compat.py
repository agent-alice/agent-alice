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
