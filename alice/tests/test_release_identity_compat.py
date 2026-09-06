"""Identity compatibility and gate contracts with real synthetic installed wheels."""

import sys

import pytest

from alice_codex.files import write_json
from alice_codex.releases import ReleaseError, ReleaseManager
from test_releases import make_wheel, project as project


def prepare_identity_gate(source):
    # Structural gate fixture only; the production native file exercises actual
    # hook registration, delivery and rebinding through the installed package.
    (source / "tests/test_identity_native.py").write_text('''import os
import subprocess
import pytest
pytestmark = pytest.mark.native

def test_synthetic_installed_identity_contract():
    result = subprocess.run([os.environ["ALICE_ARTIFACT_PYTHON"], "-I", "-c", "from alice_codex.identity import IDENTITY_HOOK_COMPAT_VERSION; print(IDENTITY_HOOK_COMPAT_VERSION)"], capture_output=True, text=True, check=True)
    assert result.stdout.strip() == "1"
''')


def stage_identity(tmp_path, project, *, capability=1, version="0.0.1", manager=None):
    manager = manager or ReleaseManager(tmp_path / "runtime")
    candidate = manager.stage_wheel(
        make_wheel(tmp_path, version, resource_schema=2, identity_capability=capability),
        source_root=project[0], codex_binary=project[1], python=sys.executable,
    )
    return manager, candidate


@pytest.mark.parametrize("capability", [None, 0, 1])
def test_identity_capability_comes_from_actual_installed_module(tmp_path, project, capability):
    manager, candidate = stage_identity(tmp_path, project, capability=capability)
    folder, manifest = manager._manifest(candidate)
    assert manifest["installed"]["identity_hook_compat_version"] == (capability or 0)
    assert manager._probe_identity_capability(manager._python(folder), folder) == (capability or 0)


@pytest.mark.parametrize("capability", [True, "1", 2])
def test_invalid_identity_declaration_cannot_stage(tmp_path, project, capability):
    with pytest.raises(ReleaseError, match="invalid identity hook capability"):
        stage_identity(tmp_path, project, capability=capability)


def test_missing_identity_native_file_cannot_be_promoted(tmp_path, project):
    manager, candidate = stage_identity(tmp_path, project)
    report = manager.verify(candidate, native=True)
    assert report["passed"] is False
    assert "required native identity hook test is missing" in report["checks"][0]["detail"]


def test_missing_metadata_cannot_inherit_identity_native_evidence(tmp_path, project):
    prepare_identity_gate(project[0])
    manager, candidate = stage_identity(tmp_path, project)
    assert manager.verify(candidate, native=True)["promotable"]
    folder, manifest = manager._manifest(candidate)
    del manifest["installed"]["identity_hook_compat_version"]
    write_json(folder / "candidate.json", manifest)
    with pytest.raises(ReleaseError, match="identity hook capability"):
        manager.activate(candidate)
    assert manager.current() is None


def identity_header():
    return {
        "version": 1, "hook_compat_version": 1,
        "python": "/missing/damaged-old-candidate/python",
    }


def test_missing_old_python_does_not_block_capable_rebinding_but_old_rollback_is_refused(tmp_path, project):
    prepare_identity_gate(project[0])
    manager, old = stage_identity(tmp_path, project, capability=None)
    assert manager.verify(old, native=True)["promotable"]
    manager.activate(old)
    manager, capable = stage_identity(tmp_path, project, version="0.0.2", manager=manager)
    assert manager.verify(capable, native=True)["promotable"]
    path = manager.home / "state/identity-runtime.json"
    write_json(path, identity_header())
    pointer = manager.activate(capable)
    before = path.read_bytes()
    assert manager.checked_current() == pointer
    for rollback in (
        manager.rollback,
        lambda: manager.automatic_rollback(capable, {capable}, expected_epoch=pointer["activation_epoch"]),
    ):
        with pytest.raises(ReleaseError, match="identity hooks require"):
            rollback()
        assert manager.current() == pointer
        assert path.read_bytes() == before
    for invalid in ({"version": True, "hook_compat_version": 1},
                    {"version": 2, "hook_compat_version": 1},
                    {"version": 1, "hook_compat_version": 2}, {}):
        write_json(path, invalid)
        with pytest.raises(ReleaseError, match="identity"):
            manager.activate(capable)
        assert manager.current() == pointer


def test_owned_hook_before_manifest_is_a_capability_footprint(tmp_path, project):
    manager, old = stage_identity(tmp_path, project, capability=None)
    assert manager.verify(old, native=True)["promotable"]
    path = manager.home / "codex/config.toml"
    path.parent.mkdir(exist_ok=True)
    path.write_text('''[hooks]
[[hooks.UserPromptSubmit]]
[[hooks.UserPromptSubmit.hooks]]
type = "command"
statusMessage = "Alice identity snapshot v1"
command = "/missing/bad-candidate/python -I -m alice_codex.identity --workspace /synthetic --state-dir /synthetic --socket /synthetic"
''')
    before = path.read_bytes()
    assert not (manager.home / "state/identity-runtime.json").exists()
    with pytest.raises(ReleaseError, match="identity hooks require"):
        manager.activate(old)
    assert manager.current() is None and path.read_bytes() == before
    # An unrelated user's command is not adopted as Alice's identity format.
    path.write_text(path.read_text().replace("Alice identity snapshot v1", "Third-party status"))
    assert manager.activate(old)["current"] == old


def test_bootstrap_must_rebind_identity_before_it_can_host_new_identity_candidate(tmp_path, project):
    from alice_codex.bootstrap import install_runtime

    prepare_identity_gate(project[0])
    manager, old = stage_identity(tmp_path, project, capability=None)
    assert manager.verify(old, native=True)["promotable"]
    pointer = manager.activate(old)
    install_runtime(manager)
    write_json(manager.home / "state/supervisor.json", {"version": 2})
    manager, capable = stage_identity(tmp_path, project, version="0.0.2", manager=manager)
    assert manager.verify(capable, native=True)["promotable"]
    with pytest.raises(ReleaseError, match="uninstall.*identity hooks"):
        manager.activate(capable)
    assert manager.current() == pointer
    assert not (manager.home / "state/identity-runtime.json").exists()
