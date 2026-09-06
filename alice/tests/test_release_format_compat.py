"""Installed synthetic packages test gate contracts, not native agent behavior."""

import copy
import json
import sys

import pytest

from alice_codex.files import sha256_file, write_json
from alice_codex.releases import ReleaseError, ReleaseManager, summary_boundary_evidence
from test_releases import make_wheel, project as project


def boundary_report():
    names = (
        "committed_missing_count_tampering_rejected",
        "pending_rendered_and_hash_forgery_rejected",
        "copied_l1_jsonl_without_local_proof_rejected",
        "giant_copied_l1_tail_proof_without_local_proof_rejected",
        "copied_l2_markdown_without_local_proof_rejected_by_l3",
        "copied_l3_markdown_without_local_proof_rejected_by_l4",
        "malformed_reserved_markdown_coverage_marker_rejected",
        "valid_local_markdown_new_date_retains_ancestor_gap",
    )
    return {
        "status": "passed", "full_boundary_suite": True,
        "model_or_native_execution": False, "isolated_python": True,
        "cases": [
            {"case": name, "status": "passed", "debug_only": False}
            for name in ("single_17mib", "multiple_over_16mib", "short_10001")
        ],
        "regression_guards_passed": True, "regression_guard_count": 8,
        "guards": [{"name": name, "passed": True} for name in names],
    }


def prepare_format_gates(source, *, evidence=None):
    for filename, module, constant, expected in (
        ("test_native_summary_partitions.py", "summary_partitions", "SUMMARY_COMMIT_SCHEMA", 2),
        ("test_native_heartbeat_source_config.py", "heartbeat", "HEARTBEAT_SOURCES_CONFIG_VERSION", 1),
    ):
        (source / "tests" / filename).write_text(f'''import os
import subprocess
import pytest
pytestmark = pytest.mark.native

def test_synthetic_installed_format_contract():
    assert os.environ.get("ALICE_SUMMARY_RESOURCE_RECEIPTS") {"== '1'" if expected == 2 else "is None"}
    assert "ALICE_SUMMARY_DIAGNOSTICS" not in os.environ
    result = subprocess.run([os.environ["ALICE_ARTIFACT_PYTHON"], "-I", "-c", "from alice_codex.{module} import {constant}; print({constant})"], capture_output=True, text=True, check=True)
    assert result.stdout.strip() == "{expected}"
''')
    fixtures = source / "tests/fixtures"
    fixtures.mkdir(exist_ok=True)
    encoded = json.dumps(boundary_report() if evidence is None else evidence)
    # Deliberately synthetic output: this checks the release parser and actual
    # isolated interpreter boundary. The production script performs the work.
    (fixtures / "summary_partition_probe.py").write_text(
        "import json\nimport sys\nfrom alice_codex.summary_partitions import SUMMARY_COMMIT_SCHEMA\n"
        "assert SUMMARY_COMMIT_SCHEMA == 2\nassert sys.flags.isolated\n"
        "assert len(sys.argv) == 1\n" + f"print(json.dumps(json.loads({encoded!r})))\n"
    )


def stage_formats(tmp_path, project, *, summary=2, heartbeat=1, version="0.0.1", manager=None):
    manager = manager or ReleaseManager(tmp_path / "runtime")
    candidate = manager.stage_wheel(
        make_wheel(tmp_path, version, summary_capability=summary, heartbeat_capability=heartbeat),
        source_root=project[0], codex_binary=project[1], python=sys.executable,
    )
    return manager, candidate


def header(schema=2, status="committed"):
    value = {
        "schema_version": schema, "batch_id": "a" * 64, "status": status,
        "manifest": {"schema_version": schema},
    }
    if schema == 2:
        value.update(format="summary-partition-v1", partition_plan_sha256=value["batch_id"])
        value["manifest"]["partition_plan"] = value["batch_id"]
    return value


def install_header(manager, value):
    path = manager.home / "memory-state/commits" / ("a" * 64 + ".json")
    write_json(path, value)
    return path


@pytest.mark.parametrize("summary,heartbeat", [(None, None), (0, 0), (1, 0), (2, 1)])
def test_format_capabilities_are_probed_from_installed_bytes(tmp_path, project, summary, heartbeat):
    manager, candidate = stage_formats(tmp_path, project, summary=summary, heartbeat=heartbeat)
    folder, manifest = manager._manifest(candidate)
    installed = manifest["installed"]
    assert installed["summary_commit_schema"] == (summary or 0)
    assert installed["heartbeat_sources_config_version"] == (heartbeat or 0)
    assert manager._probe_summary_capability(manager._python(folder), folder) == (summary or 0)
    assert manager._probe_heartbeat_capability(manager._python(folder), folder) == (heartbeat or 0)


@pytest.mark.parametrize("field,bad", [
    ("summary", True), ("summary", 3), ("heartbeat", "1"), ("heartbeat", 2),
])
def test_invalid_installed_format_declaration_cannot_stage(tmp_path, project, field, bad):
    with pytest.raises(ReleaseError, match="invalid .*capability"):
        stage_formats(tmp_path, project, **{field: bad})


def test_new_explicit_zero_report_can_promote_only_before_format_footprints(tmp_path, project):
    manager, candidate = stage_formats(tmp_path, project, summary=None, heartbeat=None)
    report = manager.verify(candidate, native=True)
    assert report["promotable"], report
    assert report["summary_commit_schema"] == report["heartbeat_sources_config_version"] == 0
    pointer = manager.activate(candidate)
    assert manager.checked_current() == pointer
    path = install_header(manager, header(schema=1))
    with pytest.raises(ReleaseError, match="summary commit"):
        manager.checked_current()
    assert manager.current() == pointer
    assert json.loads(path.read_text()) == header(schema=1)


@pytest.mark.parametrize("field", ["summary_commit_schema", "heartbeat_sources_config_version"])
def test_missing_even_zero_metadata_or_report_never_inherits_previous_receipt(tmp_path, project, field):
    manager, candidate = stage_formats(tmp_path, project, summary=None, heartbeat=None)
    assert manager.verify(candidate, native=True)["promotable"]
    folder, manifest = manager._manifest(candidate)
    report_path = folder / "verification.json"
    original_report = report_path.read_bytes()
    missing = copy.deepcopy(manifest)
    del missing["installed"][field]
    write_json(folder / "candidate.json", missing)
    with pytest.raises(ReleaseError, match="metadata; restage"):
        manager.verify(candidate, native=True)
    assert report_path.read_bytes() == original_report
    with pytest.raises(ReleaseError, match="metadata; restage"):
        manager.activate(candidate)
    report = json.loads(original_report)
    del report[field]
    write_json(report_path, report)
    manifest["verified_report_sha256"] = sha256_file(report_path)
    write_json(folder / "candidate.json", manifest)
    with pytest.raises(ReleaseError, match="required checks"):
        manager.activate(candidate)
    assert manager.current() is None


def test_full_format_gate_selects_each_native_group_once_and_binds_actual_capabilities(tmp_path, project, monkeypatch):
    # Parent-shell flags cannot disable strict receipts or select a diagnostic
    # wrapper in the actual native-summary verification subprocess.
    monkeypatch.setenv("ALICE_SUMMARY_RESOURCE_RECEIPTS", "0")
    monkeypatch.setenv("ALICE_SUMMARY_DIAGNOSTICS", "1")
    evidence = boundary_report()
    evidence["synthetic_metrics_padding"] = "x" * 13000
    prepare_format_gates(project[0], evidence=evidence)
    manager, candidate = stage_formats(tmp_path, project)
    report = manager.verify(candidate, native=True)
    assert report["promotable"], report
    checks = {check["name"]: check for check in report["checks"]}
    assert {"summary_boundary", "native_summary", "native_heartbeat_sources"} <= checks.keys()
    assert "--ignore=tests/test_native_summary_partitions.py" in checks["native"]["command"]
    assert "--ignore=tests/test_native_heartbeat_source_config.py" in checks["native"]["command"]
    assert checks["summary_boundary"]["command"][1] == "-I"
    assert len(checks["summary_boundary"]["command"]) == 3
    assert len(checks["summary_boundary"]["output"]) > 12000
    assert summary_boundary_evidence(checks["summary_boundary"]["output"]) is None
    folder, manifest = manager._manifest(candidate)
    pointer = manager.activate(candidate)
    for field in ("summary_commit_schema", "heartbeat_sources_config_version"):
        changed = copy.deepcopy(manifest)
        changed["installed"][field] = 0
        write_json(folder / "candidate.json", changed)
        with pytest.raises(ReleaseError, match="capability.*metadata"):
            manager.checked_current()
        assert manager.current() == pointer
    write_json(folder / "candidate.json", manifest)
    assert manager.checked_current() == pointer


@pytest.mark.parametrize("missing", [
    "tests/fixtures/summary_partition_probe.py",
    "tests/test_native_summary_partitions.py",
    "tests/test_native_heartbeat_source_config.py",
])
def test_required_format_file_cannot_disappear_from_collection(tmp_path, project, missing):
    prepare_format_gates(project[0])
    (project[0] / missing).unlink()
    manager, candidate = stage_formats(tmp_path, project)
    report = manager.verify(candidate, native=True)
    assert not report["passed"] and not report["promotable"]
    assert "missing" in report["checks"][0]["detail"]


def test_exit_zero_partial_probe_and_empty_native_group_block_promotion(tmp_path, project):
    report = boundary_report()
    report["guards"].pop()
    prepare_format_gates(project[0], evidence=report)
    (project[0] / "tests/test_native_heartbeat_source_config.py").write_text("# No native tests\n")
    manager, candidate = stage_formats(tmp_path, project)
    report = manager.verify(candidate, native=True)
    checks = {check["name"]: check for check in report["checks"]}
    assert checks["summary_boundary"]["returncode"] == 0
    assert checks["summary_boundary"]["status"] == "failed"
    assert "eight summary" in checks["summary_boundary"]["detail"]
    assert checks["native_heartbeat_sources"]["status"] == "failed"
    assert not report["promotable"]
    with pytest.raises(ReleaseError, match="verification"):
        manager.activate(candidate)


@pytest.mark.parametrize("field,value", [
    ("full_boundary_suite", False), ("isolated_python", 1),
    ("model_or_native_execution", True), ("regression_guards_passed", 1),
    ("regression_guard_count", True), ("regression_guard_count", 7),
    ("guards", []), ("cases", []), ("status", "failed"),
])
def test_probe_flags_must_be_complete_and_typed(field, value):
    report = boundary_report()
    report[field] = value
    assert summary_boundary_evidence(json.dumps(report)) is not None


@pytest.mark.parametrize("change", ["guard_false", "guard_duplicate", "debug", "case_duplicate", "case_type"])
def test_probe_case_and_guard_details_cannot_be_replaced_by_success_flag(change):
    report = boundary_report()
    if change == "guard_false":
        report["guards"][0]["passed"] = False
    elif change == "guard_duplicate":
        report["guards"][0] = report["guards"][1]
    elif change == "debug":
        report["cases"][0]["debug_only"] = True
    elif change == "case_duplicate":
        report["cases"][0] = report["cases"][1]
    else:
        report["cases"][0]["case"] = []
    assert summary_boundary_evidence(json.dumps(report)) is not None


@pytest.mark.parametrize("capability,schema,status,allowed", [
    (0, 1, "committed", False), (1, 1, "pending", True),
    (1, 2, "partitioning", False), (1, 2, "pending", False),
    (1, 2, "committed", False), (2, 1, "committed", True),
    (2, 2, "partitioning", True), (2, 2, "pending", True), (2, 2, "committed", True),
])
def test_all_summary_commit_states_use_shared_header_validator(tmp_path, capability, schema, status, allowed):
    manager = ReleaseManager(tmp_path)
    path = install_header(manager, header(schema, status))
    before = path.read_bytes()
    manifest = {"installed": {"summary_commit_schema": capability}}
    if allowed:
        manager._check_summary_commit_compat(manifest)
    else:
        with pytest.raises(ReleaseError, match="summary commit"):
            manager._check_summary_commit_compat(manifest)
    assert path.read_bytes() == before


@pytest.mark.parametrize("change", ["unknown_schema", "bool_schema", "bad_batch", "wrong_plan", "broken_json"])
def test_capable_candidate_rejects_corrupt_summary_headers_without_rewriting_them(tmp_path, change):
    manager = ReleaseManager(tmp_path)
    value = header()
    if change == "unknown_schema":
        value["schema_version"] = 3
    elif change == "bool_schema":
        value["schema_version"] = True
    elif change == "bad_batch":
        value["batch_id"] = "invalid"
    elif change == "wrong_plan":
        value["partition_plan_sha256"] = "b" * 64
    path = install_header(manager, value)
    if change == "broken_json":
        path.write_text("{")
    before = path.read_bytes()
    with pytest.raises(ReleaseError, match="summary commit"):
        manager._check_summary_commit_compat({"installed": {"summary_commit_schema": 2}})
    assert path.read_bytes() == before


@pytest.mark.parametrize("raw,valid", [
    (None, True), ({"version": 1, "sources": []}, True),
    ({"version": True, "sources": []}, False), ({}, False), ([], False),
])
def test_heartbeat_key_presence_requires_capability_even_if_null_or_empty(tmp_path, raw, valid):
    manager = ReleaseManager(tmp_path)
    write_json(tmp_path / "config.json", {})
    manager._check_heartbeat_sources_compat({"installed": {}})
    write_json(tmp_path / "config.json", {"heartbeat_sources": raw})
    before = (tmp_path / "config.json").read_bytes()
    with pytest.raises(ReleaseError, match="source-config-capable"):
        manager._check_heartbeat_sources_compat({"installed": {}})
    capable = {"installed": {"heartbeat_sources_config_version": 1}}
    if valid:
        manager._check_heartbeat_sources_compat(capable)
    else:
        with pytest.raises(ReleaseError, match="invalid heartbeat"):
            manager._check_heartbeat_sources_compat(capable)
    assert (tmp_path / "config.json").read_bytes() == before


def test_manual_and_automatic_rollback_reject_new_formats_without_losing_data(tmp_path, project):
    from alice_codex.config import RuntimeConfig

    prepare_format_gates(project[0])
    manager, old = stage_formats(tmp_path, project, summary=None, heartbeat=None)
    assert manager.verify(old, native=True)["promotable"]
    manager.activate(old)
    _, new = stage_formats(tmp_path, project, version="0.0.2", manager=manager)
    assert manager.verify(new, native=True)["promotable"]
    pointer = manager.activate(new)
    for kind in ("summary", "heartbeat"):
        if kind == "summary":
            path = install_header(manager, header())
        else:
            path = manager.home / "config.json"
            _, manifest = manager._manifest(new)
            RuntimeConfig(
                str(manager.home), manifest["codex_binary"], manifest["codex_version"], manifest["codex_sha256"]
            ).save()
            config = json.loads(path.read_text())
            config["heartbeat_sources"] = None
            write_json(path, config)
        before = path.read_bytes()
        for switch in (manager.rollback, lambda: manager.automatic_rollback(new, {new})):
            with pytest.raises(ReleaseError, match=kind):
                switch()
            assert manager.current() == pointer
            assert path.read_bytes() == before
        path.unlink()


@pytest.mark.parametrize("summary,heartbeat,match", [(2, 0, "summary commit"), (0, 1, "heartbeat source")])
def test_old_bootstrap_refuses_new_formats_before_first_footprint(tmp_path, project, summary, heartbeat, match):
    from alice_codex.bootstrap import install_runtime

    prepare_format_gates(project[0])
    manager, old = stage_formats(tmp_path, project, summary=None, heartbeat=None)
    assert manager.verify(old, native=True)["promotable"]
    pointer = manager.activate(old)
    bootstrap = install_runtime(manager)
    write_json(manager.home / "state/supervisor.json", {"version": 2})
    _, new = stage_formats(tmp_path, project, version="0.0.2", manager=manager, summary=summary, heartbeat=heartbeat)
    assert manager.verify(new, native=True)["promotable"]
    with pytest.raises(ReleaseError, match="uninstall.*" + match):
        manager.activate(new)
    assert manager.current() == pointer
    assert not (manager.home / "memory-state/commits").exists()
    assert not (manager.home / "config.json").exists()
    metadata_path = manager.home / "bootstrap" / bootstrap["generation"] / "bootstrap.json"
    metadata = json.loads(metadata_path.read_text())
    field = "summary_commit_schema" if summary else "heartbeat_sources_config_version"
    metadata["installed"][field] = summary or heartbeat
    write_json(metadata_path, metadata)
    with pytest.raises(ReleaseError, match="capability metadata changed"):
        manager.checked_current()


@pytest.mark.parametrize("relative", ["memory-state", "memory-state/commits", "memory-state/commits/x.json", "config.json"])
def test_dangling_format_paths_cannot_bypass_compatibility(tmp_path, relative):
    manager = ReleaseManager(tmp_path)
    path = tmp_path / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    target = tmp_path / "absent-target"
    path.symlink_to(target)
    check = manager._check_heartbeat_sources_compat if relative == "config.json" else manager._check_summary_commit_compat
    with pytest.raises(ReleaseError, match="symbolic link|unlinked"):
        check({"installed": {}})
    assert path.is_symlink() and not target.exists()
