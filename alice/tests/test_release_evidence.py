"""Real synthetic pytest groups retain failure fixtures, not native proof."""

from pathlib import Path

from test_releases import project as project, stage


def test_failed_pytest_group_evidence_survives_later_groups_and_verification(tmp_path, project):
    source, _ = project
    index = tmp_path / "synthetic-proof-paths.txt"
    (source / "tests/test_unit.py").write_text(f'''from pathlib import Path

def test_deliberate_failure(tmp_path):
    proof = tmp_path / "original-failure.txt"
    proof.write_text("synthetic original failure evidence")
    with Path({str(index)!r}).open("a") as handle:
        handle.write(str(proof) + "\\n")
    assert False, "deliberate failure before later pytest groups"
''')
    observer = f'''

def test_earlier_failure_fixtures_still_exist(tmp_path):
    from pathlib import Path
    (tmp_path / "later-group-proof.txt").write_text("synthetic later group")
    for line in Path({str(index)!r}).read_text().splitlines():
        assert Path(line).read_text() == "synthetic original failure evidence"
'''
    for name in ("test_artifact_smoke.py", "test_native.py", "test_native_code_mode.py"):
        path = source / "tests" / name
        path.write_text(path.read_text() + observer)
    manager, candidate = stage(tmp_path, project)
    all_paths = set()
    run_paths = set()
    for run in range(2):
        report = manager.verify(candidate, native=True)
        assert not report["passed"] and not report["promotable"]
        checks = {check["name"]: check for check in report["checks"]}
        assert checks["unit"]["status"] == "failed"
        assert all(checks[name]["status"] == "passed" for name in ("ruff", "artifact", "native", "native_pair"))
        proofs = [Path(line) for line in index.read_text().splitlines()]
        assert len(proofs) == run + 1
        assert all(path.read_text() == "synthetic original failure evidence" for path in proofs)
        this_run = set()
        for name in ("unit", "artifact", "native", "native_pair"):
            command = checks[name]["command"]
            args = [arg for arg in command if arg.startswith("--basetemp=")]
            assert len(args) == 1
            base = Path(args[0].split("=", 1)[1])
            assert base.is_absolute() and base.name == name
            assert base.parent.parent == manager._candidate(candidate) / "verification-runs"
            assert base not in all_paths and base.is_dir()
            assert base.stat().st_mode & 0o077 == 0
            all_paths.add(base)
            this_run.add(base.parent)
        assert len(this_run) == 1 and not this_run & run_paths
        run_paths.update(this_run)
    assert len(all_paths) == 8 and len(run_paths) == 2
    assert manager.current() is None


def test_verification_evidence_link_does_not_change_an_unrelated_directory(tmp_path, project):
    manager, candidate = stage(tmp_path, project)
    outside = tmp_path / "unrelated"
    outside.mkdir(mode=0o755)
    (manager._candidate(candidate) / "verification-runs").symlink_to(outside, target_is_directory=True)
    report = manager.verify(candidate, native=True)
    assert not report["passed"] and not report["promotable"]
    assert report["checks"][0]["name"] == "integrity"
    assert "symbolic link" in report["checks"][0]["detail"]
    assert outside.stat().st_mode & 0o777 == 0o755
    assert list(outside.iterdir()) == []
