from pathlib import Path
import subprocess
import sys

import pytest

from alice_codex.releases import pytest_evidence, run_check


@pytest.mark.parametrize(
    "xml",
    [
        "<testsuites/>",
        "<testsuites><testsuite><testcase><skipped/></testcase></testsuite></testsuites>",
        "<testsuites><testsuite><testcase><failure/></testcase></testsuite></testsuites>",
        "<testsuites><testsuite><testcase><error/></testcase></testsuite></testsuites>",
        "not xml",
    ],
)
def test_invalid_or_skipped_required_evidence_is_failure(tmp_path, xml):
    path = tmp_path / "result.xml"
    path.write_text(xml)
    assert pytest_evidence(path) is not None


def test_required_evidence_must_exist(tmp_path):
    assert pytest_evidence(tmp_path / "missing.xml") is not None


def test_successful_process_cannot_hide_skipped_or_missing_tests(tmp_path):
    junit = tmp_path / "result.xml"
    result = run_check(
        "pytest",
        [sys.executable, "-c", "print('looks green')"],
        cwd=tmp_path,
        env={},
        timeout=5,
        junit=junit,
    )
    assert result.returncode == 0
    assert result.status == "failed"


def test_command_failure_and_timeout_block_gate(tmp_path):
    bad = run_check(
        "bad", [sys.executable, "-c", "raise SystemExit(7)"], cwd=tmp_path, env={}, timeout=5
    )
    assert bad.status == "failed" and bad.returncode == 7
    hung = run_check(
        "hung",
        [sys.executable, "-c", "import time; time.sleep(60)"],
        cwd=tmp_path,
        env={},
        timeout=0.05,
    )
    assert hung.status == "timed_out" and hung.returncode != 0


def test_check_entry_missing_binary_is_nonzero_and_reports_failure(tmp_path):
    root = Path(__file__).resolve().parents[1]
    report = tmp_path / "report.json"
    result = subprocess.run(
        [
            sys.executable,
            str(root / "tools/check.py"),
            "--source",
            str(tmp_path),
            "--codex-binary",
            str(tmp_path / "missing"),
            "--report",
            str(report),
        ],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 1
    assert '"passed": false' in report.read_text()
