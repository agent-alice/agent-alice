import base64
import csv
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import venv
import zipfile

import pytest

from alice_codex.releases import ReleaseError, ReleaseManager, source_fingerprint
from alice_codex.store import Store


def make_wheel(path, version="0.0.1", value="good"):
    """A real minimal wheel: installation/entry execution need no network."""
    filename = path / f"alice_codex-{version}-py3-none-any.whl"
    info = f"alice_codex-{version}.dist-info"
    files = {
        "alice_codex/__init__.py": f'__version__ = "{version}"\n',
        "alice_codex/store.py": "class Store:\n    SCHEMA_VERSION = 1\n",
        "alice_codex/memory.py": "class MemoryStore:\n    SCHEMA_VERSION = 1\n",
        "alice_codex/resources.py": "class ResourceLedger:\n    SCHEMA_VERSION = 1\n",
        "alice_codex/probe.py": f'print("{value}")\n',
        "alice_codex/supervisor.py": "BOOTSTRAP_PROTOCOL = 1\n",
        f"{info}/METADATA": f"Metadata-Version: 2.1\nName: alice-codex\nVersion: {version}\n",
        f"{info}/WHEEL": "Wheel-Version: 1.0\nGenerator: alice-test\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
    }
    record = io.StringIO()
    writer = csv.writer(record)
    for name, text in files.items():
        data = text.encode()
        digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).decode().rstrip("=")
        writer.writerow([name, f"sha256={digest}", len(data)])
    writer.writerow([f"{info}/RECORD", "", ""])
    files[f"{info}/RECORD"] = record.getvalue()
    with zipfile.ZipFile(filename, "w") as wheel:
        for name, text in files.items():
            wheel.writestr(name, text)
    return filename


@pytest.fixture
def project(tmp_path):
    source = tmp_path / "source"
    (source / "src").mkdir(parents=True)
    (source / "tests").mkdir()
    (source / "requirements.lock").write_text(
        "# Minimal test package has no runtime dependencies.\n"
    )
    (source / "pyproject.toml").write_text("""[tool.pytest.ini_options]
markers = ["artifact: installed candidate", "native: real Codex", "live: model"]

[tool.ruff.lint]
select = ["E4", "E7", "E9", "F"]
""")
    (source / "tests/test_unit.py").write_text("def test_unit():\n    assert 2 + 2 == 4\n")
    (source / "tests/test_artifact_smoke.py").write_text("""import os
import subprocess
import pytest

pytestmark = pytest.mark.artifact

def test_installed_behavior():
    result = subprocess.run([os.environ["ALICE_ARTIFACT_PYTHON"], "-I", "-m", "alice_codex.probe"], capture_output=True, text=True, timeout=5)
    assert result.returncode == 0
    assert result.stdout.strip() == "good"
""")
    binary = tmp_path / "codex"
    binary.write_text('#!/bin/sh\nprintf "codex-cli test-fixture\\n"\n')
    binary.chmod(0o700)
    return source, binary


def stage(tmp_path, project, *, version="0.0.1", value="good", manager=None):
    source, binary = project
    manager = manager or ReleaseManager(tmp_path / "runtime")
    candidate = manager.stage_wheel(
        make_wheel(tmp_path, version, value),
        source_root=source,
        python=sys.executable,
        codex_binary=binary,
    )
    return manager, candidate


def test_unverified_candidate_cannot_replace_active_pointer(tmp_path, project):
    manager, candidate = stage(tmp_path, project)
    with pytest.raises(ReleaseError, match="verification"):
        manager.activate(candidate)
    assert manager.current() is None


def test_candidate_install_honors_constraints_and_rejects_missing_lock(tmp_path, project):
    source, _ = project
    (source / "requirements.lock").unlink()
    with pytest.raises(ReleaseError, match="requirements.lock"):
        stage(tmp_path, project)
    # A contradictory local package pin must fail before it can become a candidate.
    (source / "requirements.lock").write_text("alice-codex==0.0.2\n")
    with pytest.raises(ReleaseError, match="candidate install failed"):
        stage(tmp_path, project, version="0.0.1")
    assert not list((tmp_path / "runtime/releases").glob("*/candidate.json"))


def test_actual_installed_artifact_passes_and_activation_keeps_same_wheel(tmp_path, project):
    manager, candidate = stage(tmp_path, project)
    report = manager.verify(candidate)
    assert report["passed"], report
    assert {check["name"] for check in report["checks"]} == {"ruff", "unit", "artifact"}
    pointer = manager.activate(candidate, data_schema=1)
    assert pointer["current"] == candidate
    assert pointer["wheel_sha256"] == report["wheel_sha256"]
    assert Path(pointer["python"]).is_file()
    assert manager.checked_current(data_schema=1) == pointer
    changed = {**pointer, "python": "/tmp/unverified-python"}
    (manager.root / "current.json").write_text(json.dumps(changed))
    with pytest.raises(ReleaseError, match="pointer"):
        manager.checked_current(data_schema=1)


def test_broken_installed_artifact_blocks_release_even_when_unit_tests_pass(tmp_path, project):
    manager, candidate = stage(tmp_path, project, value="broken")
    report = manager.verify(candidate)
    checks = {item["name"]: item for item in report["checks"]}
    assert checks["unit"]["status"] == "passed"
    assert checks["artifact"]["status"] == "failed"
    assert not report["passed"]
    with pytest.raises(ReleaseError):
        manager.activate(candidate)
    assert manager.current() is None


def test_post_verification_wheel_or_environment_changes_are_rejected(tmp_path, project):
    manager, candidate = stage(tmp_path, project)
    assert manager.verify(candidate)["passed"]
    directory, manifest = manager._manifest(candidate)
    wheel = directory / manifest["wheel"]
    original = wheel.read_bytes()
    wheel.write_bytes(original + b"changed")
    with pytest.raises(ReleaseError, match="wheel changed"):
        manager.activate(candidate)
    wheel.write_bytes(original)
    package = next((directory / "venv/lib").glob("python*/site-packages/alice_codex/probe.py"))
    package.write_text('print("regression after check")\n')
    with pytest.raises(ReleaseError, match="environment changed"):
        manager.activate(candidate)


def test_rollback_keeps_new_data_and_rejects_incompatible_schema(tmp_path, project):
    manager, first = stage(tmp_path, project)
    assert manager.verify(first)["passed"]
    manager.activate(first)
    _, second = stage(tmp_path, project, manager=manager, version="0.0.2")
    assert manager.verify(second)["passed"]
    manager.activate(second)
    database = manager.home / "state/schedules.sqlite3"
    with Store(database) as store:
        job = store.create_job(name="new data", schedule_type="at", schedule_value=100, now=0)
        store.set_autonomy_paused(True)
    with pytest.raises(ReleaseError, match="disagrees"):
        manager.rollback(data_schema=2)
    assert manager.current()["current"] == second
    assert manager.rollback(data_schema=1)["current"] == first
    with Store(database) as store:
        assert store.get_job(job.id).name == "new data"
        assert store.is_autonomy_paused()
        store._connection().execute("PRAGMA user_version=2")
    with pytest.raises(ReleaseError, match="schema"):
        manager.activate(second)
    assert manager.current()["current"] == first


def test_missing_required_smoke_cannot_be_reported_as_passed(tmp_path, project):
    source, _ = project
    (source / "tests/test_artifact_smoke.py").unlink()
    manager, candidate = stage(tmp_path, project)
    report = manager.verify(candidate)
    assert not report["passed"]
    assert "smoke test is missing" in report["checks"][0]["detail"]


@pytest.mark.parametrize(
    ("name", "path"),
    [("memory", "memory-state/sources.sqlite3"), ("resource", "state/resources.sqlite3")],
)
def test_business_schema_guard_preserves_new_data_and_blocks_future_format(
    tmp_path, project, name, path
):
    manager, candidate = stage(tmp_path, project)
    assert manager.verify(candidate)["passed"]
    index = manager.home / path
    index.parent.mkdir()
    with sqlite3.connect(index) as db:
        db.execute("PRAGMA user_version=1")
        db.execute("CREATE TABLE evidence (text TEXT)")
        db.execute("INSERT INTO evidence VALUES ('retained after promotion')")
    manager.activate(candidate)
    with sqlite3.connect(index) as db:
        assert db.execute("SELECT text FROM evidence").fetchone()[0] == "retained after promotion"
        db.execute("PRAGMA user_version=2")
    with pytest.raises(ReleaseError, match=f"{name} schema 1, data requires 2"):
        manager.checked_current()
    with pytest.raises(ReleaseError, match=f"{name} schema"):
        manager.activate(candidate)
    with sqlite3.connect(index) as db:
        assert db.execute("SELECT text FROM evidence").fetchone()[0] == "retained after promotion"


def test_source_change_since_build_requires_new_candidate(tmp_path, project):
    manager, candidate = stage(tmp_path, project)
    (project[0] / "tests/test_unit.py").write_text("def test_regression():\n    assert False\n")
    report = manager.verify(candidate)
    assert not report["passed"]
    assert "changed since" in report["checks"][0]["detail"]


def test_native_gate_never_runs_live_without_selection_and_missing_live_prerequisites_fail(
    tmp_path, project
):
    (project[0] / "tests/test_live.py").write_text("""import pytest

@pytest.mark.native
def test_native_probe():
    assert True

@pytest.mark.native
@pytest.mark.live
def test_expensive_prerequisite():
    pytest.fail("explicit live prerequisites are unavailable")
""")
    manager, candidate = stage(tmp_path, project)
    assert manager.verify(candidate, native=True)["passed"]
    with pytest.raises(ReleaseError, match="requires.*native"):
        manager.verify(candidate, live=True)
    with pytest.raises(ReleaseError, match="verification"):
        manager.activate(candidate)
    report = manager.verify(candidate, native=True, live=True)
    checks = {item["name"]: item for item in report["checks"]}
    assert checks["native"]["status"] == "passed"
    assert checks["live"]["status"] == "failed" and not report["passed"]
    with pytest.raises(ReleaseError):
        manager.activate(candidate)


def test_claimed_passed_report_is_not_an_execution_receipt(tmp_path, project):
    manager, candidate = stage(tmp_path, project)
    directory = manager._candidate(candidate)
    (directory / "verification.json").write_text(json.dumps({"passed": True}))
    with pytest.raises(ReleaseError, match="verification"):
        manager.activate(candidate)


def test_source_inventory_does_not_read_legacy_data(tmp_path):
    (tmp_path / "src").mkdir()
    (tmp_path / "src/code.py").write_text("value = 1\n")
    before = source_fingerprint(tmp_path)
    (tmp_path / "memory").mkdir()
    (tmp_path / "memory/private.json").write_text("not part of build")
    assert source_fingerprint(tmp_path) == before
    (tmp_path / "src/code.py").write_text("value = 2\n")
    assert source_fingerprint(tmp_path) != before


def test_verification_imports_requested_source_over_other_editable_install(tmp_path, project):
    source, binary = project
    package = source / "src/alice_codex"
    package.mkdir()
    (package / "__init__.py").write_text('ORIGIN = "requested-source"\n')
    (source / "tests/test_unit.py").write_text(
        "from alice_codex import ORIGIN\n\ndef test_source_origin():\n"
        '    assert ORIGIN == "requested-source"\n'
    )

    # An isolated interpreter with a genuine .pth editable installation pointing
    # elsewhere recreates the exported-source vs development-venv failure. Reuse
    # test tool packages through another .pth; never alter the developer's venv.
    foreign = tmp_path / "other-project/src/alice_codex"
    foreign.mkdir(parents=True)
    (foreign / "__init__.py").write_text('ORIGIN = "other-editable-install"\n')
    verifier = tmp_path / "verification-python"
    venv.EnvBuilder(with_pip=False).create(verifier)
    python = verifier / "bin/python"
    environment = ReleaseManager._environment(tmp_path / "isolated-home")
    paths = subprocess.run(
        [str(python), "-c", "import sysconfig; print(sysconfig.get_path('purelib'))"],
        check=True,
        capture_output=True,
        text=True,
        env=environment,
        timeout=10,
    )
    installed = Path(paths.stdout.strip())
    (installed / "00-other-editable.pth").write_text(str(foreign.parent) + "\n")
    (installed / "90-verification-tools.pth").write_text(
        str(Path(pytest.__file__).parents[1]) + "\n"
    )

    # The unfixed subprocess environment demonstrably imports the wrong package;
    # this control must fail the same assertion, not merely inspect an env string.
    control = subprocess.run(
        [str(python), "-m", "pytest", "tests/test_unit.py", "-q"],
        cwd=source,
        env=environment,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert control.returncode == 1, control.stdout + control.stderr
    assert "other-editable-install" in control.stdout
    assert "1 failed" in control.stdout

    manager = ReleaseManager(tmp_path / "runtime")
    candidate = manager.stage_wheel(
        make_wheel(tmp_path),
        source_root=source,
        python=str(python),
        codex_binary=binary,
    )
    report = manager.verify(candidate, source_root=source)
    assert report["passed"], report
    checks = {item["name"]: item for item in report["checks"]}
    assert "1 passed" in checks["unit"]["output"]
    # The installed artifact still runs in isolation through -I; binding source
    # imports in the verifier must not replace the wheel's implementation.
    assert "1 passed" in checks["artifact"]["output"]


def test_check_subprocess_keeps_explicit_proxies_but_not_model_credentials(tmp_path, monkeypatch):
    proxies = {
        key: "localhost,127.0.0.1,::1" if key.lower() == "no_proxy" else "http://127.0.0.1:8118"
        for key in (
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "NO_PROXY",
            "http_proxy",
            "https_proxy",
            "all_proxy",
            "no_proxy",
        )
    }
    for key, value in proxies.items():
        monkeypatch.setenv(key, value)
    secrets = ("OPENAI_API_KEY", "CODEX_API_KEY", "PRIVATE_PROVIDER_TOKEN")
    for key in secrets:
        monkeypatch.setenv(key, "synthetic-test-value")
    child = subprocess.run(
        [
            sys.executable,
            "-c",
            "import json,os; "
            "print(json.dumps({'proxies':{k:v for k,v in os.environ.items() "
            "if k.lower() in {'http_proxy','https_proxy','all_proxy','no_proxy'}}, "
            "'credentials_present':[k for k in "
            "['OPENAI_API_KEY','CODEX_API_KEY','PRIVATE_PROVIDER_TOKEN'] if k in os.environ]}))",
        ],
        env=ReleaseManager._environment(tmp_path / "check-home"),
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )
    assert json.loads(child.stdout) == {"proxies": proxies, "credentials_present": []}


def test_automatic_rollback_is_conditional_and_never_revisits_failed_candidate(tmp_path, project):
    manager, first = stage(tmp_path, project)
    assert manager.verify(first)["passed"]
    manager.activate(first)
    _, second = stage(tmp_path, project, manager=manager, version="0.0.2")
    assert manager.verify(second)["passed"]
    promoted = manager.activate(second)
    marker = manager.home / "new-evidence.json"
    marker.write_text('{"new":true}')
    with pytest.raises(ReleaseError, match="epoch changed"):
        manager.automatic_rollback(second, {second}, expected_epoch="stale")
    restored = manager.automatic_rollback(
        second, {second}, expected_epoch=promoted["activation_epoch"]
    )
    assert restored["current"] == first
    assert restored["activation_epoch"] == promoted["activation_epoch"]
    assert marker.read_text() == '{"new":true}'
    with pytest.raises(ReleaseError, match="no unfailed"):
        manager.automatic_rollback(first, {first, second})
    assert manager.current() == restored
    assert manager.activate(first)["activation_epoch"] != restored["activation_epoch"]
    database = manager.home / "memory-state/sources.sqlite3"
    database.parent.mkdir()
    with sqlite3.connect(database) as db:
        db.execute("PRAGMA user_version=2")
    before = manager.current()
    with pytest.raises(ReleaseError, match="memory schema"):
        manager.automatic_rollback(first, {first})
    assert manager.current() == before
    assert marker.read_text() == '{"new":true}'


def test_independent_bootstrap_survives_damaged_candidate_and_rejects_own_damage(tmp_path, project):
    from alice_codex.bootstrap import checked_runtime, install_runtime

    manager, candidate = stage(tmp_path, project)
    assert manager.verify(candidate)["passed"]
    manager.activate(candidate)
    bootstrap = install_runtime(manager)
    independent = Path(bootstrap["python"])
    candidate_folder = manager._candidate(candidate)
    original = next(
        candidate_folder.glob("venv/lib/python*/site-packages/alice_codex/supervisor.py")
    )
    original.write_text("raise RuntimeError('bad candidate startup')\n")
    with pytest.raises(ReleaseError, match="environment changed"):
        manager.checked_current()
    assert checked_runtime(manager.home)["python"] == str(independent)
    probe = subprocess.run(
        [
            str(independent),
            "-I",
            "-c",
            "import alice_codex.supervisor as s; print(s.BOOTSTRAP_PROTOCOL)",
        ],
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert probe.returncode == 0 and probe.stdout.strip() == "1"
    copied = next(
        independent.parents[1].glob("lib/python*/site-packages/alice_codex/supervisor.py")
    )
    copied.write_text("raise RuntimeError('damaged stable environment')\n")
    with pytest.raises(ReleaseError, match="bootstrap or base interpreter changed"):
        checked_runtime(manager.home)


def test_bootstrap_rejects_environment_links_to_unrelated_external_files(tmp_path):
    from alice_codex.bootstrap import _check_links

    environment = tmp_path / "environment"
    (environment / "bin").mkdir(parents=True)
    (environment / "bin/python").symlink_to(Path(sys.executable).resolve())
    external = tmp_path / "unrelated.txt"
    external.write_text("unrelated synthetic evidence")
    (environment / "escaped-data").symlink_to(external)
    with pytest.raises(ReleaseError, match="unsupported external"):
        _check_links(environment)
    assert external.read_text() == "unrelated synthetic evidence"
