"""Installed launcher and real verified synthetic wheel; no native/model claim."""

import json
import os
import subprocess

import pytest

from test_releases import project as project, stage

pytestmark = pytest.mark.artifact


def test_installed_launcher_executes_only_the_verified_candidate(tmp_path, project):
    python = os.environ.get("ALICE_ARTIFACT_PYTHON")
    if not python:
        pytest.skip("installed-artifact gate requires ALICE_ARTIFACT_PYTHON")
    manager, candidate = stage(tmp_path, project, command_probe=True)
    # These minimal gates certify the synthetic fixture's contracts only.
    assert manager.verify(candidate, native=True)["promotable"]
    pointer = manager.activate(candidate)
    damaged = manager.home / "memory-state/sources.sqlite3"
    damaged.parent.mkdir()
    damaged.write_bytes(b"synthetic damage must not hide diagnostic commands")
    before = damaged.read_bytes()
    shadow = tmp_path / "shadow/alice_codex"
    shadow.mkdir(parents=True)
    (shadow / "__init__.py").write_text("raise RuntimeError('untrusted cwd import')\n")
    environment = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": str(tmp_path / "private-home"),
        "PYTHONPATH": str(shadow.parent),
    }

    def launch(*args):
        return subprocess.run(
            [python, "-I", "-m", "alice_codex.launcher", "--home", str(manager.home), *args],
            cwd=shadow.parent, env=environment, capture_output=True, text=True, timeout=15,
        )

    arguments = ["status", "literal $(touch injected) `whoami`", "--target", "with spaces"]
    result = launch(*arguments)
    assert result.returncode == 0, result.stderr
    observed = json.loads(result.stdout)
    assert observed == {
        "python": pointer["python"],
        "argv": ["--home", str(manager.home), *arguments],
        "pythonpath": None,
        "cwd": str(manager.home),
    }
    assert damaged.read_bytes() == before
    default = launch()
    assert default.returncode == 0, default.stderr
    assert json.loads(default.stdout)["argv"][-1] == "chat"

    active = manager.root / "current.json"
    active.write_text(json.dumps({**pointer, "python": "/unverified/bin/python"}))
    refused = launch("status")
    assert refused.returncode == 1 and "pointer" in refused.stderr and not refused.stdout
    active.write_text(json.dumps(pointer))
    folder, _ = manager._manifest(candidate)
    entry = next((folder / "venv/lib").glob("python*/site-packages/alice_codex/__main__.py"))
    entry.write_text("raise RuntimeError('changed code must not execute')\n")
    refused = launch("status")
    assert refused.returncode == 1 and "environment" in refused.stderr and not refused.stdout
    assert damaged.read_bytes() == before
