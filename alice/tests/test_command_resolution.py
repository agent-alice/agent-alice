"""Real minimal installed wheels exercise release authority, with no model calls."""

import json
import sqlite3
import subprocess
import time
from unittest.mock import Mock

import pytest

from alice_codex.releases import ReleaseError
from test_releases import project as project, stage


def test_command_resolution_retains_code_guards_without_opening_business_data(
    tmp_path, project, monkeypatch, record_testsuite_property
):
    manager, candidate = stage(tmp_path, project)
    assert manager.verify(candidate, native=True)["promotable"]
    pointer = manager.activate(candidate)
    database = manager.home / "memory-state/sources.sqlite3"
    database.parent.mkdir()
    with sqlite3.connect(database) as db:
        db.execute("PRAGMA user_version=1")
        db.execute("CREATE TABLE evidence(value BLOB)")
        db.executemany("INSERT INTO evidence VALUES (zeroblob(131072))", [()] * 512)
    start = time.perf_counter()
    assert manager.checked_current() == pointer
    checked_seconds = time.perf_counter() - start
    start = time.perf_counter()
    assert manager.resolve_current() == pointer
    resolved_seconds = time.perf_counter() - start
    record_testsuite_property("synthetic_command_resolution", json.dumps({
        "index_bytes": database.stat().st_size,
        "full_guard_seconds": checked_seconds,
        "code_resolution_seconds": resolved_seconds,
        "scope": "64 MiB synthetic SQLite; excludes CLI process and production startup",
    }))
    database.write_bytes(b"synthetic damaged data; diagnostics must remain reachable")
    before = database.read_bytes()
    with monkeypatch.context() as isolated:
        connect = Mock(side_effect=AssertionError("code resolution must not open business data"))
        isolated.setattr("alice_codex.releases.sqlite3.connect", connect)
        assert manager.resolve_current() == pointer
        connect.assert_not_called()
    executed = subprocess.run(
        [pointer["python"], "-I", "-m", "alice_codex.probe"],
        capture_output=True, text=True, check=True, timeout=10,
    )
    assert executed.stdout.strip() == "good"
    with pytest.raises(ReleaseError, match="memory"):
        manager.checked_current()
    with pytest.raises(ReleaseError, match="memory"):
        manager.activate(candidate)
    assert database.read_bytes() == before
    assert manager.current() == pointer

    active = manager.root / "current.json"
    active.write_text(json.dumps({**pointer, "python": "/unverified/bin/python"}))
    with pytest.raises(ReleaseError, match="pointer"):
        manager.resolve_current()
    active.write_text(json.dumps(pointer))
    assert manager.resolve_current() == pointer

    folder, _ = manager._manifest(candidate)
    probe = next((folder / "venv/lib").glob("python*/site-packages/alice_codex/probe.py"))
    probe.write_text("raise RuntimeError('changed after verification')\n")
    with pytest.raises(ReleaseError, match="environment"):
        manager.resolve_current()
    assert database.read_bytes() == before
