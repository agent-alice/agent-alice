"""Synthetic summary gaps and interrupted commits; no private runtime data."""

import datetime as dt
import hashlib
import json
import os
from pathlib import Path

import pytest

from alice_codex import memory
from alice_codex.memory import (
    MemoryConflictError,
    MemoryStore,
    SourceChangedError,
    SummaryValidationError,
)


NOW = dt.datetime(2026, 10, 5, tzinfo=dt.timezone.utc)


def summary_fixture(tmp_path):
    store = MemoryStore(tmp_path / "data")
    source = store.workspace / "memory/chronicle/hourly/2026-09-01.jsonl"
    source.parent.mkdir(parents=True)
    source.write_text(
        json.dumps({"time_start": "2026-09-01T00:00:00", "content": "Synthetic observation"}) + "\n"
    )
    target = store.workspace / "memory/chronicle/diary/2026-09-01.md"
    target.parent.mkdir(parents=True)
    target.write_text("Original handwritten note\n")
    batch = store.prepare_summary("L2", "2026-09-01", now=NOW)
    manifest = json.loads(Path(batch["manifest_path"]).read_text())
    source_id = manifest["sources"][0]["source_id"]
    candidate = {
        "content": f"Synthetic observation [source:{source_id}]",
        "source_ids": [source_id],
        "covered_source_ids": [source_id],
        "missing": [],
    }
    return store, batch, target, candidate


def test_all_malformed_sources_can_commit_an_explicit_gap(tmp_path):
    store = MemoryStore(tmp_path / "data")
    source = store.workspace / "memory/chronicle/traces/2026-09-01.jsonl"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"BROKEN\n[1, 2]\n")
    batch = store.prepare_summary("L1", "2026-09-01T00:00", now=NOW)
    manifest = json.loads(Path(batch["manifest_path"]).read_text())
    assert batch["source_count"] == 2
    assert all(entry["parse_error"] for entry in manifest["sources"])
    candidate = {
        "content": "Both original records are malformed; the observations remain unknown.",
        "source_ids": [],
        "covered_source_ids": [],
        "missing": [
            {"source_id": entry["source_id"], "reason": entry["parse_error"]}
            for entry in manifest["sources"]
        ],
    }
    result = store.commit_summary(batch["batch_id"], candidate)
    committed = json.loads(Path(result["target_path"]).read_text())
    assert committed["missing"] == candidate["missing"]
    assert committed["source_ids"] == []
    assert source.read_bytes() == b"BROKEN\n[1, 2]\n"
    assert store.commit_summary(batch["batch_id"], candidate)["already_committed"]


def test_summary_with_covered_sources_still_requires_citations(tmp_path):
    store, batch, target, candidate = summary_fixture(tmp_path)
    before = target.read_bytes()
    candidate.update(content="An uncited observation", source_ids=[])
    with pytest.raises(SummaryValidationError, match="citations"):
        store.commit_summary(batch["batch_id"], candidate)
    assert target.read_bytes() == before
    assert not (store.state / "commits").exists()


def test_manual_edit_after_summary_intent_is_preserved_as_conflict(tmp_path, monkeypatch):
    store, batch, target, candidate = summary_fixture(tmp_path)
    intent_path = store.state / "commits" / f"{batch['batch_id']}.json"
    original_atomic = memory._atomic

    def edit_after_intent(path, content):
        original_atomic(path, content)
        if path == intent_path and json.loads(content)["status"] == "pending":
            target.write_text("New handwritten note while the intent was persisted\n")

    monkeypatch.setattr(memory, "_atomic", edit_after_intent)
    with pytest.raises(MemoryConflictError, match="conflicts with a later edit"):
        store.commit_summary(batch["batch_id"], candidate)
    assert target.read_text() == "New handwritten note while the intent was persisted\n"
    pending = json.loads(intent_path.read_text())
    assert pending["status"] == "pending"
    assert "Original handwritten note" in pending["rendered"]
    assert candidate["content"] in pending["rendered"]
    before = intent_path.read_bytes()
    monkeypatch.setattr(memory, "_atomic", original_atomic)
    with pytest.raises(MemoryConflictError, match="conflicts with a later edit"):
        MemoryStore(store.data_dir)
    assert intent_path.read_bytes() == before
    assert target.read_text() == "New handwritten note while the intent was persisted\n"


def test_summary_recovers_if_canonical_write_precedes_commit_receipt(tmp_path, monkeypatch):
    store, batch, target, candidate = summary_fixture(tmp_path)
    intent_path = store.state / "commits" / f"{batch['batch_id']}.json"
    original_atomic = memory._atomic

    def fail_receipt(path, content):
        if path == intent_path and json.loads(content)["status"] == "committed":
            raise OSError("Synthetic failure after canonical write")
        original_atomic(path, content)

    monkeypatch.setattr(memory, "_atomic", fail_receipt)
    with pytest.raises(OSError, match="after canonical write"):
        store.commit_summary(batch["batch_id"], candidate)
    canonical = target.read_bytes()
    assert candidate["content"].encode() in canonical
    assert json.loads(intent_path.read_text())["status"] == "pending"
    monkeypatch.setattr(memory, "_atomic", original_atomic)
    recovered = MemoryStore(store.data_dir)
    assert recovered.commit_summary(batch["batch_id"], candidate)["already_committed"]
    assert target.read_bytes() == canonical
    assert target.read_text().count(f"<!-- anima-summary:{batch['batch_id']} -->") == 1
    assert json.loads(intent_path.read_text())["status"] == "committed"


def test_future_summary_manifest_is_rejected_without_writes(tmp_path):
    store, batch, target, candidate = summary_fixture(tmp_path)
    directory = Path(batch["manifest_path"]).parent
    manifest = json.loads(Path(batch["manifest_path"]).read_text())
    manifest.pop("batch_id")
    manifest["schema_version"] = 2
    serialized = (
        json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    ).encode()
    future_id = hashlib.sha256(serialized).hexdigest()
    manifest["batch_id"] = future_id
    directory.rename(store.batches / future_id)
    manifest_path = store.batches / future_id / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    )
    before = target.read_bytes(), manifest_path.read_bytes()
    with pytest.raises(SummaryValidationError, match="Unsupported summary batch schema"):
        store.commit_summary(future_id, candidate)
    assert (target.read_bytes(), manifest_path.read_bytes()) == before
    assert not (store.state / "commits").exists()


@pytest.mark.parametrize("status", ["pending", "committed"])
@pytest.mark.parametrize("future", ["intent", "manifest"])
def test_future_summary_commit_is_preserved_and_blocks_recovery(tmp_path, status, future):
    store, batch, target, candidate = summary_fixture(tmp_path)
    store.commit_summary(batch["batch_id"], candidate)
    intent_path = store.state / "commits" / f"{batch['batch_id']}.json"
    intent = json.loads(intent_path.read_text())
    intent["status"] = status
    if future == "intent":
        intent["schema_version"] = 2
    else:
        intent["manifest"]["schema_version"] = 2
    intent_path.write_text(json.dumps(intent))
    before = target.read_bytes(), intent_path.read_bytes()
    with pytest.raises(MemoryConflictError, match="Unsupported summary .* schema"):
        MemoryStore(store.data_dir)
    assert (target.read_bytes(), intent_path.read_bytes()) == before


@pytest.mark.parametrize("mutation", ["add", "delete", "replace", "append"])
@pytest.mark.parametrize("stage", ["select", "freeze"])
def test_summary_source_inventory_changes_fail_without_publishing(
    tmp_path, monkeypatch, mutation, stage
):
    store = MemoryStore(tmp_path / "data")
    source = store.workspace / "memory/chronicle/traces/2026-09-01/first.jsonl"
    source.parent.mkdir(parents=True)
    raw = json.dumps({"timestamp": "2026-09-01T00:10:00", "content": "First observation"}) + "\n"
    source.write_text(raw)

    def mutate():
        if mutation == "add":
            (source.parent / "second.jsonl").write_text(raw)
        elif mutation == "delete":
            source.unlink()
        elif mutation == "replace":
            replacement = source.parent / "replacement.jsonl"
            replacement.write_text(raw)
            replacement.replace(source)
        else:
            source.write_text(raw + raw)

    if stage == "select":
        original_records = memory._records

        def change_during_selection(path):
            yield from original_records(path)
            if path == source:
                mutate()

        monkeypatch.setattr(memory, "_records", change_during_selection)
    else:
        original_copy = memory._stable_copy

        def change_during_freezing(path, target):
            result = original_copy(path, target)
            if path == source:
                mutate()
            return result

        monkeypatch.setattr(memory, "_stable_copy", change_during_freezing)
    with pytest.raises(SourceChangedError):
        store.prepare_summary("L1", "2026-09-01T00:00", now=NOW)
    assert list(store.batches.iterdir()) == []
    assert store.index_status()["source_count"] == 0


@pytest.mark.parametrize("damage", ["content", "missing", "manifest"])
def test_reused_summary_batch_verifies_its_frozen_evidence(tmp_path, damage):
    store, batch, target, _ = summary_fixture(tmp_path)
    manifest_path = Path(batch["manifest_path"])
    manifest = json.loads(manifest_path.read_text())
    frozen = manifest_path.parent / "files" / manifest["files"][0]["path"]
    if damage == "content":
        frozen.write_text("Corrupted frozen evidence\n")
    elif damage == "missing":
        frozen.unlink()
    else:
        manifest["period"] = "2026-09-02"
        manifest_path.write_text(json.dumps(manifest))
    before = target.read_bytes(), manifest_path.read_bytes()
    expected_error = SummaryValidationError if damage == "manifest" else SourceChangedError
    with pytest.raises(expected_error):
        store.prepare_summary("L2", "2026-09-01", now=NOW)
    assert (target.read_bytes(), manifest_path.read_bytes()) == before
    assert len(list(store.batches.iterdir())) == 1


@pytest.mark.parametrize("missing", ["workspace", "frozen"])
def test_summary_commit_reports_missing_source_without_writing(tmp_path, missing):
    store, batch, target, candidate = summary_fixture(tmp_path)
    manifest_path = Path(batch["manifest_path"])
    manifest = json.loads(manifest_path.read_text())
    root = store.workspace if missing == "workspace" else manifest_path.parent / "files"
    (root / manifest["files"][0]["path"]).unlink()
    before = target.read_bytes()
    with pytest.raises(SourceChangedError, match="source is missing"):
        store.commit_summary(batch["batch_id"], candidate)
    assert target.read_bytes() == before
    assert not (store.state / "commits").exists()


@pytest.mark.parametrize(
    "relative_path,blocked",
    [("2026-08-31.jsonl", False), ("2026-09-01/large.jsonl", True), ("unknown.jsonl", True)],
)
def test_oversized_summary_record_uses_known_file_dates_conservatively(
    tmp_path, relative_path, blocked
):
    store = MemoryStore(tmp_path / "data")
    root = store.workspace / "memory/chronicle/traces"
    root.mkdir(parents=True)
    (root / "2026-09-01.jsonl").write_text(
        json.dumps({"timestamp": "2026-09-01T00:10:00", "content": "Valid observation"}) + "\n"
    )
    oversized = root / relative_path
    oversized.parent.mkdir(exist_ok=True)
    oversized.write_text("x" * (memory._PARSE_CHARS + 1) + "\n")
    if blocked:
        with pytest.raises(SummaryValidationError, match="explicit partitioning"):
            store.prepare_summary("L1", "2026-09-01T00:00", now=NOW)
        assert list(store.batches.iterdir()) == []
    else:
        batch = store.prepare_summary("L1", "2026-09-01T00:00", now=NOW)
        assert batch["source_count"] == 1
        manifest = json.loads(Path(batch["manifest_path"]).read_text())
        assert manifest["files"][0]["path"].endswith("traces/2026-09-01.jsonl")
    assert oversized.stat().st_size == memory._PARSE_CHARS + 2


@pytest.mark.parametrize("kind", ["symlink", "fifo"])
def test_summary_rejects_unreadable_source_types_instead_of_skipping(tmp_path, kind):
    store = MemoryStore(tmp_path / "data")
    source = store.workspace / "memory/chronicle/traces/2026-09-01.jsonl"
    source.parent.mkdir(parents=True)
    if kind == "symlink":
        outside = tmp_path / "outside.jsonl"
        outside.write_text("Unselected synthetic source\n")
        source.symlink_to(outside)
    else:
        os.mkfifo(source)
    with pytest.raises(SummaryValidationError, match="symlinks|not a regular file"):
        store.prepare_summary("L1", "2026-09-01T00:00", now=NOW)
    assert source.exists()
    assert list(store.batches.iterdir()) == []


@pytest.mark.parametrize(
    "raw",
    ["BROKEN\n", '{"content":"Missing timestamp"}\n', '{"timestamp":"bad-time"}\n'],
)
def test_summary_rejects_unknown_window_for_bad_source(tmp_path, raw):
    store = MemoryStore(tmp_path / "data")
    source = store.workspace / "memory/chronicle/traces/unknown.jsonl"
    source.parent.mkdir(parents=True)
    source.write_text(raw)
    with pytest.raises(
        SummaryValidationError, match="no valid timestamp or recognizable file date"
    ):
        store.prepare_summary("L1", "2026-09-01T00:00", now=NOW)
    assert source.read_text() == raw
    assert list(store.batches.iterdir()) == []


def test_summary_accepts_valid_timestamp_without_a_dated_file_path(tmp_path):
    store = MemoryStore(tmp_path / "data")
    source = store.workspace / "memory/chronicle/traces/unknown.jsonl"
    source.parent.mkdir(parents=True)
    source.write_text('{"timestamp":"2026-09-01T00:10:00","content":"Dated observation"}\n')
    batch = store.prepare_summary("L1", "2026-09-01T00:00", now=NOW)
    assert batch["source_count"] == 1
    manifest = json.loads(Path(batch["manifest_path"]).read_text())
    assert manifest["sources"][0]["parse_error"] is None
