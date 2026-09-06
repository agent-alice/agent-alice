"""Synthetic crash boundaries and compatibility for the persisted partition protocol."""

from copy import deepcopy
import datetime as dt
import json
from pathlib import Path

import pytest

from alice_codex import memory, summary_partitions as partitions
from alice_codex.memory import MemoryConflictError, MemoryStore, SummaryValidationError


NOW = dt.datetime(2026, 10, 5, tzinfo=dt.timezone.utc)


def setup_window(tmp_path):
    store = MemoryStore(tmp_path / "data")
    source = store.workspace / "memory/chronicle/hourly/2026-09-01.jsonl"
    source.parent.mkdir(parents=True)
    source.write_text(
        json.dumps({"time_start": "2026-09-01T00:00:00+08:00", "content": "synthetic " * 20000})
        + "\n"
    )
    target = store.workspace / "memory/chronicle/diary/2026-09-01.md"
    target.parent.mkdir(parents=True)
    target.write_text("手记：保留已写下的新想法。\n")
    return store, source, target


def candidate(node):
    sources = json.loads(Path(node["manifest_path"]).read_text())["sources"]
    covered = [s["source_id"] for s in sources if not s["parse_error"]]
    return {
        "content": "Synthetic summary "
        + (f"[source:{covered[0]}]" if covered else "with explicit gaps"),
        "source_ids": covered[:1],
        "covered_source_ids": covered,
        "missing": [
            {"source_id": s["source_id"], "reason": "Synthetic parse gap"}
            for s in sources
            if s["parse_error"]
        ],
    }


def ready_root(store, batch):
    plan = json.loads(Path(batch["manifest_path"]).read_text())
    for _ in range(100):
        page = store.summary_partition_next(batch["batch_id"])
        assert not page["complete"] and page["ready"]
        for node in page["ready"]:
            if node["node_id"] == plan["root_node_id"]:
                return node
            store.commit_summary_partition(batch["batch_id"], node["node_id"], candidate(node))
    pytest.fail("Synthetic DAG failed to reach its root")


def test_placeholder_before_plan_publication_recovers_without_source_rewrite(tmp_path, monkeypatch):
    store, source, target = setup_window(tmp_path)
    original, notes = source.read_bytes(), target.read_bytes()
    replace = partitions.os.replace

    def crash_publish(before, after):
        if Path(before).name.startswith(".prepared-"):
            raise RuntimeError("Synthetic interruption before plan publish")
        return replace(before, after)

    with monkeypatch.context() as patch:
        patch.setattr(partitions.os, "replace", crash_publish)
        with pytest.raises(RuntimeError, match="Synthetic interruption"):
            store.prepare_summary("L2", "2026-09-01", now=NOW)
    intents = list((store.state / "commits").glob("*.json"))
    assert len(intents) == 1
    intent = json.loads(intents[0].read_text())
    assert intent["status"] == "partitioning" and intent["schema_version"] == 2
    staged = store.state / "summary-partitions" / (".prepared-" + intent["batch_id"])
    assert staged.is_dir()

    resumed = MemoryStore(store.data_dir)
    page = resumed.summary_partition_next(intent["batch_id"])
    assert page["ready"] and not page["complete"]
    assert not staged.exists()
    assert source.read_bytes() == original and target.read_bytes() == notes
    batch = resumed.prepare_summary("L2", "2026-09-01", now=NOW)
    assert batch["batch_id"] == intent["batch_id"]
    root = ready_root(resumed, batch)
    assert resumed.commit_summary_partition(batch["batch_id"], root["node_id"], candidate(root))[
        "complete"
    ]
    assert target.read_bytes().startswith(notes)


def test_durable_root_receipt_before_final_intent_retries_on_restart(tmp_path, monkeypatch):
    store, _, target = setup_window(tmp_path)
    batch = store.prepare_summary("L2", "2026-09-01", now=NOW)
    root = ready_root(store, batch)
    before = target.read_bytes()
    finalize = partitions._finalize
    with monkeypatch.context() as patch:

        def interrupted(*args):
            raise RuntimeError("Synthetic interruption after root receipt")

        patch.setattr(partitions, "_finalize", interrupted)
        with pytest.raises(RuntimeError, match="after root receipt"):
            store.commit_summary_partition(batch["batch_id"], root["node_id"], candidate(root))
    assert partitions._finalize is finalize
    assert target.read_bytes() == before
    resumed = MemoryStore(store.data_dir)
    assert resumed.summary_partition_next(batch["batch_id"])["complete"]
    assert target.read_text().count(f"<!-- anima-summary:{batch['batch_id']} -->") == 1


def test_final_target_written_before_receipt_status_recovers_once(tmp_path, monkeypatch):
    store, _, target = setup_window(tmp_path)
    batch = store.prepare_summary("L2", "2026-09-01", now=NOW)
    root = ready_root(store, batch)
    atomic = memory._atomic

    def interrupted(path, raw):
        if (
            Path(path).parent == store.state / "commits"
            and json.loads(raw).get("status") == "committed"
        ):
            raise RuntimeError("Synthetic receipt interruption")
        return atomic(path, raw)

    with monkeypatch.context() as patch:
        patch.setattr(memory, "_atomic", interrupted)
        with pytest.raises(RuntimeError, match="receipt interruption"):
            store.commit_summary_partition(batch["batch_id"], root["node_id"], candidate(root))
    written = target.read_bytes()
    receipt_path = store.state / "commits" / (batch["batch_id"] + ".json")
    assert json.loads(receipt_path.read_text())["status"] == "pending"
    resumed = MemoryStore(store.data_dir)
    assert resumed.summary_partition_next(batch["batch_id"])["complete"]
    assert target.read_bytes() == written
    assert json.loads(receipt_path.read_text())["status"] == "committed"


def test_manual_edit_during_final_intent_is_preserved_as_conflict(tmp_path, monkeypatch):
    store, _, target = setup_window(tmp_path)
    batch = store.prepare_summary("L2", "2026-09-01", now=NOW)
    root = ready_root(store, batch)
    notes = target.read_bytes() + "此时新写的手记。\n".encode()
    atomic = partitions._atomic

    def edit_after_intent(path, raw):
        atomic(path, raw)
        if (
            Path(path).parent == store.state / "commits"
            and json.loads(raw).get("status") == "pending"
        ):
            target.write_bytes(notes)

    with monkeypatch.context() as patch:
        patch.setattr(partitions, "_atomic", edit_after_intent)
        with pytest.raises(MemoryConflictError, match="later edit"):
            store.commit_summary_partition(batch["batch_id"], root["node_id"], candidate(root))
    assert target.read_bytes() == notes
    with pytest.raises(MemoryConflictError, match="later edit"):
        MemoryStore(store.data_dir)
    assert target.read_bytes() == notes


def test_empty_markdown_is_an_explicit_gap_in_complete_partition_window(tmp_path):
    store = MemoryStore(tmp_path / "data")
    root = store.workspace / "memory/chronicle/diary"
    root.mkdir(parents=True)
    (root / "2026-09-01.md").write_text("Large synthetic diary. " * 10000)
    (root / "2026-09-02.md").write_bytes(b"")
    batch = store.prepare_summary("L3", "2026-W36", now=NOW)
    node = ready_root(store, batch)
    assert store.commit_summary_partition(batch["batch_id"], node["node_id"], candidate(node))[
        "complete"
    ]
    receipt = json.loads((store.state / "commits" / (batch["batch_id"] + ".json")).read_text())
    assert receipt["manifest"]["coverage_ref"]["records"] == 2
    assert receipt["manifest"]["coverage_ref"]["missing_fragments"] == 1


@pytest.mark.parametrize("status", ["partitioning", "pending", "committed"])
def test_pure_header_validator_rejects_old_capability_without_mutation(status, monkeypatch):
    identifier = "a" * 64
    header = {
        "schema_version": 2,
        "format": partitions.FORMAT,
        "batch_id": identifier,
        "partition_plan_sha256": identifier,
        "status": status,
        "manifest": {"schema_version": 2, "partition_plan": identifier},
    }
    before = deepcopy(header)

    def no_io(*args, **kwargs):
        raise AssertionError("Header validation cannot read or write")

    with monkeypatch.context() as patch:
        patch.setattr("builtins.open", no_io)
        patch.setattr(Path, "open", no_io)
        assert partitions.validate_summary_commit_header(header) == 2
        with pytest.raises(SummaryValidationError, match="Unsupported summary commit schema"):
            partitions.validate_summary_commit_header(header, supported_schema=1)
    assert header == before
