"""Independent synthetic provenance regressions for partition review."""

import datetime as dt
import hashlib
import json
from pathlib import Path

import pytest

from alice_codex import memory
from alice_codex.memory import (
    MemoryConflictError,
    MemoryStore,
    SourceChangedError,
    SummaryValidationError,
    _record_slices,
)


NOW = dt.datetime(2026, 10, 5, tzinfo=dt.timezone.utc)
PERIOD = "2026-09-01T00:00"


def make_window(tmp_path, *, broken=False, separator=b"\n"):
    store = MemoryStore(tmp_path / "data")
    source = store.workspace / "memory/chronicle/traces/2026-09-01.jsonl"
    source.parent.mkdir(parents=True)
    records = [
        json.dumps(
            {"timestamp": "2026-09-01T00:15:00+08:00", "content": f"Synthetic {index}"}
        ).encode()
        for index in range(65)
    ]
    source.write_bytes(
        (b"BROKEN" + separator if broken else b"") + separator.join(records) + separator
    )
    return store, source, store.prepare_summary("L1", PERIOD, now=NOW)


def candidate(node):
    sources = json.loads(Path(node["manifest_path"]).read_text())["sources"]
    good = [source["source_id"] for source in sources if not source["parse_error"]]
    return {
        "content": "Synthetic summary " + " ".join(f"[source:{sid}]" for sid in good[:1]),
        "source_ids": good[:1],
        "covered_source_ids": good,
        "missing": [
            {"source_id": source["source_id"], "reason": "Synthetic malformed input"}
            for source in sources
            if source["parse_error"]
        ],
    }


def drain(store, batch):
    for _ in range(100):
        page = store.summary_partition_next(batch["batch_id"])
        if page["complete"]:
            return
        assert page["ready"], "An incomplete synthetic DAG must make progress"
        for node in page["ready"]:
            store.commit_summary_partition(batch["batch_id"], node["node_id"], candidate(node))
    pytest.fail("Synthetic DAG did not reach its root")


def test_committed_output_must_equal_the_validated_candidate(tmp_path):
    store, _, batch = make_window(tmp_path)
    node = store.summary_partition_next(batch["batch_id"])["ready"][0]
    store.commit_summary_partition(batch["batch_id"], node["node_id"], candidate(node))
    directory = Path(batch["manifest_path"]).parent
    output = directory / "outputs" / f"{node['node_id']}.md"
    receipt_path = directory / "receipts" / f"{node['node_id']}.json"
    receipt = json.loads(receipt_path.read_text())
    output.write_text("Synthetic damaged output that was never submitted as a candidate.")
    receipt["summary_sha256"] = hashlib.sha256(output.read_bytes()).hexdigest()
    receipt_path.write_text(json.dumps(receipt))

    with pytest.raises(SourceChangedError):
        drain(store, batch)
    assert not (store.workspace / "memory/chronicle/hourly/2026-09-01.jsonl").exists()


def test_fragment_locator_cannot_relabel_an_existing_parent_source(tmp_path):
    store, _, batch = make_window(tmp_path)
    node = store.summary_partition_next(batch["batch_id"])["ready"][0]
    source = json.loads(Path(node["manifest_path"]).read_text())["sources"][0]
    source_id = source["source_id"]
    expected_content = store.read_source(source_id)["content"]
    assert "Synthetic" in expected_content
    locator_path = store.state / "partition-sources" / f"{source_id}.json"
    locator = json.loads(locator_path.read_text())
    locator["path"] = "memory/chronicle/traces/falsely-claimed-source.jsonl"
    locator["line"] = 999
    locator_path.write_text(json.dumps(locator))

    with pytest.raises(SourceChangedError):
        store.read_source(source_id)


def test_raw_cr_record_boundaries_preserve_legacy_parent_identity(tmp_path):
    store, source, batch = make_window(tmp_path, separator=b"\r")
    expected_count = len(list(_record_slices(source, 4096)))
    assert expected_count == 65
    assert batch["source_count"] == expected_count
    page = store.summary_partition_next(batch["batch_id"])
    for node in page["ready"]:
        sources = json.loads(Path(node["manifest_path"]).read_text())["sources"]
        assert all(source["parse_error"] is None for source in sources)


def test_higher_layer_carries_partition_ancestor_gaps(tmp_path):
    store, _, first = make_window(tmp_path, broken=True)
    drain(store, first)
    hourly = json.loads((store.workspace / "memory/chronicle/hourly/2026-09-01.jsonl").read_text())
    assert hourly["coverage_ref"]["missing_fragments"] == 1

    second = store.prepare_summary("L2", "2026-09-01", now=NOW)
    assert second.get("strategy") == "partitioned-v1"
    drain(store, second)
    receipt = json.loads((store.state / "commits" / f"{second['batch_id']}.json").read_text())
    reference = receipt["manifest"]["coverage_ref"]
    assert reference["has_inherited_gaps"]
    upstream = reference["upstream_ref"]
    upstream_path = Path(second["manifest_path"]).parent / upstream["path"]
    raw = upstream_path.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == upstream["sha256"]
    assert first["batch_id"] in raw.decode()


def test_pending_partition_cannot_redirect_the_frozen_target_on_restart(tmp_path, monkeypatch):
    store, _, batch = make_window(tmp_path)
    plan = json.loads(Path(batch["manifest_path"]).read_text())
    root_id = plan["root_node_id"]
    for _ in range(100):
        page = store.summary_partition_next(batch["batch_id"])
        root = next((node for node in page["ready"] if node["node_id"] == root_id), None)
        if root is not None:
            break
        assert page["ready"]
        for node in page["ready"]:
            store.commit_summary_partition(batch["batch_id"], node["node_id"], candidate(node))
    else:
        pytest.fail("Synthetic DAG did not make its root ready")

    target = store.workspace / plan["target"]
    atomic = memory._atomic

    def stop_before_target(path, raw):
        if Path(path) == target:
            raise RuntimeError("Synthetic interruption before canonical target write")
        return atomic(path, raw)

    with monkeypatch.context() as patch:
        patch.setattr(memory, "_atomic", stop_before_target)
        with pytest.raises(RuntimeError, match="before canonical target"):
            store.commit_summary_partition(batch["batch_id"], root_id, candidate(root))

    intent_path = store.state / "commits" / f"{batch['batch_id']}.json"
    intent = json.loads(intent_path.read_text())
    assert intent["status"] == "pending" and not target.exists()
    # One damaged mutable field must not override the immutable plan's target.
    intent["target"] = "memory/chronicle/hourly/falsely-selected-target.jsonl"
    intent_path.write_text(json.dumps(intent))
    wrong_target = store.workspace / intent["target"]

    with pytest.raises((MemoryConflictError, SummaryValidationError)):
        MemoryStore(store.data_dir)
    assert not wrong_target.exists()
    assert not target.exists()
    assert json.loads(intent_path.read_text())["status"] == "pending"


def test_legacy_commit_api_cannot_complete_a_partition_without_its_root(tmp_path):
    store, _, batch = make_window(tmp_path)
    node = store.summary_partition_next(batch["batch_id"])["ready"][0]

    with pytest.raises((FileNotFoundError, SummaryValidationError)):
        store.commit_summary(batch["batch_id"], candidate(node))

    page = store.summary_partition_next(batch["batch_id"])
    assert not page["complete"] and page["completed_nodes"] == 0
    assert not (store.workspace / "memory/chronicle/hourly/2026-09-01.jsonl").exists()
    drain(store, batch)
    assert store.summary_partition_next(batch["batch_id"])["complete"]


def test_committed_coverage_count_cannot_hide_an_ancestor_gap(tmp_path):
    store, source, batch = make_window(tmp_path, broken=True)
    drain(store, batch)
    intent_path = store.state / "commits" / f"{batch['batch_id']}.json"
    intent = json.loads(intent_path.read_text())
    target = store.workspace / intent["target"]
    source_before, target_before = source.read_bytes(), target.read_bytes()
    assert intent["manifest"]["coverage_ref"]["missing_fragments"] == 1
    intent["manifest"]["coverage_ref"]["missing_fragments"] = 0
    intent_path.write_text(json.dumps(intent))
    damaged_intent = intent_path.read_bytes()

    with pytest.raises((MemoryConflictError, SummaryValidationError)):
        MemoryStore(store.data_dir)

    assert source.read_bytes() == source_before
    assert target.read_bytes() == target_before
    assert intent_path.read_bytes() == damaged_intent


@pytest.mark.parametrize("target_written", [False, True], ids=["before-target", "after-target"])
def test_pending_rendered_output_is_bound_to_root_candidate(tmp_path, monkeypatch, target_written):
    store, source, batch = make_window(tmp_path)
    plan = json.loads(Path(batch["manifest_path"]).read_text())
    for _ in range(100):
        page = store.summary_partition_next(batch["batch_id"])
        root = next(
            (node for node in page["ready"] if node["node_id"] == plan["root_node_id"]),
            None,
        )
        if root is not None:
            break
        assert page["ready"]
        for node in page["ready"]:
            store.commit_summary_partition(batch["batch_id"], node["node_id"], candidate(node))
    else:
        pytest.fail("Synthetic DAG did not make its root ready")

    target = store.workspace / plan["target"]
    intent_path = store.state / "commits" / f"{batch['batch_id']}.json"
    atomic = memory._atomic

    def stop_at_boundary(path, raw):
        if (not target_written and Path(path) == target) or (
            target_written
            and Path(path) == intent_path
            and json.loads(raw).get("status") == "committed"
        ):
            raise RuntimeError("Synthetic interruption with a durable pending intent")
        return atomic(path, raw)

    with monkeypatch.context() as patch:
        patch.setattr(memory, "_atomic", stop_at_boundary)
        with pytest.raises(RuntimeError, match="durable pending intent"):
            store.commit_summary_partition(batch["batch_id"], root["node_id"], candidate(root))

    assert target.exists() == target_written
    target_before = target.read_bytes() if target_written else None
    source_before = source.read_bytes()
    intent = json.loads(intent_path.read_text())
    assert intent["status"] == "pending"
    # A matching self-reported hash cannot authorize text the root never committed.
    intent["rendered"] = "Unvalidated synthetic replacement of the approved summary.\n"
    intent["after_sha256"] = hashlib.sha256(intent["rendered"].encode()).hexdigest()
    intent_path.write_text(json.dumps(intent))
    damaged_intent = intent_path.read_bytes()

    with pytest.raises((MemoryConflictError, SummaryValidationError)):
        MemoryStore(store.data_dir)

    assert target.exists() == target_written
    if target_written:
        assert target.read_bytes() == target_before
    assert source.read_bytes() == source_before
    assert intent_path.read_bytes() == damaged_intent
