"""Copied synthetic summaries cannot silently shed embedded coverage evidence."""

import hashlib
import json
from pathlib import Path

import pytest

from alice_codex.memory import MemoryStore, SummaryValidationError
from test_summary_partition_review import NOW, drain, make_window


def copy_into(store, relative, raw):
    target = store.workspace / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(raw)
    return target


def hourly_with_gap(tmp_path):
    store, _, batch = make_window(tmp_path, broken=True)
    drain(store, batch)
    path = store.workspace / "memory/chronicle/hourly/2026-09-01.jsonl"
    value = json.loads(path.read_text())
    assert value["coverage_ref"]["missing_fragments"] == 1
    return store, batch, path


def diary_with_gap(tmp_path):
    store, ancestor, _ = hourly_with_gap(tmp_path)
    batch = store.prepare_summary("L2", "2026-09-01", now=NOW)
    assert batch["strategy"] == "partitioned-v1"
    drain(store, batch)
    path = store.workspace / "memory/chronicle/diary/2026-09-01.md"
    assert f"<!-- anima-coverage:{batch['batch_id']}:" in path.read_text()
    return store, ancestor, batch, path


@pytest.mark.parametrize("giant", [False, True], ids=["small-jsonl", "giant-jsonl-tail-reference"])
def test_copied_jsonl_without_local_proof_cannot_become_a_gap_free_higher_summary(tmp_path, giant):
    _, _, source = hourly_with_gap(tmp_path / "producer")
    value = json.loads(source.read_text())
    if giant:
        # Keep the reference after more than the legacy parser's 1 MiB limit.
        # A bounded scanner must detect it without treating absence in a prefix
        # as evidence that the complete record has no upstream dependency.
        value["content"] = "Synthetic copied long summary. " + "x" * (1024 * 1024 + 1)
        reference = value.pop("coverage_ref")
        value["coverage_ref"] = reference
    raw = (json.dumps(value, ensure_ascii=False) + "\n").encode()
    imported = MemoryStore(tmp_path / "imported")
    copied = copy_into(imported, "memory/chronicle/hourly/2026-09-01.jsonl", raw)
    assert not (imported.state / "commits").exists()

    with pytest.raises(SummaryValidationError):
        imported.prepare_summary("L2", "2026-09-01", now=NOW)

    assert copied.read_bytes() == raw
    assert not (imported.workspace / "memory/chronicle/diary/2026-09-01.md").exists()
    assert not (imported.state / "commits").exists()


@pytest.mark.parametrize(
    "level,period,target",
    [
        ("L3", "2026-W36", "memory/chronicle/weekly/2026-W36.md"),
        ("L4", "2026-09", "memory/chronicle/monthly/2026-09.md"),
    ],
    ids=["daily-to-weekly", "weekly-to-monthly"],
)
def test_copied_markdown_marker_without_local_proof_cannot_drop_ancestor_gaps(
    tmp_path, level, period, target
):
    producer, _, _, source = diary_with_gap(tmp_path / "producer")
    if level == "L4":
        weekly = producer.prepare_summary("L3", "2026-W36", now=NOW)
        assert weekly["strategy"] == "partitioned-v1"
        drain(producer, weekly)
        source = producer.workspace / "memory/chronicle/weekly/2026-W36.md"
        assert f"<!-- anima-coverage:{weekly['batch_id']}:" in source.read_text()
    raw = source.read_bytes()
    imported = MemoryStore(tmp_path / "imported")
    copied = copy_into(imported, source.relative_to(producer.workspace).as_posix(), raw)
    assert not (imported.state / "commits").exists()

    with pytest.raises(SummaryValidationError):
        imported.prepare_summary(level, period, now=NOW)

    assert copied.read_bytes() == raw
    assert not (imported.workspace / target).exists()
    assert not (imported.state / "commits").exists()


@pytest.mark.parametrize("boundary_overlap", [None, 10, 40], ids=["plain", "split-prefix", "split-body"])
def test_valid_local_marker_copied_to_another_week_keeps_its_original_ancestor(
    tmp_path, boundary_overlap
):
    store, ancestor, daily, original = diary_with_gap(tmp_path)
    original_bytes = original.read_bytes()
    raw = original_bytes
    if boundary_overlap is not None:
        # Exercise both a split reserved prefix and a complete prefix whose
        # identifier/digest only becomes available in the following read.
        marker_offset = raw.index(b"<!-- anima-coverage:")
        padding = 65536 - boundary_overlap - marker_offset
        assert padding > 0
        raw = b" " * padding + raw
        assert raw.index(b"<!-- anima-coverage:") == 65536 - boundary_overlap
    # The original Sep 1 document is outside W37. Only the byte-identical Sep 8
    # copy is selected, so lineage cannot be inferred from the committed target
    # filename; the embedded marker must carry its actual local proof.
    copied = copy_into(store, "memory/chronicle/diary/2026-09-08.md", raw)

    batch = store.prepare_summary("L3", "2026-W37", now=NOW)

    assert batch.get("strategy") == "partitioned-v1"
    assert batch["source_count"] == 1
    drain(store, batch)
    receipt_path = store.state / "commits" / f"{batch['batch_id']}.json"
    reference = json.loads(receipt_path.read_text())["manifest"]["coverage_ref"]
    assert reference["has_inherited_gaps"]
    upstream = reference["upstream_ref"]
    upstream_path = Path(batch["manifest_path"]).parent / upstream["path"]
    raw_upstream = upstream_path.read_bytes()
    assert hashlib.sha256(raw_upstream).hexdigest() == upstream["sha256"]
    rows = [json.loads(line) for line in raw_upstream.splitlines()]
    assert any(row["batch_id"] == daily["batch_id"] for row in rows)
    daily_ref = next(row["coverage_ref"] for row in rows if row["batch_id"] == daily["batch_id"])
    assert daily_ref["has_inherited_gaps"]
    daily_upstream = Path(daily["manifest_path"]).parent / daily_ref["upstream_ref"]["path"]
    assert ancestor["batch_id"] in daily_upstream.read_text()
    assert original.read_bytes() == original_bytes
    assert copied.read_bytes() == raw


def test_repeated_batch_marker_must_validate_every_digest_before_deduplication(tmp_path):
    store, _, daily, original = diary_with_gap(tmp_path)
    original_bytes = original.read_bytes()
    prefix = f"<!-- anima-coverage:{daily['batch_id']}:".encode()
    correct_digest = original_bytes.split(prefix, 1)[1].split(b" -->", 1)[0].decode()
    wrong_digest = "0" * 64 if correct_digest != "0" * 64 else "1" * 64
    raw = original_bytes + b"\n" + prefix + wrong_digest.encode() + b" -->\n"
    assert raw.count(prefix) == 2
    copied = copy_into(store, "memory/chronicle/diary/2026-09-08.md", raw)

    with pytest.raises(SummaryValidationError):
        store.prepare_summary("L3", "2026-W37", now=NOW)

    assert original.read_bytes() == original_bytes
    assert copied.read_bytes() == raw
    assert not (store.workspace / "memory/chronicle/weekly/2026-W37.md").exists()


@pytest.mark.parametrize("damage", ["illegal-hex", "short-id", "truncated-eof"])
def test_malformed_reserved_markers_cannot_be_treated_as_ordinary_markdown(tmp_path, damage):
    store, _, daily, original = diary_with_gap(tmp_path)
    original_bytes = original.read_bytes()
    identifier = daily["batch_id"].encode()
    prefix = b"<!-- anima-coverage:"
    digest = original_bytes.split(prefix + identifier + b":", 1)[1].split(b" -->", 1)[0]
    if damage == "illegal-hex":
        malformed = prefix + identifier + b":" + b"g" + digest[1:] + b" -->"
    elif damage == "short-id":
        malformed = prefix + identifier[:-1] + b":" + digest + b" -->"
    else:
        malformed = prefix + identifier + b":" + digest[:12]
    # Keep a valid local marker first so missing imported proof cannot mask
    # rejection of the malformed reserved marker that follows it.
    raw = original_bytes + b"\n" + malformed
    copied = copy_into(store, "memory/chronicle/diary/2026-09-08.md", raw)

    with pytest.raises(SummaryValidationError):
        store.prepare_summary("L3", "2026-W37", now=NOW)

    assert original.read_bytes() == original_bytes
    assert copied.read_bytes() == raw
    assert not (store.workspace / "memory/chronicle/weekly/2026-W37.md").exists()


@pytest.mark.parametrize("boundary_overlap", [10, 40], ids=["split-prefix", "split-body"])
def test_malformed_reserved_marker_crossing_a_read_boundary_is_rejected(tmp_path, boundary_overlap):
    store, _, daily, original = diary_with_gap(tmp_path)
    original_bytes = original.read_bytes()
    marker = b"<!-- anima-coverage:" + daily["batch_id"].encode() + b":" + b"g" * 64 + b" -->"
    start = 65536 - boundary_overlap
    assert len(original_bytes) < start
    raw = original_bytes + b" " * (start - len(original_bytes)) + marker
    assert raw[start:] == marker
    assert start < 65536 < len(raw)
    copied = copy_into(store, "memory/chronicle/diary/2026-09-08.md", raw)

    with pytest.raises(SummaryValidationError):
        store.prepare_summary("L3", "2026-W37", now=NOW)

    assert original.read_bytes() == original_bytes
    assert copied.read_bytes() == raw
    assert not (store.workspace / "memory/chronicle/weekly/2026-W37.md").exists()
