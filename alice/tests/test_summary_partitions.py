"""Public-API synthetic partition acceptance; no model or private runtime data."""

from concurrent.futures import ThreadPoolExecutor
import datetime as dt
import hashlib
import json
from pathlib import Path

import pytest

from alice_codex.memory import (
    AUTO_SEPARATOR,
    MemoryConflictError,
    MemoryStore,
    SourceChangedError,
    SummaryValidationError,
)


NOW = dt.datetime(2026, 10, 5, tzinfo=dt.timezone.utc)
PERIOD = "2026-09-01T00:00"
L0_PATH = "memory/chronicle/traces/2026-09-01.jsonl"


def put(root: Path, relative: str, content: bytes) -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def record(content, timestamp="2026-09-01T00:15:00+08:00") -> bytes:
    # Keep the real top-level timestamp after potentially enormous content.
    return (
        json.dumps({"content": content, "timestamp": timestamp}, ensure_ascii=False) + "\n"
    ).encode()


def node_manifest(node: dict) -> dict:
    return json.loads(Path(node["manifest_path"]).read_text())


def candidate_for(node: dict) -> dict:
    sources = node_manifest(node)["sources"]
    good = [source["source_id"] for source in sources if not source["parse_error"]]
    bad = [source["source_id"] for source in sources if source["parse_error"]]
    cited = good[:1]
    return {
        "content": "Synthetic structural summary. " + " ".join(f"[source:{sid}]" for sid in cited),
        "source_ids": cited,
        "covered_source_ids": good,
        "missing": [
            {"source_id": sid, "reason": "Synthetic source has an explicit parse gap"}
            for sid in bad
        ],
    }


def partition(store: MemoryStore, level="L1", period=PERIOD) -> dict:
    result = store.prepare_summary(level, period, now=NOW)
    assert result["strategy"] == "partitioned-v1"
    assert Path(result["manifest_path"]).is_file()
    return result


def check_ready_bounds(store: MemoryStore, page: dict, limit=4) -> None:
    assert len(page["ready"]) <= limit
    for node in page["ready"]:
        sources = node_manifest(node)["sources"]
        assert 0 < len(sources) <= 64
        assert node["source_count"] == len(sources)
        assert Path(node["candidate_path"]).is_relative_to(store.workspace)
        assert len(node["prompt"].encode()) <= 128 * 1024
        size = 0
        for source in sources:
            value = store.read_source(source["source_id"], max_chars=1_000_000)
            raw = value["content"].encode()
            assert not value["truncated"]
            if "byte_start" in value:
                assert value["byte_end"] - value["byte_start"] <= 64 * 1024
            else:
                assert len(raw) <= 16 * 1024
            size += len(raw)
        assert size <= 128 * 1024


def drain(store: MemoryStore, batch_id: str, inspect=None) -> list[tuple[dict, dict, dict]]:
    committed = []
    for _ in range(2000):
        page = store.summary_partition_next(batch_id, limit=4)
        if page["complete"]:
            assert page["ready"] == []
            assert page["completed_nodes"] == page["total_nodes"]
            assert committed and sum(result["complete"] for _, _, result in committed) == 1
            return committed
        assert page["ready"], "An unfinished DAG without a ready node cannot progress"
        for node in page["ready"]:
            if inspect is not None:
                inspect(node)
            candidate = candidate_for(node)
            result = store.commit_summary_partition(batch_id, node["node_id"], candidate)
            committed.append((node, candidate, result))
    pytest.fail("Synthetic partition plan did not reach its root within a bounded number of pages")


def test_small_window_retains_schema_one_and_existing_commit_api(tmp_path):
    store = MemoryStore(tmp_path / "data")
    put(store.workspace, L0_PATH, record("Small original source"))

    batch = store.prepare_summary("L1", PERIOD, now=NOW)

    assert batch.get("strategy") != "partitioned-v1"
    assert node_manifest(batch)["schema_version"] == 1
    candidate = candidate_for(batch)
    first = store.commit_summary(batch["batch_id"], candidate)
    assert not first["already_committed"]
    target = Path(first["target_path"])
    original = target.read_bytes()
    assert store.commit_summary(batch["batch_id"], candidate)["already_committed"]
    assert target.read_bytes() == original


def test_giant_jsonl_uses_trailing_top_level_timestamp_and_covers_exact_original_bytes(tmp_path):
    store = MemoryStore(tmp_path / "data")
    content = "".join(f"{number:06d}: 展示片段🌊 " + "x" * 64 for number in range(14000))
    selected = record(content, timestamp="2026-09-02T00:15:00+08:00")
    excluded = record({"timestamp": "2026-09-02T00:15:00+08:00", "content": "Wrong top-level day"})
    raw = selected + excluded
    assert len(selected) > 1024 * 1024
    source = put(store.workspace, L0_PATH, raw)

    batch = partition(store, period="2026-09-02T00:00")
    assert batch["source_count"] == 1
    assert store.prepare_summary("L1", "2026-09-02T00:00", now=NOW)["batch_id"] == batch["batch_id"]
    ranges, parents = [], set()

    def inspect(node):
        for item in node_manifest(node)["sources"]:
            if item["path"] != L0_PATH:
                continue
            value = store.read_source(item["source_id"], max_chars=1_000_000)
            start, end = value["byte_start"], value["byte_end"]
            assert 0 <= start < end <= len(selected)
            assert end - start <= 64 * 1024
            assert value["content"].encode() == raw[start:end]
            assert value["file_sha256"] == hashlib.sha256(raw).hexdigest()
            assert not item["parse_error"]
            ranges.append((start, end))
            parents.add(value["parent_source_id"])

    commits = drain(store, batch["batch_id"], inspect)
    assert len(commits) > 3
    assert len(parents) == 1 and len(ranges) > 16
    position = 0
    for start, end in sorted(ranges):
        assert start == position, "Original bytes must have neither a gap nor duplicate coverage"
        position = end
    assert position == len(selected)
    assert source.read_bytes() == raw
    final = store.workspace / "memory/chronicle/hourly/2026-09-02.jsonl"
    assert len(final.read_text().splitlines()) == 1


@pytest.mark.parametrize("boundary", ["bytes_over_16_mib", "records_over_10000"])
def test_large_windows_automatically_prepare_bounded_work_without_manual_partition(
    tmp_path, boundary
):
    store = MemoryStore(tmp_path / "data")
    if boundary == "bytes_over_16_mib":
        count = 1024
        raw = b"".join(record(f"Record {index}: " + "x" * (17 * 1024)) for index in range(count))
        assert len(raw) > 16 * 1024 * 1024
    else:
        count = 10001
        raw = b"".join(record(f"Record {index}") for index in range(count))
    put(store.workspace, L0_PATH, raw)

    batch = partition(store)
    page = store.summary_partition_next(batch["batch_id"], limit=4)

    assert batch["source_count"] == count
    assert not page["complete"] and page["total_nodes"] > 4
    assert page["completed_nodes"] == 0
    check_ready_bounds(store, page)
    assert not (store.workspace / "memory/chronicle/hourly/2026-09-01.jsonl").exists()


def test_root_cannot_be_ready_or_complete_while_a_required_leaf_is_uncommitted(tmp_path):
    store = MemoryStore(tmp_path / "data")
    put(store.workspace, L0_PATH, b"".join(record(f"Source {i}") for i in range(193)))
    batch = partition(store)
    first = store.summary_partition_next(batch["batch_id"], limit=4)
    assert len(first["ready"]) >= 2
    withheld = first["ready"][0]
    target = store.workspace / "memory/chronicle/hourly/2026-09-01.jsonl"
    for _ in range(100):
        page = store.summary_partition_next(batch["batch_id"], limit=4)
        assert not page["complete"]
        ready = [node for node in page["ready"] if node["node_id"] != withheld["node_id"]]
        if not ready:
            assert [node["node_id"] for node in page["ready"]] == [withheld["node_id"]]
            break
        for node in ready:
            result = store.commit_summary_partition(
                batch["batch_id"], node["node_id"], candidate_for(node)
            )
            assert not result["complete"]
        assert not target.exists()
    else:
        pytest.fail("Independent work did not settle while withholding one required leaf")
    assert not target.exists()
    assert page["completed_nodes"] < page["total_nodes"]
    store.commit_summary_partition(batch["batch_id"], withheld["node_id"], candidate_for(withheld))
    drain(store, batch["batch_id"])
    assert len(target.read_text().splitlines()) == 1


def test_concurrent_identical_node_commit_is_durable_and_conflicting_retry_is_rejected(tmp_path):
    store = MemoryStore(tmp_path / "data")
    put(store.workspace, L0_PATH, record("Concurrent source " * 10000))
    batch = partition(store)
    node = store.summary_partition_next(batch["batch_id"], limit=1)["ready"][0]
    candidate = candidate_for(node)

    def commit():
        return MemoryStore(store.data_dir).commit_summary_partition(
            batch["batch_id"], node["node_id"], candidate
        )

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: commit(), range(4)))
    assert all(not result["complete"] for result in results)
    recovered = MemoryStore(store.data_dir)
    next_page = recovered.summary_partition_next(batch["batch_id"], limit=4)
    assert next_page["completed_nodes"] == 1
    assert node["node_id"] not in {item["node_id"] for item in next_page["ready"]}
    with pytest.raises(MemoryConflictError):
        recovered.commit_summary_partition(
            batch["batch_id"],
            node["node_id"],
            {
                **candidate,
                "content": "Different candidate. " + candidate["content"],
            },
        )
    commits = drain(recovered, batch["batch_id"])
    root, root_candidate, result = commits[-1]
    assert result["complete"]
    target = recovered.workspace / "memory/chronicle/hourly/2026-09-01.jsonl"
    before = target.read_bytes()
    assert MemoryStore(store.data_dir).commit_summary_partition(
        batch["batch_id"], root["node_id"], root_candidate
    )["complete"]
    assert target.read_bytes() == before


def test_partition_candidate_rejects_missing_coverage_and_oversized_summary(tmp_path):
    store = MemoryStore(tmp_path / "data")
    put(store.workspace, L0_PATH, b"".join(record(f"Source {i}") for i in range(65)))
    batch = partition(store)
    node = store.summary_partition_next(batch["batch_id"], limit=1)["ready"][0]
    candidate = candidate_for(node)
    invalid = [
        {**candidate, "covered_source_ids": candidate["covered_source_ids"][:-1]},
        {**candidate, "content": candidate["content"] + "x" * (16 * 1024)},
    ]
    for value in invalid:
        with pytest.raises(SummaryValidationError):
            store.commit_summary_partition(batch["batch_id"], node["node_id"], value)
        assert store.summary_partition_next(batch["batch_id"])["completed_nodes"] == 0


def test_frozen_partition_accepts_later_append_and_new_plan_keeps_added_source(tmp_path):
    store = MemoryStore(tmp_path / "data")
    original = record("Frozen prefix " * 12000)
    source = put(store.workspace, L0_PATH, original)
    batch = partition(store)
    appended = record("New message after the frozen source")
    with source.open("ab") as stream:
        stream.write(appended)

    drain(store, batch["batch_id"])
    newer = partition(store)

    assert newer["batch_id"] != batch["batch_id"]
    assert newer["source_count"] == batch["source_count"] + 1
    assert source.read_bytes() == original + appended


def test_replaced_source_blocks_final_commit_and_preserves_original_plan(tmp_path):
    store = MemoryStore(tmp_path / "data")
    original = record("Original long evidence " * 8000)
    source = put(store.workspace, L0_PATH, original)
    batch = partition(store)
    frozen_manifest = Path(batch["manifest_path"]).read_bytes()
    changed = put(tmp_path, "replacement.jsonl", record("Replacement evidence " * 9000))
    changed.replace(source)
    replacement = source.read_bytes()

    with pytest.raises(SourceChangedError):
        drain(store, batch["batch_id"])

    assert source.read_bytes() == replacement
    assert Path(batch["manifest_path"]).read_bytes() == frozen_manifest
    assert not (store.workspace / "memory/chronicle/hourly/2026-09-01.jsonl").exists()


def test_large_markdown_l3_partition_preserves_manual_notes_and_previous_summary(tmp_path):
    store = MemoryStore(tmp_path / "data")
    original = ("Long diary synthetic paragraph. 展示片段。\n" * 6000).encode()
    relative = "memory/chronicle/diary/2026-09-01.md"
    source = put(store.workspace, relative, original)
    previous = ("## 手记\nPreserve manual notes\n\n" + AUTO_SEPARATOR + "\nOld summary\n").encode()
    target = put(store.workspace, "memory/chronicle/weekly/2026-W36.md", previous)
    batch = partition(store, "L3", "2026-W36")
    assert batch["source_count"] == 1
    manual = b"\n## New note during partition work\nRetain this too.\n"
    with target.open("ab") as stream:
        stream.write(manual)

    drain(store, batch["batch_id"])

    assert source.read_bytes() == original
    assert target.read_bytes().startswith(previous + manual)
    assert target.read_text().count(f"<!-- anima-summary:{batch['batch_id']} -->") == 1


def test_partition_root_output_remains_usable_through_all_four_summary_levels(tmp_path):
    store = MemoryStore(tmp_path / "data")
    source = put(store.workspace, L0_PATH, record("Four level structural chain " * 7000))
    source_before = source.read_bytes()
    windows = [("L1", PERIOD), ("L2", "2026-09-01"), ("L3", "2026-W36"), ("L4", "2026-09")]
    for index, (level, period) in enumerate(windows):
        store = MemoryStore(store.data_dir)
        batch = store.prepare_summary(level, period, now=NOW)
        assert batch["source_count"] >= 1
        if index == 0:
            assert batch["strategy"] == "partitioned-v1"
        if batch.get("strategy") == "partitioned-v1":
            drain(store, batch["batch_id"])
        else:
            result = store.commit_summary(batch["batch_id"], candidate_for(batch))
            assert Path(result["target_path"]).is_file()
    assert source.read_bytes() == source_before
    final = store.workspace / "memory/chronicle/monthly/2026-09.md"
    assert "Synthetic structural summary" in final.read_text()


def test_bad_source_requires_missing_and_root_proof_retains_inherited_gap(tmp_path):
    store = MemoryStore(tmp_path / "data")
    raw = b"Synthetic broken JSONL with invalid UTF-8 \xff\n" + b"".join(
        record(f"Valid source {index}") for index in range(65)
    )
    original = put(store.workspace, L0_PATH, raw)
    batch = partition(store)
    ready = store.summary_partition_next(batch["batch_id"], limit=4)["ready"]
    bad_node = next(
        node
        for node in ready
        if any(source["parse_error"] for source in node_manifest(node)["sources"])
    )
    sources = node_manifest(bad_node)["sources"]
    bad_ids = {source["source_id"] for source in sources if source["parse_error"]}
    valid = candidate_for(bad_node)
    with pytest.raises(SummaryValidationError):
        store.commit_summary_partition(
            batch["batch_id"],
            bad_node["node_id"],
            {
                **valid,
                "covered_source_ids": [source["source_id"] for source in sources],
                "missing": [],
            },
        )
    assert store.summary_partition_next(batch["batch_id"])["completed_nodes"] == 0

    drain(store, batch["batch_id"])

    target = store.workspace / "memory/chronicle/hourly/2026-09-01.jsonl"
    summary = json.loads(target.read_text())
    reference = summary["coverage_ref"]
    coverage_path = Path(batch["manifest_path"]).parent / reference["path"]
    coverage_bytes = coverage_path.read_bytes()
    rows = [json.loads(line) for line in coverage_bytes.splitlines()]
    assert reference["sha256"] == hashlib.sha256(coverage_bytes).hexdigest()
    assert reference["records"] == batch["source_count"] == 66
    assert reference["fragments"] == len(rows)
    missing = [row for row in rows if row["status"] == "missing"]
    assert reference["missing_fragments"] == len(missing) == len(bad_ids)
    assert {row["source_id"] for row in missing} == bad_ids
    assert all(row["reason"] for row in missing)
    assert original.read_bytes() == raw
