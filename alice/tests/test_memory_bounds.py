"""Synthetic resource limits: raw bytes survive; only retrieval previews truncate."""

import datetime as dt
import hashlib
import json
from pathlib import Path
import tracemalloc

import pytest

from alice_codex.memory import MemoryStore


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def test_large_records_archive_seed_and_read_with_bounded_heap(tmp_path, record_property):
    source = tmp_path / "old"
    note = source / "memory/notebook/large.md"
    log = source / "sessions/large.jsonl"
    for path, prefix, suffix, blocks in (
        (note, b"synthetic-note ", b" LAST-NOTE", 256),
        (log, b'{"content":"synthetic-log ', b' LAST-LOG"}\n{"content":"tail"}\n', 512),
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as stream:
            stream.write(prefix)
            for _ in range(blocks):
                stream.write(b"x" * 65536)
            stream.write(suffix)
    expected = {
        p.relative_to(source).as_posix(): (p.stat().st_size, sha256(p)) for p in (note, log)
    }

    tracemalloc.start()
    try:
        store = MemoryStore(tmp_path / "data")
        snapshot = store.snapshot_legacy(source, snapshot_id="large")
        note_id = store.search("synthetic-note")[0]["source_id"]
        log_id = store.search("synthetic-log")[0]["source_id"]
        note_end = store.read_source(note_id, max_chars=10, offset_chars=note.stat().st_size - 10)
        log_size = log.stat().st_size - len(b'{"content":"tail"}\n')
        log_end = store.read_source(log_id, max_chars=12, offset_chars=log_size - 12)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert peak < 12 * 1024 * 1024, f"Python heap exceeded 12 MiB: {peak}"
    record_property("peak_python_bytes", peak)
    record_property("raw_bytes", sum(size for size, _ in expected.values()))
    record_property("index_bytes", store.index_path.stat().st_size)
    assert note_end["content"] == " LAST-NOTE"[-10:]
    assert log_end["content"].endswith('LAST-LOG"}\n')
    assert note_end["next_offset"] is None and log_end["next_offset"] is None
    assert note_end["total_chars"] == note.stat().st_size
    assert log_end["total_chars"] == log_size
    manifest = json.loads(Path(snapshot["manifest_path"]).read_text())
    assert snapshot["total_bytes"] == sum(size for size, _ in expected.values())
    for entry in manifest["files"]:
        archived = Path(snapshot["manifest_path"]).parent / "files" / entry["path"]
        assert (entry["size"], entry["sha256"]) == expected[entry["path"]]
        assert sha256(archived) == entry["sha256"]
    assert sha256(store.workspace / "memory/notebook/large.md") == sha256(note)
    assert store.index_path.stat().st_size < 512 * 1024
    assert store.index_status()["source_count"] == 3
    assert store.index_status()["truncated_record_count"] == 2
    assert store.search("synthetic-log")[0]["parse_error"] == "record_exceeds_parse_limit"
    assert (
        store.read_source(store.search("tail")[0]["source_id"])["content"] == '{"content":"tail"}\n'
    )


@pytest.mark.parametrize("suffix", [".jsonl", ".log", ".md"])
def test_unicode_invalid_bytes_and_newlines_keep_character_paging(tmp_path, suffix):
    source = tmp_path / "old"
    path = source / "sessions" / ("unicode" + suffix)
    path.parent.mkdir(parents=True)
    raw = ("start中文🙂" + "界" * 70000).encode() + b"\xff\r\nsecond\rthird\n"
    path.write_bytes(raw)
    store = MemoryStore(tmp_path / "data")
    store.snapshot_legacy(source, snapshot_id="unicode")
    hit = store.search("start")[0]
    expected = raw.decode("utf-8", errors="replace").replace("\r\n", "\n").replace("\r", "\n")
    if suffix != ".md":
        expected = expected.splitlines(keepends=True)[0]
    chunks, offset = [], 0
    while offset is not None:
        result = store.read_source(hit["source_id"], max_chars=7001, offset_chars=offset)
        chunks.append(result["content"])
        offset = result["next_offset"]
    assert "".join(chunks) == expected
    assert result["total_chars"] == len(expected)
    assert store.read_source(hit["source_id"], offset_chars=len(expected) + 2)["content"] == ""


def test_summary_partitions_oversized_record_without_truncating_archive(tmp_path):
    store = MemoryStore(tmp_path / "data")
    path = store.workspace / "memory/chronicle/traces/2026-09-01.jsonl"
    path.parent.mkdir(parents=True)
    raw = json.dumps({"timestamp": "2026-09-01T00:30:00+08:00", "content": "x" * (2 * 1024 * 1024)})
    path.write_text(raw)
    before = sha256(path)
    batch = store.prepare_summary(
        "L1", "2026-09-01T00:00", now=dt.datetime(2026, 10, 1, tzinfo=dt.timezone.utc)
    )
    assert batch["strategy"] == "partitioned-v1"
    frozen = Path(batch["manifest_path"]).parent / "files" / path.relative_to(store.workspace)
    assert sha256(frozen) == before
    assert store.summary_partition_next(batch["batch_id"])["total_nodes"] > 1
    assert sha256(path) == before
    assert not list(store.batches.iterdir())


def test_deeply_nested_json_is_addressable_invalid_source(tmp_path, monkeypatch):
    from alice_codex import memory

    source = tmp_path / "old"
    path = source / "sessions/deep.jsonl"
    path.parent.mkdir(parents=True)
    depth = 2000
    path.write_text('{"nested":' + "[" * depth + '"synthetic-deep"' + "]" * depth + "}\n")
    store = MemoryStore(tmp_path / "data")
    real_loads = memory.json.loads

    def parse(value, *args, **kwargs):
        if '"nested":' in value:
            raise RecursionError("Platform parser recursion bound")
        return real_loads(value, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(memory.json, "loads", parse)
        store.snapshot_legacy(source, snapshot_id="deep")
    hit = store.search("synthetic-deep")[0]
    assert hit["parse_error"] == "invalid_json"
    assert store.read_source(hit["source_id"])["content"] == path.read_text()
