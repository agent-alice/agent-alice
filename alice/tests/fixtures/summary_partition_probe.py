"""Installed-runtime structural probe; no pytest, model, native process or private data.

Run the caller-selected environment's Python with -I and this file's absolute
path. The default runs three full boundary cases; --case smoke is debug only.
The caller controls the timeout and binds the report to its verified wheel.
"""

import argparse
import datetime as dt
import gc
import hashlib
import json
from pathlib import Path
import platform
import sys
import tempfile
import time
import tracemalloc

from alice_codex import memory, summary_partitions, summary_stream


FULL_CASES = ("single_17mib", "multiple_over_16mib", "short_10001")
SOURCE_PATH = "memory/chronicle/traces/2026-09-01.jsonl"
NOW = dt.datetime(2026, 10, 5, tzinfo=dt.timezone.utc)
PAGE_CHARS = 4096
READ_BYTES = 65536
HEAP_LIMIT = 16 * 1024 * 1024


class ProbeFailure(Exception):
    """Messages describe synthetic checks and never include runtime paths."""


def require(condition, message):
    if not condition:
        raise ProbeFailure(message)


def digest_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(READ_BYTES):
            digest.update(block)
    return digest.hexdigest()


def read_json(path, limit=256 * 1024):
    with path.open("rb") as handle:
        raw = handle.read(limit + 1)
    require(len(raw) <= limit, "JSON evidence exceeds the probe's metadata bound")
    return json.loads(raw)


def json_lines(path):
    with path.open("rb") as handle:
        while raw := handle.readline(256 * 1024 + 1):
            require(len(raw) <= 256 * 1024, "Catalog row exceeds its metadata bound")
            yield json.loads(raw)


def write_row(handle, value):
    handle.write(json.dumps(value, sort_keys=True) + "\n")


def source_id(namespace, relative, digest, line):
    # Independent encoding of the existing persistent source-ID contract.
    raw = (json.dumps([namespace, relative, digest, line],
                      ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()
    return "s_" + hashlib.sha256(raw).hexdigest()


def module_evidence():
    return {module.__name__: {
        "file": Path(module.__file__).name,
        "sha256": digest_file(Path(module.__file__)),
    } for module in (memory, summary_partitions, summary_stream)}


def disk_bytes(root):
    count = logical = allocated = 0
    for path in root.rglob("*"):
        if path.is_file():
            info = path.stat()
            count += 1
            logical += info.st_size
            allocated += info.st_blocks * 512
    return {"files": count, "logical_bytes": logical, "allocated_bytes": allocated}


def tree_digest(root):
    """Compare immutable plan bytes after restart, independent of inode/mtime."""
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*")):
        if path.is_file():
            digest.update(path.relative_to(root).as_posix().encode() + b"\0")
            digest.update(bytes.fromhex(digest_file(path)))
    return digest.hexdigest()


class Meter:
    def __init__(self):
        self.metrics = {}
        self.peak = 0
        tracemalloc.start()

    def call(self, stage, function, *args, **kwargs):
        current, previous_peak = tracemalloc.get_traced_memory()
        self.peak = max(self.peak, previous_peak)
        tracemalloc.reset_peak()
        started = time.perf_counter()
        result = function(*args, **kwargs)
        elapsed = time.perf_counter() - started
        _, peak = tracemalloc.get_traced_memory()
        self.peak = max(self.peak, peak)
        item = self.metrics.setdefault(stage, {
            "calls": 0, "seconds": 0.0, "max_call_seconds": 0.0,
            "peak_python_bytes": 0, "max_call_added_peak_bytes": 0,
        })
        item["calls"] += 1
        item["seconds"] += elapsed
        item["max_call_seconds"] = max(item["max_call_seconds"], elapsed)
        item["peak_python_bytes"] = max(item["peak_python_bytes"], peak)
        item["max_call_added_peak_bytes"] = max(item["max_call_added_peak_bytes"], peak - current)
        return result

    def finish(self):
        self.peak = max(self.peak, tracemalloc.get_traced_memory()[1])
        tracemalloc.stop()
        require(self.peak <= HEAP_LIMIT, "Peak traced Python allocations exceed 16 MiB")


def make_source(path, case):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as handle:
        if case == "single_17mib":
            handle.write(b'{"content":"')
            for _ in range(272):
                handle.write(b"x" * READ_BYTES)
            handle.write(b'","timestamp":"2026-09-01T00:15:00+08:00"}\n')
            count = 1
        else:
            count = 1024 if case == "multiple_over_16mib" else 10001 if case == "short_10001" else 65
            content = "x" * (17 * 1024) if case == "multiple_over_16mib" else (
                "smoke 中文🙂 " * 500 if case == "smoke" else "short"
            )
            for index in range(count):
                row = {"content": f"Record {index}: " + content,
                       "timestamp": "2026-09-01T00:15:00+08:00"}
                handle.write((json.dumps(row, ensure_ascii=False) + "\n").encode())
    if case in FULL_CASES[:2]:
        require(path.stat().st_size > 16 * 1024 * 1024, "Large fixture missed the byte boundary")
    return count


def read_complete(store, item, meter, raw_digest):
    offset = total_bytes = pages = 0
    total_chars = None
    digest = hashlib.sha256()
    raw = "byte_start" in item
    while True:
        value = meter.call("read_page", store.read_source, item["source_id"],
                           max_chars=PAGE_CHARS, offset_chars=offset)
        require(value["source_id"] == item["source_id"], "Read returned another source identity")
        require(value["offset_chars"] == offset, "Source paging offset changed")
        if total_chars is None:
            total_chars = value["total_chars"]
        require(total_chars == value["total_chars"], "Source length changed while paging")
        text = value["content"]
        require(len(text) <= PAGE_CHARS, "Source page exceeds its character bound")
        body = text.encode("utf-8")
        digest.update(body)
        if raw:
            raw_digest.update(body)
            require(value["byte_start"] == item["byte_start"]
                    and value["byte_end"] == item["byte_end"], "Fragment range changed while paging")
        total_bytes += len(body)
        pages += 1
        offset += len(text)
        if not value["truncated"]:
            require(value["next_offset"] is None and offset == total_chars,
                    "Final source page did not reach its complete character length")
            break
        require(text and value["next_offset"] == offset and offset < total_chars,
                "Source paging made no contiguous progress")
    require(digest.hexdigest() == item["sha256"], "Paged source bytes failed their declared SHA256")
    require(total_bytes <= (65536 if raw else 16384), "Source exceeds the node input bound")
    if raw:
        require(total_bytes == item["byte_end"] - item["byte_start"], "Paged fragment lost raw bytes")
    return total_bytes, pages


def verify_coverage(directory, plan, reference, observed_path, source, raw_sha, expected_records):
    require(reference["partition_plan"] == plan["batch_id"], "Root references another plan")
    for name in ("files", "records", "nodes"):
        catalog = plan[name]
        require(digest_file(directory / catalog["path"]) == catalog["sha256"],
                "Plan catalog SHA256 mismatch")
        require(sum(1 for _ in json_lines(directory / catalog["path"])) == catalog["count"],
                "Plan catalog row count mismatch")
    files = list(json_lines(directory / plan["files"]["path"]))
    require(len(files) == 1 and files[0]["path"] == SOURCE_PATH
            and files[0]["sha256"] == raw_sha and files[0]["size"] == source.stat().st_size,
            "Frozen file inventory does not match the independent fixture")
    require(digest_file(directory / "files" / SOURCE_PATH) == raw_sha, "Frozen source SHA256 mismatch")
    coverage_path = directory / reference["path"]
    require(digest_file(coverage_path) == reference["sha256"], "Root coverage SHA256 mismatch")
    coverage = iter(json_lines(coverage_path))
    observed = iter(json_lines(observed_path))
    position = fragments = records = 0
    raw_digest, ranges_digest = hashlib.sha256(), hashlib.sha256()
    with source.open("rb") as raw:
        for record in json_lines(directory / plan["records"]["path"]):
            records += 1
            require(record["line"] == records and record["byte_start"] == position,
                    "Record inventory has a gap, overlap or changed physical line")
            parent = source_id("workspace", SOURCE_PATH, raw_sha, records)
            require(record["parent_source_id"] == parent and record["file_sha256"] == raw_sha
                    and record["path"] == SOURCE_PATH and record["parse_error"] is None,
                    "Record inventory has wrong provenance or an unexpected parse gap")
            count = 0
            while position < record["byte_end"]:
                row, read = next(coverage, None), next(observed, None)
                require(row is not None and read is not None, "Coverage omitted an observed fragment")
                require(row["byte_start"] == position and row["parent_source_id"] == parent
                        and row["byte_end"] <= record["byte_end"]
                        and row["status"] == "covered" and row["reason"] is None,
                        "Coverage has a gap, overlap, wrong parent or missing source")
                require(all(row[key] == read[key] for key in (
                    "source_id", "parent_source_id", "byte_start", "byte_end", "node_id"
                )), "Coverage does not match the sources actually read")
                size = row["byte_end"] - position
                require(0 < size <= READ_BYTES, "Coverage byte range exceeds its bound")
                body = raw.read(size)
                digest = hashlib.sha256(body).hexdigest()
                require(len(body) == size and digest == read["sha256"],
                        "Coverage range hash does not match original bytes and actual reads")
                expected_id = source_id("summary-fragment-v1",
                                        f"{parent}/{position}-{row['byte_end']}", digest, 0)
                require(row["source_id"] == expected_id, "Fragment ID does not bind its original range")
                raw_digest.update(body)
                ranges_digest.update((json.dumps(read, sort_keys=True) + "\n").encode())
                position = row["byte_end"]
                fragments += 1
                count += 1
            require(count == record["fragment_count"], "Record fragment count is incomplete")
        require(not raw.read(1), "Record coverage omitted original trailing bytes")
    require(next(coverage, None) is None and next(observed, None) is None,
            "Coverage contains unaccounted fragments")
    require(records == expected_records == reference["records"] == plan["source_count"],
            "Root record count is incomplete")
    require(fragments == reference["fragments"] == plan["fragment_count"]
            and reference["missing_fragments"] == 0 and not reference["has_inherited_gaps"],
            "Root fragment or missing counts disagree")
    require(position == plan["source_bytes"] and raw_digest.hexdigest() == raw_sha,
            "Root coverage does not reconstruct the entire original byte stream")
    return {"records": records, "fragments": fragments, "missing_fragments": 0,
            "raw_bytes": position, "raw_sha256": raw_digest.hexdigest(),
            "coverage_sha256": reference["sha256"], "ranges_sha256": ranges_digest.hexdigest()}


def run_case(root, name):
    store = memory.MemoryStore(root / "data")
    source = store.workspace / SOURCE_PATH
    expected_records = make_source(source, name)
    raw_bytes, raw_sha = source.stat().st_size, digest_file(source)
    gc.collect()
    meter = Meter()
    started = time.perf_counter()
    try:
        batch = meter.call("prepare", store.prepare_summary, "L1", "2026-09-01T00:00", now=NOW)
        require(batch.get("strategy") == "partitioned-v1", "Boundary window was not partitioned")
        directory = Path(batch["manifest_path"]).parent
        plan = read_json(Path(batch["manifest_path"]))
        require(plan["source_count"] == expected_records and plan["source_bytes"] == raw_bytes,
                "Prepared plan did not select the complete synthetic window")
        target = store.workspace / plan["target"]
        observed_path = root / "observed-reads.jsonl"
        raw_read_sha = hashlib.sha256()
        position = commits = reads = pages = intermediate_bytes = 0
        max_node_bytes = max_node_sources = 0
        root_candidate = None
        with observed_path.open("w", encoding="utf-8") as observed:
            for _ in range(plan["nodes"]["count"] + 2):
                page = meter.call("next", store.summary_partition_next, batch["batch_id"], limit=4)
                if page["complete"]:
                    require(not page["ready"] and page["completed_nodes"] == commits
                            and page["total_nodes"] == commits, "Completed plan still has pending work")
                    break
                require(0 < len(page["ready"]) <= 4, "Incomplete DAG has no bounded ready work")
                for node in page["ready"]:
                    manifest = read_json(Path(node["manifest_path"]))
                    inputs = manifest["sources"]
                    require(0 < len(inputs) <= 64, "Node source count exceeds its bound")
                    node_bytes = 0
                    covered = []
                    for item in inputs:
                        require(item["parse_error"] is None, "Synthetic source unexpectedly has a parse gap")
                        count, read_pages = read_complete(store, item, meter, raw_read_sha)
                        reads += 1
                        pages += read_pages
                        node_bytes += count
                        covered.append(item["source_id"])
                        if "byte_start" in item:
                            require(item["byte_start"] == position and item["path"] == SOURCE_PATH
                                    and item["file_sha256"] == raw_sha,
                                    "Actually read raw sources are not the contiguous original version")
                            position = item["byte_end"]
                            write_row(observed, {**{key: item[key] for key in (
                                "source_id", "parent_source_id", "byte_start", "byte_end", "sha256"
                            )}, "node_id": node["node_id"]})
                        else:
                            intermediate_bytes += count
                    require(node_bytes <= 128 * 1024, "Node input bytes exceed 128 KiB")
                    max_node_bytes = max(max_node_bytes, node_bytes)
                    max_node_sources = max(max_node_sources, len(inputs))
                    candidate = {"content": f"Synthetic structural summary. [source:{covered[0]}]",
                                 "source_ids": covered[:1], "covered_source_ids": covered, "missing": []}
                    is_root = node["node_id"] == plan["root_node_id"]
                    require(not target.exists(), "A non-root task published the final summary")
                    outcome = meter.call("commit", store.commit_summary_partition,
                                         batch["batch_id"], node["node_id"], candidate)
                    commits += 1
                    require(not outcome["already_committed"] and outcome["complete"] == is_root,
                            "Unexpected commit duplication or non-root finalization")
                    if is_root:
                        root_candidate = candidate
            else:
                raise ProbeFailure("DAG did not complete within its declared node count")
        require(root_candidate is not None and commits == plan["nodes"]["count"],
                "DAG has no fully committed root")
        require(position == raw_bytes and raw_read_sha.hexdigest() == raw_sha,
                "Actual paged reads did not reconstruct the complete original byte stream")
        require(reads == plan["fragment_count"] + commits - 1, "Some tree inputs were never read")
        rows = list(json_lines(target))
        require(len(rows) == 1 and rows[0]["batch_id"] == batch["batch_id"]
                and rows[0]["source_manifest"] == batch["batch_id"],
                "L1 target does not contain exactly one root batch marker")
        coverage = meter.call("verify_coverage", verify_coverage, directory, plan,
                              rows[0]["coverage_ref"], observed_path, source, raw_sha, expected_records)
        before_target, before_plan = digest_file(target), tree_digest(directory)
        resumed = meter.call("restart", memory.MemoryStore, store.data_dir)
        final = meter.call("restart_next", resumed.summary_partition_next, batch["batch_id"])
        require(final["complete"] and not final["ready"], "Restart reopened a completed DAG")
        retry = meter.call("retry_root", resumed.commit_summary_partition,
                           batch["batch_id"], plan["root_node_id"], root_candidate)
        require(retry["complete"] and retry["already_committed"], "Root retry was not idempotent")
        require(digest_file(target) == before_target and tree_digest(directory) == before_plan
                and digest_file(source) == raw_sha, "Restart/retry changed preserved source or plan/target bytes")
        intent = read_json(store.state / "commits" / (batch["batch_id"] + ".json"))
        require(intent["schema_version"] == 2 and intent["status"] == "committed",
                "Restart did not retain a committed schema-2 intent")
        meter.finish()
        return {"case": name, "debug_only": name == "smoke", "status": "passed",
                "raw_bytes": raw_bytes, "raw_sha256": raw_sha,
                "frozen_disk": disk_bytes(directory / "files"), "total_disk": disk_bytes(store.data_dir),
                "leaves": plan["leaf_count"], "nodes": commits,
                "input_sources_read": reads, "source_pages_read": pages,
                "intermediate_bytes_read": intermediate_bytes,
                "max_node_input_bytes": max_node_bytes, "max_node_sources": max_node_sources,
                "root_batch_markers": 1, "root_target_sha256": before_target,
                "restart_retry_preserved_plan_and_target": True, "coverage": coverage,
                "seconds": time.perf_counter() - started, "peak_python_bytes": meter.peak,
                "metrics": meter.metrics}
    finally:
        if tracemalloc.is_tracing():
            tracemalloc.stop()


class GuardRun:
    """Count work actually performed by a synthetic regression guard."""

    def __init__(self):
        self.committed_nodes = 0
        self.source_pages_read = 0
        self.expected_rejections = 0

    def call(self, stage, function, *args, **kwargs):
        result = function(*args, **kwargs)
        if stage == "read_page":
            self.source_pages_read += 1
        return result

    def reject(self, function, *args, **kwargs):
        try:
            function(*args, **kwargs)
        except (memory.MemoryConflictError, memory.SummaryValidationError):
            self.expected_rejections += 1
        else:
            raise ProbeFailure("A damaged or unproven summary was accepted")


def guard_window(root, *, broken=False):
    store = memory.MemoryStore(root / "data")
    source = store.workspace / SOURCE_PATH
    source.parent.mkdir(parents=True)
    with source.open("wb") as handle:
        if broken:
            handle.write(b"BROKEN\n")
        for index in range(65):
            handle.write((json.dumps({"timestamp": "2026-09-01T00:15:00+08:00",
                                      "content": f"Synthetic guard {index}"}) + "\n").encode())
    batch = store.prepare_summary("L1", "2026-09-01T00:00", now=NOW)
    require(batch.get("strategy") == "partitioned-v1", "Guard did not create a partition plan")
    return store, source, batch


def guard_candidate(store, node, run):
    sources = read_json(Path(node["manifest_path"]))["sources"]
    for item in sources:
        read_complete(store, item, run, hashlib.sha256())
    good = [item["source_id"] for item in sources if not item["parse_error"]]
    missing = [{"source_id": item["source_id"], "reason": "Synthetic malformed original"}
               for item in sources if item["parse_error"]]
    return {"content": "Synthetic guard summary " + " ".join(f"[source:{sid}]" for sid in good[:1]),
            "source_ids": good[:1], "covered_source_ids": good, "missing": missing}


def guard_drain(store, batch, run, *, stop_before_root=False):
    plan = read_json(Path(batch["manifest_path"]))
    for _ in range(plan["nodes"]["count"] + 2):
        page = store.summary_partition_next(batch["batch_id"])
        if page["complete"]:
            require(not stop_before_root and not page["ready"], "Guard missed the root boundary")
            return None
        require(page["ready"], "Guard DAG has no ready work")
        for node in page["ready"]:
            if stop_before_root and node["node_id"] == plan["root_node_id"]:
                return node
            result = store.commit_summary_partition(batch["batch_id"], node["node_id"],
                                                    guard_candidate(store, node, run))
            require(not result["already_committed"], "Guard silently reused an earlier node commit")
            run.committed_nodes += 1
    raise ProbeFailure("Guard DAG did not complete within its declared node count")


def guard_hourly(root, run):
    store, source, batch = guard_window(root, broken=True)
    guard_drain(store, batch, run)
    target = store.workspace / "memory/chronicle/hourly/2026-09-01.jsonl"
    require(read_json(target)["coverage_ref"]["missing_fragments"] == 1,
            "Guard's malformed source did not produce exactly one explicit gap")
    return store, source, batch, target


def guard_diary(root, run):
    store, _, ancestor, _ = guard_hourly(root, run)
    daily = store.prepare_summary("L2", "2026-09-01", now=NOW)
    require(daily.get("strategy") == "partitioned-v1", "L2 guard did not retain its upstream plan")
    guard_drain(store, daily, run)
    target = store.workspace / "memory/chronicle/diary/2026-09-01.md"
    require(f"<!-- anima-coverage:{daily['batch_id']}:" in target.read_text(),
            "L2 guard has no embedded coverage marker")
    return store, ancestor, daily, target


def guard_committed_gap_tampering(root, run):
    store, source, batch, target = guard_hourly(root, run)
    intent_path = store.state / "commits" / (batch["batch_id"] + ".json")
    intent = read_json(intent_path)
    require(intent["status"] == "committed"
            and intent["manifest"]["coverage_ref"]["missing_fragments"] == 1,
            "Committed guard precondition did not contain its gap")
    source_before, target_before = digest_file(source), digest_file(target)
    intent["manifest"]["coverage_ref"]["missing_fragments"] = 0
    intent_path.write_text(json.dumps(intent))
    damaged_intent = digest_file(intent_path)
    run.reject(memory.MemoryStore, store.data_dir)
    require(digest_file(source) == source_before and digest_file(target) == target_before
            and digest_file(intent_path) == damaged_intent,
            "Rejected committed tampering rewrote original, target or damaged intent")


def guard_pending_rendering_tampering(root, run):
    store, source, batch = guard_window(root)
    node = guard_drain(store, batch, run, stop_before_root=True)
    plan = read_json(Path(batch["manifest_path"]))
    target = store.workspace / plan["target"]
    candidate = guard_candidate(store, node, run)
    original_atomic = memory._atomic
    interruptions = 0

    class SyntheticInterruption(Exception):
        pass

    def stop_before_target(path, raw):
        nonlocal interruptions
        if Path(path) == target:
            interruptions += 1
            raise SyntheticInterruption()
        return original_atomic(path, raw)

    try:
        memory._atomic = stop_before_target
        try:
            store.commit_summary_partition(batch["batch_id"], node["node_id"], candidate)
        except SyntheticInterruption:
            pass
        else:
            raise ProbeFailure("The pending-write guard never reached its interruption point")
    finally:
        memory._atomic = original_atomic
    require(interruptions == 1 and not target.exists(), "Pending guard did not stop before target write")
    intent_path = store.state / "commits" / (batch["batch_id"] + ".json")
    intent = read_json(intent_path)
    require(intent["status"] == "pending", "Interruption left no durable pending intent")
    source_before = digest_file(source)
    intent["rendered"] = "Synthetic forged text that the root candidate never committed.\n"
    intent["after_sha256"] = hashlib.sha256(intent["rendered"].encode()).hexdigest()
    intent_path.write_text(json.dumps(intent))
    damaged_intent = digest_file(intent_path)
    run.reject(memory.MemoryStore, store.data_dir)
    require(not target.exists() and digest_file(source) == source_before
            and digest_file(intent_path) == damaged_intent,
            "Rejected forged rendering published a target or changed original/intent bytes")


def guard_import_rejected(root, raw, relative, level, period, target_relative, run):
    imported = memory.MemoryStore(root / "data")
    copied = imported.workspace / relative
    copied.parent.mkdir(parents=True, exist_ok=True)
    copied.write_bytes(raw)
    original = digest_file(copied)
    require(not list((imported.state / "commits").glob("*.json")), "Import guard unexpectedly has local proof")
    run.reject(imported.prepare_summary, level, period, now=NOW)
    require(digest_file(copied) == original
            and not (imported.workspace / target_relative).exists()
            and not list((imported.state / "commits").glob("*.json")),
            "Rejected import changed the source or published a summary/commit")


def guard_json_import(root, run, *, giant=False):
    _, _, _, hourly = guard_hourly(root / "producer", run)
    value = read_json(hourly)
    if giant:
        value["content"] = "Synthetic giant copied summary. " + "x" * (1024 * 1024 + 1)
        reference = value.pop("coverage_ref")
        value["coverage_ref"] = reference
    raw = (json.dumps(value, ensure_ascii=False) + "\n").encode()
    guard_import_rejected(root / "imported", raw, "memory/chronicle/hourly/2026-09-01.jsonl",
                          "L2", "2026-09-01", "memory/chronicle/diary/2026-09-01.md", run)


def guard_markdown_import(root, run, *, level):
    producer, _, _, source = guard_diary(root / "producer", run)
    if level == "L3":
        relative, period, target = ("memory/chronicle/diary/2026-09-01.md", "2026-W36",
                                    "memory/chronicle/weekly/2026-W36.md")
    else:
        weekly = producer.prepare_summary("L3", "2026-W36", now=NOW)
        require(weekly.get("strategy") == "partitioned-v1", "L3 guard did not retain its upstream plan")
        guard_drain(producer, weekly, run)
        source = producer.workspace / "memory/chronicle/weekly/2026-W36.md"
        require(f"<!-- anima-coverage:{weekly['batch_id']}:" in source.read_text(),
                "L3 guard has no embedded coverage marker")
        relative, period, target = ("memory/chronicle/weekly/2026-W36.md", "2026-09",
                                    "memory/chronicle/monthly/2026-09.md")
    guard_import_rejected(root / "imported", source.read_bytes(), relative, level, period, target, run)


def guard_malformed_marker(root, run):
    markers = (
        "<!-- anima-coverage:" + "a" * 63 + "z:" + "b" * 64 + " -->",
        "<!-- anima-coverage:" + "a" * 64 + ":truncated",
    )
    for index, marker in enumerate(markers):
        raw = ("Synthetic imported note.\n" + marker + "\n").encode()
        guard_import_rejected(root / str(index), raw, "memory/chronicle/diary/2026-09-01.md",
                              "L3", "2026-W36", "memory/chronicle/weekly/2026-W36.md", run)


def guard_local_copy_keeps_ancestor(root, run):
    store, ancestor, daily, original = guard_diary(root, run)
    raw = original.read_bytes()
    copied = original.parent / "2026-09-08.md"
    copied.write_bytes(raw)
    batch = store.prepare_summary("L3", "2026-W37", now=NOW)
    require(batch.get("strategy") == "partitioned-v1" and batch["source_count"] == 1,
            "Local-copy guard did not select only the new-date copy")
    guard_drain(store, batch, run)
    intent = read_json(store.state / "commits" / (batch["batch_id"] + ".json"))
    reference = intent["manifest"]["coverage_ref"]
    require(reference["has_inherited_gaps"], "New-date copy silently lost an ancestor gap")
    upstream = reference["upstream_ref"]
    upstream_path = Path(batch["manifest_path"]).parent / upstream["path"]
    require(digest_file(upstream_path) == upstream["sha256"], "Local-copy upstream catalog hash changed")
    rows = list(json_lines(upstream_path))
    daily_rows = [row for row in rows if row["batch_id"] == daily["batch_id"]]
    require(len(daily_rows) == 1 and daily_rows[0]["coverage_ref"]["has_inherited_gaps"],
            "New-date copy references no unique original daily proof with inherited gaps")
    daily_upstream = daily_rows[0]["coverage_ref"]["upstream_ref"]
    daily_path = Path(daily["manifest_path"]).parent / daily_upstream["path"]
    require(digest_file(daily_path) == daily_upstream["sha256"], "Daily ancestor catalog hash changed")
    ancestor_rows = [row for row in json_lines(daily_path) if row["batch_id"] == ancestor["batch_id"]]
    require(len(ancestor_rows) == 1 and ancestor_rows[0]["coverage_ref"]["missing_fragments"] == 1,
            "Local-copy lineage does not reach the actual malformed original fragment")
    require(original.read_bytes() == copied.read_bytes() == raw, "Valid local-copy summary bytes changed")


def regression_guards(root, report):
    checks = (
        ("committed_missing_count_tampering_rejected", guard_committed_gap_tampering),
        ("pending_rendered_and_hash_forgery_rejected", guard_pending_rendering_tampering),
        ("copied_l1_jsonl_without_local_proof_rejected", guard_json_import),
        ("giant_copied_l1_tail_proof_without_local_proof_rejected",
         lambda path, run: guard_json_import(path, run, giant=True)),
        ("copied_l2_markdown_without_local_proof_rejected_by_l3",
         lambda path, run: guard_markdown_import(path, run, level="L3")),
        ("copied_l3_markdown_without_local_proof_rejected_by_l4",
         lambda path, run: guard_markdown_import(path, run, level="L4")),
        ("malformed_reserved_markdown_coverage_marker_rejected", guard_malformed_marker),
        ("valid_local_markdown_new_date_retains_ancestor_gap", guard_local_copy_keeps_ancestor),
    )
    for name, function in checks:
        result = {"name": name, "passed": False}
        report["guards"].append(result)
        run = GuardRun()
        started = time.perf_counter()
        function(root / name, run)
        result.update(passed=True, seconds=time.perf_counter() - started,
                      committed_nodes=run.committed_nodes, source_pages_read=run.source_pages_read,
                      expected_rejections=run.expected_rejections)
        report["regression_guard_count"] += 1
    require(report["regression_guard_count"] == len(checks), "Some regression guards did not execute")
    report["regression_guards_passed"] = True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", action="append", choices=(*FULL_CASES, "smoke"), dest="cases")
    args = parser.parse_args()
    cases = args.cases or FULL_CASES
    report = {"schema_version": 1, "host_structural_only": True, "status": "failed",
              "python": platform.python_version(), "platform": platform.system(),
              "isolated_python": bool(sys.flags.isolated), "page_chars": PAGE_CHARS,
              "heap_limit_bytes": HEAP_LIMIT,
              "memory_scope": "Peak traced Python allocations after fixture/store setup; includes the probe and schema-2 restart validation, excludes RSS and native allocations.",
              "model_or_native_execution": False, "full_boundary_suite": tuple(cases) == FULL_CASES,
              "modules": module_evidence(), "cases": [], "guards": [],
              "regression_guard_count": 0, "regression_guards_passed": False}
    current = "configuration"
    try:
        require(sys.flags.isolated == 1, "Use the selected runtime's Python with -I")
        require(len(cases) == len(set(cases)), "A case may only be selected once")
        with tempfile.TemporaryDirectory(prefix="alice-summary-artifact-probe-") as scratch:
            for current in cases:
                report["cases"].append(run_case(Path(scratch) / current, current))
            current = "regression_guards"
            regression_guards(Path(scratch) / "guards", report)
        require(module_evidence() == report["modules"], "Runtime module files changed during the probe")
        report["status"] = "passed"
    except Exception as exc:
        # Arbitrary runtime exception messages can contain host paths. Preserve
        # only our path-free check messages; a failure never becomes success.
        report["error"] = {"case": current, "type": type(exc).__name__,
                           "check": str(exc) if isinstance(exc, ProbeFailure) else "Unexpected runtime failure"}
    print(json.dumps(report, sort_keys=True))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
