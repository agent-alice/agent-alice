"""Bounded Chronicle work plans. Codex executes workers; this module only stores evidence.

Plan catalogs are streamed, node manifests are bounded, and raw fragments remain
byte ranges of frozen files. No model, conversation history, or model loop lives here.
"""

import datetime as dt
from importlib import resources
import json
import os
from pathlib import Path
import re
import shutil
import stat
from string import Template
import tempfile
from typing import Mapping

from .memory import (
    MemoryConflictError,
    NoSourcesError,
    SourceChangedError,
    SummaryValidationError,
    _atomic,
    _digest,
    _fingerprint,
    _fsync_dir,
    _hash_file,
    _in_period,
    _join,
    _json,
    _mkdir,
    _period_bounds,
    _render_summary,
    _source_id,
    _stable_copy,
    _summary_target,
    _valid_record_time,
    _validate_candidate,
)
from .summary_stream import FragmentSpan, fragment_ranges, scan_records

SUMMARY_COMMIT_SCHEMA = 2
FORMAT = "summary-partition-v1"
FRAGMENT_BYTES = 65536
NODE_INPUT_BYTES = 128 * 1024
NODE_SOURCE_COUNT = 64
NODE_OUTPUT_BYTES = 16 * 1024
FAN_IN = 8
_HEX = re.compile(r"[a-f0-9]{64}\Z")
_NODE = re.compile(r"n([0-9]{4})-([0-9]{12})\Z")


def validate_summary_commit_header(
    value: Mapping, *, supported_schema: int = SUMMARY_COMMIT_SCHEMA
) -> int:
    """Pure, non-mutating compatibility check for release selection and recovery.

    This validates the protocol header, not referenced files or DAG completion.
    Missing capability in an old candidate means supported_schema=1.
    """
    if not isinstance(value, Mapping) or type(supported_schema) is not int:
        raise SummaryValidationError("Invalid summary commit header")
    version = value.get("schema_version")
    if type(version) is not int or version not in (1, 2) or version > supported_schema:
        raise SummaryValidationError("Unsupported summary commit schema version")
    if not isinstance(value.get("batch_id"), str) or not _HEX.fullmatch(value["batch_id"]):
        raise SummaryValidationError("Invalid summary commit batch identifier")
    manifest = value.get("manifest")
    if not isinstance(manifest, Mapping) or manifest.get("schema_version") != version:
        raise SummaryValidationError("Unsupported summary batch schema version in commit")
    statuses = (
        {"pending", "committed"} if version == 1 else {"partitioning", "pending", "committed"}
    )
    if value.get("status") not in statuses:
        raise SummaryValidationError("Summary commit has an invalid status")
    if version == 2 and (
        value.get("format") != FORMAT
        or value.get("partition_plan_sha256") != value["batch_id"]
        or manifest.get("partition_plan") != value["batch_id"]
    ):
        raise SummaryValidationError("Invalid partition commit protocol header")
    return version


def _load(path: Path) -> dict:
    try:
        value = json.loads(path.read_text())
    except (ValueError, OSError) as exc:
        raise SummaryValidationError("Missing or damaged partition evidence") from exc
    if not isinstance(value, dict):
        raise SummaryValidationError("Invalid partition evidence")
    return value


def _catalog(directory: Path, reference: dict):
    path = _join(directory, reference["path"])
    if _hash_file(path) != reference["sha256"]:
        raise SourceChangedError("Partition catalog integrity check failed")
    count = 0
    with path.open() as stream:
        for line in stream:
            if len(line.encode()) > 256 * 1024:
                raise SummaryValidationError("Partition catalog row exceeds its bound")
            count += 1
            yield json.loads(line)
    if count != reference["count"]:
        raise SummaryValidationError("Partition catalog count mismatch")


def _row(stream, value):
    stream.write(json.dumps(value, ensure_ascii=False, sort_keys=True) + "\n")


def _node_id(tier: int, index: int) -> str:
    value = f"n{tier:04d}-{index:012d}"
    if not _NODE.fullmatch(value):
        raise SummaryValidationError("Partition node identifier exceeds its bound")
    return value


def _fragment_id(source: dict) -> str:
    if (
        _source_id("workspace", source["path"], source["file_sha256"], source["line"])
        != source["parent_source_id"]
    ):
        raise SourceChangedError("Fragment parent provenance integrity check failed")
    identifier = _source_id(
        "summary-fragment-v1",
        f"{source['parent_source_id']}/{source['byte_start']}-{source['byte_end']}",
        source["sha256"],
        0,
    )
    if "source_id" in source and source["source_id"] != identifier:
        raise SourceChangedError("Fragment source identity changed")
    return identifier


def _inventory(memory, level):
    lower = {"L1": "traces", "L2": "hourly", "L3": "diary", "L4": "weekly"}[level]
    root = _join(memory.workspace, "memory/chronicle/" + lower)
    result = {}
    for path in sorted(root.rglob("*")):
        mode = path.lstat().st_mode
        if stat.S_ISDIR(mode):
            continue
        if not stat.S_ISREG(mode):
            raise SummaryValidationError("Summary source must be a regular file without symlinks")
        relative = path.relative_to(memory.workspace).as_posix()
        result[relative] = _fingerprint(_join(memory.workspace, relative))
    return result


def _dated_path(path, level, timezone):
    labels = (path.stem, path.parent.name) if level in {"L1", "L2"} else (path.stem,)
    for label in labels:
        try:
            if level == "L4":
                _period_bounds("L3", label, timezone)
                return True
            if dt.date.fromisoformat(label).isoformat() == label:
                return True
        except (ValueError, SummaryValidationError):
            pass
    return False


def has_upstream(memory, targets):
    """New coverage references require the partition-aware reader at later levels."""
    for path in _join(memory.state, "commits").glob("*.json"):
        intent = _load(path)
        if (
            intent.get("schema_version") == 2
            and intent.get("status") == "committed"
            and intent.get("target") in targets
        ):
            return True
    return False


def _upstream(memory, targets):
    for path in sorted(_join(memory.state, "commits").glob("*.json")):
        intent = _load(path)
        if (
            intent.get("schema_version") != 2
            or intent.get("status") != "committed"
            or intent.get("target") not in targets
        ):
            continue
        validate_summary_commit_header(intent)
        reference = intent["manifest"]["coverage_ref"]
        directory, _ = _plan(memory, intent["batch_id"])
        if _hash_file(_join(directory, reference["path"])) != reference["sha256"]:
            raise SourceChangedError("Upstream summary coverage integrity check failed")
        yield {
            "batch_id": intent["batch_id"],
            "source_target": intent["target"],
            "coverage_ref": reference,
        }


def prepare(memory, level, period, *, now=None, timezone="Asia/Shanghai"):
    start, end = _period_bounds(level, period, timezone)
    current = now or dt.datetime.now(dt.timezone.utc)
    if current.tzinfo is None or end > current:
        raise SummaryValidationError("Summary period has not closed or now lacks a timezone")
    root = _join(memory.state, "summary-partitions")
    _mkdir(root)
    staging = Path(tempfile.mkdtemp(prefix=".building-", dir=root))
    try:
        initial = _inventory(memory, level)
        counters = {"files": 0, "records": 0, "nodes": 0}
        fragments = total_bytes = leaf_count = 0
        pending, pending_bytes = [], 0
        latest_hash = None
        selected_targets = set()
        with (
            (staging / "files.jsonl").open("w") as files,
            (staging / "records.jsonl").open("w") as records,
            (staging / "nodes.jsonl").open("w") as nodes,
        ):

            def write_node(node):
                nonlocal latest_hash
                relative = f"nodes/{node['node_id']}.json"
                raw = _json(node)
                if len(raw) > 128 * 1024:
                    raise SummaryValidationError("Node metadata exceeds its bound")
                _atomic(_join(staging, relative), raw)
                _row(nodes, {"node_id": node["node_id"], "path": relative, "sha256": _digest(raw)})
                counters["nodes"] += 1
                latest_hash = _digest(raw)
                return latest_hash

            def flush_leaf():
                nonlocal pending, pending_bytes, leaf_count
                if pending:
                    identifier = _node_id(0, leaf_count)
                    digest = write_node(
                        {
                            "node_id": identifier,
                            "children": [],
                            "child_versions": {},
                            "sources": pending,
                        }
                    )
                    with (staging / "tier-0000.jsonl").open("a") as tier_refs:
                        _row(tier_refs, {"node_id": identifier, "sha256": digest})
                    leaf_count += 1
                    pending, pending_bytes = [], 0

            for relative, before in initial.items():
                path = _join(memory.workspace, relative)
                digest = _hash_file(path)
                selected_file = False
                for record in scan_records(path):
                    valid_time = (
                        _valid_record_time(level, record.metadata)
                        if level in {"L1", "L2"}
                        else True
                    )
                    if not valid_time and not _dated_path(path, level, timezone):
                        raise SummaryValidationError(
                            "Summary source has no valid timestamp or recognizable file date"
                        )
                    if not _in_period(level, path, record.metadata, start, end):
                        continue
                    error = record.parse_error or (
                        None if valid_time else "missing_or_invalid_timestamp"
                    )
                    parent = _source_id("workspace", relative, digest, record.line)
                    record_fragments = 0
                    spans = fragment_ranges(
                        path, record.byte_start, record.byte_end, FRAGMENT_BYTES
                    )
                    if record.byte_start == record.byte_end:
                        spans = [
                            FragmentSpan(
                                record.byte_start, record.byte_end, _digest(b""), "empty_source"
                            )
                        ]
                    for fragment in spans:
                        size = fragment.byte_end - fragment.byte_start
                        source = {
                            "namespace": "summary-fragment-v1",
                            "parent_source_id": parent,
                            "path": relative,
                            "line": record.line,
                            "file_sha256": digest,
                            "byte_start": fragment.byte_start,
                            "byte_end": fragment.byte_end,
                            "sha256": fragment.sha256,
                            "parse_error": error or fragment.parse_error,
                            "timestamp": record.metadata.get("timestamp")
                            or record.metadata.get("time_start"),
                        }
                        source["source_id"] = _fragment_id(source)
                        # Also bound metadata; long paths cannot make a tiny-input node huge.
                        if pending and (
                            len(pending) >= NODE_SOURCE_COUNT
                            or pending_bytes + size > NODE_INPUT_BYTES
                            or len(_json(pending + [source])) > 120 * 1024
                        ):
                            flush_leaf()
                        pending.append(source)
                        pending_bytes += size
                        fragments += 1
                        record_fragments += 1
                        total_bytes += size
                    _row(
                        records,
                        {
                            "parent_source_id": parent,
                            "path": relative,
                            "line": record.line,
                            "file_sha256": digest,
                            "byte_start": record.byte_start,
                            "byte_end": record.byte_end,
                            "fragment_count": record_fragments,
                            "parse_error": error,
                        },
                    )
                    counters["records"] += 1
                    selected_file = True
                if before != _fingerprint(path):
                    raise SourceChangedError("Summary input changed while selecting records")
                if selected_file:
                    selected_targets.add(relative)
                    info = _stable_copy(path, _join(staging / "files", relative))
                    if info["sha256"] != digest or before != _fingerprint(path):
                        raise SourceChangedError("Summary input changed before freezing")
                    _row(files, {"path": relative, "sha256": digest, "size": before[2]})
                    counters["files"] += 1
            flush_leaf()
            if not fragments:
                raise NoSourcesError("No source records cover this period")
            tier, width = 0, leaf_count
            while width > 1:
                with (
                    (staging / f"tier-{tier:04d}.jsonl").open() as previous,
                    (staging / f"tier-{tier + 1:04d}.jsonl").open("w") as following,
                ):
                    for index in range((width + FAN_IN - 1) // FAN_IN):
                        children = [
                            json.loads(previous.readline())
                            for _ in range(min(FAN_IN, width - index * FAN_IN))
                        ]
                        identifier = _node_id(tier + 1, index)
                        digest = write_node(
                            {
                                "node_id": identifier,
                                "sources": [],
                                "children": [child["node_id"] for child in children],
                                "child_versions": {
                                    child["node_id"]: child["sha256"] for child in children
                                },
                            }
                        )
                        _row(following, {"node_id": identifier, "sha256": digest})
                width = (width + FAN_IN - 1) // FAN_IN
                tier += 1
            for stream in (files, records, nodes):
                stream.flush()
                os.fsync(stream.fileno())
        upstream_count, inherited_gaps = 0, False
        with (staging / "upstream.jsonl").open("w") as upstream:
            for entry in _upstream(memory, selected_targets):
                _row(upstream, entry)
                upstream_count += 1
                coverage = entry["coverage_ref"]
                inherited_gaps |= bool(
                    coverage["missing_fragments"] or coverage.get("has_inherited_gaps")
                )
            upstream.flush()
            os.fsync(upstream.fileno())
        manifest = {
            "schema_version": 1,
            "format": FORMAT,
            "level": level,
            "period": period,
            "timezone": timezone,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "target": _summary_target(level, period),
            "prompt_version": "chronicle-partitions-v1",
            "limits": {
                "fragment_bytes": FRAGMENT_BYTES,
                "input_bytes": NODE_INPUT_BYTES,
                "source_count": NODE_SOURCE_COUNT,
                "output_bytes": NODE_OUTPUT_BYTES,
                "fan_in": FAN_IN,
            },
            "source_count": counters["records"],
            "fragment_count": fragments,
            "source_bytes": total_bytes,
            "leaf_count": leaf_count,
            "root_node_id": _node_id(tier, 0),
            "root_node_sha256": latest_hash,
            "has_inherited_gaps": inherited_gaps,
            "upstream": {
                "path": "upstream.jsonl",
                "sha256": _hash_file(staging / "upstream.jsonl"),
                "count": upstream_count,
            },
        }
        for name, count in counters.items():
            manifest[name] = {
                "path": f"{name}.jsonl",
                "sha256": _hash_file(staging / f"{name}.jsonl"),
                "count": count,
            }
        batch_id = _digest(_json(manifest))
        manifest["batch_id"] = batch_id
        _atomic(staging / "manifest.json", _json(manifest))
        with memory._lock():
            memory._recover_commits()
            if initial != _inventory(memory, level):
                raise SourceChangedError("Summary source inventory changed while freezing")
            directory = _join(root, batch_id)
            prepared = _join(root, ".prepared-" + batch_id)
            intent_path = _join(memory.state, f"commits/{batch_id}.json")
            if directory.exists():
                if _load(directory / "manifest.json") != manifest:
                    raise SourceChangedError("Existing partition plan has changed")
            else:
                if prepared.exists():
                    if _load(prepared / "manifest.json") != manifest:
                        raise SourceChangedError("Prepared partition plan has changed")
                else:
                    os.replace(staging, prepared)
                    _fsync_dir(root)
                if not intent_path.exists():
                    _atomic(
                        intent_path,
                        _json(
                            {
                                "schema_version": 2,
                                "format": FORMAT,
                                "batch_id": batch_id,
                                "partition_plan_sha256": batch_id,
                                "status": "partitioning",
                                "target": manifest["target"],
                                "manifest": {"schema_version": 2, "partition_plan": batch_id},
                            }
                        ),
                    )
                recover_plan(memory, _load(intent_path))
        return _prepared_result(memory, directory, manifest)
    except FileNotFoundError as exc:
        raise SourceChangedError("A summary source disappeared while freezing") from exc
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def _plan(memory, batch_id):
    if not isinstance(batch_id, str) or not _HEX.fullmatch(batch_id):
        raise SummaryValidationError("Invalid batch identifier")
    directory = _join(memory.state, "summary-partitions/" + batch_id)
    manifest = _load(_join(directory, "manifest.json"))
    check = dict(manifest)
    if (
        check.pop("batch_id", None) != batch_id
        or _digest(_json(check)) != batch_id
        or manifest.get("format") != FORMAT
        or manifest.get("schema_version") != 1
    ):
        raise SummaryValidationError("Partition plan integrity check failed")
    return directory, manifest


def recover_plan(memory, intent):
    validate_summary_commit_header(intent)
    batch_id = intent["batch_id"]
    root = _join(memory.state, "summary-partitions")
    destination, prepared = _join(root, batch_id), _join(root, ".prepared-" + batch_id)
    if not destination.exists():
        manifest = _load(_join(prepared, "manifest.json"))
        check = dict(manifest)
        if check.pop("batch_id", None) != batch_id or _digest(_json(check)) != batch_id:
            raise MemoryConflictError(
                "Prepared partition evidence is damaged; preserved for recovery"
            )
        os.replace(prepared, destination)
        _fsync_dir(root)
    directory, plan = _plan(memory, batch_id)
    if intent.get("target") != plan["target"]:
        raise MemoryConflictError("Partition commit target does not match its frozen plan")
    if intent["status"] in {"pending", "committed"}:
        manifest = intent["manifest"]
        if any(
            manifest.get(key) != plan[key]
            for key in ("batch_id", "level", "start", "end", "target")
        ):
            raise MemoryConflictError("Partition commit manifest does not match its frozen plan")
        reference = manifest.get("coverage_ref", {})
        if (
            reference.get("partition_plan") != batch_id
            or reference.get("path") != "coverage.jsonl"
            or reference.get("upstream_ref") != plan["upstream"]
            or reference.get("has_inherited_gaps") != plan["has_inherited_gaps"]
        ):
            raise MemoryConflictError("Partition commit coverage does not match its frozen plan")
        if _hash_file(_join(directory, reference["path"])) != reference.get("sha256"):
            raise MemoryConflictError("Partition commit coverage proof has changed")
        receipt = _receipt(directory, plan, plan["root_node_id"])
        if (
            intent.get("candidate_sha256") != receipt["candidate_sha256"]
            or intent.get("candidate") != receipt["candidate"]
        ):
            raise MemoryConflictError(
                "Partition commit candidate does not match its validated root"
            )


def _template(name, **values):
    raw = resources.files("alice_codex").joinpath("templates/" + name).read_text()
    return Template(raw).substitute(**values)


def _prepared_result(memory, directory, plan):
    candidate = _join(memory.workspace, f".alice/candidates/{plan['batch_id']}.json")
    _mkdir(candidate.parent)
    return {
        "strategy": "partitioned-v1",
        "batch_id": plan["batch_id"],
        "manifest_path": str(directory / "manifest.json"),
        "candidate_path": str(candidate),
        "source_count": plan["source_count"],
        "workspace_path": str(memory.workspace),
        "prompt": _template(
            "chronicle-partition-coordinator.md",
            batch_id=plan["batch_id"],
            manifest_path=directory / "manifest.json",
        ),
    }


def _receipt_path(directory, node_id):
    if not isinstance(node_id, str) or not _NODE.fullmatch(node_id):
        raise SummaryValidationError("Invalid partition node identifier")
    return _join(directory, f"receipts/{node_id}.json")


def _node(directory, plan, node_id):
    _receipt_path(directory, node_id)
    wanted_tier, wanted_index = map(int, _NODE.fullmatch(node_id).groups())
    tier = int(_NODE.fullmatch(plan["root_node_id"]).group(1))
    if wanted_tier > tier or wanted_index // FAN_IN ** (tier - wanted_tier) != 0:
        raise SummaryValidationError("Unknown partition node")
    current, digest = plan["root_node_id"], plan["root_node_sha256"]
    while True:
        path = _join(directory, f"nodes/{current}.json")
        if _hash_file(path) != digest:
            raise SourceChangedError("Partition node integrity check failed")
        node = _load(path)
        if node["node_id"] != current:
            raise SourceChangedError("Partition node identity changed")
        if current == node_id:
            return node
        tier -= 1
        child = _node_id(tier, wanted_index // FAN_IN ** (tier - wanted_tier))
        if child not in node["children"] or child not in node["child_versions"]:
            raise SummaryValidationError("Unknown partition node")
        current, digest = child, node["child_versions"][child]


def _receipt(directory, plan, node_id):
    value = _load(_receipt_path(directory, node_id))
    if (
        value.get("schema_version") != 1
        or value.get("batch_id") != plan["batch_id"]
        or value.get("node_id") != node_id
        or value.get("status") != "committed"
    ):
        raise SummaryValidationError("Invalid partition node receipt")
    candidate = value.get("candidate")
    if _digest(_json(candidate)) != value.get("candidate_sha256"):
        raise SourceChangedError("Partition candidate integrity check failed")
    if _hash_file(_join(directory, f"outputs/{node_id}.md")) != value.get("summary_sha256"):
        raise SourceChangedError("Partition summary integrity check failed")
    task_path = _join(directory, f"tasks/{node_id}.json")
    if _hash_file(task_path) != value.get("input_manifest_sha256"):
        raise SourceChangedError("Partition task integrity check failed")
    _validate_candidate(_load(task_path), candidate)
    if (
        len(candidate["content"].encode()) > NODE_OUTPUT_BYTES
        or _digest(candidate["content"].encode()) != value["summary_sha256"]
    ):
        raise SourceChangedError("Partition summary no longer matches its validated candidate")
    return value


def _child_source(plan, child_id, receipt):
    relative = f"{plan['batch_id']}/{child_id}.md"
    return {
        "source_id": _source_id("summary-node-v1", relative, receipt["summary_sha256"], 0),
        "namespace": "summary-node-v1",
        "path": relative,
        "line": 0,
        "node_id": child_id,
        "sha256": receipt["summary_sha256"],
        "parse_error": None,
        "inherited_missing_count": receipt["coverage"]["missing_fragments"],
        "fragment_count": receipt["coverage"]["fragments"],
    }


def _task(memory, directory, plan, node):
    sources = node["sources"]
    if node["children"]:
        sources = [
            _child_source(plan, child, _receipt(directory, plan, child))
            for child in node["children"]
        ]
    manifest = {
        "schema_version": 1,
        "format": "summary-partition-node-v1",
        "batch_id": plan["batch_id"],
        "node_id": node["node_id"],
        "level": plan["level"],
        "start": plan["start"],
        "end": plan["end"],
        "sources": sources,
        "output_bytes": NODE_OUTPUT_BYTES,
    }
    if node["node_id"] == plan["root_node_id"] and plan["upstream"]["count"]:
        manifest["upstream_coverage_ref"] = {
            **plan["upstream"],
            "has_inherited_gaps": plan["has_inherited_gaps"],
        }
    path = _join(directory, f"tasks/{node['node_id']}.json")
    if path.exists():
        if _load(path) != manifest:
            raise SourceChangedError("Partition inputs changed after task preparation")
    else:
        _atomic(path, _json(manifest))
    for source in sources:
        if source["namespace"] == "summary-fragment-v1":
            stored = _join(directory / "files", source["path"])
        else:
            stored = _join(directory, f"outputs/{source['node_id']}.md")
        locator = {**source, "stored_path": stored.relative_to(memory.data_dir).as_posix()}
        locator_path = _join(memory.state, f"partition-sources/{source['source_id']}.json")
        if not locator_path.exists() or _load(locator_path) != locator:
            _atomic(locator_path, _json(locator))
    candidate = _join(
        memory.workspace, f".alice/candidates/{plan['batch_id']}/{node['node_id']}.json"
    )
    _mkdir(candidate.parent)
    return {
        "node_id": node["node_id"],
        "manifest_path": str(path),
        "candidate_path": str(candidate),
        "source_count": len(sources),
        "prompt": _template(
            "chronicle-partition-node.md",
            **{key: manifest[key] for key in ("batch_id", "node_id", "level", "start", "end")},
            manifest_path=path,
            candidate_path=candidate,
        ),
    }


def read_source(memory, source_id, *, max_chars, offset_chars):
    if not isinstance(source_id, str) or not re.fullmatch(r"s_[a-f0-9]{64}", source_id):
        return None
    path = _join(memory.state, f"partition-sources/{source_id}.json")
    if not path.exists():
        return None
    source = _load(path)
    stored = _join(memory.data_dir, source["stored_path"])
    if source.get("namespace") == "summary-fragment-v1":
        if _fragment_id(source) != source_id:
            raise SourceChangedError("Fragment provenance integrity check failed")
        raw = _range(stored, source["byte_start"], source["byte_end"])
    elif source.get("namespace") == "summary-node-v1":
        if (
            _source_id("summary-node-v1", source["path"], source["sha256"], 0) != source_id
            or stored.stat().st_size > NODE_OUTPUT_BYTES
        ):
            raise SourceChangedError("Node provenance integrity check failed")
        raw = stored.read_bytes()
    else:
        raise SummaryValidationError("Unknown partition source namespace")
    if _digest(raw) != source["sha256"]:
        raise SourceChangedError("Frozen partition source integrity check failed")
    text = raw.decode("utf-8", errors="replace")
    end = offset_chars + max_chars
    return {
        **{key: value for key, value in source.items() if key != "stored_path"},
        "relative_path": source["path"],
        "line_number": source["line"],
        "content": text[offset_chars:end],
        "total_chars": len(text),
        "offset_chars": offset_chars,
        "truncated": len(text) > end,
        "next_offset": end if len(text) > end else None,
    }


def _range(path, start, end):
    if (
        type(start) is not int
        or type(end) is not int
        or start < 0
        or not 0 <= end - start <= FRAGMENT_BYTES
    ):
        raise SummaryValidationError("Invalid fragment byte range")
    with path.open("rb") as stream:
        stream.seek(start)
        result = stream.read(end - start)
    if len(result) != end - start:
        raise SourceChangedError("Partition source was truncated")
    return result


def next_nodes(memory, batch_id, *, limit=4):
    if type(limit) is not int or not 1 <= limit <= 4:
        raise SummaryValidationError("Partition query limit must be between 1 and 4")
    with memory._lock():
        memory._recover_commits()
        directory, plan = _plan(memory, batch_id)
        if _receipt_path(directory, plan["root_node_id"]).exists():
            _finalize(memory, directory, plan)
        intent = _load(_join(memory.state, f"commits/{batch_id}.json"))
        completed, ready = 0, []
        for entry in _catalog(directory, plan["nodes"]):
            if _receipt_path(directory, entry["node_id"]).exists():
                completed += 1
            elif len(ready) < limit:
                path = _join(directory, entry["path"])
                if _hash_file(path) != entry["sha256"]:
                    raise SourceChangedError("Partition node integrity check failed")
                node = _load(path)
                if all(_receipt_path(directory, child).exists() for child in node["children"]):
                    ready.append(_task(memory, directory, plan, node))
        return {
            "batch_id": batch_id,
            "complete": intent["status"] == "committed",
            "ready": ready,
            "completed_nodes": completed,
            "total_nodes": plan["nodes"]["count"],
        }


def commit_node(memory, batch_id, node_id, candidate):
    with memory._lock():
        memory._recover_commits()
        directory, plan = _plan(memory, batch_id)
        node = _node(directory, plan, node_id)
        task = _task(memory, directory, plan, node)
        manifest = _load(Path(task["manifest_path"]))
        _validate_candidate(manifest, candidate)
        if (
            len(candidate["content"].encode()) > NODE_OUTPUT_BYTES
            or len(_json(candidate)) > 64 * 1024
        ):
            raise SummaryValidationError("Partition candidate exceeds its output bound")
        receipt_path = _receipt_path(directory, node_id)
        already = receipt_path.exists()
        if already:
            if _receipt(directory, plan, node_id)["candidate_sha256"] != _digest(_json(candidate)):
                raise MemoryConflictError(
                    "This partition already has a different committed summary"
                )
        else:
            if not node["children"]:
                for source in manifest["sources"]:
                    for root in (memory.workspace, directory / "files"):
                        try:
                            raw = _range(
                                _join(root, source["path"]),
                                source["byte_start"],
                                source["byte_end"],
                            )
                        except FileNotFoundError as exc:
                            raise SourceChangedError(
                                "A frozen partition source is missing"
                            ) from exc
                        if _digest(raw) != source["sha256"]:
                            raise SourceChangedError("A frozen partition source was modified")
            missing = {entry["source_id"] for entry in candidate["missing"]}
            fragments = missing_fragments = 0
            for source in manifest["sources"]:
                count = source.get("fragment_count", 1)
                fragments += count
                missing_fragments += (
                    count
                    if source["source_id"] in missing
                    else source.get("inherited_missing_count", 0)
                )
            raw = candidate["content"].encode()
            _atomic(_join(directory, f"outputs/{node_id}.md"), raw)
            receipt = {
                "schema_version": 1,
                "status": "committed",
                "batch_id": batch_id,
                "node_id": node_id,
                "candidate": candidate,
                "candidate_sha256": _digest(_json(candidate)),
                "summary_sha256": _digest(raw),
                "input_manifest_sha256": _hash_file(Path(task["manifest_path"])),
                "coverage": {"fragments": fragments, "missing_fragments": missing_fragments},
            }
            _atomic(receipt_path, _json(receipt))
        if _receipt_path(directory, plan["root_node_id"]).exists():
            _finalize(memory, directory, plan)
        intent = _load(_join(memory.state, f"commits/{batch_id}.json"))
        return {
            "batch_id": batch_id,
            "node_id": node_id,
            "already_committed": already,
            "complete": intent["status"] == "committed",
        }


def _inherited_missing(directory, plan, leaf_index):
    tier, index = 0, leaf_index
    child = _node_id(tier, index)
    reason = None
    while child != plan["root_node_id"]:
        tier, index = tier + 1, index // FAN_IN
        parent = _node_id(tier, index)
        parent_receipt = _receipt(directory, plan, parent)
        child_receipt = _receipt(directory, plan, child)
        source_id = _child_source(plan, child, child_receipt)["source_id"]
        for entry in parent_receipt["candidate"]["missing"]:
            if entry["source_id"] == source_id:
                reason = f"{parent}: {entry['reason']}"
        child = parent
    return reason


def _coverage(memory, directory, plan):
    records = iter(_catalog(directory, plan["records"]))
    record, offset, record_fragments = None, None, 0
    total = missing_count = completed_records = 0
    temporary = _join(directory, "coverage.building.jsonl")
    with temporary.open("w") as output:
        for leaf_index in range(plan["leaf_count"]):
            node_id = _node_id(0, leaf_index)
            node = _node(directory, plan, node_id)
            receipt = _receipt(directory, plan, node_id)
            manifest = _load(_join(directory, f"tasks/{node_id}.json"))
            if manifest["sources"] != node["sources"]:
                raise SourceChangedError("Leaf task no longer matches the frozen plan")
            inherited = _inherited_missing(directory, plan, leaf_index)
            missing = {
                entry["source_id"]: entry["reason"] for entry in receipt["candidate"]["missing"]
            }
            for source in node["sources"]:
                if record is None:
                    record = next(records, None)
                    if record is None:
                        raise SummaryValidationError("Partition coverage contains extra sources")
                    offset, record_fragments = record["byte_start"], 0
                if (
                    source["parent_source_id"] != record["parent_source_id"]
                    or source["path"] != record["path"]
                    or source["file_sha256"] != record["file_sha256"]
                    or source["byte_start"] != offset
                    or source["byte_end"] > record["byte_end"]
                    or _fragment_id(source) != source["source_id"]
                ):
                    raise SummaryValidationError(
                        "Partition coverage has a gap, overlap or wrong source version"
                    )
                reason = inherited or missing.get(source["source_id"])
                _row(
                    output,
                    {
                        "source_id": source["source_id"],
                        "parent_source_id": record["parent_source_id"],
                        "byte_start": source["byte_start"],
                        "byte_end": source["byte_end"],
                        "status": "missing" if reason else "covered",
                        "reason": reason,
                        "node_id": node_id,
                        "candidate_sha256": receipt["candidate_sha256"],
                    },
                )
                total += 1
                missing_count += int(reason is not None)
                record_fragments += 1
                offset = source["byte_end"]
                if offset == record["byte_end"]:
                    if record_fragments != record["fragment_count"]:
                        raise SummaryValidationError("Partition fragment count mismatch")
                    completed_records += 1
                    record = None
        if (
            record is not None
            or next(records, None) is not None
            or total != plan["fragment_count"]
            or completed_records != plan["source_count"]
        ):
            raise SummaryValidationError("Partition root coverage is incomplete")
        output.flush()
        os.fsync(output.fileno())
    path = _join(directory, "coverage.jsonl")
    os.replace(temporary, path)
    _fsync_dir(directory)
    return {
        "path": "coverage.jsonl",
        "sha256": _hash_file(path),
        "fragments": total,
        "missing_fragments": missing_count,
        "records": completed_records,
    }


def _finalize(memory, directory, plan):
    intent_path = _join(memory.state, f"commits/{plan['batch_id']}.json")
    intent = _load(intent_path)
    validate_summary_commit_header(intent)
    if intent["status"] == "committed":
        return
    root = _receipt(directory, plan, plan["root_node_id"])
    # Every intermediate receipt must match the exact committed child versions.
    for entry in _catalog(directory, plan["nodes"]):
        node = _node(directory, plan, entry["node_id"])
        _receipt(directory, plan, node["node_id"])
        _task(memory, directory, plan, node)
    for entry in _catalog(directory, plan["files"]):
        try:
            current = _hash_file(_join(memory.workspace, entry["path"]), entry["size"])
            frozen = _hash_file(_join(directory / "files", entry["path"]))
        except FileNotFoundError as exc:
            raise SourceChangedError("A frozen summary source is missing") from exc
        if current != entry["sha256"] or frozen != entry["sha256"]:
            raise SourceChangedError("A frozen summary source was modified")
    coverage = _coverage(memory, directory, plan)
    inherited_gaps = False
    for entry in _catalog(directory, plan["upstream"]):
        upstream_directory, _ = _plan(memory, entry["batch_id"])
        reference = entry["coverage_ref"]
        if _hash_file(_join(upstream_directory, reference["path"])) != reference["sha256"]:
            raise SourceChangedError("Upstream summary coverage integrity check failed")
        inherited_gaps |= bool(
            reference["missing_fragments"] or reference.get("has_inherited_gaps")
        )
    if inherited_gaps != plan["has_inherited_gaps"]:
        raise SummaryValidationError("Upstream summary gap declarations disagree")
    if root["coverage"] != {
        "fragments": coverage["fragments"],
        "missing_fragments": coverage["missing_fragments"],
    }:
        raise SummaryValidationError("Partition root coverage totals disagree")
    manifest = {
        "schema_version": 2,
        "partition_plan": plan["batch_id"],
        **{key: plan[key] for key in ("batch_id", "level", "start", "end", "target")},
        "coverage_ref": {
            "partition_plan": plan["batch_id"],
            **coverage,
            "upstream_ref": plan["upstream"],
            "has_inherited_gaps": inherited_gaps,
        },
    }
    target = _join(memory.workspace, plan["target"])
    before = target.read_bytes() if target.exists() else None
    candidate = root["candidate"]
    rendered = _render_summary(manifest, candidate, before)
    _atomic(
        intent_path,
        _json(
            {
                "schema_version": 2,
                "format": FORMAT,
                "batch_id": plan["batch_id"],
                "partition_plan_sha256": plan["batch_id"],
                "status": "pending",
                "target": plan["target"],
                "candidate_sha256": root["candidate_sha256"],
                "before_sha256": _digest(before) if before is not None else None,
                "after_sha256": _digest(rendered),
                "rendered": rendered.decode(),
                "candidate": candidate,
                "manifest": manifest,
            }
        ),
    )
    memory._recover_commits()
