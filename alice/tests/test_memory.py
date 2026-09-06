"""Regression tests use temporary sources only; no model or production data."""

import datetime as dt
import json
from pathlib import Path
import stat

import pytest

from alice_codex import memory
from alice_codex.memory import (
    MemoryStore,
    MemoryError,
    MemoryConflictError,
    SourceChangedError,
    SummaryValidationError,
    AUTO_SEPARATOR,
)


NOW = dt.datetime(2026, 10, 5, tzinfo=dt.timezone.utc)


def put(root: Path, rel: str, text: str) -> Path:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


def jsonl(*records: dict) -> str:
    return "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in records)


def legacy_fixture(root: Path) -> None:
    put(root, "SOUL.md", "Stable identity\n")
    put(root, "USER.md", "Preference A\n")
    put(root, "AGENTS.md", "Old harness instructions\n")
    put(root, "memory/MEMORY.md", "A confirmed fact\n")
    put(root, "memory/notebook/thinking.md", "A personal note\n")
    put(
        root,
        "memory/chronicle/traces/2026-09-01.jsonl",
        jsonl(
            {"timestamp": "2026-09-01T00:15:00", "role": "user", "content": "Origin evidence"},
            {"timestamp": "2026-09-01T00:25:00", "role": "assistant", "content": "Observed result"},
        ),
    )
    record = {
        "time_start": "2026-09-01T00:00:00",
        "time_end": "2026-09-01T02:00:00",
        "source_l0": "traces/2026-09-01.jsonl",
        "content": "Summary [L0: 00:15-00:25]",
    }
    put(
        root,
        "memory/chronicle/hourly/2026-09-01.jsonl",
        jsonl(record, {**record, "content": "Different version [L0: 00:15-00:25]"}),
    )
    put(
        root,
        "memory/chronicle/diary/2026-09-01.md",
        "## 手记\nKeep verbatim\n\n" + AUTO_SEPARATOR + "\nDaily [L1: 00:00-02:00]\n",
    )
    put(root, "memory/chronicle/weekly/2026-W36.md", "Weekly [L2: 2026-09-01]\n")
    put(root, "memory/chronicle/monthly/2026-09.md", "Monthly [W: 2026-W36]\n")
    put(
        root,
        "sessions/example.jsonl",
        jsonl({"timestamp": "2026-09-01T00:15:00", "content": "Conversation"}),
    )


def candidate(store: MemoryStore, batch: dict, text: str = "Verified observation") -> dict:
    manifest = json.loads(Path(batch["manifest_path"]).read_text())
    good = [s["source_id"] for s in manifest["sources"] if not s["parse_error"]]
    bad = [s["source_id"] for s in manifest["sources"] if s["parse_error"]]
    return {
        "content": text + " " + " ".join(f"[source:{s}]" for s in good),
        "source_ids": good,
        "covered_source_ids": good,
        "missing": [{"source_id": s, "reason": "Malformed original record"} for s in bad],
    }


def test_private_snapshot_preserves_raw_bytes_and_excludes_credentials(tmp_path):
    source, data = tmp_path / "old", tmp_path / "new"
    legacy_fixture(source)
    put(source, "memory/notebook/.env", "API_KEY=fixture-secret\n")
    put(source, "memory/notebook/.zhihu_cookies.txt", "fixture-cookie\n")
    put(source, "memory/notebook/auth.json", '{"token":"fixture-secret"}')
    put(source, "memory/notebook/config.json", '{"apiKey":"fixture-secret"}')
    external = tmp_path / "external"
    raw_log = put(
        external,
        "observe.jsonl",
        '{"content":"historical Authorization: opaque-fixture"}\nBROKEN LINE\n',
    )
    put(external, "astra.env", "excluded independent credentials")
    (source / "memory/notebook/escape.md").symlink_to(raw_log)
    store = MemoryStore(data)
    result = store.snapshot_legacy(
        source, snapshot_id="initial", source_roots={"runtime-logs": external}
    )
    manifest = json.loads(Path(result["manifest_path"]).read_text())
    archived = Path(result["manifest_path"]).parent / "files"
    assert (archived / "runtime-logs/observe.jsonl").read_bytes() == raw_log.read_bytes()
    assert result["excluded_count"] == 6
    assert not (store.workspace / "AGENTS.md").exists()
    assert (store.workspace / "SOUL.md").read_bytes() == (source / "SOUL.md").read_bytes()
    assert stat.S_IMODE(data.stat().st_mode) == 0o700
    assert all(stat.S_IMODE(p.stat().st_mode) == 0o600 for p in data.rglob("*") if p.is_file())
    assert all(stat.S_IMODE(p.stat().st_mode) == 0o700 for p in data.rglob("*") if p.is_dir())
    assert manifest["consistent"]
    assert (
        store.read_source(store.search("BROKEN LINE")[0]["source_id"])["parse_error"]
        == "invalid_json"
    )
    assert (
        "opaque-fixture"
        in store.read_source(store.search("opaque-fixture")[0]["source_id"])["content"]
    )


def test_snapshot_request_identity_includes_external_mapping(tmp_path):
    source = tmp_path / "old"
    legacy_fixture(source)
    first = put(tmp_path, "one.log", "first")
    second = put(tmp_path, "two.log", "second")
    store = MemoryStore(tmp_path / "data")
    store.snapshot_legacy(source, snapshot_id="fixed", source_roots={"logs/events.log": first})
    with pytest.raises(MemoryConflictError, match="source mappings"):
        store.snapshot_legacy(source, snapshot_id="fixed", source_roots={"logs/events.log": second})
    with pytest.raises(MemoryError, match="does not exist"):
        store.snapshot_legacy(source, source_roots={"logs/missing": tmp_path / "missing"})
    with pytest.raises(MemoryError, match="existing directory"):
        store.snapshot_legacy(tmp_path / "missing")


def test_read_source_can_page_to_last_character(tmp_path):
    source = tmp_path / "old"
    original = "Reading a larger original record. " * 3000
    put(source, "memory/notebook/large.md", original)
    store = MemoryStore(tmp_path / "data")
    store.snapshot_legacy(source)
    sid = store.search("Reading a larger")[0]["source_id"]
    offset, chunks = 0, []
    while offset is not None:
        chunk = store.read_source(sid, max_chars=12000, offset_chars=offset)
        chunks.append(chunk["content"])
        offset = chunk["next_offset"]
    assert "".join(chunks) == original
    assert chunk["total_chars"] == len(original)
    assert store.search("Reading a larger")[0]["indexed_truncated"]
    assert store.index_status()["truncated_record_count"] == 1
    assert store.index_status()["source_count"] == 1
    with pytest.raises(MemoryError):
        store.read_source(sid, offset_chars=-1)


def test_duplicate_versions_and_complete_legacy_reference_chain(tmp_path):
    source = tmp_path / "old"
    legacy_fixture(source)
    store = MemoryStore(tmp_path / "data")
    store.snapshot_legacy(source, snapshot_id="one")
    monthly = store.search("Monthly")[0]
    weekly = store.resolve_legacy_reference(monthly["source_id"])
    assert len(weekly) == 1
    daily = store.resolve_legacy_reference(weekly[0]["source_id"])
    assert len(daily) == 1
    hourly = store.resolve_legacy_reference(daily[0]["source_id"])
    assert len(hourly) == 2
    assert hourly[0]["source_id"] != hourly[1]["source_id"]
    traces = store.resolve_legacy_reference(hourly[0]["source_id"])
    assert len(traces) == 2
    assert "Origin evidence" in store.read_source(traces[0]["source_id"])["content"]


def test_incremental_final_snapshot_three_way_seed_and_idempotence(tmp_path):
    source = tmp_path / "old"
    legacy_fixture(source)
    store = MemoryStore(tmp_path / "data")
    first = store.snapshot_legacy(source, snapshot_id="first")
    first_manifest = Path(first["manifest_path"]).read_bytes()
    put(store.workspace, "SOUL.md", "New migrated identity\n")
    put(source, "SOUL.md", "Old runtime also edited identity\n")
    put(source, "USER.md", "Preference B\n")
    put(source, "memory/notebook/new.md", "Late new fact\n")
    second = store.snapshot_legacy(
        source, snapshot_id="final", previous_snapshot_id="first", final=True
    )
    assert second["workspace_conflicts"] == [{"path": "SOUL.md", "reason": "both_changed"}]
    assert (store.workspace / "SOUL.md").read_text() == "New migrated identity\n"
    assert (store.workspace / "USER.md").read_text() == "Preference B\n"
    assert (store.workspace / "memory/notebook/new.md").read_text() == "Late new fact\n"
    assert Path(first["manifest_path"]).read_bytes() == first_manifest
    repeated = store.snapshot_legacy(
        source, snapshot_id="final", previous_snapshot_id="first", final=True
    )
    assert repeated["manifest_sha256"] == second["manifest_sha256"]
    assert len(store.search("Late new fact")) == 1
    with pytest.raises(MemoryConflictError):
        store.snapshot_legacy(source, snapshot_id="final")


def test_final_rejects_source_change_without_publishing_partial_snapshot(tmp_path, monkeypatch):
    source = tmp_path / "old"
    legacy_fixture(source)
    store = MemoryStore(tmp_path / "data")
    original = memory._stable_copy
    changed = False

    def mutate(source_path, target):
        nonlocal changed
        result = original(source_path, target)
        if not changed:
            changed = True
            source_path.write_text(source_path.read_text() + "late write")
        return result

    monkeypatch.setattr(memory, "_stable_copy", mutate)
    with pytest.raises(SourceChangedError):
        store.snapshot_legacy(source, snapshot_id="bad", final=True)
    assert not (store.archives / "bad").exists()
    assert not list(store.archives.glob(".snapshot-*"))
    changed = False
    result = store.snapshot_legacy(source, snapshot_id="provisional")
    assert result["consistent"] is False
    assert not (store.workspace / "SOUL.md").exists()


def test_snapshot_content_and_manifest_tampering_is_detected(tmp_path):
    source = tmp_path / "old"
    legacy_fixture(source)
    store = MemoryStore(tmp_path / "data")
    result = store.snapshot_legacy(source, snapshot_id="one")
    sid = store.search("A personal note")[0]["source_id"]
    put(store.archives / "one/files", "memory/notebook/thinking.md", "tampered")
    with pytest.raises(MemoryError, match="integrity"):
        store.read_source(sid)
    with pytest.raises(MemoryError, match="integrity"):
        store.snapshot_legacy(source, snapshot_id="one")
    manifest_path = Path(result["manifest_path"])
    manifest = json.loads(manifest_path.read_text())
    manifest["file_count"] = 100000
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(MemoryError, match="manifest integrity"):
        store.snapshot_legacy(source, snapshot_id="one")


def test_reject_path_traversal_symlink_output_and_recursive_source(tmp_path):
    store = MemoryStore(tmp_path / "data")
    source = tmp_path / "old"
    source.mkdir()
    with pytest.raises(MemoryError):
        store.snapshot_legacy(source, source_roots={"../escape": source})
    with pytest.raises(MemoryError):
        store.snapshot_legacy(source, source_roots={"recursive": tmp_path})
    outside = tmp_path / "outside"
    outside.mkdir()
    (store.workspace / "memory").symlink_to(outside, target_is_directory=True)
    legacy_fixture(source)
    with pytest.raises(MemoryError, match="Symlink"):
        store.snapshot_legacy(source, snapshot_id="blocked")
    assert not (outside / "MEMORY.md").exists()


@pytest.mark.parametrize(
    "level,period,count",
    [
        ("L1", "2026-09-01T00:00", 2),
        ("L2", "2026-09-01", 2),
        ("L3", "2026-W36", 1),
        ("L4", "2026-09", 1),
    ],
)
def test_prepare_all_four_levels_and_keep_versions(tmp_path, level, period, count):
    source = tmp_path / "old"
    legacy_fixture(source)
    store = MemoryStore(tmp_path / "data")
    store.snapshot_legacy(source)
    batch = store.prepare_summary(level, period, now=NOW)
    assert batch["source_count"] == count
    assert store.prepare_summary(level, period, now=NOW)["batch_id"] == batch["batch_id"]
    assert batch["candidate_path"] in batch["prompt"]
    assert Path(batch["candidate_path"]).is_relative_to(store.workspace)
    manifest = json.loads(Path(batch["manifest_path"]).read_text())
    target = store.workspace / manifest["target"]
    old = target.read_bytes()
    result = store.commit_summary(batch["batch_id"], candidate(store, batch))
    assert result["already_committed"] is False
    assert target.read_bytes().startswith(old)
    after = target.read_bytes()
    assert store.commit_summary(batch["batch_id"], candidate(store, batch))["already_committed"]
    assert target.read_bytes() == after


def test_period_boundaries_and_no_open_windows(tmp_path):
    store = MemoryStore(tmp_path / "data")
    zone = dt.timezone(dt.timedelta(hours=8))
    start, end = memory._period_bounds("L1", "2026-12-31T22:00", "Asia/Shanghai")
    assert end == dt.datetime(2027, 1, 1, tzinfo=zone)
    start, end = memory._period_bounds("L3", "2026-W01", "Asia/Shanghai")
    assert start.date() == dt.date(2025, 12, 29)
    assert end.date() == dt.date(2026, 1, 5)
    start, end = memory._period_bounds("L4", "2026-12", "Asia/Shanghai")
    assert end.date() == dt.date(2027, 1, 1)
    with pytest.raises(SummaryValidationError, match="not closed"):
        store.prepare_summary("L2", "2026-09-01", now=dt.datetime(2026, 9, 1, tzinfo=zone))
    for level, period in [
        ("L1", "2026-09-01T01:00"),
        ("L3", "2026-W54"),
        ("L4", "../escape"),
        ("L0", "2026-09-01"),
    ]:
        with pytest.raises(SummaryValidationError):
            store.prepare_summary(level, period, now=NOW)


def test_candidate_coverage_and_citation_errors_do_not_write(tmp_path):
    source = tmp_path / "old"
    legacy_fixture(source)
    store = MemoryStore(tmp_path / "data")
    store.snapshot_legacy(source)
    batch = store.prepare_summary("L1", "2026-09-01T00:00", now=NOW)
    target = store.workspace / "memory/chronicle/hourly/2026-09-01.jsonl"
    old = target.read_bytes()
    valid = candidate(store, batch)
    bad = {**valid, "covered_source_ids": valid["covered_source_ids"][:1]}
    with pytest.raises(SummaryValidationError, match="coverage"):
        store.commit_summary(batch["batch_id"], bad)
    bad = {**valid, "content": "An unsupported success claim"}
    with pytest.raises(SummaryValidationError, match="citations"):
        store.commit_summary(batch["batch_id"], bad)
    bad = {**valid, "source_ids": ["s_" + "0" * 64]}
    with pytest.raises(SummaryValidationError, match="unknown"):
        store.commit_summary(batch["batch_id"], bad)
    assert target.read_bytes() == old
    assert not (store.state / "commits").exists()


def test_malformed_and_missing_timestamp_sources_are_not_silently_lost(tmp_path):
    store = MemoryStore(tmp_path / "data")
    put(
        store.workspace,
        "memory/chronicle/traces/2026-09-01.jsonl",
        jsonl(
            {"timestamp": "2026-09-01T00:10:00", "content": "Good"},
            {"timestamp": "bad-time", "content": "Unknown timing"},
        )
        + "BROKEN\n",
    )
    batch = store.prepare_summary("L1", "2026-09-01T00:00", now=NOW)
    assert batch["source_count"] == 3
    value = candidate(store, batch)
    assert len(value["missing"]) == 2
    store.commit_summary(batch["batch_id"], value)


def test_source_modification_rejected_but_later_append_allowed(tmp_path):
    store = MemoryStore(tmp_path / "data")
    source = put(
        store.workspace,
        "memory/chronicle/traces/2026-09-01.jsonl",
        jsonl({"timestamp": "2026-09-01T00:10:00", "content": "Before"}),
    )
    batch = store.prepare_summary("L1", "2026-09-01T00:00", now=NOW)
    original = source.read_text()
    source.write_text(original.replace("Before", "After!"))
    with pytest.raises(SourceChangedError):
        store.commit_summary(batch["batch_id"], candidate(store, batch))
    source.write_text(original + jsonl({"timestamp": "2026-09-01T02:10:00", "content": "Later"}))
    store.commit_summary(batch["batch_id"], candidate(store, batch))


def test_commit_recovers_after_failure_before_canonical_write(tmp_path, monkeypatch):
    source = tmp_path / "old"
    legacy_fixture(source)
    store = MemoryStore(tmp_path / "data")
    store.snapshot_legacy(source)
    batch = store.prepare_summary("L2", "2026-09-01", now=NOW)
    target = store.workspace / "memory/chronicle/diary/2026-09-01.md"
    original = target.read_text()
    real_atomic = memory._atomic

    def fail_target(path, content):
        if path == target:
            raise OSError("simulated disk failure")
        return real_atomic(path, content)

    monkeypatch.setattr(memory, "_atomic", fail_target)
    with pytest.raises(OSError):
        store.commit_summary(batch["batch_id"], candidate(store, batch))
    assert target.read_text() == original
    monkeypatch.setattr(memory, "_atomic", real_atomic)
    recovered = MemoryStore(tmp_path / "data")
    assert target.read_text().startswith(original)
    assert "Verified observation" in target.read_text()
    assert recovered.commit_summary(batch["batch_id"], candidate(store, batch))["already_committed"]


def test_pending_commit_does_not_overwrite_new_manual_edit(tmp_path, monkeypatch):
    store = MemoryStore(tmp_path / "data")
    put(
        store.workspace,
        "memory/chronicle/hourly/2026-09-01.jsonl",
        jsonl(
            {
                "time_start": "2026-09-01T00:00:00",
                "time_end": "2026-09-01T02:00:00",
                "content": "Source",
            }
        ),
    )
    target = put(
        store.workspace, "memory/chronicle/diary/2026-09-01.md", "Initial handwritten note\n"
    )
    batch = store.prepare_summary("L2", "2026-09-01", now=NOW)
    real_atomic = memory._atomic

    def fail_target(path, content):
        if path == target:
            raise OSError("simulated")
        return real_atomic(path, content)

    monkeypatch.setattr(memory, "_atomic", fail_target)
    with pytest.raises(OSError):
        store.commit_summary(batch["batch_id"], candidate(store, batch))
    monkeypatch.setattr(memory, "_atomic", real_atomic)
    target.write_text("New manual edit after crash\n")
    with pytest.raises(MemoryConflictError):
        MemoryStore(tmp_path / "data")
    assert target.read_text() == "New manual edit after crash\n"


def test_summary_revision_preserves_old_auto_and_manual_sections(tmp_path):
    source = tmp_path / "old"
    legacy_fixture(source)
    store = MemoryStore(tmp_path / "data")
    store.snapshot_legacy(source)
    first = store.prepare_summary("L2", "2026-09-01", now=NOW)
    store.commit_summary(first["batch_id"], candidate(store, first, "Version one"))
    lower = store.workspace / "memory/chronicle/hourly/2026-09-01.jsonl"
    with lower.open("a") as stream:
        stream.write(
            jsonl(
                {
                    "time_start": "2026-09-01T04:00:00",
                    "time_end": "2026-09-01T06:00:00",
                    "content": "A late source",
                }
            )
        )
    second = store.prepare_summary("L2", "2026-09-01", now=NOW)
    assert second["batch_id"] != first["batch_id"]
    store.commit_summary(second["batch_id"], candidate(store, second, "Version two"))
    text = (store.workspace / "memory/chronicle/diary/2026-09-01.md").read_text()
    assert "Keep verbatim" in text and "Daily [L1:" in text
    assert "Version one" in text and "Version two" in text
    assert len(list((store.state / "commits").glob("*.json"))) == 2


def test_concurrent_summary_commit_is_written_once(tmp_path):
    from concurrent.futures import ThreadPoolExecutor

    source = tmp_path / "old"
    legacy_fixture(source)
    store = MemoryStore(tmp_path / "data")
    store.snapshot_legacy(source)
    batch = store.prepare_summary("L2", "2026-09-01", now=NOW)
    value = candidate(store, batch)

    def commit():
        return MemoryStore(tmp_path / "data").commit_summary(batch["batch_id"], value)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: commit(), range(2)))
    assert sorted(r["already_committed"] for r in results) == [False, True]
    target = store.workspace / "memory/chronicle/diary/2026-09-01.md"
    assert target.read_text().count("<!-- anima-summary:" + batch["batch_id"] + " -->") == 1


def test_live_events_are_idempotent_searchable_and_summarizable(tmp_path):
    store = MemoryStore(tmp_path / "data")
    event = {
        "kind": "turn_completed",
        "thread_id": "thread-1",
        "turn_id": "turn-1",
        "content": "Fresh observed result",
    }
    first = store.append_event("turn-1:completed", event, timestamp="2026-09-01T00:20:00+08:00")
    assert not first["already_recorded"]
    assert store.append_event("turn-1:completed", event, timestamp="2026-09-01T00:20:00+08:00")[
        "already_recorded"
    ]
    assert store.search("Fresh observed")[0]["source_id"] == first["source_id"]
    assert "Fresh observed result" in store.read_source(first["source_id"])["content"]
    assert len(list((store.workspace / "memory/chronicle/traces").rglob("*.jsonl"))) == 1
    batch = store.prepare_summary("L1", "2026-09-01T00:00", now=NOW)
    assert batch["source_count"] == 1
    store.commit_summary(batch["batch_id"], candidate(store, batch))
    with pytest.raises(MemoryConflictError):
        store.append_event(
            "turn-1:completed",
            {**event, "content": "Changed"},
            timestamp="2026-09-01T00:20:00+08:00",
        )
    with pytest.raises(MemoryError, match="one event"):
        store.append_event("history", {"messages": [{"content": "Do not repeat history"}]})


def test_live_event_recovery_retains_raw_record_and_does_not_overwrite_edit(tmp_path, monkeypatch):
    store = MemoryStore(tmp_path / "data")
    real_atomic = memory._atomic

    def fail_workspace(path, content):
        if path.is_relative_to(store.workspace):
            raise OSError("simulated canonical write failure")
        return real_atomic(path, content)

    monkeypatch.setattr(memory, "_atomic", fail_workspace)
    with pytest.raises(OSError):
        store.append_event(
            "event-1", {"content": "Private retained event"}, timestamp="2026-09-01T00:10:00+08:00"
        )
    assert len(list((store.state / "events/raw").glob("*.jsonl"))) == 1
    assert not list((store.workspace / "memory/chronicle/traces").rglob("*.jsonl"))
    monkeypatch.setattr(memory, "_atomic", real_atomic)
    recovered = MemoryStore(tmp_path / "data")
    result = recovered.append_event(
        "event-1", {"content": "Private retained event"}, timestamp="2026-09-01T00:10:00+08:00"
    )
    assert result["already_recorded"]
    assert recovered.search("Private retained event")
    Path(result["path"]).write_text("Later manual edit\n")
    with pytest.raises(MemoryConflictError):
        recovered.append_event(
            "event-1", {"content": "Private retained event"}, timestamp="2026-09-01T00:10:00+08:00"
        )
    assert Path(result["path"]).read_text() == "Later manual edit\n"


def test_runtime_templates_install_without_replacing_local_instructions(tmp_path):
    store = MemoryStore(tmp_path / "data")
    result = store.install_workspace_templates()
    assert result["installed"][:2] == ["AGENTS.md", ".alice/prompts/autonomy-review.md"]
    assert "SOUL.md" in (store.workspace / "AGENTS.md").read_text()
    assert (store.workspace / ".alice/prompts/autonomy-review.md").is_file()
    from alice_codex.identity import build_identity_bundle
    assert build_identity_bundle(store.workspace).revision
    for relative in ("SOUL.md", "USER.md", "memory/MEMORY.md"):
        assert (store.workspace / relative).is_file()
    assert store.install_workspace_templates()["installed"] == []
    (store.workspace / "AGENTS.md").write_text("Deliberate local runtime rules\n")
    assert store.install_workspace_templates()["preserved"] == ["AGENTS.md"]
    assert (store.workspace / "AGENTS.md").read_text() == "Deliberate local runtime rules\n"


def test_malformed_record_in_new_l0_layout_is_an_explicit_gap(tmp_path):
    store = MemoryStore(tmp_path / "data")
    store.append_event(
        "valid", {"content": "A valid source"}, timestamp="2026-09-01T00:10:00+08:00"
    )
    put(store.workspace, "memory/chronicle/traces/2026-09-01/bad.jsonl", "BROKEN\n")
    batch = store.prepare_summary("L1", "2026-09-01T00:00", now=NOW)
    assert batch["source_count"] == 2
    assert len(candidate(store, batch)["missing"]) == 1


def test_installed_skill_tree_is_discoverable_and_preserves_customization(tmp_path, monkeypatch):
    package = tmp_path / "package"
    put(package, "templates/AGENTS.md", "Generic runtime guidance")
    put(package, "templates/autonomy-review.md", "Generic review")
    for name in ("SOUL.md", "USER.md", "MEMORY.md"):
        put(package, "templates/" + name, "Synthetic identity default")
    put(
        package,
        "templates/.agents/skills/verify-outcome/SKILL.md",
        "---\nname: verify-outcome\n---\nVerify evidence",
    )
    put(package, "templates/.agents/skills/verify-outcome/references/schema.json", "{}")
    put(package, "templates/unrelated/private.txt", "Must not install")
    monkeypatch.setattr(memory.resources, "files", lambda _: package)
    store = MemoryStore(tmp_path / "data")
    result = store.install_workspace_templates()
    discovered = list((store.workspace / ".agents/skills").glob("*/SKILL.md"))
    assert [path.parent.name for path in discovered] == ["verify-outcome"]
    assert ".agents/skills/verify-outcome/references/schema.json" in result["installed"]
    assert not (store.workspace / "unrelated").exists()
    discovered[0].write_text("Custom verified skill\n")
    assert store.install_workspace_templates()["preserved"] == [
        ".agents/skills/verify-outcome/SKILL.md"
    ]
    assert discovered[0].read_text() == "Custom verified skill\n"


def test_index_version_zero_is_validated_then_upgraded_without_losing_sources(tmp_path):
    import sqlite3

    store = MemoryStore(tmp_path / "data")
    event = store.append_event("version-fixture", {"content": "Version preserved"})
    with sqlite3.connect(store.index_path) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 1
        db.execute("PRAGMA user_version=0")
    upgraded = MemoryStore(tmp_path / "data")
    assert upgraded.search("Version preserved")[0]["source_id"] == event["source_id"]
    with sqlite3.connect(store.index_path) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 1


@pytest.mark.parametrize("damage", ["future", "unknown_zero", "incomplete_one"])
def test_unsupported_index_schema_fails_without_recreating_data(tmp_path, damage):
    import sqlite3

    store = MemoryStore(tmp_path / "data")
    with sqlite3.connect(store.index_path) as db:
        if damage == "future":
            db.execute("PRAGMA user_version=99")
        elif damage == "unknown_zero":
            db.execute("PRAGMA user_version=0")
            db.execute("ALTER TABLE sources ADD COLUMN unrelated TEXT")
        else:
            db.execute("DROP TABLE sources")
    before = store.index_path.read_bytes()
    with pytest.raises(MemoryError, match="schema"):
        MemoryStore(tmp_path / "data")
    assert store.index_path.read_bytes() == before


def test_default_identity_templates_preserve_imported_and_edited_records(tmp_path):
    store = MemoryStore(tmp_path / "data")
    for relative in ("SOUL.md", "USER.md", "memory/MEMORY.md"):
        target = store.workspace / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"Synthetic preserved custom bytes\r\n")
    result = store.install_workspace_templates()
    assert set(result["preserved"]) == {"SOUL.md", "USER.md", "memory/MEMORY.md"}
    for relative in result["preserved"]:
        assert (store.workspace / relative).read_bytes() == b"Synthetic preserved custom bytes\r\n"
