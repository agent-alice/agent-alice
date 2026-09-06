"""Synthetic archive recovery and compatibility evidence; never a private cutover."""

import errno
import hashlib
import json
from pathlib import Path

import pytest

from alice_codex import memory
from alice_codex.memory import MemoryConflictError, MemoryError, MemoryStore


def put(root: Path, relative: str, content: bytes) -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def manifest(result: dict) -> dict:
    return json.loads(Path(result["manifest_path"]).read_text())


def assert_archive(result: dict, expected: dict[str, bytes]) -> dict:
    value = manifest(result)
    root = Path(result["manifest_path"]).parent / "files"
    entries = {entry["path"]: entry for entry in value["files"]}
    assert set(entries) == set(expected)
    assert result["file_count"] == value["file_count"] == len(expected)
    assert result["total_bytes"] == value["total_bytes"] == sum(map(len, expected.values()))
    for relative, content in expected.items():
        assert (root / relative).read_bytes() == content
        assert entries[relative]["size"] == len(content)
        assert entries[relative]["sha256"] == hashlib.sha256(content).hexdigest()
    return entries


def test_index_failure_same_snapshot_id_recovers_published_bytes_without_copying(
    tmp_path, monkeypatch
):
    source = tmp_path / "old"
    originals = {
        "memory/notebook/personal.md": b"Original personal note\r\n",
        "sessions/complete.jsonl": b'{"content":"Original log"}\n{malformed}\n',
    }
    for relative, content in originals.items():
        put(source, relative, content)
    store = MemoryStore(tmp_path / "new")
    original_index = store._index_file

    def interrupted_index(*args):
        original_index(*args)
        raise MemoryError("Synthetic index interruption after inserting source rows")

    with monkeypatch.context() as fault:
        fault.setattr(store, "_index_file", interrupted_index)
        with pytest.raises(MemoryError, match="Synthetic index interruption"):
            store.snapshot_legacy(source, snapshot_id="recover")

    published = store.archives / "recover"
    original_manifest = (published / "manifest.json").read_bytes()
    archived_inodes = {
        relative: (published / "files" / relative).stat().st_ino for relative in originals
    }
    assert store.index_status()["source_count"] == 0
    assert not (store.state / "last-seed.json").exists()
    assert list(store.workspace.iterdir()) == []
    put(source, "sessions/complete.jsonl", b'{"content":"New writer data"}\n')
    put(source, "memory/notebook/later.md", b"Later legacy note\n")

    original_copy = memory._stable_copy

    def reject_legacy_copy(source_path, target):
        if source_path.is_relative_to(source):
            pytest.fail("A published snapshot retry must not copy legacy source bytes again")
        return original_copy(source_path, target)

    monkeypatch.setattr(memory, "_stable_copy", reject_legacy_copy)
    recovered = MemoryStore(store.data_dir)
    result = recovered.snapshot_legacy(source, snapshot_id="recover")
    assert_archive(result, originals)
    assert (published / "manifest.json").read_bytes() == original_manifest
    assert {path.name for path in store.archives.iterdir()} == {"recover"}
    assert {
        relative: (published / "files" / relative).stat().st_ino for relative in originals
    } == archived_inodes
    assert recovered.index_status()["source_count"] == 3
    assert recovered.index_status()["malformed_record_count"] == 1
    assert len(recovered.search("Original log")) == 1
    assert recovered.search("New writer data") == []
    assert (recovered.workspace / "memory/notebook/personal.md").read_bytes() == originals[
        "memory/notebook/personal.md"
    ]
    assert not (recovered.workspace / "memory/notebook/later.md").exists()


def test_incremental_manifest_and_three_way_conflicts_preserve_both_histories(tmp_path):
    source = tmp_path / "old"
    first_bytes = {
        "SOUL.md": b"Old identity\n",
        "memory/history.jsonl": b'{"content":"First legacy message"}\n',
        "memory/notebook/deleted-by-source.md": b"Old source note\n",
        "memory/notebook/deleted-by-workspace.md": b"Old workspace note\n",
        "memory/notebook/retained-after-removal.md": b"Keep original evidence\n",
        "memory/notebook/untouched.md": b"Unchanged note\n",
    }
    for relative, content in first_bytes.items():
        put(source, relative, content)
    store = MemoryStore(tmp_path / "new")
    first = store.snapshot_legacy(source, snapshot_id="before")
    assert_archive(first, first_bytes)
    first_manifest = Path(first["manifest_path"]).read_bytes()

    new_event = store.append_event(
        "synthetic-new-message",
        {"content": "New runtime message"},
        timestamp="2026-09-01T01:00:00+08:00",
    )
    event_bytes = Path(new_event["path"]).read_bytes()
    new_note = put(store.workspace, "memory/notebook/new-runtime.md", b"New personal note\n")
    put(store.workspace, "SOUL.md", b"New workspace identity\n")
    put(store.workspace, "memory/notebook/deleted-by-source.md", b"New workspace note\n")
    (store.workspace / "memory/notebook/deleted-by-workspace.md").unlink()

    replacement = put(tmp_path, "replacement.md", b"Replacement source identity\n")
    replacement.replace(source / "SOUL.md")
    with (source / "memory/history.jsonl").open("ab") as stream:
        stream.write(b'{"content":"Appended legacy message"}\r\n')
    put(source, "memory/notebook/deleted-by-workspace.md", b"Source changed after deletion\n")
    put(source, "memory/notebook/new-source.md", b"New source note\n")
    (source / "memory/notebook/deleted-by-source.md").unlink()
    (source / "memory/notebook/retained-after-removal.md").unlink()
    second_bytes = {
        relative: (source / relative).read_bytes()
        for relative in first_bytes
        if (source / relative).exists()
    }
    second_bytes["memory/notebook/new-source.md"] = b"New source note\n"

    second = store.snapshot_legacy(
        source, snapshot_id="after", previous_snapshot_id="before", final=True
    )
    entries = assert_archive(second, second_bytes)
    assert {relative: entry["change"] for relative, entry in entries.items()} == {
        "SOUL.md": "changed",
        "memory/history.jsonl": "changed",
        "memory/notebook/deleted-by-workspace.md": "changed",
        "memory/notebook/untouched.md": "unchanged",
        "memory/notebook/new-source.md": "added",
    }
    assert set(manifest(second)["removed_since_previous"]) == {
        "memory/notebook/deleted-by-source.md",
        "memory/notebook/retained-after-removal.md",
    }
    assert {(item["path"], item["reason"]) for item in second["workspace_conflicts"]} == {
        ("SOUL.md", "both_changed"),
        ("memory/notebook/deleted-by-workspace.md", "workspace_deleted_source_changed"),
        ("memory/notebook/deleted-by-source.md", "source_deleted_workspace_changed"),
    }
    assert (store.workspace / "SOUL.md").read_bytes() == b"New workspace identity\n"
    assert (store.workspace / "memory/notebook/deleted-by-source.md").read_bytes() == (
        b"New workspace note\n"
    )
    assert not (store.workspace / "memory/notebook/deleted-by-workspace.md").exists()
    assert (store.workspace / "memory/notebook/retained-after-removal.md").read_bytes() == (
        b"Keep original evidence\n"
    )
    assert new_note.read_bytes() == b"New personal note\n"
    assert Path(new_event["path"]).read_bytes() == event_bytes
    assert "New runtime message" in store.read_source(new_event["source_id"])["content"]
    assert_archive(first, first_bytes)
    assert Path(first["manifest_path"]).read_bytes() == first_manifest
    old_messages = store.search("First legacy message")
    assert {row["namespace"] for row in old_messages} == {"before", "after"}
    assert len({row["source_id"] for row in old_messages}) == 2
    assert {row["namespace"] for row in store.search("Appended legacy message")} == {"after"}


@pytest.mark.parametrize("operation", ["retry", "incremental"])
def test_future_manifest_schema_with_valid_hash_is_rejected_without_rewriting(tmp_path, operation):
    source = tmp_path / "old"
    put(source, "memory/notebook/personal.md", b"Preserved note\n")
    store = MemoryStore(tmp_path / "new")
    result = store.snapshot_legacy(source, snapshot_id="future")
    path = Path(result["manifest_path"])
    value = json.loads(path.read_text())
    value.pop("manifest_sha256")
    value["schema_version"] = 99
    canonical = (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()
    value["manifest_sha256"] = hashlib.sha256(canonical).hexdigest()
    future_bytes = (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()
    path.write_bytes(future_bytes)
    workspace_before = (store.workspace / "memory/notebook/personal.md").read_bytes()
    index_before = store.index_path.read_bytes()

    with pytest.raises(MemoryError, match="schema"):
        if operation == "retry":
            store.snapshot_legacy(source, snapshot_id="future")
        else:
            store.snapshot_legacy(source, snapshot_id="next", previous_snapshot_id="future")

    assert path.read_bytes() == future_bytes
    assert store.index_path.read_bytes() == index_before
    assert (store.workspace / "memory/notebook/personal.md").read_bytes() == workspace_before
    assert {item.name for item in store.archives.iterdir()} == {"future"}


def test_completed_seed_retry_does_not_restore_a_deleted_workspace_note(tmp_path):
    source = tmp_path / "old"
    put(source, "memory/notebook/personal.md", b"Later deleted deliberately\n")
    store = MemoryStore(tmp_path / "new")
    first = store.snapshot_legacy(source, snapshot_id="seed")
    (store.workspace / "memory/notebook/personal.md").unlink()
    new_note = put(store.workspace, "memory/notebook/new.md", b"Newer personal note\n")

    recovered = MemoryStore(store.data_dir)
    repeated = recovered.snapshot_legacy(source, snapshot_id="seed")

    assert repeated == first
    assert not (store.workspace / "memory/notebook/personal.md").exists()
    assert new_note.read_bytes() == b"Newer personal note\n"
    assert (store.archives / "seed/files/memory/notebook/personal.md").read_bytes() == (
        b"Later deleted deliberately\n"
    )


def test_old_seed_replay_cannot_roll_back_a_newer_seed_or_resurrect_deleted_note(tmp_path):
    source = tmp_path / "old"
    put(source, "memory/notebook/personal.md", b"Original note\n")
    store = MemoryStore(tmp_path / "new")
    store.snapshot_legacy(source, snapshot_id="older")
    put(source, "memory/notebook/personal.md", b"Latest legacy note\n")
    store.snapshot_legacy(source, snapshot_id="newer", previous_snapshot_id="older")
    note = store.workspace / "memory/notebook/personal.md"
    note.unlink()
    last_seed = (store.state / "last-seed.json").read_bytes()

    with pytest.raises(MemoryConflictError, match="latest seeded snapshot"):
        store.snapshot_legacy(source, snapshot_id="older")

    assert not note.exists()
    assert (store.state / "last-seed.json").read_bytes() == last_seed
    assert len(store.search("Original note")) == 1
    assert len(store.search("Latest legacy note")) == 1


def test_incremental_unchanged_archive_fits_only_changed_file_copy_budget(tmp_path, monkeypatch):
    source = tmp_path / "old"
    unchanged = b"Synthetic retained raw log\r\n" * 65536
    earlier = b"Earlier small note\n"
    changed = b"Updated small note\n"
    assert len(earlier) == len(changed)
    put(source, "sessions/unchanged.log", unchanged)
    put(source, "memory/notebook/change.md", earlier)
    store = MemoryStore(tmp_path / "new")
    first = store.snapshot_legacy(source, snapshot_id="base", seed_workspace=False)
    put(source, "memory/notebook/change.md", changed)
    original_copy = memory._stable_copy
    copied_bytes = 0

    def limited_copy(source_path, target):
        nonlocal copied_bytes
        size = source_path.stat().st_size
        if copied_bytes + size > len(changed):
            raise OSError(errno.ENOSPC, "Synthetic budget allows only changed source bytes")
        result = original_copy(source_path, target)
        copied_bytes += size
        return result

    monkeypatch.setattr(memory, "_stable_copy", limited_copy)
    second = store.snapshot_legacy(
        source, snapshot_id="incremental", previous_snapshot_id="base", seed_workspace=False
    )

    assert copied_bytes == len(changed)
    entries = assert_archive(
        second,
        {
            "sessions/unchanged.log": unchanged,
            "memory/notebook/change.md": changed,
        },
    )
    assert entries["sessions/unchanged.log"]["change"] == "unchanged"
    assert entries["memory/notebook/change.md"]["change"] == "changed"
    first_root = Path(first["manifest_path"]).parent / "files"
    second_root = Path(second["manifest_path"]).parent / "files"
    original = first_root / "sessions/unchanged.log"
    linked = second_root / "sessions/unchanged.log"
    assert original.samefile(linked)
    assert not original.samefile(source / "sessions/unchanged.log")
    assert linked.stat().st_nlink == 2
    assert (first_root / "memory/notebook/change.md").read_bytes() == earlier
    assert not (first_root / "memory/notebook/change.md").samefile(
        second_root / "memory/notebook/change.md"
    )
    assert not list(store.archives.glob(".snapshot-*"))


def test_seed_conflict_after_copy_preserves_new_workspace_note_and_original_archive(
    tmp_path, monkeypatch
):
    source = tmp_path / "old"
    relative = "memory/notebook/personal.md"
    original = b"Original archived personal note\n"
    newer = b"Personal note written while seeding\n"
    put(source, relative, original)
    store = MemoryStore(tmp_path / "new")
    target = store.workspace / relative
    original_copy = memory._stable_copy
    introduced_note = False

    def write_note_after_seed_copy(source_path, temporary):
        nonlocal introduced_note
        copied = original_copy(source_path, temporary)
        if source_path.is_relative_to(store.archives):
            assert not target.exists()
            assert temporary.read_bytes() == original
            put(store.workspace, relative, newer)
            introduced_note = True
        return copied

    monkeypatch.setattr(memory, "_stable_copy", write_note_after_seed_copy)
    with pytest.raises(MemoryConflictError, match="later edit"):
        store.snapshot_legacy(source, snapshot_id="seed-race")

    archived = store.archives / "seed-race/files" / relative
    assert introduced_note
    assert target.read_bytes() == newer
    assert archived.read_bytes() == original
    assert (source / relative).read_bytes() == original
    assert not list(target.parent.glob(".seed-*"))
    assert not (store.state / "last-seed.json").exists()
    assert len(store.search("Original archived personal note")) == 1
    result = store.snapshot_legacy(source, snapshot_id="seed-race")
    assert result["workspace_conflicts"] == [
        {"path": relative, "reason": "existing_workspace_file"}
    ]
    assert_archive(result, {relative: original})
    assert target.read_bytes() == newer


def test_default_source_parent_symlink_is_rejected_before_archiving(tmp_path):
    source = tmp_path / "old"
    source.mkdir()
    outside = tmp_path / "outside"
    note = put(outside, "MEMORY.md", b"Synthetic data beyond source boundary\n")
    (source / "memory").symlink_to(outside, target_is_directory=True)
    store = MemoryStore(tmp_path / "new")

    with pytest.raises(MemoryError, match="parent.*symlink"):
        store.snapshot_legacy(source, snapshot_id="parent-symlink")

    assert list(store.archives.iterdir()) == []
    assert list(store.workspace.iterdir()) == []
    assert store.index_status()["source_count"] == 0
    assert (source / "memory").is_symlink()
    assert note.read_bytes() == b"Synthetic data beyond source boundary\n"


@pytest.mark.parametrize(
    "overlap",
    [
        "source-data",
        "source-workspace",
        "mapped-data",
        "mapped-workspace",
        "mapped-file",
        "mapped-ancestor",
    ],
)
def test_archive_input_cannot_read_its_own_data_or_containing_directory(tmp_path, overlap):
    source = tmp_path / "old"
    source.mkdir()
    store = MemoryStore(tmp_path / "new")
    note = put(store.workspace, "memory/notebook/current.md", b"Current runtime personal note\n")
    index_before = store.index_path.read_bytes()
    source_root = source
    mappings = None
    if overlap == "source-data":
        source_root = store.data_dir
    elif overlap == "source-workspace":
        source_root = store.workspace
    else:
        mapped_path = {
            "mapped-data": store.data_dir,
            "mapped-workspace": store.workspace,
            "mapped-file": store.index_path,
            "mapped-ancestor": store.data_dir.parent,
        }[overlap]
        mappings = {"selected-input": mapped_path}

    with pytest.raises(MemoryError, match="input and output directories must be separate"):
        store.snapshot_legacy(source_root, snapshot_id="self-input", source_roots=mappings)

    assert list(store.archives.iterdir()) == []
    assert store.index_path.read_bytes() == index_before
    assert store.index_status()["source_count"] == 0
    assert note.read_bytes() == b"Current runtime personal note\n"
    assert not (store.state / "last-seed.json").exists()


def test_explicit_mapping_equal_to_source_root_terminates_and_archives(tmp_path):
    source = tmp_path / "old"
    original = b"Synthetic source root mapping\n"
    put(source, "complete.log", original)
    store = MemoryStore(tmp_path / "new")

    result = store.snapshot_legacy(
        source,
        snapshot_id="whole-root",
        source_roots={"complete-old": source},
        seed_workspace=False,
    )

    assert_archive(result, {"complete-old/complete.log": original})
    assert len(store.search("Synthetic source root mapping")) == 1


def test_first_seed_after_archive_only_snapshot_imports_unchanged_and_changed_notes(tmp_path):
    source = tmp_path / "old"
    put(source, "SOUL.md", b"First identity\n")
    put(source, "memory/notebook/unchanged.md", b"Archived but never seeded\n")
    store = MemoryStore(tmp_path / "new")
    store.snapshot_legacy(source, snapshot_id="archive-only", seed_workspace=False)
    assert list(store.workspace.iterdir()) == []
    assert not (store.state / "last-seed.json").exists()
    expected = {
        "SOUL.md": b"Latest identity\n",
        "memory/notebook/unchanged.md": b"Archived but never seeded\n",
        "memory/notebook/new.md": b"Added since archive-only snapshot\n",
    }
    for relative, content in expected.items():
        put(source, relative, content)

    result = store.snapshot_legacy(
        source, snapshot_id="first-seed", previous_snapshot_id="archive-only"
    )

    assert result["workspace_conflicts"] == []
    assert_archive(result, expected)
    for relative, content in expected.items():
        assert (store.workspace / relative).read_bytes() == content
    assert json.loads((store.state / "last-seed.json").read_text())["snapshot_id"] == "first-seed"


def test_seed_after_archive_only_increment_uses_last_workspace_seed_as_merge_base(tmp_path):
    source = tmp_path / "old"
    first_bytes = {
        "SOUL.md": b"Initial seeded identity\n",
        "memory/notebook/both-changed.md": b"Initial seeded personal note\n",
        "memory/notebook/deleted-edited.md": b"Initial note later deleted by source\n",
        "memory/notebook/deleted-unchanged.md": b"Preserve original after source deletion\n",
    }
    for relative, content in first_bytes.items():
        put(source, relative, content)
    store = MemoryStore(tmp_path / "new")
    first = store.snapshot_legacy(source, snapshot_id="seed-a")
    new_notes = {
        "memory/notebook/both-changed.md": b"New runtime personal note\n",
        "memory/notebook/deleted-edited.md": b"New runtime note before source deletion\n",
    }
    for relative, content in new_notes.items():
        put(store.workspace, relative, content)
    intermediate_bytes = {
        "SOUL.md": b"Intermediate archived identity\n",
        "memory/notebook/both-changed.md": b"Changed legacy personal note\n",
    }
    for relative, content in intermediate_bytes.items():
        put(source, relative, content)
    for name in ("deleted-edited.md", "deleted-unchanged.md"):
        (source / "memory/notebook" / name).unlink()
    intermediate = store.snapshot_legacy(
        source, snapshot_id="archive-b", previous_snapshot_id="seed-a", seed_workspace=False
    )
    assert (store.workspace / "SOUL.md").read_bytes() == first_bytes["SOUL.md"]
    assert json.loads((store.state / "last-seed.json").read_text())["snapshot_id"] == "seed-a"

    final_bytes = {**intermediate_bytes, "SOUL.md": b"Final source identity\n"}
    put(source, "SOUL.md", final_bytes["SOUL.md"])
    final = store.snapshot_legacy(
        source, snapshot_id="seed-c", previous_snapshot_id="archive-b", final=True
    )

    assert (store.workspace / "SOUL.md").read_bytes() == final_bytes["SOUL.md"]
    assert {(item["path"], item["reason"]) for item in final["workspace_conflicts"]} == {
        ("memory/notebook/both-changed.md", "both_changed"),
        ("memory/notebook/deleted-edited.md", "source_deleted_workspace_changed"),
    }
    for relative, content in new_notes.items():
        assert (store.workspace / relative).read_bytes() == content
    assert (store.workspace / "memory/notebook/deleted-unchanged.md").read_bytes() == (
        first_bytes["memory/notebook/deleted-unchanged.md"]
    )
    assert manifest(final)["removed_since_previous"] == []
    assert_archive(first, first_bytes)
    assert_archive(intermediate, intermediate_bytes)
    assert_archive(final, final_bytes)
    assert json.loads((store.state / "last-seed.json").read_text())["snapshot_id"] == "seed-c"
    assert (
        store.snapshot_legacy(
            source, snapshot_id="seed-c", previous_snapshot_id="archive-b", final=True
        )
        == final
    )
