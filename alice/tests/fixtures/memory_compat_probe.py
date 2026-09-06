"""Synthetic-only data probe around an externally managed code-pointer rollback.

Run with each installed candidate's isolated Python. This creates no processes,
changes no release pointer, and never consumes an existing runtime directory.
"""

import argparse
import hashlib
import json
from pathlib import Path
import sqlite3

from alice_codex.memory import MemoryConflictError, MemoryStore


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("prepare", "check"))
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    root = args.root.resolve()
    if args.phase == "prepare":
        if root.exists():
            raise ValueError("Synthetic probe requires a new, nonexistent root")
        root.mkdir(mode=0o700, parents=True)
        legacy_note = root / "legacy/memory/notebook/probe.md"
        legacy_note.parent.mkdir(parents=True)
        legacy_note.write_text("Synthetic legacy note v0\n")
        store = MemoryStore(root / "runtime")
        store.snapshot_legacy(root / "legacy", snapshot_id="compat-old")
        legacy_note.write_text("Synthetic legacy note v1\n")
        store.snapshot_legacy(
            root / "legacy", snapshot_id="compat-latest", previous_snapshot_id="compat-old"
        )
        event = store.append_event(
            "compat:new-event",
            {"content": "Synthetic event after cutover"},
            timestamp="2026-09-01T00:30:00+08:00",
        )
        note = store.workspace / "memory/notebook/probe.md"
        note.write_text("Synthetic new-side handwritten note\n")
        receipt = {
            "synthetic_probe": 1,
            "source_id": event["source_id"],
            "event_path": Path(event["path"]).relative_to(root).as_posix(),
            "event_sha256": digest(Path(event["path"])),
            "note_path": note.relative_to(root).as_posix(),
            "note_sha256": digest(note),
        }
        (root / "probe.json").write_text(json.dumps(receipt))
    receipt = json.loads((root / "probe.json").read_text())
    assert receipt["synthetic_probe"] == 1
    store = MemoryStore(root / "runtime")
    with sqlite3.connect(store.index_path) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 1
    assert "Synthetic event after cutover" in store.read_source(receipt["source_id"])["content"]
    try:
        store.snapshot_legacy(root / "legacy", snapshot_id="compat-old")
    except MemoryConflictError:
        pass
    else:
        raise AssertionError("Old snapshot seed was not rejected")
    for kind in ("event", "note"):
        assert digest(root / receipt[f"{kind}_path"]) == receipt[f"{kind}_sha256"]
    print(
        json.dumps(
            {
                "schema": 1,
                "event_count": 1,
                "note_count": 1,
                "event_sha256": receipt["event_sha256"],
                "note_sha256": receipt["note_sha256"],
                "old_seed_rejected": True,
                "phase": args.phase,
            }
        )
    )


if __name__ == "__main__":
    main()
