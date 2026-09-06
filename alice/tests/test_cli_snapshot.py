"""Verify recoverable snapshot IDs through the user-facing CLI."""

from dataclasses import asdict
import json

from alice_codex import cli
from alice_codex.config import RuntimeConfig
from alice_codex.files import write_json
from alice_codex.memory import MemoryStore


def fixture_args(tmp_path):
    config = RuntimeConfig(str(tmp_path / "alice"), "/usr/bin/true", "fixture", "unused")
    config.prepare_directories()
    write_json(config.root / "config.json", asdict(config))
    source = tmp_path / "legacy"
    source.mkdir()
    (source / "SOUL.md").write_text("Synthetic continuity fixture\n")
    return config, ["--home", config.home, "memory", "snapshot", str(source)]


def test_cli_snapshot_id_reuses_original_archive(tmp_path, capsys):
    config, args = fixture_args(tmp_path)
    args += ["--snapshot-id", "repeatable-fixture"]
    assert cli.main(args) == 0
    first = json.loads(capsys.readouterr().out)
    archived = config.root / "archives/repeatable-fixture/files/SOUL.md"
    before = archived.stat()
    assert cli.main(args) == 0
    second = json.loads(capsys.readouterr().out)
    assert second["snapshot_id"] == first["snapshot_id"] == "repeatable-fixture"
    assert second["manifest_sha256"] == first["manifest_sha256"]
    after = archived.stat()
    assert (after.st_ino, after.st_mtime_ns, after.st_size) == (
        before.st_ino,
        before.st_mtime_ns,
        before.st_size,
    )
    assert [p.name for p in (config.root / "archives").iterdir()] == ["repeatable-fixture"]


def test_cli_reports_id_before_index_failure_and_retry_resumes(tmp_path, monkeypatch, capsys):
    config, args = fixture_args(tmp_path)
    original = MemoryStore._index_snapshot
    attempts = []

    def fail_once(self, manifest, destination):
        attempts.append(manifest["snapshot_id"])
        if len(attempts) == 1:
            raise OSError("synthetic index failure after publishing archive")
        return original(self, manifest, destination)

    monkeypatch.setattr(MemoryStore, "_index_snapshot", fail_once)
    assert cli.main(args) == 1
    failure = capsys.readouterr()
    assert not failure.out
    snapshot_id = json.loads(failure.err.splitlines()[0])["snapshot_id"]
    assert "synthetic index failure" in failure.err
    archived = config.root / "archives" / snapshot_id / "files/SOUL.md"
    before = archived.stat()
    assert cli.main([*args, "--snapshot-id", snapshot_id]) == 0
    recovered = json.loads(capsys.readouterr().out)
    assert recovered["snapshot_id"] == snapshot_id
    assert attempts == [snapshot_id, snapshot_id]
    assert archived.stat().st_ino == before.st_ino
    assert [p.name for p in (config.root / "archives").iterdir()] == [snapshot_id]
    assert MemoryStore(config.root).search("Synthetic continuity")


def test_duplicate_external_prefix_does_not_silently_drop_a_source(tmp_path, capsys):
    config, args = fixture_args(tmp_path)
    first = tmp_path / "first.log"
    second = tmp_path / "second.log"
    first.write_text("first fixture\n")
    second.write_text("second fixture\n")
    assert (
        cli.main([*args, "--external", f"extra.log={first}", "--external", f"extra.log={second}"])
        == 1
    )
    assert "prefixes must be unique" in capsys.readouterr().err
    assert not list((config.root / "archives").iterdir())


def test_explicit_empty_snapshot_id_is_rejected_before_archive_creation(tmp_path, capsys):
    config, args = fixture_args(tmp_path)
    assert cli.main([*args, "--snapshot-id", ""]) == 1
    result = capsys.readouterr()
    assert "Snapshot identifier must not be empty" in result.err
    assert '"snapshot_id"' not in result.err
    assert not result.out
    assert not list((config.root / "archives").iterdir())
