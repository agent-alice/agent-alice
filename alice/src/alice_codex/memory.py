"""Private legacy archives and provenance-preserving Chronicle summaries.

This module never invokes a model or changes Codex's native history/memories.
Archive bytes are retained verbatim, including malformed records. Indexing and
summary validation establish structural provenance, not factual correctness.
"""

import contextlib
import datetime as dt
import fcntl
import hashlib
from importlib import resources
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import sqlite3
import stat
from string import Template
import tempfile
from typing import Any, Iterator, Mapping
import uuid
from zoneinfo import ZoneInfo


DEFAULT_SOURCES = (
    "SOUL.md",
    "USER.md",
    "AGENTS.md",
    "HEARTBEAT.md",
    "memory/MEMORY.md",
    "memory/history.jsonl",
    "memory/notebook",
    "memory/chronicle",
    "sessions",
)
AUTO_SEPARATOR = "<!-- auto-generated below -->"
_SENSITIVE_NAMES = {
    "auth.json",
    "credentials.json",
    "secrets.json",
    "config.json",
    "cookies.txt",
    ".zhihu_cookies.txt",
    "cookies.json",
    "credentials",
    "id_rsa",
    "id_ed25519",
}
_CITATION = re.compile(r"\[source:(s_[a-f0-9]{64})\]")
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$")
_READ_CHARS = 65536
_PARSE_CHARS = 1024 * 1024


class MemoryError(RuntimeError):
    """A memory operation failed without discarding its source data."""


class SourceChangedError(MemoryError):
    pass


class MemoryConflictError(MemoryError):
    pass


class SummaryValidationError(MemoryError):
    pass


class NoSourcesError(SummaryValidationError):
    """The closed period contains no eligible source records."""


class _PartitionRequired(SummaryValidationError):
    """Select the bounded partition protocol without truncating a legacy batch."""


def _json(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()


def _digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _relative(value: str) -> str:
    path = PurePosixPath(value)
    if not value or path.is_absolute() or ".." in path.parts or "\\" in value or "\x00" in value:
        raise MemoryError("Expected a confined relative path")
    return path.as_posix()


def _join(root: Path, value: str) -> Path:
    value = _relative(value)
    current = root
    for part in PurePosixPath(value).parts:
        current = current / part
        if current.is_symlink():
            raise MemoryError("Symlink traversal is not allowed")
    return current


def _mkdir(path: Path) -> None:
    if path.is_symlink():
        raise MemoryError("Symlink directories are not allowed")
    missing = []
    current = path
    while not current.exists():
        missing.append(current)
        current = current.parent
    if current.is_symlink():
        raise MemoryError("Symlink directories are not allowed")
    for directory in reversed(missing):
        directory.mkdir(exist_ok=True, mode=0o700)
    path.chmod(0o700)


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _atomic(path: Path, content: bytes) -> None:
    _mkdir(path.parent)
    if path.is_symlink():
        raise MemoryError("Cannot replace a symlink")
    fd, name = tempfile.mkstemp(prefix=".write-", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
        _fsync_dir(path.parent)
    finally:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(name)


def _fingerprint(path: Path) -> tuple[int, int, int, int, int]:
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise MemoryError("Source is not a regular file")
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def _hash_file(path: Path, size: int | None = None) -> str:
    digest = hashlib.sha256()
    remaining = size
    with path.open("rb") as stream:
        while remaining is None or remaining > 0:
            block = stream.read(1024 * 1024 if remaining is None else min(remaining, 1024 * 1024))
            if not block:
                if remaining:
                    raise SourceChangedError("A source was truncated")
                break
            digest.update(block)
            if remaining is not None:
                remaining -= len(block)
    return digest.hexdigest()


def _stable_copy(source: Path, target: Path) -> dict[str, Any]:
    before = _fingerprint(source)
    _mkdir(target.parent)
    digest = hashlib.sha256()
    fd = os.open(source, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        opened = os.fstat(fd)
        if (opened.st_dev, opened.st_ino) != before[:2]:
            raise SourceChangedError("Source changed before copying")
        out = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(out, "wb") as destination, os.fdopen(fd, "rb", closefd=False) as stream:
            remaining = before[2]
            while remaining:
                block = stream.read(min(remaining, 1024 * 1024))
                if not block:
                    raise SourceChangedError("Source was truncated while copying")
                digest.update(block)
                destination.write(block)
                remaining -= len(block)
            destination.flush()
            os.fsync(destination.fileno())
        if before != _fingerprint(source):
            raise SourceChangedError("Source changed while copying")
        return {"sha256": digest.hexdigest(), "size": before[2], "fingerprint": list(before)}
    finally:
        os.close(fd)


def _excluded(path: Path) -> bool:
    name = path.name.lower()
    return (
        name in _SENSITIVE_NAMES
        or name == ".env"
        or name.startswith(".env.")
        or name.endswith((".env", ".pem", ".key", ".p12", ".pfx", ".keychain-db"))
        or "cookie" in name
        or name.startswith(("auth-token", "api-key", "secret-key"))
        or name == "__pycache__"
        or name == ".git"
    )


def _inventory(roots: Mapping[str, Path]) -> tuple[dict[str, Path], list[dict[str, str]]]:
    files: dict[str, Path] = {}
    excluded: list[dict[str, str]] = []

    def visit(path: Path, rel: str) -> None:
        if _excluded(path) or path.is_symlink():
            excluded.append(
                {
                    "path": rel,
                    "reason": "symlink" if path.is_symlink() else "credential_or_runtime_file",
                }
            )
            return
        if path.is_dir():
            for child in sorted(path.iterdir()):
                visit(child, f"{rel}/{child.name}")
        elif path.is_file():
            if rel in files:
                raise MemoryError("Overlapping archive source mappings")
            files[rel] = path
        elif path.exists():
            excluded.append({"path": rel, "reason": "not_regular"})

    for rel, path in roots.items():
        visit(path, _relative(rel))
    return files, excluded


def _source_id(namespace: str, rel: str, digest: str, line: int) -> str:
    return "s_" + _digest(_json([namespace, rel, digest, line]))


def _kind(rel: str) -> str:
    for name in ("traces", "hourly", "diary", "weekly", "monthly", "notebook", "sessions"):
        if name in PurePosixPath(rel).parts:
            return name
    return "identity" if rel in {"SOUL.md", "USER.md", "memory/MEMORY.md"} else "log"


def _records(path: Path) -> Iterator[tuple[int, str, dict[str, Any] | None, str | None]]:
    """Bound parsing, retaining oversized/malformed originals as addressable gaps."""
    for number, text, total in _record_slices(path, _PARSE_CHARS):
        error, obj = None, None
        if total > _PARSE_CHARS:
            error = "record_exceeds_parse_limit"
        elif path.suffix.lower() == ".jsonl":
            try:
                obj = json.loads(text)
                if not isinstance(obj, dict):
                    error, obj = "json_not_object", None
            except (ValueError, RecursionError):
                error, obj = "invalid_json", None
        yield number, text, obj, error


def _record_slices(
    path: Path, max_chars: int, offset_chars: int = 0
) -> Iterator[tuple[int, str, int]]:
    """Scan in fixed chunks, retaining only the requested character range.

    Universal-newline/replacement decoding matches the schema-1 reader. Markdown
    remains record 0; other files retain physical line numbers and source IDs.
    Raw bytes are never decoded or truncated during archival copying.
    """
    document = path.suffix.lower() in {".md", ".markdown"}
    number, total, parts = (0 if document else 1), 0, []
    with path.open("r", encoding="utf-8", errors="replace") as stream:
        while True:
            chunk = stream.read(_READ_CHARS) if document else stream.readline(_READ_CHARS)
            if not chunk:
                if total or document:
                    yield number, "".join(parts), total
                return
            end = total + len(chunk)
            left, right = max(offset_chars, total), min(offset_chars + max_chars, end)
            if left < right:
                parts.append(chunk[left - total : right - total])
            total = end
            if not document and chunk.endswith("\n"):
                yield number, "".join(parts), total
                number, total, parts = number + 1, 0, []


def _atomic_copy(source: Path, target: Path, expected: str, before: str | None) -> None:
    """Seed large documents without loading the full archive into memory."""
    _mkdir(target.parent)
    fd, name = tempfile.mkstemp(prefix=".seed-", dir=target.parent)
    os.close(fd)
    temporary = Path(name)
    temporary.unlink()
    try:
        if _stable_copy(source, temporary)["sha256"] != expected:
            raise MemoryError("Snapshot content integrity check failed")
        if target.is_symlink():
            raise MemoryError("Cannot replace a symlink")
        if (_hash_file(target) if target.exists() else None) != before:
            raise MemoryConflictError("Workspace seed conflicts with a later edit")
        os.replace(temporary, target)
        _fsync_dir(target.parent)
    finally:
        temporary.unlink(missing_ok=True)


class MemoryStore:
    SCHEMA_VERSION = 1

    def __init__(self, data_dir: str | Path):
        requested = Path(data_dir).expanduser()
        if requested.is_symlink():
            raise MemoryError("Data directory cannot be a symlink")
        self.data_dir = requested.resolve()
        self.workspace = self.data_dir / "workspace"
        self.archives = self.data_dir / "archives"
        self.batches = self.data_dir / "summary-batches"
        self.state = self.data_dir / "memory-state"
        for path in (self.data_dir, self.workspace, self.archives, self.batches, self.state):
            _mkdir(path)
        self.index_path = self.state / "sources.sqlite3"
        with self._lock():
            self._initialize_index()
            self._recover_commits()
            self._recover_events()

    @contextlib.contextmanager
    def _lock(self) -> Iterator[None]:
        path = _join(self.state, "write.lock")
        fd = os.open(path, os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    @contextlib.contextmanager
    def _db(self) -> Iterator[sqlite3.Connection]:
        _join(self.state, "sources.sqlite3")
        db = None
        try:
            db = sqlite3.connect(self.index_path, timeout=20)
            db.row_factory = sqlite3.Row
            with db:
                yield db
        except sqlite3.Error as exc:
            raise MemoryError("Memory source index operation failed") from exc
        finally:
            if db is not None:
                db.close()

    def _initialize_index(self) -> None:
        _join(self.state, "sources.sqlite3")
        fd = os.open(self.index_path, os.O_CREAT | os.O_RDWR, 0o600)
        os.close(fd)
        self.index_path.chmod(0o600)
        with self._db() as db:
            version = db.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, self.SCHEMA_VERSION):
                raise MemoryError("Unsupported memory index schema version; original preserved")
            if db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise MemoryError("Memory index integrity check failed")
            tables = {
                row[0]
                for row in db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                )
            }
            if version == self.SCHEMA_VERSION and not tables:
                raise MemoryError("Memory index schema is incomplete")
            db.execute("BEGIN IMMEDIATE")
            if not tables:
                db.execute("""CREATE TABLE sources (
                    source_id TEXT PRIMARY KEY, namespace TEXT NOT NULL,
                    relative_path TEXT NOT NULL, stored_path TEXT NOT NULL,
                    file_sha256 TEXT NOT NULL, line_number INTEGER NOT NULL,
                    kind TEXT NOT NULL, timestamp TEXT, window_start TEXT,
                    window_end TEXT, source_l0 TEXT, parse_error TEXT,
                    content TEXT NOT NULL, indexed_truncated INTEGER NOT NULL DEFAULT 0
                )""")
                db.execute("CREATE INDEX sources_namespace ON sources(namespace)")
                db.execute("CREATE INDEX sources_kind_window ON sources(kind,window_start)")
                db.execute("CREATE TABLE indexed_snapshots (snapshot_id TEXT PRIMARY KEY)")
                tables = {"sources", "indexed_snapshots"}
            expected = [
                "source_id",
                "namespace",
                "relative_path",
                "stored_path",
                "file_sha256",
                "line_number",
                "kind",
                "timestamp",
                "window_start",
                "window_end",
                "source_l0",
                "parse_error",
                "content",
                "indexed_truncated",
            ]
            columns = list(db.execute("PRAGMA table_info(sources)"))
            source_shape = [(row[1], row[2], row[5]) for row in columns]
            expected_shape = [
                (
                    name,
                    "INTEGER" if name in {"line_number", "indexed_truncated"} else "TEXT",
                    int(name == "source_id"),
                )
                for name in expected
            ]
            snapshot_shape = [
                (row[1], row[2], row[5])
                for row in db.execute("PRAGMA table_info(indexed_snapshots)")
            ]
            indices = {row[1] for row in db.execute("PRAGMA index_list(sources)")}
            if (
                tables != {"sources", "indexed_snapshots"}
                or source_shape != expected_shape
                or snapshot_shape != [("snapshot_id", "TEXT", 1)]
                or not {"sources_namespace", "sources_kind_window"} <= indices
            ):
                raise MemoryError("Unknown memory index schema; explicit migration required")
            # Version 0 is accepted only after validating the exact earlier
            # table layout. Adding the version stamp preserves all source rows.
            if version == 0:
                db.execute(f"PRAGMA user_version={self.SCHEMA_VERSION}")

    def install_workspace_templates(self) -> dict:
        """Install generic runtime guidance once; preserve local customizations."""
        mappings = {
            "AGENTS.md": "AGENTS.md",
            "autonomy-review.md": ".alice/prompts/autonomy-review.md",
        }
        package = resources.files("alice_codex").joinpath("templates")
        skill_root = package.joinpath(".agents", "skills")
        pending = [(skill_root, ".agents/skills")] if skill_root.is_dir() else []
        while pending:
            directory, prefix = pending.pop()
            for resource in sorted(directory.iterdir(), key=lambda item: item.name):
                rel = _relative(prefix + "/" + resource.name)
                if getattr(resource, "is_symlink", lambda: False)():
                    raise MemoryError("Skill template symlinks are not allowed")
                if resource.is_dir():
                    pending.append((resource, rel))
                elif resource.is_file():
                    mappings[rel] = rel
        installed, preserved = [], []
        with self._lock():
            for resource, rel in mappings.items():
                target = _join(self.workspace, rel)
                content = package.joinpath(resource).read_bytes()
                if target.exists():
                    if target.read_bytes() != content:
                        preserved.append(rel)
                    continue
                _atomic(target, content)
                installed.append(rel)
        return {
            "workspace_path": str(self.workspace),
            "installed": installed,
            "preserved": preserved,
        }

    def snapshot_legacy(
        self,
        source_root: str | Path,
        *,
        snapshot_id: str | None = None,
        previous_snapshot_id: str | None = None,
        final: bool = False,
        include_logs: bool = False,
        seed_workspace: bool = True,
        source_roots: Mapping[str, str | Path] | None = None,
    ) -> dict[str, Any]:
        """Publish an immutable private snapshot, then index/three-way seed it.

        Explicit mappings are archive-relative prefix -> local source file/dir.
        They add to the fixed workspace allowlist. No credential files, symlinks,
        native auth databases, or API configuration are selected. Log *content*
        is deliberately not redacted: private raw evidence must retain hashes.
        Non-final snapshots can describe cross-file changes; final snapshots
        fail closed if the inventory changes while collecting it.
        """
        source_root = Path(source_root).expanduser().resolve()
        if not source_root.is_dir():
            raise MemoryError("Legacy workspace must be an existing directory")
        roots = {rel: source_root / rel for rel in DEFAULT_SOURCES}
        if include_logs:
            roots[".runtime/nanobot-anima/logs"] = source_root / ".runtime/nanobot-anima/logs"
        for rel, path in (source_roots or {}).items():
            rel = _relative(rel)
            if rel in roots:
                raise MemoryError("Explicit source mapping overlaps a default source")
            roots[rel] = Path(path).expanduser().absolute()
            if not roots[rel].exists() and not roots[rel].is_symlink():
                raise MemoryError("Explicit archive source does not exist")
        for path in roots.values():
            # Default allowlist entries may begin below a symlinked parent
            # without the leaf itself being a symlink. Do not traverse those.
            if path != source_root and path.is_relative_to(source_root):
                parent = path.parent
                while parent != source_root:
                    if parent.is_symlink():
                        raise MemoryError("Archive source parent cannot be a symlink")
                    parent = parent.parent
            resolved = path.resolve()
            if resolved.is_relative_to(self.data_dir) or resolved in self.data_dir.parents:
                raise MemoryError("Archive input and output directories must be separate")
        snapshot_id = (
            snapshot_id
            or dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ-") + uuid.uuid4().hex[:8]
        )
        if not _ID.fullmatch(snapshot_id):
            raise MemoryError("Invalid snapshot identifier")
        destination = _join(self.archives, snapshot_id)
        source_mapping = {rel: str(path) for rel, path in roots.items()}
        with self._lock():
            if destination.exists():
                manifest = self._load_manifest(snapshot_id)
                if (
                    manifest["source_root"] != str(source_root)
                    or manifest["final"] != final
                    or manifest["previous_snapshot_id"] != previous_snapshot_id
                ):
                    raise MemoryConflictError(
                        "Snapshot identifier already belongs to another request"
                    )
                if manifest["source_mapping"] != source_mapping:
                    raise MemoryConflictError(
                        "Snapshot identifier already belongs to different source mappings"
                    )
                self._verify_snapshot(manifest, destination)
                self._index_snapshot(manifest, destination)
                previous = (
                    self._load_manifest(previous_snapshot_id) if previous_snapshot_id else None
                )
                conflicts = (
                    self._seed(manifest, destination, previous)
                    if seed_workspace and manifest["consistent"]
                    else []
                )
                return self._snapshot_result(manifest, destination, conflicts)
            previous = self._load_manifest(previous_snapshot_id) if previous_snapshot_id else None
            previous_files = {f["path"]: f for f in previous["files"]} if previous else {}
            selected, exclusions = _inventory(roots)
            before = {rel: _fingerprint(path) for rel, path in selected.items()}
            staging = Path(tempfile.mkdtemp(prefix=".snapshot-", dir=self.archives))
            try:
                entries = []
                for rel, path in selected.items():
                    target = _join(staging / "files", rel)
                    old = previous_files.get(rel)
                    unchanged = False
                    if old and old["size"] == before[rel][2]:
                        digest = _hash_file(path, before[rel][2])
                        if before[rel] != _fingerprint(path):
                            raise SourceChangedError("Source changed while checking increment")
                        unchanged = digest == old["sha256"]
                    if unchanged:
                        prior_file = _join(
                            _join(self.archives, previous_snapshot_id) / "files", rel
                        )
                        if _hash_file(prior_file) != old["sha256"]:
                            raise MemoryError("Previous snapshot integrity check failed")
                        _mkdir(target.parent)
                        os.link(prior_file, target)
                        details = {
                            "sha256": digest,
                            "size": before[rel][2],
                            "fingerprint": list(before[rel]),
                        }
                    else:
                        details = _stable_copy(path, target)
                    entry = {"path": rel, **details, "source_path": str(path)}
                    entry["change"] = (
                        "unchanged"
                        if old and old["sha256"] == details["sha256"]
                        else "changed"
                        if old
                        else "added"
                    )
                    entries.append(entry)
                after, _ = _inventory(roots)
                changed = sorted(
                    rel
                    for rel in set(before) | set(after)
                    if rel not in before
                    or rel not in after
                    or before[rel] != _fingerprint(after[rel])
                )
                if final and changed:
                    raise SourceChangedError(
                        "Final snapshot source inventory changed; stop old writers and retry"
                    )
                manifest = {
                    "schema_version": 1,
                    "snapshot_id": snapshot_id,
                    "previous_snapshot_id": previous_snapshot_id,
                    "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "source_root": str(source_root),
                    "final": final,
                    "source_mapping": source_mapping,
                    "consistent": not changed,
                    "changed_during_snapshot": changed,
                    "files": entries,
                    "excluded": exclusions,
                    "removed_since_previous": sorted(set(previous_files) - set(selected)),
                    "file_count": len(entries),
                    "total_bytes": sum(f["size"] for f in entries),
                }
                manifest["manifest_sha256"] = _digest(_json(manifest))
                _atomic(staging / "manifest.json", _json(manifest))
                os.replace(staging, destination)
                _fsync_dir(self.archives)
            finally:
                if staging.exists():
                    shutil.rmtree(staging)
            self._index_snapshot(manifest, destination)
            conflicts = (
                self._seed(manifest, destination, previous)
                if seed_workspace and manifest["consistent"]
                else []
            )
            return self._snapshot_result(manifest, destination, conflicts)

    def _snapshot_result(self, manifest: dict, destination: Path, conflicts: list) -> dict:
        return {
            "snapshot_id": manifest["snapshot_id"],
            "manifest_path": str(destination / "manifest.json"),
            "manifest_sha256": manifest["manifest_sha256"],
            "workspace_path": str(self.workspace),
            "file_count": manifest["file_count"],
            "total_bytes": manifest["total_bytes"],
            "consistent": manifest["consistent"],
            "final": manifest["final"],
            "excluded_count": len(manifest["excluded"]),
            "workspace_conflicts": conflicts,
        }

    def _load_manifest(self, snapshot_id: str | None) -> dict:
        if not snapshot_id or not _ID.fullmatch(snapshot_id):
            raise MemoryError("Invalid snapshot identifier")
        path = _join(self.archives, snapshot_id + "/manifest.json")
        manifest = json.loads(path.read_text())
        if manifest.get("schema_version") != self.SCHEMA_VERSION:
            raise MemoryError("Unsupported snapshot schema version; original preserved")
        check = dict(manifest)
        expected = check.pop("manifest_sha256")
        if expected != _digest(_json(check)) or manifest["snapshot_id"] != snapshot_id:
            raise MemoryError("Snapshot manifest integrity check failed")
        return manifest

    def _verify_snapshot(self, manifest: dict, destination: Path) -> None:
        for entry in manifest["files"]:
            if _hash_file(_join(destination / "files", entry["path"])) != entry["sha256"]:
                raise MemoryError("Snapshot content integrity check failed")

    def _index_snapshot(self, manifest: dict, destination: Path) -> None:
        namespace = manifest["snapshot_id"]
        with self._db() as db:
            if db.execute(
                "SELECT 1 FROM indexed_snapshots WHERE snapshot_id=?", (namespace,)
            ).fetchone():
                return
            for entry in manifest["files"]:
                rel = entry["path"]
                path = _join(destination / "files", rel)
                self._index_file(db, namespace, rel, path, entry["sha256"])
            db.execute("INSERT INTO indexed_snapshots VALUES (?)", (namespace,))

    def _index_file(
        self, db: sqlite3.Connection, namespace: str, rel: str, path: Path, digest: str
    ) -> None:
        for line, text, obj, error in _records(path):
            metadata = obj or {}
            content = metadata.get("content", text)
            if not isinstance(content, str):
                content = json.dumps(content, ensure_ascii=False)
            # Bounded index previews keep giant observe payloads from doubling
            # archive size. read_source reads original bytes and reports limits.
            db.execute(
                """INSERT OR IGNORE INTO sources VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    _source_id(namespace, rel, digest, line),
                    namespace,
                    rel,
                    str(path),
                    digest,
                    line,
                    _kind(rel),
                    _metadata_text(metadata.get("timestamp")),
                    _metadata_text(metadata.get("time_start")),
                    _metadata_text(metadata.get("time_end")),
                    _metadata_text(metadata.get("source_l0")),
                    error,
                    content[:65536],
                    int(len(content) > 65536),
                ),
            )

    def _seed(self, manifest: dict, destination: Path, previous: dict | None) -> list[dict]:
        seed_state = _join(self.state, "last-seed.json")
        seeded_previous = seed_state.exists()
        baseline = None
        if seeded_previous:
            last = json.loads(seed_state.read_text())
            if last["snapshot_id"] == manifest["snapshot_id"]:
                # Recovery of indexing is allowed; replaying a completed seed
                # must never resurrect deleted notes or overwrite newer data.
                return last["conflicts"]
            ancestor, seen = previous, set()
            while ancestor is not None and ancestor["snapshot_id"] != last["snapshot_id"]:
                if ancestor["snapshot_id"] in seen:
                    raise MemoryConflictError("Snapshot ancestry contains a cycle")
                seen.add(ancestor["snapshot_id"])
                parent_id = ancestor["previous_snapshot_id"]
                ancestor = self._load_manifest(parent_id) if parent_id else None
            if ancestor is None:
                raise MemoryConflictError("Workspace seed must extend the latest seeded snapshot")
            # Archive-only increments advance archival ancestry, not the
            # workspace merge base. Compare against the last actual seed.
            baseline = ancestor
        old = {e["path"]: e for e in baseline["files"]} if baseline else {}
        conflicts = []
        for entry in manifest["files"]:
            rel = entry["path"]
            if rel not in {"SOUL.md", "USER.md", "memory/MEMORY.md"} and not rel.startswith(
                ("memory/notebook/", "memory/chronicle/")
            ):
                continue
            target = _join(self.workspace, rel)
            actual = None
            if not target.exists() and rel in old and seeded_previous:
                if old[rel]["sha256"] != entry["sha256"]:
                    conflicts.append({"path": rel, "reason": "workspace_deleted_source_changed"})
                continue
            if target.exists():
                actual = _hash_file(target)
                if actual == entry["sha256"]:
                    continue
                if rel not in old or actual != old[rel]["sha256"]:
                    if rel not in old or old[rel]["sha256"] != entry["sha256"]:
                        conflicts.append(
                            {
                                "path": rel,
                                "reason": "both_changed"
                                if rel in old
                                else "existing_workspace_file",
                            }
                        )
                    continue
            _atomic_copy(_join(destination / "files", rel), target, entry["sha256"], actual)
        removed = sorted(set(old) - {entry["path"] for entry in manifest["files"]})
        for rel in removed:
            target = _join(self.workspace, rel)
            if target.is_file() and _hash_file(target) != old[rel]["sha256"]:
                conflicts.append({"path": rel, "reason": "source_deleted_workspace_changed"})
        _atomic(
            self.state / "last-seed.json",
            _json({"snapshot_id": manifest["snapshot_id"], "conflicts": conflicts}),
        )
        return conflicts

    def search(self, query: str, *, limit: int = 20, kind: str | None = None) -> list[dict]:
        if not query.strip() or not 1 <= limit <= 100:
            raise MemoryError("A query and limit between 1 and 100 are required")
        escaped = query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        sql = "SELECT source_id,namespace,relative_path,line_number,kind,timestamp,window_start,window_end,parse_error,indexed_truncated FROM sources WHERE content LIKE ? ESCAPE '\\'"
        args: list[Any] = [f"%{escaped}%"]
        if kind:
            sql += " AND kind=?"
            args.append(kind)
        sql += " ORDER BY namespace DESC,relative_path,line_number LIMIT ?"
        args.append(limit)
        with self._db() as db:
            return [dict(r) for r in db.execute(sql, args)]

    def index_status(self) -> dict:
        """Make search limitations visible; no match is not proof of absence."""
        with self._db() as db:
            row = db.execute(
                "SELECT COUNT(*) AS source_count, COALESCE(SUM(indexed_truncated),0) AS truncated_record_count, COALESCE(SUM(parse_error IS NOT NULL),0) AS malformed_record_count FROM sources"
            ).fetchone()
            result = dict(row)
            result["namespaces"] = [
                dict(r)
                for r in db.execute(
                    "SELECT namespace,COUNT(*) AS source_count FROM sources GROUP BY namespace ORDER BY namespace"
                )
            ]
        result.update(
            index_chars_per_record=65536,
            search_scope="indexed record previews; no match does not establish absence",
        )
        return result

    def read_source(self, source_id: str, *, max_chars: int = 32768, offset_chars: int = 0) -> dict:
        if not 1 <= max_chars <= 1_000_000 or not isinstance(offset_chars, int) or offset_chars < 0:
            raise MemoryError("Invalid read limit")
        with self._db() as db:
            found = db.execute("SELECT * FROM sources WHERE source_id=?", (source_id,)).fetchone()
        if not found:
            from .summary_partitions import read_source

            fragment = read_source(self, source_id, max_chars=max_chars, offset_chars=offset_chars)
            if fragment is not None:
                return fragment
            raise MemoryError("Unknown source identifier")
        row = dict(found)
        path = Path(row["stored_path"])
        _join(self.data_dir, path.relative_to(self.data_dir).as_posix())
        if _hash_file(path) != row["file_sha256"]:
            raise MemoryError("Source archive integrity check failed")
        for line, text, total in _record_slices(path, max_chars, offset_chars):
            if line == row["line_number"]:
                result = {
                    k: row[k]
                    for k in (
                        "source_id",
                        "namespace",
                        "relative_path",
                        "line_number",
                        "kind",
                        "timestamp",
                        "parse_error",
                    )
                }
                end = offset_chars + max_chars
                result.update(
                    content=text,
                    truncated=total > end,
                    offset_chars=offset_chars,
                    total_chars=total,
                    next_offset=end if total > end else None,
                )
                return result
        raise MemoryError("Source record is missing")

    def resolve_legacy_reference(self, source_id: str) -> list[dict]:
        """Resolve legacy L0/L1/L2/W citations, preserving all source versions.

        An empty result means unresolved/missing, not validated. Timestamp
        citations are only locators and do not establish claim correctness.
        """
        with self._db() as db:
            row = db.execute("SELECT * FROM sources WHERE source_id=?", (source_id,)).fetchone()
            if not row:
                return []
            root = row["relative_path"].split("/" + row["kind"] + "/", 1)[0]
            columns = "source_id,relative_path,line_number,timestamp,window_start,window_end"
            result: dict[str, dict] = {}
            if row["source_l0"]:
                rel = _relative(row["source_l0"])
                if not rel.startswith(root + "/"):
                    rel = root + "/" + rel
                for found in db.execute(
                    f"SELECT {columns} FROM sources WHERE namespace=? AND relative_path=? AND (timestamp>=? AND timestamp<? OR timestamp IS NULL) ORDER BY line_number",
                    (row["namespace"], rel, row["window_start"], row["window_end"]),
                ):
                    result[found["source_id"]] = dict(found)
            for kind, period in re.findall(r"\[(L1|L2|W):\s*([^\]]+)\]", row["content"]):
                if kind == "L1":
                    date = Path(row["relative_path"]).stem
                    match = re.fullmatch(r"(\d{2}:\d{2})[–-](\d{2}:\d{2})", period.strip())
                    if not match:
                        continue
                    rel = root + "/hourly/" + date + ".jsonl"
                    found_rows = db.execute(
                        f"SELECT {columns} FROM sources WHERE namespace=? AND relative_path=? AND substr(window_start,12,5)=? AND substr(window_end,12,5)=? ORDER BY line_number",
                        (row["namespace"], rel, *match.groups()),
                    )
                else:
                    pattern = r"\d{4}-\d{2}-\d{2}" if kind == "L2" else r"\d{4}-W\d{2}"
                    if not re.fullmatch(pattern, period.strip()):
                        continue
                    rel = (
                        root + ("/diary/" if kind == "L2" else "/weekly/") + period.strip() + ".md"
                    )
                    found_rows = db.execute(
                        f"SELECT {columns} FROM sources WHERE namespace=? AND relative_path=? ORDER BY line_number",
                        (row["namespace"], rel),
                    )
                for found in found_rows:
                    result[found["source_id"]] = dict(found)
            return list(result.values())

    def append_event(
        self,
        event_id: str,
        event: Mapping[str, Any],
        *,
        timestamp: dt.datetime | str | None = None,
        timezone: str = "Asia/Shanghai",
    ) -> dict:
        """Retain one new runtime event, idempotently, as an addressable L0 record.

        The host supplies a stable transport/business event ID, not a fresh ID
        on retry. This is Alice evidence, never a fabricated native rollout.
        Each event has its own atomic file, so recording an event never rewrites
        an entire conversation. Event bodies are private, not console output.
        """
        if not isinstance(event_id, str) or not event_id or len(event_id) > 1024:
            raise MemoryError("A stable bounded event identifier is required")
        if not isinstance(event, Mapping):
            raise MemoryError("Event must be a mapping")
        value = dict(event)
        if {"history", "messages", "conversation_history", "_anima_ingest"} & value.keys():
            raise MemoryError("Record one event, not a conversation history")
        if "event_id" in value and value["event_id"] != event_id:
            raise MemoryConflictError("Event identifiers disagree")
        zone = ZoneInfo(timezone)
        supplied_time = timestamp.isoformat() if isinstance(timestamp, dt.datetime) else timestamp
        if supplied_time is not None and not isinstance(supplied_time, str):
            raise MemoryError("Event timestamp must be ISO text or a datetime")
        request_hash = _digest(
            _json({"event": value, "timestamp": supplied_time, "timezone": timezone})
        )
        name = _digest(event_id.encode())
        source = _join(self.state, f"events/raw/{name}.jsonl")
        with self._lock():
            existed = source.exists()
            if existed:
                record = self._load_event(source)
                if (
                    record["event_id"] != event_id
                    or record["_anima_ingest"]["request_sha256"] != request_hash
                ):
                    raise MemoryConflictError("Stable event ID was reused with different content")
            else:
                raw_time = supplied_time or value.get("timestamp")
                try:
                    event_time = (
                        dt.datetime.fromisoformat(raw_time)
                        if raw_time
                        else dt.datetime.now(dt.timezone.utc)
                    )
                except (TypeError, ValueError) as exc:
                    raise MemoryError("Event timestamp is invalid") from exc
                if event_time.tzinfo is None:
                    event_time = event_time.replace(tzinfo=zone)
                record = {
                    **value,
                    "event_id": event_id,
                    "timestamp": event_time.isoformat(),
                    "_anima_ingest": {
                        "schema_version": 1,
                        "request_sha256": request_hash,
                        "timezone": timezone,
                    },
                }
                raw = (json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n").encode()
                if len(raw) > 16 * 1024 * 1024:
                    raise MemoryError(
                        "Event exceeds 16 MiB; record explicit chunks with stable IDs"
                    )
                _atomic(source, raw)
            result = self._materialize_event(source, record)
            result["already_recorded"] = existed
            return result

    def _load_event(self, source: Path) -> dict:
        try:
            record = json.loads(source.read_text())
            metadata = record["_anima_ingest"]
            if (
                metadata["schema_version"] != 1
                or _digest(record["event_id"].encode()) != source.stem
            ):
                raise ValueError()
            if not re.fullmatch(r"[a-f0-9]{64}", metadata["request_sha256"]):
                raise ValueError()
            return record
        except (ValueError, TypeError, KeyError, AttributeError) as exc:
            raise MemoryError("Event archive is invalid") from exc

    def _materialize_event(self, source: Path, record: dict) -> dict:
        day = (
            dt.datetime.fromisoformat(record["timestamp"])
            .astimezone(ZoneInfo(record["_anima_ingest"]["timezone"]))
            .date()
        )
        rel = f"memory/chronicle/traces/{day.isoformat()}/{source.name}"
        target = _join(self.workspace, rel)
        digest = _hash_file(source)
        receipt_path = _join(self.state, f"events/receipts/{source.stem}.json")
        if receipt_path.exists():
            receipt = json.loads(receipt_path.read_text())
            if receipt["sha256"] != digest:
                raise MemoryError("Event archive integrity check failed")
        if target.exists():
            if _hash_file(target) != digest:
                raise MemoryConflictError("Recorded L0 event has a conflicting workspace edit")
        else:
            _atomic(target, source.read_bytes())
        with self._db() as db:
            self._index_file(db, "events", rel, source, digest)
        result = {
            "event_id": record["event_id"],
            "source_id": _source_id("events", rel, digest, 1),
            "path": str(target),
            "sha256": digest,
        }
        _atomic(receipt_path, _json(result))
        return result

    def _recover_events(self) -> None:
        root = _join(self.state, "events/raw")
        if not root.exists():
            return
        for source in sorted(root.glob("*.jsonl")):
            _join(root, source.name)
            receipt = _join(self.state, f"events/receipts/{source.stem}.json")
            if not receipt.exists():
                self._materialize_event(source, self._load_event(source))

    def prepare_summary(
        self,
        level: str,
        period: str,
        *,
        now: dt.datetime | None = None,
        timezone: str = "Asia/Shanghai",
    ) -> dict:
        """Freeze a closed window, automatically partitioning inputs beyond one worker."""
        try:
            return self._prepare_legacy_summary(level, period, now=now, timezone=timezone)
        except _PartitionRequired:
            from .summary_partitions import prepare

            return prepare(self, level, period, now=now, timezone=timezone)

    def summary_partition_next(self, batch_id: str, limit: int = 4) -> dict:
        """Return bounded ready work descriptors; repeated queries do not claim work."""
        from .summary_partitions import next_nodes

        return next_nodes(self, batch_id, limit=limit)

    def commit_summary_partition(self, batch_id: str, node_id: str, candidate: dict) -> dict:
        """Validate one immutable work unit and finalize only after full root coverage."""
        from .summary_partitions import commit_node

        return commit_node(self, batch_id, node_id, candidate)

    def _prepare_legacy_summary(
        self,
        level: str,
        period: str,
        *,
        now: dt.datetime | None = None,
        timezone: str = "Asia/Shanghai",
    ) -> dict:
        """Freeze closed-period source files and return a task for Codex.

        L1 period: YYYY-MM-DDTHH:00 (an even hour); L2: YYYY-MM-DD;
        L3: YYYY-Www; L4: YYYY-MM. Only structural validation is automated.
        """
        start, end = _period_bounds(level, period, timezone)
        current = now or dt.datetime.now(dt.timezone.utc)
        if current.tzinfo is None:
            raise SummaryValidationError("now must include a timezone")
        if end > current:
            raise SummaryValidationError("Summary period has not closed")
        lower = {"L1": "traces", "L2": "hourly", "L3": "diary", "L4": "weekly"}[level]
        root = _join(self.workspace, "memory/chronicle/" + lower)

        def inventory() -> dict[str, tuple[int, int, int, int, int]]:
            found = {}
            for path in sorted(root.rglob("*")):
                mode = path.lstat().st_mode
                if stat.S_ISLNK(mode):
                    raise SummaryValidationError("Summary source symlinks are not allowed")
                if stat.S_ISDIR(mode):
                    continue
                if not stat.S_ISREG(mode):
                    raise SummaryValidationError("Summary source is not a regular file")
                rel = path.relative_to(self.workspace).as_posix()
                found[rel] = _fingerprint(_join(self.workspace, rel))
            return found

        def dated_path(path: Path) -> bool:
            labels = (path.stem, path.parent.name) if level in {"L1", "L2"} else (path.stem,)
            for label in labels:
                try:
                    if level == "L4":
                        _period_bounds("L3", label, timezone)
                        return True
                    if dt.date.fromisoformat(label).isoformat() == label:
                        return True
                except (ValueError, SummaryValidationError):
                    continue
            return False

        with self._lock():
            try:
                initial = inventory()
                selected: list[dict] = []
                files: dict[str, dict] = {}
                total_bytes = 0
                for rel, before in initial.items():
                    path = _join(self.workspace, rel)
                    digest = _hash_file(path)
                    for line, text, obj, error in _records(path):
                        eligible = _in_period(level, path, obj, start, end)
                        if error == "record_exceeds_parse_limit":
                            # Its valid timestamp might follow a giant string.
                            # The streaming scanner must decide eligibility.
                            raise _PartitionRequired("Summary record needs automatic partitioning")
                        if (
                            level in {"L1", "L2"}
                            and not _valid_record_time(level, obj)
                            and not dated_path(path)
                        ):
                            raise SummaryValidationError(
                                "Summary source has no valid timestamp or recognizable file date"
                            )
                        if not eligible:
                            continue
                        from .summary_partitions import embedded_references

                        if embedded_references(self, path, obj=obj):
                            raise _PartitionRequired(
                                "Embedded partition coverage requires the partition protocol"
                            )
                        source_id = _source_id("workspace", rel, digest, line)
                        if level in {"L1", "L2"} and not _valid_record_time(level, obj):
                            error = error or "missing_or_invalid_timestamp"
                        selected.append(
                            {
                                "source_id": source_id,
                                "path": rel,
                                "line": line,
                                "parse_error": error,
                            }
                        )
                        total_bytes += len(text.encode())
                        files[rel] = {"path": rel, "sha256": digest, "size": before[2]}
                        if len(selected) > 64 or total_bytes > 128 * 1024 or len(text.encode()) > 65536:
                            raise _PartitionRequired(
                                "Summary batch needs automatic partitioning; no sources were silently truncated"
                            )
                    if before != _fingerprint(path):
                        raise SourceChangedError("Summary input changed while selecting records")
                if inventory() != initial:
                    raise SourceChangedError("Summary source inventory changed while selecting")
                if not selected:
                    raise NoSourcesError("No source records cover this period")
                if level != "L1":
                    from .summary_partitions import has_upstream

                    if has_upstream(self, files):
                        raise _PartitionRequired("Upstream partition coverage requires the partition protocol")
                manifest = {
                    "schema_version": 1,
                    "level": level,
                    "period": period,
                    "timezone": timezone,
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "prompt_version": "chronicle-v1",
                    "files": list(files.values()),
                    "sources": selected,
                    "target": _summary_target(level, period),
                }
                batch_id = _digest(_json(manifest))
                manifest["batch_id"] = batch_id
                directory = _join(self.batches, batch_id)
                if not directory.exists():
                    staging = Path(tempfile.mkdtemp(prefix=".batch-", dir=self.batches))
                    try:
                        for entry in manifest["files"]:
                            info = _stable_copy(
                                _join(self.workspace, entry["path"]),
                                _join(staging / "files", entry["path"]),
                            )
                            if info["sha256"] != entry["sha256"]:
                                raise SourceChangedError("Summary input changed before freezing")
                        _atomic(staging / "manifest.json", _json(manifest))
                        if inventory() != initial:
                            raise SourceChangedError(
                                "Summary source inventory changed while freezing"
                            )
                        os.replace(staging, directory)
                        _fsync_dir(self.batches)
                    finally:
                        if staging.exists():
                            shutil.rmtree(staging)
                else:
                    if json.loads(_join(directory, "manifest.json").read_text()) != manifest:
                        raise SummaryValidationError("Frozen batch manifest integrity check failed")
                    for entry in manifest["files"]:
                        if _hash_file(_join(directory / "files", entry["path"])) != entry["sha256"]:
                            raise SourceChangedError("Frozen batch integrity check failed")
                    if inventory() != initial:
                        raise SourceChangedError("Summary source inventory changed while freezing")
                with self._db() as db:
                    for entry in manifest["files"]:
                        self._index_file(
                            db,
                            "workspace",
                            entry["path"],
                            _join(directory / "files", entry["path"]),
                            entry["sha256"],
                        )
            except FileNotFoundError as exc:
                raise SourceChangedError("A summary source disappeared while freezing") from exc
        # The worker may write inside --cd under workspace-write. Frozen
        # evidence stays outside that writable root; the host commits via MCP.
        candidate_path = _join(self.workspace, f".alice/candidates/{batch_id}.json")
        _mkdir(candidate_path.parent)
        prompt = _summary_prompt(manifest, directory / "manifest.json", candidate_path)
        return {
            "batch_id": batch_id,
            "manifest_path": str(directory / "manifest.json"),
            "candidate_path": str(candidate_path),
            "prompt": prompt,
            "source_count": len(selected),
            "workspace_path": str(self.workspace),
        }

    def commit_summary(self, batch_id: str, candidate: dict) -> dict:
        if not re.fullmatch(r"[a-f0-9]{64}", batch_id):
            raise SummaryValidationError("Invalid batch identifier")
        if _join(self.state, "summary-partitions/" + batch_id).exists():
            raise SummaryValidationError("Partition plans require commit_summary_partition for each ready node")
        directory = _join(self.batches, batch_id)
        manifest = json.loads(_join(directory, "manifest.json").read_text())
        if manifest.get("schema_version") != 1:
            raise SummaryValidationError("Unsupported summary batch schema version")
        check = dict(manifest)
        if check.pop("batch_id") != batch_id or _digest(_json(check)) != batch_id:
            raise SummaryValidationError("Batch manifest integrity check failed")
        _validate_candidate(manifest, candidate)
        with self._lock():
            self._recover_commits()
            intent_path = _join(self.state, f"commits/{batch_id}.json")
            candidate_hash = _digest(_json(candidate))
            if intent_path.exists():
                intent = json.loads(intent_path.read_text())
                if intent["candidate_sha256"] != candidate_hash:
                    raise MemoryConflictError(
                        "This source version already has a different committed summary"
                    )
                return {
                    "batch_id": batch_id,
                    "target_path": str(_join(self.workspace, intent["target"])),
                    "already_committed": True,
                }
            for entry in manifest["files"]:
                current = _join(self.workspace, entry["path"])
                # A later append does not invalidate the frozen earlier prefix.
                try:
                    current_hash = _hash_file(current, entry["size"])
                    frozen_hash = _hash_file(_join(directory / "files", entry["path"]))
                except FileNotFoundError as exc:
                    raise SourceChangedError("A frozen summary source is missing") from exc
                if current_hash != entry["sha256"]:
                    raise SourceChangedError("A frozen summary source was modified")
                if frozen_hash != entry["sha256"]:
                    raise SourceChangedError("Frozen batch integrity check failed")
            target = _join(self.workspace, manifest["target"])
            before = target.read_bytes() if target.exists() else None
            rendered = _render_summary(manifest, candidate, before)
            intent = {
                "schema_version": 1,
                "batch_id": batch_id,
                "status": "pending",
                "target": manifest["target"],
                "candidate_sha256": candidate_hash,
                "before_sha256": _digest(before) if before is not None else None,
                "after_sha256": _digest(rendered),
                "rendered": rendered.decode(),
                "candidate": candidate,
                "manifest": manifest,
            }
            _atomic(intent_path, _json(intent))
            # Reconcile the target again after the durable intent. A manual
            # edit during that write must receive the same conflict protection
            # as an edit made during a process interruption.
            self._recover_commits()
            return {"batch_id": batch_id, "target_path": str(target), "already_committed": False}

    def _recover_commits(self) -> None:
        commits = _join(self.state, "commits")
        if not commits.exists():
            return
        for path in sorted(commits.glob("*.json")):
            _join(commits, path.name)
            intent = json.loads(path.read_text())
            version = intent.get("schema_version")
            if version == 2:
                from .summary_partitions import recover_plan, validate_summary_commit_header

                try:
                    validate_summary_commit_header(intent)
                    recover_plan(self, intent)
                except SummaryValidationError as exc:
                    raise MemoryConflictError(str(exc)) from exc
                if intent["status"] == "partitioning":
                    continue
            elif version != 1:
                raise MemoryConflictError("Unsupported summary commit schema version")
            if intent.get("manifest", {}).get("schema_version") != version:
                raise MemoryConflictError("Unsupported summary batch schema version in commit")
            if intent.get("status") not in {"pending", "committed"}:
                raise MemoryConflictError("Summary commit has an invalid status")
            if intent["status"] != "pending":
                continue
            target = _join(self.workspace, intent["target"])
            actual = _hash_file(target) if target.exists() else None
            after = intent["rendered"].encode()
            if _digest(after) != intent["after_sha256"]:
                raise MemoryConflictError("Pending summary commit is corrupt")
            if actual == intent["before_sha256"]:
                _atomic(target, after)
            elif actual != intent["after_sha256"]:
                raise MemoryConflictError("Interrupted summary commit conflicts with a later edit")
            intent["status"] = "committed"
            _atomic(path, _json(intent))


def _period_bounds(level: str, period: str, timezone: str) -> tuple[dt.datetime, dt.datetime]:
    zone = ZoneInfo(timezone)
    try:
        if level == "L1":
            start = dt.datetime.strptime(period, "%Y-%m-%dT%H:%M")
            if start.hour % 2 or start.minute:
                raise ValueError()
            end = start + dt.timedelta(hours=2)
        elif level == "L2":
            start = dt.datetime.strptime(period, "%Y-%m-%d")
            end = start + dt.timedelta(days=1)
        elif level == "L3":
            start = dt.datetime.strptime(period + "-1", "%G-W%V-%u")
            if start.strftime("%G-W%V") != period:
                raise ValueError()
            end = start + dt.timedelta(days=7)
        elif level == "L4":
            start = dt.datetime.strptime(period, "%Y-%m")
            end = (
                start.replace(year=start.year + 1, month=1)
                if start.month == 12
                else start.replace(month=start.month + 1)
            )
        else:
            raise ValueError()
    except ValueError as exc:
        raise SummaryValidationError("Invalid Chronicle level or period") from exc
    return start.replace(tzinfo=zone), end.replace(tzinfo=zone)


def _in_period(
    level: str, path: Path, obj: dict | None, start: dt.datetime, end: dt.datetime
) -> bool:
    try:
        if level in {"L1", "L2"}:
            if not _valid_record_time(level, obj):
                # Preserve malformed rows as explicit missing/unknown evidence
                # in the relevant day's batch rather than pretending they vanish.
                return (
                    path.stem == start.date().isoformat()
                    or path.parent.name == start.date().isoformat()
                )
            raw = obj.get("timestamp" if level == "L1" else "time_start")
            if not raw:
                return (
                    path.stem == start.date().isoformat()
                    or path.parent.name == start.date().isoformat()
                )
            value = dt.datetime.fromisoformat(raw)
            if value.tzinfo is None:
                value = value.replace(tzinfo=start.tzinfo)
            return start <= value < end
        if level == "L3":
            value = dt.datetime.strptime(path.stem, "%Y-%m-%d").replace(tzinfo=start.tzinfo)
            return start <= value < end
        value = dt.datetime.strptime(path.stem + "-1", "%G-W%V-%u").replace(tzinfo=start.tzinfo)
        return value < end and value + dt.timedelta(days=7) > start
    except (ValueError, TypeError):
        return False


def _valid_record_time(level: str, obj: dict | None) -> bool:
    try:
        if not obj:
            return False
        dt.datetime.fromisoformat(obj["timestamp" if level == "L1" else "time_start"])
        return True
    except (ValueError, TypeError, KeyError):
        return False


def _metadata_text(value: Any) -> str | None:
    if value is None or isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _summary_target(level: str, period: str) -> str:
    if level == "L1":
        return f"memory/chronicle/hourly/{period[:10]}.jsonl"
    folder = {"L2": "diary", "L3": "weekly", "L4": "monthly"}[level]
    return f"memory/chronicle/{folder}/{period}.md"


def _summary_prompt(manifest: dict, manifest_path: Path, candidate_path: Path) -> str:
    template = (
        resources.files("alice_codex")
        .joinpath("templates/chronicle-summary.md")
        .read_text(encoding="utf-8")
    )
    return Template(template).substitute(
        level=manifest["level"],
        start=manifest["start"],
        end=manifest["end"],
        manifest_path=manifest_path,
        candidate_path=candidate_path,
    )


def _validate_candidate(manifest: dict, candidate: dict) -> None:
    if not isinstance(candidate, dict) or set(candidate) != {
        "content",
        "source_ids",
        "covered_source_ids",
        "missing",
    }:
        raise SummaryValidationError(
            "Candidate must contain content, source_ids, covered_source_ids and missing"
        )
    content = candidate["content"]
    if not isinstance(content, str) or not content.strip() or len(content.encode()) > 128 * 1024:
        raise SummaryValidationError("Invalid candidate content")
    expected = {s["source_id"] for s in manifest["sources"]}
    for name in ("source_ids", "covered_source_ids"):
        value = candidate[name]
        if (
            not isinstance(value, list)
            or not all(isinstance(s, str) for s in value)
            or len(value) != len(set(value))
        ):
            raise SummaryValidationError("Candidate source lists must contain unique identifiers")
        if not set(value) <= expected:
            raise SummaryValidationError("Candidate cites an unknown source")
    missing = candidate["missing"]
    if not isinstance(missing, list) or not all(
        isinstance(m, dict)
        and set(m) == {"source_id", "reason"}
        and isinstance(m["source_id"], str)
        and isinstance(m["reason"], str)
        and m["reason"].strip()
        for m in missing
    ):
        raise SummaryValidationError("Invalid missing-source declarations")
    missing_ids = [m["source_id"] for m in missing]
    covered = set(candidate["covered_source_ids"])
    if (
        len(missing_ids) != len(set(missing_ids))
        or covered & set(missing_ids)
        or covered | set(missing_ids) != expected
    ):
        raise SummaryValidationError("Source coverage is incomplete or contradictory")
    cited = set(_CITATION.findall(content))
    if cited != set(candidate["source_ids"]) or (covered and not cited) or not cited <= covered:
        raise SummaryValidationError("Inline citations must match known, covered sources")
    malformed = {s["source_id"] for s in manifest["sources"] if s["parse_error"]}
    if not malformed <= set(missing_ids):
        raise SummaryValidationError("Malformed source records must be declared as missing/unknown")


def _render_summary(manifest: dict, candidate: dict, before: bytes | None) -> bytes:
    existing = before.decode("utf-8") if before else ""
    if manifest["level"] == "L1":
        record = {
            "batch_id": manifest["batch_id"],
            "time_start": manifest["start"],
            "time_end": manifest["end"],
            "content": candidate["content"],
            "source_ids": candidate["source_ids"],
            "source_manifest": manifest["batch_id"],
            "missing": candidate["missing"],
        }
        if "coverage_ref" in manifest:
            record["coverage_ref"] = manifest["coverage_ref"]
        return (
            existing
            + ("\n" if existing and not existing.endswith("\n") else "")
            + json.dumps(record, ensure_ascii=False)
            + "\n"
        ).encode()
    # Existing prose is never thrown away, including manual edits after the old
    # auto separator. New versions append; consumers can select by batch marker.
    marker = f"<!-- anima-summary:{manifest['batch_id']} -->"
    addition = f"{marker}\n\n{candidate['content'].strip()}\n"
    if "coverage_ref" in manifest:
        addition += f"\n<!-- anima-coverage:{manifest['batch_id']}:{manifest['coverage_ref']['sha256']} -->\n"
    if AUTO_SEPARATOR not in existing:
        addition = AUTO_SEPARATOR + "\n\n" + addition
    return (existing + ("\n\n" if existing else "") + addition).encode()
