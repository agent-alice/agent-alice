"""Private durable files and a process-owned singleton lock."""

import fcntl
import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Any


def private_dir(path: Path) -> Path:
    """Create a user-private directory without changing an existing parent's mode."""
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.chmod(0o700)
    return path


def atomic_write(path: Path, value: bytes, *, mode: int = 0o600) -> None:
    """Publish a fully flushed file; a failed write leaves the old file intact."""
    private_dir(path.parent)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(fd, "wb") as stream:
            os.fchmod(stream.fileno(), mode)
            stream.write(value)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        parent_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
    finally:
        temporary.unlink(missing_ok=True)


def write_json(path: Path, value: Any) -> None:
    atomic_write(path, (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode())


def read_json(path: Path) -> Any:
    """Read state strictly; corruption must not masquerade as an empty store."""
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeError, json.JSONDecodeError) as error:
        raise ValueError(f"Invalid JSON state: {path.name}; original file preserved") from error


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class SingletonLock:
    """Hold a kernel lock for the service lifetime; a stale PID file is not a lock."""

    def __init__(self, path: Path):
        self.path = path
        self.stream = None

    def acquire(self) -> None:
        private_dir(self.path.parent)
        stream = self.path.open("a+")
        os.chmod(self.path, 0o600)
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (BlockingIOError, OSError):
            stream.close()
            raise RuntimeError("Another Alice service owns this data directory") from None
        stream.seek(0)
        stream.truncate()
        stream.write(str(os.getpid()))
        stream.flush()
        self.stream = stream

    def close(self) -> None:
        if self.stream:
            fcntl.flock(self.stream, fcntl.LOCK_UN)
            self.stream.close()
            self.stream = None

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *args):
        self.close()
