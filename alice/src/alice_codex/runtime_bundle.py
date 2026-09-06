"""Pin Codex and its same-distribution Code Mode host as one runtime bundle.

This records file identity, not a claim that a tool ran successfully. Native
acceptance must exercise Code Mode through the installed pair separately.
"""

from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import shutil
import tempfile

from .files import private_dir, read_json, sha256_file, write_json


BUNDLE_VERSION = 1
BUNDLE_MANIFEST = ".alice-codex-bundle.json"
HOST_NAME = "codex-code-mode-host"


@dataclass(frozen=True)
class CodexBundle:
    binary: Path
    host: Path
    binary_sha256: str
    host_sha256: str


def _executable(path: Path, name: str) -> None:
    if not path.is_file() or not os.access(path, os.X_OK):
        raise ValueError(f"Codex {name} is missing or not executable")


def _source_host(binary: Path) -> Path:
    """Follow the native sibling/managed-package layouts, without a PATH search."""
    parent = binary.parent
    package = None
    if parent.name in {"bin", "codex-resources"}:
        package = parent.parent
    elif parent.name == "MacOS" and parent.parent.name == "Contents":
        if parent.parent.parent.name == "CodexCLI.app":
            package = parent.parent.parent.parent
    candidates = []
    source_root = parent
    if (
        package is not None
        and (package / "bin").is_dir()
        and (package / "codex-package.json").is_file()
    ):
        source_root = package
        candidates.extend((package / "codex-resources" / HOST_NAME, package / "bin" / HOST_NAME))
    candidates.append(parent / HOST_NAME)
    for path in candidates:
        if path.is_file():
            resolved = path.resolve()
            if not resolved.is_relative_to(source_root.resolve()):
                raise ValueError("Codex Code Mode host points outside the selected distribution")
            _executable(resolved, "Code Mode host")
            return resolved
    raise ValueError(
        "Selected Codex distribution has no Code Mode host; choose a complete matching "
        "distribution containing codex-code-mode-host beside Codex or in codex-resources. "
        "Alice does not substitute a host from PATH or silently disable Code Mode."
    )


def inspect_bundle(binary: Path) -> CodexBundle:
    binary = Path(binary).expanduser().resolve()
    _executable(binary, "executable")
    host = _source_host(binary)
    return CodexBundle(binary, host, sha256_file(binary), sha256_file(host))


def _verify_files(bundle: CodexBundle) -> None:
    for path, digest, name in (
        (bundle.binary, bundle.binary_sha256, "executable"),
        (bundle.host, bundle.host_sha256, "Code Mode host"),
    ):
        _executable(path, name)
        if sha256_file(path) != digest:
            raise ValueError(f"Codex {name} changed; validate a new runtime bundle")


def _manifest(bundle: CodexBundle, source: CodexBundle, version: str, kind: str) -> dict:
    return {
        "version": BUNDLE_VERSION,
        "kind": kind,
        "codex_version": version,
        "codex_binary": str(bundle.binary),
        "codex_sha256": bundle.binary_sha256,
        "codex_code_mode_host": str(bundle.host),
        "codex_code_mode_host_sha256": bundle.host_sha256,
        "source_binary": str(source.binary),
        "source_code_mode_host": str(source.host),
    }


def _reference_manifest(home: Path, binary: Path, digest: str) -> Path:
    key = hashlib.sha256((str(binary) + "\0" + digest).encode()).hexdigest()
    return home / "state/codex-bundles" / (key + ".json")


def pin_bundle(home: Path, source: CodexBundle, version: str, *, pin: bool = True) -> CodexBundle:
    """Prepare and verify the entire pair before publishing any bundle directory."""
    home = home.resolve()
    if not pin:
        _verify_files(source)
        path = _reference_manifest(home, source.binary, source.binary_sha256)
        if any(item.is_symlink() for item in (home / "state", path.parent, path)):
            raise ValueError("Codex bundle manifest must not be a symbolic link")
        private_dir(path.parent)
        expected = _manifest(source, source, version, "reference")
        if path.exists() and read_json(path) != expected:
            raise ValueError("Existing Codex bundle reference differs; original preserved")
        write_json(path, expected)
        return source
    root = home / "bin"
    if root.is_symlink():
        raise ValueError("Codex bundle directory must not be a symbolic link")
    private_dir(root)
    identity = hashlib.sha256(
        (source.binary_sha256 + "\0" + source.host_sha256).encode()
    ).hexdigest()
    destination = root / ("codex-" + identity[:24])
    bundle = CodexBundle(
        destination / "codex", destination / HOST_NAME, source.binary_sha256, source.host_sha256
    )
    expected = _manifest(bundle, source, version, "pinned")
    if destination.exists() or destination.is_symlink():
        if destination.is_symlink() or any(
            (destination / name).is_symlink() for name in ("codex", HOST_NAME, BUNDLE_MANIFEST)
        ):
            raise ValueError("Existing Codex bundle must not contain symbolic links")
        value = read_json(destination / BUNDLE_MANIFEST)
        # Source location is provenance; an identical pair may come from a
        # relocated copy of the same distribution without changing its identity.
        if not isinstance(value, dict) or any(
            value.get(key) != expected[key] for key in expected if not key.startswith("source_")
        ):
            raise ValueError("Existing Codex bundle manifest differs; original preserved")
        _verify_files(bundle)
        return bundle
    candidate = Path(tempfile.mkdtemp(prefix=".codex-candidate-", dir=root))
    try:
        for original, name in ((source.binary, "codex"), (source.host, HOST_NAME)):
            copied = candidate / name
            shutil.copyfile(original, copied)
            copied.chmod(0o700)
            with copied.open("rb") as stream:
                os.fsync(stream.fileno())
        _verify_files(
            CodexBundle(
                candidate / "codex", candidate / HOST_NAME, source.binary_sha256, source.host_sha256
            )
        )
        _verify_files(source)
        write_json(candidate / BUNDLE_MANIFEST, expected)
        candidate.rename(destination)
        root_fd = os.open(root, os.O_RDONLY)
        try:
            os.fsync(root_fd)
        finally:
            os.close(root_fd)
    finally:
        if candidate.exists():
            shutil.rmtree(candidate)
    return bundle


def verify_runtime_bundle(config, *, require: bool = False) -> dict:
    """Check the manifest selected by this config, never adopt a nearby unknown host."""
    binary = Path(config.codex_binary)
    path = binary.parent / BUNDLE_MANIFEST
    if not path.exists():
        if binary.name == "codex" and binary.parent.parent == config.root / "bin":
            raise ValueError("Selected pinned Codex bundle manifest is missing")
        path = _reference_manifest(config.root, binary, config.codex_sha256)
        if any(item.is_symlink() for item in (config.root / "state", path.parent)):
            raise ValueError("Codex bundle manifest directory must not be a symbolic link")
    if not path.exists():
        if require:
            raise ValueError(
                "Legacy Codex runtime has no recorded companion bundle; explicitly repin "
                "from the complete original distribution before requiring Code Mode"
            )
        return {"status": "unverified_legacy", "paired": False, "native_verified": False}
    if path.is_symlink():
        raise ValueError("Codex bundle manifest must not be a symbolic link")
    value = read_json(path)
    if (
        not isinstance(value, dict)
        or value.get("version") != BUNDLE_VERSION
        or value.get("kind") not in {"pinned", "reference"}
        or value.get("codex_binary") != str(binary)
        or value.get("codex_sha256") != config.codex_sha256
        or value.get("codex_version") != config.codex_version
    ):
        raise ValueError("Codex bundle manifest does not match the selected runtime")
    try:
        host = Path(value["codex_code_mode_host"])
        digest = value["codex_code_mode_host_sha256"]
        if not host.is_absolute() or len(digest) != 64:
            raise ValueError("Invalid Codex companion identity")
        int(digest, 16)
    except (KeyError, TypeError) as error:
        raise ValueError("Invalid Codex companion identity") from error
    if value["kind"] == "pinned":
        if host != binary.parent / HOST_NAME or any(
            item.is_symlink() for item in (binary.parent, binary, host)
        ):
            raise ValueError("Pinned Codex companion must remain beside its executable")
    elif _source_host(binary) != host:
        raise ValueError("Referenced Codex companion no longer matches its distribution")
    _verify_files(CodexBundle(binary, host, config.codex_sha256, digest))
    return {
        "status": "verified_files",
        "paired": True,
        "native_verified": False,
        "manifest": str(path),
        "codex_code_mode_host": str(host),
        "codex_code_mode_host_sha256": digest,
    }


def runtime_bundle_status(config) -> dict:
    """Read-only doctor data. File integrity is separate from native execution."""
    try:
        return verify_runtime_bundle(config)
    except (OSError, ValueError) as error:
        return {"status": "invalid", "paired": False, "native_verified": False, "error": str(error)}
