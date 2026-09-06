"""Install an independent verified environment for the small process supervisor.

The copy deliberately keeps the underlying system Python dependency; its real
path and bytes are bound separately. Absolute links back into a release, editable
imports, and unrelated external links are rejected. This is an installation copy,
not a relocated candidate receipt or another model runtime.
"""

import json
from pathlib import Path
import re
import shutil
import subprocess
from uuid import uuid4

from .files import private_dir, read_json, sha256_file, write_json
from .releases import ReleaseError, ReleaseManager


def _check_links(environment: Path) -> Path:
    interpreter = (environment / "bin/python").resolve(strict=True)
    if interpreter.is_relative_to(environment.resolve()):
        raise ReleaseError("bootstrap requires an external stable base interpreter")
    for path in environment.rglob("*"):
        if not path.is_symlink():
            continue
        target = path.resolve(strict=True)
        if target.is_relative_to(environment.resolve()):
            if Path(path.readlink()).is_absolute():
                raise ReleaseError("absolute environment symlink would retain candidate dependency")
        elif not (path.parent == environment / "bin" and target == interpreter):
            raise ReleaseError("unsupported external environment symlink")
    return interpreter


def checked_runtime(home: Path | str) -> dict:
    root = Path(home).resolve() / "bootstrap"
    record = read_json(root / "current.json")
    generation = record.get("generation", "")
    if not re.fullmatch(r"[0-9a-f]{32}", generation):
        raise ReleaseError("invalid bootstrap generation")
    folder = root / generation
    if folder.is_symlink() or not folder.is_dir():
        raise ReleaseError("bootstrap environment is missing or linked")
    manifest = read_json(folder / "bootstrap.json")
    if manifest.get("version") != 1 or manifest.get("generation") != generation:
        raise ReleaseError("invalid bootstrap manifest")
    python = str(folder / "venv/bin/python")
    if record.get("python") != python:
        raise ReleaseError("bootstrap interpreter pointer changed")
    interpreter = _check_links(folder / "venv")
    if (
        str(interpreter) != manifest["base_python"]
        or sha256_file(interpreter) != manifest["base_python_sha256"]
        or ReleaseManager._environment_fingerprint(folder) != manifest["environment_fingerprint"]
    ):
        raise ReleaseError("bootstrap or base interpreter changed")
    return {**record, "manifest": manifest}


def install_runtime(manager: ReleaseManager) -> dict:
    """Copy one exact verified non-editable environment without pip or a rebuild."""
    current = manager.checked_current()
    if current is None:
        raise ReleaseError("bootstrap installation requires a verified active candidate")
    candidate, manifest = manager._verified(current["current"])
    root = private_dir(manager.home / "bootstrap")
    if (root / "current.json").exists():
        prior = checked_runtime(manager.home)
        if prior["manifest"]["source_candidate"] == current["current"]:
            return prior
    generation = uuid4().hex
    target = private_dir(root / generation)
    base = _check_links(candidate / "venv")
    shutil.copytree(
        candidate / "venv",
        target / "venv",
        symlinks=True,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo"),
    )
    # Bind executable links directly to the validated base, never through the
    # original candidate or an environment alias that may later be removed.
    for path in (target / "venv/bin").iterdir():
        if path.is_symlink() and path.resolve() == base:
            path.unlink()
            path.symlink_to(base)
    if (
        _check_links(target / "venv") != base
        or ReleaseManager._environment_fingerprint(target) != manifest["environment_fingerprint"]
    ):
        raise ReleaseError("bootstrap copy does not match verified installed environment")
    manager._assert_unchanged(candidate, manifest)
    python = target / "venv/bin/python"
    result = subprocess.run(
        [
            str(python),
            "-I",
            "-c",
            "import json,sys; import alice_codex.supervisor as s; print(json.dumps({'prefix':sys.prefix,'module':s.__file__,'protocol':s.BOOTSTRAP_PROTOCOL}))",
        ],
        cwd=target,
        env=manager._environment(target / "probe-home"),
        capture_output=True,
        text=True,
        timeout=20,
    )
    if result.returncode:
        raise ReleaseError("independent bootstrap import probe failed: " + result.stderr[-1500:])
    probe = json.loads(result.stdout)
    if (
        Path(probe["prefix"]).resolve() != (target / "venv").resolve()
        or not Path(probe["module"]).resolve().is_relative_to((target / "venv").resolve())
        or probe["protocol"] != 1
    ):
        raise ReleaseError("bootstrap import escaped its independent installed environment")
    metadata = {
        "version": 1,
        "generation": generation,
        "source_candidate": current["current"],
        "wheel_sha256": manifest["wheel_sha256"],
        "environment_fingerprint": manifest["environment_fingerprint"],
        "base_python": str(base),
        "base_python_sha256": sha256_file(base),
        "installed": manifest["installed"],
        "codex_sha256": manifest["codex_sha256"],
        "probe": probe,
    }
    write_json(target / "bootstrap.json", metadata)
    record = {"version": 1, "generation": generation, "python": str(python)}
    write_json(root / "current.json", record)
    return checked_runtime(manager.home)
