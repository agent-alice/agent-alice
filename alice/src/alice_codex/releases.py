"""Build once, test the installed candidate, then switch only code pointers.

This is a local regression gate, not a security boundary against code with this
user's filesystem permissions. Reports come from commands this module runs; a
caller cannot mark a candidate passed. The service lifecycle belongs to the CLI.
No activation/rollback operation copies, migrates, or restores business data.
"""

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
from typing import Any
from uuid import uuid4
import xml.etree.ElementTree as ET

from .files import SingletonLock, private_dir, read_json, sha256_file, write_json


class ReleaseError(RuntimeError):
    pass


@dataclass(frozen=True)
class CheckResult:
    name: str
    command: list[str]
    returncode: int | None
    status: str
    duration_seconds: float
    output: str = ""
    detail: str = ""


def source_fingerprint(root: Path) -> str:
    """Hash the explicit code/check surface; never crawl legacy memories/logs."""
    paths = []
    for name in (
        "AGENTS.md",
        "pyproject.toml",
        "requirements.lock",
        ".python-version",
        "ruff.toml",
        "pytest.ini",
        "setup.cfg",
        "alice.toml",
        "src",
        "tests",
        "tools",
    ):
        path = root / name
        if path.is_file():
            paths.append(path)
        elif path.is_dir():
            paths.extend(p for p in path.rglob("*") if p.is_file())
    digest = hashlib.sha256()
    for path in sorted(paths):
        if (
            "__pycache__" in path.parts
            or path.suffix in {".pyc", ".pyo"}
            or any(part.endswith(".egg-info") for part in path.parts)
        ):
            continue
        if path.is_symlink():
            raise ReleaseError(
                f"source verification surface cannot contain symlinks: {path.relative_to(root)}"
            )
        digest.update(str(path.relative_to(root)).encode() + b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def pytest_evidence(path: Path) -> str | None:
    """Return a failure reason for absent, empty, failed or skipped JUnit data."""
    try:
        tree = ET.parse(path)
        cases = list(tree.iter("testcase"))
        if not cases:
            return "required pytest check collected no tests"
        for case in cases:
            if any(case.find(name) is not None for name in ("skipped", "failure", "error")):
                return "required pytest check contains skipped, failed or errored cases"
        return None
    except (OSError, ET.ParseError) as exc:
        return f"required pytest evidence is missing or invalid: {exc}"


def run_check(
    name: str,
    command: list[str],
    *,
    cwd: Path,
    env: dict[str, str],
    timeout: float,
    junit: Path | None = None,
) -> CheckResult:
    """Run a shell-free owned process group and retain its observed outcome."""
    started = time.monotonic()
    process = None
    status, code, output, detail = "failed", None, "", ""
    try:
        process = subprocess.Popen(
            command,
            cwd=cwd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
        try:
            output, _ = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            output, _ = process.communicate()
            status, detail = "timed_out", "required command exceeded its timeout"
        else:
            code = process.returncode
            status = "passed" if code == 0 else "failed"
            if code is not None and code < 0:
                status = "cancelled"
            if status == "passed" and junit is not None:
                failure = pytest_evidence(junit)
                if failure:
                    status, detail = "failed", failure
    except (OSError, ValueError) as exc:
        detail = f"{type(exc).__name__}: {exc}"
    except BaseException:
        if process is not None and process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.communicate()
        raise
    if process is not None:
        code = process.returncode
    return CheckResult(
        name, command, code, status, round(time.monotonic() - started, 4), output[-12000:], detail
    )


class ReleaseManager:
    """Candidates live in home/releases; current.json is only a code selector."""

    POLICY_VERSION = 4

    def __init__(self, home: Path | str):
        self.home = Path(home).expanduser().resolve()
        self.root = private_dir(self.home / "releases")

    def _candidate(self, candidate_id: str) -> Path:
        if not re.fullmatch(r"[0-9a-f]{16}-[0-9a-f]{12}", candidate_id):
            raise ReleaseError("invalid candidate id")
        path = self.root / candidate_id
        if path.is_symlink() or not path.is_dir():
            raise ReleaseError("candidate directory is missing or is a symbolic link")
        return path

    @staticmethod
    def _python(candidate: Path) -> Path:
        return candidate / "venv" / "bin" / "python"

    @staticmethod
    def _environment(home: Path) -> dict[str, str]:
        # No user API tokens, shared Codex state or source PYTHONPATH leaks into checks.
        env = {
            key: os.environ[key]
            for key in (
                "PATH",
                "LANG",
                "LC_ALL",
                "SYSTEMROOT",
                "TMPDIR",
                "HTTP_PROXY",
                "HTTPS_PROXY",
                "ALL_PROXY",
                "NO_PROXY",
                "http_proxy",
                "https_proxy",
                "all_proxy",
                "no_proxy",
            )
            if key in os.environ
        }
        env.update(
            HOME=str(private_dir(home)),
            PYTHONDONTWRITEBYTECODE="1",
            PIP_DISABLE_PIP_VERSION_CHECK="1",
        )
        return env

    def _manifest(self, candidate_id: str) -> tuple[Path, dict[str, Any]]:
        candidate = self._candidate(candidate_id)
        manifest = read_json(candidate / "candidate.json")
        if (
            not isinstance(manifest, dict)
            or manifest.get("id") != candidate_id
            or manifest.get("policy_version") != self.POLICY_VERSION
        ):
            raise ReleaseError("invalid candidate manifest")
        return candidate, manifest

    def build(
        self,
        source_root: Path | str,
        *,
        python: str = sys.executable,
        codex_binary: Path | str,
        timeout: float = 600,
    ) -> str:
        source = Path(source_root).resolve()
        if not (source / "pyproject.toml").is_file():
            raise ReleaseError("source tree has no pyproject.toml")
        before = source_fingerprint(source)
        with tempfile.TemporaryDirectory(prefix=".build-", dir=self.root) as temporary:
            output = Path(temporary)
            result = run_check(
                "build",
                [python, "-m", "build", "--wheel", "--outdir", str(output)],
                cwd=source,
                env=self._environment(output / "home"),
                timeout=timeout,
            )
            wheels = list(output.glob("*.whl"))
            if result.status != "passed" or len(wheels) != 1:
                raise ReleaseError(
                    f"candidate wheel build failed: {result.detail}\n{result.output}"
                )
            if source_fingerprint(source) != before:
                raise ReleaseError("source changed during build; candidate was not staged")
            candidate_id = self.stage_wheel(
                wheels[0],
                source_root=source,
                python=python,
                codex_binary=codex_binary,
                timeout=timeout,
            )
            if source_fingerprint(source) != before:
                raise ReleaseError("source changed during candidate installation; rebuild required")
            write_json(self._candidate(candidate_id) / "build-result.json", asdict(result))
            return candidate_id

    def stage_wheel(
        self,
        wheel: Path | str,
        *,
        source_root: Path | str,
        python: str = sys.executable,
        codex_binary: Path | str,
        timeout: float = 600,
    ) -> str:
        """Install one exact wheel in a private venv. Verification is still required."""
        wheel, source, binary = (
            Path(wheel).resolve(),
            Path(source_root).resolve(),
            Path(codex_binary).resolve(),
        )
        if not wheel.is_file() or wheel.suffix != ".whl" or not binary.is_file():
            raise ReleaseError("wheel and pinned Codex binary must exist")
        constraints = source / "requirements.lock"
        if not constraints.is_file():
            raise ReleaseError("source requirements.lock is required for candidate installation")
        initial_source = source_fingerprint(source)
        digest = sha256_file(wheel)
        candidate_id = f"{digest[:16]}-{uuid4().hex[:12]}"
        candidate = private_dir(self.root / candidate_id)
        env = self._environment(candidate / "build-home")
        try:
            shutil.copyfile(wheel, candidate / wheel.name)
            shutil.copyfile(constraints, candidate / "requirements.lock")
            if sha256_file(candidate / wheel.name) != digest:
                raise ReleaseError("wheel changed during staging")
            installed = self._python(candidate)
            for name, command in (
                ("venv", [python, "-m", "venv", str(candidate / "venv")]),
                (
                    "install",
                    [
                        str(installed),
                        "-m",
                        "pip",
                        "install",
                        "-c",
                        str(candidate / "requirements.lock"),
                        str(candidate / wheel.name),
                    ],
                ),
            ):
                result = run_check(name, command, cwd=candidate, env=env, timeout=timeout)
                write_json(candidate / f"{name}-result.json", asdict(result))
                if result.status != "passed":
                    raise ReleaseError(f"candidate {name} failed: {result.detail}\n{result.output}")
            probe = run_check(
                "installed-metadata",
                [
                    str(installed),
                    "-I",
                    "-c",
                    "import json,sys,importlib.metadata as m; from alice_codex.store import Store; "
                    "from alice_codex.memory import MemoryStore; "
                    "from alice_codex.resources import ResourceLedger; "
                    "print(json.dumps({'python':sys.version, 'schedule_schema':Store.SCHEMA_VERSION, "
                    "'memory_schema':MemoryStore.SCHEMA_VERSION, "
                    "'resource_schema':ResourceLedger.SCHEMA_VERSION, "
                    "'packages':sorted((d.metadata['Name'],d.version) for d in m.distributions())}))",
                ],
                cwd=candidate,
                env=env,
                timeout=30,
            )
            if probe.status != "passed":
                raise ReleaseError(f"installed candidate cannot load its schema: {probe.output}")
            installed_metadata = json.loads(probe.output)
            head = subprocess.run(
                ["git", "rev-parse", "HEAD"], cwd=source, capture_output=True, text=True, timeout=10
            )
            version = subprocess.run(
                [str(binary), "--version"],
                cwd=candidate,
                env=env,
                capture_output=True,
                text=True,
                timeout=15,
            )
            if version.returncode != 0 or not version.stdout.strip().startswith("codex-cli "):
                raise ReleaseError("pinned executable did not identify as Codex CLI")
            if source_fingerprint(source) != initial_source:
                raise ReleaseError("source changed during candidate installation")
            manifest = {
                "policy_version": self.POLICY_VERSION,
                "id": candidate_id,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "source_root": str(source),
                "source_sha": head.stdout.strip() if head.returncode == 0 else None,
                "source_fingerprint": initial_source,
                "verification_python": str(Path(python).absolute()),
                "wheel": wheel.name,
                "wheel_sha256": digest,
                "codex_binary": str(binary),
                "codex_sha256": sha256_file(binary),
                "codex_version": version.stdout.strip(),
                "installed": installed_metadata,
                "environment_fingerprint": self._environment_fingerprint(candidate),
                "verified_report_sha256": None,
            }
            write_json(candidate / "candidate.json", manifest)
        except BaseException:
            # Preserve failed build/install evidence, but no manifest means not activatable.
            raise
        return candidate_id

    @staticmethod
    def _environment_fingerprint(candidate: Path) -> str:
        digest = hashlib.sha256()
        environment = candidate / "venv"
        for path in sorted(environment.rglob("*")):
            if not path.is_file() or "__pycache__" in path.parts or path.suffix in {".pyc", ".pyo"}:
                continue
            digest.update(str(path.relative_to(environment)).encode() + b"\0")
            # Following the interpreter link binds the underlying Python bytes too.
            digest.update(path.read_bytes())
            digest.update(b"\0")
        return digest.hexdigest()

    def _assert_unchanged(self, candidate: Path, manifest: dict) -> None:
        if Path(manifest["wheel"]).name != manifest["wheel"]:
            raise ReleaseError("invalid wheel path in manifest")
        if sha256_file(candidate / manifest["wheel"]) != manifest["wheel_sha256"]:
            raise ReleaseError("candidate wheel changed after staging")
        if sha256_file(Path(manifest["codex_binary"])) != manifest["codex_sha256"]:
            raise ReleaseError("pinned Codex binary changed")
        if self._environment_fingerprint(candidate) != manifest["environment_fingerprint"]:
            raise ReleaseError("installed candidate or dependency environment changed")

    def verify(
        self,
        candidate_id: str,
        *,
        source_root: Path | str | None = None,
        native: bool = False,
        live: bool = False,
        timeout: float = 300,
    ) -> dict[str, Any]:
        candidate, manifest = self._manifest(candidate_id)
        # Invalidate a previous successful result before any new verification attempt.
        manifest["verified_report_sha256"] = None
        write_json(candidate / "candidate.json", manifest)
        if live and not native:
            raise ReleaseError("live verification requires the explicit native gate too")
        source = Path(source_root or manifest["source_root"]).resolve()
        results: list[CheckResult] = []
        try:
            self._assert_unchanged(candidate, manifest)
            if source_fingerprint(source) != manifest["source_fingerprint"]:
                raise ReleaseError("source/checks changed since this candidate was built")
            if not (source / "tests" / "test_artifact_smoke.py").is_file():
                raise ReleaseError("required installed-artifact smoke test is missing")
            env = self._environment(candidate / "verification-home")
            # The verification interpreter may have another editable checkout
            # installed. Test this explicit source tree; installed-artifact
            # subprocesses use the candidate interpreter with -I instead.
            env["PYTHONPATH"] = str(source / "src")
            env["ALICE_ARTIFACT_PYTHON"] = str(self._python(candidate))
            env["ALICE_TEST_CODEX_BINARY"] = manifest["codex_binary"]
            python = manifest["verification_python"]
            static_paths = [name for name in ("src", "tests", "tools") if (source / name).exists()]
            commands: list[tuple[str, list[str], Path | None]] = [
                (
                    "ruff",
                    [
                        python,
                        "-m",
                        "ruff",
                        "check",
                        "--config",
                        str(source / "pyproject.toml"),
                        *static_paths,
                    ],
                    None,
                ),
                (
                    "unit",
                    [
                        python,
                        "-m",
                        "pytest",
                        "tests",
                        "-m",
                        "not native and not live and not artifact",
                        "-q",
                    ],
                    candidate / "unit.xml",
                ),
                (
                    "artifact",
                    [
                        python,
                        "-m",
                        "pytest",
                        "tests/test_artifact_smoke.py",
                        "-m",
                        "not live and not native",
                        "-q",
                    ],
                    candidate / "artifact.xml",
                ),
            ]
            if native:
                commands.append(
                    (
                        "native",
                        [python, "-m", "pytest", "tests", "-m", "native and not live", "-q"],
                        candidate / "native.xml",
                    )
                )
            if live:
                commands.append(
                    (
                        "live",
                        [python, "-m", "pytest", "tests", "-m", "live", "-q"],
                        candidate / "live.xml",
                    )
                )
            for name, command, junit in commands:
                if junit is not None:
                    junit.unlink(missing_ok=True)
                    command = [*command, f"--junitxml={junit}"]
                results.append(
                    run_check(name, command, cwd=source, env=env, timeout=timeout, junit=junit)
                )
            self._assert_unchanged(candidate, manifest)
            if source_fingerprint(source) != manifest["source_fingerprint"]:
                raise ReleaseError("source/checks changed during candidate verification")
        except (ReleaseError, OSError, ValueError) as exc:
            results.append(CheckResult("integrity", [], None, "failed", 0, detail=str(exc)))
        report = {
            "policy_version": self.POLICY_VERSION,
            "candidate_id": candidate_id,
            "source_sha": manifest["source_sha"],
            "source_fingerprint": manifest["source_fingerprint"],
            "wheel_sha256": manifest["wheel_sha256"],
            "codex_sha256": manifest["codex_sha256"],
            "environment_fingerprint": manifest["environment_fingerprint"],
            "native_required": native,
            "live_required": live,
            "checks": [asdict(result) for result in results],
            "passed": bool(results) and all(result.status == "passed" for result in results),
        }
        write_json(candidate / "verification.json", report)
        if report["passed"]:
            manifest["verified_report_sha256"] = sha256_file(candidate / "verification.json")
            write_json(candidate / "candidate.json", manifest)
        return report

    def _verified(self, candidate_id: str) -> tuple[Path, dict]:
        candidate, manifest = self._manifest(candidate_id)
        self._assert_unchanged(candidate, manifest)
        report_path = candidate / "verification.json"
        if (
            not manifest.get("verified_report_sha256")
            or not report_path.is_file()
            or sha256_file(report_path) != manifest["verified_report_sha256"]
        ):
            raise ReleaseError("candidate has no matching successful verification")
        report = read_json(report_path)
        required = {"ruff", "unit", "artifact"}
        if report.get("native_required"):
            required.add("native")
        if report.get("live_required"):
            required.add("live")
        checks = report.get("checks", [])
        if (
            not report.get("passed")
            or not required <= {check.get("name") for check in checks}
            or any(
                check.get("status") != "passed" or check.get("returncode") != 0 for check in checks
            )
            or report.get("wheel_sha256") != manifest["wheel_sha256"]
            or report.get("candidate_id") != candidate_id
        ):
            raise ReleaseError("candidate required checks did not all pass")
        return candidate, manifest

    def _check_data_schema(self, manifest: dict, requested: int | None) -> None:
        # These are database format guards, not a claim that every JSON/document
        # format or future migration can be reversed. No data is overwritten.
        databases = {
            "schedule": self.home / "state/schedules.sqlite3",
            "memory": self.home / "memory-state/sources.sqlite3",
            "resource": self.home / "state/resources.sqlite3",
        }
        for name, database in databases.items():
            supported = manifest["installed"][f"{name}_schema"]
            actual = None
            if database.exists():
                try:
                    with sqlite3.connect(database.as_uri() + "?mode=ro", uri=True) as db:
                        if db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                            raise ReleaseError(f"{name} database integrity check failed")
                        actual = db.execute("PRAGMA user_version").fetchone()[0]
                except sqlite3.Error as exc:
                    raise ReleaseError(f"cannot validate {name} data schema: {exc}") from exc
            if name == "schedule" and requested is not None:
                if actual is not None and requested != actual:
                    raise ReleaseError("requested data schema disagrees with the existing database")
                actual = requested if actual is None else actual
            if actual is not None and actual != supported:
                raise ReleaseError(
                    f"candidate supports {name} schema {supported}, data requires {actual}"
                )
        config_path = self.home / "config.json"
        if config_path.exists():
            config = read_json(config_path)
            if config.get("codex_sha256") != manifest["codex_sha256"]:
                raise ReleaseError("candidate was verified against a different Codex binary")

    def current(self) -> dict[str, Any] | None:
        path = self.root / "current.json"
        if not path.exists():
            return None
        value = read_json(path)
        if not isinstance(value, dict) or "current" not in value:
            raise ReleaseError("invalid active release pointer")
        epoch = value.get("activation_epoch")
        if epoch is not None and (
            not isinstance(epoch, str)
            or not re.fullmatch(r"[0-9a-f]{32}|legacy:[0-9a-f]{16}-[0-9a-f]{12}", epoch)
        ):
            raise ReleaseError("invalid activation epoch")
        return value

    def checked_current(self, *, data_schema: int | None = None) -> dict[str, Any] | None:
        """Resolve a runnable active candidate; never execute a pointer's raw path."""
        pointer = self.current()
        if pointer is None:
            return None
        candidate, manifest = self._verified(pointer["current"])
        self._check_data_schema(manifest, data_schema)
        canonical = {
            "current": manifest["id"],
            "previous": pointer.get("previous"),
            "python": str(self._python(candidate)),
            "wheel_sha256": manifest["wheel_sha256"],
        }
        if "activation_epoch" in pointer:
            canonical["activation_epoch"] = pointer["activation_epoch"]
        if (
            pointer.get("python") != canonical["python"]
            or pointer.get("wheel_sha256") != canonical["wheel_sha256"]
        ):
            raise ReleaseError("active release pointer does not match the verified candidate")
        return canonical

    def activate(self, candidate_id: str, *, data_schema: int | None = None) -> dict[str, Any]:
        """Select verified code after the caller has stopped its service."""
        with SingletonLock(self.root / ".switch.lock"):
            candidate, manifest = self._verified(candidate_id)
            self._check_data_schema(manifest, data_schema)
            old = self.current()
            pointer = {
                "current": candidate_id,
                "previous": (
                    old.get("previous")
                    if old and old["current"] == candidate_id
                    else old["current"]
                    if old
                    else None
                ),
                "activation_epoch": uuid4().hex,
                "python": str(self._python(candidate)),
                "wheel_sha256": manifest["wheel_sha256"],
            }
            write_json(self.root / "current.json", pointer)
            return pointer

    def rollback(self, *, data_schema: int | None = None) -> dict[str, Any]:
        """Return to previous verified code without restoring an old data copy."""
        with SingletonLock(self.root / ".switch.lock"):
            old = self.current()
            if old is None or not old.get("previous"):
                raise ReleaseError("no previous verified release is available")
            candidate, manifest = self._verified(old["previous"])
            self._check_data_schema(manifest, data_schema)
            pointer = {
                "current": old["previous"],
                "previous": old["current"],
                "activation_epoch": uuid4().hex,
                "python": str(self._python(candidate)),
                "wheel_sha256": manifest["wheel_sha256"],
            }
            write_json(self.root / "current.json", pointer)
            return pointer

    def automatic_rollback(
        self,
        expected_current: str,
        failed_candidates: set[str],
        *,
        expected_epoch: str | None = None,
    ) -> dict[str, Any]:
        """One conditional fallback within the same explicit activation epoch.

        The failed set is durable supervisor state. Keeping the epoch and failed
        candidate prevents a restart from bouncing between A and B. This never
        restores old data and never trusts the failed current environment.
        """
        with SingletonLock(self.root / ".switch.lock"):
            old = self.current()
            if old is None or old["current"] != expected_current:
                raise ReleaseError("active candidate changed during startup recovery")
            epoch = old.get("activation_epoch", "legacy:" + old["current"])
            if expected_epoch is not None and epoch != expected_epoch:
                raise ReleaseError("activation epoch changed during startup recovery")
            previous = old.get("previous")
            if not previous or previous in failed_candidates or previous == expected_current:
                raise ReleaseError("no unfailed previous candidate is available")
            candidate, manifest = self._verified(previous)
            self._check_data_schema(manifest, None)
            pointer = {
                "current": previous,
                "previous": expected_current,
                "activation_epoch": epoch,
                "python": str(self._python(candidate)),
                "wheel_sha256": manifest["wheel_sha256"],
            }
            write_json(self.root / "current.json", pointer)
            return pointer
