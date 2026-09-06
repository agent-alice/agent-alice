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

    POLICY_VERSION = 6

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
            or manifest.get("policy_version") not in {4, 5, self.POLICY_VERSION}
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
        from .runtime_bundle import inspect_bundle, pin_bundle

        source_bundle = inspect_bundle(binary)
        initial_source = source_fingerprint(source)
        digest = sha256_file(wheel)
        candidate_id = f"{digest[:16]}-{uuid4().hex[:12]}"
        candidate = private_dir(self.root / candidate_id)
        env = self._environment(candidate / "build-home")
        try:
            version = subprocess.run(
                [str(source_bundle.binary), "--version"],
                cwd=candidate,
                env=env,
                capture_output=True,
                text=True,
                timeout=15,
            )
            if version.returncode != 0 or not version.stdout.strip().startswith("codex-cli "):
                raise ReleaseError("pinned executable did not identify as Codex CLI")
            # Freeze the matching pair before dependency installation. The
            # original distribution is provenance, not a deployment dependency.
            bundle = pin_bundle(candidate / "codex-runtime", source_bundle, version.stdout.strip())
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
                    "import importlib,importlib.util; "
                    "service=importlib.import_module('alice_codex.service') if "
                    "importlib.util.find_spec('alice_codex.service') is not None else None; "
                    "identity=importlib.import_module('alice_codex.identity') if "
                    "importlib.util.find_spec('alice_codex.identity') is not None else None; "
                    "print(json.dumps({'python':sys.version, 'schedule_schema':Store.SCHEMA_VERSION, "
                    "'memory_schema':MemoryStore.SCHEMA_VERSION, "
                    "'resource_schema':ResourceLedger.SCHEMA_VERSION, "
                    "'resource_epoch_capability':getattr(service,'RESOURCE_EPOCH_CAPABILITY',0), "
                    "'identity_hook_compat_version':getattr(identity,'IDENTITY_HOOK_COMPAT_VERSION',0), "
                    "'packages':sorted((d.metadata['Name'],d.version) for d in m.distributions())}))",
                ],
                cwd=candidate,
                env=env,
                timeout=30,
            )
            if probe.status != "passed":
                raise ReleaseError(f"installed candidate cannot load its schema: {probe.output}")
            installed_metadata = json.loads(probe.output)
            self._epoch_capability(installed_metadata)
            self._identity_capability(installed_metadata)
            head = subprocess.run(
                ["git", "rev-parse", "HEAD"], cwd=source, capture_output=True, text=True, timeout=10
            )
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
                "codex_binary": str(bundle.binary),
                "codex_sha256": bundle.binary_sha256,
                "codex_code_mode_host": str(bundle.host),
                "codex_code_mode_host_sha256": bundle.host_sha256,
                "codex_source_binary": str(source_bundle.binary),
                "codex_source_code_mode_host": str(source_bundle.host),
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

    def _candidate_pair(self, manifest: dict):
        if manifest.get("policy_version") != self.POLICY_VERSION:
            raise ReleaseError(
                "legacy candidate needs current-policy verification; old report preserved"
            )
        from .config import RuntimeConfig
        from .runtime_bundle import CodexBundle, verify_runtime_bundle

        home = self._candidate(manifest["id"]) / "codex-runtime"
        try:
            binary = Path(manifest["codex_binary"])
            if not binary.is_absolute() or not binary.resolve().is_relative_to(home / "bin"):
                raise ValueError("candidate runtime escaped its private bundle directory")
            config = RuntimeConfig(
                str(home), str(binary), manifest["codex_version"], manifest["codex_sha256"]
            )
            pair = verify_runtime_bundle(config, require=True)
            if (
                pair["codex_code_mode_host"] != manifest["codex_code_mode_host"]
                or pair["codex_code_mode_host_sha256"] != manifest["codex_code_mode_host_sha256"]
            ):
                raise ValueError("candidate companion disagrees with the verification identity")
            return CodexBundle(
                binary,
                Path(pair["codex_code_mode_host"]),
                config.codex_sha256,
                pair["codex_code_mode_host_sha256"],
            )
        except (KeyError, OSError, ValueError) as error:
            raise ReleaseError(f"candidate runtime pair is invalid: {error}") from error

    def _configured_pair(self, manifest: dict):
        from .config import load_config
        from .runtime_bundle import CodexBundle, verify_runtime_bundle

        try:
            config = load_config(self.home)
            pair = verify_runtime_bundle(config, require=True)
            if config.codex_sha256 != manifest["codex_sha256"]:
                raise ValueError("runtime config expects a different Codex binary")
            if pair["codex_code_mode_host_sha256"] != manifest.get("codex_code_mode_host_sha256"):
                raise ValueError("runtime config expects a different or unverified Code Mode host")
            return CodexBundle(
                Path(config.codex_binary),
                Path(pair["codex_code_mode_host"]),
                config.codex_sha256,
                pair["codex_code_mode_host_sha256"],
            )
        except (OSError, ValueError) as error:
            raise ReleaseError(f"runtime pair is invalid: {error}") from error

    def _runtime_codex_binary(self, manifest: dict) -> Path:
        fixed = self._candidate_pair(manifest)
        config_path = self.home / "config.json"
        if not config_path.exists():
            return fixed.binary
        return self._configured_pair(manifest).binary

    def _assert_unchanged(self, candidate: Path, manifest: dict) -> None:
        if Path(manifest["wheel"]).name != manifest["wheel"]:
            raise ReleaseError("invalid wheel path in manifest")
        if sha256_file(candidate / manifest["wheel"]) != manifest["wheel_sha256"]:
            raise ReleaseError("candidate wheel changed after staging")
        if sha256_file(self._runtime_codex_binary(manifest)) != manifest["codex_sha256"]:
            raise ReleaseError("pinned Codex binary changed")
        if self._environment_fingerprint(candidate) != manifest["environment_fingerprint"]:
            raise ReleaseError("installed candidate or dependency environment changed")
        if self._probe_epoch_capability(self._python(candidate), candidate) != self._epoch_capability(
            manifest["installed"]
        ):
            raise ReleaseError("installed resource epoch capability does not match candidate metadata")
        if self._probe_identity_capability(
            self._python(candidate), candidate
        ) != self._identity_capability(manifest["installed"]):
            raise ReleaseError("installed identity hook capability does not match candidate metadata")

    @staticmethod
    def _epoch_capability(installed: dict) -> int:
        """Missing legacy declarations mean no support, never inferred support."""
        if not isinstance(installed, dict):
            raise ReleaseError("invalid installed compatibility metadata")
        value = installed.get("resource_epoch_capability", 0)
        if type(value) is not int or value not in {0, 1}:
            raise ReleaseError("unsupported or invalid resource epoch capability")
        return value

    def _probe_epoch_capability(self, python: Path, directory: Path) -> int:
        value = self._probe_capability(
            python, directory, "alice_codex.service", "RESOURCE_EPOCH_CAPABILITY", "resource_epoch_capability"
        )
        return self._epoch_capability(value)

    @staticmethod
    def _identity_capability(installed: dict) -> int:
        if not isinstance(installed, dict):
            raise ReleaseError("invalid installed compatibility metadata")
        value = installed.get("identity_hook_compat_version", 0)
        if type(value) is not int or value not in {0, 1}:
            raise ReleaseError("unsupported or invalid identity hook capability")
        return value

    def _probe_identity_capability(self, python: Path, directory: Path) -> int:
        value = self._probe_capability(
            python, directory, "alice_codex.identity", "IDENTITY_HOOK_COMPAT_VERSION", "identity_hook_compat_version"
        )
        return self._identity_capability(value)

    def _probe_capability(
        self, python: Path, directory: Path, module: str, constant: str, key: str
    ) -> dict:
        result = run_check(
            "installed-" + key,
            [
                str(python), "-I", "-c",
                "import importlib,importlib.util,json; "
                f"module=importlib.import_module({module!r}) if "
                f"importlib.util.find_spec({module!r}) is not None else None; "
                f"print(json.dumps({{{key!r}:getattr(module,{constant!r},0)}}))",
            ],
            cwd=directory,
            env=self._environment(directory / "compatibility-probe-home"),
            timeout=30,
        )
        if result.status != "passed":
            raise ReleaseError(f"installed {key} probe failed: " + result.output)
        try:
            value = json.loads(result.output)
            if not isinstance(value, dict) or key not in value:
                raise ValueError("missing capability value")
            return value
        except (TypeError, ValueError) as error:
            raise ReleaseError(f"invalid installed {key} probe") from error

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
        if manifest.get("policy_version") != self.POLICY_VERSION:
            raise ReleaseError(
                "legacy candidate needs current-policy verification; old report preserved"
            )
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
            pair = self._candidate_pair(manifest)
            env["ALICE_TEST_CODEX_BINARY"] = str(pair.binary)
            env["ALICE_TEST_CODEX_SHA256"] = pair.binary_sha256
            env["ALICE_TEST_CODEX_HOST_BINARY"] = str(pair.host)
            env["ALICE_TEST_CODEX_HOST_SHA256"] = pair.host_sha256
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
                        "tests",
                        "-m",
                        "artifact and not live and not native",
                        "-q",
                    ],
                    candidate / "artifact.xml",
                ),
            ]
            if native:
                if not (source / "tests/test_native_code_mode.py").is_file():
                    raise ReleaseError("required native Code Mode pair test is missing")
                epoch_capability = self._epoch_capability(manifest["installed"])
                epoch_test = "tests/test_native_service_resource_epochs.py"
                identity_capability = self._identity_capability(manifest["installed"])
                identity_test = "tests/test_identity_native.py"
                if epoch_capability and not (source / epoch_test).is_file():
                    raise ReleaseError("required native Service resource epoch test is missing")
                if identity_capability and not (source / identity_test).is_file():
                    raise ReleaseError("required native identity hook test is missing")
                commands.append(
                    (
                        "native",
                        [
                            python,
                            "-m",
                            "pytest",
                            "tests",
                            "--ignore=tests/test_native_code_mode.py",
                            "--ignore=" + epoch_test,
                            "--ignore=" + identity_test,
                            "-m",
                            "native and not live and not native_resource_epoch",
                            "-q",
                        ],
                        candidate / "native.xml",
                    )
                )
                commands.append(
                    (
                        "native_pair",
                        [
                            python,
                            "-m",
                            "pytest",
                            "tests/test_native_code_mode.py",
                            "-m",
                            "native and not live",
                            "-q",
                        ],
                        candidate / "native-pair.xml",
                    )
                )
                if epoch_capability:
                    commands.append(
                        (
                            "native_resource_epoch",
                            [
                                python, "-m", "pytest", epoch_test,
                                "-m", "native and native_resource_epoch and not live", "-q",
                            ],
                            candidate / "native-resource-epoch.xml",
                        )
                    )
                if identity_capability:
                    commands.append(
                        (
                            "native_identity",
                            [python, "-m", "pytest", identity_test, "-m", "native and not live", "-q"],
                            candidate / "native-identity.xml",
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
            "codex_code_mode_host_sha256": manifest["codex_code_mode_host_sha256"],
            "environment_fingerprint": manifest["environment_fingerprint"],
            "resource_epoch_capability": self._epoch_capability(manifest["installed"]),
            "identity_hook_compat_version": self._identity_capability(manifest["installed"]),
            "native_required": native,
            "live_required": live,
            "checks": [asdict(result) for result in results],
            "passed": bool(results) and all(result.status == "passed" for result in results),
        }
        report["promotable"] = (
            report["passed"]
            and native
            and any(item.name == "native_pair" and item.status == "passed" for item in results)
        )
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
        required.update({"native", "native_pair"})
        if self._epoch_capability(manifest["installed"]):
            required.add("native_resource_epoch")
        if self._identity_capability(manifest["installed"]):
            required.add("native_identity")
        if report.get("live_required"):
            required.add("live")
        checks = report.get("checks", [])
        if (
            not report.get("passed")
            or not report.get("promotable")
            or not report.get("native_required")
            or report.get("policy_version") != self.POLICY_VERSION
            or not required <= {check.get("name") for check in checks}
            or any(
                check.get("status") != "passed" or check.get("returncode") != 0 for check in checks
            )
            or report.get("wheel_sha256") != manifest["wheel_sha256"]
            or report.get("candidate_id") != candidate_id
            or report.get("source_sha") != manifest["source_sha"]
            or report.get("source_fingerprint") != manifest["source_fingerprint"]
            or report.get("environment_fingerprint") != manifest["environment_fingerprint"]
            or report.get("codex_sha256") != manifest["codex_sha256"]
            or report.get("codex_code_mode_host_sha256") != manifest["codex_code_mode_host_sha256"]
            or self._epoch_capability(report) != self._epoch_capability(manifest["installed"])
            or self._identity_capability(report) != self._identity_capability(manifest["installed"])
        ):
            raise ReleaseError("candidate required checks did not all pass")
        return candidate, manifest

    def _check_data_schema(self, manifest: dict, requested: int | None) -> None:
        # These are database format guards, not a claim that every JSON/document
        # format or future migration can be reversed. No data is overwritten.
        self._check_resource_epoch_compat(manifest)
        self._check_identity_compat(manifest)
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
            self._configured_pair(manifest)

    def _check_resource_epoch_compat(self, manifest: dict) -> None:
        capability = self._epoch_capability(manifest["installed"])
        path = self.home / "state/runtime.json"
        if path.is_symlink() or path.parent.is_symlink():
            raise ReleaseError("resource epoch runtime state must not be a symbolic link")
        if not path.exists():
            return
        try:
            state = read_json(path)
            if not isinstance(state, dict):
                raise ValueError("runtime state must be an object")
            server = state.get("server")
            present = "resource_epochs" in state or (
                isinstance(server, dict) and "resource_epoch_id" in server
            )
            if not present:
                return
            if capability < 1:
                raise ReleaseError("runtime resource epochs require an epoch-capable candidate")
            if type(state.get("version")) is not int or state["version"] != 1:
                raise ValueError("unsupported runtime state version with resource epochs")
            # The same pure validator is owned by Service, so release checks
            # cannot silently drift from the actual crash-recovery protocol.
            from .service import validate_resource_epoch_journal

            validate_resource_epoch_journal(state)
        except (ImportError, OSError, TypeError, ValueError) as error:
            raise ReleaseError(f"invalid resource epoch journal: {error}") from error

    def _check_bootstrap_policy(self, manifest: dict) -> None:
        path = self.home / "state/supervisor.json"
        if path.is_symlink() or path.parent.is_symlink():
            raise ReleaseError("supervisor compatibility state must not be a symbolic link")
        if not path.exists():
            return
        from .bootstrap import checked_runtime

        runtime = checked_runtime(self.home)
        bootstrap = runtime["manifest"]
        bootstrap_capability = self._epoch_capability(bootstrap["installed"])
        bootstrap_python = Path(runtime["python"])
        if self._probe_epoch_capability(
            bootstrap_python, bootstrap_python.parents[2]
        ) != bootstrap_capability:
            raise ReleaseError("installed bootstrap resource epoch capability metadata changed")
        if bootstrap_capability < self._epoch_capability(manifest["installed"]):
            # Reject before the first capable service can introduce its new
            # journal under a supervisor whose copied code cannot recover it.
            raise ReleaseError(
                "stop and uninstall the previous supervisor before introducing resource epochs"
            )
        bootstrap_identity = self._identity_capability(bootstrap["installed"])
        if self._probe_identity_capability(
            bootstrap_python, bootstrap_python.parents[2]
        ) != bootstrap_identity:
            raise ReleaseError("installed bootstrap identity hook capability metadata changed")
        if bootstrap_identity < self._identity_capability(manifest["installed"]):
            raise ReleaseError(
                "stop and uninstall the previous supervisor before introducing identity hooks"
            )
        if (
            bootstrap.get("release_policy_version") != self.POLICY_VERSION
            or bootstrap.get("codex_sha256") != manifest["codex_sha256"]
            or bootstrap.get("codex_code_mode_host_sha256")
            != manifest["codex_code_mode_host_sha256"]
        ):
            raise ReleaseError(
                "stop and uninstall the previous supervisor before changing its verified runtime pair"
            )

    def _check_identity_compat(self, manifest: dict) -> None:
        capability = self._identity_capability(manifest["installed"])
        path = self.home / "state/identity-runtime.json"
        delivery = self.home / "state/identity-delivery"
        config_path = self.home / "codex/config.toml"
        if any(item.is_symlink() for item in (path, path.parent, delivery, config_path, config_path.parent)):
            raise ReleaseError("identity compatibility state must not be a symbolic link")
        present = path.exists() or delivery.exists()
        try:
            if config_path.exists():
                import tomllib
                from .identity import has_owned_identity_hooks

                document = tomllib.loads(config_path.read_text(encoding="utf-8"))
                # init/configuration can write hooks before the ready manifest.
                # Detect that prefix window with the owner's exact recognizer.
                present = present or has_owned_identity_hooks(document)
            if not present:
                return
            if capability < 1:
                raise ReleaseError("runtime identity hooks require a rebinding-capable candidate")
            if path.exists():
                from .identity import IdentityError, validate_identity_runtime_manifest

                try:
                    validate_identity_runtime_manifest(read_json(path))
                except IdentityError as error:
                    raise ReleaseError(f"invalid identity runtime manifest: {error}") from error
        except (ImportError, OSError, TypeError, ValueError) as error:
            raise ReleaseError(f"invalid identity compatibility state: {error}") from error

    def current(self) -> dict[str, Any] | None:
        path = self.root / "current.json"
        if not path.exists():
            return None
        value = read_json(path)
        if not isinstance(value, dict) or "current" not in value:
            raise ReleaseError("invalid active release pointer")
        if not isinstance(value["current"], str) or not re.fullmatch(
            r"[0-9a-f]{16}-[0-9a-f]{12}", value["current"]
        ):
            raise ReleaseError("invalid active candidate id")
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
        self._check_bootstrap_policy(manifest)
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
            self._check_bootstrap_policy(manifest)
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
            self._check_bootstrap_policy(manifest)
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
            self._check_bootstrap_policy(manifest)
            pointer = {
                "current": previous,
                "previous": expected_current,
                "activation_epoch": epoch,
                "python": str(self._python(candidate)),
                "wheel_sha256": manifest["wheel_sha256"],
            }
            write_json(self.root / "current.json", pointer)
            return pointer
