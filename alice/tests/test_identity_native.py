"""Native identity checks; artifact mode executes the candidate Python with -I."""

import asyncio
import importlib.util
import json
import os
from pathlib import Path
import signal

import pytest


pytestmark = pytest.mark.native


async def run_check(tmp_path, mode):
    binary = Path(os.environ.get("ALICE_TEST_CODEX_BINARY", "/nonexistent/required-codex"))
    assert binary.is_file(), "Select the exact verified native pair with ALICE_TEST_CODEX_BINARY"
    checker = Path(__file__).resolve().parents[1] / "tools/check_identity_native.py"
    output = tmp_path / ("owned-" + (mode or "protocol"))
    artifact_python = os.environ.get("ALICE_ARTIFACT_PYTHON")
    if artifact_python:
        command = [
            artifact_python,
            "-I",
            str(checker),
            "--codex",
            str(binary),
            "--hook-python",
            artifact_python,
            "--output",
            str(output),
        ]
        if mode:
            command.append("--" + mode)
        process = await asyncio.create_subprocess_exec(
            *command,
            cwd=tmp_path,
            env={"HOME": str(tmp_path), "PATH": "/usr/bin:/bin", "LANG": "en_US.UTF-8"},
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        communication = asyncio.create_task(process.communicate())
        try:
            stdout, stderr = await asyncio.wait_for(asyncio.shield(communication), 120)
        except BaseException:
            # asyncio.run in the checker cancels its main task on SIGINT, so its
            # finally block stops and waits for every owned native runtime.
            if process.returncode is None:
                process.send_signal(signal.SIGINT)
            try:
                await asyncio.wait_for(asyncio.shield(communication), 40)
            except TimeoutError:
                process.kill()
                await process.wait()
            raise
        assert process.returncode == 0, (stdout + stderr).decode(errors="replace")[-18000:]
        report = json.loads((output / "identity-native-report.json").read_text())
        prefix = Path(artifact_python).expanduser().absolute().parent.parent.resolve()
        assert Path(report["checker_prefix"]).resolve() == prefix
        assert Path(report["checker_identity_path"]).resolve().is_relative_to(prefix), (
            "Artifact checker imported identity from outside the candidate environment"
        )
        assert report["hook_package_mode"] == "installed_interpreter"
        assert report["checker_identity_sha256"] == report["identity_module_sha256"]
    else:
        spec = importlib.util.spec_from_file_location("alice_owned_identity_check", checker)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        report = await module.check_identity_native(
            output, binary, delivery=mode == "delivery", tool_failure=mode == "tool-failure"
        )
        assert report["hook_package_mode"] == "isolated_source_link"
    assert report["passed"] and report["native_model_calls"] == 0
    assert report["owned_processes_remaining"] == []


async def test_identity_baseline_recovery_compaction_hooks_and_remote_tui(tmp_path):
    await run_check(tmp_path, "")


async def test_identity_delivery_receipts_retry_compact_and_tui(tmp_path):
    await run_check(tmp_path, "delivery")


async def test_identity_survives_actual_code_mode_first_tool_failure(tmp_path):
    await run_check(tmp_path, "tool-failure")
