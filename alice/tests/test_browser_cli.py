"""Exercise browser commands and the synchronous installer's async boundary."""

import asyncio
import json
from pathlib import Path
import subprocess
import sys

from alice_codex import browser, cli
from alice_codex.config import RuntimeConfig


def configured_home(tmp_path):
    config = RuntimeConfig(str(tmp_path / "alice"), "/usr/bin/true", "fixture", "unused")
    config.prepare_directories()
    config.save()
    return config


def test_status_cli_does_not_create_runtime_or_claim_missing_browser(tmp_path):
    config = configured_home(tmp_path)
    before = {str(p.relative_to(config.root)) for p in config.root.rglob("*")}
    result = subprocess.run(
        [sys.executable, "-m", "alice_codex", "--home", config.home, "browser", "status"],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "installed": False,
        "native_verified": False,
        "server": "alice_browser",
    }
    assert {str(p.relative_to(config.root)) for p in config.root.rglob("*")} == before


async def test_cli_installer_allows_native_verifiers_own_event_loop(tmp_path, monkeypatch):
    config = configured_home(tmp_path)
    supplied = {name: tmp_path / name for name in ("node", "npm_cli", "browser_executable")}

    def install(actual_config, **paths):
        assert actual_config.home == config.home
        assert paths == supplied and all(isinstance(p, Path) for p in paths.values())

        async def native_check():
            return {"installed": True, "native_verified": True}

        # The real installer calls synchronous verification with asyncio.run.
        # Calling it on the CLI's already-running loop would fail at this point.
        return asyncio.run(native_check())

    monkeypatch.setattr(browser, "install", install)
    args = cli.parser().parse_args(
        [
            "--home",
            config.home,
            "browser",
            "install",
            "--node",
            str(supplied["node"]),
            "--npm-cli",
            str(supplied["npm_cli"]),
            "--browser-executable",
            str(supplied["browser_executable"]),
        ]
    )
    assert await cli.execute(args) == {"installed": True, "native_verified": True}
