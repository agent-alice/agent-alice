"""Browser installation regression cases; native validation is never self-certified."""

import json
from pathlib import Path
import tomllib

import pytest

from alice_codex import browser
from alice_codex.config import RuntimeConfig
from alice_codex.files import SingletonLock, sha256_file


@pytest.fixture
def setup(tmp_path, monkeypatch):
    root = tmp_path.resolve()
    node, npm, chrome, codex = (root / name for name in ("node", "npm.js", "chrome", "codex"))
    for path, output in (
        (node, "v24.0.0"),
        (chrome, "Synthetic Chrome 1"),
        (codex, "codex-cli 0.test"),
    ):
        path.write_text(f'#!/bin/sh\nprintf "%s\\n" "{output}"\n')
        path.chmod(0o700)
    npm.write_text("// synthetic npm entry\n")
    config = RuntimeConfig(
        home=str(root / "alice"),
        codex_binary=str(codex),
        codex_version="codex-cli 0.test",
        codex_sha256=sha256_file(codex),
    )
    config.prepare_directories()
    config.save()
    config.write_codex_config()
    calls = []

    def fake_run(command, *, cwd, env, log, timeout):
        calls.append(command)
        assert command[2] == "ci" and "--ignore-scripts" in command
        assert "npx" not in command and "--save" not in command
        assert env["HOME"] != str(Path.home())
        # npm's /tmp alias behavior once produced location-dependent lock keys.
        # Exercise the actual packaged resource copied into a new candidate.
        lock = json.loads((cwd / "package-lock.json").read_text())
        assert all(key == "" or key.startswith("node_modules/") for key in lock["packages"])
        module = cwd / "node_modules/@playwright/mcp"
        module.mkdir(parents=True)
        (module / "cli.js").write_text("// synthetic installed entry\n")
        log.write_text("synthetic install completed")

    def fake_verify(binary, settings):
        calls.append("native_verify")
        assert binary == codex
        assert Path(settings["args"][0]).is_file()
        assert settings["enabled_tools"] == list(browser.TOOLS)
        assert settings["default_tools_approval_mode"] == "prompt"
        assert settings["required"] is False
        return {"status": "passed", "model_calls": 0, "remaining_owned_processes": []}

    monkeypatch.setattr(browser, "_run", fake_run)
    monkeypatch.setattr(browser, "verify", fake_verify)
    return config, {"node": node, "npm_cli": npm, "browser_executable": chrome}, calls


def test_install_merges_only_browser_then_reuses_verified_unchanged_artifacts(setup):
    config, arguments, calls = setup
    path = config.codex_home / "config.toml"
    path.write_text(
        path.read_text() + '\n# operator extension\n[mcp_servers.other]\ncommand="keep-me"\n'
    )
    result = browser.install(config, **arguments)
    assert result["installed"] and result["native_verified"]
    text = path.read_text()
    settings = tomllib.loads(text)
    assert "# operator extension" in text
    assert settings["mcp_servers"]["other"]["command"] == "keep-me"
    assert "alice" in settings["mcp_servers"]
    assert len(calls) == 2
    assert browser.install(config, **arguments) == result
    assert len(calls) == 2
    assert path.read_text() == text
    config.write_codex_config()
    assert browser.status(config)["native_verified"]


def test_status_is_read_only_and_missing_is_not_installed(setup):
    config, _, calls = setup
    before = sorted(str(p.relative_to(config.root)) for p in config.root.rglob("*"))
    assert browser.status(config) == {
        "installed": False,
        "native_verified": False,
        "server": "alice_browser",
    }
    assert sorted(str(p.relative_to(config.root)) for p in config.root.rglob("*")) == before
    assert calls == []


@pytest.mark.parametrize("mutation", ["package", "node", "config"])
def test_verified_status_detects_artifact_or_configuration_drift(setup, mutation):
    config, arguments, _ = setup
    installed = browser.install(config, **arguments)
    if mutation == "package":
        Path(installed["runtime_root"], "node_modules/@playwright/mcp/cli.js").write_text("changed")
    elif mutation == "node":
        arguments["node"].write_text("changed")
    else:
        path = config.codex_home / "config.toml"
        path.write_text(path.read_text().replace('"--headless", ', ""))
    result = browser.status(config)
    assert not result["installed"] and not result["native_verified"] and result["error"]


@pytest.mark.parametrize(
    "existing",
    ['[mcp_servers.alice_browser]\ncommand="operator-owned"\n', 'mcp_servers="invalid"\n'],
)
def test_unowned_or_invalid_namespace_is_preserved_before_install(setup, existing):
    config, arguments, calls = setup
    path = config.codex_home / "config.toml"
    path.write_text(existing)
    with pytest.raises(ValueError):
        browser.install(config, **arguments)
    assert path.read_text() == existing
    assert calls == []


def test_active_service_lock_rejects_install(setup):
    config, arguments, calls = setup
    with SingletonLock(config.root / "state/service.lock"):
        with pytest.raises(RuntimeError, match="Another Alice"):
            browser.install(config, **arguments)
    assert calls == []


@pytest.mark.parametrize("responds", [True, False])
def test_control_presence_is_checked_and_uncertainty_is_not_stopped(setup, monkeypatch, responds):
    config, arguments, calls = setup
    config.control_socket.touch()
    checked = []

    async def request(socket, action, *, timeout):
        checked.append((socket, action))
        if not responds:
            raise TimeoutError("synthetic unresponsive control")
        return {"ready": True}

    monkeypatch.setattr(browser, "request", request)
    try:
        with pytest.raises(RuntimeError, match="Stop Alice|uncertain"):
            browser.install(config, **arguments)
        assert checked == [(config.control_socket, "status")]
        assert calls == []
    finally:
        config.control_socket.unlink()


def test_failed_native_verification_keeps_original_config_and_diagnostics(setup, monkeypatch):
    config, arguments, _ = setup
    path = config.codex_home / "config.toml"
    original = path.read_bytes()
    monkeypatch.setattr(
        browser, "verify", lambda *_: {"status": "failed", "error": "synthetic browser failure"}
    )
    with pytest.raises(RuntimeError, match="native verification failed"):
        browser.install(config, **arguments)
    assert path.read_bytes() == original
    assert not browser.status(config)["installed"]
    reports = list((config.root / "extensions/browser").glob("*/verification.json"))
    assert len(reports) == 1 and json.loads(reports[0].read_text())["status"] == "failed"


def test_concurrent_config_edit_is_not_lost_after_native_verification(setup, monkeypatch):
    config, arguments, _ = setup
    path = config.codex_home / "config.toml"
    original = path.read_text()

    def concurrent(*_):
        path.write_text(original + "\n# concurrent operator edit\n")
        return {"status": "passed"}

    monkeypatch.setattr(browser, "verify", concurrent)
    with pytest.raises(RuntimeError, match="concurrent edit preserved"):
        browser.install(config, **arguments)
    assert path.read_text() == original + "\n# concurrent operator edit\n"
    assert not browser.status(config)["installed"]


def test_failed_npm_install_preserves_config_and_creates_no_active_manifest(setup, monkeypatch):
    config, arguments, _ = setup
    path = config.codex_home / "config.toml"
    original = path.read_bytes()

    def fail(*_, **__):
        raise RuntimeError("synthetic npm failed")

    monkeypatch.setattr(browser, "_run", fail)
    with pytest.raises(RuntimeError, match="npm failed"):
        browser.install(config, **arguments)
    assert path.read_bytes() == original
    assert not (config.root / "state/browser.json").exists()
    assert len(list((config.root / "extensions/browser").glob("*/failure.json"))) == 1


def test_outside_package_symlink_invalidates_status(setup):
    config, arguments, _ = setup
    installed = browser.install(config, **arguments)
    Path(installed["runtime_root"], "node_modules/escape").symlink_to(config.root)
    assert "escapes" in browser.status(config)["error"]


def test_config_symlink_is_rejected_without_touching_its_target(setup):
    config, arguments, _ = setup
    path = config.codex_home / "config.toml"
    target = config.root / "operator.toml"
    path.rename(target)
    original = target.read_bytes()
    path.symlink_to(target)
    with pytest.raises(ValueError, match="symbolic link"):
        browser.install(config, **arguments)
    assert target.read_bytes() == original


def test_active_manifest_write_failure_restores_original_config(setup, monkeypatch):
    config, arguments, _ = setup
    path = config.codex_home / "config.toml"
    original = path.read_bytes()
    real_write = browser.write_json

    def fail_manifest(path, value):
        if path == config.root / "state/browser.json":
            raise OSError("synthetic state write failure")
        return real_write(path, value)

    monkeypatch.setattr(browser, "write_json", fail_manifest)
    with pytest.raises(OSError, match="state write failure"):
        browser.install(config, **arguments)
    assert path.read_bytes() == original
    assert not (config.root / "state/browser.json").exists()


def test_old_node_is_rejected_before_npm_or_native_execution(setup):
    config, arguments, calls = setup
    arguments["node"].write_text('#!/bin/sh\nprintf "v18.0.0\\n"\n')
    with pytest.raises(ValueError, match="Node >=20"):
        browser.install(config, **arguments)
    assert calls == []
