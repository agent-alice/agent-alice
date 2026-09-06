"""The actual MCP tool exposes local executable metadata without dispatching work."""

from dataclasses import asdict
import json
from pathlib import Path
import sys

import pytest

from alice_codex.config import RuntimeConfig, UNATTENDED_TOOLS
from alice_codex.files import write_json
from alice_codex.mcp import create_server
from alice_codex import evaluation


@pytest.mark.asyncio
async def test_runtime_info_is_callable_without_running_control_service(tmp_path):
    config = RuntimeConfig(str(tmp_path / "runtime"), "/usr/bin/true", "fixture", "unused")
    config.prepare_directories()
    write_json(config.root / "config.json", asdict(config))
    server = create_server(config.root)
    assert "runtime_info" in UNATTENDED_TOOLS
    result = await server.call_tool("runtime_info", {})
    info = json.loads(result[0].text)
    assert info["python"] == str(Path(sys.executable).absolute())
    assert info["commands"]["collect"][-3:] == ["--home", config.home, "collect"]
    assert all(command[:3] == [info["python"], "-I", "-m"] for command in info["commands"].values())
    assert info["environment_source"] == "running_mcp_process"
    tasks = evaluation.read_json(info["learning_inputs"]["tasks"])
    oracle = evaluation.read_json(info["learning_inputs"]["oracle"])
    evaluation.validate_suite(tasks, oracle)
    assert not config.control_socket.exists()
