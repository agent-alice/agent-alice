"""Partition tool routing and calendar receipts use synthetic local data only."""

import datetime as dt
from importlib import resources
import json
from pathlib import Path
from string import Template
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from mcp.server.fastmcp.exceptions import ToolError

from alice_codex import mcp as alice_mcp
from alice_codex.calendar import dispatch_completion, prepare_dispatch
from alice_codex.config import UNATTENDED_TOOLS
from alice_codex.service import Service
from alice_codex.store import DispatchEvent, Job


def control_fixture(tmp_path, monkeypatch):
    host = Service.__new__(Service)
    host.ready, host.stopping = True, False
    host.memory = Mock()
    socket = tmp_path / "synthetic-control.sock"
    monkeypatch.setattr(alice_mcp, "load_config", lambda _: SimpleNamespace(control_socket=socket))
    requests = []

    async def request(path, action, params, *, timeout):
        assert path == socket and timeout == 110
        requests.append((action, params))
        return await host.handle(action, params)

    monkeypatch.setattr(alice_mcp, "request", request)
    return host, alice_mcp.create_server(tmp_path), requests


async def test_partition_mcp_catalog_and_service_forwarding(tmp_path, monkeypatch):
    host, server, requests = control_fixture(tmp_path, monkeypatch)
    tools = {tool.name: tool for tool in await server.list_tools()}
    next_name = "memory_summary_partition_next"
    commit_name = "memory_commit_summary_partition"
    assert {next_name, commit_name, "memory_prepare_summary", "memory_commit_summary"} <= set(
        UNATTENDED_TOOLS
    )
    assert set(UNATTENDED_TOOLS) <= set(tools)
    assert tools[next_name].inputSchema["required"] == ["batch_id"]
    assert tools[next_name].inputSchema["properties"]["limit"]["default"] == 4
    assert set(tools[commit_name].inputSchema["required"]) == {"batch_id", "node_id", "candidate"}
    batch_id, node_id = "a" * 64, "node-fixture"
    ready = {
        "batch_id": batch_id,
        "complete": False,
        "ready": [{"node_id": node_id, "source_count": 1}],
        "completed_nodes": 0,
        "total_nodes": 1,
    }
    host.memory.summary_partition_next.return_value = ready
    result = await server.call_tool(next_name, {"batch_id": batch_id})
    assert json.loads(result[0].text) == ready
    assert requests[-1] == ("summary_partition_next", {"batch_id": batch_id, "limit": 4})
    host.memory.summary_partition_next.assert_called_once_with(batch_id=batch_id, limit=4)
    await server.call_tool(next_name, {"batch_id": batch_id, "limit": 2})
    host.memory.summary_partition_next.assert_called_with(batch_id=batch_id, limit=2)

    candidate = {
        "content": "The synthetic source is malformed; its content remains unknown.",
        "source_ids": [],
        "covered_source_ids": [],
        "missing": [{"source_id": "s_" + "b" * 64, "reason": "invalid_json"}],
    }
    receipt = {
        "batch_id": batch_id,
        "node_id": node_id,
        "already_committed": False,
        "complete": True,
    }
    host.memory.commit_summary_partition.return_value = receipt
    arguments = {"batch_id": batch_id, "node_id": node_id, "candidate": candidate}
    result = await server.call_tool(commit_name, arguments)
    assert json.loads(result[0].text) == receipt
    assert requests[-1] == ("commit_summary_partition", arguments)
    host.memory.commit_summary_partition.assert_called_once_with(**arguments)


async def test_partition_control_rejections_remain_errors(tmp_path, monkeypatch):
    host, server, _ = control_fixture(tmp_path, monkeypatch)
    host.memory.summary_partition_next.side_effect = ValueError(
        "Synthetic partition source changed"
    )
    with pytest.raises(ToolError, match="partition source changed"):
        await server.call_tool("memory_summary_partition_next", {"batch_id": "a" * 64})
    host.stopping = True
    with pytest.raises(ToolError, match="not accepting work"):
        await server.call_tool(
            "memory_commit_summary_partition",
            {"batch_id": "a" * 64, "node_id": "node-fixture", "candidate": {}},
        )
    host.memory.commit_summary_partition.assert_not_called()


def test_calendar_mixed_batches_require_whole_window_partition_receipt(tmp_path):
    state = tmp_path / "state"
    small_id, partition_id, child_id = "1" * 64, "2" * 64, "3" * 64

    def prepare(level, period, **kwargs):
        partitioned = period == "2026-09-02"
        batch_id = partition_id if partitioned else small_id
        result = {
            "batch_id": batch_id,
            "manifest_path": str(tmp_path / batch_id / "manifest.json"),
            "candidate_path": str(tmp_path / "workspace" / f"{batch_id}.json"),
            "prompt": "Synthetic coordinator" if partitioned else "Synthetic small batch",
            "source_count": 65 if partitioned else 1,
            "workspace_path": str(tmp_path / "workspace"),
        }
        if partitioned:
            result["strategy"] = "partitioned-v1"
        return result

    memory = SimpleNamespace(state=state, prepare_summary=prepare)
    job = Job(
        "synthetic-summary",
        "summary:L2",
        "cron",
        "15 0 * * *",
        timezone="UTC",
        target="summary:L2",
    )
    event = DispatchEvent(
        "synthetic-event",
        job.id,
        job,
        dt.datetime(2026, 9, 2, 0, 15, tzinfo=dt.timezone.utc).timestamp(),
        dt.datetime(2026, 9, 3, 0, 15, tzinfo=dt.timezone.utc).timestamp(),
        True,
        "sending",
    )
    plan = prepare_dispatch(event, memory, now=dt.datetime(2026, 10, 1, tzinfo=dt.timezone.utc))
    assert plan["schema_version"] == 1
    assert plan["prepared"][0]["batch_id"] == small_id
    assert "strategy" not in plan["prepared"][0]
    assert plan["prepared"][1]["strategy"] == "partitioned-v1"
    assert "memory_summary_partition_next" in plan["prompt"]
    assert "memory_commit_summary_partition" in plan["prompt"]
    assert "memory_commit_summary" in plan["prompt"]
    assert json.loads(Path(plan["plan_path"]).read_text())["prepared"] == plan["prepared"]

    receipts = state / "commits"
    receipts.mkdir()
    for batch_id in (small_id, child_id):
        (receipts / f"{batch_id}.json").write_text(
            json.dumps({"batch_id": batch_id, "status": "committed"})
        )
    assert dispatch_completion(plan, memory)["pending_batch_ids"] == [partition_id]
    root_receipt = receipts / f"{partition_id}.json"
    root_receipt.write_text(json.dumps({"batch_id": partition_id, "status": "pending"}))
    assert not dispatch_completion(plan, memory)["complete"]
    root_receipt.write_text(json.dumps({"batch_id": partition_id, "status": "committed"}))
    assert dispatch_completion(plan, memory)["complete"]


def test_partition_templates_expose_the_agreed_substitution_contract(tmp_path):
    templates = resources.files("alice_codex").joinpath("templates")
    batch_id, node_id = "a" * 64, "node-fixture"
    manifest_path, candidate_path = tmp_path / "manifest.json", tmp_path / "candidate.json"
    coordinator = Template(templates.joinpath("chronicle-partition-coordinator.md").read_text())
    coordinator_text = coordinator.substitute(batch_id=batch_id, manifest_path=manifest_path)
    assert batch_id in coordinator_text and str(manifest_path) in coordinator_text
    node = Template(templates.joinpath("chronicle-partition-node.md").read_text())
    node_text = node.substitute(
        batch_id=batch_id,
        node_id=node_id,
        level="L1",
        start="2026-09-01T00:00:00+08:00",
        end="2026-09-01T02:00:00+08:00",
        manifest_path=manifest_path,
        candidate_path=candidate_path,
    )
    assert batch_id in node_text and node_id in node_text
    assert str(manifest_path) in node_text and str(candidate_path) in node_text
