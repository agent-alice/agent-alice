"""Stdio MCP tools delegate to Alice's single private control service."""

import argparse
from pathlib import Path

from mcp.server.fastmcp import FastMCP

from .config import default_home, load_config
from .control import request


def create_server(home: Path) -> FastMCP:
    config = load_config(home)
    server = FastMCP(
        "alice", instructions="Alice continuity tools. Use native Codex for execution and context."
    )

    async def call(action, **params):
        return await request(config.control_socket, action, params, timeout=110)

    @server.tool()
    async def status() -> dict:
        """Inspect runtime, task roots, persisted pause and pinned Codex version."""
        return await call("status")

    @server.tool()
    async def cron_create(
        name: str,
        schedule_type: str,
        schedule_value: str | float,
        prompt: str,
        target: str = "main",
        timezone: str = "Asia/Shanghai",
        kind: str = "task",
        enabled: bool = True,
        catch_up: bool = False,
    ) -> dict:
        """Persist an at/every/cron schedule. Every is seconds; cron is five fields.

        Heartbeats should read HEARTBEAT.md and use kind=heartbeat. A schedule does
        not override a user pause. Accepted delivery is not verified task success.
        """
        return await call(
            "cron_create",
            name=name,
            schedule_type=schedule_type,
            schedule_value=schedule_value,
            prompt=prompt,
            target=target,
            timezone=timezone,
            kind=kind,
            enabled=enabled,
            catch_up=catch_up,
        )

    @server.tool()
    async def cron_list() -> dict:
        """List persisted schedules and their enabled state."""
        return await call("cron_list")

    @server.tool()
    async def cron_update(job_id: str, changes: dict) -> dict:
        """Update explicitly selected schedule fields without recreating the job."""
        return await call("cron_update", job_id=job_id, changes=changes)

    @server.tool()
    async def cron_delete(job_id: str) -> dict:
        """Remove a schedule; this does not undo an already executed action."""
        return await call("cron_delete", job_id=job_id)

    @server.tool()
    async def task_start(name: str, prompt: str, request_id: str) -> dict:
        """Send work to an independent named root with its own context and children.

        Prefer native subagents for short work. Use a stable UUID request_id, and
        do not retry an unknown outcome under a new UUID. Acknowledgement is not completion.
        This autonomous delegation respects Alice's persisted pause and resource limits.
        """
        return await call("ask", target=name, text=prompt, request_id=request_id, automatic=True)

    @server.tool()
    async def task_status(name: str = "main") -> dict:
        """Read native task history, evidence and persisted policy/usage/decision.

        Limits may be not_configured on older runtimes. Waiting and exhausted are
        host decisions, not verified business outcomes. This does not start a turn,
        extend limits, clear usage or undo a persisted pause.
        """
        return await call("task_status", target=name)

    @server.tool()
    async def autonomy_pause(target: str | None = None) -> dict:
        """Persist a pause and stop owned work; omit target to pause all autonomy."""
        return await call("pause", target=target)

    @server.tool()
    async def autonomy_resume(target: str | None = None) -> dict:
        """Resume only on explicit user instruction. A heartbeat cannot undo pause."""
        return await call("resume", target=target)

    @server.tool()
    async def memory_search(query: str, limit: int = 20, kind: str | None = None) -> dict:
        """Find experience sources by text. Results are metadata; read selected IDs."""
        return await call("memory_search", query=query, limit=limit, kind=kind)

    @server.tool()
    async def memory_read(source_id: str, offset_chars: int = 0, max_chars: int = 32768) -> dict:
        """Read exact private source text with provenance and pagination."""
        return await call(
            "memory_read", source_id=source_id, offset_chars=offset_chars, max_chars=max_chars
        )

    @server.tool()
    async def memory_prepare_summary(
        level: str, period: str, timezone: str = "Asia/Shanghai"
    ) -> dict:
        """Freeze a closed calendar period; returns source IDs, prompt and candidate path.

        L1 YYYY-MM-DDTHH:00 even hour; L2 YYYY-MM-DD; L3 YYYY-Www; L4 YYYY-MM.
        Read sources and write candidate; never fabricate missing coverage.
        For strategy=partitioned-v1, follow the returned coordinator prompt and
        use memory_summary_partition_next plus memory_commit_summary_partition.
        """
        return await call("memory_prepare", level=level, period=period, timezone=timezone)

    @server.tool()
    async def memory_commit_summary(batch_id: str, candidate: dict) -> dict:
        """Validate source coverage and atomically commit a generated summary.

        candidate has content, source_ids, covered_source_ids, missing. Validation
        checks structure/provenance, not the truth of a model's interpretation.
        """
        return await call("memory_commit", batch_id=batch_id, candidate=candidate)

    @server.tool()
    async def memory_summary_partition_next(batch_id: str, limit: int = 4) -> dict:
        """Read ready bounded summary nodes and the host's whole-batch completion state.

        This is not a claim or a new model task. Dispatch each node_id at most once
        concurrently through native Codex children; committed nodes are omitted.
        An empty ready list alone does not establish completion or permit replay.
        """
        return await call("summary_partition_next", batch_id=batch_id, limit=limit)

    @server.tool()
    async def memory_commit_summary_partition(batch_id: str, node_id: str, candidate: dict) -> dict:
        """Validate and commit one immutable summary node, idempotently.

        Use the four candidate fields from its prompt, with content at most 16 KiB.
        Only host complete=true establishes final whole-window commit. A child
        success claim or an intermediate node receipt is not whole-batch completion.
        """
        return await call(
            "commit_summary_partition", batch_id=batch_id, node_id=node_id, candidate=candidate
        )

    @server.tool()
    async def resources_status() -> dict:
        """Inspect observed native tokens, quota, income receipts and virtual accounting state.

        Token counts are not a dollar invoice. Unknown quota is not zero.
        Virtual survival accounting is disabled pending explicit reconciliation.
        """
        return await call("resources_status")

    @server.tool()
    async def resources_record_observation(receipt_id: str, document: dict, period: str) -> dict:
        """Record raw bounded collector evidence for YYYY-MM-DD; validate coverage again.

        Missing data never earns credit. Reuse the same receipt_id on retries.
        This tool cannot enable virtual accounting or invent paid usage/income.
        """
        return await call(
            "resources_observation", receipt_id=receipt_id, document=document, period=period
        )

    return server


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--home", type=Path, default=default_home())
    args = parser.parse_args(argv)
    create_server(args.home).run(transport="stdio")


if __name__ == "__main__":
    main()
