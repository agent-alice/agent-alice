"""CLI entry point; all running mutations go through the single service."""

import argparse
import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from uuid import uuid4

from . import __version__
from .config import default_home, initialize_config, load_config
from .control import request
from .files import atomic_write, read_json
from .memory import MemoryStore
from .store import Store


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="alice", description="Alice on Codex CLI")
    p.add_argument("--version", action="version", version=__version__)
    p.add_argument("--home", type=Path, default=default_home())
    commands = p.add_subparsers(dest="command", required=True)
    init = commands.add_parser("init")
    init.add_argument("--codex", required=True, type=Path)
    init.add_argument("--source", type=Path)
    init.add_argument("--login-home", type=Path)
    init.add_argument("--model", default="gpt-6-astra")
    init.add_argument("--no-pin", action="store_true")
    for command in ("check-draft", "summarize-observation"):
        commands.add_parser(command).add_argument("path", type=Path)
    collect = commands.add_parser("collect", help="Collect a bounded read-only HTTP collection")
    collect.add_argument("url")
    collect.add_argument("--subject", required=True)
    collect.add_argument("--collection", required=True)
    collect.add_argument("--output", type=Path, required=True)
    collect.add_argument("--header-env", action="append", default=[], metavar="HEADER=ENV_NAME")
    collect.add_argument("--max-pages", type=int, default=20)
    collect.add_argument("--max-items", type=int, default=2000)
    collect.add_argument("--expected-count", type=int)
    collect.add_argument("--independent", type=Path)
    collect.add_argument("--max-age-seconds", type=float)
    for command in ("serve", "start", "status", "stop", "mcp", "intents", "doctor"):
        commands.add_parser(command)
    resources = commands.add_parser("resources").add_subparsers(dest="operation", required=True)
    for operation in ("status", "refresh"):
        resources.add_parser(operation)
    observation = resources.add_parser("observation")
    observation.add_argument("path", type=Path)
    observation.add_argument("--receipt-id", required=True)
    observation.add_argument("--period", required=True)
    money = resources.add_parser("money")
    money.add_argument("--receipt-id", required=True)
    money.add_argument("--kind", choices=["income", "cost"], required=True)
    money.add_argument("--amount-microusd", type=int, required=True)
    money.add_argument("--source", required=True)
    for command in ("pause", "resume", "chat", "task-status"):
        cmd = commands.add_parser(command)
        cmd.add_argument("--target", default="main" if command in {"chat", "task-status"} else None)
    task_policy = commands.add_parser(
        "task-policy", help="Inspect or explicitly extend persistent task limits"
    ).add_subparsers(dest="operation", required=True)
    policy_status = task_policy.add_parser("status")
    policy_status.add_argument("--target", default="main")
    policy_set = task_policy.add_parser("set")
    policy_set.add_argument("--target", required=True)
    policy_set.add_argument("--request-id", required=True)
    policy_set.add_argument("--max-elapsed-seconds", type=float, required=True)
    policy_set.add_argument("--max-attempts", type=int, required=True)
    policy_set.add_argument("--max-retries", type=int, required=True)
    policy_set.add_argument("--retry-wait-seconds", type=float, required=True)
    policy_set.add_argument("--unchanged-wait-seconds", type=float, required=True)
    ask = commands.add_parser("ask")
    ask.add_argument("text")
    ask.add_argument("--target", default="main")
    ask.add_argument("--request-id")
    ask.add_argument("--wait", type=float, default=0)
    cron = commands.add_parser("cron").add_subparsers(dest="operation", required=True)
    cron.add_parser("list")
    create = cron.add_parser("create")
    create.add_argument("--name", required=True)
    schedule = create.add_mutually_exclusive_group(required=True)
    schedule.add_argument("--every", type=float)
    schedule.add_argument("--at")
    schedule.add_argument("--cron")
    create.add_argument("--prompt", required=True)
    create.add_argument("--target", default="main")
    create.add_argument("--timezone")
    create.add_argument("--heartbeat", action="store_true")
    create.add_argument("--disabled", action="store_true")
    create.add_argument("--catch-up", action="store_true")
    for op in ("delete", "enable", "disable"):
        cron.add_parser(op).add_argument("job_id")
    memory = commands.add_parser("memory").add_subparsers(dest="operation", required=True)
    search = memory.add_parser("search")
    search.add_argument("query")
    search.add_argument("--limit", type=int, default=20)
    read = memory.add_parser("read")
    read.add_argument("source_id")
    read.add_argument("--offset", type=int, default=0)
    read.add_argument("--max-chars", type=int, default=32768)
    snapshot = memory.add_parser("snapshot")
    snapshot.add_argument("source", type=Path)
    snapshot.add_argument(
        "--snapshot-id", help="Reuse an existing ID to resume the same snapshot request"
    )
    snapshot.add_argument("--previous")
    snapshot.add_argument("--final", action="store_true")
    snapshot.add_argument("--include-logs", action="store_true")
    snapshot.add_argument("--external", action="append", default=[], metavar="PREFIX=PATH")
    summary = memory.add_parser("prepare")
    summary.add_argument("level", choices=["L1", "L2", "L3", "L4"])
    summary.add_argument("period")
    commit = memory.add_parser("commit")
    commit.add_argument("batch_id")
    commit.add_argument("candidate", type=Path)
    release = commands.add_parser("release").add_subparsers(dest="operation", required=True)
    build = release.add_parser("build")
    build.add_argument("source", type=Path)
    for op in ("verify", "activate"):
        cmd = release.add_parser(op)
        cmd.add_argument("candidate_id")
        if op == "verify":
            cmd.add_argument("--native", action="store_true")
            cmd.add_argument("--live", action="store_true")
    for op in ("current", "rollback"):
        release.add_parser(op)
    service = commands.add_parser("service").add_subparsers(dest="operation", required=True)
    for op in ("install", "uninstall", "status"):
        service.add_parser(op)
    legacy = commands.add_parser("legacy").add_subparsers(dest="operation", required=True)
    export = legacy.add_parser("export")
    export.add_argument("source", type=Path)
    export.add_argument("--output", type=Path, required=True)
    legacy.add_parser("import").add_argument("plan", type=Path)
    return p


def seed_templates(workspace: Path) -> None:
    MemoryStore(workspace.parent).install_workspace_templates()
    if not (workspace / "HEARTBEAT.md").exists():
        atomic_write(
            workspace / "HEARTBEAT.md",
            b"# Heartbeat\n\nReview existing commitments; waiting is valid.\n",
        )


async def running(config) -> dict | None:
    try:
        return await request(config.control_socket, "status", timeout=2)
    except (OSError, TimeoutError):
        return None


async def start(config) -> dict:
    from .control import ControlError

    supervised = (config.root / "state/supervisor.json").exists()

    async def observe():
        try:
            return await running(config)
        except ControlError:
            # An installed supervisor can reconcile an interrupted status read
            # using owned process identities. Do not weaken running(), which also
            # guards offline data writes, or tolerate unknown mutation outcomes.
            if supervised:
                return None
            raise

    status = await observe()
    if status and status.get("ready"):
        return status
    if supervised:
        from .launchd import start as start_supervisor
        from .launchd import status as supervisor_status

        # The stable supervisor must be allowed to recover a damaged current
        # candidate before resolving the runtime interpreter or writing MCP config.
        await asyncio.to_thread(start_supervisor, config)
        deadline = time.monotonic() + 180
        next_supervisor_check = 0
        while time.monotonic() < deadline:
            status = await observe()
            if status and status.get("ready"):
                return status
            if time.monotonic() >= next_supervisor_check:
                supervision = await asyncio.to_thread(supervisor_status, config)
                state = supervision.get("supervisor") or {}
                if state.get("lifecycle") == "blocked":
                    raise RuntimeError(
                        "Supervisor could not start a compatible verified release; "
                        "inspect service status and private launchd.log"
                    )
                next_supervisor_check = time.monotonic() + 1
            await asyncio.sleep(0.2)
        raise TimeoutError("User service startup unconfirmed; inspect private launchd.log")
    from .files import SingletonLock

    with SingletonLock(config.root / "state/lifecycle.lock"):
        return await _start_unsupervised(config)


async def _start_unsupervised(config) -> dict:
    # Serialize process creation with release switching, including the interval
    # before the daemon has acquired service.lock or created a control socket.
    status = await running(config)
    if status and status.get("ready"):
        return status
    if status:
        # Another caller already started the daemon. A responsive control socket
        # is not readiness, and launching a second daemon cannot fix initialization.
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            status = await running(config)
            if status and status.get("ready"):
                return status
            await asyncio.sleep(0.1)
        raise TimeoutError("Existing service has not become ready; inspect private log")
    log_path = config.root / "logs/alice-service.log"
    from .releases import ReleaseManager

    release = ReleaseManager(config.root).checked_current()
    python = release["python"] if release else sys.executable
    # MCP must use exactly the runtime being started, not a development venv.
    config.write_codex_config(python=python)
    with log_path.open("ab", buffering=0) as log:
        child = subprocess.Popen(
            [python, "-m", "alice_codex", "--home", config.home, "serve"],
            env=config.environment(),
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=log,
            start_new_session=True,
        )
    for _ in range(300):
        status = await running(config)
        if status and status.get("ready"):
            return status
        if child.poll() is not None:
            raise RuntimeError(
                f"Service exited during startup ({child.returncode}); inspect {log_path}"
            )
        await asyncio.sleep(0.1)
    raise TimeoutError(
        "Service startup unconfirmed; inspect status and private log before retrying"
    )


async def execute(args) -> dict | None:
    if args.command == "collect":
        from .collector import collect_collection
        from .files import write_json

        headers = {}
        for entry in args.header_env:
            name, variable = entry.split("=", 1)
            if name.lower() in {key.lower() for key in headers}:
                raise ValueError("Collector header names must be unique")
            headers[name] = variable
        result = await asyncio.to_thread(
            collect_collection,
            args.url,
            subject=args.subject,
            collection=args.collection,
            header_env=headers,
            max_pages=args.max_pages,
            max_items=args.max_items,
            expected_count=args.expected_count,
            independent=read_json(args.independent) if args.independent else None,
            max_age_seconds=args.max_age_seconds,
        )
        write_json(args.output, result)
        return {
            "output": str(args.output),
            "complete": result["summary"]["complete"],
            "summary": result["summary"],
        }
    if args.command in {"check-draft", "summarize-observation"}:
        from .business import check_draft, summarize_observation

        if args.command == "check-draft":
            return check_draft(args.path.read_text())
        return summarize_observation(read_json(args.path))
    if args.command == "init":
        config = initialize_config(
            args.home,
            args.codex,
            source_root=args.source,
            model=args.model,
            pin_binary=not args.no_pin,
            login_home=args.login_home,
        )
        with Store(config.database) as store:
            store.set_autonomy_paused(True)
            from .calendar import register_default_jobs

            register_default_jobs(store, timezone=config.timezone)
        memory = MemoryStore(config.root)
        result = None
        if args.source:
            result = memory.snapshot_legacy(args.source)
        seed_templates(config.workspace)
        return {
            "initialized": config.home,
            "codex_version": config.codex_version,
            "autonomy_paused": True,
            "snapshot": result,
        }
    config = load_config(args.home)
    if args.command == "task-policy":
        params = {"target": args.target}
        if args.operation == "set":
            from dataclasses import asdict

            from .resources import TaskPolicy

            params["request_id"] = args.request_id
            params["policy"] = asdict(
                TaskPolicy(
                    max_elapsed_seconds=args.max_elapsed_seconds,
                    max_attempts=args.max_attempts,
                    max_retries=args.max_retries,
                    retry_wait_seconds=args.retry_wait_seconds,
                    unchanged_wait_seconds=args.unchanged_wait_seconds,
                )
            )
        return await request(config.control_socket, "task_policy_" + args.operation, params)
    if args.command == "resources":
        action, params = "resources_" + args.operation, {}
        if args.operation == "observation":
            raw = read_json(args.path)
            params = {
                "receipt_id": args.receipt_id,
                "document": raw.get("observation", raw),
                "period": args.period,
            }
        elif args.operation == "money":
            params = {
                "receipt_id": args.receipt_id,
                "kind": args.kind,
                "amount_microusd": args.amount_microusd,
                "source": args.source,
            }
        if await running(config):
            return await request(config.control_socket, action, params)
        if args.operation == "refresh":
            raise ValueError("Start Alice before refreshing native account usage")
        from .resources import ResourceLedger

        ledger = ResourceLedger(config.root / "state/resources.sqlite3")
        if args.operation == "status":
            return ledger.status()
        if args.operation == "observation":
            return ledger.record_observation(**params)
        return ledger.record_money(**params)
    if args.command == "serve":
        from .releases import ReleaseManager

        release = ReleaseManager(config.root).checked_current()
        if release and Path(sys.executable).absolute() != Path(release["python"]).absolute():
            os.execve(
                release["python"],
                [release["python"], "-m", "alice_codex", "--home", config.home, "serve"],
                config.environment(),
            )
        from .service import serve

        await serve(config)
        return None
    if args.command == "start":
        return await start(config)
    if args.command == "status":
        status = await running(config)
        if status:
            return status
        state = config.root / "state/runtime.json"
        with Store(config.database) as store:
            return {
                "ready": False,
                "autonomy_paused": store.is_autonomy_paused(),
                "last_state": read_json(state) if state.exists() else None,
            }
    if args.command == "stop":
        if (config.root / "state/supervisor.json").exists():
            from .control import ControlError
            from .launchd import stop as stop_supervisor

            # A failed or starting candidate may not expose a usable control
            # socket. The supervisor still owns and must confirm process cleanup.
            try:
                status = await running(config)
                if status:
                    await request(config.control_socket, "shutdown", timeout=5)
            except (OSError, TimeoutError, ControlError):
                pass
            result = await asyncio.to_thread(stop_supervisor, config)
            if not isinstance(result, dict) or result.get("stopped") is not True:
                raise RuntimeError("Supervisor has not confirmed owned process shutdown")
            return result
        status = await running(config)
        if not status:
            # Do not claim a crashed daemon's orphaned server was stopped.
            state_path = config.root / "state/runtime.json"
            if state_path.exists() and read_json(state_path).get("server"):
                raise RuntimeError(
                    "Daemon is unavailable; run start to recover its recorded server, then stop"
                )
            return {"stopped": True, "already_stopped": True}
        await request(config.control_socket, "shutdown")
        for _ in range(600):
            state_path = config.root / "state/runtime.json"
            if state_path.exists() and read_json(state_path).get("lifecycle") == "stopped":
                return {"stopped": True}
            await asyncio.sleep(0.1)
        raise TimeoutError("Shutdown not yet confirmed; no stop success claimed")
    if args.command == "doctor":
        config.verify_binary()
        with Store(config.database) as store:
            jobs = len(store.list_jobs())
        result = subprocess.run(
            [config.codex_binary, "features", "list"],
            env=config.environment(),
            capture_output=True,
            text=True,
            timeout=20,
        )
        if result.returncode:
            raise RuntimeError(
                "Pinned Codex rejected its configuration; inspect with its doctor command"
            )
        return {
            "codex_binary_verified": True,
            "config_parsed": True,
            "schedule_database_valid": True,
            "job_count": jobs,
            "service": await running(config),
        }
    if args.command == "chat":
        await start(config)
        task = await request(config.control_socket, "thread", {"target": args.target})
        os.execve(
            config.codex_binary,
            [
                config.codex_binary,
                "--remote",
                f"unix://{config.codex_socket}",
                "--cd",
                str(config.workspace),
                "resume",
                task["thread_id"],
            ],
            config.environment(),
        )
    if args.command in {"pause", "resume", "task-status"}:
        return await request(
            config.control_socket, args.command.replace("-", "_"), {"target": args.target}
        )
    if args.command == "ask":
        result = await request(
            config.control_socket,
            "ask",
            {"target": args.target, "text": args.text, "request_id": args.request_id},
        )
        if args.wait:
            deadline = time.monotonic() + args.wait
            while time.monotonic() < deadline:
                intents = await request(config.control_socket, "intents")
                current = next(row for row in intents["intents"] if row["id"] == result["id"])
                if current["status"] in {"completed", "failed", "unknown"}:
                    return {
                        "intent": current,
                        "thread": await request(
                            config.control_socket, "task_status", {"target": args.target}
                        ),
                    }
                await asyncio.sleep(0.2)
            return {
                "intent": result,
                "wait_expired": True,
                "detail": "Observation timeout; execution was not cancelled",
            }
        return result
    if args.command == "intents":
        return await request(config.control_socket, "intents")
    if args.command == "cron":
        if args.operation == "list":
            return await request(config.control_socket, "cron_list")
        if args.operation == "create":
            kind = "every" if args.every is not None else "at" if args.at else "cron"
            return await request(
                config.control_socket,
                "cron_create",
                {
                    "name": args.name,
                    "schedule_type": kind,
                    "schedule_value": getattr(args, kind),
                    "prompt": args.prompt,
                    "target": args.target,
                    "timezone": args.timezone or config.timezone,
                    "kind": "heartbeat" if args.heartbeat else "task",
                    "enabled": not args.disabled,
                    "catch_up": args.catch_up,
                },
            )
        if args.operation == "delete":
            return await request(config.control_socket, "cron_delete", {"job_id": args.job_id})
        return await request(
            config.control_socket,
            "cron_update",
            {"job_id": args.job_id, "changes": {"enabled": args.operation == "enable"}},
        )
    if args.command == "memory":
        memory = MemoryStore(config.root)
        if args.operation == "snapshot":
            if await running(config):
                raise ValueError(
                    "Stop the destination service before seeding an incremental snapshot"
                )
            external = {}
            for value in args.external:
                prefix, path = value.split("=", 1)
                if prefix in external:
                    raise ValueError("External archive source prefixes must be unique")
                external[prefix] = Path(path)
            snapshot_id = args.snapshot_id
            if snapshot_id == "":
                raise ValueError("Snapshot identifier must not be empty")
            if snapshot_id is None:
                snapshot_id = time.strftime("%Y%m%dT%H%M%SZ-", time.gmtime()) + uuid4().hex[:8]
            # Persisted archive/index recovery needs this ID even when the first
            # command is interrupted or indexing fails before stdout is emitted.
            print(
                json.dumps(
                    {
                        "snapshot_id": snapshot_id,
                        "recovery": "Reuse --snapshot-id with the same source and options to resume",
                    }
                ),
                file=sys.stderr,
                flush=True,
            )
            return memory.snapshot_legacy(
                args.source,
                snapshot_id=snapshot_id,
                previous_snapshot_id=args.previous,
                final=args.final,
                include_logs=args.include_logs,
                source_roots=external or None,
            )
        if args.operation == "search":
            return {
                "sources": memory.search(args.query, limit=args.limit),
                "index": memory.index_status(),
            }
        if args.operation == "read":
            return memory.read_source(
                args.source_id, offset_chars=args.offset, max_chars=args.max_chars
            )
        if args.operation == "prepare":
            return memory.prepare_summary(args.level, args.period, timezone=config.timezone)
        return memory.commit_summary(args.batch_id, read_json(args.candidate))
    if args.command == "release":
        from .releases import ReleaseManager

        releases = ReleaseManager(config.root)
        if args.operation == "build":
            return {"candidate_id": releases.build(args.source, codex_binary=config.codex_binary)}
        if args.operation == "verify":
            return releases.verify(args.candidate_id, native=args.native, live=args.live)
        if args.operation in {"activate", "rollback"}:
            from .files import SingletonLock
            from .launchd import _assert_owned_stopped, status as supervisor_status

            # Socket absence does not prove that startup/cleanup is idle. The
            # same lifecycle lock guards launchd transitions and direct start;
            # owner locks cover a running or interrupted supervisor/daemon.
            with (
                SingletonLock(config.root / "state/lifecycle.lock"),
                SingletonLock(config.root / "state/bootstrap.lock"),
                SingletonLock(config.root / "state/service.lock"),
            ):
                if (config.root / "state/supervisor.json").exists():
                    supervision = await asyncio.to_thread(supervisor_status, config)
                    if supervision["loaded"]:
                        raise ValueError("Stop the installed supervisor before switching its release")
                _assert_owned_stopped(config)
                if await running(config):
                    raise ValueError("Stop the service before switching its release")
                if args.operation == "activate":
                    return releases.activate(args.candidate_id)
                return releases.rollback()
        return releases.current()
    if args.command == "service":
        from . import launchd

        if args.operation == "status":
            return launchd.status(config)
        if await running(config):
            stop_args = argparse.Namespace(command="stop", home=args.home)
            await execute(stop_args)
        if args.operation == "uninstall":
            return launchd.uninstall(config)
        return launchd.install(config)
    if args.command == "legacy":
        from .legacy import export_legacy_plan, import_legacy_plan

        if args.operation == "export":
            from .files import write_json

            plan = export_legacy_plan(args.source)
            write_json(args.output, plan)
            return {
                "plan_path": str(args.output),
                "detail": "Private review plan written; no schedules activated",
            }
        if await running(config):
            raise ValueError("Stop the service before importing legacy schedules")
        with Store(config.database) as store:
            return import_legacy_plan(store, read_json(args.plan))
    raise ValueError("Unsupported command")


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.command == "mcp":
        from .mcp import main as mcp_main

        mcp_main(["--home", str(args.home)])
        return 0
    try:
        result = asyncio.run(execute(args))
        if result is not None:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        if args.command == "check-draft" and not result["passed"]:
            return 2
        if args.command in {"summarize-observation", "collect"} and not result["complete"]:
            return 2
        return 0
    except (Exception, KeyboardInterrupt) as error:
        print(f"alice: {type(error).__name__}: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
