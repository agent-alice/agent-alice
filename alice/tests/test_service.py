"""Regressions for lifecycle faults and durable intent reconciliation."""

import asyncio
import json
import subprocess
import sys
from unittest.mock import AsyncMock, Mock

import pytest

from alice_codex.config import RuntimeConfig
from alice_codex.codex import CodexClient
from alice_codex.journal import NativeJournal
from alice_codex.memory import MemoryError
from alice_codex.rpc import RpcError
from alice_codex.scheduler import RejectedDispatch
from alice_codex.service import Service
import alice_codex.service as service_module


@pytest.fixture
def service(tmp_path):
    config = RuntimeConfig(str(tmp_path), "/usr/bin/true", "codex-cli fixture", "unused")
    config.prepare_directories()
    item = Service(config)
    item.ready = True
    item.codex = Mock()
    item.codex.thread_start = AsyncMock(return_value={"thread": {"id": "new-root"}})
    item.codex.thread_read = AsyncMock(
        return_value={"thread": {"id": "root", "status": {"type": "idle"}}}
    )
    item.codex.turn_start = AsyncMock(return_value={"turn": {"id": "turn-1"}})
    item.codex.queue_add = AsyncMock(return_value={"data": []})
    item.codex.stop_tree = AsyncMock(return_value={"stopped": []})
    item.codex.find_turn_by_client_id = AsyncMock(return_value=None)
    item.rpc = Mock()
    item.rpc.request = AsyncMock(return_value={"data": []})
    item.rpc.close = AsyncMock()
    yield item
    item.store.close()
    for socket in config.socket_dir.iterdir():
        socket.unlink()
    config.socket_dir.rmdir()


async def test_disk_failure_still_terminates_and_waits_owned_child(service, tmp_path):
    service.process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        "import time; time.sleep(120)",
        start_new_session=True,
        stdin=subprocess.DEVNULL,
    )
    service.save = Mock(side_effect=OSError("disk full"))
    log = (tmp_path / "owned.log").open("ab")
    with pytest.raises(OSError, match="disk full"):
        await service._shutdown(log)
    assert service.process.returncode is not None
    assert log.closed
    service.rpc.close.assert_awaited_once()


async def test_stop_arriving_during_read_prevents_input_rpc(service):
    task = {"thread_id": "root", "paused": False, "has_input": True}
    service.state["tasks"]["main"] = task
    service.ensure_thread = AsyncMock(return_value=task)

    async def stopped_during_read(*args, **kwargs):
        service.stopping = True
        return {"thread": {"status": {"type": "idle"}}}

    service.codex.thread_read = AsyncMock(side_effect=stopped_during_read)
    with pytest.raises(RejectedDispatch):
        await service.submit("main", "write result", intent_id="stable")
    service.codex.turn_start.assert_not_called()
    service.codex.queue_add.assert_not_called()
    assert service.state["intents"]["stable"]["status"] == "failed"


async def test_request_id_retries_same_input_but_rejects_different_input(service):
    first = await service.submit("main", "write result", intent_id="stable")
    assert await service.submit("main", "write result", intent_id="stable") == first
    service.codex.turn_start.assert_awaited_once()
    with pytest.raises(ValueError, match="different input"):
        await service.submit("main", "publish different result", intent_id="stable")
    service.codex.turn_start.assert_awaited_once()


async def test_empty_thread_only_can_be_replaced_without_history_or_intent(service):
    service.state["tasks"]["main"] = {"thread_id": "empty", "has_input": False}
    service.codex.thread_read.side_effect = RpcError("no rollout found for thread id empty", -32600)
    result = await service.ensure_thread("main")
    assert result["thread_id"] == "new-root"
    assert service.state["replaced_empty_threads"] == ["empty"]
    service.state["tasks"]["main"] = {"thread_id": "important", "has_input": True}
    with pytest.raises(RpcError):
        await service.ensure_thread("main")
    assert service.state["tasks"]["main"]["thread_id"] == "important"
    service.codex.thread_start.assert_awaited_once()


async def test_unknown_dispatch_absence_never_replays(service):
    service.state["intents"]["stable"] = {"id": "stable", "status": "unknown", "thread_id": "root"}
    await service.reconcile()
    await service.reconcile()
    assert service.state["intents"]["stable"]["status"] == "unknown"
    service.codex.turn_start.assert_not_called()
    service.codex.queue_add.assert_not_called()


async def test_queued_intent_recovered_by_native_client_id_and_turn_receipt(service):
    service.state["intents"]["stable"] = {"id": "stable", "status": "queued", "thread_id": "root"}
    service.codex.find_turn_by_client_id.return_value = "turn-2"
    service.rpc.request.side_effect = [
        {"data": [{"id": "turn-later", "status": "completed"}], "nextCursor": "older"},
        {"data": [{"id": "turn-2", "status": "completed"}], "nextCursor": None},
    ]
    await service.reconcile()
    assert service.state["intents"]["stable"]["status"] == "completed"
    assert service.state["intents"]["stable"]["turn_id"] == "turn-2"
    service.codex.turn_start.assert_not_called()


def test_background_fault_stops_even_if_pause_cannot_be_persisted(service):
    service.store.set_autonomy_paused = Mock(side_effect=OSError("disk full"))
    service._fail("archive writer failed")
    assert service.stop_event.is_set()
    assert service.stopping and not service.ready


def test_corrupt_runtime_preserved_instead_of_silent_empty_state(tmp_path):
    path = tmp_path / "state/runtime.json"
    path.parent.mkdir()
    path.write_text('{"version":99}')
    before = path.read_bytes()
    config = RuntimeConfig(str(tmp_path), "/usr/bin/true", "fixture", "unused")
    with pytest.raises(ValueError):
        Service(config)
    assert path.read_bytes() == before


def test_module_entrypoint_reports_failure_as_nonzero(tmp_path):
    result = subprocess.run(
        [sys.executable, "-m", "alice_codex", "--home", str(tmp_path), "status"],
        text=True,
        capture_output=True,
    )
    assert result.returncode == 1
    assert "not initialized" in result.stderr


async def test_native_tui_interruption_persists_pause_before_next_heartbeat(service):
    service.state["tasks"]["main"] = {"thread_id": "root", "paused": False, "has_input": True}
    stopped = asyncio.Event()
    service.codex.stop_tree.side_effect = lambda *a, **k: stopped.set()
    service._on_notification(
        {
            "method": "turn/completed",
            "params": {"threadId": "root", "turn": {"id": "tui-only", "status": "interrupted"}},
        }
    )
    # No Alice intent exists: the user acted through the native TUI.
    assert service.state["intents"] == {}
    assert service.store.is_autonomy_paused()
    assert json.loads(service.path.read_text())["tasks"]["main"]["paused"] is True
    with pytest.raises(RejectedDispatch):
        await service.submit("main", "heartbeat", automatic=True)
    service.codex.turn_start.assert_not_called()
    worker = asyncio.create_task(service._record_events())
    try:
        await asyncio.wait_for(stopped.wait(), 2)
        service.codex.stop_tree.assert_awaited_once_with("root")
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


def test_child_interruption_does_not_pause_unrelated_roots(service):
    service.state["tasks"]["main"] = {"thread_id": "root", "paused": False}
    service._on_notification(
        {
            "method": "turn/completed",
            "params": {"threadId": "child", "turn": {"id": "child-turn", "status": "interrupted"}},
        }
    )
    assert not service.store.is_autonomy_paused()
    assert not service.state["tasks"]["main"]["paused"]


async def test_new_root_during_busy_read_defers_dispatch_without_crashing(service):
    service.state["tasks"]["main"] = {"thread_id": "root", "paused": False}

    async def add_concurrent_root(*args, **kwargs):
        service.state["tasks"]["other"] = {"thread_id": "other-root", "paused": False}
        return {"thread": {"status": {"type": "idle"}}}

    service.codex.thread_read.side_effect = add_concurrent_root
    assert await service.is_busy("new") is True
    service.codex.thread_read.assert_awaited_once()
    assert not service.stop_event.is_set()


async def test_missing_history_quarantines_only_unknown_task_and_never_replays(service):
    service.state["tasks"] = {
        "lost": {"thread_id": "lost-root", "has_input": False},
        "good": {"thread_id": "good-root", "has_input": True},
    }
    service.state["intents"] = {
        "lost-request": {"id": "lost-request", "thread_id": "lost-root", "status": "unknown"},
        "good-request": {
            "id": "good-request",
            "thread_id": "good-root",
            "status": "accepted",
            "turn_id": "good-turn",
        },
    }
    service.codex.find_turn_by_client_id.side_effect = RpcError(
        "no rollout found for thread id lost-root", -32600
    )
    service.rpc.request.return_value = {"data": [{"id": "good-turn", "status": "completed"}]}
    await service.reconcile()
    await service.reconcile()
    assert service.state["intents"]["lost-request"]["status"] == "unknown"
    assert service.state["intents"]["good-request"]["status"] == "completed"
    assert service.state["tasks"]["lost"]["thread_id"] == "lost-root"
    assert service.state["tasks"]["lost"]["paused"] is True
    assert not service.store.is_autonomy_paused()
    assert await service.is_busy("lost") is True
    assert await service.is_busy("good") is False
    assert not service.stop_event.is_set()
    service.codex.turn_start.assert_not_called()
    service.codex.thread_start.assert_not_called()
    service.codex.find_turn_by_client_id.assert_awaited_once()


async def test_native_quota_blocks_automatic_work_without_enabling_virtual_budget(service):
    service.rpc.request.return_value = {"rateLimits": {"primary": {"usedPercent": 100}}}
    status = await service.refresh_resources()
    assert status["virtual_budget_enabled"] is False
    assert not service.resources.can_dispatch(automatic=True)["allowed"]
    with pytest.raises(RejectedDispatch):
        await service.submit("main", "automatic task", automatic=True)
    service.codex.turn_start.assert_not_called()
    # Unknown refresh cannot invent restored quota. Manual input still reaches
    # native Codex, which makes the authoritative account admission decision.
    service.rpc.request.side_effect = RpcError("usage unavailable")
    await service.refresh_resources()
    assert not service.resources.can_dispatch(automatic=True)["allowed"]
    await service.submit("main", "explicit manual task")
    service.codex.turn_start.assert_awaited_once()


async def test_native_token_notifications_record_usage_once(service):
    usage = {
        "inputTokens": 20,
        "cachedInputTokens": 8,
        "outputTokens": 3,
        "reasoningOutputTokens": 1,
        "totalTokens": 23,
    }
    event = {
        "method": "thread/tokenUsage/updated",
        "params": {
            "threadId": "root",
            "turnId": "turn-1",
            "tokenUsage": {"total": usage, "last": usage},
        },
    }
    service._on_notification(event)
    service._on_notification(event)
    archived = asyncio.Event()
    service.memory.append_event = Mock(side_effect=lambda *a, **k: archived.set())
    service._on_notification(
        {
            "method": "turn/completed",
            "params": {"threadId": "root", "turn": {"id": "turn-1", "status": "completed"}},
        }
    )
    worker = asyncio.create_task(service._record_events())
    try:
        await asyncio.wait_for(archived.wait(), 2)
        status = service.resources.status()
        assert status["tokens"]["threads"]["root"]["totalTokens"] == 23
        assert status["tokens"]["cost_microusd"] is None
        assert status["virtual_budget_enabled"] is False
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


def archived_native_items(service):
    return [
        json.loads(path.read_text())
        for path in (service.memory.state / "events/raw").glob("*.jsonl")
    ]


def history_item(thread, identifier="result"):
    return {
        "turnId": "turn-1",
        "item": {"id": identifier, "type": "agentMessage", "text": f"Evidence from {thread}"},
    }


async def test_startup_archive_discovers_only_owned_history_and_persists_gaps(service):
    service.state["tasks"] = {"main": {"thread_id": "root"}, "missing": {"thread_id": "missing"}}
    service.codex = CodexClient(service.rpc, owned_root_ids=["root", "missing"])
    service.journal = NativeJournal(service.memory, service.rpc, service.codex.owns)

    async def native_request(method, params):
        if method == "thread/list":
            return {
                "data": [
                    {"id": "root"},
                    {"id": "child", "parentThreadId": "root"},
                    {"id": "stranger"},
                    {"id": "foreign-child", "parentThreadId": "stranger"},
                ]
            }
        if method == "thread/loaded/list":
            return {"data": []}
        if params["threadId"] == "missing":
            raise RpcError("no rollout found for thread id missing", -32600)
        if method == "thread/items/list":
            return {"data": [history_item(params["threadId"])]}
        return {"data": []}

    service.rpc.request.side_effect = native_request
    await service.archive_native_history("startup")
    report = json.loads(service.path.read_text())["journal"]
    assert report["reason"] == "startup"
    assert not report["traversal_complete"] and not report["notification_log_reconstructed"]
    assert report["threads"]["missing"]["gaps"][0]["reason"] == "no_rollout"
    assert {r["thread_id"] for r in archived_native_items(service)} == {"root", "child"}
    history_calls = [
        call.args[1]["threadId"]
        for call in service.rpc.request.await_args_list
        if call.args[0] in {"thread/items/list", "thread/turns/list"}
    ]
    assert set(history_calls) == {"root", "child", "missing"}
    assert not service.stop_event.is_set()
    service.codex.close()


async def test_native_inventory_error_is_persisted_as_incomplete(service):
    service.codex.discover_owned_threads = AsyncMock(side_effect=RpcError("inventory unavailable"))
    service.journal = Mock(backfill=AsyncMock())
    await service.archive_native_history("startup")
    report = json.loads(service.path.read_text())["journal"]
    assert report["error"] == "native_inventory_unavailable"
    assert report["traversal_complete"] is False
    assert report["notification_log_reconstructed"] is False
    service.journal.backfill.assert_not_called()


async def test_native_archive_deadline_cancels_backfill_and_reports_gap(service, monkeypatch):
    real_timeout = asyncio.timeout
    monkeypatch.setattr(service_module.asyncio, "timeout", lambda _: real_timeout(0))
    service.codex.discover_owned_threads = AsyncMock(return_value={"root"})
    cancelled = asyncio.Event()

    async def stalled_backfill(threads):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    service.journal = Mock(backfill=AsyncMock(side_effect=stalled_backfill))
    await service.archive_native_history("shutdown")
    assert cancelled.is_set()
    report = json.loads(service.path.read_text())["journal"]
    assert report["error"] == "native_archive_deadline_exceeded"
    assert not report["traversal_complete"] and not report["notification_log_reconstructed"]


@pytest.mark.parametrize("error", [OSError("disk full"), MemoryError("archive damaged")])
async def test_archive_storage_errors_propagate_instead_of_becoming_native_gaps(service, error):
    service.codex.discover_owned_threads = AsyncMock(return_value={"root"})
    service.journal = NativeJournal(service.memory, service.rpc, lambda thread: thread == "root")
    service.rpc.request.return_value = {"data": [history_item("root")]}
    service.memory.append_event = Mock(side_effect=error)
    with pytest.raises(type(error), match=str(error)):
        await service.archive_native_history("startup")
    assert "journal" not in service.state


async def test_shutdown_archive_write_failure_still_closes_rpc_and_owned_process(service, tmp_path):
    service.state["tasks"]["main"] = {"thread_id": "root"}
    service.codex.discover_owned_threads = AsyncMock(return_value={"root"})
    service.journal = NativeJournal(service.memory, service.rpc, lambda thread: thread == "root")
    service.rpc.request.return_value = {"data": [history_item("root")]}
    service.memory.append_event = Mock(side_effect=OSError("archive disk full"))
    service.process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        "import time; time.sleep(120)",
        start_new_session=True,
        stdin=subprocess.DEVNULL,
    )
    log = (tmp_path / "archive-failure.log").open("ab")
    with pytest.raises(OSError, match="archive disk full"):
        await service._shutdown(log)
    assert service.process.returncode is not None
    assert log.closed
    service.rpc.close.assert_awaited_once()
    service.codex.stop_tree.assert_awaited_once()


async def test_run_archives_before_ready_and_shutdown_tail_before_rpc_close(service, monkeypatch):
    # The actual Service.run/NativeJournal/CodexClient path, with a deterministic
    # process/transport boundary. This does not claim real Codex process coverage.
    service.ready = False
    service.state["tasks"]["main"] = {"thread_id": "root"}
    service.config.verify_binary = Mock()
    order, at_close, stopped = [], [], False
    process = Mock(pid=123456789, returncode=None)

    async def spawn(*args, **kwargs):
        service.config.codex_socket.touch()
        return process

    monkeypatch.setattr(service_module.asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setattr(service_module, "process_identity", lambda _: "fixture identity")
    monkeypatch.setattr(service_module, "process_birth", lambda _: "fixture birth")
    monkeypatch.setattr(
        service_module.RpcClient, "connect_unix", AsyncMock(return_value=service.rpc)
    )
    service.rpc.initialize = AsyncMock()

    async def request(method, params):
        if method == "thread/list":
            return {"data": [{"id": "root"}]}
        if method == "thread/items/list":
            order.append("history_after_stop" if stopped else "history_before_ready")
            values = [history_item("root", "before-start")]
            if stopped:
                values.append(history_item("root", "shutdown-tail"))
            return {"data": values}
        return {"data": []}

    service.rpc.request.side_effect = request

    async def stop_tree(*args, **kwargs):
        nonlocal stopped
        order.append("native_stop")
        stopped = True
        process.returncode = 0
        return {"stopped": ["root"]}

    monkeypatch.setattr(service_module.CodexClient, "stop_tree", stop_tree)

    async def close_rpc():
        order.append("rpc_close")
        at_close.append(
            (
                {r["content"]["id"] for r in archived_native_items(service)},
                service.state["journal"]["reason"],
            )
        )

    service.rpc.close.side_effect = close_rpc

    class ControlServer:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

    async def listen(*args, **kwargs):
        order.append("control_listen")
        assert not service.ready
        assert service.state["journal"]["reason"] == "startup"
        assert len(archived_native_items(service)) == 1
        service.config.control_socket.touch()
        return ControlServer()

    monkeypatch.setattr(service_module.asyncio, "start_unix_server", listen)

    async def request_stop_once_ready():
        assert service.ready
        service.stop_event.set()
        await asyncio.Event().wait()

    service._tick = request_stop_once_ready
    await service.run()
    assert order == [
        "history_before_ready",
        "control_listen",
        "native_stop",
        "history_after_stop",
        "rpc_close",
    ]
    assert at_close == [({"before-start", "shutdown-tail"}, "shutdown")]
    assert not service.ready
    assert json.loads(service.path.read_text())["journal"]["traversal_complete"] is True
