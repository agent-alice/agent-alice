"""Regressions for lifecycle faults and durable intent reconciliation."""

import asyncio
from datetime import datetime, timezone
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
from alice_codex.store import DispatchReceipt
import alice_codex.service as service_module


@pytest.fixture
def service(tmp_path):
    config = RuntimeConfig(str(tmp_path), "/usr/bin/true", "codex-cli fixture", "unused")
    config.prepare_directories()
    (config.workspace / "memory").mkdir(exist_ok=True)
    for name in ("SOUL.md", "USER.md", "memory/MEMORY.md"):
        (config.workspace / name).write_text("Synthetic service identity: " + name)
    item = Service(config)
    item.ready = True
    item.codex = Mock()
    item.codex.thread_start = AsyncMock(
        return_value={"thread": {"id": "new-root", "status": {"type": "idle"}}}
    )
    item.codex.record_runtime_initialization = AsyncMock(return_value={})
    item.codex.thread_resume = AsyncMock(
        return_value={"thread": {"id": "new-root", "status": {"type": "idle"}}}
    )
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
    assert "stable" not in service.state["intents"]  # Definitely not sent; safe stable-ID retry.


async def test_request_id_retries_same_input_but_rejects_different_input(service):
    first = await service.submit("main", "write result", intent_id="stable")
    assert await service.submit("main", "write result", intent_id="stable") == first
    service.codex.turn_start.assert_awaited_once()
    with pytest.raises(ValueError, match="different input"):
        await service.submit("main", "publish different result", intent_id="stable")
    service.codex.turn_start.assert_awaited_once()


async def test_request_id_is_global_even_across_concurrent_targets(service):
    entered, release = asyncio.Event(), asyncio.Event()

    async def create_thread(**kwargs):
        entered.set()
        await release.wait()
        return {"thread": {"id": "new-root", "status": {"type": "idle"}}}

    service.codex.thread_start.side_effect = create_thread
    first = asyncio.create_task(service.submit("main", "input", intent_id="global-id"))
    await entered.wait()
    second = asyncio.create_task(service.submit("other", "input", intent_id="global-id"))
    await asyncio.sleep(0)
    release.set()
    results = await asyncio.gather(first, second, return_exceptions=True)
    assert results[0]["target"] == "main"
    assert isinstance(results[1], ValueError)
    assert service.state["intents"]["global-id"]["target"] == "main"
    service.codex.turn_start.assert_awaited_once()


async def test_automatic_retry_returns_receipt_while_paused(service):
    first = await service.submit("main", "input", intent_id="stable", automatic=True)
    service.store.set_autonomy_paused(True)
    assert await service.submit("main", "input", intent_id="stable", automatic=True) == first
    service.codex.turn_start.assert_awaited_once()


async def test_concurrent_automatic_inputs_reserve_capacity_until_reconciled(service):
    service.config.max_active_tasks = 1
    # Native status can lag its acknowledgement. An unresolved durable intent
    # must reserve the slot until history or a terminal event resolves it.
    results = await asyncio.gather(
        service.submit("main", "one", intent_id="one", automatic=True),
        service.submit("other", "two", intent_id="two", automatic=True),
        return_exceptions=True,
    )
    assert sum(isinstance(result, dict) for result in results) == 1
    assert sum(isinstance(result, RejectedDispatch) for result in results) == 1
    service.codex.turn_start.assert_awaited_once()
    service._complete_turn("new-root", {"id": "turn-1", "status": "completed"})
    service.codex.thread_start.return_value = {
        "thread": {"id": "other-root", "status": {"type": "idle"}}
    }
    assert (await service.submit("other", "two", intent_id="two", automatic=True))[
        "status"
    ] == "accepted"


async def test_automatic_input_does_not_queue_behind_active_or_unknown_work(service):
    service.state["tasks"]["main"] = {"thread_id": "root", "paused": False}
    service.codex.thread_read.return_value = {"thread": {"status": {"type": "active"}}}
    with pytest.raises(RejectedDispatch):
        await service.submit("main", "automatic", automatic=True)
    service.codex.queue_add.assert_not_called()
    service.codex.thread_read.return_value = {"thread": {"status": {"type": "idle"}}}
    service.state["intents"]["unknown"] = {
        "id": "unknown",
        "thread_id": "root",
        "target": "main",
        "status": "unknown",
    }
    with pytest.raises(RejectedDispatch):
        await service.submit("main", "new automatic", automatic=True)
    service.codex.turn_start.assert_not_called()


async def test_scheduled_pause_after_sending_marker_retries_same_window_after_restart(service):
    job = service.store.create_job(name="one shot", schedule_type="at", schedule_value=10, now=0)
    service.scheduler.clock = lambda: 10
    original_mark = service.store.mark_sending

    def pause_after_marker(*args, **kwargs):
        marked = original_mark(*args, **kwargs)
        service.store.set_autonomy_paused(True)
        return marked

    service.store.mark_sending = pause_after_marker
    deferred = (await service.scheduler.poll())[0]
    assert deferred.status == "pending"
    assert not service.state["intents"]
    service.codex.turn_start.assert_not_called()
    restarted = Service(service.config)
    restarted.ready, restarted.codex = True, service.codex
    restarted.scheduler.clock = lambda: 20
    try:
        assert await restarted.scheduler.poll() == []
        restarted.store.set_autonomy_paused(False)
        delivered = (await restarted.scheduler.poll())[0]
        assert delivered.id == deferred.id
        assert (delivered.due_at, delivered.through_at) == (10, 10)
        assert delivered.status == "accepted"
        assert restarted.store.get_job(job.id).next_due is None
        service.codex.turn_start.assert_awaited_once()
    finally:
        restarted.store.close()


async def test_summary_dispatch_waits_for_its_window_and_ignores_unrelated_failed_history(service):
    def timestamp(value):
        return datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp()

    for name, level, due in [
        ("old", "L1", "2000-01-01T02:05:00"),
        ("current", "L1", "2000-02-01T02:05:00"),
        ("daily", "L2", "2000-02-02T00:15:00"),
    ]:
        service.store.create_job(
            job_id=name,
            name="summary:" + level,
            target="summary:" + level,
            schedule_type="at",
            schedule_value=timestamp(due),
            now=0,
        )
    now = timestamp("2000-02-02T01:00:00")
    service.store.acquire_lease("fixture", now=now)
    events = {event.job_id: event for event in service.store.materialize_due("fixture", now=now)}

    def complete(name, status):
        event = events[name]
        service.store.claim_event(event.id, "fixture", now=now)
        service.store.mark_sending(event.id, "fixture", now=now)
        service.store.record_receipt(event.id, DispatchReceipt(status))

    complete("old", "failed")
    assert not await service.is_busy("summary:L2")  # Capacity check is window-independent.
    with pytest.raises(RejectedDispatch, match="lower summary"):
        await service.dispatch(events["daily"])
    service.codex.turn_start.assert_not_called()
    complete("current", "completed")
    result = await service.dispatch(events["daily"])
    assert result.status == "completed" and "No source records" in result.detail
    assert service.store.get_event(events["old"].id).status == "failed"


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


@pytest.mark.parametrize("input_state", ["empty", "observed", "unknown"])
async def test_unloaded_thread_resumes_same_id_before_considering_empty_replacement(
    service, input_state
):
    old = {"thread_id": "old-root", "paused": True, "has_input": input_state == "observed"}
    service.state["tasks"]["main"] = old
    if input_state == "unknown":
        service.state["intents"]["unknown"] = {"thread_id": "old-root", "status": "unknown"}
    service.save()
    service.codex.thread_read.side_effect = RpcError("thread not loaded: old-root", -32600)

    async def resume(thread_id, **kwargs):
        if thread_id == "old-root":
            raise RpcError("no rollout found for thread id old-root", -32600)
        return {"thread": {"id": thread_id, "status": {"type": "idle"}}}

    service.codex.thread_resume = AsyncMock(side_effect=resume)
    if input_state == "empty":
        replacement = await service.ensure_thread("main")
        assert replacement["thread_id"] == "new-root" and replacement["paused"]
        assert json.loads(service.path.read_text())["tasks"]["main"]["paused"]
        service.codex.thread_start.assert_awaited_once()
    else:
        with pytest.raises(RpcError, match="no rollout"):
            await service.ensure_thread("main")
        assert service.state["tasks"]["main"] == old
        service.codex.thread_start.assert_not_called()
    assert service.codex.thread_resume.await_args_list[0].args == ("old-root",)
    if input_state == "empty":
        assert service.codex.thread_resume.await_args_list[-1].args == ("new-root",)


async def test_unloaded_durable_history_is_resumed_without_replacement(service):
    service.state["tasks"]["main"] = {"thread_id": "root", "paused": True, "has_input": True}
    service.codex.thread_read.side_effect = RpcError("thread not loaded: root", -32600)
    service.codex.thread_resume = AsyncMock(return_value={"thread": {"status": {"type": "idle"}}})
    assert (await service.ensure_thread("main"))["thread_id"] == "root"
    service.codex.thread_start.assert_not_called()


async def test_failed_empty_replacement_preserves_alias_and_pause(service):
    old = {"thread_id": "root", "paused": True, "has_input": False}
    service.state["tasks"]["main"] = old
    service.save()
    service.codex.thread_read.side_effect = RpcError("no rollout found for thread id root", -32600)
    service.codex.thread_start.side_effect = RpcError("required MCP failed")
    with pytest.raises(RpcError, match="MCP"):
        await service.ensure_thread("main")
    assert json.loads(service.path.read_text())["tasks"]["main"] == old
    assert not service.state.get("replaced_empty_threads")


@pytest.mark.parametrize("has_input", [False, True])
@pytest.mark.parametrize("native_error", ["thread not loaded", "thread not found"])
async def test_pause_checks_unloaded_empty_alias_without_creating_a_thread(
    service, has_input, native_error
):
    service.state["tasks"]["empty"] = {"thread_id": "empty-root", "has_input": has_input}
    service.codex.stop_tree.side_effect = RpcError(f"{native_error}: empty-root", -32600)
    service.codex.thread_resume = AsyncMock(
        side_effect=RpcError("no rollout found for thread id empty-root", -32600)
    )
    if has_input:
        with pytest.raises(RuntimeError, match="Could not prove"):
            await service.pause()
        assert service.stop_event.is_set()
    else:
        result = await service.pause()
        assert result["tasks"]["empty"]["absent_empty_thread"] == "empty-root"
        assert not service.stop_event.is_set()
    assert service.state["tasks"]["empty"]["paused"]
    assert service.state["tasks"]["empty"]["thread_id"] == "empty-root"
    service.codex.thread_start.assert_not_called()


@pytest.mark.parametrize("has_input", [False, True])
async def test_shutdown_restart_global_resume_and_due_dispatch_hydrates_original_task(
    service, tmp_path, has_input
):
    service.state["tasks"]["main"] = {
        "thread_id": "old-root",
        "paused": False,
        "has_input": has_input,
    }
    service.store.create_job(name="due", schedule_type="at", schedule_value=10, now=0)
    await service._shutdown((tmp_path / "shutdown.log").open("ab"))
    assert not json.loads(service.path.read_text())["tasks"]["main"]["paused"]
    restarted = Service(service.config)
    restarted.ready, restarted.codex = True, service.codex
    restarted.scheduler.clock = lambda: 20
    loaded = set()

    async def read(thread_id):
        if thread_id == "old-root" and thread_id not in loaded:
            raise RpcError("thread not loaded: old-root", -32600)
        return {"thread": {"id": thread_id, "status": {"type": "idle"}}}

    async def resume(thread_id, **kwargs):
        if not has_input and thread_id == "old-root":
            raise RpcError("no rollout found for thread id old-root", -32600)
        loaded.add(thread_id)
        return await read(thread_id)

    restarted.codex.thread_read.side_effect = read
    restarted.codex.thread_resume = AsyncMock(side_effect=resume)
    try:
        assert restarted.store.is_autonomy_paused()
        assert await restarted.handle("resume", {}) == {"resumed": "autonomy"}
        events = await restarted.scheduler.poll()
        assert len(events) == 1 and events[0].status == "accepted"
        assert not restarted.stop_event.is_set()
        expected = "old-root" if has_input else "new-root"
        assert events[0].receipt.thread_id == expected
        assert restarted.state["tasks"]["main"]["thread_id"] == expected
        if has_input:
            restarted.codex.thread_start.assert_not_called()
        else:
            restarted.codex.thread_start.assert_awaited_once()
        restarted.codex.turn_start.assert_awaited_once()
    finally:
        restarted.store.close()


async def test_capacity_query_does_not_hydrate_unrelated_paused_root(service):
    service.state["tasks"]["paused"] = {
        "thread_id": "paused-root",
        "paused": True,
        "has_input": True,
    }
    service.codex.thread_read.side_effect = RpcError("thread not loaded: paused-root", -32600)
    service.codex.thread_resume = AsyncMock()
    assert not await service.is_busy("new")
    service.codex.thread_resume.assert_not_called()
    service.codex.thread_start.assert_not_called()


@pytest.mark.parametrize("method", ["turn/started", "item/completed"])
async def test_native_input_marks_history_durable_before_archive_worker(service, method):
    service.state["tasks"]["main"] = {"thread_id": "root", "paused": True, "has_input": False}
    service._on_notification(
        {
            "method": method,
            "params": {
                "threadId": "root",
                "turn": {"id": "tui-turn"},
                "item": {"type": "userMessage", "id": "tui-message"},
            },
        }
    )
    assert json.loads(service.path.read_text())["tasks"]["main"]["has_input"]
    restarted = Service(service.config)
    restarted.codex = service.codex
    service.codex.thread_read.side_effect = RpcError("no rollout found for thread id root", -32600)
    try:
        with pytest.raises(RpcError):
            await restarted.ensure_thread("main")
        service.codex.thread_start.assert_not_called()
    finally:
        restarted.store.close()


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


async def test_quota_exhaustion_persists_and_stops_only_automatic_roots(service):
    await service.submit("automatic", "automatic work", intent_id="auto", automatic=True)
    service.state["tasks"]["manual"] = {
        "thread_id": "manual-root",
        "paused": False,
        "automatic": False,
    }
    service.state["tasks"]["legacy"] = {"thread_id": "legacy-root", "paused": False}
    service._on_notification(
        {
            "method": "account/rateLimits/updated",
            "params": {"rateLimits": {"limitId": "codex", "primary": {"usedPercent": 100}}},
        }
    )
    persisted = json.loads(service.path.read_text())
    assert persisted["tasks"]["automatic"]["resource_pause_pending"]
    assert persisted["tasks"]["automatic"]["paused"]
    assert persisted["intents"]["auto"]["automatic"] is True
    assert not persisted["tasks"]["manual"]["paused"]
    assert not persisted["tasks"]["legacy"]["paused"]
    # Simulate a crash between durable pause and native stop. Even replenished
    # quota cannot undo that unacknowledged stop obligation on restart.
    restarted = Service(service.config)
    restarted.codex = service.codex
    try:
        restarted.rpc = Mock(
            request=AsyncMock(return_value={"rateLimits": {"primary": {"usedPercent": 20}}})
        )
        await restarted.refresh_resources()
        await restarted._stop_resource_paused_tasks()
        service.codex.stop_tree.assert_awaited_once_with("new-root")
        task = json.loads(service.path.read_text())["tasks"]["automatic"]
        assert task["paused"] and "resource_pause_pending" not in task
        assert restarted.resources.can_dispatch(automatic=True)["allowed"]
    finally:
        restarted.store.close()


async def test_quota_stop_failure_preserves_obligation_and_fails_closed(service):
    service.state["tasks"]["auto"] = {"thread_id": "root", "paused": False, "automatic": True}
    service.codex.stop_tree.side_effect = RpcError("native stop failed")
    service.rpc.request.return_value = {"rateLimits": {"primary": {"usedPercent": 100}}}
    with pytest.raises(RuntimeError, match="Could not prove"):
        await service.refresh_resources()
    assert service.stop_event.is_set() and service.store.is_autonomy_paused()
    assert json.loads(service.path.read_text())["tasks"]["auto"]["resource_pause_pending"]


async def test_manual_followup_cannot_detach_an_automatic_tree_from_quota_stop(service):
    await service.submit("main", "automatic goal", automatic=True)
    service.codex.thread_read.return_value = {"thread": {"status": {"type": "active"}}}
    await service.submit("main", "manual follow-up")
    service.codex.queue_add.assert_awaited_once()
    service.rpc.request.return_value = {"rateLimits": {"primary": {"usedPercent": 100}}}
    await service.refresh_resources()
    assert service.state["tasks"]["main"]["automatic"] is True
    assert service.state["tasks"]["main"]["paused"] is True
    service.codex.stop_tree.assert_awaited_once_with("new-root")


async def test_quota_policy_stops_mixed_root_after_direct_tui_input(service):
    await service.submit("main", "heartbeat", intent_id="heartbeat", automatic=True)
    service._complete_turn("new-root", {"id": "turn-1", "status": "completed"})
    recorded = asyncio.Event()
    service.journal = Mock(record_live=lambda event: recorded.set())
    # No Alice request/client ID exists for this new manual TUI input. Root
    # ownership must not be represented as evidence that this turn is automatic.
    service._on_notification(
        {
            "method": "item/completed",
            "params": {
                "threadId": "new-root",
                "turnId": "tui-manual",
                "item": {
                    "id": "tui-input",
                    "type": "userMessage",
                    "content": [{"type": "text", "text": "manual"}],
                },
            },
        }
    )
    worker = asyncio.create_task(service._record_events())
    try:
        await asyncio.wait_for(recorded.wait(), 2)
        assert set(service.state["intents"]) == {"heartbeat"}
        service.rpc.request.return_value = {"rateLimits": {"primary": {"usedPercent": 100}}}
        await service.refresh_resources()
        # Deliberate conservative policy: the whole mixed root is paused, even
        # its manual turn, because autonomous Goals/descendants can outlive input.
        service.codex.stop_tree.assert_awaited_once_with("new-root")
        assert service.state["tasks"]["main"]["paused"]
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


@pytest.mark.parametrize("automatic", [1, "true", None])
async def test_automatic_admission_requires_boolean_ownership(service, automatic):
    with pytest.raises(ValueError, match="boolean"):
        await service.submit("main", "input", automatic=automatic)
    service.codex.turn_start.assert_not_called()


@pytest.mark.parametrize("pause_kind", ["quota", "manual", "other_task"])
async def test_pause_during_resume_stops_late_goal_ack_and_keeps_pause(service, pause_kind):
    service.state["tasks"]["main"] = {
        "thread_id": "root",
        "paused": True,
        "automatic": True,
        "has_input": True,
    }
    service.state["tasks"]["other"] = {"thread_id": "other-root", "paused": False}
    service.store.set_autonomy_paused(True)
    service.codex.queue_list = AsyncMock(return_value={"data": []})
    entered, release = asyncio.Event(), asyncio.Event()
    goal_state = "paused"

    async def rpc(method, params):
        nonlocal goal_state
        if method == "thread/goal/get":
            return {"goal": {"status": goal_state}}
        if method == "thread/goal/set":
            entered.set()
            await release.wait()
            goal_state = "active"
        return {}

    async def stop_tree(*args):
        nonlocal goal_state
        if args[0] == "root":
            goal_state = "paused"
        return {"stopped": [args[0]]}

    service.rpc.request.side_effect = rpc
    service.codex.stop_tree.side_effect = stop_tree
    resuming = asyncio.create_task(service.handle("resume", {}))
    await asyncio.wait_for(entered.wait(), 2)
    if pause_kind == "quota":
        service._on_notification(
            {
                "method": "account/rateLimits/updated",
                "params": {"rateLimits": {"primary": {"usedPercent": 100}}},
            }
        )
        assert json.loads(service.path.read_text())["tasks"]["main"]["resource_pause_pending"]
        stopping = asyncio.create_task(service._stop_resource_paused_tasks())
    elif pause_kind == "other_task":
        stopping = asyncio.create_task(service.pause("other"))
    else:
        stopping = asyncio.create_task(service.pause())
    await asyncio.sleep(0)
    release.set()
    results = await asyncio.wait_for(asyncio.gather(resuming, stopping, return_exceptions=True), 2)
    assert not isinstance(results[1], BaseException)
    if pause_kind == "other_task":
        assert results[0] == {"resumed": "autonomy"}
        assert goal_state == "active"
        assert not service.state["tasks"]["main"]["paused"]
        assert service.state["tasks"]["other"]["paused"]
        service.codex.stop_tree.assert_awaited_once_with("other-root")
        return
    assert isinstance(results[0], RejectedDispatch)
    assert goal_state == "paused"
    assert service.state["tasks"]["main"]["paused"]
    assert service.store.is_autonomy_paused()
    assert any(call.args == ("root",) for call in service.codex.stop_tree.await_args_list)


async def test_unbound_token_notifications_are_rejected_without_disabling_archival(service):
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
        assert status["tokens"]["threads"] == {}
        assert status["tokens"]["epochs"] == {}
        assert service.stopping and "epoch" in service.error
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
    monkeypatch.setattr(service_module.os, "getpgid", lambda pid: pid)
    monkeypatch.setattr(
        service_module.RpcClient, "connect_unix", AsyncMock(return_value=service.rpc)
    )
    service.rpc.initialize = AsyncMock()
    service.rpc.events, service.rpc.event_sequence = [], 0

    async def request(method, params):
        if method == "hooks/list":
            from alice_codex.identity import HOOK_STATUS

            hooks = []
            for event, groups in service.config.identity_hooks().items():
                group, handler = groups[0], groups[0]["hooks"][0]
                hooks.append(
                    {
                        "key": str(service.config.codex_home / "config.toml") + ":" + event,
                        "eventName": event[0].lower() + event[1:],
                        "handlerType": "command",
                        "command": handler["command"],
                        "async": False,
                        "matcher": group.get("matcher"),
                        "timeoutSec": 10,
                        "statusMessage": HOOK_STATUS,
                        "additionalContextLimit": handler.get("additionalContextLimit"),
                        "source": "user",
                        "sourcePath": str(service.config.codex_home / "config.toml"),
                        "enabled": True,
                        "currentHash": "sha256:" + "a" * 64,
                        "trustStatus": "trusted",
                    }
                )
            return {
                "data": [
                    {
                        "cwd": str(service.config.workspace),
                        "warnings": [],
                        "errors": [],
                        "hooks": hooks,
                    }
                ]
            }
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


async def test_host_supplies_identity_without_a_model_read_tool(service):
    from alice_codex.identity import build_identity_bundle

    await service.ensure_thread("identity-root")
    parameters = service.codex.thread_start.call_args.kwargs
    assert (
        parameters["developerInstructions"]
        == build_identity_bundle(service.config.workspace).developer_instructions
    )
    assert "reference_data_not_instructions" in parameters["developerInstructions"]
    service.codex.turn_start.assert_not_called()


async def test_missing_identity_fails_before_creating_native_root(service):
    from alice_codex.identity import IdentityError

    (service.config.workspace / "USER.md").unlink()
    with pytest.raises(IdentityError, match="missing_or_linked_file"):
        await service.ensure_thread("missing-source")
    service.codex.thread_start.assert_not_called()
    assert "missing-source" not in service.state["tasks"]
