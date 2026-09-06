"""V2 ownership through synthetic public notifications and the real token ledger.

The native activity type supplies the relationship; model text and agent paths
do not. No native binary, model, account, or external endpoint is used.
"""

import asyncio
from collections import Counter

import pytest

from alice_codex.resources import ResourceLedger
from test_service_resource_epochs import attach, epoch_tokens, host as host, token


def activity(child, parent="root"):
    return {
        "method": "item/completed",
        "params": {
            "threadId": parent,
            "turnId": "parent-turn",
            "item": {
                "id": "activity-" + child,
                "type": "subAgentActivity",
                "kind": "started",
                "agentThreadId": child,
                "agentPath": "/root/" + child,
            },
        },
    }


def test_native_v2_activity_attributes_315_replayed_token_notifications(host):
    epoch, rpc, codex, observer = attach(host)
    children = [f"child-{index}" for index in range(9)]
    for index, child in enumerate(children):
        rpc.publish(activity(child))
        for _ in range(35):
            rpc.publish(token(100 + index, thread=child))

    assert not host.stopping, "Native V2 child observations exhausted the unknown buffer"
    assert observer.pending_count == 0
    assert all(codex.owns(child) for child in children)
    assert not rpc.listener_errors
    for index, child in enumerate(children):
        recorded = epoch_tokens(host, epoch, child)
        assert recorded["high_water"]["totalTokens"] == 100 + index
        assert recorded["event_count"] == 1
    tokens = host.resources.status()["tokens"]
    assert set(tokens["epochs"][epoch]) == set(children)
    assert tokens["threads"] == {} and tokens["actual_usage_total"] is None


def test_native_activity_immediately_flushes_earlier_child_tokens(host):
    epoch, rpc, codex, observer = attach(host)
    rpc.publish(token(10, thread="child"))
    rpc.publish(token(30, thread="child", turn="later"))
    assert observer.pending_count == 2 and not codex.owns("child")
    rpc.publish(activity("child"))
    assert observer.pending_count == 0 and codex.owns("child")
    child = epoch_tokens(host, epoch, "child")
    assert child["first_observed"]["totalTokens"] == 10
    assert child["high_water"]["totalTokens"] == 30 and child["event_count"] == 2
    assert not host.stopping


async def eventually(predicate):
    async with asyncio.timeout(1):
        while not predicate():
            await asyncio.sleep(0)


def thread_result(thread_id, parent=None):
    thread = {"id": thread_id, "status": {"type": "idle"}}
    if parent is not None:
        thread["parentThreadId"] = parent
    return {"thread": thread}


async def test_reconciliation_waits_for_explicit_post_initialize_start(host):
    epoch, rpc, _, observer = attach(host)
    reads = []

    def read(method, params):
        assert method == "thread/read"
        reads.append(params["threadId"])
        return thread_result(params["threadId"], "root")

    rpc.on_request = read
    try:
        rpc.publish(token(40, thread="child"))
        await asyncio.sleep(0)
        assert reads == [] and observer.pending_count == 1
        assert epoch_tokens(host, epoch, "child") is None
        await rpc.initialize()
        observer.start_reconciliation()
        await eventually(lambda: observer.pending_count == 0)
        assert reads == ["child"]
        assert epoch_tokens(host, epoch, "child")["high_water"]["totalTokens"] == 40
    finally:
        await observer.close()


async def test_slow_read_does_not_block_publish_and_replayed_id_is_read_once(host):
    epoch, rpc, _, observer = attach(host)
    entered, release = asyncio.Event(), asyncio.Event()
    reads = []

    async def read(method, params):
        assert method == "thread/read"
        reads.append(params["threadId"])
        entered.set()
        await release.wait()
        return thread_result(params["threadId"], "root")

    rpc.on_request = read
    observer.start_reconciliation()
    try:
        rpc.publish(token(130, thread="child"))
        await asyncio.wait_for(entered.wait(), 1)
        for _ in range(34):
            rpc.publish(token(130, thread="child"))
        rpc.publish(token(60, thread="second-child"))
        rpc.publish(token(20, thread="root"))
        assert epoch_tokens(host, epoch)["high_water"]["totalTokens"] == 20
        await asyncio.sleep(0)
        assert observer.pending_count == 36 and reads == ["child"]
        release.set()
        await eventually(lambda: observer.pending_count == 0)
        child = epoch_tokens(host, epoch, "child")
        assert child["high_water"]["totalTokens"] == 130 and child["event_count"] == 1
        assert reads == ["child", "second-child"] and not host.stopping
        assert epoch_tokens(host, epoch, "second-child")["high_water"]["totalTokens"] == 60
    finally:
        release.set()
        await observer.close()


async def test_public_parent_chain_recovers_child_missing_from_restart_inventory(host):
    epoch, rpc, codex, observer = attach(host)
    reads = []
    parents = {"child": "intermediate", "intermediate": "root"}

    def read(method, params):
        assert method == "thread/read", "Unknown IDs must be reconciled by public exact-ID reads"
        thread = params["threadId"]
        reads.append(thread)
        return thread_result(thread, parents.get(thread))

    rpc.on_request = read
    observer.start_reconciliation()
    try:
        assert codex._parents == {}
        rpc.publish(token(70, thread="child"))
        await eventually(lambda: observer.pending_count == 0)
        assert codex.owns("child") and codex.owns("intermediate")
        assert reads == ["child", "intermediate"]
        assert epoch_tokens(host, epoch, "child")["high_water"]["totalTokens"] == 70
        assert not host.stopping
    finally:
        await observer.close()


async def test_foreign_parent_stays_pending_without_repeated_reads(host):
    epoch, rpc, codex, observer = attach(host)
    reads = Counter()
    reached_foreign = asyncio.Event()

    def read(method, params):
        assert method == "thread/read"
        thread = params["threadId"]
        reads[thread] += 1
        if thread == "foreign":
            reached_foreign.set()
            return thread_result(thread)
        return thread_result(thread, "foreign")

    rpc.on_request = read
    observer.start_reconciliation()
    try:
        rpc.publish(token(75, thread="child"))
        await asyncio.wait_for(reached_foreign.wait(), 1)
        for _ in range(5):
            rpc.publish(token(75, thread="child"))
            await asyncio.sleep(0)
        assert observer.pending_count == 6
        assert reads == {"child": 1, "foreign": 1}
        assert not codex.owns("child") and epoch_tokens(host, epoch, "child") is None
    finally:
        await observer.close()
    assert observer.pending_count == 6


async def test_reconciliation_timeout_preserves_buffer_and_bounds_unknowns(host, monkeypatch):
    epoch, rpc, _, observer = attach(host)
    cancelled = asyncio.Event()
    reads = []

    async def read(method, params):
        assert method == "thread/read"
        reads.append(params["threadId"])
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    rpc.on_request = read
    monkeypatch.setattr(observer, "RECONCILE_TIMEOUT", 0.03)
    observer.start_reconciliation()
    try:
        rpc.publish(token(90, thread="unknown"))
        await asyncio.wait_for(cancelled.wait(), 1)
        assert observer.pending_count == 1 and epoch_tokens(host, epoch, "unknown") is None
        for _ in range(255):
            rpc.publish(token(90, thread="unknown"))
        await asyncio.sleep(0)
        assert observer.pending_count == 256 and reads == ["unknown"]
        rpc.publish(token(90, thread="unknown"))
        assert observer.pending_count == 256 and host.stopping and host.stop_event.is_set()
        assert epoch_tokens(host, epoch, "unknown") is None
    finally:
        await observer.close()


async def test_late_reconciliation_result_keeps_original_epoch_after_client_replacement(host):
    old_epoch, old_rpc, old_codex, old_observer = attach(host)
    entered, release = asyncio.Event(), asyncio.Event()

    async def old_read(method, params):
        assert method == "thread/read" and params["threadId"] == "child"
        entered.set()
        await release.wait()
        return thread_result("child", "root")

    old_rpc.on_request = old_read
    old_observer.start_reconciliation()
    new_observer = None
    try:
        old_rpc.publish(token(130, thread="child"))
        await asyncio.wait_for(entered.wait(), 1)
        new_epoch, new_rpc, new_codex, new_observer = attach(host)
        assert new_epoch != old_epoch and new_codex is not old_codex
        new_rpc.on_request = lambda method, params: thread_result(params["threadId"], "root")
        new_observer.start_reconciliation()
        new_rpc.publish(token(600, thread="child"))
        await eventually(lambda: new_observer.pending_count == 0)
        assert old_observer.pending_count == 1
        release.set()
        await eventually(lambda: old_observer.pending_count == 0)
        assert epoch_tokens(host, old_epoch, "child")["high_water"]["totalTokens"] == 130
        assert epoch_tokens(host, new_epoch, "child")["high_water"]["totalTokens"] == 600
        assert not host.stopping
    finally:
        release.set()
        await old_observer.close()
        if new_observer is not None:
            await new_observer.close()


async def test_close_cancels_and_awaits_read_then_prevents_further_writes(host, monkeypatch):
    epoch, rpc, _, observer = attach(host)
    entered, cancelled, cleanup = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def read(method, params):
        assert method == "thread/read"
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            await cleanup.wait()
            raise

    rpc.on_request = read
    monkeypatch.setattr(observer, "RECONCILE_CLOSE_TIMEOUT", 0.03)
    observer.start_reconciliation()
    close = None
    try:
        rpc.publish(token(45, thread="child"))
        await asyncio.wait_for(entered.wait(), 1)
        close = asyncio.create_task(observer.close())
        await asyncio.wait_for(cancelled.wait(), 1)
        assert not close.done(), "close returned while its RPC read was still being cancelled"
        cleanup.set()
        await asyncio.wait_for(close, 1)
        rpc.publish(activity("child"))
        rpc.publish(token(99, thread="child"))
        rpc.publish(token(50, thread="root"))
        observer.flush()
        assert epoch_tokens(host, epoch, "child") is None
        assert epoch_tokens(host, epoch, "root") is None
        assert observer.pending_count == 1
    finally:
        cleanup.set()
        if close is not None:
            await asyncio.gather(close, return_exceptions=True)
        await observer.close()


async def test_old_worker_failure_does_not_stop_replacement_epoch(host):
    old_epoch, old_rpc, _, old_observer = attach(host)
    entered, release = asyncio.Event(), asyncio.Event()

    async def old_read(method, params):
        assert method == "thread/read"
        entered.set()
        await release.wait()
        raise RuntimeError("synthetic old connection failure")

    old_rpc.on_request = old_read
    old_observer.start_reconciliation()
    new_observer = None
    try:
        old_rpc.publish(token(20, thread="child"))
        await asyncio.wait_for(entered.wait(), 1)
        new_epoch, new_rpc, _, new_observer = attach(host)
        release.set()
        await asyncio.wait_for(asyncio.shield(old_observer._resolver), 1)
        assert not host.stopping, "An old epoch's failed read stopped the replacement client"
        assert not host.stop_event.is_set() and new_rpc.connected
        assert old_observer.error and old_observer.pending_count == 1
        assert epoch_tokens(host, old_epoch, "child") is None
        new_rpc.publish(token(60))
        assert epoch_tokens(host, new_epoch)["high_water"]["totalTokens"] == 60
    finally:
        release.set()
        await old_observer.close()
        if new_observer is not None:
            await new_observer.close()


async def test_old_callback_persistence_failure_does_not_stop_replacement_epoch(host, monkeypatch):
    old_epoch, old_rpc, _, old_observer = attach(host)
    new_epoch, new_rpc, _, new_observer = attach(host)
    record = host.resources.record_token_usage

    def fail_old_write(params, *, epoch_id):
        if epoch_id == old_epoch:
            raise OSError("synthetic old epoch persistence failure")
        return record(params, epoch_id=epoch_id)

    monkeypatch.setattr(host.resources, "record_token_usage", fail_old_write)
    try:
        old_rpc.publish(token(30))
        assert not host.stopping, "An old epoch's failed write stopped the replacement client"
        assert not host.stop_event.is_set() and new_rpc.connected
        assert old_observer.error and old_observer.pending_count == 1
        assert epoch_tokens(host, old_epoch) is None
        new_rpc.publish(token(90))
        assert epoch_tokens(host, new_epoch)["high_water"]["totalTokens"] == 90
    finally:
        await old_observer.close()
        await new_observer.close()


async def test_old_listener_keeps_captured_ledger_after_host_ledger_replacement(host):
    old_epoch, old_rpc, _, old_observer = attach(host)
    old_ledger = host.resources
    host.resources = ResourceLedger(host.config.root / "state/replacement-resources.sqlite3")
    new_epoch, new_rpc, _, new_observer = attach(host)
    try:
        old_rpc.publish(token(25))
        new_rpc.publish(token(85))
        old_tokens = old_ledger.status()["tokens"]["epochs"]
        new_tokens = host.resources.status()["tokens"]["epochs"]
        assert old_tokens[old_epoch]["root"]["high_water"]["totalTokens"] == 25
        assert new_tokens[new_epoch]["root"]["high_water"]["totalTokens"] == 85
        assert new_epoch not in old_tokens and old_epoch not in new_tokens
    finally:
        await old_observer.close()
        await new_observer.close()


@pytest.mark.parametrize("replace_client", [False, True])
async def test_close_with_unresolved_observations_keeps_explicit_error(host, replace_client):
    epoch, rpc, _, observer = attach(host)
    rpc.publish(token(40, thread="unresolved"))
    replacement = None
    if replace_client:
        new_epoch, new_rpc, _, replacement = attach(host)
    try:
        await observer.close()
        assert observer.error, (
            "Closing an unresolved buffer must not claim a clean observation tail"
        )
        assert observer.pending_count == 1 and epoch_tokens(host, epoch, "unresolved") is None
        if replace_client:
            assert not host.stopping and not host.stop_event.is_set()
            new_rpc.publish(token(100))
            assert epoch_tokens(host, new_epoch)["high_water"]["totalTokens"] == 100
    finally:
        await observer.close()
        if replacement is not None:
            await replacement.close()


async def test_cancel_close_during_read_cleanup_seals_listener_and_finishes_worker(
    host, monkeypatch
):
    epoch, rpc, codex, observer = attach(host)
    entered, cancelling, finish_cleanup = asyncio.Event(), asyncio.Event(), asyncio.Event()
    cleanup_done = asyncio.Event()

    async def read(method, params):
        assert method == "thread/read"
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelling.set()
            try:
                await finish_cleanup.wait()
            finally:
                cleanup_done.set()
            raise

    rpc.on_request = read
    monkeypatch.setattr(observer, "RECONCILE_CLOSE_TIMEOUT", 0.03)
    observer.start_reconciliation()
    close = None
    try:
        rpc.publish(token(45, thread="child"))
        await asyncio.wait_for(entered.wait(), 1)
        close = asyncio.create_task(observer.close())
        await asyncio.wait_for(cancelling.wait(), 1)
        close.cancel()  # Interrupt close's own await of already-cancelling work.
        await asyncio.sleep(0)
        finish_cleanup.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(close, 1)
        assert cleanup_done.is_set() and observer._resolver.done()
        codex._on_event(activity("child"))
        observer.receive(activity("child"))
        observer.receive(token(99, thread="child"))
        observer.flush()
        assert epoch_tokens(host, epoch, "child") is None, (
            "Cancelled close allowed a late ledger write"
        )
        assert observer.pending_count == 1 and observer.error
    finally:
        finish_cleanup.set()
        if close is not None:
            await asyncio.gather(close, return_exceptions=True)
        await observer.close()
