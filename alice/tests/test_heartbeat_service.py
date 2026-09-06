"""Heartbeat admission uses isolated Service/Store and synthetic host receipts.

HTTP collection itself is covered by test_heartbeat.py. These tests exercise
the internal host registration, durable comparison, and native-send boundary.
"""

from dataclasses import asdict, replace
import hashlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from alice_codex.config import RuntimeConfig
from alice_codex.heartbeat import CollectionSpec, VALIDATOR_VERSION
from alice_codex.resources import TaskPolicy
from alice_codex.rpc import RpcTimeout
from alice_codex.scheduler import RejectedDispatch
from alice_codex.service import Service
import alice_codex.service as service_module


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


class HeartbeatRuntime:
    def __init__(self, config, clock):
        self.config, self.clock = config, clock
        self.instances = []
        self.observation_state = "known"
        self.content = "snapshot-a"
        self.observation_sequence = 0
        self.event_sequence = 0
        self.native_sequence = 0
        self.codex = Mock()
        self.codex.thread_read = AsyncMock(side_effect=self.read_thread)
        self.codex.thread_start = AsyncMock(side_effect=AssertionError("fixture root must be reused"))
        self.codex.turn_start = AsyncMock(side_effect=self.start_turn)
        self.codex.queue_add = AsyncMock(return_value={"data": []})
        self.codex.queue_list = AsyncMock(return_value={"data": []})
        self.codex.queue_start = AsyncMock(return_value={"data": []})
        self.codex.stop_tree = AsyncMock(return_value={"stopped": []})
        self.codex.find_turn_by_client_id = AsyncMock(return_value=None)
        self.rpc = Mock()
        self.rpc.request = AsyncMock(return_value={"data": [], "goal": None})
        self.service = self.restart()
        self.service.store.acquire_lease("heartbeat-tests", now=0, seconds=100000)
        self.source = CollectionSpec(
            source_id="fixture-answers", url="https://example.invalid/answers?filter=one",
            subject="fixture-member", collection="answers", auth_context_version="fixture-v1",
            required_metrics=("voteup_count",), max_age_seconds=60,
        )
        self.seed()
        (config.workspace / "HEARTBEAT.md").write_text("Perform the synthetic periodic review.\n")

    def restart(self):
        service = Service(self.config)
        service.ready, service.codex, service.rpc = True, self.codex, self.rpc
        self.instances.append(service)
        self.service = service
        return service

    def seed(self):
        task = {
            "thread_id": "monitor-root", "has_input": True, "paused": False,
            "bootstrap": {"version": 1, "thread_id": "monitor-root", "state": "ready"},
        }
        self.service.state["tasks"]["monitor"] = task
        self.service.save()
        return task

    async def read_thread(self, thread_id, **kwargs):
        return {"thread": {"id": thread_id, "status": {"type": "idle"}}}

    async def start_turn(self, *args, **kwargs):
        self.native_sequence += 1
        return {"turn": {"id": f"turn-{self.native_sequence}"}}

    @property
    def disk(self):
        return json.loads(self.service.path.read_text())

    def register(self, source=None):
        self.source = source or self.source
        self.service.register_heartbeat_source("monitor", self.source, wait_seconds=10)
        self.observer = Mock(side_effect=self.observe)
        self.service._heartbeat.observe = self.observer

    def observe(self, target, *, now):
        self.observation_sequence += 1
        known = self.observation_state == "known"
        return {
            "version": 1, "id": f"observation-{self.observation_sequence}", "target": target,
            "source_id": self.source.source_id, "scope_sha256": self.source.scope_sha256,
            "validator_version": VALIDATOR_VERSION, "observed_at": now,
            "state": self.observation_state,
            "content_sha256": digest(self.source.scope_sha256 + self.content) if known else None,
            "evidence_sha256": digest(f"{self.observation_sequence}:{now}:{known}"),
            "reason": None if known else "synthetic_collection_failed",
        }

    def advance(self, seconds=10):
        self.clock.now += seconds
        self.clock.monotonic += seconds

    def event(self):
        self.event_sequence += 1
        job = self.service.store.create_job(
            job_id=f"heartbeat-{self.event_sequence}", name="synthetic heartbeat",
            target="monitor", kind="heartbeat", prompt="Check the synthetic monitor.",
            schedule_type="at", schedule_value=self.clock.now, now=self.clock.now - 1,
        )
        event = next(
            event for event in self.service.store.materialize_due("heartbeat-tests", now=self.clock.now)
            if event.job_id == job.id
        )
        self.service.store.claim_event(event.id, "heartbeat-tests", now=self.clock.now)
        assert self.service.store.mark_sending(event.id, "heartbeat-tests", now=self.clock.now)
        return self.service.store.get_event(event.id)

    async def dispatch(self, event=None):
        event = event or self.event()
        receipt = await self.service.dispatch(event)
        self.service.store.record_receipt(event.id, receipt)
        return receipt

    def complete(self, receipt):
        self.service._complete_turn(receipt.thread_id, {"id": receipt.turn_id, "status": "completed"})


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    clock = SimpleNamespace(now=100.0, monotonic=1000.0)
    # Do not replace asyncio's monotonic clock when controlling collection waits.
    monkeypatch.setattr(service_module, "time", SimpleNamespace(
        time=lambda: clock.now, monotonic=lambda: clock.monotonic,
    ))
    config = RuntimeConfig(str(tmp_path), "/usr/bin/true", "codex-cli fixture", "unused")
    config.prepare_directories()
    item = HeartbeatRuntime(config, clock)
    yield item
    for service in item.instances:
        service.store.close()
    for socket in config.socket_dir.iterdir():
        socket.unlink()
    config.socket_dir.rmdir()


async def test_unconfigured_source_retains_ordinary_periodic_review_without_fake_consumption(runtime):
    first = await runtime.dispatch()
    evidence = runtime.service.store.get_heartbeat_state("monitor")
    assert first.status == "accepted" and evidence["latest"]["state"] == "unconfigured"
    assert evidence["comparison"]["state"] == "unconfigured" and evidence["last_good"] is None
    assert not runtime.disk.get("heartbeat_consumed")
    runtime.complete(first)
    runtime.advance()
    second = await runtime.dispatch()
    assert second.status == "accepted" and runtime.codex.turn_start.await_count == 2
    assert not runtime.disk.get("heartbeat_consumed")


async def test_unconfigured_review_still_obeys_persistent_pause_and_explicit_policy(runtime):
    await runtime.service.set_task_policy(
        "monitor", asdict(TaskPolicy(120, 1, 0, 5, 30)), "policy-monitor"
    )
    runtime.service.state["tasks"]["monitor"]["paused"] = True
    runtime.service.save()
    event = runtime.event()
    with pytest.raises(RejectedDispatch):
        await runtime.dispatch(event)
    runtime.codex.turn_start.assert_not_called()
    assert runtime.service.store.get_task_policy("monitor")["usage"] is None
    await runtime.service.resume("monitor")
    first = await runtime.dispatch(event)
    runtime.complete(first)
    runtime.advance()
    with pytest.raises(RejectedDispatch):
        await runtime.dispatch()
    assert runtime.service.store.get_task_policy("monitor")["usage"]["attempts"] == 1
    assert not runtime.disk.get("heartbeat_consumed")


async def test_registered_unknown_never_uses_retained_known_snapshot_as_current_evidence(runtime):
    runtime.register()
    good = await runtime.service.observe_heartbeat("monitor")
    assert good["latest"]["state"] == "known" and good["configured"]
    runtime.advance()
    runtime.observation_state = "unknown"
    event = runtime.event()
    with pytest.raises(RejectedDispatch, match="unknown"):
        await runtime.dispatch(event)
    state = runtime.service.store.get_heartbeat_state("monitor")
    assert state["latest"]["state"] == "unknown"
    assert state["last_good"] == good["latest"]
    assert not runtime.disk.get("heartbeat_consumed")
    runtime.codex.turn_start.assert_not_called()
    runtime.advance()
    runtime.observation_state = "known"
    assert (await runtime.dispatch(event)).status == "accepted"


async def test_new_receipt_ids_and_heartbeat_file_edits_do_not_redispatch_consumed_content(runtime):
    runtime.register()
    first = await runtime.dispatch()
    consumed = runtime.disk["heartbeat_consumed"]["monitor"]
    runtime.complete(first)
    runtime.advance()
    with pytest.raises(RejectedDispatch, match="unchanged"):
        await runtime.dispatch()
    latest = runtime.service.store.get_heartbeat_state("monitor")["latest"]
    assert latest["id"] != consumed["id"] and latest["content_sha256"] == consumed["content_sha256"]
    (runtime.config.workspace / "HEARTBEAT.md").write_text("A revised synthetic review instruction.\n")
    runtime.advance()
    with pytest.raises(RejectedDispatch, match="unchanged"):
        await runtime.dispatch()
    assert runtime.disk["heartbeat_consumed"]["monitor"] == consumed
    assert runtime.observer.call_count == 3
    runtime.codex.turn_start.assert_awaited_once()


async def test_changed_candidate_survives_pause_and_unchanged_recheck_before_admission(runtime):
    runtime.register()
    first = await runtime.dispatch()
    original = runtime.disk["heartbeat_consumed"]["monitor"]
    runtime.complete(first)
    await runtime.service.pause("monitor")
    runtime.content = "snapshot-b"
    runtime.advance()
    event = runtime.event()
    with pytest.raises(RejectedDispatch):
        await runtime.dispatch(event)
    assert runtime.disk["heartbeat_consumed"]["monitor"] == original
    pending = runtime.service.store.get_heartbeat_state("monitor")["latest"]
    assert pending["content_sha256"] != original["content_sha256"]
    await runtime.service.resume("monitor")
    runtime.advance()
    assert (await runtime.dispatch(event)).status == "accepted"
    state = runtime.service.store.get_heartbeat_state("monitor")
    assert state["comparison"]["state"] == "unchanged"
    assert state["latest"]["id"] != pending["id"]
    assert runtime.disk["heartbeat_consumed"]["monitor"] == state["latest"]
    assert runtime.codex.turn_start.await_count == 2


async def test_consumption_and_sending_intent_share_a_durable_save_before_native_rpc(runtime):
    runtime.register()
    snapshots = []
    save = runtime.service.save

    def capture_save():
        save()
        snapshots.append(runtime.disk)

    runtime.service.save = capture_save
    event = runtime.event()

    async def start(thread_id, text, **kwargs):
        state = runtime.disk
        consumed = state["heartbeat_consumed"]["monitor"]
        intent = state["intents"][event.id]
        assert intent["status"] == "sending"
        assert intent["heartbeat_receipt_id"] == consumed["id"]
        assert consumed == runtime.service.store.get_heartbeat_state("monitor")["latest"]
        return {"turn": {"id": "turn-1"}}

    runtime.codex.turn_start.side_effect = start
    assert (await runtime.dispatch(event)).status == "accepted"
    for snapshot in snapshots:
        if snapshot.get("heartbeat_consumed"):
            assert snapshot["intents"][event.id]["heartbeat_receipt_id"] == snapshot["heartbeat_consumed"]["monitor"]["id"]
    assert snapshots[0]["intents"][event.id]["status"] == "sending"


async def test_restart_requires_new_registration_and_old_known_does_not_enable_optimization(runtime):
    runtime.register()
    first = await runtime.dispatch()
    runtime.complete(first)
    consumed = runtime.disk["heartbeat_consumed"]["monitor"]
    old_observer = runtime.observer
    runtime.advance()
    runtime.restart()
    state = await runtime.service.observe_heartbeat("monitor")
    assert not state["configured"] and state["latest"]["state"] == "unconfigured"
    assert state["last_good"] == consumed
    assert (await runtime.dispatch()).status == "accepted"  # Compatibility self-review.
    assert runtime.disk["heartbeat_consumed"]["monitor"] == consumed
    assert old_observer.call_count == 1
    assert runtime.codex.turn_start.await_count == 2


async def test_re_registered_identical_scope_after_restart_remains_consumed(runtime):
    runtime.register()
    first = await runtime.dispatch()
    runtime.complete(first)
    consumed = runtime.disk["heartbeat_consumed"]["monitor"]
    runtime.advance()
    runtime.restart()
    runtime.register()
    with pytest.raises(RejectedDispatch, match="unchanged"):
        await runtime.dispatch()
    assert runtime.disk["heartbeat_consumed"]["monitor"] == consumed
    runtime.codex.turn_start.assert_awaited_once()


async def test_authentication_scope_change_is_a_new_candidate(runtime):
    runtime.register()
    first = await runtime.dispatch()
    runtime.complete(first)
    previous = runtime.disk["heartbeat_consumed"]["monitor"]
    runtime.advance()
    runtime.register(replace(runtime.source, auth_context_version="fixture-v2"))
    assert (await runtime.dispatch()).status == "accepted"
    current = runtime.disk["heartbeat_consumed"]["monitor"]
    assert current["scope_sha256"] != previous["scope_sha256"]
    assert runtime.service.store.get_heartbeat_state("monitor")["comparison"]["state"] == "baseline"
    assert runtime.codex.turn_start.await_count == 2


async def test_changed_source_cannot_clear_unknown_native_dispatch(runtime):
    runtime.register()
    event = runtime.event()
    runtime.codex.turn_start.side_effect = RpcTimeout("synthetic acknowledgement lost")
    with pytest.raises(RpcTimeout):
        await runtime.dispatch(event)
    consumed = runtime.disk["heartbeat_consumed"]["monitor"]
    runtime.advance()
    runtime.content = "snapshot-b"
    with pytest.raises(RejectedDispatch):
        await runtime.dispatch()
    assert runtime.disk["intents"][event.id]["status"] == "unknown"
    assert runtime.disk["heartbeat_consumed"]["monitor"] == consumed
    assert runtime.service.store.get_heartbeat_state("monitor")["latest"]["content_sha256"] != consumed["content_sha256"]
    runtime.codex.turn_start.assert_awaited_once()


async def test_changed_source_cannot_clear_policy_outcome_unknown_or_its_consumption(runtime):
    runtime.register()
    await runtime.service.set_task_policy(
        "monitor", asdict(TaskPolicy(120, 4, 1, 5, 30)), "policy-monitor"
    )
    first = await runtime.dispatch()
    runtime.complete(first)
    consumed = runtime.disk["heartbeat_consumed"]["monitor"]
    runtime.advance()
    runtime.content = "snapshot-b"
    with pytest.raises(RejectedDispatch):
        await runtime.dispatch()
    usage = runtime.service.store.get_task_policy("monitor")["usage"]
    assert usage["last_outcome"] == "unknown" and usage["attempts"] == 1
    assert runtime.disk["heartbeat_consumed"]["monitor"] == consumed
    runtime.codex.turn_start.assert_awaited_once()


@pytest.mark.parametrize(
    "incoming_state,change_scope", [("known", False), ("unknown", False), ("known", True)]
)
async def test_clock_rollback_rejected_collection_cannot_borrow_old_unconsumed_known(
    runtime, incoming_state, change_scope
):
    runtime.register()
    first = await runtime.service.observe_heartbeat("monitor")
    assert first["latest"]["state"] == "known"
    runtime.clock.now = 90
    runtime.clock.monotonic += 10
    runtime.observation_state = incoming_state
    if change_scope:
        runtime.register(replace(runtime.source, auth_context_version="fixture-v2"))
    event = runtime.event()
    with pytest.raises(RejectedDispatch, match="watermark"):
        await runtime.dispatch(event)
    rejected = await runtime.service.observe_heartbeat("monitor")
    assert rejected["collection_rejected"] and rejected["latest"] == first["latest"]
    with pytest.raises(RejectedDispatch, match="watermark"):
        await runtime.dispatch(event)
    assert runtime.observation_sequence == 2  # Cached rejection remains a rejection.
    assert not runtime.disk.get("heartbeat_consumed")
    runtime.codex.turn_start.assert_not_called()
    runtime.clock.now = 110
    runtime.clock.monotonic += 10
    runtime.observation_state = "known"
    assert (await runtime.dispatch(event)).status == "accepted"
    assert runtime.disk["heartbeat_consumed"]["monitor"]["id"] != first["latest"]["id"]


async def test_observation_superseded_during_native_read_is_not_consumed_or_sent(runtime):
    runtime.register()
    superseded = False

    async def read_and_reobserve(thread_id, **kwargs):
        nonlocal superseded
        if not superseded:
            superseded = True
            runtime.advance()
            runtime.observation_state = "unknown"
            await runtime.service.observe_heartbeat("monitor")
        return await runtime.read_thread(thread_id, **kwargs)

    runtime.codex.thread_read.side_effect = read_and_reobserve
    with pytest.raises(RejectedDispatch, match="superseded"):
        await runtime.dispatch()
    assert not runtime.disk.get("heartbeat_consumed") and not runtime.disk["intents"]
    assert runtime.service.store.get_heartbeat_state("monitor")["latest"]["state"] == "unknown"
    runtime.codex.turn_start.assert_not_called()


async def test_unregistered_restart_during_clock_rollback_keeps_ordinary_review_semantics(runtime):
    runtime.register()
    previous = await runtime.service.observe_heartbeat("monitor")
    runtime.clock.now = 90
    runtime.clock.monotonic += 10
    runtime.restart()
    state = await runtime.service.observe_heartbeat("monitor")
    assert state["collection_rejected"] and not state["configured"]
    assert state["latest"] == previous["latest"]  # Store preserved its evidence watermark.
    assert (await runtime.dispatch()).status == "accepted"
    assert not runtime.disk.get("heartbeat_consumed")


@pytest.mark.parametrize("wait", [0, -1, True, float("nan")])
def test_internal_source_registration_requires_explicit_positive_wait(runtime, wait):
    with pytest.raises(ValueError):
        runtime.service.register_heartbeat_source("monitor", runtime.source, wait_seconds=wait)
