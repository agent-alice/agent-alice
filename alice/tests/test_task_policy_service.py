"""Service policy regressions with temporary stores and synthetic native controls."""

from dataclasses import asdict
import hashlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from alice_codex.config import RuntimeConfig
from alice_codex.resources import TaskPolicy
from alice_codex.rpc import RpcError, RpcTimeout
from alice_codex.scheduler import RejectedDispatch
from alice_codex.service import Service
import alice_codex.service as service_module


class PolicyRuntime:
    def __init__(self, config, clock):
        self.config, self.clock = config, clock
        self.instances = []
        self.native_status = {}
        self.codex = Mock()
        self.codex.thread_read = AsyncMock(side_effect=self.read_thread)
        self.codex.thread_start = AsyncMock(side_effect=AssertionError("existing root must be reused"))
        self.codex.turn_start = AsyncMock(return_value={"turn": {"id": "turn-1"}})
        self.codex.queue_add = AsyncMock(return_value={"data": []})
        self.codex.queue_list = AsyncMock(return_value={"data": []})
        self.codex.queue_start = AsyncMock(return_value={"data": []})
        self.codex.stop_tree = AsyncMock(return_value={"stopped": []})
        self.codex.find_turn_by_client_id = AsyncMock(return_value=None)
        self.rpc = Mock()
        self.rpc.request = AsyncMock(return_value={"data": [], "goal": None})
        self.service = self.restart()

    def restart(self):
        service = Service(self.config)
        service.ready, service.codex, service.rpc = True, self.codex, self.rpc
        self.instances.append(service)
        self.service = service
        return service

    async def read_thread(self, thread_id, **kwargs):
        return {"thread": {"id": thread_id, "status": {"type": self.native_status.get(thread_id, "idle")}}}

    def seed(self, target="research", *, paused=False):
        thread_id = "root-" + target
        task = {
            "thread_id": thread_id, "paused": paused, "has_input": True,
            "bootstrap": {"version": 1, "thread_id": thread_id, "state": "ready"},
        }
        self.service.state["tasks"][target] = task
        self.service.save()
        return task

    @property
    def disk(self):
        return json.loads(self.service.path.read_text())

    async def set_policy(self, target="research", *, request_id="policy-1", **limits):
        policy = {**asdict(TaskPolicy(120, 4, 2, 5, 30)), **limits}
        await self.service.handle(
            "task_policy_set", {"target": target, "policy": policy, "request_id": request_id}
        )
        return policy


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    clock = SimpleNamespace(now=100.0)
    monkeypatch.setattr(service_module.time, "time", lambda: clock.now)
    config = RuntimeConfig(str(tmp_path), "/usr/bin/true", "codex-cli fixture", "unused")
    config.prepare_directories()
    runtime = PolicyRuntime(config, clock)
    yield runtime
    for service in runtime.instances:
        service.store.close()
    for socket in config.socket_dir.iterdir():
        socket.unlink()
    config.socket_dir.rmdir()


async def test_explicit_policy_set_and_status_do_not_start_task_or_native_work(runtime):
    empty = await runtime.service.handle("task_policy_status", {"target": "research"})
    assert empty == {
        "target": "research", "policy": None, "usage": None, "decision": None,
        "enforcement_scope": "alice_admission",
    }
    expected = await runtime.set_policy()
    status = await runtime.service.handle("task_policy_status", {"target": "research"})
    assert status["target"] == "research" and status["policy"] == expected
    assert status["enforcement_scope"] == "alice_admission"
    assert status["usage"] is None and status["decision"]["allowed"]
    assert not runtime.service.state["tasks"] and not runtime.service.state["intents"]
    runtime.codex.thread_start.assert_not_called()
    runtime.codex.turn_start.assert_not_called()
    with pytest.raises(ValueError):
        await runtime.service.handle("task_policy_set", {
            "target": "research", "request_id": "invalid", "policy": {"max_attempts": 3},
        })
    assert runtime.service.store.get_task_policy("research")["policy"] == expected
    assert (await runtime.restart().handle("task_policy_status", {"target": "research"}))["policy"] == expected


@pytest.mark.parametrize("target", ["", "new", "summary:L1", "scheduled:hourly"])
async def test_policy_requires_stable_supported_explicit_target(runtime, target):
    with pytest.raises(ValueError):
        await runtime.set_policy(target)
    assert runtime.service.store.list_task_policies() == []


@pytest.mark.parametrize(
    "target,summary_plan,expected",
    [
        ("research", None, True),
        ("main", None, False),
        ("new", None, False),
        ("summary:L1", None, False),
        ("scheduled:hourly", None, False),
        ("named-summary", "unused-synthetic-summary-plan", False),
    ],
)
async def test_configured_default_only_bounds_named_work(runtime, target, summary_plan, expected):
    runtime.config.task_policy = asdict(TaskPolicy(120, 4, 2, 5, 30))
    runtime.seed(target)
    result = await runtime.service.submit(target, "synthetic input", intent_id="attempt-1", summary_plan=summary_plan)
    record = runtime.service.store.get_task_policy(target)
    assert result["policy_charged"] is expected
    if expected:
        assert record["usage"]["attempts"] == 1
        assert record["policy"] == runtime.config.task_policy
    else:
        assert record is None


async def test_explicit_main_policy_is_allowed_and_config_default_does_not_replace_it(runtime):
    runtime.config.task_policy = asdict(TaskPolicy(120, 4, 2, 5, 30))
    expected = await runtime.set_policy("main", max_attempts=1)
    runtime.seed("main")
    await runtime.service.submit("main", "synthetic input", intent_id="attempt-1")
    assert runtime.service.store.get_task_policy("main")["policy"] == expected


async def test_native_task_status_includes_policy_without_consuming_another_attempt(runtime):
    expected = await runtime.set_policy()
    task = runtime.seed()
    await runtime.service.submit("research", "synthetic input", intent_id="attempt-1")
    status = await runtime.service.handle("task_status", {"target": "research"})
    assert status["thread"]["id"] == task["thread_id"]
    assert status["enforcement_scope"] == "alice_admission"
    assert status["policy"] == expected and status["usage"]["attempts"] == 1
    assert status["decision"]["reasons"] == ["task_busy"]
    runtime.codex.thread_read.assert_awaited_with(task["thread_id"], include_turns=True)
    runtime.codex.turn_start.assert_awaited_once()


async def test_attempt_is_durable_before_native_rpc_and_duplicate_or_new_id_cannot_refund_it(runtime):
    await runtime.set_policy(max_attempts=1)
    task = runtime.seed()

    async def start(thread_id, text, **kwargs):
        record = runtime.service.store.get_task_policy("research")
        assert record["usage"]["attempts"] == 1 and record["usage"]["last_outcome"] == "running"
        assert record["attempts"]["attempt-1"]["thread_id"] == thread_id == task["thread_id"]
        assert runtime.disk["intents"]["attempt-1"]["status"] == "sending"
        assert runtime.disk["intents"]["attempt-1"]["policy_charged"] is True
        return {"turn": {"id": "turn-1"}}

    runtime.codex.turn_start.side_effect = start
    first = await runtime.service.submit("research", "synthetic input", intent_id="attempt-1")
    assert await runtime.service.submit("research", "synthetic input", intent_id="attempt-1") == first
    runtime.clock.now = 101
    runtime.service._complete_turn(task["thread_id"], {"id": "turn-1", "status": "failed"})
    runtime.clock.now = 106
    with pytest.raises(RejectedDispatch, match="task_attempts_exhausted"):
        await runtime.service.submit("research", "synthetic input", intent_id="new-request")
    assert await runtime.service.submit("research", "synthetic input", intent_id="attempt-1") == first
    assert runtime.service.store.get_task_policy("research")["usage"]["attempts"] == 1
    assert "new-request" not in runtime.service.state["intents"]
    runtime.codex.turn_start.assert_awaited_once()


async def test_expanding_limits_preserves_consumption_receipts_and_persistent_pause(runtime):
    original = await runtime.set_policy(max_attempts=1)
    task = runtime.seed()
    await runtime.service.submit("research", "synthetic input", intent_id="attempt-1")
    runtime.clock.now = 101
    runtime.service._complete_turn(task["thread_id"], {"id": "turn-1", "status": "failed"})
    await runtime.service.pause("research")
    before = runtime.service.store.get_task_policy("research")
    expanded = await runtime.set_policy(request_id="policy-2", max_attempts=6, max_elapsed_seconds=600)
    after = runtime.service.store.get_task_policy("research")
    assert after["policy"] == expanded
    assert after["usage"] == before["usage"] and after["attempts"] == before["attempts"]
    assert runtime.disk["tasks"]["research"]["paused"] is True
    await runtime.service.handle("task_policy_set", {
        "target": "research", "policy": original, "request_id": "policy-1",
    })
    assert runtime.service.store.get_task_policy("research")["policy"] == expanded
    restarted = runtime.restart()
    assert restarted.state["tasks"]["research"]["paused"] is True
    assert restarted.store.get_task_policy("research")["usage"] == before["usage"]
    with pytest.raises(RejectedDispatch, match="paused"):
        await restarted.submit("research", "another input", intent_id="attempt-2")
    runtime.codex.turn_start.assert_awaited_once()


async def test_last_legal_busy_attempt_runs_until_deadline_even_with_zero_attempts_left(runtime):
    await runtime.set_policy(max_attempts=1, max_elapsed_seconds=10)
    task = runtime.seed()
    await runtime.service.submit("research", "synthetic input", intent_id="attempt-1")
    runtime.native_status[task["thread_id"]] = "active"
    runtime.clock.now = 109
    status = await runtime.service.task_policy_status("research")
    assert status["decision"]["remaining"]["attempts"] == 0
    assert status["decision"]["state"] == "waiting"
    assert status["decision"]["reasons"] == ["task_busy"]
    assert 0 < await runtime.service._enforce_task_deadlines() <= 1
    assert not task["paused"]
    runtime.codex.stop_tree.assert_not_called()


async def test_deadline_persists_pause_before_native_stop_and_survives_restart_and_clock_rollback(runtime):
    await runtime.set_policy(max_attempts=1, max_elapsed_seconds=10)
    task = runtime.seed()
    await runtime.service.submit("research", "synthetic input", intent_id="attempt-1")

    async def stop(thread_id, **kwargs):
        saved = runtime.disk["tasks"]["research"]
        assert saved["paused"] is True and saved["policy_pause_pending"] is True
        assert thread_id == task["thread_id"]
        return {"stopped": [thread_id]}

    runtime.codex.stop_tree.side_effect = stop
    runtime.clock.now = 110
    await runtime.service._enforce_task_deadlines()
    assert runtime.disk["tasks"]["research"]["paused"] is True
    assert runtime.disk["tasks"]["research"]["policy_deadline_stopped"] is True
    assert "policy_pause_pending" not in runtime.disk["tasks"]["research"]
    runtime.codex.stop_tree.assert_awaited_once()
    runtime.clock.now = 105
    restarted = runtime.restart()
    await restarted._enforce_task_deadlines()
    status = await restarted.task_policy_status("research")
    assert status["decision"]["remaining"]["seconds"] == 0
    assert "task_time_exhausted" in status["decision"]["reasons"]
    assert restarted.state["tasks"]["research"]["paused"] is True
    runtime.codex.stop_tree.assert_awaited_once()


async def test_unproven_deadline_stop_keeps_stop_obligation_for_restart(runtime):
    await runtime.set_policy(max_elapsed_seconds=10)
    runtime.seed()
    await runtime.service.submit("research", "synthetic input", intent_id="attempt-1")
    runtime.clock.now = 110
    runtime.codex.stop_tree.side_effect = RpcError("controlled stop acknowledgement lost")
    with pytest.raises(RuntimeError, match="paused"):
        await runtime.service._enforce_task_deadlines()
    assert runtime.disk["tasks"]["research"]["paused"] is True
    assert runtime.disk["tasks"]["research"]["policy_pause_pending"] is True
    runtime.codex.stop_tree.side_effect = None
    restarted = runtime.restart()
    await restarted._enforce_task_deadlines()
    assert runtime.disk["tasks"]["research"]["policy_deadline_stopped"] is True
    assert runtime.codex.stop_tree.await_count == 2


async def test_sqlite_charge_without_runtime_intent_recovers_unknown_and_never_replays(runtime):
    await runtime.set_policy()
    task = runtime.seed()
    text = "synthetic input"
    fingerprint = hashlib.sha256(("research\0" + text).encode()).hexdigest()
    runtime.service.store.admit_task_attempt(
        "research", "attempt-1", fingerprint, now=100, busy=False, thread_id=task["thread_id"]
    )
    assert not runtime.disk["intents"]
    restarted = runtime.restart()
    await restarted._enforce_task_deadlines()
    recovered = restarted.state["intents"]["attempt-1"]
    assert recovered["status"] == "unknown" and recovered["recovered_policy_receipt"] is True
    assert recovered["thread_id"] == task["thread_id"]
    assert restarted.store.get_task_policy("research")["usage"]["last_outcome"] == "unknown"
    assert await restarted.submit("research", text, intent_id="attempt-1") == recovered
    with pytest.raises(RejectedDispatch, match="attempt_outcome_unknown"):
        await restarted.submit("research", text, intent_id="different-id")
    runtime.codex.turn_start.assert_not_called()
    runtime.codex.queue_add.assert_not_called()
    assert restarted.store.get_task_policy("research")["usage"]["attempts"] == 1


async def test_lost_native_ack_consumes_once_and_remains_unknown_across_restart(runtime):
    await runtime.set_policy()
    runtime.seed()
    runtime.codex.turn_start.side_effect = RpcTimeout("synthetic acknowledgement lost")
    with pytest.raises(RpcTimeout):
        await runtime.service.submit("research", "synthetic input", intent_id="attempt-1")
    record = runtime.service.store.get_task_policy("research")
    assert record["usage"]["attempts"] == 1 and record["usage"]["last_outcome"] == "unknown"
    assert runtime.disk["intents"]["attempt-1"]["status"] == "unknown"
    runtime.clock.now = 101
    restarted = runtime.restart()
    repeated = await restarted.submit("research", "synthetic input", intent_id="attempt-1")
    assert repeated["status"] == "unknown"
    with pytest.raises(RejectedDispatch, match="attempt_outcome_unknown"):
        await restarted.submit("research", "synthetic input", intent_id="different-id")
    assert restarted.store.get_task_policy("research")["usage"]["attempts"] == 1
    runtime.codex.turn_start.assert_awaited_once()
    runtime.codex.queue_add.assert_not_called()


async def test_orphan_charge_cannot_bind_to_replacement_alias_or_start_more_work(runtime):
    await runtime.set_policy()
    task = runtime.seed()
    runtime.service.store.admit_task_attempt(
        "research", "attempt-1", hashlib.sha256(b"synthetic").hexdigest(),
        now=100, busy=False, thread_id=task["thread_id"],
    )
    task["thread_id"] = "replacement-root"
    runtime.service.save()
    with pytest.raises(RuntimeError, match="matching owned root"):
        await runtime.restart()._enforce_task_deadlines()
    runtime.codex.thread_start.assert_not_called()
    runtime.codex.turn_start.assert_not_called()


async def test_native_completed_is_unknown_business_outcome_and_repeats_do_not_extend_evidence(runtime):
    await runtime.set_policy()
    task = runtime.seed()
    await runtime.service.submit("research", "synthetic input", intent_id="attempt-1")
    runtime.clock.now = 101
    runtime.service._complete_turn(task["thread_id"], {"id": "turn-1", "status": "completed"})
    record = runtime.service.store.get_task_policy("research")
    assert record["usage"]["last_outcome"] == "unknown"
    assert record["usage"]["consecutive_failures"] == 0
    assert runtime.service.state["intents"]["attempt-1"]["status"] == "completed"
    assert (await runtime.service.task_policy_status("research"))["decision"]["state"] == "reconciliation_required"
    runtime.clock.now = 102
    runtime.service._complete_turn(task["thread_id"], {"id": "turn-1", "status": "completed"})
    assert runtime.service.store.get_task_policy("research")["usage"] == record["usage"]
    with pytest.raises(RejectedDispatch, match="attempt_outcome_unknown"):
        await runtime.service.submit("research", "new input", intent_id="attempt-2")
    runtime.codex.turn_start.assert_awaited_once()


async def test_extending_policy_and_resuming_cannot_clear_unknown_outcome(runtime):
    await runtime.set_policy()
    task = runtime.seed()
    await runtime.service.submit("research", "synthetic input", intent_id="attempt-1")
    runtime.clock.now = 101
    runtime.service._complete_turn(task["thread_id"], {"id": "turn-1", "status": "completed"})
    await runtime.service.pause("research")
    await runtime.set_policy(request_id="extended", max_elapsed_seconds=600, max_attempts=20)
    with pytest.raises(RejectedDispatch, match="unknown|policy|reconcil"):
        await runtime.service.resume("research")
    assert runtime.disk["tasks"]["research"]["paused"] is True
    assert runtime.service.store.get_task_policy("research")["usage"]["last_outcome"] == "unknown"
    runtime.codex.queue_start.assert_not_called()
    assert all(call.args[0] != "thread/goal/set" for call in runtime.rpc.request.await_args_list)
