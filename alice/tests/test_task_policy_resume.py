"""Policy resume regressions with isolated stores and synthetic native receipts."""

from dataclasses import asdict
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from alice_codex.config import RuntimeConfig
from alice_codex.resources import TaskPolicy
from alice_codex.scheduler import RejectedDispatch
from alice_codex.service import Service
import alice_codex.service as service_module


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    clock = SimpleNamespace(now=100.0)
    monkeypatch.setattr(service_module.time, "time", lambda: clock.now)
    config = RuntimeConfig(str(tmp_path), "/usr/bin/true", "codex-cli fixture", "unused")
    config.prepare_directories()
    service = Service(config)
    service.ready = True
    service.codex = Mock()
    service.codex.thread_read = AsyncMock(
        return_value={"thread": {"id": "owned-root", "status": {"type": "idle"}}}
    )
    service.codex.turn_start = AsyncMock(return_value={"turn": {"id": "charged-turn"}})
    service.codex.queue_list = AsyncMock(return_value={"data": []})
    service.codex.queue_start = AsyncMock(return_value={})
    service.codex.stop_tree = AsyncMock(return_value={"stopped": ["owned-root"]})
    service.rpc = Mock()
    service.rpc.request = AsyncMock(return_value={"goal": None})
    service.state["tasks"]["research"] = {
        "thread_id": "owned-root",
        "paused": False,
        "has_input": True,
        "bootstrap": {"version": 1, "thread_id": "owned-root", "state": "ready"},
    }
    service.save()
    yield SimpleNamespace(service=service, clock=clock)
    service.store.close()
    for socket in config.socket_dir.iterdir():
        socket.unlink()
    config.socket_dir.rmdir()


async def policy(runtime, *, seconds=120, attempts=4):
    await runtime.service.set_task_policy(
        "research", asdict(TaskPolicy(seconds, attempts, 3, 1, 1)), "policy-1"
    )


def old_native_work(service, kind):
    if kind == "queue":
        service.codex.queue_list.return_value = {"data": [{"id": "old-native-input"}]}
    else:
        service.rpc.request.side_effect = lambda method, params: (
            {"goal": {"status": "paused"}} if method == "thread/goal/get" else {}
        )
    service.state["tasks"]["research"]["paused"] = True
    service.save()


def activated_goal(service):
    return any(
        call.args[0] == "thread/goal/set" and call.args[1].get("status") == "active"
        for call in service.rpc.request.await_args_list
    )


def disk_task(service):
    return json.loads(service.path.read_text())["tasks"]["research"]


@pytest.mark.parametrize("kind", ["queue", "goal"])
@pytest.mark.parametrize(
    "outcome", [None, "failed", "progress", "unchanged", "complete", "unknown"]
)
async def test_resume_never_reactivates_unfunded_or_finished_native_work(runtime, kind, outcome):
    service = runtime.service
    await policy(runtime)
    if outcome is not None:
        await service.submit("research", "one admitted attempt", intent_id="attempt-1")
        # Native completion alone leaves the business outcome unresolved.
        service._complete_turn("owned-root", {"id": "charged-turn", "status": "completed"})
        if outcome != "unknown":
            service.store.finish_task_attempt(
                "research",
                "attempt-1",
                outcome,
                now=101,
                evidence={"host_verified_fixture": True}
                if outcome in {"progress", "unchanged", "complete"}
                else None,
            )
    runtime.clock.now = 105  # Retry/unchanged waits have elapsed; they cannot mask this guard.
    old_native_work(service, kind)
    before = service.store.get_task_policy("research")
    with pytest.raises(RejectedDispatch):
        await service.resume("research")
    after = service.store.get_task_policy("research")
    assert disk_task(service)["paused"] is True
    service.codex.queue_start.assert_not_called()
    assert not activated_goal(service)
    assert after["attempts"] == before["attempts"]
    assert (after["usage"] is None) == (outcome is None)
    if outcome is not None:
        assert after["usage"]["attempts"] == 1
        assert after["usage"]["last_outcome"] == outcome


@pytest.mark.parametrize("kind", ["queue", "goal"])
@pytest.mark.parametrize("status", ["accepted", "queued"])
async def test_resume_continues_confirmed_final_admitted_attempt_without_another_charge(
    runtime, kind, status
):
    service = runtime.service
    await policy(runtime, attempts=1)
    intent = await service.submit("research", "one admitted attempt", intent_id="attempt-1")
    intent["status"] = status  # Synthetic known native acceptance/queue acknowledgement.
    old_native_work(service, kind)
    before = service.store.get_task_policy("research")
    assert before["usage"]["attempts"] == 1
    assert (await service.resume("research"))["resumed"] == "research"
    assert disk_task(service)["paused"] is False
    assert service.codex.queue_start.await_count == (kind == "queue")
    assert activated_goal(service) == (kind == "goal")
    after = service.store.get_task_policy("research")
    assert after["usage"] == before["usage"]
    assert after["attempts"] == before["attempts"]
    service.codex.turn_start.assert_awaited_once()


@pytest.mark.parametrize("kind", ["queue", "goal"])
@pytest.mark.parametrize("status", ["sending", "unknown"])
async def test_running_charge_without_confirmed_native_acceptance_cannot_resume(
    runtime, kind, status
):
    service = runtime.service
    await policy(runtime)
    intent = await service.submit("research", "one admitted attempt", intent_id="attempt-1")
    intent["status"] = (
        status  # Runtime crash/acknowledgement boundary, SQLite charge remains running.
    )
    old_native_work(service, kind)
    with pytest.raises(RejectedDispatch):
        await service.resume("research")
    assert disk_task(service)["paused"] is True
    service.codex.queue_start.assert_not_called()
    assert not activated_goal(service)
    assert service.store.get_task_policy("research")["usage"]["attempts"] == 1


async def test_empty_policy_root_can_clear_pause_without_starting_or_charging_work(runtime):
    service = runtime.service
    await policy(runtime)
    task = service.state["tasks"]["research"]
    task.update(paused=True, has_input=False)
    service.save()
    await service.resume("research")
    assert disk_task(service)["paused"] is False
    assert service.store.get_task_policy("research")["usage"] is None
    service.codex.turn_start.assert_not_called()
    service.codex.queue_start.assert_not_called()
    assert not activated_goal(service)


async def test_new_native_turn_rearms_expired_stop_without_inventing_a_charge(runtime):
    service = runtime.service
    await policy(runtime, seconds=2, attempts=1)
    await service.submit("research", "one admitted attempt", intent_id="attempt-1")
    service._complete_turn("owned-root", {"id": "charged-turn", "status": "interrupted"})
    runtime.clock.now = 102

    async def stop(thread_id, **kwargs):
        assert thread_id == "owned-root"
        assert disk_task(service)["paused"] is True
        assert disk_task(service)["policy_pause_pending"] is True
        return {"stopped": [thread_id]}

    service.codex.stop_tree.side_effect = stop
    await service._enforce_task_deadlines()
    assert disk_task(service)["policy_deadline_stopped"] is True
    before = service.store.get_task_policy("research")
    service._policy_wakeup.clear()
    service._on_notification(
        {
            "method": "turn/started",
            "params": {
                "threadId": "owned-root",
                "turn": {"id": "external-new-turn", "status": "inProgress"},
            },
        }
    )
    assert "policy_deadline_stopped" not in disk_task(service)
    assert disk_task(service)["paused"] is True
    assert service._policy_wakeup.is_set()
    await service._enforce_task_deadlines()
    assert service.codex.stop_tree.await_count == 2
    assert disk_task(service)["policy_deadline_stopped"] is True
    assert "policy_pause_pending" not in disk_task(service)
    after = service.store.get_task_policy("research")
    assert after["usage"] == before["usage"]
    assert after["attempts"] == before["attempts"]
    assert set(service.state["intents"]) == {"attempt-1"}


async def test_new_turn_before_stop_returns_preserves_the_next_stop_obligation(runtime):
    service = runtime.service
    await policy(runtime, seconds=2, attempts=1)
    await service.submit("research", "one admitted attempt", intent_id="attempt-1")
    service._complete_turn("owned-root", {"id": "charged-turn", "status": "interrupted"})
    runtime.clock.now = 102

    async def stop(thread_id, **kwargs):
        assert disk_task(service)["policy_pause_pending"] is True
        if service.codex.stop_tree.await_count == 1:
            # A second client starts work after the final native idle proof,
            # before the first stop operation returns to its caller.
            service._on_notification(
                {
                    "method": "turn/started",
                    "params": {
                        "threadId": thread_id,
                        "turn": {"id": "started-during-stop-return", "status": "inProgress"},
                    },
                }
            )
        return {"stopped": [thread_id]}

    service.codex.stop_tree.side_effect = stop
    service._policy_wakeup.clear()
    delay = await service._enforce_task_deadlines()
    assert service.codex.stop_tree.await_count == 1
    assert disk_task(service)["paused"] is True
    assert disk_task(service)["policy_pause_pending"] is True
    assert "policy_deadline_stopped" not in disk_task(service)
    assert service._policy_wakeup.is_set()
    assert 0 < delay <= 0.01
    before = service.store.get_task_policy("research")
    await service._enforce_task_deadlines()
    assert service.codex.stop_tree.await_count == 2
    assert disk_task(service)["policy_deadline_stopped"] is True
    assert "policy_pause_pending" not in disk_task(service)
    after = service.store.get_task_policy("research")
    assert after["usage"] == before["usage"]
    assert after["attempts"] == before["attempts"]
    assert set(service.state["intents"]) == {"attempt-1"}
