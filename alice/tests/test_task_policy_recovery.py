"""Crash and namespace evidence from real temporary stores, without model calls."""

import pytest
from dataclasses import replace

from test_task_policy_service import runtime as runtime
from alice_codex.scheduler import RejectedDispatch


async def test_restart_marks_existing_accepted_receipt_unknown_without_resending(runtime):
    runtime.seed()
    await runtime.set_policy()
    first = await runtime.service.submit("research", "work", intent_id="attempt")
    original_root = first["thread_id"]
    assert first["status"] == "accepted"
    restarted = runtime.restart()
    restarted._recover_task_policy_usage()
    record = restarted.store.get_task_policy("research")
    assert record["usage"]["attempts"] == 1
    assert record["usage"]["last_outcome"] == "unknown"
    replay = await restarted.submit("research", "work", intent_id="attempt")
    assert replay["status"] == "unknown" and replay["thread_id"] == original_root
    runtime.codex.turn_start.assert_awaited_once()
    assert (await restarted.task_policy_status("research"))["decision"]["recovery_required"]


@pytest.mark.parametrize("mutation", ["alias", "intent_root", "fingerprint", "charged"])
async def test_conflicting_runtime_does_not_reassign_a_charged_attempt(runtime, mutation):
    runtime.seed()
    await runtime.set_policy()
    await runtime.service.submit("research", "work", intent_id="attempt")
    intent = runtime.service.state["intents"]["attempt"]
    if mutation == "alias":
        runtime.service.state["tasks"]["research"]["thread_id"] = "other-root"
    elif mutation == "intent_root":
        intent["thread_id"] = "other-root"
    elif mutation == "fingerprint":
        intent["input_sha256"] = "0" * 64
    else:
        intent["policy_charged"] = False
    runtime.service.save()
    before = runtime.service.store.get_task_policy("research")
    restarted = runtime.restart()
    with pytest.raises(RuntimeError, match="charged policy receipt|matching owned root"):
        restarted._recover_task_policy_usage()
    assert restarted.store.get_task_policy("research") == before
    runtime.codex.turn_start.assert_awaited_once()


async def test_policy_request_id_cannot_reuse_prior_unbudgeted_input(runtime):
    runtime.seed()
    await runtime.service.submit("research", "work", intent_id="same-id")
    runtime.service._complete_turn("root-research", {"id": "turn-1", "status": "completed"})
    with pytest.raises(ValueError, match="already identifies task input"):
        await runtime.set_policy(request_id="same-id")
    assert runtime.service.store.get_task_policy("research") is None


@pytest.mark.parametrize("outcome", ["failed", "unknown", "complete", "unfunded_goal"])
async def test_global_resume_preserves_blocked_task_and_restores_unbudgeted_main(runtime, outcome):
    runtime.seed()
    await runtime.set_policy(max_attempts=1)
    if outcome != "unfunded_goal":
        await runtime.service.submit("research", "work", intent_id="attempt")
        runtime.service._complete_turn("root-research", {"id": "turn-1", "status": "completed"})
        if outcome != "unknown":
            runtime.service.store.finish_task_attempt(
                "research",
                "attempt",
                outcome,
                now=101,
                evidence={"host_fixture": "verified"} if outcome == "complete" else None,
            )
    else:
        runtime.rpc.request.side_effect = lambda method, params: {
            "goal": {"status": "paused"} if params.get("threadId") == "root-research" else None
        }
    runtime.clock.now = 110
    runtime.service.state["tasks"]["research"]["paused"] = True
    runtime.seed("main", paused=True)
    runtime.service.store.set_autonomy_paused(True)
    before = runtime.service.store.get_task_policy("research")
    result = await runtime.service.resume()
    assert result["resumed"] == "autonomy"
    assert set(result["blocked_tasks"]) == {"research"}
    assert result["blocked_tasks"]["research"]
    assert runtime.disk["tasks"]["research"]["paused"]
    assert not runtime.disk["tasks"]["main"]["paused"]
    assert not runtime.service.store.is_autonomy_paused()
    after = runtime.service.store.get_task_policy("research")
    assert after["attempts"] == before["attempts"]
    assert after["current_attempt_id"] == before["current_attempt_id"]
    runtime.codex.queue_start.assert_not_called()
    assert not any(
        call.args[0] == "thread/goal/set" for call in runtime.rpc.request.await_args_list
    )
    with pytest.raises(RejectedDispatch):
        await runtime.service.resume("research")


@pytest.mark.parametrize("new_execution", [None, "native_active", "start_notification"])
async def test_confirmed_deadline_stop_releases_only_execution_capacity(runtime, new_execution):
    runtime.config = replace(runtime.config, max_active_tasks=1)
    runtime.service.config = runtime.config
    runtime.seed()
    await runtime.set_policy(max_attempts=1)
    await runtime.service.submit("research", "work", intent_id="attempt")
    runtime.seed("main", paused=True)
    service = runtime.restart()
    service._recover_task_policy_usage()
    runtime.clock.now = 110
    service.store.set_autonomy_paused(False)
    service.state["tasks"]["main"]["paused"] = False
    # Idle metadata alone does not resolve an unacknowledged native request.
    assert await service.is_busy("main")
    runtime.clock.now = 230
    service.state["tasks"]["main"]["paused"] = True
    service.store.set_autonomy_paused(True)
    resumed = await service.resume()
    assert "research" in resumed["blocked_tasks"]
    assert service.state["tasks"]["research"]["policy_deadline_stopped"]
    runtime.codex.stop_tree.assert_awaited_once_with("root-research")
    if new_execution == "native_active":
        runtime.native_status["root-research"] = "active"
    elif new_execution == "start_notification":
        service._on_notification(
            {
                "method": "turn/started",
                "params": {"threadId": "root-research", "turn": {"id": "new-turn"}},
            }
        )
        assert not service.state["tasks"]["research"].get("policy_deadline_stopped")
    if new_execution is None:
        assert not await service.is_busy("main")
        admitted = await service.submit(
            "main", "periodic maintenance", intent_id="main-after-stop", automatic=True
        )
        assert admitted["status"] == "accepted"
        assert runtime.codex.turn_start.await_count == 2
    else:
        assert await service.is_busy("main")
        with pytest.raises(RejectedDispatch):
            await service.submit(
                "main", "periodic maintenance", intent_id="main-after-stop", automatic=True
            )
        runtime.codex.turn_start.assert_awaited_once()
    with pytest.raises(RejectedDispatch):
        await service.submit("research", "same unknown work again", intent_id="different-id")
    assert service.state["intents"]["attempt"]["status"] == "unknown"
    record = service.store.get_task_policy("research")
    assert record["usage"]["attempts"] == 1 and record["usage"]["last_outcome"] == "unknown"
