"""Crash and namespace evidence from real temporary stores, without model calls."""

import pytest

from test_task_policy_service import runtime as runtime


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
