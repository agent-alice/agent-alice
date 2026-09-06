"""Synthetic task policy acceptance: no service, native model or external actions."""

from dataclasses import asdict, replace

import pytest

from alice_codex.resources import ResourceLedger, TaskPolicy, TaskUsage


@pytest.fixture
def policy():
    return TaskPolicy(
        max_elapsed_seconds=120,
        max_attempts=4,
        max_retries=1,
        retry_wait_seconds=5,
        unchanged_wait_seconds=30,
    )


def finished(outcome, *, attempts=1, failures=0, at=105):
    return TaskUsage(
        started_at=100,
        attempts=attempts,
        consecutive_failures=failures,
        last_outcome=outcome,
        last_finished_at=at,
    )


def test_busy_suppression_preserves_usage_and_reports_time_limit(policy):
    usage = TaskUsage(started_at=100)
    before = asdict(usage)
    for now in [100, 110, 219]:
        result = policy.decide(usage, now=now, busy=True)
        assert not result["allowed"]
        assert result["reasons"] == ["task_busy"]
        assert result["next_attempt_at"] is None
        assert result["remaining"]["attempts"] == 4
    assert asdict(usage) == before
    assert policy.decide(usage, now=220, busy=True)["reasons"] == ["task_time_exhausted"]
    assert policy.decide(usage, now=219, busy=False)["allowed"]


@pytest.mark.parametrize("prior_failures", [0, 1])
def test_last_admitted_attempt_can_run_until_deadline(prior_failures):
    policy = TaskPolicy(120, prior_failures + 1, prior_failures, 5, 30)
    usage = TaskUsage(
        started_at=100,
        attempts=prior_failures + 1,
        consecutive_failures=prior_failures,
        last_outcome="running",
    )
    for now in (101, 219.999):
        result = policy.decide(usage, now=now, busy=True)
        assert result["state"] == "waiting"
        assert result["reasons"] == ["task_busy"]
        assert result["remaining"]["attempts"] == 0
        assert not result["allowed"] and not result["recovery_required"]
    deadline = policy.decide(usage, now=220, busy=True)
    assert deadline["state"] == "exhausted"
    assert "task_time_exhausted" in deadline["reasons"]
    assert deadline["recovery_required"]
    terminal = replace(usage, last_outcome="progress", last_finished_at=105, consecutive_failures=0)
    next_attempt = policy.decide(terminal, now=106, busy=False)
    assert next_attempt["state"] == "exhausted"
    assert next_attempt["reasons"] == ["task_attempts_exhausted"]


def test_busy_wait_does_not_consume_retry_or_relax_next_dispatch_limit(policy):
    usage = finished("failed", attempts=2, failures=2)
    result = policy.decide(usage, now=106, busy=True)
    assert result["state"] == "waiting" and result["reasons"] == ["task_busy"]
    assert result["remaining"]["retries"] == 0
    assert policy.decide(usage, now=106, busy=False)["reasons"] == ["task_retries_exhausted"]
    assert "task_time_exhausted" in policy.decide(usage, now=220, busy=True)["reasons"]


@pytest.mark.parametrize("outcome", ["running", "unknown"])
def test_busy_deadline_watermark_requires_stop_even_after_clock_rollback(policy, outcome):
    usage = TaskUsage(
        started_at=100,
        attempts=1,
        last_outcome=outcome,
        last_finished_at=105 if outcome == "unknown" else None,
        last_checked_at=220,
    )
    result = policy.decide(usage, now=219, busy=True)
    assert result["state"] == "exhausted"
    assert result["reasons"] == ["task_time_exhausted"]
    assert result["observed_at"] == 220 and result["remaining"]["seconds"] == 0
    extended = replace(policy, max_elapsed_seconds=180)
    assert extended.decide(usage, now=220, busy=False)["state"] == "reconciliation_required"
    assert usage.last_outcome == outcome


def test_unchanged_observations_wait_then_stop_without_infinite_polling(policy):
    usage = TaskUsage(started_at=100)
    executed = []
    for now in range(100, 221):
        result = policy.decide(usage, now=now, busy=False)
        if result["allowed"]:
            executed.append(now)
            usage = replace(
                usage,
                last_checked_at=result["observed_at"],
                attempts=usage.attempts + 1,
                last_outcome="unchanged",
                last_finished_at=now,
            )
    assert executed == [100, 130, 160, 190]
    assert result["state"] == "exhausted"
    assert result["remaining"] == {"seconds": 0, "attempts": 0, "retries": 1}
    assert result["recovery_required"]
    assert usage.attempts == 4


def test_confirmed_failure_waits_and_retry_allowance_stops_repeated_error(policy):
    first = finished("failed", failures=1)
    wait = policy.decide(first, now=109, busy=False)
    assert wait["state"] == "waiting" and wait["reasons"] == ["retry_wait"]
    assert wait["next_attempt_at"] == 110
    assert policy.decide(first, now=110, busy=False)["allowed"]
    repeated = finished("failed", attempts=2, failures=2, at=115)
    decision = policy.decide(repeated, now=120, busy=False)
    assert decision["state"] == "exhausted"
    assert decision["reasons"] == ["task_retries_exhausted"]
    assert decision["remaining"]["retries"] == 0
    assert decision["remaining"]["attempts"] == 2
    assert decision["recovery_required"]
    no_retry = replace(policy, max_retries=0)
    assert no_retry.decide(first, now=110, busy=False)["state"] == "exhausted"


def test_task_time_includes_waiting_and_cannot_dispatch_at_exact_deadline(policy):
    before = finished("progress", at=105)
    assert policy.decide(before, now=219.999, busy=False)["allowed"]
    boundary = policy.decide(before, now=220, busy=False)
    assert boundary["reasons"] == ["task_time_exhausted"]
    waiting = finished("unchanged", at=195)
    insufficient = policy.decide(waiting, now=195, busy=False)
    assert insufficient["state"] == "exhausted"
    assert insufficient["reasons"] == ["task_time_insufficient_for_wait"]
    assert insufficient["next_attempt_at"] is None


def test_explicit_limit_extension_recovers_without_resetting_caller_facts(policy):
    usage = finished("progress", attempts=4, at=215)
    saved_facts = asdict(usage)
    assert policy.decide(usage, now=220, busy=False)["state"] == "exhausted"
    extended = replace(policy, max_elapsed_seconds=180, max_attempts=5)
    restored = TaskUsage(**saved_facts)
    resumed = extended.decide(restored, now=220, busy=False)
    assert resumed["allowed"]
    assert resumed["remaining"] == {"seconds": 60, "attempts": 1, "retries": 1}
    assert asdict(restored) == saved_facts


@pytest.mark.parametrize("outcome", ["unknown", "running"])
def test_unknown_or_orphaned_attempt_requires_reconciliation_even_after_extension(policy, outcome):
    usage = TaskUsage(
        started_at=100,
        attempts=1,
        last_outcome=outcome,
        last_finished_at=105 if outcome == "unknown" else None,
    )
    for limits, now in [
        (policy, 110),
        (policy, 999),
        (replace(policy, max_elapsed_seconds=1000), 999),
    ]:
        result = limits.decide(usage, now=now, busy=False)
        assert result["state"] == "reconciliation_required"
        assert result["reasons"] == ["attempt_outcome_unknown"]
        assert not result["allowed"] and result["next_attempt_at"] is None
    if outcome == "running":
        assert policy.decide(usage, now=110, busy=True)["reasons"] == ["task_busy"]


def test_complete_result_stays_complete_even_when_limits_have_elapsed(policy):
    result = policy.decide(finished("complete", attempts=4), now=999, busy=False)
    assert result["state"] == "complete"
    assert not result["allowed"] and not result["recovery_required"]


def test_clock_regression_does_not_reset_elapsed_time_or_allow_early_retry(policy):
    usage = finished("failed", failures=1, at=115)
    result = policy.decide(usage, now=110, busy=False)
    assert result["reasons"] == ["clock_before_task_evidence"]
    assert not result["allowed"]
    assert result["next_attempt_at"] == 115
    assert policy.decide(usage, now=115, busy=False)["next_attempt_at"] == 120


def test_saved_clock_watermark_cannot_refund_exhausted_budget_after_restart(policy):
    usage = finished("progress", at=105)
    exhausted = policy.decide(usage, now=220, busy=False)
    assert exhausted["state"] == "exhausted"
    persisted = {**asdict(usage), "last_checked_at": exhausted["observed_at"]}
    for clock in [219, 100, 50]:
        usage = TaskUsage(**persisted)
        rollback = policy.decide(usage, now=clock, busy=False)
        assert not rollback["allowed"]
        assert rollback["reasons"] == ["clock_before_task_evidence"]
        assert rollback["remaining"]["seconds"] == 0
        assert rollback["observed_at"] == 220
        persisted["last_checked_at"] = rollback["observed_at"]
    assert policy.decide(TaskUsage(**persisted), now=220, busy=False)["state"] == "exhausted"
    extended = replace(policy, max_elapsed_seconds=180)
    assert not extended.decide(TaskUsage(**persisted), now=219, busy=False)["allowed"]
    assert extended.decide(TaskUsage(**persisted), now=220, busy=False)["allowed"]


def test_policy_has_no_money_or_token_side_effects(tmp_path, policy):
    ledger = ResourceLedger(tmp_path / "resources.sqlite3")
    ledger.record_money("synthetic-invoice", kind="cost", amount_microusd=500, source="fixture")
    ledger.record_token_usage(
        {
            "threadId": "synthetic-thread",
            "turnId": "synthetic-turn",
            "tokenUsage": {
                "total": {
                    "inputTokens": 90,
                    "cachedInputTokens": 5,
                    "outputTokens": 10,
                    "reasoningOutputTokens": 3,
                    "totalTokens": 100,
                }
            },
        }
    )
    before = ledger.status(now=100)
    assert policy.decide(TaskUsage(started_at=100), now=220, busy=False)["state"] == "exhausted"
    after = ResourceLedger(ledger.path).status(now=220)
    assert after == before
    assert after["tokens"]["cost_microusd"] is None
    assert after["money_receipts"]["cost"] == {"amount_microusd": 500, "receipts": 1}
    assert after["virtual_budget_enabled"] is False


@pytest.mark.parametrize(
    "field", ["max_elapsed_seconds", "retry_wait_seconds", "unchanged_wait_seconds"]
)
@pytest.mark.parametrize("value", [True, False, None, 0, -1, float("nan"), float("inf")])
def test_invalid_durations_rejected(policy, field, value):
    with pytest.raises(ValueError):
        replace(policy, **{field: value})


@pytest.mark.parametrize("field", ["max_attempts", "max_retries"])
@pytest.mark.parametrize("value", [True, -1, 1.5, float("nan"), float("inf")])
def test_invalid_limits_rejected(policy, field, value):
    with pytest.raises(ValueError):
        replace(policy, **{field: value})


@pytest.mark.parametrize("field", ["attempts", "consecutive_failures"])
@pytest.mark.parametrize("value", [True, -1, 1.5, float("nan"), float("inf")])
def test_invalid_usage_counts_rejected(field, value):
    with pytest.raises(ValueError):
        TaskUsage(started_at=100, **{field: value})


@pytest.mark.parametrize("value", [True, None, float("nan"), float("inf")])
def test_invalid_timestamps_rejected(policy, value):
    with pytest.raises(ValueError):
        TaskUsage(started_at=value)
    with pytest.raises(ValueError):
        finished("progress", at=value)
    with pytest.raises(ValueError):
        policy.decide(TaskUsage(started_at=100), now=value, busy=False)
    if value is not None:  # None is explicitly the initial absent check watermark.
        with pytest.raises(ValueError):
            TaskUsage(started_at=100, last_checked_at=value)


@pytest.mark.parametrize(
    "changes",
    [
        {"attempts": 1},
        {"last_outcome": "complete"},
        {"attempts": 1, "last_outcome": "running", "last_finished_at": 101},
        {"attempts": 1, "last_outcome": "failed", "last_finished_at": 101},
        {
            "attempts": 1,
            "last_outcome": "progress",
            "last_finished_at": 101,
            "consecutive_failures": 1,
        },
        {"attempts": 1, "last_outcome": "progress", "last_finished_at": 99},
        {"consecutive_failures": 1},
        {"last_outcome": []},
        {"last_checked_at": 99},
    ],
)
def test_inconsistent_usage_is_rejected(changes):
    with pytest.raises(ValueError):
        TaskUsage(started_at=100, **changes)
