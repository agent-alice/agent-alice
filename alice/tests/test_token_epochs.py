"""Synthetic replay of process-scoped counters; no native process or model calls."""

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy

import pytest

from alice_codex.resources import ResourceError, ResourceLedger, TOKEN_FIELDS


def notification(total, *, cached=0, output=0, last=None, turn="synthetic-turn"):
    counters = dict(
        totalTokens=total,
        inputTokens=total - output,
        cachedInputTokens=cached,
        outputTokens=output,
        reasoningOutputTokens=0,
        cacheWriteInputTokens=0,
    )
    return {
        "threadId": "synthetic-thread",
        "turnId": turn,
        "tokenUsage": {
            "total": counters,
            "last": counters.copy() if last is None else {"totalTokens": last},
        },
    }


def f01_events():
    # Counter numbers from an existing synthetic acceptance run; source IDs replaced.
    return [
        ("server-a", notification(11992, output=60, turn="turn-a")),
        ("server-a", notification(25125, cached=11776, output=154, last=13133, turn="turn-a")),
        ("server-b", notification(13291, cached=12800, output=61, turn="turn-b")),
        ("server-b", notification(27719, cached=25856, output=153, last=14428, turn="turn-b")),
        ("server-b", notification(27719, cached=25856, output=153, last=6844, turn="compact")),
        ("server-b", notification(40788, cached=34688, output=212, last=13069, turn="turn-c")),
        ("server-b", notification(54990, cached=47488, output=302, last=14202, turn="turn-c")),
    ]


def test_f01_preserves_both_epoch_observations_without_inventing_billing(tmp_path):
    ledger = ResourceLedger(tmp_path / "resources.sqlite3")
    changes = []
    for epoch, event in f01_events():
        result = ledger.record_token_usage(event, epoch_id=epoch)
        changes.append(None if result["increase"] is None else result["increase"]["totalTokens"])
    assert changes == [None, 13133, None, 14428, 0, 13069, 14202]
    tokens = ResourceLedger(ledger.path).status()["tokens"]
    assert tokens["epochs"]["server-a"]["synthetic-thread"]["high_water"]["totalTokens"] == 25125
    assert tokens["epochs"]["server-b"]["synthetic-thread"]["high_water"]["totalTokens"] == 54990
    assert tokens["sum_epoch_high_water_marks"]["totalTokens"] == 80115
    assert tokens["sum_observed_increases_after_first"]["totalTokens"] == 54832
    assert tokens["epoch_boundary_state"] == "unverified"
    assert tokens["actual_usage_total"] is None and tokens["cost_microusd"] is None
    assert tokens["unknown_or_out_of_order_events"] == 0
    assert ledger.status()["money_receipts"] == {}
    assert not ledger.status()["virtual_budget_enabled"]


@pytest.mark.parametrize("start,end", [(50, 90), (230, 260)])
def test_epoch_start_is_a_baseline_even_when_above_or_below_previous_total(tmp_path, start, end):
    ledger = ResourceLedger(tmp_path / "resources.sqlite3")
    ledger.record_token_usage(notification(100), epoch_id="first")
    ledger.record_token_usage(notification(200), epoch_id="first")
    baseline = ledger.record_token_usage(notification(start), epoch_id="second")
    assert baseline["increase"] is None
    next_observation = ledger.record_token_usage(notification(end), epoch_id="second")
    assert next_observation["increase"]["totalTokens"] == end - start
    assert ledger.status()["tokens"]["actual_usage_total"] is None


def test_payload_dedup_is_per_epoch_but_stable_receipts_cannot_be_reassigned(tmp_path):
    ledger = ResourceLedger(tmp_path / "resources.sqlite3")
    event = notification(100)
    assert ledger.record_token_usage(event, epoch_id="first", event_id="event-a")["recorded"]
    assert not ledger.record_token_usage(event, epoch_id="first", event_id="event-a")["recorded"]
    assert ledger.record_token_usage(event, epoch_id="second", event_id="event-b")["recorded"]
    with pytest.raises(ResourceError, match="conflict"):
        ledger.record_token_usage(event, epoch_id="third", event_id="event-a")
    assert "third" not in ledger.status()["tokens"]["epochs"]


def test_duplicate_alias_is_durably_bound_before_returning(tmp_path):
    ledger = ResourceLedger(tmp_path / "resources.sqlite3")
    event = notification(100)
    ledger.record_token_usage(event, epoch_id="server", event_id="receipt-a")
    assert not ledger.record_token_usage(event, epoch_id="server", event_id="receipt-b")["recorded"]
    with pytest.raises(ResourceError, match="conflict"):
        ResourceLedger(ledger.path).record_token_usage(
            notification(120), epoch_id="server", event_id="receipt-b"
        )
    with pytest.raises(ResourceError, match="conflict"):
        ledger.record_token_usage(event, epoch_id="other-server", event_id="receipt-b")


def test_decline_is_ambiguous_and_does_not_guess_a_new_epoch(tmp_path):
    ledger = ResourceLedger(tmp_path / "resources.sqlite3")
    ledger.record_token_usage(notification(200), epoch_id="server")
    lower = ledger.record_token_usage(notification(100), epoch_id="server")
    assert lower["state"] == "out_of_order_or_counter_reset"
    assert lower["increase"] is None
    assert (
        ledger.record_token_usage(notification(210), epoch_id="server")["increase"]["totalTokens"]
        == 10
    )
    tokens = ledger.status()["tokens"]
    assert list(tokens["epochs"]) == ["server"]
    assert tokens["state"] == "unknown" and tokens["actual_usage_total"] is None


def test_last_or_context_window_changes_are_not_added_to_usage(tmp_path):
    ledger = ResourceLedger(tmp_path / "resources.sqlite3")
    first = notification(100, last=100)
    ledger.record_token_usage(first, epoch_id="server")
    for value in (6844, 900000):
        observed = deepcopy(first)
        observed["tokenUsage"].update(last={"totalTokens": value}, modelContextWindow=value * 2)
        result = ledger.record_token_usage(observed, epoch_id="server")
        assert result["increase"] == dict.fromkeys(TOKEN_FIELDS, 0)
    assert ledger.status()["tokens"]["sum_epoch_high_water_marks"]["totalTokens"] == 100


def test_unknown_and_decreasing_subcounter_preserve_uncertainty(tmp_path):
    ledger = ResourceLedger(tmp_path / "resources.sqlite3")
    malformed = notification(100)
    malformed["tokenUsage"]["total"].pop("inputTokens")
    assert ledger.record_token_usage(malformed, epoch_id="server")["state"] == "unknown"
    ledger.record_token_usage(notification(200, output=20), epoch_id="server")
    assert (
        ledger.record_token_usage(notification(210, output=10), epoch_id="server")["state"]
        == "out_of_order_or_counter_reset"
    )
    tokens = ledger.status()["tokens"]
    assert tokens["epochs"]["server"]["synthetic-thread"]["high_water"]["totalTokens"] == 200
    assert tokens["unknown_or_out_of_order_events"] == 2


def test_concurrent_duplicate_notifications_observe_once(tmp_path):
    path = tmp_path / "resources.sqlite3"
    ResourceLedger(path)

    def record(index):
        return ResourceLedger(path).record_token_usage(
            notification(100), epoch_id="server", event_id=f"envelope-{index}"
        )["recorded"]

    with ThreadPoolExecutor(max_workers=4) as workers:
        assert sum(workers.map(record, range(12))) == 1
    with pytest.raises(ResourceError, match="conflict"):
        ResourceLedger(path).record_token_usage(
            notification(101), epoch_id="server", event_id="envelope-11"
        )


def test_missing_epoch_preserves_legacy_view_without_mixing_scopes(tmp_path):
    ledger = ResourceLedger(tmp_path / "resources.sqlite3")
    ledger.record_token_usage(notification(200))
    ledger.record_token_usage(notification(50), epoch_id="server")
    tokens = ledger.status()["tokens"]
    assert tokens["threads"]["synthetic-thread"]["totalTokens"] == 200
    assert tokens["sum_epoch_high_water_marks"]["totalTokens"] == 50
    assert tokens["legacy_unscoped"] is True
    assert tokens["actual_usage_total"] is None


def test_legacy_replay_does_not_silently_guess_epochs_from_counter_declines(tmp_path):
    ledger = ResourceLedger(tmp_path / "resources.sqlite3")
    for _, event in f01_events():
        ledger.record_token_usage(event)
    tokens = ledger.status()["tokens"]
    assert tokens["unknown_or_out_of_order_events"] == 3
    assert tokens["threads"]["synthetic-thread"]["totalTokens"] == 54990
    assert tokens["epochs"] == {}
    assert tokens["sum_epoch_high_water_marks"] is None
    assert tokens["sum_observed_increases_after_first"] is None
    assert tokens["actual_usage_total"] is None and tokens["legacy_unscoped"]
