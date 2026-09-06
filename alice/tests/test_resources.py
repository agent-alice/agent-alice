from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime
import sqlite3

import pytest

from alice_codex.resources import ResourceError, ResourceLedger


NOW = datetime.fromisoformat("2026-06-30T12:00:00+08:00").timestamp()


@pytest.fixture
def ledger(tmp_path):
    return ResourceLedger(tmp_path / "resources.sqlite3")


def rule(**changes):
    # Explicit synthetic confirmation, never loaded from the user's real data.
    return {
        "effective_date": "2026-06-01",
        "timezone": "Asia/Shanghai",
        "subject": "fixture",
        "collection": "answers",
        "metric": "upvotes",
        "baseline_period": "2026-05-31",
        "baseline_count": 3,
        "daily_base_microusd": 7_000_000,
        "likes_per_usd": [
            {"start_day": 0, "likes": 2},
            {"start_day": 5, "likes": 4},
            {"start_day": 11, "likes": 8},
        ],
        "rounding": "floor_micro_usd",
        "negative_delta": "hold",
        "require_independent": True,
        **changes,
    }


def enable(ledger, **changes):
    ledger.configure_virtual_budget(
        rule(**changes),
        opening_balance_microusd=20_000_000,
        confirmation_id="synthetic-confirmed-baseline",
    )
    ledger.set_virtual_budget_enabled(True, confirmation_id="synthetic-enable")


def observation(count=4, period="2026-06-01", **changes):
    rows = [{"id": "answer-1", "upvotes": count}]
    stamp = f"{period}T23:59:00+08:00"
    return {
        "schema_version": 1,
        "subject": "fixture",
        "collection": "answers",
        "required_metrics": ["upvotes"],
        "expected_count": 1,
        "pages": [
            {
                "source": "fixture-api",
                "observed_at": stamp,
                "cursor": None,
                "next_cursor": None,
                "status": "ok",
                "items": rows,
            }
        ],
        "independent": {
            "source": "fixture-browser",
            "observed_at": stamp,
            "subject": "fixture",
            "collection": "answers",
            "coverage": "complete",
            "items": deepcopy(rows),
        },
        **changes,
    }


def usage(total=100, *, thread="main", turn="turn-1"):
    return {
        "threadId": thread,
        "turnId": turn,
        "tokenUsage": {
            "total": {
                "inputTokens": total - 10,
                "cachedInputTokens": 5,
                "outputTokens": 10,
                "reasoningOutputTokens": 3,
                "totalTokens": total,
            },
            "last": {"totalTokens": total},
        },
    }


def quota(percent, **extra):
    return {
        "rateLimits": {"primary": {"usedPercent": percent, "resetsAt": 10}, "secondary": None},
        **extra,
    }


def test_confirmed_default_records_without_virtual_enforcement(ledger):
    state = ledger.status()
    assert state["virtual_budget_enabled"] is False
    assert state["virtual_budget"] == {
        "state": "unconfigured",
        "currency": "USD",
        "balance_microusd": None,
        "configuration": None,
    }
    assert state["historical_rule_reference"] is None
    assert ledger.can_dispatch(automatic=True)["allowed"]
    assert ledger.can_dispatch(automatic=False)["allowed"]
    assert state["tokens"]["state"] == "unknown" and state["money_receipts"] == {}
    recorded = ledger.record_observation("observed", observation(), period="2026-06-01", now=NOW)
    assert recorded["recorded"] and not recorded["settled"]
    assert ledger.status()["latest_observation"]["summary"]["metrics"]["upvotes"]["value"] == 4
    assert ledger.status()["virtual_budget"]["balance_microusd"] is None
    with pytest.raises(ResourceError, match="baseline"):
        ledger.set_virtual_budget_enabled(True, confirmation_id="insufficient")


def test_explicit_configuration_requires_every_policy_field(ledger):
    incomplete = rule()
    del incomplete["rounding"]
    with pytest.raises(ValueError, match="every"):
        ledger.configure_virtual_budget(incomplete, opening_balance_microusd=1, confirmation_id="x")
    ledger.configure_virtual_budget(rule(), opening_balance_microusd=1, confirmation_id="x")
    assert not ledger.status()["virtual_budget_enabled"]
    with pytest.raises(ResourceError, match="already configured"):
        ledger.configure_virtual_budget(rule(), opening_balance_microusd=999, confirmation_id="x")


def test_native_tokens_use_cumulative_counters_not_total_plus_last(ledger):
    first = ledger.record_token_usage(usage())
    assert first == {"recorded": True, "state": "known", "increase": None}
    assert not ledger.record_token_usage(usage())["recorded"]
    assert not ledger.record_token_usage(usage(), event_id="duplicate-under-new-envelope")[
        "recorded"
    ]
    second = ledger.record_token_usage(usage(120, turn="turn-2"))
    assert second["increase"]["totalTokens"] == 20
    counters = ledger.status()["tokens"]["threads"]["main"]
    assert counters["totalTokens"] == 120 and counters["cacheWriteInputTokens"] is None
    assert ledger.status()["tokens"]["cost_microusd"] is None
    assert ledger.can_dispatch(automatic=True)["allowed"]


def test_token_event_id_collision_cannot_alias_another_existing_payload(ledger):
    ledger.record_token_usage(usage(), event_id="first")
    ledger.record_token_usage(usage(120), event_id="second")
    with pytest.raises(ResourceError, match="conflicting"):
        ledger.record_token_usage(usage(120), event_id="first")


@pytest.mark.parametrize("missing", [None, {}, {"totalTokens": 0}])
def test_missing_token_fields_remain_unknown_without_advancing_counters(ledger, missing):
    event = usage()
    event["tokenUsage"]["total"] = missing
    assert ledger.record_token_usage(event)["state"] == "unknown"
    assert ledger.status()["tokens"]["threads"] == {}
    assert ledger.status()["tokens"]["state"] == "unknown"


def test_out_of_order_or_reset_does_not_refund_native_usage(ledger):
    ledger.record_token_usage(usage(200))
    assert ledger.record_token_usage(usage(100))["state"] == "out_of_order_or_counter_reset"
    assert ledger.status()["tokens"]["threads"]["main"]["totalTokens"] == 200
    assert ledger.record_token_usage(usage(210))["increase"]["totalTokens"] == 10
    reopened = ResourceLedger(ledger.path)
    assert not reopened.record_token_usage(usage(210))["recorded"]
    assert reopened.status()["tokens"]["threads"]["main"]["totalTokens"] == 210


def test_native_quota_blocks_only_automatic_and_never_recovers_from_clock(ledger):
    ledger.record_rate_limits(quota(100), observed_at=1)
    assert not ledger.can_dispatch(automatic=True, now=100_000)["allowed"]
    assert ledger.can_dispatch(automatic=False, now=100_000)["allowed"]
    ledger.record_rate_limits(None, observed_at=100_001)
    assert ledger.status()["account_limits"]["state"] == "unknown"
    assert not ledger.can_dispatch(automatic=True)["allowed"]
    assert not ledger.record_rate_limits(quota(0), observed_at=2)["recorded"]
    ledger.record_rate_limits(quota(0), observed_at=100_002)
    assert ledger.status()["account_limits"]["state"] == "available"
    assert ledger.can_dispatch(automatic=True)["allowed"]


def test_separate_model_buckets_and_unknown_are_not_global_zero(ledger):
    ledger.record_rate_limits(
        {
            "rateLimits": {"primary": {"usedPercent": 100}},
            "rateLimitsByLimitId": {
                "spark": {"primary": {"usedPercent": 100}},
                "codex": {"primary": {"usedPercent": 20}},
            },
        },
        observed_at=1,
    )
    assert ledger.can_dispatch(automatic=True, limit_id="codex")["allowed"]
    assert not ledger.can_dispatch(automatic=True, limit_id="spark")["allowed"]
    assert ledger.status(limit_id="other")["account_limits"]["state"] == "unknown"


@pytest.mark.parametrize("response", [None, {}, quota(None), quota(False)])
def test_unknown_native_limits_do_not_invent_zero_or_block_by_default(ledger, response):
    ledger.record_rate_limits(response, observed_at=1)
    assert ledger.status()["account_limits"]["state"] == "unknown"
    assert ledger.can_dispatch(automatic=True)["allowed"]


def test_native_spend_control_remains_separate_from_virtual_income(ledger):
    response = quota(20)
    response["rateLimits"]["spendControlReached"] = True
    ledger.record_rate_limits(response, observed_at=1)
    ledger.record_money(
        "income", kind="income", amount_microusd=10_000_000, source="fixture-receipt"
    )
    assert not ledger.can_dispatch(automatic=True)["allowed"]
    assert ledger.can_dispatch(automatic=False)["allowed"]


def test_partial_snapshot_cannot_clear_different_exhausted_dimension(ledger):
    response = quota(20)
    response["rateLimits"].update(secondary={"usedPercent": 100}, spendControlReached=True)
    ledger.record_rate_limits(response, observed_at=1)
    ledger.record_rate_limits(quota(0), observed_at=2)
    assert not ledger.can_dispatch(automatic=True)["allowed"]
    recovered = ResourceLedger(ledger.path)
    assert not recovered.can_dispatch(automatic=True)["allowed"]
    response = quota(0)
    response["rateLimits"].update(secondary={"usedPercent": 0}, spendControlReached=False)
    recovered.record_rate_limits(response, observed_at=3)
    assert recovered.can_dispatch(automatic=True)["allowed"]


def test_duplicate_income_cost_and_restart_preserve_negative_virtual_balance(ledger):
    enable(ledger)
    ledger.record_money(
        "income", kind="income", amount_microusd=5_000_000, source="fixture-receipt"
    )
    ledger.record_money("cost", kind="cost", amount_microusd=30_000_000, source="fixture-invoice")
    assert not ledger.record_money(
        "cost", kind="cost", amount_microusd=30_000_000, source="fixture-invoice"
    )["recorded"]
    with pytest.raises(ResourceError, match="conflicting"):
        ledger.record_money("cost", kind="cost", amount_microusd=1, source="fixture-invoice")
    recovered = ResourceLedger(ledger.path)
    assert recovered.status()["virtual_budget"]["balance_microusd"] == -5_000_000
    assert not recovered.can_dispatch(automatic=True)["allowed"]
    assert recovered.can_dispatch(automatic=False)["allowed"]
    recovered.set_virtual_budget_enabled(False, confirmation_id="explicit-disable")
    assert recovered.can_dispatch(automatic=True)["allowed"]


@pytest.mark.parametrize(
    ("period", "baseline", "expected"),
    [
        ("2026-06-01", "2026-05-31", 7_500_000),
        ("2026-06-06", "2026-06-05", 7_250_000),
        ("2026-06-12", "2026-06-11", 7_125_000),
    ],
)
def test_explicit_rule_stages_settle_once_per_closed_calendar_day(
    ledger, period, baseline, expected
):
    enable(ledger, baseline_period=baseline)
    result = ledger.record_observation(
        "receipt", observation(period=period), period=period, now=NOW
    )
    assert result["credit_microusd"] == expected
    assert not ledger.record_observation(
        "receipt", observation(period=period), period=period, now=NOW
    )["recorded"]
    assert (
        ledger.record_observation(
            "same-day-new-envelope", observation(period=period), period=period, now=NOW
        )["reason"]
        == "already_settled"
    )
    assert ledger.status()["virtual_budget"]["balance_microusd"] == 20_000_000 + expected


@pytest.mark.parametrize(
    "change",
    [
        "unknown",
        "pagination",
        "wrong_scope",
        "independent_missing",
        "independent_count_missing",
        "independent_count_null",
        "wrong_day",
    ],
)
def test_incomplete_or_mismatched_observation_never_earns_even_base_credit(ledger, change):
    enable(ledger)
    document = observation()
    if change == "unknown":
        document["pages"][0]["items"][0]["upvotes"] = None
    elif change == "pagination":
        document["pages"][0]["next_cursor"] = "unread-page"
    elif change == "wrong_scope":
        document["subject"] = "someone-else"
    elif change == "independent_missing":
        document.pop("independent")
    elif change == "independent_count_missing":
        document["independent"]["items"][0].pop("upvotes")
    elif change == "independent_count_null":
        document["independent"]["items"][0]["upvotes"] = None
    else:
        document["pages"][0]["observed_at"] = "2026-05-31T23:59:00+08:00"
    assert not ledger.record_observation("bad", document, period="2026-06-01", now=NOW)["settled"]
    assert ledger.status()["virtual_budget"]["balance_microusd"] == 20_000_000


def test_negative_delta_missing_day_and_future_day_hold_for_reconciliation(ledger):
    enable(ledger)
    assert ledger.record_observation("decrease", observation(2), period="2026-06-01", now=NOW)[
        "reason"
    ].startswith("negative_")
    assert ledger.record_observation(
        "gap", observation(8, "2026-06-02"), period="2026-06-02", now=NOW
    )["reason"].startswith("settlement_gap")
    current = datetime.fromisoformat("2026-06-01T23:59:30+08:00").timestamp()
    assert (
        ledger.record_observation("open-day", observation(), period="2026-06-01", now=current)[
            "reason"
        ]
        == "period_not_closed"
    )
    assert ledger.status()["virtual_budget"]["balance_microusd"] == 20_000_000


def test_parallel_duplicate_external_receipts_book_once(ledger):
    def record(_):
        return ledger.record_money(
            "same-receipt", kind="income", amount_microusd=5, source="fixture"
        )

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(record, range(8)))
    assert sum(item["recorded"] for item in results) == 1
    assert ledger.status()["money_receipts"]["income"] == {"amount_microusd": 5, "receipts": 1}


def test_corrupt_or_future_database_is_preserved(tmp_path):
    path = tmp_path / "bad.sqlite3"
    path.write_bytes(b"not-a-database")
    with pytest.raises(ResourceError):
        ResourceLedger(path)
    assert path.read_bytes() == b"not-a-database"
    other = tmp_path / "future.sqlite3"
    ResourceLedger(other)
    with sqlite3.connect(other) as db:
        db.execute("PRAGMA user_version=2")
    before = other.read_bytes()
    with pytest.raises(ResourceError, match="unsupported"):
        ResourceLedger(other)
    assert other.read_bytes() == before


def test_database_write_failure_does_not_publish_partial_checkpoint(ledger, monkeypatch):
    ledger.record_token_usage(usage())
    connect = sqlite3.connect

    def reject_writes(*args, **kwargs):
        db = connect(*args, **kwargs)
        db.set_authorizer(
            lambda action, *_: (
                sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_INSERT else sqlite3.SQLITE_OK
            )
        )
        return db

    with monkeypatch.context() as patch:
        patch.setattr(sqlite3, "connect", reject_writes)
        with pytest.raises(ResourceError, match="persistence failed"):
            ledger.record_token_usage(usage(200))
    assert ledger.status()["tokens"]["threads"]["main"]["totalTokens"] == 100
    assert ledger.record_token_usage(usage(200))["recorded"]


def test_reference_import_is_idempotent_and_never_configures_accounting(ledger):
    reference = {
        "note": "synthetic archived example",
        "virtual_budget_enabled": True,
        "opening_balance_microusd": 987_000_000,
        "rule": {"example_rate": 23},
    }
    before = ledger.status(now=NOW)
    receipt = ledger.import_rule_reference(
        reference, receipt_id="fixture-reference", source="synthetic-reference.json"
    )
    assert receipt["imported"] is True
    recovered = ResourceLedger(ledger.path)
    assert recovered.import_rule_reference(
        reference, receipt_id="fixture-reference", source="synthetic-reference.json"
    ) == {**receipt, "imported": False}
    after = recovered.status(now=NOW)
    stored = after.pop("historical_rule_reference")
    assert stored == {
        "status": "reference_only",
        "receipt_id": "fixture-reference",
        "source": "synthetic-reference.json",
        "reference_sha256": receipt["reference_sha256"],
        "reference": reference,
    }
    before.pop("historical_rule_reference")
    assert after == before
    with sqlite3.connect(ledger.path) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 1
        assert db.execute("SELECT COUNT(*) FROM money").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM settlements").fetchone()[0] == 0
    with pytest.raises(ResourceError, match="baseline"):
        recovered.set_virtual_budget_enabled(True, confirmation_id="reference-is-not-configuration")


def test_conflicting_reference_import_preserves_prior_evidence(ledger):
    reference = {"note": "synthetic reference"}
    ledger.import_rule_reference(reference, receipt_id="fixture-reference", source="fixture-source")
    before = ledger.status(now=NOW)
    for args in (
        {
            "reference": {"note": "changed"},
            "receipt_id": "fixture-reference",
            "source": "fixture-source",
        },
        {"reference": reference, "receipt_id": "different", "source": "fixture-source"},
        {"reference": reference, "receipt_id": "fixture-reference", "source": "different-source"},
    ):
        with pytest.raises(ResourceError, match="already imported"):
            ledger.import_rule_reference(**args)
        assert ledger.status(now=NOW) == before


@pytest.mark.parametrize(
    "reference", [None, [], {}, {"text": "x" * 8193}, {"number": float("nan")}]
)
def test_invalid_reference_cannot_change_ledger(ledger, reference):
    before = ledger.status(now=NOW)
    with pytest.raises(ValueError):
        ledger.import_rule_reference(
            reference, receipt_id="fixture-reference", source="fixture-source"
        )
    assert ledger.status(now=NOW) == before


def test_parallel_reference_import_records_once(ledger):
    def import_once(_):
        return ledger.import_rule_reference(
            {"note": "synthetic reference"}, receipt_id="same-reference", source="fixture-source"
        )

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(import_once, range(12)))
    assert sum(result["imported"] for result in results) == 1
    assert ledger.status(now=NOW)["virtual_budget_enabled"] is False
