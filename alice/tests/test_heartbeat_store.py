"""Synthetic host receipt persistence; receipt JSON is not caller authentication."""

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import hashlib
import json
import threading

import pytest

from alice_codex.heartbeat import HostHeartbeatAdapter
from alice_codex.resources import TaskPolicy
from alice_codex.store import Store, StoreError


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def receipt(identifier, *, at=100, content="same", state="known", target="monitor", **changes):
    value = {
        "version": 1,
        "id": identifier,
        "target": target,
        "source_id": "host-source",
        "scope_sha256": digest("scope"),
        "validator_version": "collector-summary-v1",
        "observed_at": at,
        "state": state,
        "content_sha256": digest(content) if state == "known" else None,
        "evidence_sha256": digest(identifier),
        "reason": None if state == "known" else "collection_unavailable",
    }
    return {**value, **changes}


@pytest.fixture
def store(tmp_path):
    with Store(tmp_path / "heartbeat.sqlite3") as value:
        yield value


def record(store, value, *, wait=30):
    return store.record_heartbeat("monitor", value, wait_seconds=wait)


def raw_record(store, target="monitor"):
    key = store._policy_key("heartbeat", target)
    return json.loads(
        store._connection().execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()[0]
    )


def raw_receipts(store):
    return {
        entry["receipt"]["id"]: entry
        for row in store._connection().execute(
            "SELECT value FROM settings WHERE key GLOB 'heartbeat_receipt/*'"
        )
        for entry in [json.loads(row[0])]
    }


def test_known_baseline_then_same_scope_unchanged_sets_one_persisted_wait(store):
    assert store.get_heartbeat_state("monitor") is None
    first = receipt("first")
    baseline = record(store, first)
    assert baseline == {
        "version": 1,
        "target": "monitor",
        "latest": first,
        "last_good": first,
        "comparison": {
            "state": "baseline",
            "accept": True,
            "update_last_good": True,
            "wake": False,
            "reason": "new_scope",
        },
        "waiting_until": None,
    }
    second = receipt("second", at=110)
    waiting = record(store, second)
    assert waiting["comparison"]["state"] == "unchanged"
    assert waiting["waiting_until"] == 140
    assert waiting["latest"] == waiting["last_good"] == second
    with Store(store.path) as reopened:
        assert reopened.get_heartbeat_state("monitor") == waiting
        assert record(reopened, second, wait=900) == waiting
        assert record(reopened, first, wait=900) == waiting
        assert reopened.get_heartbeat_state("monitor") == waiting
        assert len(raw_receipts(reopened)) == 2


def test_new_evidence_clears_wait_without_resolving_policy_or_pause(store):
    store.set_task_policy("monitor", TaskPolicy(120, 4, 1, 5, 30), "policy", now=0)
    store.admit_task_attempt("monitor", "attempt", digest("input"), now=100, busy=False)
    store.finish_task_attempt("monitor", "attempt", "unknown", now=101)
    policy_before = store.get_task_policy("monitor")
    store.set_autonomy_paused(True)
    record(store, receipt("first"))
    record(store, receipt("same", at=110))
    changed = record(store, receipt("changed", at=120, content="new"))
    assert changed["comparison"]["state"] == "new_evidence"
    assert changed["comparison"]["wake"] is True
    assert changed["waiting_until"] is None
    assert store.get_task_policy("monitor") == policy_before
    assert store.is_autonomy_paused()


def test_unknown_advances_watermark_but_preserves_last_good_for_later_comparison(store):
    good = receipt("good")
    record(store, good)
    record(store, receipt("same", at=110))
    unknown = record(store, receipt("unknown", at=120, state="unknown"))
    assert unknown["latest"]["state"] == "unknown"
    assert unknown["last_good"]["id"] == "same"
    assert unknown["waiting_until"] is None
    with Store(store.path) as reopened:
        assert reopened.get_heartbeat_state("monitor") == unknown
        assert (
            record(reopened, receipt("stale-known", at=115, content="old changed data")) == unknown
        )
        recovered = record(reopened, receipt("recovered", at=130))
        assert recovered["comparison"]["state"] == "unchanged"
        assert recovered["waiting_until"] == 160
        assert (
            record(reopened, receipt("older-unknown", at=129, state="unknown"), wait=800)
            == recovered
        )


@pytest.mark.parametrize(
    "change",
    [
        {"scope_sha256": digest("new-scope")},
        {"source_id": "new-source"},
        {"validator_version": "new-validator"},
    ],
)
def test_scope_change_is_a_new_baseline_and_clears_unchanged_wait(store, change):
    record(store, receipt("first"))
    record(store, receipt("same", at=110))
    reset = record(store, receipt("new-scope", at=120, **change))
    assert reset["comparison"]["state"] == "baseline"
    assert not reset["comparison"]["wake"]
    assert reset["waiting_until"] is None


def test_unconfigured_receipt_is_explicit_and_never_a_known_snapshot(store):
    unconfigured = HostHeartbeatAdapter().observe("monitor", now=100)
    result = record(store, unconfigured, wait=5)
    assert result["comparison"]["state"] == "unconfigured"
    assert result["latest"]["source_id"] is None
    assert result["last_good"] is None and result["waiting_until"] is None
    record(store, receipt("known", at=110))
    later = HostHeartbeatAdapter().observe("monitor", now=120)
    result = record(store, later, wait=5)
    assert result["last_good"]["id"] == "known"
    assert result["waiting_until"] is None


@pytest.mark.parametrize("at", [50, 100])
def test_stale_and_equal_time_receipts_never_advance_wait_or_baseline(store, at):
    record(store, receipt("first", at=90))
    current = record(store, receipt("current", at=100))
    stale = receipt("stale", at=at, content="different")
    assert record(store, stale, wait=900) == current
    assert store.get_heartbeat_state("monitor") == current
    assert raw_receipts(store)["stale"]["receipt"] == stale


@pytest.mark.parametrize("older_id", ["first", "stale"])
def test_conflicting_old_receipt_ids_are_detected_after_restart(store, older_id):
    first, stale = receipt("first", at=100), receipt("stale", at=90)
    record(store, first)
    record(store, receipt("current", at=110))
    expected = record(store, stale)
    original = first if older_id == "first" else stale
    with Store(store.path) as reopened:
        with pytest.raises(ValueError, match="conflicting evidence"):
            record(reopened, {**original, "observed_at": 999})
        assert reopened.get_heartbeat_state("monitor") == expected
        assert record(reopened, original) == expected


def test_receipt_target_and_global_id_cannot_be_rebound(store):
    original = receipt("global-id")
    current = record(store, original)
    with pytest.raises(ValueError, match="target binding"):
        store.record_heartbeat("other", original, wait_seconds=30)
    with pytest.raises(ValueError, match="conflicting evidence"):
        store.record_heartbeat("other", {**original, "target": "other"}, wait_seconds=30)
    assert store.get_heartbeat_state("other") is None
    assert store.get_heartbeat_state("monitor") == current


def test_received_and_returned_mappings_do_not_alias_persisted_state(store):
    original = receipt("first")
    preserved = deepcopy(original)
    result = record(store, original)
    original["content_sha256"] = digest("changed caller object")
    result["latest"]["content_sha256"] = digest("changed returned object")
    assert store.get_heartbeat_state("monitor")["latest"] == preserved
    copy = store.get_heartbeat_state("monitor")
    copy["comparison"]["wake"] = True
    assert not store.get_heartbeat_state("monitor")["comparison"]["wake"]


def test_concurrent_duplicate_receipts_are_persisted_once(store):
    barrier = threading.Barrier(2)
    first = receipt("concurrent")

    def collect():
        with Store(store.path) as independent:
            barrier.wait(timeout=5)
            return record(independent, first)

    with ThreadPoolExecutor(max_workers=2) as workers:
        results = list(workers.map(lambda _: collect(), range(2)))
    assert results[0] == results[1]
    assert len(raw_receipts(store)) == 1


@pytest.mark.parametrize("wait", [None, True, 0, -1, "30", float("inf"), float("nan"), 10**400])
def test_wait_is_always_an_explicit_positive_finite_number(store, wait):
    with pytest.raises(ValueError):
        record(store, receipt("first"), wait=wait)
    assert store.get_heartbeat_state("monitor") is None


def test_unchanged_wait_overflow_does_not_replace_existing_receipts(store):
    before = record(store, receipt("first", at=1e308), wait=1e308)
    with pytest.raises(ValueError, match="deadline must be finite"):
        record(store, receipt("later", at=1.1e308), wait=1e308)
    assert store.get_heartbeat_state("monitor") == before
    assert list(raw_receipts(store)) == ["first"]


def test_failed_persistence_keeps_receipts_and_retry_can_accept_once(store):
    before = record(store, receipt("first"))
    store._connection().execute(
        "CREATE TRIGGER reject_heartbeat BEFORE UPDATE ON settings "
        "WHEN NEW.key GLOB 'heartbeat/*' BEGIN SELECT RAISE(ABORT,'injected full disk'); END"
    )
    later = receipt("later", at=110)
    with pytest.raises(StoreError, match="injected full disk"):
        record(store, later)
    assert store.get_heartbeat_state("monitor") == before
    store._connection().execute("DROP TRIGGER reject_heartbeat")
    after = record(store, later)
    assert after["waiting_until"] == 140
    assert len(raw_receipts(store)) == 2


@pytest.mark.parametrize(
    "damage",
    [
        "version",
        "key",
        "latest",
        "last_good",
        "wait",
        "receipt_version",
        "receipt_id",
        "sequence",
        "index_version",
        "previous_latest",
        "previous_good",
        "count",
        "tail",
    ],
)
def test_damaged_heartbeat_state_fails_closed_on_read_and_restart(store, damage):
    record(store, receipt("first"))
    record(store, receipt("same", at=110))
    key = store._policy_key("heartbeat", "monitor")
    raw = raw_record(store)
    if damage == "version":
        raw["version"] = 900
    elif damage == "latest":
        raw["latest"]["content_sha256"] = digest("tampered")
    elif damage == "last_good":
        raw["last_good"] = None
    elif damage == "wait":
        raw["waiting_until"] += 1
    elif damage == "count":
        raw["receipt_count"] += 1
    elif damage == "tail":
        raw["last_receipt_id"] = "first"
    elif damage != "key":
        key = store._policy_key("heartbeat_receipt", "same")
        raw = raw_receipts(store)["same"]
        if damage == "receipt_version":
            raw["receipt"]["version"] = 2
        elif damage == "receipt_id":
            raw["receipt"]["id"] = "another-id"
        elif damage == "sequence":
            raw["sequence"] = 5
        elif damage == "index_version":
            raw["version"] = 2
        elif damage == "previous_latest":
            raw["previous_latest_id"] = "missing"
        elif damage == "previous_good":
            raw["previous_good_id"] = None
    persisted_key = "heartbeat/wrong-target" if damage == "key" else key
    store._connection().execute(
        "UPDATE settings SET key=?,value=? WHERE key=?", (persisted_key, json.dumps(raw), key)
    )
    with pytest.raises(StoreError, match="persisted heartbeat"):
        store.get_heartbeat_state("monitor")
    with pytest.raises(StoreError, match="persisted heartbeat"):
        Store(store.path)
    assert (
        json.loads(
            store._connection()
            .execute("SELECT value FROM settings WHERE key=?", (persisted_key,))
            .fetchone()[0]
        )
        == raw
    )


def test_unknown_receipt_version_and_extra_claims_are_rejected_without_mutation(store):
    before = record(store, receipt("first"))
    for invalid in (receipt("future", at=110, version=2), receipt("extra", at=110, passed=True)):
        with pytest.raises(ValueError):
            record(store, invalid)
    assert store.get_heartbeat_state("monitor") == before


def test_schema_one_keeps_all_observations_and_unrelated_settings(store):
    store._connection().execute(
        "INSERT INTO settings VALUES ('other_owner/future', '{\"version\":99}')"
    )
    record(store, receipt("first"))
    record(store, receipt("unknown", at=110, state="unknown"))
    record(store, receipt("stale", at=50))
    record(store, receipt("same", at=120))
    with Store(store.path) as reopened:
        assert reopened._connection().execute("PRAGMA user_version").fetchone()[0] == 1
        assert set(raw_receipts(reopened)) == {"first", "unknown", "stale", "same"}
        assert (
            reopened._connection()
            .execute("SELECT value FROM settings WHERE key='other_owner/future'")
            .fetchone()[0]
            == '{"version":99}'
        )


def test_poll_queries_and_writes_do_not_grow_with_receipt_history(store):
    store._connection().execute(
        "CREATE TRIGGER immutable_heartbeat_receipts BEFORE UPDATE ON settings "
        "WHEN OLD.key GLOB 'heartbeat_receipt/*' "
        "BEGIN SELECT RAISE(ABORT,'receipt cannot be overwritten'); END"
    )
    store._connection().execute(
        "CREATE TRIGGER retained_heartbeat_receipts BEFORE DELETE ON settings "
        "WHEN OLD.key GLOB 'heartbeat_receipt/*' "
        "BEGIN SELECT RAISE(ABORT,'receipt cannot be removed'); END"
    )

    def measured_poll(number):
        statements = []
        store._connection().set_trace_callback(statements.append)
        before = store._connection().total_changes
        try:
            store.get_heartbeat_state("monitor")
            state = record(store, receipt(f"r-{number}", at=number))
        finally:
            store._connection().set_trace_callback(None)
        assert store._connection().total_changes - before == 2
        selects = [sql for sql in statements if sql.startswith("SELECT")]
        assert all("WHERE key=" in sql and "GLOB" not in sql for sql in selects)
        assert len(selects) <= 9
        return len(selects), len(json.dumps(raw_record(store))), state

    for number in range(3):
        record(store, receipt(f"r-{number}", at=number))
    initial_queries, initial_size, _ = measured_poll(3)
    # Grow this target and unrelated targets. No poll scans any of their history.
    for number in range(4, 300):
        record(store, receipt(f"r-{number}", at=number))
        store.record_heartbeat(
            f"other-{number % 5}",
            receipt(f"other-r-{number}", at=number, target=f"other-{number % 5}"),
            wait_seconds=30,
        )
    later_queries, later_size, state = measured_poll(300)
    assert later_queries == initial_queries
    assert later_size < initial_size + 50
    assert raw_record(store)["receipt_count"] == 301
    assert len(raw_receipts(store)) == 301 + 296
    before = store._connection().total_changes
    assert record(store, receipt("r-0", at=0), wait=800) == state
    assert store._connection().total_changes == before
    with Store(store.path) as reopened:
        assert reopened.get_heartbeat_state("monitor") == state
        assert len(raw_receipts(reopened)) == 597


def test_concurrent_distinct_observations_keep_both_and_latest_wins(store):
    barrier = threading.Barrier(2)
    values = [receipt("earlier", at=100), receipt("later", at=110)]

    def collect(value):
        with Store(store.path) as independent:
            barrier.wait(timeout=5)
            return record(independent, value)

    with ThreadPoolExecutor(max_workers=2) as workers:
        list(workers.map(collect, values))
    result = store.get_heartbeat_state("monitor")
    assert result["latest"]["id"] == "later"
    assert set(raw_receipts(store)) == {"earlier", "later"}
    with Store(store.path) as reopened:
        assert reopened.get_heartbeat_state("monitor") == result


def test_startup_audits_unreferenced_history_and_explicit_old_id_read_fails_closed(store):
    for number in range(6):
        record(store, receipt(f"r-{number}", at=number))
    expected = store.get_heartbeat_state("monitor")
    key = store._policy_key("heartbeat_receipt", "r-0")
    original = raw_receipts(store)["r-0"]
    damaged = {**original, "version": 2}
    store._connection().execute(
        "UPDATE settings SET value=? WHERE key=?", (json.dumps(damaged), key)
    )
    # Polling intentionally checks only its current references. Historical rows
    # are audited on startup and when their exact ID is requested again.
    assert store.get_heartbeat_state("monitor") == expected
    with pytest.raises(StoreError, match="persisted heartbeat"):
        record(store, original["receipt"])
    with pytest.raises(StoreError, match="persisted heartbeat"):
        Store(store.path)
    assert raw_receipts(store)["r-0"] == damaged


@pytest.mark.parametrize("damage", ["orphan", "gap", "predecessor", "duplicate_sequence"])
def test_startup_checks_every_historical_sequence_and_predecessor(store, damage):
    for number in range(6):
        record(store, receipt(f"r-{number}", at=number))
    key = store._policy_key("heartbeat_receipt", "r-1")
    raw = raw_receipts(store)["r-1"]
    if damage == "orphan":
        raw["target"] = raw["receipt"]["target"] = "orphan"
    elif damage == "predecessor":
        raw["previous_latest_id"] = "r-5"
    elif damage == "duplicate_sequence":
        raw["sequence"] = 3
    if damage == "gap":
        store._connection().execute("DELETE FROM settings WHERE key=?", (key,))
    else:
        store._connection().execute(
            "UPDATE settings SET value=? WHERE key=?", (json.dumps(raw), key)
        )
    with pytest.raises(StoreError, match="persisted heartbeat"):
        Store(store.path)


def test_failed_first_snapshot_write_rolls_back_the_appended_receipt(store):
    store._connection().execute(
        "CREATE TRIGGER reject_heartbeat BEFORE INSERT ON settings "
        "WHEN NEW.key GLOB 'heartbeat/*' BEGIN SELECT RAISE(ABORT,'injected full disk'); END"
    )
    with pytest.raises(StoreError, match="injected full disk"):
        record(store, receipt("first"))
    assert raw_receipts(store) == {}
    assert store.get_heartbeat_state("monitor") is None
    store._connection().execute("DROP TRIGGER reject_heartbeat")
    assert record(store, receipt("first"))["latest"]["id"] == "first"
    assert raw_record(store)["receipt_count"] == 1
