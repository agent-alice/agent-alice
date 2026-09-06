"""Durable task admission and reconciliation using synthetic host evidence only."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, replace
import hashlib
import json
import threading

import pytest

from alice_codex.resources import TaskPolicy
from alice_codex.store import Store, StoreError


def digest(text="input"):
    return hashlib.sha256(text.encode()).hexdigest()


@pytest.fixture
def policy():
    return TaskPolicy(120, 4, 2, 5, 30)


@pytest.fixture
def store(tmp_path, policy):
    with Store(tmp_path / "policy.sqlite3") as value:
        value.set_task_policy("main", policy, "policy-1", now=0)
        yield value


def admit(store, intent_id="attempt-1", *, now=100, busy=False, target="main", text="input"):
    return store.admit_task_attempt(target, intent_id, digest(text), now=now, busy=busy)


def finish(store, outcome, intent_id="attempt-1", *, now=105, evidence=None):
    return store.finish_task_attempt("main", intent_id, outcome, now=now, evidence=evidence)


def test_policy_requests_replay_original_receipt_without_resetting_later_usage(store, policy):
    original = store.set_task_policy("main", asdict(policy), "policy-1", now=999)
    assert original["recorded_at"] == 0
    admit(store)
    finish(store, "unknown")
    before = store.get_task_policy("main")
    extended = replace(policy, max_elapsed_seconds=600, max_attempts=20, max_retries=10)
    receipt = store.set_task_policy("main", extended, "policy-2", now=110)
    assert receipt["policy"] == asdict(extended)
    assert store.set_task_policy("main", policy, "policy-1", now=120) == original
    after = store.get_task_policy("main")
    assert after["policy"] == asdict(extended)
    assert after["usage"] == before["usage"]
    assert after["attempts"] == before["attempts"]
    assert after["current_attempt_id"] == before["current_attempt_id"]
    with Store(store.path) as reopened:
        assert reopened.get_task_policy("main") == after
        assert (
            reopened.task_policy_status("main", now=120, busy=False)["decision"]["state"]
            == "reconciliation_required"
        )


def test_policy_and_attempt_request_ids_are_global_and_bound_to_content(store, policy):
    store.set_task_policy("other", policy, "policy-other", now=0)
    for target, changed in [("other", policy), ("main", replace(policy, max_attempts=8))]:
        with pytest.raises(ValueError, match="Request ID"):
            store.set_task_policy(target, changed, "policy-1", now=1)
    admit(store)
    with pytest.raises(ValueError, match="Request ID"):
        admit(store, target="other")
    with pytest.raises(ValueError, match="Request ID"):
        admit(store, text="different input")
    with pytest.raises(ValueError, match="Request ID"):
        admit(store, "policy-1")
    with pytest.raises(ValueError, match="Request ID"):
        store.set_task_policy("main", policy, "attempt-1", now=101)
    assert store.get_task_policy("main")["usage"]["attempts"] == 1


@pytest.mark.parametrize("change", ["missing", "extra", "invalid"])
def test_policy_requires_exactly_five_validated_fields(store, policy, change):
    invalid = asdict(policy)
    if change == "missing":
        invalid.pop("max_retries")
    elif change == "extra":
        invalid["reset_usage"] = True
    else:
        invalid["max_attempts"] = True
    before = store.get_task_policy("main")
    with pytest.raises(ValueError):
        store.set_task_policy("main", invalid, "invalid-policy", now=10)
    assert store.get_task_policy("main") == before


def test_concurrent_stores_charge_identical_request_exactly_once(store):
    barrier = threading.Barrier(2)

    def submit():
        with Store(store.path) as independent:
            barrier.wait(timeout=5)
            return admit(independent)

    with ThreadPoolExecutor(max_workers=2) as workers:
        results = list(workers.map(lambda _: submit(), range(2)))
    assert all(result["admitted"] for result in results)
    assert sorted(result["replayed"] for result in results) == [False, True]
    record = store.get_task_policy("main")
    assert record["usage"]["attempts"] == 1
    assert record["current_attempt_id"] == "attempt-1"
    assert list(record["attempts"]) == ["attempt-1"]


def test_replayed_charge_survives_missing_runtime_intent_and_reopen(store):
    first = admit(store)
    with Store(store.path) as reopened:
        second = admit(reopened, now=150)
        assert second["admitted"] and second["replayed"]
        assert second["attempt"] == first["attempt"]
        assert second["usage"]["attempts"] == 1
        different = admit(reopened, "attempt-2", now=151)
        assert not different["admitted"]
        assert different["decision"]["state"] == "reconciliation_required"


def test_attempt_preserves_original_native_root_and_rejects_rebinding(store):
    first = store.admit_task_attempt(
        "main", "native-attempt", digest(), now=100, busy=False, thread_id="original-root"
    )
    assert first["attempt"]["thread_id"] == "original-root"
    with Store(store.path) as reopened:
        replay = reopened.admit_task_attempt(
            "main", "native-attempt", digest(), now=101, busy=False, thread_id="original-root"
        )
        assert replay["replayed"] and replay["attempt"] == first["attempt"]
        for wrong_root in ("replacement-root", None):
            with pytest.raises(ValueError, match="Request ID"):
                reopened.admit_task_attempt(
                    "main", "native-attempt", digest(), now=101, busy=False, thread_id=wrong_root
                )
    assert store.get_task_policy("main")["usage"]["attempts"] == 1


def test_waiting_before_first_admission_never_starts_or_spends_task_budget(store):
    for now in (10, 100, 500):
        result = store.task_policy_status("main", now=now, busy=True)
        assert result["decision"]["reasons"] == ["task_busy"]
        assert result["usage"] is None
        denied = admit(store, now=now, busy=True)
        assert not denied["admitted"] and not denied["replayed"]
        assert denied["attempt"] is None and denied["usage"] is None
    record = store.get_task_policy("main")
    assert record["last_checked_at"] == 500
    assert record["attempts"] == {} and record["current_attempt_id"] is None
    assert admit(store, now=600)["usage"]["started_at"] == 600
    assert store.get_task_policy("main")["usage"]["attempts"] == 1


def test_unstarted_watermark_including_zero_survives_clock_rollback(store):
    store.task_policy_status("main", now=0, busy=True)
    denied = admit(store, now=-1)
    assert denied["decision"]["reasons"] == ["clock_before_task_evidence"]
    assert denied["decision"]["observed_at"] == 0
    assert store.get_task_policy("main")["usage"] is None
    assert admit(store, now=0)["admitted"]


def test_unchanged_wait_denial_does_not_consume_id_or_restart_elapsed_time(store):
    admit(store)
    finish(store, "unchanged", evidence={"host_check": "no new observations"})
    for now in (105, 110, 134):
        denied = admit(store, "attempt-2", now=now)
        assert not denied["admitted"]
        assert denied["decision"]["next_attempt_at"] == 135
        assert denied["usage"]["attempts"] == 1
        assert denied["usage"]["started_at"] == 100
    accepted = admit(store, "attempt-2", now=135)
    assert accepted["admitted"] and not accepted["replayed"]
    assert accepted["usage"]["attempts"] == 2
    assert accepted["usage"]["started_at"] == 100


def test_last_attempt_busy_is_not_exhausted_until_deadline_and_watermark_persists(store, policy):
    store.set_task_policy("main", replace(policy, max_attempts=1), "one-attempt", now=0)
    admit(store)
    for now in (100, 150, 219):
        status = store.task_policy_status("main", now=now, busy=True)
        assert status["decision"]["state"] == "waiting"
        assert status["decision"]["remaining"]["attempts"] == 0
    assert store.task_policy_status("main", now=220, busy=True)["decision"]["state"] == "exhausted"
    with Store(store.path) as reopened:
        status = reopened.task_policy_status("main", now=150, busy=True)
        assert status["decision"]["state"] == "exhausted"
        assert status["decision"]["observed_at"] == 220
        assert status["usage"]["last_checked_at"] == 220


def test_unknown_extension_and_failed_reconciliation_preserve_prior_failures(store, policy):
    admit(store)
    finish(store, "failed")
    admit(store, "attempt-2", now=110)
    unknown = finish(store, "unknown", "attempt-2", now=112)
    assert unknown["usage"]["consecutive_failures"] == 1
    assert unknown["attempts"]["attempt-2"]["prior_failures"] == 1
    store.set_task_policy("main", replace(policy, max_elapsed_seconds=1000), "extend", now=500)
    assert (
        store.task_policy_status("main", now=500, busy=False)["decision"]["state"]
        == "reconciliation_required"
    )
    failed = finish(store, "failed", "attempt-2", now=113)
    assert failed["usage"]["consecutive_failures"] == 2
    assert failed["usage"]["last_finished_at"] == 500
    assert finish(store, "failed", "attempt-2", now=900) == failed
    assert (
        store.task_policy_status("main", now=504, busy=False)["decision"]["next_attempt_at"] == 505
    )
    assert admit(store, "attempt-3", now=505)["admitted"]


def test_finish_clock_rollback_clamps_to_admission_and_checked_watermark(store):
    admit(store)
    store.task_policy_status("main", now=150, busy=True)
    record = finish(store, "failed", now=50)
    assert record["usage"]["last_finished_at"] == 150
    assert record["usage"]["last_checked_at"] == 150
    assert record["usage"]["consecutive_failures"] == 1


@pytest.mark.parametrize("outcome", ["progress", "unchanged", "complete"])
@pytest.mark.parametrize("evidence", [None, {}, [], "model says done"])
def test_business_outcomes_require_nonempty_host_mapping(store, outcome, evidence):
    admit(store)
    before = store.get_task_policy("main")
    with pytest.raises(ValueError, match="evidence"):
        finish(store, outcome, evidence=evidence)
    assert store.get_task_policy("main") == before


def test_unknown_can_reconcile_business_result_and_complete_stays_complete(store, policy):
    admit(store)
    finish(store, "unknown")
    complete = finish(store, "complete", now=110, evidence={"artifact_hash": digest("result")})
    assert complete["usage"]["last_outcome"] == "complete"
    assert complete["usage"]["consecutive_failures"] == 0
    store.set_task_policy("main", replace(policy, max_attempts=100), "extend", now=120)
    assert admit(store, "new-attempt", now=999)["decision"]["state"] == "complete"


def test_early_unknown_and_old_attempt_results_cannot_overwrite_current_attempt(store):
    with pytest.raises(ValueError, match="current admitted"):
        finish(store, "unknown")
    admit(store)
    finish(store, "progress", evidence={"verified_count": 1})
    admit(store, "attempt-2", now=110)
    before = store.get_task_policy("main")
    with pytest.raises(ValueError, match="current admitted"):
        finish(store, "failed", "attempt-1", now=120)
    assert store.get_task_policy("main") == before
    finished = finish(store, "failed", "attempt-2", now=120)
    with pytest.raises(ValueError, match="confirmed"):
        finish(store, "unknown", "attempt-2", now=121)
    assert store.get_task_policy("main") == finished


def test_admission_write_failure_rolls_back_both_usage_and_request_reservation(store):
    store._connection().execute(
        "CREATE TRIGGER reject_policy_update BEFORE UPDATE ON settings "
        "WHEN NEW.key GLOB 'task_policy/*' BEGIN SELECT RAISE(ABORT,'injected full disk'); END"
    )
    before = store.get_task_policy("main")
    with pytest.raises(StoreError, match="injected full disk"):
        admit(store)
    assert store.get_task_policy("main") == before
    store._connection().execute("DROP TRIGGER reject_policy_update")
    result = admit(store)
    assert result["admitted"] and not result["replayed"]
    assert result["usage"]["attempts"] == 1


@pytest.mark.parametrize(
    "damage",
    [
        "version",
        "key",
        "usage",
        "decision",
        "missing_request",
        "request_target",
        "request_version",
        "request_thread",
    ],
)
def test_own_namespace_corruption_fails_closed_on_read_and_reopen(store, damage):
    admit(store)
    db = store._connection()
    key = store._policy_key("task_policy", "main")
    if damage in {"version", "usage", "decision"}:
        record = store.get_task_policy("main")
        if damage == "version":
            record["version"] = 900
        elif damage == "usage":
            record["usage"]["attempts"] = 3
        else:
            record["attempts"]["attempt-1"]["decision"].pop("remaining")
        db.execute("UPDATE settings SET value=? WHERE key=?", (json.dumps(record), key))
    elif damage == "key":
        db.execute("UPDATE settings SET key='task_policy/wrong-binding' WHERE key=?", (key,))
    else:
        key = store._policy_key("task_policy_request", "attempt-1")
        if damage == "missing_request":
            db.execute("DELETE FROM settings WHERE key=?", (key,))
        else:
            request = json.loads(
                db.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()[0]
            )
            field, value = {
                "request_target": ("target", "other"),
                "request_version": ("version", 2),
                "request_thread": ("thread_id", "changed-root"),
            }[damage]
            request[field] = value
            db.execute("UPDATE settings SET value=? WHERE key=?", (json.dumps(request), key))
    original = list(db.execute("SELECT key,value FROM settings ORDER BY key"))
    with pytest.raises(StoreError, match="persisted task policy"):
        store.get_task_policy("main")
    with pytest.raises(StoreError, match="persisted task policy"):
        Store(store.path)
    assert list(db.execute("SELECT key,value FROM settings ORDER BY key")) == original


def test_schema_one_keeps_extra_unrelated_settings_and_all_attempt_receipts(store):
    db = store._connection()
    db.execute("INSERT INTO settings VALUES ('other_owner/future', '{\"version\":999}')")
    admit(store)
    finish(store, "progress", evidence={"host_verified": True})
    admit(store, "attempt-2", now=110)
    with Store(store.path) as reopened:
        assert reopened._connection().execute("PRAGMA user_version").fetchone()[0] == 1
        assert {
            row[0]
            for row in reopened._connection().execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        } == {"settings", "jobs", "events", "lease"}
        assert len(reopened.get_task_policy("main")["attempts"]) == 2
        assert (
            reopened._connection()
            .execute("SELECT value FROM settings WHERE key='other_owner/future'")
            .fetchone()[0]
            == '{"version":999}'
        )
        assert [record["target"] for record in reopened.list_task_policies()] == ["main"]
        assert reopened.get_task_policy("absent") is None
        assert reopened.task_policy_status("absent", now=110, busy=False) is None
        assert admit(reopened, "unmanaged-request", target="absent") is None


@pytest.mark.parametrize("now", [None, True, "100", float("nan"), float("inf")])
def test_timestamps_must_be_explicit_finite_host_numbers(store, policy, now):
    with pytest.raises(ValueError):
        store.set_task_policy("main", policy, "new-policy", now=now)
    with pytest.raises(ValueError):
        store.task_policy_status("main", now=now, busy=False)
    with pytest.raises(ValueError):
        admit(store, now=now)
    with pytest.raises(ValueError):
        finish(store, "unknown", now=now)


def test_status_uses_the_shared_policy_decision_implementation(store, monkeypatch):
    calls = []
    original = TaskPolicy.decide

    def observed(self, usage, *, now, busy):
        calls.append((usage.attempts, now, busy))
        return original(self, usage, now=now, busy=busy)

    monkeypatch.setattr(TaskPolicy, "decide", observed)
    store.task_policy_status("main", now=100, busy=True)
    admit(store, now=110)
    assert calls == [(0, 100, True), (0, 110, False)]
