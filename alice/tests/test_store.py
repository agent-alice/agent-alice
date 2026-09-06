import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

from alice_codex.store import DispatchReceipt, LeaseLost, Store, StoreError


@pytest.fixture
def store(tmp_path):
    with Store(tmp_path / "schedule.sqlite") as value:
        yield value


def at(store, **kwargs):
    return store.create_job(name="once", schedule_type="at", schedule_value=10, now=0, **kwargs)


def due(store, owner="one", now=10):
    assert store.acquire_lease(owner, now=now)
    return store.materialize_due(owner, now=now)[0]


def test_crud_and_pause_survive_reopen(tmp_path):
    path = tmp_path / "state.db"
    with Store(path) as store:
        job = at(store, target="task-root", timezone="Asia/Shanghai", prompt="inspect")
        assert store.update_job(job.id, enabled=False, now=1).enabled is False
        store.set_autonomy_paused(True)
    with Store(path) as store:
        assert store.is_autonomy_paused()
        assert store.get_job(job.id).target == "task-root"
        assert store.get_job(job.id).enabled is False
        assert len(store.list_jobs()) == 1
        assert store.delete_job(job.id)
        assert not store.delete_job(job.id)
        assert store.list_jobs() == []
        with pytest.raises(KeyError):
            store.get_job(job.id)


def test_corrupt_database_is_not_replaced(tmp_path):
    path = tmp_path / "corrupt.db"
    original = b"broken database with important forensic evidence"
    path.write_bytes(original)
    with pytest.raises(StoreError, match="cannot open"):
        Store(path)
    assert path.read_bytes() == original


def test_future_schema_and_bad_business_records_fail_closed(tmp_path):
    path = tmp_path / "future.db"
    with sqlite3.connect(path) as db:
        db.execute("PRAGMA user_version=900")
    before = path.read_bytes()
    with pytest.raises(StoreError, match="unsupported"):
        Store(path)
    assert path.read_bytes() == before
    path = tmp_path / "invalid.db"
    with Store(path) as store:
        job = at(store)
    with sqlite3.connect(path) as db:
        db.execute("UPDATE jobs SET definition=?", (json.dumps({"id": job.id}),))
    before = path.read_bytes()
    with pytest.raises(StoreError, match="invalid persisted job"):
        Store(path)
    assert path.read_bytes() == before


def test_write_failure_preserves_job_and_existing_pause(store):
    job = at(store)
    store._connection().execute("PRAGMA query_only=ON")
    with pytest.raises(StoreError, match="transaction failed"):
        store.update_job(job.id, prompt="bad update")
    with pytest.raises(StoreError):
        store.set_autonomy_paused(True)
    store._connection().execute("PRAGMA query_only=OFF")
    assert store.get_job(job.id) == job
    assert not store.is_autonomy_paused()


def test_materialization_rolls_back_cursor_if_event_write_fails(store):
    job = at(store)
    store.acquire_lease("one", now=10)
    store._connection().execute(
        "CREATE TRIGGER reject_event BEFORE INSERT ON events BEGIN SELECT RAISE(ABORT,'injected full disk'); END"
    )
    with pytest.raises(StoreError, match="injected full disk"):
        store.materialize_due("one", now=10)
    assert store.get_job(job.id).next_due == 10
    assert store.list_events() == []
    store._connection().execute("DROP TRIGGER reject_event")
    assert len(store.materialize_due("one", now=10)) == 1


def test_two_connections_cannot_claim_same_event(tmp_path):
    path = tmp_path / "state.db"
    with Store(path) as first, Store(path) as second:
        at(first)
        event = due(first)
        assert not second.acquire_lease("two", now=11)
        with pytest.raises(LeaseLost):
            second.claim_event(event.id, "two", now=11)
        assert first.claim_event(event.id, "one", now=11)
        assert not first.claim_event(event.id, "one", now=11)
        assert second.list_events()[0].status == "claimed"


def test_restart_reclaims_unsent_but_quarantines_uncertain_sending(tmp_path):
    path = tmp_path / "state.db"
    with Store(path) as store:
        first = at(store)
        second = at(store)
        store.acquire_lease("dead", now=10, seconds=10)
        events = store.materialize_due("dead", now=10)
        by_job = {e.job_id: e for e in events}
        claimed, sending = by_job[first.id], by_job[second.id]
        assert store.claim_event(claimed.id, "dead", now=10)
        assert store.claim_event(sending.id, "dead", now=10)
        assert store.mark_sending(sending.id, "dead", now=10)
    with Store(path) as store:
        assert not store.acquire_lease("new", now=19)
        assert store.acquire_lease("new", now=21)
        assert store.get_event(claimed.id).status == "pending"
        assert store.get_event(sending.id).status == "unknown"
        assert not store.claim_event(sending.id, "new", now=21)
        assert store.claim_event(claimed.id, "new", now=21)


def test_receipt_acknowledgement_is_not_completion_and_survives_restart(tmp_path):
    path = tmp_path / "state.db"
    with Store(path) as store:
        at(store)
        event = due(store)
        store.claim_event(event.id, "one", now=10)
        store.mark_sending(event.id, "one", now=10)
        assert (
            store.record_receipt(event.id, DispatchReceipt("accepted", "thread", "turn")).status
            == "accepted"
        )
    with Store(path) as store:
        store.acquire_lease("new", now=100)
        assert store.get_event(event.id).status == "accepted"
        done = store.record_receipt(event.id, DispatchReceipt("completed"))
        assert done.receipt.thread_id == "thread"
        assert done.receipt.turn_id == "turn"
        with pytest.raises(ValueError, match="terminal"):
            store.record_receipt(event.id, DispatchReceipt("unknown"))


def test_disable_or_delete_cancels_only_unsent_events(store):
    job = at(store)
    event = due(store)
    store.claim_event(event.id, "one", now=10)
    store.update_job(job.id, enabled=False, now=10)
    assert not store.mark_sending(event.id, "one", now=10)
    assert store.get_event(event.id).status == "cancelled"
    store.delete_job(job.id)
    assert store.get_event(event.id).job.prompt == job.prompt


def test_pause_between_claim_and_send_blocks_dispatch(store):
    at(store)
    event = due(store)
    store.claim_event(event.id, "one", now=10)
    store.set_autonomy_paused(True)
    assert not store.mark_sending(event.id, "one", now=10)
    assert store.get_event(event.id).status == "pending"


def test_unknown_outcome_blocks_future_periods_until_reconciled(store):
    job = store.create_job(name="effect", schedule_type="every", schedule_value=10, now=0)
    event = due(store)
    store.claim_event(event.id, "one", now=10)
    store.mark_sending(event.id, "one", now=10)
    store.record_receipt(event.id, DispatchReceipt("unknown"))
    assert store.materialize_due("one", now=30) == []
    assert store.get_job(job.id).next_due == 20
    store.record_receipt(event.id, DispatchReceipt("completed", thread_id="verified"))
    assert len(store.materialize_due("one", now=30)) == 1


def test_atomic_materialization_has_stable_identity(store):
    at(store)
    event = due(store)
    assert store.materialize_due("one", now=10) == []
    store.release_lease("one")
    assert store.acquire_lease("two", now=11)
    assert store.materialize_due("two", now=11) == []
    assert [item.id for item in store.list_events()] == [event.id]


def test_clock_rollback_does_not_steal_live_lease(store):
    assert store.acquire_lease("first", now=100)
    assert not store.acquire_lease("second", now=50)
    with pytest.raises(LeaseLost):
        store.renew_lease("first", now=131)


def test_abrupt_owned_process_exit_preserves_pause_and_unknown_receipt(tmp_path):
    """Real process dies without connection.close/finally; only disk survives."""
    path = tmp_path / "abrupt.db"
    script = """
import os, sys
from alice_codex.store import Store
store = Store(sys.argv[1])
store.create_job(name='effect', schedule_type='at', schedule_value=10, now=0)
store.acquire_lease('dead-process', now=10, seconds=10)
event = store.materialize_due('dead-process', now=10)[0]
store.claim_event(event.id, 'dead-process', now=10)
store.mark_sending(event.id, 'dead-process', now=10)
store.set_autonomy_paused(True)
print(event.id, flush=True)
os._exit(23)
"""
    env = {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")}
    result = subprocess.run(
        [sys.executable, "-c", script, str(path)],
        capture_output=True,
        text=True,
        timeout=5,
        env=env,
    )
    assert result.returncode == 23, result.stderr
    event_id = result.stdout.strip()
    with Store(path) as store:
        assert store.is_autonomy_paused()
        assert store.acquire_lease("replacement", now=21)
        assert store.get_event(event_id).status == "unknown"
        assert store.materialize_due("replacement", now=21) == []
        assert len(store.list_jobs()) == 1
