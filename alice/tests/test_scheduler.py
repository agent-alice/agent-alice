import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from alice_codex.scheduler import DeferredDispatch, RejectedDispatch, Scheduler, next_due
from alice_codex.store import DispatchReceipt, Store, StoreError


class Clock:
    def __init__(self, now=0):
        self.now = now

    def __call__(self):
        return self.now


class Dispatcher:
    def __init__(self, *, busy=False, receipt=None, error=None):
        self.busy, self.error = busy, error
        self.receipt = receipt or DispatchReceipt("accepted", "thread", "turn")
        self.seen = []

    async def is_busy(self, target):
        return self.busy

    async def dispatch(self, event):
        self.seen.append(event)
        if self.error is not None:
            raise self.error
        return self.receipt


def ts(value, timezone="UTC"):
    return datetime.fromisoformat(value).replace(tzinfo=ZoneInfo(timezone)).timestamp()


@pytest.fixture
def store(tmp_path):
    with Store(tmp_path / "schedule.db") as value:
        yield value


@pytest.mark.parametrize(
    "options",
    [
        {"schedule_type": "every", "schedule_value": 0},
        {"schedule_type": "every", "schedule_value": float("nan")},
        {"schedule_type": "cron", "schedule_value": "* * * * * *"},
        {"schedule_type": "cron", "schedule_value": "nonsense"},
        {
            "schedule_type": "at",
            "schedule_value": "2026-03-08T02:30:00",
            "timezone": "America/New_York",
        },
        {"schedule_type": "at", "schedule_value": 1, "timezone": "not/a-zone"},
    ],
)
def test_invalid_schedules_do_not_create_jobs(store, options):
    with pytest.raises(ValueError):
        store.create_job(name="invalid", now=0, **options)
    assert store.list_jobs() == []


@pytest.mark.parametrize(
    "cron,now,expected",
    [
        ("0 * * * *", "2026-09-06T12:15:00", "2026-09-06T13:00:00"),
        ("0 0 * * *", "2026-09-06T23:59:00", "2026-09-07T00:00:00"),
        ("0 0 * * 1", "2026-09-06T12:00:00", "2026-09-07T00:00:00"),
        ("0 0 1 * *", "2026-12-31T23:59:00", "2027-01-01T00:00:00"),
        ("0 0 29 2 *", "2027-01-01T00:00:00", "2028-02-29T00:00:00"),
    ],
)
def test_calendar_boundaries_in_named_timezone(store, cron, now, expected):
    job = store.create_job(
        name="calendar",
        schedule_type="cron",
        schedule_value=cron,
        timezone="Asia/Shanghai",
        now=ts(now, "Asia/Shanghai"),
    )
    assert job.next_due == ts(expected, "Asia/Shanghai")


def test_calendar_day_keeps_local_midnight_across_dst(store):
    zone = "America/New_York"
    job = store.create_job(
        name="daily",
        schedule_type="cron",
        schedule_value="0 0 * * *",
        timezone=zone,
        now=ts("2026-03-08T00:01:00", zone),
    )
    assert job.next_due == ts("2026-03-09T00:00:00", zone)


def test_one_shot_runs_once_and_accepted_does_not_mean_completed(store):
    job = store.create_job(name="once", schedule_type="at", schedule_value=10, now=0)
    clock, dispatcher = Clock(9), Dispatcher()
    scheduler = Scheduler(store, dispatcher, clock=clock)
    assert asyncio.run(scheduler.poll()) == []
    clock.now = 10
    assert asyncio.run(scheduler.poll())[0].status == "accepted"
    clock.now = 100
    assert asyncio.run(scheduler.poll()) == []
    assert len(dispatcher.seen) == 1
    assert store.get_job(job.id).next_due is None


def test_busy_heartbeat_coalesces_one_pending_across_ticks(store):
    job = store.create_job(
        name="heartbeat", kind="heartbeat", schedule_type="every", schedule_value=10, now=0
    )
    clock, dispatcher = Clock(10), Dispatcher(busy=True)
    scheduler = Scheduler(store, dispatcher, clock=clock)
    for instant in (10, 20, 90):
        clock.now = instant
        assert asyncio.run(scheduler.poll()) == []
    events = store.list_events()
    assert len(events) == 1
    assert (events[0].due_at, events[0].through_at) == (10, 90)
    assert store.get_job(job.id).next_due == 100
    dispatcher.busy = False
    asyncio.run(scheduler.poll())
    assert len(dispatcher.seen) == 1
    assert dispatcher.seen[0].id == events[0].id


def test_coalesced_heartbeat_keeps_catchup_flag_on_each_exact_tick(store):
    store.create_job(
        name="heartbeat",
        kind="heartbeat",
        schedule_type="every",
        schedule_value=10,
        catch_up=True,
        now=0,
    )
    clock = Clock(10)
    scheduler = Scheduler(store, Dispatcher(busy=True), clock=clock)
    assert asyncio.run(scheduler.poll()) == []
    first = store.list_events()[0]
    assert not first.catch_up
    for instant in (20, 90, 100):
        clock.now = instant
        assert asyncio.run(scheduler.poll()) == []
        event = store.list_events()[0]
        assert event.id == first.id
        assert (event.due_at, event.through_at, event.catch_up) == (10, instant, True)


def test_pause_after_mark_sending_keeps_same_window_for_restart(tmp_path):
    path = tmp_path / "deferred.db"

    class Pausing(Dispatcher):
        async def dispatch(self, event):
            self.seen.append(event)
            assert store.get_event(event.id).status == "sending"
            store.set_autonomy_paused(True)
            raise DeferredDispatch("paused before native submission")

    with Store(path) as store:
        store.create_job(
            name="closed windows",
            schedule_type="every",
            schedule_value=10,
            catch_up=True,
            now=0,
        )
        dispatcher = Pausing()
        results = asyncio.run(Scheduler(store, dispatcher, clock=Clock(90)).poll())
        event = results[0]
        assert (event.status, event.due_at, event.through_at) == ("pending", 10, 90)
        assert event.receipt is None
        assert len(dispatcher.seen) == 1

    with Store(path) as store:
        dispatcher = Dispatcher()
        scheduler = Scheduler(store, dispatcher, clock=Clock(90))
        assert store.is_autonomy_paused()
        assert asyncio.run(scheduler.poll()) == []
        assert dispatcher.seen == []
        store.set_autonomy_paused(False)
        resumed = asyncio.run(scheduler.poll())
        assert [(e.id, e.status) for e in resumed] == [(event.id, "accepted")]
        assert [(e.due_at, e.through_at) for e in dispatcher.seen] == [(10, 90)]
        assert len(store.list_events()) == 1


@pytest.mark.parametrize("change", ["update", "disable", "delete"])
def test_deferred_dispatch_cancels_obsolete_definition(store, change):
    job = store.create_job(name="once", schedule_type="at", schedule_value=0, now=0)

    class Changed(Dispatcher):
        async def dispatch(self, event):
            if change == "delete":
                store.delete_job(job.id)
            elif change == "disable":
                store.update_job(job.id, enabled=False, now=0)
            else:
                store.update_job(job.id, prompt="new definition", now=0)
            raise DeferredDispatch("temporarily busy before native submission")

    outcomes = asyncio.run(Scheduler(store, Changed(), clock=Clock()).poll())
    assert len(outcomes) == 1
    assert outcomes[0].status == "cancelled"
    assert outcomes[0].job == job
    assert outcomes[0].receipt is None


def test_pause_persists_through_restart_and_never_unpauses_on_tick(tmp_path):
    path = tmp_path / "state.db"
    dispatcher = Dispatcher()
    with Store(path) as store:
        store.create_job(
            name="heartbeat", kind="heartbeat", schedule_type="every", schedule_value=10, now=0
        )
        store.set_autonomy_paused(True)
        assert asyncio.run(Scheduler(store, dispatcher, clock=Clock(10)).poll()) == []
    with Store(path) as store:
        scheduler = Scheduler(store, dispatcher, clock=Clock(100))
        assert asyncio.run(scheduler.poll()) == []
        assert store.is_autonomy_paused()
        assert store.list_events() == []
        store.set_autonomy_paused(False)
        assert len(asyncio.run(scheduler.poll())) == 1
    assert len(dispatcher.seen) == 1


def test_downtime_calendar_catchup_preserves_missing_period_window(store):
    job = store.create_job(
        name="daily memory",
        schedule_type="cron",
        schedule_value="0 0 * * *",
        timezone="Asia/Shanghai",
        catch_up=True,
        target="new",
        now=ts("2026-09-01T00:01:00", "Asia/Shanghai"),
    )
    dispatcher = Dispatcher(receipt=DispatchReceipt("completed", "worker"))
    now = ts("2026-09-05T12:00:00", "Asia/Shanghai")
    results = asyncio.run(Scheduler(store, dispatcher, clock=Clock(now)).poll())
    assert len(results) == 1 and results[0].catch_up
    assert results[0].due_at == ts("2026-09-02T00:00:00", "Asia/Shanghai")
    assert results[0].through_at == ts("2026-09-05T00:00:00", "Asia/Shanghai")
    assert store.get_job(job.id).next_due == ts("2026-09-06T00:00:00", "Asia/Shanghai")


def test_forward_and_backward_clock_moves_do_not_replay_intervals(store):
    job = store.create_job(name="clock", schedule_type="every", schedule_value=10, now=0)
    dispatcher, clock = Dispatcher(), Clock(95)
    scheduler = Scheduler(store, dispatcher, clock=clock)
    assert len(asyncio.run(scheduler.poll())) == 1
    assert store.get_job(job.id).next_due == 100
    clock.now = 15
    assert asyncio.run(scheduler.poll()) == []
    assert next_due(store.get_job(job.id), 15) == 100
    clock.now = 100
    assert len(asyncio.run(scheduler.poll())) == 1
    assert len(dispatcher.seen) == 2


@pytest.mark.parametrize(
    "error,status",
    [(RejectedDispatch("not sent"), "failed"), (ConnectionError("ack lost"), "unknown")],
)
def test_confirmed_rejection_and_unknown_transport_are_different(store, error, status):
    store.create_job(name="effect", schedule_type="at", schedule_value=0, now=0)
    dispatcher = Dispatcher(error=error)
    scheduler = Scheduler(store, dispatcher, clock=Clock())
    assert asyncio.run(scheduler.poll())[0].status == status
    assert asyncio.run(scheduler.poll()) == []
    assert len(dispatcher.seen) == 1


def test_cancellation_during_dispatch_records_unknown_and_stops_owned_task(store):
    store.create_job(name="effect", schedule_type="at", schedule_value=0, now=0)

    async def scenario():
        entered, stopped = asyncio.Event(), asyncio.Event()

        class Blocking(Dispatcher):
            async def dispatch(self, event):
                entered.set()
                try:
                    await asyncio.Future()
                finally:
                    stopped.set()

        task = asyncio.create_task(Scheduler(store, Blocking(), clock=Clock()).poll())
        await asyncio.wait_for(entered.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert stopped.is_set()

    asyncio.run(scenario())
    assert store.list_events()[0].status == "unknown"


def test_two_schedulers_do_not_dispatch_in_parallel(store, tmp_path):
    store.create_job(name="once", schedule_type="at", schedule_value=0, now=0)

    async def scenario():
        entered, finish = asyncio.Event(), asyncio.Event()

        class Blocking(Dispatcher):
            async def dispatch(self, event):
                self.seen.append(event)
                entered.set()
                await finish.wait()
                return DispatchReceipt("accepted", "first")

        first, second = Blocking(), Dispatcher()
        one = asyncio.create_task(Scheduler(store, first, clock=Clock(), owner="one").poll())
        await asyncio.wait_for(entered.wait(), 1)
        with Store(store.path) as other:
            assert await Scheduler(other, second, clock=Clock(), owner="two").poll() == []
        finish.set()
        await one
        assert len(first.seen) == 1
        assert second.seen == []

    asyncio.run(scenario())


def test_ack_persistence_failure_is_recovered_as_unknown_not_resent(store, monkeypatch):
    store.create_job(name="effect", schedule_type="at", schedule_value=0, now=0)
    dispatcher = Dispatcher()
    scheduler = Scheduler(store, dispatcher, clock=Clock())
    original = store.record_receipt

    def fail(*args):
        raise StoreError("disk full after remote acknowledgement")

    monkeypatch.setattr(store, "record_receipt", fail)
    with pytest.raises(StoreError, match="disk full"):
        asyncio.run(scheduler.poll())
    monkeypatch.setattr(store, "record_receipt", original)
    assert store.list_events()[0].status == "sending"
    assert asyncio.run(Scheduler(store, dispatcher, clock=Clock(1)).poll()) == []
    assert store.list_events()[0].status == "unknown"
    assert len(dispatcher.seen) == 1


def test_busy_probe_failure_leaves_safe_unsent_event(store):
    store.create_job(name="once", schedule_type="at", schedule_value=0, now=0)

    class Broken(Dispatcher):
        async def is_busy(self, target):
            raise ConnectionError("state service unavailable")

    dispatcher = Broken()
    with pytest.raises(ConnectionError):
        asyncio.run(Scheduler(store, dispatcher, clock=Clock()).poll())
    assert store.list_events()[0].status == "pending"
    assert dispatcher.seen == []


@pytest.mark.parametrize("terminal", ["completed", "failed"])
def test_terminal_notification_before_ack_is_preserved(store, terminal):
    store.create_job(name="fast", schedule_type="at", schedule_value=0, now=0)

    class Fast(Dispatcher):
        async def dispatch(self, event):
            store.record_receipt(
                event.id,
                DispatchReceipt(terminal, "thread", "turn", "observed terminal notification"),
            )
            return DispatchReceipt("accepted", "thread", "turn", "late ack")

    outcomes = asyncio.run(Scheduler(store, Fast(), clock=Clock()).poll())
    assert outcomes[0].status == terminal
    assert outcomes[0].receipt.detail == "observed terminal notification"
    assert not store.is_autonomy_paused()
