"""Deterministic regression for the native heartbeat test's settled-state observer."""

import asyncio
from copy import deepcopy
from dataclasses import asdict

import pytest

from alice_codex.scheduler import Scheduler
from alice_codex.service import Service
from alice_codex.store import Store
from test_heartbeat_store import receipt
from test_native_task_policy import settled_unchanged_heartbeat, until


@pytest.mark.parametrize("phase", ["claimed", "sending"])
async def test_unchanged_observer_waits_for_same_deferred_event(tmp_path, phase):
    with Store(tmp_path / "heartbeat.sqlite3") as store:
        store.set_autonomy_paused(False)
        first = receipt("observation-0")
        for index in range(4):
            store.record_heartbeat(
                "monitor", receipt(f"observation-{index}", at=100 + index), wait_seconds=30
            )
        service = Service.__new__(Service)
        service.store = store
        service.state = {
            "heartbeat_consumed": {"monitor": first},
            "intents": {"first": {"id": "first", "target": "monitor", "status": "completed"}},
        }
        before = deepcopy(service.state)
        # The native case separately proves this first request. Here any second
        # submission is a failure; no model/native process is used by this test.
        requests = ["first-recorded-request"]

        async def submit(*args, **kwargs):
            requests.append("unexpected-second-request")
            raise AssertionError("unchanged evidence must not submit a second request")

        async def observe(target):
            return {
                **store.get_heartbeat_state(target),
                "configured": True,
                "collection_rejected": False,
            }

        service.submit, service.observe_heartbeat = submit, observe
        entered, release = asyncio.Event(), asyncio.Event()

        class BarrierDispatcher:
            async def is_busy(self, target):
                if phase == "claimed":
                    entered.set()
                    await release.wait()
                return False

            async def dispatch(self, event):
                if phase == "sending":
                    entered.set()
                    await release.wait()
                return await service.dispatch(event)

        store.create_job(
            name="heartbeat",
            kind="heartbeat",
            target="monitor",
            schedule_type="every",
            schedule_value=10,
            now=90,
        )
        scheduler = Scheduler(store, BarrierDispatcher(), clock=lambda: 100)

        def snapshot():
            return {
                "heartbeat": store.get_heartbeat_state("monitor"),
                "events": [asdict(item) for item in store.list_events()],
                "intents": deepcopy(list(service.state["intents"].values())),
                "consumed": deepcopy(service.state["heartbeat_consumed"]["monitor"]),
            }

        async def settled():
            return settled_unchanged_heartbeat(snapshot(), observed_checks=4, minimum_checks=4)

        poll = asyncio.create_task(scheduler.poll())
        try:
            await asyncio.wait_for(entered.wait(), timeout=2)
            inflight = snapshot()
            event_id = inflight["events"][0]["event_id"]
            assert [item["status"] for item in inflight["events"]] == [phase]
            # All original readiness conditions hold in this legal window.
            evidence = inflight["heartbeat"]
            assert evidence["comparison"]["state"] == "unchanged"
            assert evidence["latest"]["state"] == "known"
            assert evidence["waiting_until"] > evidence["latest"]["observed_at"]
            assert await settled() is None

            release.set()
            await asyncio.wait_for(poll, timeout=2)
            result = await until(settled, timeout=2)
            assert [item["status"] for item in result["events"]] == ["pending"]
            assert result["events"][0]["event_id"] == event_id
            assert service.state == before
            assert requests == ["first-recorded-request"]
        finally:
            release.set()
            if not poll.done():
                poll.cancel()
            await asyncio.gather(poll, return_exceptions=True)


@pytest.mark.parametrize("states", [["sending"], ["completed"], [], ["pending", "sending"]])
async def test_unchanged_observer_times_out_when_event_never_settles(states):
    snapshot = {
        "heartbeat": {
            "comparison": {"state": "unchanged"},
            "latest": {"state": "known", "observed_at": 100},
            "waiting_until": 130,
        },
        "events": [{"status": state} for state in states],
    }

    async def unchanged_forever():
        return settled_unchanged_heartbeat(snapshot, observed_checks=4, minimum_checks=4)

    # The same bounded waiter used by the native test must reject a permanent
    # lost/stuck event. Timeout is a test failure there, not an accepted result.
    with pytest.raises(TimeoutError):
        await until(unchanged_forever, timeout=0.1)
