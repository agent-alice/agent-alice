"""Calendar-to-memory integration uses temporary archives and no model calls."""

from dataclasses import replace
import datetime as dt
import json
from pathlib import Path

import pytest

from alice_codex.calendar import (
    CalendarError,
    closed_periods,
    dispatch_completion,
    prepare_dispatch,
    register_default_jobs,
    summary_level,
)
from alice_codex.memory import MemoryStore, SourceChangedError, SummaryValidationError
from alice_codex.store import DispatchEvent, Job, Store


NOW = dt.datetime(2027, 1, 10, tzinfo=dt.timezone.utc)


def occurrence(level, first, last=None):
    job = Job(
        "test-" + level,
        "summary:" + level,
        "cron",
        "5 */2 * * *",
        timezone="Asia/Shanghai",
        target="summary:" + level,
    )
    begin = dt.datetime.fromisoformat(first + "+08:00").timestamp()
    end = dt.datetime.fromisoformat((last or first) + "+08:00").timestamp()
    return DispatchEvent("event-" + level, job.id, job, begin, end, end > begin, "sending")


@pytest.mark.parametrize(
    "level,first,last,expected",
    [
        (
            "L1",
            "2026-09-02T00:05:00",
            "2026-09-02T06:05:00",
            ["2026-09-01T22:00", "2026-09-02T00:00", "2026-09-02T02:00", "2026-09-02T04:00"],
        ),
        (
            "L2",
            "2026-09-01T00:15:00",
            "2026-09-04T00:15:00",
            ["2026-08-31", "2026-09-01", "2026-09-02", "2026-09-03"],
        ),
        ("L3", "2026-01-05T00:25:00", "2026-01-19T00:25:00", ["2026-W01", "2026-W02", "2026-W03"]),
        (
            "L4",
            "2026-01-01T00:35:00",
            "2026-04-01T00:35:00",
            ["2025-12", "2026-01", "2026-02", "2026-03"],
        ),
    ],
)
def test_catchup_keeps_every_closed_period(level, first, last, expected):
    event = occurrence(level, first, last)
    assert closed_periods(event, now=NOW) == expected
    assert closed_periods(replace(event, catch_up=False), now=NOW) == expected


def test_calendar_rejects_future_invalid_and_oversized_ranges():
    event = occurrence("L1", "2026-09-01T02:05:00", "2026-09-01T06:05:00")
    with pytest.raises(CalendarError, match="partitioning"):
        closed_periods(event, now=NOW, max_periods=2)
    with pytest.raises(CalendarError, match="time range"):
        closed_periods(replace(event, through_at=event.due_at - 1), now=NOW)
    with pytest.raises(CalendarError, match="time range"):
        closed_periods(replace(event, due_at=float("nan")), now=NOW)
    with pytest.raises(CalendarError, match="current time"):
        closed_periods(event, now=dt.datetime(2026, 8, 1, tzinfo=dt.timezone.utc))
    with pytest.raises(CalendarError):
        summary_level(replace(event.job, target="summary:L2"))
    assert summary_level(replace(event.job, target="main", name="ordinary")) is None


def test_default_registration_does_not_reset_live_or_customized_state(tmp_path):
    with Store(tmp_path / "jobs.sqlite3") as store:
        jobs = register_default_jobs(
            store, now=dt.datetime(2026, 9, 1, tzinfo=dt.timezone.utc).timestamp()
        )
        assert all(not job.enabled for job in jobs)
        changed = store.update_job(
            jobs[0].id, enabled=True, prompt="Local adjusted task", now=1_788_240_000
        )
        again = register_default_jobs(store, now=1_888_240_000)
        assert again == [changed, *jobs[1:]]
        assert store.list_events() == []
        store.update_job(jobs[2].id, schedule_value="0 12 * * 1", now=1_788_240_000)
        preserved = store.list_jobs()
        with pytest.raises(CalendarError, match="differs"):
            register_default_jobs(store)
        assert store.list_jobs() == preserved


def test_prepare_dispatch_records_empty_gaps_and_checks_actual_commits(tmp_path):
    memory = MemoryStore(tmp_path / "data")
    for hour in (0, 4):
        memory.append_event(
            f"event-{hour}",
            {"content": f"Observation at {hour}"},
            timestamp=f"2026-09-01T{hour:02d}:20:00+08:00",
        )
    event = occurrence("L1", "2026-09-01T02:05:00", "2026-09-01T06:05:00")
    plan = prepare_dispatch(event, memory, now=NOW)
    assert plan["periods"] == ["2026-09-01T00:00", "2026-09-01T02:00", "2026-09-01T04:00"]
    assert plan["skipped"] == [
        {"period": "2026-09-01T02:00", "reason": "no_sources", "status": "skipped"}
    ]
    assert [batch["period"] for batch in plan["prepared"]] == [
        "2026-09-01T00:00",
        "2026-09-01T04:00",
    ]
    assert Path(plan["plan_path"]).is_file()
    assert plan["plan_path"] in plan["prompt"]
    assert prepare_dispatch(event, memory, now=NOW)["plan_sha256"] == plan["plan_sha256"]
    assert not dispatch_completion(plan, memory)["complete"]
    for index, batch in enumerate(plan["prepared"]):
        manifest = json.loads(Path(batch["manifest_path"]).read_text())
        ids = [source["source_id"] for source in manifest["sources"]]
        candidate = {
            "content": "Observed " + " ".join(f"[source:{sid}]" for sid in ids),
            "source_ids": ids,
            "covered_source_ids": ids,
            "missing": [],
        }
        # Candidate creation alone must not count as committed work.
        Path(batch["candidate_path"]).write_text(json.dumps(candidate))
        assert not dispatch_completion(plan, memory)["complete"]
        memory.commit_summary(batch["batch_id"], candidate)
        assert dispatch_completion(plan, memory)["complete"] == (index == 1)
    assert dispatch_completion(plan, memory) == {
        "complete": True,
        "committed_batch_ids": [b["batch_id"] for b in plan["prepared"]],
        "pending_batch_ids": [],
        "skipped": plan["skipped"],
    }


@pytest.mark.parametrize(
    "error", [SourceChangedError("changed input"), SummaryValidationError("bad source")]
)
def test_planner_does_not_misreport_bad_source_as_empty(tmp_path, monkeypatch, error):
    memory = MemoryStore(tmp_path / "data")

    def fail(*args, **kwargs):
        raise error

    monkeypatch.setattr(memory, "prepare_summary", fail)
    with pytest.raises(type(error), match=str(error)):
        prepare_dispatch(occurrence("L2", "2026-09-01T00:15:00"), memory, now=NOW)
    assert not (memory.state / "calendar").exists()


def test_no_sources_plan_is_explicitly_skipped_without_model_work(tmp_path):
    memory = MemoryStore(tmp_path / "data")
    plan = prepare_dispatch(occurrence("L4", "2026-09-01T00:35:00"), memory, now=NOW)
    assert plan["prepared"] == []
    assert plan["skipped"] == [{"period": "2026-08", "reason": "no_sources", "status": "skipped"}]
    assert dispatch_completion(plan, memory)["complete"]
