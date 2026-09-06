"""Legacy schedule migration tests never touch production sources or send data."""

import copy
from contextlib import contextmanager
import json
from pathlib import Path

import pytest

from alice_codex.legacy import LegacyPlanError, export_legacy_plan, import_legacy_plan
from alice_codex.memory import SourceChangedError
from alice_codex.store import Store


def job(identifier, text, **payload):
    return {
        "id": identifier,
        "name": identifier,
        "enabled": True,
        "schedule": {"kind": "cron", "expr": "0 4 * * *", "tz": "Asia/Shanghai"},
        "payload": {
            "kind": "agent_turn",
            "message": text,
            "deliver": True,
            "channel": "test-channel",
            "to": "test-destination",
            **payload,
        },
        "state": {
            "nextRunAtMs": 123456000,
            "lastRunAtMs": 123450000,
            "lastStatus": "error",
            "lastError": "private fixture error text",
            "runHistory": [{"status": "error"}],
        },
    }


def fixture(root: Path):
    jobs = [
        job("l1", "python scripts/generate_l1.py memory/chronicle/hourly"),
        job("l2", "python scripts/generate_l2.py memory/chronicle/hourly"),
        job("l3", "python scripts/generate_l3.py memory/chronicle/weekly"),
        job("l4", "Read memory/chronicle/weekly and write memory/chronicle/monthly"),
        job("dream", "", kind="system_event"),
        job("diary", "Reflect on the day", save_response_to="diary"),
        job("weekly-reflection", "Look back at notes", inject_diary="recent_7d"),
        job("rumination", "Think about conversations", min_message_count=50),
        job("budget", "python scripts/budget_monitor.py"),
        job("business", "检查知乎反馈"),
        job("portable", "回顾今天已完成的目标，指出未验证的结果"),
    ]
    (root / "cron").mkdir(parents=True)
    (root / "cron/jobs.json").write_text(json.dumps({"version": 1, "jobs": jobs}))
    runtime = root / ".runtime/nanobot-anima"
    runtime.mkdir(parents=True)
    (runtime / "config.json").write_text(
        json.dumps(
            {
                "providers": {"fake": {"apiKey": "do-not-copy-api-key"}},
                "channels": {"test": {"token": "do-not-copy-channel-secret"}},
                "agents": {"defaults": {"dream": {"enabled": False}}},
                "gateway": {"heartbeat": {"enabled": True, "interval_s": 60}},
            }
        )
    )
    (runtime / "secrets.env").write_text("DO_NOT_READ=this")
    (root / "scripts").mkdir()
    (root / "scripts/budget_monitor.py").write_text("DO_NOT_EXECUTE_OR_READ")
    return jobs


def test_private_export_classifies_real_intent_without_reading_scripts_or_credentials(
    tmp_path, monkeypatch
):
    old = tmp_path / "old"
    fixture(old)
    original = Path.open
    reads = []

    def guarded(path, *args, **kwargs):
        assert path.suffix not in {".env", ".py"}
        reads.append(path.relative_to(old).as_posix())
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded)
    plan = export_legacy_plan(old)
    assert reads == ["cron/jobs.json", ".runtime/nanobot-anima/config.json"]
    assert plan["summary"] == {
        "legacy_job_count": 11,
        "original_enabled_count": 11,
        "actions": {
            "replace_summary": 4,
            "retire_framework": 1,
            "consolidate_autonomy": 3,
            "requires_collector": 2,
            "preserve": 1,
        },
        "new_jobs_enabled": 0,
    }
    assert [entry["replacement_level"] for entry in plan["entries"][:4]] == ["L1", "L2", "L3", "L4"]
    serialized = json.dumps(plan)
    assert "do-not-copy-api-key" not in serialized
    assert "do-not-copy-channel-secret" not in serialized
    assert "private fixture error text" not in serialized
    assert plan["heartbeat"]["proposed_interval_s"] == 60
    assert plan["heartbeat"]["cadence_changed"] is False
    assert plan["entries"][0]["delivery_metadata"]["to"] == "test-destination"
    assert plan["entries"][0]["recent_state"]["lastStatus"] == "error"
    assert list(old.glob("legacy-imports")) == []


def test_import_is_disabled_and_idempotent_without_resetting_operator_changes(tmp_path):
    old = tmp_path / "old"
    fixture(old)
    plan = export_legacy_plan(old)
    with Store(tmp_path / "new/schedules.sqlite3") as store:
        result = import_legacy_plan(store, plan)
        assert len(result["created_job_ids"]) == 6
        assert result["deferred_count"] == 3
        assert all(not item.enabled for item in store.list_jobs())
        assert store.list_events() == []
        assert store.get_job(result["autonomy_job_id"]).schedule_value == 60
        assert store.get_job(result["autonomy_job_id"]).kind == "heartbeat"
        saved = json.loads(Path(result["private_plan_path"]).read_text())
        assert saved == plan
        mapped = {entry["legacy_id"]: entry["new_job_id"] for entry in result["mappings"]}
        assert mapped["diary"] == mapped["weekly-reflection"] == mapped["rumination"]
        assert mapped["dream"] is None and mapped["budget"] is None
        store.update_job(
            mapped["portable"], enabled=True, prompt="Operator reviewed and changed task"
        )
        before = store.list_jobs()
        again = import_legacy_plan(store, plan)
        assert again["created_job_ids"] == []
        assert store.list_jobs() == before


def test_partial_import_retry_retains_created_jobs(tmp_path, monkeypatch):
    old = tmp_path / "old"
    fixture(old)
    plan = export_legacy_plan(old)
    with Store(tmp_path / "new/schedules.sqlite3") as store:
        original, calls = store.create_job, 0

        def fail_once(**kwargs):
            nonlocal calls
            calls += 1
            if calls == 3:
                raise OSError("simulated storage interruption")
            return original(**kwargs)

        monkeypatch.setattr(store, "create_job", fail_once)
        with pytest.raises(OSError):
            import_legacy_plan(store, plan)
        first = store.list_jobs()
        assert len(first) == 2
        monkeypatch.setattr(store, "create_job", original)
        result = import_legacy_plan(store, plan)
        assert len(store.list_jobs()) == 6
        assert all(store.get_job(item.id) == item for item in first)
        assert set(result["preserved_job_ids"]) == {item.id for item in first}


def test_unknown_inputs_and_tampered_plans_never_create_empty_replacements(tmp_path):
    old = tmp_path / "old"
    fixture(old)
    plan = export_legacy_plan(old)
    bad = copy.deepcopy(plan)
    bad["entries"][0]["action"] = "preserve"
    with Store(tmp_path / "new/schedules.sqlite3") as store:
        with pytest.raises(LegacyPlanError, match="integrity"):
            import_legacy_plan(store, bad)
        assert store.list_jobs() == []
    (old / "cron/jobs.json").write_text("{broken")
    with pytest.raises(LegacyPlanError, match="invalid"):
        export_legacy_plan(old)
    assert (old / "cron/jobs.json").read_text() == "{broken"
    (old / "cron/jobs.json").unlink()
    with pytest.raises(LegacyPlanError, match="missing"):
        export_legacy_plan(old)


def test_duplicate_ids_and_invalid_schedules_are_visible(tmp_path):
    old = tmp_path / "old"
    jobs = fixture(old)
    path = old / "cron/jobs.json"
    path.write_text(json.dumps({"version": 1, "jobs": [jobs[0], jobs[0]]}))
    with pytest.raises(LegacyPlanError, match="duplicate"):
        export_legacy_plan(old)
    jobs[-1]["schedule"]["expr"] = "broken cron"
    path.write_text(json.dumps({"version": 1, "jobs": [jobs[-1]]}))
    plan = export_legacy_plan(old)
    assert plan["entries"][0]["action"] == "needs_review"
    assert plan["entries"][0]["proposed_schedule"] is None


@pytest.mark.parametrize("header", [{}, {"version": 1}])
def test_legacy_cron_missing_or_version_one_remains_compatible(tmp_path, header):
    old = tmp_path / "old"
    jobs = fixture(old)
    (old / "cron/jobs.json").write_text(json.dumps({**header, "jobs": jobs}))
    assert export_legacy_plan(old)["summary"]["legacy_job_count"] == len(jobs)


@pytest.mark.parametrize("version", [2, 99, "1", None, True])
def test_unknown_explicit_legacy_cron_version_is_rejected(tmp_path, version):
    old = tmp_path / "old"
    jobs = fixture(old)
    source = old / "cron/jobs.json"
    source.write_text(json.dumps({"version": version, "jobs": jobs}))
    before = source.read_bytes()
    with pytest.raises(LegacyPlanError, match="version"):
        export_legacy_plan(old)
    assert source.read_bytes() == before
    assert not (old / "legacy-imports").exists()


def test_legacy_source_review_limit_accepts_boundary_and_rejects_larger_file(tmp_path):
    old = tmp_path / "old"
    (old / "cron").mkdir(parents=True)
    source = old / "cron/jobs.json"
    body = b'{"version":1,"jobs":[]}'
    limit = 16 * 1024 * 1024
    source.write_bytes(body + b" " * (limit - len(body)))
    assert export_legacy_plan(old)["summary"]["legacy_job_count"] == 0
    with source.open("ab") as stream:
        stream.write(b" ")
    with pytest.raises(LegacyPlanError, match="review limit"):
        export_legacy_plan(old)
    assert source.stat().st_size == limit + 1


def test_legacy_source_growth_uses_bounded_read_and_fails_without_export(tmp_path, monkeypatch):
    old = tmp_path / "old"
    (old / "cron").mkdir(parents=True)
    source = old / "cron/jobs.json"
    source.write_text('{"version":1,"jobs":[]}')
    original = Path.open
    reads = []

    @contextmanager
    def growing(path, mode="r", *args, **kwargs):
        with original(path, mode, *args, **kwargs) as stream:
            if path != source or mode != "rb":
                yield stream
                return

            class GrowingReader:
                def read(self, size=-1):
                    reads.append(size)
                    assert size == 16 * 1024 * 1024 + 1
                    with original(source, "ab") as writer:
                        for _ in range(17):
                            writer.write(b" " * 1024 * 1024)
                    return stream.read(size)

            yield GrowingReader()

    monkeypatch.setattr(Path, "open", growing)
    with pytest.raises(SourceChangedError, match="changed while exporting"):
        export_legacy_plan(old)
    assert reads == [16 * 1024 * 1024 + 1]
    assert source.stat().st_size > 16 * 1024 * 1024
