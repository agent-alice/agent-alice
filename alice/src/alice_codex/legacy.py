"""Review and import legacy scheduling intent without running old commands.

Exports contain private task text and delivery metadata. Callers must report the
summary only and keep the complete plan in private storage, never stdout/git.
"""

from collections import Counter
import hashlib
import json
import math
from pathlib import Path
import re

from .calendar import register_default_jobs
from .memory import MemoryConflictError, SourceChangedError, _atomic, _join
from .scheduler import validate_job
from .store import Job, Store


class LegacyPlanError(ValueError):
    """Legacy input needs review; it must not become an empty schedule."""


def _encoded(value) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()


def _read(path: Path) -> tuple[dict, str]:
    if path.is_symlink() or not path.is_file():
        raise LegacyPlanError("Legacy source must be a regular, non-symlink JSON file")
    before = path.stat()
    if before.st_size > 16 * 1024 * 1024:
        raise LegacyPlanError("Legacy source exceeds the review limit")
    raw = path.read_bytes()
    after = path.stat()
    if (before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    ):
        raise SourceChangedError("Legacy schedule changed while exporting")
    try:
        value = json.loads(raw)
    except (ValueError, UnicodeError) as exc:
        raise LegacyPlanError("Legacy schedule/config JSON is invalid; original preserved") from exc
    if not isinstance(value, dict):
        raise LegacyPlanError("Legacy JSON must contain an object")
    return value, hashlib.sha256(raw).hexdigest()


def _classify(job: dict) -> tuple[str, str | None, str]:
    payload = job.get("payload", {})
    text = (str(job.get("name", "")) + " " + str(payload.get("message", ""))).lower()
    explicit = re.search(r"generate_l([1-4])\.py\b", text)
    if explicit:
        return (
            "replace_summary",
            "L" + explicit[1],
            "Use frozen-source Codex Chronicle batches instead of the old summarizer",
        )
    for number, period in ((4, "monthly"), (3, "weekly"), (2, "diary"), (1, "hourly")):
        if "chronicle" in text and period in text:
            return (
                "replace_summary",
                f"L{number}",
                "Use frozen-source Codex Chronicle batches instead of the old summarizer",
            )
    if payload.get("kind") == "system_event" or re.search(
        r"\bnanobot\s|dream|autocompact|memory_consolidat", text
    ):
        return (
            "retire_framework",
            None,
            "Old runtime callback/compaction commands are not portable work",
        )
    if (
        payload.get("save_response_to")
        or payload.get("inject_diary")
        or payload.get("min_message_count")
    ):
        return (
            "consolidate_autonomy",
            None,
            "Keep reflection intent; replace implicit diary injection/save/counters with one reviewed autonomy task",
        )
    if re.search(
        r"[\w.-]+\.py\b|\bpython\d*\b|\bcurl\b|\bbash\b|budget|token_stats|capability_review|知乎|zhihu|/users/|/home/",
        text,
    ):
        return (
            "requires_collector",
            None,
            "Port and verify the business collector or script before enabling this intent",
        )
    if not str(payload.get("message", "")).strip():
        return "needs_review", None, "Empty task has no portable execution intent"
    return "preserve", None, "Portable task intent can be retained with an explicit Codex target"


def _schedule(raw: dict, default_timezone: str) -> dict:
    kind = raw.get("kind")
    if kind == "cron":
        value = raw.get("expr")
    elif kind == "every":
        value = raw.get("everyMs", raw.get("every_ms"))
        value = value / 1000 if isinstance(value, (float, int)) else None
    elif kind == "at":
        value = raw.get("atMs", raw.get("at_ms"))
        value = value / 1000 if isinstance(value, (float, int)) else None
    else:
        raise LegacyPlanError("Unsupported legacy schedule kind")
    result = {
        "schedule_type": kind,
        "schedule_value": value,
        "timezone": raw.get("tz") or default_timezone,
    }
    try:
        validate_job(Job(id="review", name="review", enabled=False, **result))
    except (TypeError, ValueError) as exc:
        raise LegacyPlanError("Invalid legacy schedule requires review") from exc
    return result


def export_legacy_plan(source_root: str | Path) -> dict:
    """Read only approved cron/config fields; return a PRIVATE review plan.

    No env/cookie/script files are opened. Provider/channel authentication fields
    are never copied into the returned plan. This function performs no writes.
    """
    root = Path(source_root).expanduser().resolve()
    if not root.is_dir():
        raise LegacyPlanError("Legacy workspace does not exist")
    candidates = [root / "cron/jobs.json", root / ".runtime/nanobot-anima/cron/jobs.json"]
    paths = [path for path in candidates if path.exists() or path.is_symlink()]
    if len(paths) > 1:
        raise LegacyPlanError("Multiple legacy cron stores require an explicit choice")
    if not paths:
        raise LegacyPlanError("Legacy cron store is missing; refusing an empty migration")
    jobs_path = paths[0]
    container, jobs_hash = _read(jobs_path)
    jobs = container.get("jobs")
    if not isinstance(jobs, list):
        raise LegacyPlanError("Legacy cron store has no jobs list")
    config_path = root / ".runtime/nanobot-anima/config.json"
    heartbeat, dream, config_hash = {}, {}, None
    if config_path.exists() or config_path.is_symlink():
        config, config_hash = _read(config_path)
        heartbeat = config.get("gateway", {}).get("heartbeat", {})
        dream = config.get("agents", {}).get("defaults", {}).get("dream", {})
        if not isinstance(heartbeat, dict) or not isinstance(dream, dict):
            raise LegacyPlanError("Invalid heartbeat/dream configuration")
    default_timezone = "Asia/Shanghai"
    entries, seen = [], set()
    for job in jobs:
        if (
            not isinstance(job, dict)
            or not isinstance(job.get("id"), str)
            or not job["id"]
            or job["id"] in seen
        ):
            raise LegacyPlanError("Invalid or duplicate legacy job identifier")
        seen.add(job["id"])
        payload, raw_schedule, state = (
            job.get("payload", {}),
            job.get("schedule", {}),
            job.get("state", {}),
        )
        if not all(isinstance(part, dict) for part in (payload, raw_schedule, state)):
            raise LegacyPlanError("Invalid legacy job structure")
        text = payload.get("message", "")
        if not isinstance(text, str):
            raise LegacyPlanError("Legacy task message must be text")
        action, level, reason = _classify(job)
        try:
            schedule = _schedule(raw_schedule, default_timezone)
        except LegacyPlanError:
            action, level, reason = (
                "needs_review",
                None,
                "Invalid legacy schedule requires manual repair",
            )
            schedule = None
        history = state.get("runHistory", state.get("run_history", [])) or []
        metadata = {key: state.get(key) for key in ("nextRunAtMs", "lastRunAtMs", "lastStatus")}
        metadata.update(
            run_history_count=len(history),
            last_error_sha256=hashlib.sha256(str(state.get("lastError", "")).encode()).hexdigest(),
        )
        schedule_metadata = {
            key: raw_schedule.get(key) for key in ("kind", "atMs", "everyMs", "expr", "tz")
        }
        entries.append(
            {
                "legacy_id": job["id"],
                "legacy_name": job.get("name", ""),
                "original_enabled": job.get("enabled", True),
                "original_schedule": schedule_metadata,
                "proposed_schedule": schedule,
                "timezone_assumed": not bool(raw_schedule.get("tz")),
                "action": action,
                "replacement_level": level,
                "reason": reason,
                "private_prompt": text,
                "prompt_sha256": hashlib.sha256(text.encode()).hexdigest(),
                "recent_state": metadata,
                "created_at_ms": job.get("createdAtMs"),
                "updated_at_ms": job.get("updatedAtMs"),
                "delivery_metadata": {
                    key: payload.get(key) for key in ("deliver", "channel", "to", "session_key")
                },
                "legacy_behavior": {
                    key: payload.get(key)
                    for key in ("kind", "save_response_to", "inject_diary", "min_message_count")
                },
                "delete_after_run": job.get("deleteAfterRun", False),
            }
        )
    interval = heartbeat.get("interval_s", heartbeat.get("intervalS"))
    if interval is not None and (
        isinstance(interval, bool)
        or not isinstance(interval, (float, int))
        or not math.isfinite(interval)
        or interval <= 0
    ):
        raise LegacyPlanError("Invalid legacy heartbeat interval")
    plan = {
        "schema_version": 1,
        "source_root": str(root),
        "jobs_path": str(jobs_path),
        "jobs_sha256": jobs_hash,
        "config_sha256": config_hash,
        "entries": entries,
        "heartbeat": {
            "original_enabled": heartbeat.get("enabled", False),
            "original_interval_s": interval,
            "proposed_interval_s": interval or 1800,
            "cadence_changed": False,
            "review_note": "Retain configured cadence; consider a lower frequency after measuring unchanged/busy suppression and budget",
            "dream_config_enabled": dream.get("enabled", False),
            "action": "consolidate_autonomy",
            "new_enabled": False,
            "requires_reviewed_heartbeat_file": True,
        },
        "summary": {
            "legacy_job_count": len(entries),
            "original_enabled_count": sum(bool(e["original_enabled"]) for e in entries),
            "actions": dict(Counter(e["action"] for e in entries)),
            "new_jobs_enabled": 0,
        },
    }
    plan["plan_sha256"] = hashlib.sha256(_encoded(plan)).hexdigest()
    return plan


def import_legacy_plan(store: Store, plan: dict) -> dict:
    """Import approved portable intent disabled; leave existing runtime state alone.

    Complete private provenance is retained beside the schedule DB. No sender,
    old script, diary injector or Dream callback is invoked by this operation.
    """
    check = dict(plan)
    supplied = check.pop("plan_sha256", None)
    if check.get("schema_version") != 1 or supplied != hashlib.sha256(_encoded(check)).hexdigest():
        raise LegacyPlanError("Legacy plan integrity check failed")
    namespace = hashlib.sha256(plan["source_root"].encode()).hexdigest()[:16]
    archive = _join(store.path.parent, f"legacy-imports/{namespace}/{supplied}.json")
    _atomic(archive, _encoded(plan))
    existing = {job.id: job for job in store.list_jobs()}
    mappings, created, preserved = [], [], []
    summary_jobs = {}
    if any(entry["action"] == "replace_summary" for entry in plan["entries"]):
        summary_jobs = {
            job.name.removeprefix("summary:"): job for job in register_default_jobs(store)
        }
        for job in summary_jobs.values():
            (preserved if job.id in existing else created).append(job.id)
    autonomy_entries = [
        entry for entry in plan["entries"] if entry["action"] == "consolidate_autonomy"
    ]
    needs_autonomy = bool(autonomy_entries or plan["heartbeat"]["original_enabled"])
    definitions = []
    autonomy_id = f"legacy-{namespace}-autonomy" if needs_autonomy else None
    if needs_autonomy:
        definitions.append(
            dict(
                job_id=autonomy_id,
                name="autonomy-review",
                schedule_type="every",
                schedule_value=plan["heartbeat"]["proposed_interval_s"],
                timezone="Asia/Shanghai",
                kind="heartbeat",
                target="main",
                prompt="读取已审核的 HEARTBEAT.md 与 .alice/prompts/autonomy-review.md。保留日记、对话反刍和周/月回顾的意图，按当前目标选择有依据的一项；不执行旧 Dream、自动注入、隐式保存或按消息计数回调。没有新情况可以安静结束。",
                catch_up=False,
            )
        )
    for entry in plan["entries"]:
        target = None
        if entry["action"] == "replace_summary":
            target = summary_jobs[entry["replacement_level"]].id
        elif entry["action"] == "consolidate_autonomy":
            target = autonomy_id
        elif entry["action"] == "preserve":
            suffix = hashlib.sha256(entry["legacy_id"].encode()).hexdigest()[:16]
            target = f"legacy-{namespace}-{suffix}"
            definitions.append(
                dict(
                    job_id=target,
                    name=entry["legacy_name"] or "legacy-task",
                    **entry["proposed_schedule"],
                    kind="task",
                    target="new",
                    catch_up=False,
                    prompt="执行已审核的迁移任务意图，使用当前 Codex/Alice 工具。旧渠道 metadata 不代表新的发送授权；不调用旧 nanobot 命令。\n\n"
                    + entry["private_prompt"],
                )
            )
        mappings.append(
            {"legacy_id": entry["legacy_id"], "action": entry["action"], "new_job_id": target}
        )
    for definition in definitions:
        old = existing.get(definition["job_id"])
        if old:
            # An explicitly enabled or edited imported job stays as the operator left it.
            if old.kind != definition["kind"] or old.target != definition["target"]:
                raise MemoryConflictError("Imported job ID collides with an unrelated runtime job")
            preserved.append(old.id)
        else:
            created.append(store.create_job(enabled=False, **definition).id)
    receipt = {
        "plan_sha256": supplied,
        "private_plan_path": str(archive),
        "created_job_ids": created,
        "preserved_job_ids": preserved,
        "mappings": mappings,
        "autonomy_job_id": autonomy_id,
        "deferred_count": sum(item["new_job_id"] is None for item in mappings),
        "new_jobs_enabled": 0,
    }
    _atomic(archive.with_suffix(".receipt.json"), _encoded(receipt))
    return receipt
