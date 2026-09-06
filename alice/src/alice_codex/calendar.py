"""Expand durable summary dispatch ranges into every closed Chronicle period.

This planner does not run a model or claim a generated candidate was committed.
Its persisted plan is a bounded task manifest, not a Codex input queue.
"""

from dataclasses import asdict
from collections.abc import Iterable
import datetime as dt
import hashlib
import json
import math
from zoneinfo import ZoneInfo

from .memory import MemoryStore, NoSourcesError, _atomic, _join, _period_bounds
from .store import DispatchEvent, Job, Store


class CalendarError(ValueError):
    """A summary calendar cannot be interpreted without losing periods."""


class UnclosedSummaryDependency(CalendarError):
    """A closed month still needs its overlapping final week to close."""

    def __init__(self, available_at: dt.datetime):
        self.available_at = available_at
        super().__init__(f"Summary dependency period remains open until {available_at.isoformat()}")


SCHEDULES = {"L1": "5 */2 * * *", "L2": "15 0 * * *", "L3": "25 0 * * 1", "L4": "35 0 1 * *"}


def summary_level(job: Job) -> str | None:
    labels = [value for value in (job.name, job.target) if value.startswith("summary:")]
    if not labels:
        return None
    levels = {label.removeprefix("summary:") for label in labels}
    if job.kind != "task" or len(levels) != 1 or not levels <= SCHEDULES.keys():
        raise CalendarError("Summary job requires kind=task and one matching summary:L1..L4 label")
    return levels.pop()


def register_default_jobs(
    store: Store,
    *,
    now: float | None = None,
    timezone: str = "Asia/Shanghai",
    enabled: bool = False,
) -> list[Job]:
    """Install missing calendar definitions; retain existing flags and revisions.

    New definitions default to disabled until cutover. Existing schedules are
    never silently reset or re-enabled by another initialization.
    """
    ZoneInfo(timezone)
    existing = {job.id: job for job in store.list_jobs()}
    definitions = []
    for level, schedule in SCHEDULES.items():
        definition = dict(
            job_id=f"anima-summary-{level}",
            name=f"summary:{level}",
            target=f"summary:{level}",
            kind="task",
            schedule_type="cron",
            schedule_value=schedule,
            timezone=timezone,
            prompt="Prepare and commit all closed Chronicle periods in this dispatch range.",
            enabled=enabled,
            catch_up=True,
        )
        prior = existing.get(definition["job_id"])
        if prior and (
            summary_level(prior) != level
            or prior.schedule_type != "cron"
            or prior.schedule_value != schedule
            or prior.timezone != timezone
        ):
            raise CalendarError("Existing summary schedule differs; migrate it explicitly")
        definitions.append(definition)
    return [
        existing[d["job_id"]] if d["job_id"] in existing else store.create_job(now=now, **d)
        for d in definitions
    ]


def _floor_end(level: str, value: dt.datetime) -> dt.datetime:
    day = value.replace(hour=0, minute=0, second=0, microsecond=0)
    if level == "L1":
        return day.replace(hour=value.hour // 2 * 2)
    if level == "L2":
        return day
    if level == "L3":
        return day - dt.timedelta(days=day.weekday())
    return day.replace(day=1)


def _move(level: str, value: dt.datetime, direction: int) -> dt.datetime:
    if level != "L4":
        delta = {
            "L1": dt.timedelta(hours=2),
            "L2": dt.timedelta(days=1),
            "L3": dt.timedelta(days=7),
        }[level]
        return value + direction * delta
    year, month = divmod(value.year * 12 + value.month - 1 + direction, 12)
    return value.replace(year=year, month=month + 1)


def closed_periods(
    event: DispatchEvent, *, now: dt.datetime | None = None, max_periods: int = 4096
) -> list[str]:
    """Return every period closed by the inclusive due_at..through_at range.

    catch_up is informational here: a coalesced range must never lose its earlier
    periods, even if a manually constructed event did not set that flag.
    """
    level = summary_level(event.job)
    if level is None:
        raise CalendarError("Dispatch does not belong to a summary calendar")
    if (
        not math.isfinite(event.due_at)
        or not math.isfinite(event.through_at)
        or event.through_at < event.due_at
        or max_periods < 1
    ):
        raise CalendarError("Invalid dispatch time range")
    current = now or dt.datetime.now(dt.timezone.utc)
    if current.tzinfo is None or event.through_at > current.timestamp():
        raise CalendarError("Dispatch extends beyond the current time")
    zone = ZoneInfo(event.job.timezone)
    first_end = _floor_end(level, dt.datetime.fromtimestamp(event.due_at, zone))
    last_end = _floor_end(level, dt.datetime.fromtimestamp(event.through_at, zone))
    result = []
    end = first_end
    while end <= last_end:
        start = _move(level, end, -1)
        result.append(
            start.strftime(
                {"L1": "%Y-%m-%dT%H:%M", "L2": "%Y-%m-%d", "L3": "%G-W%V", "L4": "%Y-%m"}[level]
            )
        )
        if len(result) > max_periods:
            raise CalendarError(
                "Dispatch needs explicit range partitioning; no periods were dropped"
            )
        end = _move(level, end, 1)
    return result


def summary_dependencies(
    event: DispatchEvent,
    events: Iterable[DispatchEvent],
    *,
    now: dt.datetime | None = None,
    max_periods: int = 4096,
) -> list[DispatchEvent]:
    """Return persisted lower-level work blocking this event's source windows.

    Compare actual half-open time intervals, never dispatch deadlines or all
    historical failures. Monthly inputs include whole overlapping weeks, just
    as MemoryStore selects weekly sources. An open final week must defer even
    when its occurrence has not yet been created.

    Completed occurrences can cover failed/cancelled occurrences only for each
    entire relevant lower-level period. In-flight work always blocks freezing;
    an older completion cannot prove its new writes are finished. This checks
    persisted occurrences, not source versions or the existence of every
    expected historical occurrence. The caller must retain pending work when
    UnclosedSummaryDependency is raised and independently verify commit receipts.
    """
    level = summary_level(event.job)
    if level is None:
        return []
    current = now or dt.datetime.now(dt.timezone.utc)
    periods = closed_periods(event, now=current, max_periods=max_periods)
    if level == "L1":
        return []
    start = _period_bounds(level, periods[0], event.job.timezone)[0]
    end = _period_bounds(level, periods[-1], event.job.timezone)[1]
    if level == "L4":
        start = _floor_end("L3", start)
        week_end = _floor_end("L3", end)
        end = week_end if end == week_end else _move("L3", week_end, 1)
        if end.timestamp() > current.timestamp():
            raise UnclosedSummaryDependency(end.astimezone(dt.timezone.utc))
    target_start, target_end = start.timestamp(), end.timestamp()
    rank = {name: index for index, name in enumerate(SCHEDULES)}
    relevant = []
    completed: dict[str, set[tuple[float, float]]] = {}
    for candidate in events:
        lower = summary_level(candidate.job)
        if lower is None or rank[lower] >= rank[level]:
            continue
        if (
            math.isfinite(candidate.due_at)
            and math.isfinite(candidate.through_at)
            and candidate.through_at >= candidate.due_at
        ):
            zone = ZoneInfo(candidate.job.timezone)
            first = _floor_end(lower, dt.datetime.fromtimestamp(candidate.due_at, zone))
            last = _floor_end(lower, dt.datetime.fromtimestamp(candidate.through_at, zone))
            # Exclude unrelated ranges before expanding them: an old oversized
            # catch-up must not exhaust the current window's partition budget.
            if _move(lower, first, -1).timestamp() >= target_end or last.timestamp() <= target_start:
                continue
        windows = set()
        for period in closed_periods(candidate, now=current, max_periods=max_periods):
            bounds = _period_bounds(lower, period, candidate.job.timezone)
            begin, finish = (value.timestamp() for value in bounds)
            if begin < target_end and finish > target_start:
                windows.add((begin, finish))
        if not windows:
            continue
        if candidate.status == "completed":
            completed.setdefault(lower, set()).update(windows)
        else:
            relevant.append((candidate, lower, windows))
    return [
        candidate
        for candidate, lower, windows in relevant
        if candidate.status not in {"failed", "cancelled"}
        or not windows <= completed.get(lower, set())
    ]


def prepare_dispatch(
    event: DispatchEvent, memory: MemoryStore, *, now: dt.datetime | None = None
) -> dict:
    """Freeze each period, recording only a real no-source condition as skipped.

    Source corruption, changed inputs, oversized batches and other validation
    errors propagate. No partial successful plan is emitted on those failures.
    """
    current = now or dt.datetime.now(dt.timezone.utc)
    level = summary_level(event.job)
    periods = closed_periods(event, now=current)
    prepared, skipped = [], []
    for period in periods:
        try:
            batch = memory.prepare_summary(level, period, now=current, timezone=event.job.timezone)
        except NoSourcesError:
            skipped.append({"period": period, "reason": "no_sources", "status": "skipped"})
        else:
            prepared.append({"level": level, "period": period, **batch})
    plan = {
        "schema_version": 1,
        "event_id": event.id,
        "level": level,
        "due_at": event.due_at,
        "through_at": event.through_at,
        "timezone": event.job.timezone,
        "job": asdict(event.job),
        "periods": periods,
        "prepared": prepared,
        "skipped": skipped,
    }
    raw = (json.dumps(plan, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()
    digest = hashlib.sha256(raw).hexdigest()
    event_key = hashlib.sha256(event.id.encode()).hexdigest()
    plan_path = _join(memory.state, f"calendar/{event_key}/{digest}.json")
    _atomic(plan_path, raw)
    prompt = (
        f"这是 Alice 经历整理派发 {event.id}，包含 {len(periods)} 个已关闭期间，"
        f"其中 {len(prepared)} 个有来源、{len(skipped)} 个已记录无来源。\n"
        f"先读取任务清单 {plan_path} 的元数据；用脚本按索引逐个读取 prepared，"
        "不要把整份清单或所有来源一次塞入上下文。按时间顺序执行每项 prompt，"
        "写入指定候选后使用 Alice memory_commit_summary 工具提交其 batch_id 和候选。"
        "遇到缺口保留来源与原因，不能只处理最后一个期间。全部有来源的批次都提交后才能报告完成；"
        "写出候选或结束模型回合均不等于完成。不得修改冻结清单或原始档案。"
    )
    return {**plan, "plan_path": str(plan_path), "plan_sha256": digest, "prompt": prompt}


def dispatch_completion(plan: dict, memory: MemoryStore) -> dict:
    """Check durable commit receipts, rather than accepting model self-report."""
    committed, pending = [], []
    for batch in plan["prepared"]:
        batch_id = batch["batch_id"]
        if (
            not isinstance(batch_id, str)
            or len(batch_id) != 64
            or any(c not in "0123456789abcdef" for c in batch_id)
        ):
            raise CalendarError("Invalid batch in dispatch plan")
        receipt_path = _join(memory.state, f"commits/{batch_id}.json")
        if not receipt_path.exists():
            pending.append(batch_id)
            continue
        receipt = json.loads(receipt_path.read_text())
        if receipt.get("batch_id") != batch_id or receipt.get("status") not in {
            "committed",
            "pending",
        }:
            raise CalendarError("Invalid summary commit receipt")
        (committed if receipt["status"] == "committed" else pending).append(batch_id)
    return {
        "complete": not pending,
        "committed_batch_ids": committed,
        "pending_batch_ids": pending,
        "skipped": plan["skipped"],
    }
