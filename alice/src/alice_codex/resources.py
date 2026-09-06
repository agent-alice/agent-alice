"""Observed resources and optional business rules, separate from Codex billing.

Native cumulative token counters are observations, not dollar invoices. The
default policy records evidence with virtual enforcement disabled.
Only explicit rule/baseline configuration can enable virtual accounting.
"""

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import time
from zoneinfo import ZoneInfo

from .business import summarize_observation
from .files import private_dir


class ResourceError(RuntimeError):
    pass


TOKEN_FIELDS = (
    "inputTokens",
    "cachedInputTokens",
    "outputTokens",
    "reasoningOutputTokens",
    "totalTokens",
)


def _json(value) -> str:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )


def _text(value, name):
    if not isinstance(value, str) or not value.strip() or len(value) > 2000:
        raise ValueError(f"{name} must be nonempty bounded text")
    return value


def _integer(value, name, *, negative=False):
    if type(value) is not int or (value < 0 and not negative) or abs(value) > 2**53:
        raise ValueError(f"{name} must be a bounded integer")
    return value


def _timestamp(value):
    value = time.time() if value is None else value
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError("observed_at must be a finite timestamp")
    return value


def _rule(value):
    if not isinstance(value, dict):
        raise ValueError("rule must be an explicit object")
    required = {
        "effective_date",
        "timezone",
        "subject",
        "collection",
        "metric",
        "baseline_period",
        "baseline_count",
        "daily_base_microusd",
        "likes_per_usd",
        "rounding",
        "negative_delta",
        "require_independent",
    }
    if set(value) != required:
        raise ValueError("rule requires every settlement, evidence and baseline field")
    for key in ("subject", "collection", "metric"):
        _text(value[key], key)
    start, baseline = (
        date.fromisoformat(value["effective_date"]),
        date.fromisoformat(value["baseline_period"]),
    )
    if baseline < start - timedelta(days=1):
        raise ValueError("baseline must precede the first settlement by at most one day")
    ZoneInfo(value["timezone"])
    for key in ("baseline_count", "daily_base_microusd"):
        _integer(value[key], key)
    stages = value["likes_per_usd"]
    if not isinstance(stages, list) or not stages:
        raise ValueError("exchange stages must be explicit")
    previous = -1
    for stage in stages:
        if not isinstance(stage, dict) or set(stage) != {"start_day", "likes"}:
            raise ValueError("invalid exchange stage")
        day, likes = _integer(stage["start_day"], "start_day"), _integer(stage["likes"], "likes")
        if day <= previous or likes == 0:
            raise ValueError("exchange stages must increase and use positive denominators")
        previous = day
    if stages[0]["start_day"] != 0:
        raise ValueError("exchange stages must begin at day zero")
    if (
        value["rounding"] != "floor_micro_usd"
        or value["negative_delta"] != "hold"
        or type(value["require_independent"]) is not bool
    ):
        raise ValueError(
            "explicit supported rounding, negative-delta and evidence policies are required"
        )
    return json.loads(_json(value))


def _account_state(response, limit_id):
    if not isinstance(response, dict):
        return "unknown", ["rate_limits_unavailable"]
    buckets = response.get("rateLimitsByLimitId")
    snapshot = buckets.get(limit_id) if isinstance(buckets, dict) else response.get("rateLimits")
    reasons = (
        ["native_ordinary_usage_disallowed"]
        if response.get("ordinaryUsageAllowed") is False
        else []
    )
    if not isinstance(snapshot, dict):
        return (
            ("exhausted", reasons)
            if reasons
            else ("unknown", ["requested_limit_bucket_unavailable"])
        )
    if snapshot.get("spendControlReached") is True:
        reasons.append("native_spend_control_reached")
    windows = [snapshot.get(key) for key in ("primary", "secondary")]
    percentages = [window.get("usedPercent") for window in windows if isinstance(window, dict)]
    valid = [
        value
        for value in percentages
        if type(value) in (int, float) and math.isfinite(value) and value >= 0
    ]
    if any(value >= 100 for value in valid):
        credits = snapshot.get("credits") or {}
        if not reasons and (credits.get("hasCredits") is True or credits.get("unlimited") is True):
            return "unknown", ["included_window_exhausted_credit_eligibility_unknown"]
        reasons.extend(
            f"native_{key}_window_exhausted"
            for key in ("primary", "secondary")
            if isinstance(snapshot.get(key), dict)
            and type(snapshot[key].get("usedPercent")) in (int, float)
            and snapshot[key]["usedPercent"] >= 100
        )
    if reasons:
        return "exhausted", reasons
    if response.get("ordinaryUsageAllowed") is True or (valid and len(valid) == len(percentages)):
        return "available", []
    return "unknown", ["native_rate_window_unknown"]


def _block_cleared(response, limit_id, reason):
    """An absent field cannot clear a previously reported block on that field."""
    if not isinstance(response, dict):
        return False
    if reason == "native_ordinary_usage_disallowed":
        return response.get("ordinaryUsageAllowed") is True
    buckets = response.get("rateLimitsByLimitId")
    snapshot = buckets.get(limit_id) if isinstance(buckets, dict) else response.get("rateLimits")
    if not isinstance(snapshot, dict):
        return False
    if reason == "native_spend_control_reached":
        return snapshot.get("spendControlReached") is False
    for key in ("primary", "secondary"):
        if reason == f"native_{key}_window_exhausted":
            window = snapshot.get(key)
            value = window.get("usedPercent") if isinstance(window, dict) else None
            return type(value) in (int, float) and math.isfinite(value) and 0 <= value < 100
    return False


@dataclass(frozen=True, slots=True)
class TaskUsage:
    """Caller-owned facts for one bounded task, independent of money and tokens.

    The scheduler must persist the incremented attempt and ``running`` outcome
    before dispatch, then record a confirmed terminal observation. ``complete``
    means the task's independent result check passed, not merely HTTP/RPC success.
    Times are UTC epoch seconds, never a process-local monotonic clock. Reload
    these facts after restart; extending limits must not reset them. After every
    decision persist last_checked_at = max(previous last_checked_at, observed_at),
    including suppressed checks; never lower the saved watermark on clock rollback.
    """

    started_at: float
    attempts: int = 0
    consecutive_failures: int = 0
    last_outcome: str | None = None
    last_finished_at: float | None = None
    last_checked_at: float | None = None

    def __post_init__(self):
        if self.started_at is None:
            raise ValueError("started_at must be an explicit timestamp")
        _timestamp(self.started_at)
        if self.last_checked_at is not None:
            _timestamp(self.last_checked_at)
            if self.last_checked_at < self.started_at:
                raise ValueError("last check cannot precede task start")
        _integer(self.attempts, "attempts")
        _integer(self.consecutive_failures, "consecutive_failures")
        if self.consecutive_failures > self.attempts:
            raise ValueError("consecutive failures cannot exceed attempts")
        outcomes = {"running", "progress", "unchanged", "failed", "unknown", "complete"}
        if self.last_outcome is not None and (
            not isinstance(self.last_outcome, str) or self.last_outcome not in outcomes
        ):
            raise ValueError("unsupported task outcome")
        if self.attempts == 0:
            if self.last_outcome is not None or self.last_finished_at is not None:
                raise ValueError("an unstarted task cannot have an attempt outcome")
        elif self.last_outcome is None:
            raise ValueError("an attempted task requires an explicit outcome")
        if self.last_outcome == "running":
            if self.last_finished_at is not None:
                raise ValueError("a running attempt cannot have a finish timestamp")
        elif self.last_outcome is not None:
            if self.last_finished_at is None:
                raise ValueError("an attempt outcome requires a finish timestamp")
            _timestamp(self.last_finished_at)
            if self.last_finished_at < self.started_at:
                raise ValueError("finish timestamp cannot precede task start")
        if self.last_outcome == "failed" and self.consecutive_failures == 0:
            raise ValueError("a failed outcome requires a consecutive failure count")
        if self.last_outcome in {"progress", "unchanged", "complete"} and self.consecutive_failures:
            raise ValueError("a confirmed nonfailure must clear the consecutive failure count")


def _positive_seconds(value, name):
    if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be positive finite seconds")
    return value


@dataclass(frozen=True, slots=True)
class TaskPolicy:
    """Pure pre-dispatch decision; this class does not run or persist a task.

    All limits are explicit. Wall time includes waiting, so unchanged observations
    cannot create an unlimited idle loop. ``max_retries`` permits that many extra
    attempts following consecutive confirmed failures. Unknown results always need
    reconciliation, including after limits are extended. A caller can recover an
    exhausted task by explicitly extending its limits while keeping all usage facts.

    The service/store owners must atomically persist usage, honor wait decisions,
    combine this result with ResourceLedger.can_dispatch, and enforce the deadline
    on an already running native turn. A decision neither grants spending/publishing
    authorization nor interrupts an existing process. No storage format is added.
    """

    max_elapsed_seconds: float
    max_attempts: int
    max_retries: int
    retry_wait_seconds: float
    unchanged_wait_seconds: float

    def __post_init__(self):
        _positive_seconds(self.max_elapsed_seconds, "max_elapsed_seconds")
        _positive_seconds(self.retry_wait_seconds, "retry_wait_seconds")
        _positive_seconds(self.unchanged_wait_seconds, "unchanged_wait_seconds")
        if _integer(self.max_attempts, "max_attempts") == 0:
            raise ValueError("max_attempts must be positive")
        _integer(self.max_retries, "max_retries")

    def decide(self, usage: TaskUsage, *, now: float, busy: bool) -> dict:
        """Check one step without consuming an attempt or changing the caller's facts.

        Example: TaskPolicy(120, 4, 1, 5, 30).decide(
            TaskUsage(100, 1, 1, "failed", 105), now=106, busy=False)
        returns waiting/retry_wait with next_attempt_at=110. There are no default
        limits. now must be an injected UTC epoch timestamp, not monotonic time.

        States/reasons: ready has allowed=True; complete never dispatches again;
        reconciliation_required/attempt_outcome_unknown requires independent
        reconciliation before retry, even after limits are extended. waiting has
        task_busy, clock_before_task_evidence, retry_wait, or unchanged_wait.
        exhausted has task_time_exhausted, task_attempts_exhausted and/or
        task_retries_exhausted, or task_time_insufficient_for_wait. All exhausted
        decisions require explicit recovery and preserve the supplied usage.
        Attempt/retry limits only restrict the next dispatch: the final admitted
        attempt can stay busy until its time budget expires. An active task whose
        clock watermark has reached the deadline remains exhausted after rollback.

        next_attempt_at is present for a scheduled wait, absent for busy or blocked
        states. remaining reports time, total attempts, and additional consecutive
        failure retries. observed_at is the clock/evidence high-water mark to save
        as last_checked_at before the next evaluation; it never goes backwards.
        """
        if not isinstance(usage, TaskUsage):
            raise ValueError("usage must contain validated TaskUsage facts")
        if now is None:
            raise ValueError("now must be an explicit timestamp")
        _timestamp(now)
        if type(busy) is not bool:
            raise ValueError("busy must be an explicit boolean")
        deadline = usage.started_at + self.max_elapsed_seconds
        if not math.isfinite(deadline):
            raise ValueError("task deadline must be finite")
        observed_at = max(
            now,
            usage.started_at,
            usage.started_at if usage.last_finished_at is None else usage.last_finished_at,
            usage.started_at if usage.last_checked_at is None else usage.last_checked_at,
        )
        remaining = {
            "seconds": max(0, min(self.max_elapsed_seconds, deadline - observed_at)),
            "attempts": max(0, self.max_attempts - usage.attempts),
            "retries": max(0, self.max_retries - max(0, usage.consecutive_failures - 1)),
        }

        def decision(state, *reasons, next_attempt_at=None, recovery_required=False):
            return {
                "allowed": state == "ready",
                "observed_at": observed_at,
                "state": state,
                "reasons": list(reasons),
                "next_attempt_at": next_attempt_at,
                "remaining": remaining,
                "recovery_required": recovery_required,
            }

        if usage.last_outcome == "complete":
            return decision("complete")
        if busy and remaining["seconds"] == 0:
            return decision("exhausted", "task_time_exhausted", recovery_required=True)
        if usage.last_outcome == "unknown" or (usage.last_outcome == "running" and not busy):
            return decision(
                "reconciliation_required", "attempt_outcome_unknown", recovery_required=True
            )
        if now < observed_at:
            return decision("waiting", "clock_before_task_evidence", next_attempt_at=observed_at)
        if busy:
            return decision("waiting", "task_busy")
        exhausted = []
        if remaining["seconds"] == 0:
            exhausted.append("task_time_exhausted")
        if remaining["attempts"] == 0:
            exhausted.append("task_attempts_exhausted")
        if usage.last_outcome == "failed" and usage.consecutive_failures > self.max_retries:
            exhausted.append("task_retries_exhausted")
        if exhausted:
            return decision("exhausted", *exhausted, recovery_required=True)
        delays = {
            "failed": (self.retry_wait_seconds, "retry_wait"),
            "unchanged": (self.unchanged_wait_seconds, "unchanged_wait"),
        }
        if usage.last_outcome in delays:
            delay, reason = delays[usage.last_outcome]
            next_attempt_at = usage.last_finished_at + delay
            if not math.isfinite(next_attempt_at):
                raise ValueError("task wait deadline must be finite")
            if now < next_attempt_at:
                if next_attempt_at >= deadline:
                    return decision(
                        "exhausted", "task_time_insufficient_for_wait", recovery_required=True
                    )
                return decision("waiting", reason, next_attempt_at=next_attempt_at)
        return decision("ready")


class ResourceLedger:
    SCHEMA_VERSION = 1

    def __init__(self, path: str | Path):
        self.path = Path(path).expanduser().resolve()
        private_dir(self.path.parent)
        with self._db() as db:
            if db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise ResourceError("resource database is damaged; preserved for recovery")
            version = db.execute("PRAGMA user_version").fetchone()[0]
            tables = {
                row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
            if version not in (0, self.SCHEMA_VERSION) or (version == 0 and tables):
                raise ResourceError("unsupported resource database schema; preserved for recovery")
            expected = {"settings", "tokens", "checkpoints", "observations", "money", "settlements"}
            if version == self.SCHEMA_VERSION and not expected <= tables:
                raise ResourceError("resource database is incomplete; preserved for recovery")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS tokens(id TEXT PRIMARY KEY, fingerprint TEXT UNIQUE NOT NULL,
                    thread TEXT NOT NULL, turn TEXT NOT NULL, payload TEXT NOT NULL, state TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS checkpoints(thread TEXT PRIMARY KEY, counters TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS observations(id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL,
                    period TEXT NOT NULL, summary TEXT NOT NULL, observed_at REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS money(seq INTEGER PRIMARY KEY, id TEXT UNIQUE NOT NULL,
                    kind TEXT NOT NULL, amount INTEGER NOT NULL, source TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS settlements(period TEXT PRIMARY KEY, receipt_id TEXT NOT NULL,
                    count INTEGER NOT NULL, amount INTEGER NOT NULL);
            """)
            db.execute(f"PRAGMA user_version={self.SCHEMA_VERSION}")
        self.path.chmod(0o600)

    @contextmanager
    def _db(self):
        db = None
        try:
            db = sqlite3.connect(self.path, timeout=10)
            db.row_factory = sqlite3.Row
            db.execute("BEGIN IMMEDIATE")
            yield db
            db.commit()
        except sqlite3.Error as exc:
            if db is not None:
                db.rollback()
            raise ResourceError(f"resource persistence failed: {exc}") from exc
        finally:
            if db is not None:
                db.close()

    @staticmethod
    def _get(db, key, default=None):
        row = db.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    @staticmethod
    def _put(db, key, value):
        db.execute(
            "INSERT INTO settings VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, _json(value)),
        )

    def import_rule_reference(self, reference: dict, *, receipt_id: str, source: str) -> dict:
        """Import bounded private evidence once, without configuring any budget.

        The caller supplies the document explicitly; no files are discovered or
        read here. Source and content hash preserve provenance. Replacement
        requires a separate data migration instead of overwriting this record.
        """
        receipt_id, source = _text(receipt_id, "receipt_id"), _text(source, "source")
        if not isinstance(reference, dict) or not reference:
            raise ValueError("reference must be a nonempty JSON object")
        try:
            encoded = _json(reference).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise ValueError("reference must contain finite JSON values") from exc
        if len(encoded) > 8192:
            raise ValueError("reference exceeds its bounded document size")
        record = {
            "status": "reference_only",
            "receipt_id": receipt_id,
            "source": source,
            "reference_sha256": hashlib.sha256(encoded).hexdigest(),
            "reference": json.loads(encoded),
        }
        with self._db() as db:
            previous = self._get(db, "historical_rule_reference")
            if previous is not None:
                if previous != record:
                    raise ResourceError(
                        "resource reference already imported; explicit migration required"
                    )
                return {
                    "imported": False,
                    "receipt_id": receipt_id,
                    "reference_sha256": record["reference_sha256"],
                }
            self._put(db, "historical_rule_reference", record)
        return {
            "imported": True,
            "receipt_id": receipt_id,
            "reference_sha256": record["reference_sha256"],
        }

    def configure_virtual_budget(
        self, rule: dict, *, opening_balance_microusd: int, confirmation_id: str
    ):
        """Store an explicitly confirmed baseline; configuration remains disabled."""
        value = {
            "rule": _rule(rule),
            "opening_balance_microusd": _integer(
                opening_balance_microusd, "opening balance", negative=True
            ),
            "confirmation_id": _text(confirmation_id, "confirmation_id"),
        }
        with self._db() as db:
            previous = self._get(db, "budget")
            if previous:
                if any(previous[key] != item for key, item in value.items()):
                    raise ResourceError(
                        "budget baseline already configured; explicit migration is required to replace it"
                    )
                return previous
            value["money_baseline_seq"] = db.execute(
                "SELECT COALESCE(MAX(seq),0) FROM money"
            ).fetchone()[0]
            self._put(db, "budget", value)
            self._put(db, "virtual_budget_enabled", False)
        return value

    def set_virtual_budget_enabled(self, enabled: bool, *, confirmation_id: str):
        if type(enabled) is not bool:
            raise ValueError("enabled must be an explicit boolean")
        _text(confirmation_id, "confirmation_id")
        with self._db() as db:
            if enabled and not self._get(db, "budget"):
                raise ResourceError("confirmed rule and opening baseline are required")
            self._put(db, "virtual_budget_enabled", enabled)
            self._put(db, "enable_confirmation", confirmation_id)

    def record_money(self, receipt_id: str, *, kind: str, amount_microusd: int, source: str):
        """Record an explicit income/cost receipt, never a token-to-dollar guess."""
        if kind not in {"income", "cost"}:
            raise ValueError("money kind must be income or cost")
        values = (
            _text(receipt_id, "receipt_id"),
            kind,
            _integer(amount_microusd, "amount"),
            _text(source, "source"),
        )
        with self._db() as db:
            previous = db.execute(
                "SELECT id,kind,amount,source FROM money WHERE id=?", (receipt_id,)
            ).fetchone()
            if previous:
                if tuple(previous) != values:
                    raise ResourceError("money receipt ID identifies conflicting evidence")
                return {"recorded": False, "receipt_id": receipt_id}
            db.execute("INSERT INTO money(id,kind,amount,source) VALUES (?,?,?,?)", values)
        return {"recorded": True, "receipt_id": receipt_id}

    def record_token_usage(self, params: dict, *, event_id: str | None = None):
        """Persist native thread/tokenUsage/updated params using cumulative high-water marks.

        The first counter may include pre-migration or fork-inherited history;
        it is not an invoice, and `last` must never be added to `total`.
        """
        if not isinstance(params, dict):
            raise ValueError("native token notification params must be an object")
        thread, turn = (
            _text(params.get("threadId"), "threadId"),
            _text(params.get("turnId"), "turnId"),
        )
        payload = _json(params)
        fingerprint = hashlib.sha256(payload.encode()).hexdigest()
        identity = _text(event_id or fingerprint, "event_id")
        usage = params.get("tokenUsage")
        total = usage.get("total") if isinstance(usage, dict) else None
        known = isinstance(total, dict) and all(
            type(total.get(key)) is int and 0 <= total[key] <= 2**53 for key in TOKEN_FIELDS
        )
        state, increase = "unknown", None
        with self._db() as db:
            previous = db.execute(
                "SELECT fingerprint,state FROM tokens WHERE id=?", (identity,)
            ).fetchone()
            if previous:
                if previous["fingerprint"] != fingerprint:
                    raise ResourceError("token event ID identifies conflicting evidence")
                return {"recorded": False, "state": previous["state"], "increase": None}
            repeated = db.execute(
                "SELECT state FROM tokens WHERE fingerprint=?", (fingerprint,)
            ).fetchone()
            if repeated:
                return {"recorded": False, "state": repeated["state"], "increase": None}
            if known:
                counters = {key: total[key] for key in TOKEN_FIELDS}
                cache_write = total.get("cacheWriteInputTokens")
                counters["cacheWriteInputTokens"] = (
                    cache_write if type(cache_write) is int and cache_write >= 0 else None
                )
                row = db.execute(
                    "SELECT counters FROM checkpoints WHERE thread=?", (thread,)
                ).fetchone()
                old = json.loads(row[0]) if row else None
                if old and any(counters[key] < old[key] for key in TOKEN_FIELDS):
                    state = "out_of_order_or_counter_reset"
                else:
                    state = "known"
                    increase = (
                        {key: counters[key] - old[key] for key in TOKEN_FIELDS} if old else None
                    )
                    db.execute(
                        "INSERT INTO checkpoints VALUES (?,?) ON CONFLICT(thread) DO UPDATE SET counters=excluded.counters",
                        (thread, _json(counters)),
                    )
            db.execute(
                "INSERT INTO tokens VALUES (?,?,?,?,?,?)",
                (identity, fingerprint, thread, turn, payload, state),
            )
        return {"recorded": True, "state": state, "increase": increase}

    def record_rate_limits(self, response: dict | None, *, observed_at: float | None = None):
        observed_at = _timestamp(observed_at)
        # Store only the native quota view: no account identities or auth material.
        value = (
            {
                key: response.get(key)
                for key in ("rateLimits", "rateLimitsByLimitId", "ordinaryUsageAllowed")
            }
            if isinstance(response, dict)
            else None
        )
        with self._db() as db:
            previous = self._get(db, "rate_limits")
            if previous and observed_at < previous["observed_at"]:
                return {"recorded": False, "reason": "older_snapshot"}
            self._put(db, "rate_limits", {"response": value, "observed_at": observed_at})
            latched = self._get(db, "quota_blocks", {})
            keys = set((value or {}).get("rateLimitsByLimitId") or {}) | set(latched) | {"codex"}
            for key in keys:
                state, reasons = _account_state(value, key)
                remaining = [
                    reason
                    for reason in latched.get(key, [])
                    if not _block_cleared(value, key, reason)
                ]
                if state == "exhausted":
                    remaining.extend(reasons)
                if remaining:
                    latched[key] = sorted(set(remaining))
                else:
                    latched.pop(key, None)
            self._put(db, "quota_blocks", latched)
        return {"recorded": True}

    def record_observation(
        self, receipt_id: str, document: dict, *, period: str, now: float | None = None
    ):
        """Persist collector evidence; partial/unknown observations never earn credit."""
        _text(receipt_id, "receipt_id")
        date.fromisoformat(period)
        now = _timestamp(now)
        summary = summarize_observation(document)
        fingerprint = hashlib.sha256(
            _json({"document": document, "period": period}).encode()
        ).hexdigest()
        with self._db() as db:
            previous = db.execute(
                "SELECT fingerprint FROM observations WHERE id=?", (receipt_id,)
            ).fetchone()
            if previous:
                if previous[0] != fingerprint:
                    raise ResourceError("observation receipt ID identifies conflicting evidence")
                return {"recorded": False, "settled": False, "summary": summary}
            db.execute(
                "INSERT INTO observations VALUES (?,?,?,?,?)",
                (receipt_id, fingerprint, period, _json(summary), now),
            )
            result = self._settle(db, receipt_id, period, summary, document, now)
        return {"recorded": True, "summary": summary, **result}

    def _settle(self, db, receipt_id, period, summary, document, now):
        budget = self._get(db, "budget")
        if not budget or not self._get(db, "virtual_budget_enabled", False):
            return {"settled": False, "reason": "virtual_budget_disabled"}
        rule = budget["rule"]
        metric = summary["metrics"].get(rule["metric"], {})
        independent = summary["coverage"]
        valid = (
            summary["complete"]
            and metric.get("state") == "known"
            and summary["subject"] == rule["subject"]
            and summary["collection"] == rule["collection"]
            and (
                not rule["require_independent"]
                or (
                    independent["independent_comparison"] == "compared"
                    and independent["independent_coverage"] == "complete"
                )
            )
        )
        if not valid:
            return {"settled": False, "reason": "observation_unknown_or_scope_unverified"}
        if rule["require_independent"] and any(
            type(row.get(rule["metric"])) is not int or row[rule["metric"]] < 0
            for row in document["independent"]["items"]
        ):
            return {"settled": False, "reason": "independent_metric_unknown"}
        sources = [*summary["sources"]]
        if document.get("independent"):
            sources.append(document["independent"])
        for source in sources:
            try:
                observed = datetime.fromisoformat(source["observed_at"])
                if (
                    observed.tzinfo is None
                    or observed.astimezone(ZoneInfo(rule["timezone"])).date().isoformat() != period
                ):
                    raise ValueError("observation date differs from the claimed settlement period")
            except (ValueError, TypeError, KeyError):
                return {"settled": False, "reason": "observation_time_not_in_period"}
        if (
            date.fromisoformat(period)
            >= datetime.fromtimestamp(now, ZoneInfo(rule["timezone"])).date()
        ):
            return {"settled": False, "reason": "period_not_closed"}
        settled = db.execute("SELECT count FROM settlements WHERE period=?", (period,)).fetchone()
        if settled:
            return {
                "settled": False,
                "reason": "already_settled"
                if settled[0] == metric["value"]
                else "settled_evidence_conflict",
            }
        last = db.execute(
            "SELECT period,count FROM settlements ORDER BY period DESC LIMIT 1"
        ).fetchone()
        previous_period, count = (
            (last[0], last[1]) if last else (rule["baseline_period"], rule["baseline_count"])
        )
        if date.fromisoformat(period) != date.fromisoformat(previous_period) + timedelta(days=1):
            return {"settled": False, "reason": "settlement_gap_requires_reconciliation"}
        delta = metric["value"] - count
        if delta < 0:
            return {
                "settled": False,
                "reason": "negative_observation_delta_requires_reconciliation",
            }
        age = (date.fromisoformat(period) - date.fromisoformat(rule["effective_date"])).days
        eligible = [stage["likes"] for stage in rule["likes_per_usd"] if stage["start_day"] <= age]
        if not eligible:
            return {"settled": False, "reason": "rule_not_effective"}
        credit = rule["daily_base_microusd"] + delta * 1_000_000 // eligible[-1]
        db.execute(
            "INSERT INTO settlements VALUES (?,?,?,?)",
            (period, receipt_id, metric["value"], credit),
        )
        return {"settled": True, "credit_microusd": credit, "increase": delta}

    def status(self, *, now: float | None = None, limit_id: str = "codex") -> dict:
        _timestamp(now)
        with self._db() as db:
            budget = self._get(db, "budget")
            enabled = self._get(db, "virtual_budget_enabled", False)
            balance = None
            if budget:
                net = db.execute(
                    "SELECT COALESCE(SUM(CASE kind WHEN 'income' THEN amount ELSE -amount END),0) FROM money WHERE seq>?",
                    (budget["money_baseline_seq"],),
                ).fetchone()[0]
                credit = db.execute("SELECT COALESCE(SUM(amount),0) FROM settlements").fetchone()[0]
                balance = budget["opening_balance_microusd"] + net + credit
            counters = {
                row[0]: json.loads(row[1])
                for row in db.execute("SELECT thread,counters FROM checkpoints")
            }
            unknown = db.execute("SELECT COUNT(*) FROM tokens WHERE state!='known'").fetchone()[0]
            financial = {
                row[0]: {"amount_microusd": row[1], "receipts": row[2]}
                for row in db.execute("SELECT kind,SUM(amount),COUNT(*) FROM money GROUP BY kind")
            }
            snapshot = self._get(db, "rate_limits")
            account_state, reasons = _account_state(
                snapshot["response"] if snapshot else None, limit_id
            )
            blocked = self._get(db, "quota_blocks", {}).get(limit_id)
            latest = db.execute(
                "SELECT id,period,summary FROM observations ORDER BY observed_at DESC,rowid DESC LIMIT 1"
            ).fetchone()
            return {
                "virtual_budget_enabled": enabled,
                "virtual_budget": {
                    "state": "configured" if budget else "unconfigured",
                    "currency": "USD",
                    "balance_microusd": balance,
                    "configuration": budget,
                },
                "historical_rule_reference": self._get(db, "historical_rule_reference"),
                "tokens": {
                    "state": "unknown" if unknown or not counters else "known",
                    "scope": "native_cumulative_per_thread_not_an_account_invoice",
                    "threads": counters,
                    "unknown_or_out_of_order_events": unknown,
                    "cost_microusd": None,
                },
                "money_receipts": financial,
                "latest_observation": {
                    "receipt_id": latest[0],
                    "period": latest[1],
                    "summary": json.loads(latest[2]),
                }
                if latest
                else None,
                "account_limits": {
                    "state": account_state,
                    "reasons": reasons,
                    "limit_id": limit_id,
                    "observed_at": snapshot["observed_at"] if snapshot else None,
                    "blocked_until_explicit_refresh": bool(blocked),
                    "blocking_reasons": blocked or [],
                },
            }

    def can_dispatch(
        self, *, automatic: bool, now: float | None = None, limit_id: str = "codex"
    ) -> dict:
        if type(automatic) is not bool:
            raise ValueError("automatic must be an explicit boolean")
        state = self.status(now=now, limit_id=limit_id)
        reasons = []
        if automatic:
            if (
                state["account_limits"]["blocked_until_explicit_refresh"]
                or state["account_limits"]["state"] == "exhausted"
            ):
                reasons.append("native_quota_exhausted_refresh_required")
            balance = state["virtual_budget"]["balance_microusd"]
            if state["virtual_budget_enabled"] and balance is not None and balance <= 0:
                reasons.append("virtual_budget_exhausted")
        return {
            "allowed": not reasons,
            "reasons": reasons,
            "automatic": automatic,
            "virtual_budget_enabled": state["virtual_budget_enabled"],
        }
