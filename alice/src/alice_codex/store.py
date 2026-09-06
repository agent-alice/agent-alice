"""Durable business schedules and dispatch receipts, not a Codex input queue.

All mutations are SQLite transactions. Corrupt/unknown databases fail closed and
are never replaced by an empty store. Sending records are deliberately ambiguous
after lease loss: only reconciliation may resolve them, never automatic replay.
"""

from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import time
from typing import Any, Iterator
from uuid import uuid4, uuid5, NAMESPACE_URL

from . import heartbeat
from .resources import TaskPolicy, TaskUsage


class StoreError(RuntimeError):
    """Storage failed; callers must stop dispatch rather than assume no jobs."""


class LeaseLost(StoreError):
    pass


@dataclass(frozen=True)
class Job:
    id: str
    name: str
    schedule_type: str
    schedule_value: str | float
    timezone: str = "UTC"
    target: str = "main"
    prompt: str = ""
    kind: str = "task"
    enabled: bool = True
    catch_up: bool = False
    next_due: float | None = None
    revision: int = 1


@dataclass(frozen=True)
class DispatchReceipt:
    # accepted means ownership transferred to Codex, NOT task success.
    status: str
    thread_id: str | None = None
    turn_id: str | None = None
    detail: str | None = None


@dataclass(frozen=True)
class DispatchEvent:
    event_id: str
    job_id: str
    job: Job
    due_at: float
    through_at: float
    catch_up: bool
    status: str
    receipt: DispatchReceipt | None = None

    @property
    def id(self) -> str:
        return self.event_id

    @property
    def target(self) -> str:
        return self.job.target

    @property
    def prompt(self) -> str:
        return self.job.prompt


def _timestamp(value: float) -> float:
    value = float(value)
    if not math.isfinite(value):
        raise ValueError("clock must be finite")
    return value


class Store:
    SCHEMA_VERSION = 1

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._db: sqlite3.Connection | None = None
        try:
            self._db = sqlite3.connect(self.path, isolation_level=None, timeout=5)
            self._db.row_factory = sqlite3.Row
            if self._db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise StoreError("schedule database integrity check failed")
            version = self._db.execute("PRAGMA user_version").fetchone()[0]
            tables = {
                r[0]
                for r in self._db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                )
            }
            if version not in (0, self.SCHEMA_VERSION) or (version == 0 and tables):
                raise StoreError(f"unsupported schedule database schema {version}")
            if version == 0:
                with self._transaction() as db:
                    db.execute("CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
                    db.execute("INSERT INTO settings VALUES ('autonomy_paused', 'false')")
                    db.execute("CREATE TABLE jobs (id TEXT PRIMARY KEY, definition TEXT NOT NULL)")
                    db.execute("""CREATE TABLE events (
                        id TEXT PRIMARY KEY, job_id TEXT NOT NULL, definition TEXT NOT NULL,
                        due_at REAL NOT NULL, through_at REAL NOT NULL, catch_up INTEGER NOT NULL,
                        status TEXT NOT NULL, owner TEXT, receipt TEXT)""")
                    db.execute("CREATE INDEX events_job_status ON events(job_id,status)")
                    db.execute(
                        "CREATE TABLE lease (id INTEGER PRIMARY KEY CHECK(id=1), owner TEXT, expires REAL NOT NULL)"
                    )
                    db.execute("INSERT INTO lease VALUES (1,NULL,0)")
                    db.execute(f"PRAGMA user_version={self.SCHEMA_VERSION}")
            elif tables != {"settings", "jobs", "events", "lease"}:
                raise StoreError("schedule database schema is incomplete")
            # Check business records too; syntactically valid SQLite can carry bad JSON.
            self.list_jobs()
            self.list_events()
            self.is_autonomy_paused()
            self.list_task_policies()
            self._heartbeat_known_targets = set(self._heartbeat_records(self._connection()))
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.execute("PRAGMA synchronous=FULL")
        except (sqlite3.Error, ValueError, TypeError, KeyError, StoreError) as exc:
            self.close()
            raise StoreError(f"cannot open schedule store {self.path}: {exc}") from exc

    def close(self) -> None:
        if self._db is not None:
            self._db.close()
            self._db = None

    def __enter__(self) -> "Store":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _connection(self) -> sqlite3.Connection:
        if self._db is None:
            raise StoreError("schedule store is closed")
        return self._db

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        db = self._connection()
        try:
            db.execute("BEGIN IMMEDIATE")
            yield db
            db.execute("COMMIT")
        except BaseException as exc:
            if db.in_transaction:
                db.execute("ROLLBACK")
            if isinstance(exc, sqlite3.Error):
                raise StoreError(f"schedule transaction failed: {exc}") from exc
            raise

    def _rows(self, sql: str, args: tuple = ()) -> list[sqlite3.Row]:
        try:
            return list(self._connection().execute(sql, args))
        except sqlite3.Error as exc:
            raise StoreError(f"schedule read failed: {exc}") from exc

    @staticmethod
    def _job(raw: str) -> Job:
        from .scheduler import validate_job

        try:
            job = Job(**json.loads(raw))
            validate_job(job)
            if job.next_due is not None:
                _timestamp(job.next_due)
            return job
        except (ValueError, TypeError, KeyError) as exc:
            raise StoreError(f"invalid persisted job: {exc}") from exc

    @classmethod
    def _event(cls, row: sqlite3.Row) -> DispatchEvent:
        try:
            receipt = DispatchReceipt(**json.loads(row["receipt"])) if row["receipt"] else None
            if row["status"] not in {
                "pending",
                "claimed",
                "sending",
                "accepted",
                "completed",
                "failed",
                "unknown",
                "cancelled",
            }:
                raise ValueError("invalid dispatch status")
            if receipt is not None and receipt.status not in {
                "accepted",
                "completed",
                "failed",
                "unknown",
            }:
                raise ValueError("invalid receipt status")
            return DispatchEvent(
                row["id"],
                row["job_id"],
                cls._job(row["definition"]),
                _timestamp(row["due_at"]),
                _timestamp(row["through_at"]),
                bool(row["catch_up"]),
                row["status"],
                receipt,
            )
        except (ValueError, TypeError, KeyError) as exc:
            raise StoreError(f"invalid persisted event: {exc}") from exc

    def create_job(
        self,
        *,
        name: str,
        schedule_type: str,
        schedule_value: str | float,
        now: float | None = None,
        job_id: str | None = None,
        **options: Any,
    ) -> Job:
        from .scheduler import next_due, validate_job

        if "next_due" in options or "revision" in options or "id" in options:
            raise ValueError("scheduler owns id/revision/next_due")
        job = Job(
            id=job_id or str(uuid4()),
            name=name,
            schedule_type=schedule_type,
            schedule_value=schedule_value,
            **options,
        )
        validate_job(job)
        job = replace(
            job,
            next_due=next_due(job, _timestamp(time.time() if now is None else now), initial=True),
        )
        with self._transaction() as db:
            db.execute("INSERT INTO jobs VALUES (?,?)", (job.id, json.dumps(asdict(job))))
        return job

    def get_job(self, job_id: str) -> Job:
        rows = self._rows("SELECT definition FROM jobs WHERE id=?", (job_id,))
        if not rows:
            raise KeyError(job_id)
        return self._job(rows[0]["definition"])

    def list_jobs(self) -> list[Job]:
        return [
            self._job(row["definition"])
            for row in self._rows("SELECT definition FROM jobs ORDER BY id")
        ]

    def update_job(self, job_id: str, *, now: float | None = None, **changes: Any) -> Job:
        from .scheduler import next_due, validate_job

        allowed = {
            "name",
            "schedule_type",
            "schedule_value",
            "timezone",
            "target",
            "prompt",
            "kind",
            "enabled",
            "catch_up",
        }
        if not changes or set(changes) - allowed:
            raise ValueError("update requires supported job fields")
        with self._transaction() as db:
            row = db.execute("SELECT definition FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                raise KeyError(job_id)
            old = self._job(row[0])
            job = replace(old, **changes, revision=old.revision + 1)
            validate_job(job)
            if {"schedule_type", "schedule_value", "timezone"} & changes.keys() or (
                job.enabled and not old.enabled
            ):
                job = replace(
                    job,
                    next_due=next_due(
                        job, _timestamp(time.time() if now is None else now), initial=True
                    ),
                )
            db.execute("UPDATE jobs SET definition=? WHERE id=?", (json.dumps(asdict(job)), job_id))
            db.execute(
                "UPDATE events SET status='cancelled',owner=NULL WHERE job_id=? AND status IN ('pending','claimed')",
                (job_id,),
            )
        return job

    def delete_job(self, job_id: str) -> bool:
        with self._transaction() as db:
            removed = db.execute("DELETE FROM jobs WHERE id=?", (job_id,)).rowcount != 0
            db.execute(
                "UPDATE events SET status='cancelled',owner=NULL WHERE job_id=? AND status IN ('pending','claimed')",
                (job_id,),
            )
            return removed

    def set_autonomy_paused(self, paused: bool) -> None:
        if type(paused) is not bool:
            raise ValueError("paused must be boolean")
        with self._transaction() as db:
            db.execute(
                "UPDATE settings SET value=? WHERE key='autonomy_paused'", (json.dumps(paused),)
            )

    def is_autonomy_paused(self) -> bool:
        rows = self._rows("SELECT value FROM settings WHERE key='autonomy_paused'")
        if not rows or rows[0][0] not in ("true", "false"):
            raise StoreError("missing or invalid autonomy pause state")
        return rows[0][0] == "true"

    @staticmethod
    def _policy_name(value: str, name: str) -> str:
        if not isinstance(value, str) or not value or (name == "target" and len(value) > 150):
            raise ValueError(f"{name} must be a nonempty string")
        return value

    @staticmethod
    def _policy_time(value: float) -> float:
        if type(value) not in (int, float) or not math.isfinite(value):
            raise ValueError("now must be an explicit finite timestamp")
        return float(value)

    @staticmethod
    def _policy_key(prefix: str, value: str) -> str:
        return prefix + "/" + hashlib.sha256(value.encode()).hexdigest()

    @staticmethod
    def _validated_policy(policy: TaskPolicy | dict) -> dict:
        if isinstance(policy, TaskPolicy):
            policy = asdict(policy)
        fields = {
            "max_elapsed_seconds",
            "max_attempts",
            "max_retries",
            "retry_wait_seconds",
            "unchanged_wait_seconds",
        }
        if not isinstance(policy, dict) or set(policy) != fields:
            raise ValueError("task policy requires exactly its five explicit fields")
        return asdict(TaskPolicy(**policy))

    @staticmethod
    def _policy_fingerprint(value: str) -> str:
        if (
            not isinstance(value, str)
            or len(value) != 64
            or any(character not in "0123456789abcdef" for character in value)
        ):
            raise ValueError("input_sha256 must be a lowercase SHA-256 digest")
        return value

    @staticmethod
    def _policy_evidence(outcome: str, evidence: dict | None) -> dict | None:
        if outcome in {"progress", "unchanged", "complete"} and not evidence:
            raise ValueError("confirmed business outcomes require nonempty host evidence")
        if evidence is None:
            return None
        if not isinstance(evidence, dict) or any(not isinstance(key, str) for key in evidence):
            raise ValueError("host evidence must be a JSON mapping")
        return json.loads(json.dumps(evidence, allow_nan=False))

    @classmethod
    def _validate_policy_record(cls, record: dict) -> None:
        required = {
            "version",
            "target",
            "policy",
            "usage",
            "current_attempt_id",
            "attempts",
            "last_checked_at",
            "created_at",
            "updated_at",
        }
        if not isinstance(record, dict) or set(record) != required:
            raise ValueError("invalid task policy record fields")
        if type(record["version"]) is not int or record["version"] != 1:
            raise ValueError("unsupported task policy record version")
        cls._policy_name(record["target"], "target")
        cls._validated_policy(record["policy"])
        cls._policy_time(record["created_at"])
        cls._policy_time(record["updated_at"])
        if record["updated_at"] < record["created_at"]:
            raise ValueError("task policy update precedes creation")
        checked = record["last_checked_at"]
        if checked is not None:
            cls._policy_time(checked)
        attempts = record["attempts"]
        if not isinstance(attempts, dict):
            raise ValueError("invalid task attempt receipts")
        if record["usage"] is None:
            if attempts or record["current_attempt_id"] is not None:
                raise ValueError("unstarted task has attempt receipts")
            return
        if not isinstance(record["usage"], dict) or set(record["usage"]) != {
            "started_at",
            "attempts",
            "consecutive_failures",
            "last_outcome",
            "last_finished_at",
            "last_checked_at",
        }:
            raise ValueError("invalid task usage")
        usage = TaskUsage(**record["usage"])
        if (
            usage.attempts != len(attempts)
            or not attempts
            or checked is None
            or checked != usage.last_checked_at
            or record["current_attempt_id"] not in attempts
        ):
            raise ValueError("task usage does not match attempt receipts")
        numbers = set()
        for intent_id, attempt in attempts.items():
            cls._policy_name(intent_id, "intent_id")
            if not isinstance(attempt, dict) or set(attempt) != {
                "intent_id",
                "target",
                "thread_id",
                "input_sha256",
                "number",
                "admitted_at",
                "prior_failures",
                "outcome",
                "finished_at",
                "evidence",
                "decision",
            }:
                raise ValueError("invalid task attempt receipt fields")
            if attempt["intent_id"] != intent_id or attempt["target"] != record["target"]:
                raise ValueError("task attempt receipt binding mismatch")
            if attempt["thread_id"] is not None:
                cls._policy_name(attempt["thread_id"], "thread_id")
            cls._policy_fingerprint(attempt["input_sha256"])
            number, prior = attempt["number"], attempt["prior_failures"]
            if (
                type(number) is not int
                or number < 1
                or type(prior) is not int
                or not 0 <= prior < number
            ):
                raise ValueError("invalid task attempt counters")
            numbers.add(number)
            admitted = cls._policy_time(attempt["admitted_at"])
            if admitted < usage.started_at:
                raise ValueError("attempt precedes task start")
            if admitted > checked:
                raise ValueError("attempt exceeds checked clock watermark")
            outcome = attempt["outcome"]
            if not isinstance(outcome, str) or outcome not in {
                "running",
                "unknown",
                "failed",
                "progress",
                "unchanged",
                "complete",
            }:
                raise ValueError("invalid task attempt outcome")
            if outcome == "running":
                if attempt["finished_at"] is not None or attempt["evidence"] is not None:
                    raise ValueError("running attempt cannot have a terminal receipt")
            elif cls._policy_time(attempt["finished_at"]) < admitted:
                raise ValueError("attempt finish precedes admission")
            elif attempt["finished_at"] > checked:
                raise ValueError("attempt finish exceeds checked clock watermark")
            cls._policy_evidence(outcome, attempt["evidence"])
            decision = attempt["decision"]
            if (
                not isinstance(decision, dict)
                or set(decision)
                != {
                    "allowed",
                    "observed_at",
                    "state",
                    "reasons",
                    "next_attempt_at",
                    "remaining",
                    "recovery_required",
                }
                or decision.get("allowed") is not True
                or decision.get("state") != "ready"
                or decision.get("observed_at") != admitted
                or decision.get("reasons") != []
                or decision.get("next_attempt_at") is not None
                or decision.get("recovery_required") is not False
            ):
                raise ValueError("invalid task admission decision")
            remaining = decision["remaining"]
            if not isinstance(remaining, dict) or set(remaining) != {
                "seconds",
                "attempts",
                "retries",
            }:
                raise ValueError("invalid task admission remaining values")
            if cls._policy_time(remaining["seconds"]) < 0 or any(
                type(remaining[key]) is not int or remaining[key] < 0
                for key in ("attempts", "retries")
            ):
                raise ValueError("invalid task admission remaining values")
        if numbers != set(range(1, usage.attempts + 1)):
            raise ValueError("task attempt sequence is incomplete")
        prior_failures, prior_finished = 0, usage.started_at
        for attempt in sorted(attempts.values(), key=lambda item: item["number"]):
            if (
                attempt["prior_failures"] != prior_failures
                or attempt["admitted_at"] < prior_finished
            ):
                raise ValueError("task attempt history does not match prior usage")
            if attempt["number"] < usage.attempts and attempt["outcome"] not in {
                "progress",
                "unchanged",
                "failed",
            }:
                raise ValueError("unresolved or complete attempt cannot have a successor")
            prior_failures = prior_failures + 1 if attempt["outcome"] == "failed" else 0
            prior_finished = (
                attempt["admitted_at"] if attempt["finished_at"] is None else attempt["finished_at"]
            )
        current = attempts[record["current_attempt_id"]]
        expected_failures = (
            current["prior_failures"] + (current["outcome"] == "failed")
            if current["outcome"] in {"running", "unknown", "failed"}
            else 0
        )
        if (
            current["number"] != usage.attempts
            or current["outcome"] != usage.last_outcome
            or current["finished_at"] != usage.last_finished_at
            or expected_failures != usage.consecutive_failures
        ):
            raise ValueError("current attempt does not match task usage")

    @classmethod
    def _task_policy_state(cls, db: sqlite3.Connection) -> tuple[dict, dict]:
        """Validate our complete settings namespace, including cross-record IDs."""
        records, requests = {}, {}
        try:
            rows = db.execute(
                "SELECT key,value FROM settings WHERE key GLOB 'task_policy/*' "
                "OR key GLOB 'task_policy_request/*'"
            ).fetchall()
            for row in rows:
                value = json.loads(row["value"])
                if row["key"].startswith("task_policy/"):
                    cls._validate_policy_record(value)
                    if row["key"] != cls._policy_key("task_policy", value["target"]):
                        raise ValueError("task policy key binding mismatch")
                    records[value["target"]] = value
                    continue
                if (
                    not isinstance(value, dict)
                    or type(value.get("version")) is not int
                    or value["version"] != 1
                ):
                    raise ValueError("unsupported task policy request version")
                request_id = cls._policy_name(value["request_id"], "request_id")
                cls._policy_name(value["target"], "target")
                if row["key"] != cls._policy_key("task_policy_request", request_id):
                    raise ValueError("task policy request key binding mismatch")
                common = {"version", "kind", "request_id", "target"}
                if value["kind"] == "policy" and set(value) == common | {"receipt"}:
                    receipt = value["receipt"]
                    if (
                        not isinstance(receipt, dict)
                        or set(receipt)
                        != {
                            "version",
                            "target",
                            "request_id",
                            "policy",
                            "recorded_at",
                        }
                        or type(receipt["version"]) is not int
                        or any(
                            receipt[key] != value[key]
                            for key in ("version", "target", "request_id")
                        )
                    ):
                        raise ValueError("task policy receipt binding mismatch")
                    cls._validated_policy(receipt["policy"])
                    cls._policy_time(receipt["recorded_at"])
                elif value["kind"] == "attempt" and set(value) == common | {
                    "input_sha256",
                    "thread_id",
                }:
                    cls._policy_fingerprint(value["input_sha256"])
                    if value["thread_id"] is not None:
                        cls._policy_name(value["thread_id"], "thread_id")
                else:
                    raise ValueError("invalid task policy request fields")
                requests[request_id] = value
            for request_id, request in requests.items():
                record = records.get(request["target"])
                if record is None:
                    raise ValueError("task policy request has no target")
                if request["kind"] == "attempt":
                    attempt = record["attempts"].get(request_id)
                    if (
                        not attempt
                        or attempt["input_sha256"] != request["input_sha256"]
                        or attempt["thread_id"] != request["thread_id"]
                    ):
                        raise ValueError("task policy request has no matching attempt")
            for record in records.values():
                if not any(
                    request["kind"] == "policy"
                    and request["target"] == record["target"]
                    and request["receipt"]["policy"] == record["policy"]
                    for request in requests.values()
                ):
                    raise ValueError("task policy has no matching configuration receipt")
                for intent_id in record["attempts"]:
                    request = requests.get(intent_id)
                    if (
                        not request
                        or request["kind"] != "attempt"
                        or request["target"] != record["target"]
                    ):
                        raise ValueError("task attempt has no matching request binding")
        except (sqlite3.Error, ValueError, TypeError, KeyError) as error:
            raise StoreError(f"invalid persisted task policy: {error}") from error
        return records, requests

    @classmethod
    def _write_policy_setting(
        cls, db: sqlite3.Connection, prefix: str, identity: str, value: dict
    ) -> None:
        db.execute(
            "INSERT INTO settings(key,value) VALUES (?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (cls._policy_key(prefix, identity), json.dumps(value, sort_keys=True, allow_nan=False)),
        )

    def get_task_policy(self, target: str) -> dict | None:
        """Read a validated record; no policy means this store does not limit the target."""
        self._policy_name(target, "target")
        return self._task_policy_state(self._connection())[0].get(target)

    def list_task_policies(self) -> list[dict]:
        records, _ = self._task_policy_state(self._connection())
        return [records[target] for target in sorted(records)]

    def set_task_policy(
        self, target: str, policy: TaskPolicy | dict, request_id: str, *, now: float
    ) -> dict:
        """Set explicit limits while retaining usage and immutable request receipts."""
        self._policy_name(target, "target")
        self._policy_name(request_id, "request_id")
        policy, now = self._validated_policy(policy), self._policy_time(now)
        with self._transaction() as db:
            records, requests = self._task_policy_state(db)
            prior = requests.get(request_id)
            if prior is not None:
                if (
                    prior["kind"] != "policy"
                    or prior["target"] != target
                    or prior["receipt"]["policy"] != policy
                ):
                    raise ValueError(
                        "Request ID already identifies a different task policy operation"
                    )
                return prior["receipt"]
            record = records.get(target) or {
                "version": 1,
                "target": target,
                "policy": policy,
                "usage": None,
                "current_attempt_id": None,
                "attempts": {},
                "last_checked_at": None,
                "created_at": now,
                "updated_at": now,
            }
            record.update(policy=policy, updated_at=max(now, record["updated_at"]))
            receipt = {
                "version": 1,
                "target": target,
                "request_id": request_id,
                "policy": policy,
                "recorded_at": now,
            }
            self._write_policy_setting(db, "task_policy", target, record)
            self._write_policy_setting(
                db,
                "task_policy_request",
                request_id,
                {
                    "version": 1,
                    "kind": "policy",
                    "request_id": request_id,
                    "target": target,
                    "receipt": receipt,
                },
            )
        return receipt

    @staticmethod
    def _decide_task_policy(record: dict, *, now: float, busy: bool) -> tuple[TaskUsage, dict]:
        usage = (
            TaskUsage(**record["usage"])
            if record["usage"] is not None
            else TaskUsage(
                now,
                last_checked_at=max(
                    now, now if record["last_checked_at"] is None else record["last_checked_at"]
                ),
            )
        )
        decision = TaskPolicy(**record["policy"]).decide(usage, now=now, busy=busy)
        usage = replace(usage, last_checked_at=decision["observed_at"])
        record["last_checked_at"] = decision["observed_at"]
        record["updated_at"] = max(record["updated_at"], decision["observed_at"])
        if record["usage"] is not None:
            record["usage"] = asdict(usage)
        return usage, decision

    def task_policy_status(self, target: str, *, now: float, busy: bool) -> dict | None:
        """Persist the shared decision's watermark without starting an unused task."""
        self._policy_name(target, "target")
        now = self._policy_time(now)
        if type(busy) is not bool:
            raise ValueError("busy must be boolean")
        with self._transaction() as db:
            record = self._task_policy_state(db)[0].get(target)
            if record is None:
                return None
            _, decision = self._decide_task_policy(record, now=now, busy=busy)
            self._write_policy_setting(db, "task_policy", target, record)
            return {
                "target": target,
                "policy": record["policy"],
                "usage": record["usage"],
                "decision": decision,
            }

    def admit_task_attempt(
        self,
        target: str,
        intent_id: str,
        input_sha256: str,
        *,
        now: float,
        busy: bool,
        thread_id: str | None = None,
    ) -> dict | None:
        """Charge once before native dispatch; replayed alone never authorizes another RPC.

        The caller must reconcile a receipt whose corresponding runtime intent
        was not saved. Its original thread ID remains available after alias changes.
        """
        self._policy_name(target, "target")
        self._policy_name(intent_id, "intent_id")
        self._policy_fingerprint(input_sha256)
        if thread_id is not None:
            self._policy_name(thread_id, "thread_id")
        now = self._policy_time(now)
        if type(busy) is not bool:
            raise ValueError("busy must be boolean")
        with self._transaction() as db:
            records, requests = self._task_policy_state(db)
            prior = requests.get(intent_id)
            if prior is not None and (
                prior["kind"] != "attempt"
                or prior["target"] != target
                or prior["input_sha256"] != input_sha256
                or prior["thread_id"] != thread_id
            ):
                raise ValueError("Request ID already identifies different task input")
            record = records.get(target)
            if record is None:
                return None
            if prior is not None:
                attempt = record["attempts"][intent_id]
                return {
                    "admitted": True,
                    "replayed": True,
                    "decision": attempt["decision"],
                    "usage": record["usage"],
                    "attempt": attempt,
                }
            usage, decision = self._decide_task_policy(record, now=now, busy=busy)
            attempt = None
            if decision["allowed"]:
                attempt = {
                    "intent_id": intent_id,
                    "target": target,
                    "thread_id": thread_id,
                    "input_sha256": input_sha256,
                    "number": usage.attempts + 1,
                    "admitted_at": now,
                    "prior_failures": usage.consecutive_failures,
                    "outcome": "running",
                    "finished_at": None,
                    "evidence": None,
                    "decision": decision,
                }
                usage = replace(
                    usage,
                    attempts=usage.attempts + 1,
                    last_outcome="running",
                    last_finished_at=None,
                )
                record["usage"] = asdict(usage)
                record["attempts"][intent_id] = attempt
                record["current_attempt_id"] = intent_id
                self._write_policy_setting(
                    db,
                    "task_policy_request",
                    intent_id,
                    {
                        "version": 1,
                        "kind": "attempt",
                        "request_id": intent_id,
                        "target": target,
                        "thread_id": thread_id,
                        "input_sha256": input_sha256,
                    },
                )
            self._write_policy_setting(db, "task_policy", target, record)
            return {
                "admitted": decision["allowed"],
                "replayed": False,
                "decision": decision,
                "usage": record["usage"],
                "attempt": attempt,
            }

    def finish_task_attempt(
        self, target: str, intent_id: str, outcome: str, *, now: float, evidence: dict | None = None
    ) -> dict:
        """Record host-observed outcomes; only the current unknown result can be reconciled."""
        self._policy_name(target, "target")
        self._policy_name(intent_id, "intent_id")
        now = self._policy_time(now)
        if not isinstance(outcome, str) or outcome not in {
            "unknown",
            "failed",
            "progress",
            "unchanged",
            "complete",
        }:
            raise ValueError("unsupported task attempt outcome")
        evidence = self._policy_evidence(outcome, evidence)
        with self._transaction() as db:
            record = self._task_policy_state(db)[0].get(target)
            if record is None or record["current_attempt_id"] != intent_id:
                raise ValueError("outcome must identify the current admitted task attempt")
            attempt = record["attempts"][intent_id]
            if attempt["outcome"] == outcome:
                return record  # Duplicate notifications cannot extend waits or add failures.
            if attempt["outcome"] not in {"running", "unknown"}:
                raise ValueError("cannot change a confirmed task attempt outcome")
            finished_at = max(now, attempt["admitted_at"], record["last_checked_at"])
            failures = (
                attempt["prior_failures"] + (outcome == "failed")
                if outcome in {"failed", "unknown"}
                else 0
            )
            usage = replace(
                TaskUsage(**record["usage"]),
                last_outcome=outcome,
                last_finished_at=finished_at,
                last_checked_at=finished_at,
                consecutive_failures=failures,
            )
            attempt.update(outcome=outcome, finished_at=finished_at, evidence=evidence)
            record.update(
                usage=asdict(usage),
                last_checked_at=finished_at,
                updated_at=max(record["updated_at"], finished_at),
            )
            self._write_policy_setting(db, "task_policy", target, record)
            return record

    @classmethod
    def _heartbeat_wait(cls, wait_seconds: float) -> float:
        try:
            wait_seconds = cls._policy_time(wait_seconds)
        except OverflowError as error:
            raise ValueError("heartbeat wait_seconds must be finite and positive") from error
        if wait_seconds <= 0:
            raise ValueError("heartbeat wait_seconds must be finite and positive")
        return wait_seconds

    @classmethod
    def _heartbeat_receipt(cls, target: str, receipt: dict) -> dict:
        if not isinstance(receipt, dict) or set(receipt) != {
            "version",
            "id",
            "target",
            "source_id",
            "scope_sha256",
            "validator_version",
            "observed_at",
            "state",
            "content_sha256",
            "evidence_sha256",
            "reason",
        }:
            raise ValueError("heartbeat receipt requires its version 1 fields")
        if receipt["target"] != target:
            raise ValueError("heartbeat receipt target binding mismatch")
        try:
            heartbeat.compare_receipts(None, receipt)  # Reuse the adapter's receipt validation.
        except OverflowError as error:
            raise ValueError("heartbeat observed_at must be finite") from error
        return json.loads(json.dumps(receipt, allow_nan=False))

    @staticmethod
    def _heartbeat_public(record: dict) -> dict:
        return json.loads(
            json.dumps(
                {
                    key: record[key]
                    for key in (
                        "version",
                        "target",
                        "latest",
                        "last_good",
                        "comparison",
                        "waiting_until",
                    )
                },
                allow_nan=False,
            )
        )

    @staticmethod
    def _advance_heartbeat(record: dict, receipt: dict, wait_seconds: float) -> None:
        comparison = heartbeat.compare_receipts(
            record["latest"], receipt, last_good=record["last_good"]
        )
        if not comparison["accept"]:
            return
        waiting_until = None
        if comparison["state"] == "unchanged":
            waiting_until = receipt["observed_at"] + wait_seconds
            if not math.isfinite(waiting_until):
                raise ValueError("heartbeat waiting deadline must be finite")
        record.update(latest=receipt, comparison=comparison, waiting_until=waiting_until)
        if comparison["update_last_good"]:
            record["last_good"] = receipt

    @classmethod
    def _heartbeat_snapshot(cls, key: str, raw: str) -> dict:
        record = json.loads(raw)
        if not isinstance(record, dict) or set(record) != {
            "version",
            "target",
            "latest",
            "last_good",
            "comparison",
            "waiting_until",
            "receipt_count",
            "last_receipt_id",
        }:
            raise ValueError("invalid heartbeat record fields")
        if type(record["version"]) is not int or record["version"] != 1:
            raise ValueError("unsupported heartbeat record version")
        target = cls._policy_name(record["target"], "target")
        if key != cls._policy_key("heartbeat", target):
            raise ValueError("heartbeat key target binding mismatch")
        cls._heartbeat_receipt(target, record["latest"])
        if record["last_good"] is not None:
            cls._heartbeat_receipt(target, record["last_good"])
            if record["last_good"]["state"] != "known":
                raise ValueError("heartbeat last_good must be known")
        if not isinstance(record["comparison"], dict) or any(
            type(record["comparison"].get(field)) is not bool
            for field in ("accept", "update_last_good", "wake")
        ):
            raise ValueError("invalid heartbeat comparison flags")
        if record["waiting_until"] is not None:
            cls._policy_time(record["waiting_until"])
        if type(record["receipt_count"]) is not int or record["receipt_count"] < 1:
            raise ValueError("invalid heartbeat receipt count")
        cls._policy_name(record["last_receipt_id"], "last_receipt_id")
        return record

    @classmethod
    def _heartbeat_entry(cls, key: str, raw: str) -> dict:
        entry = json.loads(raw)
        if not isinstance(entry, dict) or set(entry) != {
            "version",
            "target",
            "sequence",
            "receipt",
            "wait_seconds",
            "previous_latest_id",
            "previous_good_id",
        }:
            raise ValueError("invalid heartbeat receipt index entry")
        if type(entry["version"]) is not int or entry["version"] != 1:
            raise ValueError("unsupported heartbeat receipt index version")
        target = cls._policy_name(entry["target"], "target")
        receipt = cls._heartbeat_receipt(target, entry["receipt"])
        if key != cls._policy_key("heartbeat_receipt", receipt["id"]):
            raise ValueError("heartbeat receipt key ID binding mismatch")
        cls._heartbeat_wait(entry["wait_seconds"])
        if type(entry["sequence"]) is not int or entry["sequence"] < 1:
            raise ValueError("invalid heartbeat receipt sequence")
        for field in ("previous_latest_id", "previous_good_id"):
            if entry[field] is not None:
                cls._policy_name(entry[field], field)
        if (entry["sequence"] == 1) != (entry["previous_latest_id"] is None) or (
            entry["previous_latest_id"] is None and entry["previous_good_id"] is not None
        ):
            raise ValueError("invalid heartbeat receipt predecessor")
        return entry

    @classmethod
    def _read_heartbeat_entry(cls, db: sqlite3.Connection, receipt_id: str) -> dict | None:
        key = cls._policy_key("heartbeat_receipt", receipt_id)
        row = db.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return cls._heartbeat_entry(key, row[0]) if row is not None else None

    @staticmethod
    def _empty_heartbeat(target: str) -> dict:
        return {
            "version": 1,
            "target": target,
            "latest": None,
            "last_good": None,
            "comparison": None,
            "waiting_until": None,
        }

    @classmethod
    def _heartbeat_records(cls, db: sqlite3.Connection) -> dict:
        """Audit the complete append-only history once when opening the store.

        Polling reads a bounded number of keyed rows instead. Sequence includes
        stale observations so old conflicting IDs remain detectable indefinitely.
        """
        records, entries = {}, {}
        try:
            for row in db.execute(
                "SELECT key,value FROM settings WHERE key GLOB 'heartbeat/*' "
                "OR key GLOB 'heartbeat_receipt/*'"
            ):
                if row["key"].startswith("heartbeat/"):
                    record = cls._heartbeat_snapshot(row["key"], row["value"])
                    records[record["target"]] = record
                else:
                    entry = cls._heartbeat_entry(row["key"], row["value"])
                    target_entries = entries.setdefault(entry["target"], {})
                    if entry["sequence"] in target_entries:
                        raise ValueError("duplicated heartbeat receipt sequence")
                    target_entries[entry["sequence"]] = entry
            if set(records) != set(entries):
                raise ValueError("heartbeat snapshot and receipt targets differ")
            for target, record in records.items():
                target_entries = entries[target]
                if record["receipt_count"] != len(target_entries):
                    raise ValueError("heartbeat receipt count does not match history")
                replay = cls._empty_heartbeat(target)
                for number in range(1, len(target_entries) + 1):
                    entry = target_entries.get(number)
                    if entry is None:
                        raise ValueError("heartbeat receipt sequence is incomplete")
                    previous_latest = replay["latest"]
                    previous_good = replay["last_good"]
                    if entry["previous_latest_id"] != (
                        previous_latest["id"] if previous_latest else None
                    ) or entry["previous_good_id"] != (
                        previous_good["id"] if previous_good else None
                    ):
                        raise ValueError("heartbeat receipt predecessor does not match history")
                    cls._advance_heartbeat(replay, entry["receipt"], entry["wait_seconds"])
                if (
                    cls._heartbeat_public(record) != replay
                    or record["last_receipt_id"] != entry["receipt"]["id"]
                ):
                    raise ValueError("heartbeat state does not match persisted receipt history")
        except (sqlite3.Error, ValueError, TypeError, KeyError, OverflowError) as error:
            raise StoreError(f"invalid persisted heartbeat: {error}") from error
        return records

    @classmethod
    def _read_heartbeat_snapshot(cls, db: sqlite3.Connection, target: str) -> dict | None:
        """Cross-check a snapshot with a fixed number of indexed receipt rows."""
        key = cls._policy_key("heartbeat", target)
        row = db.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        if row is None:
            return None
        record = cls._heartbeat_snapshot(key, row[0])
        cache = {}

        def entry_for(receipt_id: str) -> dict:
            if receipt_id not in cache:
                entry = cls._read_heartbeat_entry(db, receipt_id)
                if entry is None or entry["target"] != target:
                    raise ValueError("heartbeat snapshot references a missing or foreign receipt")
                cache[receipt_id] = entry
            return cache[receipt_id]

        latest = entry_for(record["latest"]["id"])
        replay = cls._empty_heartbeat(target)
        for field, pointer in (("latest", "previous_latest_id"), ("last_good", "previous_good_id")):
            if latest[pointer] is not None:
                previous = entry_for(latest[pointer])
                if previous["sequence"] >= latest["sequence"]:
                    raise ValueError("heartbeat predecessor does not precede latest receipt")
                replay[field] = previous["receipt"]
        if (
            replay["latest"] is not None
            and replay["latest"]["state"] == "known"
            and (replay["last_good"] != replay["latest"])
        ):
            raise ValueError("known heartbeat predecessor must match last_good")
        comparison = heartbeat.compare_receipts(
            replay["latest"], latest["receipt"], last_good=replay["last_good"]
        )
        if not comparison["accept"]:
            raise ValueError("heartbeat latest receipt was not accepted")
        cls._advance_heartbeat(replay, latest["receipt"], latest["wait_seconds"])
        if cls._heartbeat_public(record) != replay:
            raise ValueError("heartbeat state does not match indexed latest receipt")
        tail = entry_for(record["last_receipt_id"])
        if tail["sequence"] != record["receipt_count"] or tail["sequence"] < latest["sequence"]:
            raise ValueError("heartbeat tail does not match receipt count")
        if tail["receipt"]["id"] != latest["receipt"]["id"] and (
            tail["sequence"] <= latest["sequence"]
            or tail["previous_latest_id"] != record["latest"]["id"]
            or tail["previous_good_id"]
            != (record["last_good"]["id"] if record["last_good"] else None)
            or heartbeat.compare_receipts(
                record["latest"], tail["receipt"], last_good=record["last_good"]
            )["state"]
            != "stale"
        ):
            raise ValueError("heartbeat tail does not retain accepted observation state")
        return record

    def get_heartbeat_state(self, target: str) -> dict | None:
        """Return a copy of the latest accepted host observation and its wait state."""
        self._policy_name(target, "target")
        try:
            # One read snapshot prevents independent writers from changing pointer
            # rows between validation queries; it does not reserve the write lock.
            db = self._connection()
            db.execute("BEGIN")
            try:
                record = self._read_heartbeat_snapshot(db, target)
                if record is None and target in self._heartbeat_known_targets:
                    raise ValueError("previously observed heartbeat snapshot is missing")
            finally:
                db.execute("ROLLBACK")
            if record is not None:
                self._heartbeat_known_targets.add(target)
            return self._heartbeat_public(record) if record is not None else None
        except (sqlite3.Error, ValueError, TypeError, KeyError, OverflowError) as error:
            raise StoreError(f"invalid persisted heartbeat: {error}") from error

    def record_heartbeat(self, target: str, receipt: dict, *, wait_seconds: float) -> dict:
        """Append an internal host observation without changing task usage or pause.

        An old/duplicate observation returns the existing accepted state, not a
        new wake receipt. Compare latest IDs to determine whether state advanced.
        The receipt structure supplies validation, not caller authentication.
        """
        self._policy_name(target, "target")
        receipt = self._heartbeat_receipt(target, receipt)
        wait_seconds = self._heartbeat_wait(wait_seconds)
        with self._transaction() as db:
            try:
                record = self._read_heartbeat_snapshot(db, target)
                if record is None and target in self._heartbeat_known_targets:
                    raise ValueError("previously observed heartbeat snapshot is missing")
                prior = self._read_heartbeat_entry(db, receipt["id"])
            except (sqlite3.Error, ValueError, TypeError, KeyError, OverflowError) as error:
                raise StoreError(f"invalid persisted heartbeat: {error}") from error
            if prior is not None:
                if prior["receipt"] != receipt:
                    raise ValueError("heartbeat receipt ID identifies conflicting evidence")
                if record is None or prior["sequence"] > record["receipt_count"]:
                    raise StoreError(
                        "invalid persisted heartbeat: receipt has no matching snapshot"
                    )
                return self._heartbeat_public(record)
            if record is None:
                record = {**self._empty_heartbeat(target), "receipt_count": 0}
            entry = {
                "version": 1,
                "target": target,
                "sequence": record["receipt_count"] + 1,
                "receipt": receipt,
                "wait_seconds": wait_seconds,
                "previous_latest_id": record["latest"]["id"] if record["latest"] else None,
                "previous_good_id": record["last_good"]["id"] if record["last_good"] else None,
            }
            self._advance_heartbeat(record, receipt, wait_seconds)
            record.update(receipt_count=entry["sequence"], last_receipt_id=receipt["id"])
            # No UPSERT for receipts: an acknowledged observation is immutable.
            db.execute(
                "INSERT INTO settings(key,value) VALUES (?,?)",
                (
                    self._policy_key("heartbeat_receipt", receipt["id"]),
                    json.dumps(entry, sort_keys=True, allow_nan=False),
                ),
            )
            self._write_policy_setting(db, "heartbeat", target, record)
            result = self._heartbeat_public(record)
        self._heartbeat_known_targets.add(target)
        return result

    @staticmethod
    def _require_lease(db: sqlite3.Connection, owner: str, now: float) -> None:
        row = db.execute("SELECT owner,expires FROM lease WHERE id=1").fetchone()
        if row is None or row["owner"] != owner or row["expires"] <= now:
            raise LeaseLost("scheduler lease expired or belongs to another instance")

    def acquire_lease(self, owner: str, *, now: float, seconds: float = 30) -> bool:
        now = _timestamp(now)
        if not owner or not math.isfinite(seconds) or seconds <= 0:
            raise ValueError("owner and positive finite lease duration required")
        with self._transaction() as db:
            row = db.execute("SELECT owner,expires FROM lease WHERE id=1").fetchone()
            if row is None:
                raise StoreError("scheduler lease record missing")
            if row["owner"] != owner and row["expires"] > now:
                return False
            if row["expires"] <= now or row["owner"] != owner:
                # Claimed but unsent is safe to try. Sending is an uncertain effect.
                db.execute("UPDATE events SET status='pending',owner=NULL WHERE status='claimed'")
                db.execute(
                    "UPDATE events SET status='unknown',owner=NULL,receipt=? WHERE status='sending'",
                    (
                        json.dumps(
                            asdict(
                                DispatchReceipt(
                                    "unknown",
                                    detail="scheduler lease lost during dispatch; reconcile before retry",
                                )
                            )
                        ),
                    ),
                )
            db.execute("UPDATE lease SET owner=?,expires=? WHERE id=1", (owner, now + seconds))
        return True

    def renew_lease(self, owner: str, *, now: float, seconds: float = 30) -> None:
        now = _timestamp(now)
        if not math.isfinite(seconds) or seconds <= 0:
            raise ValueError("positive finite lease duration required")
        with self._transaction() as db:
            self._require_lease(db, owner, now)
            db.execute("UPDATE lease SET expires=? WHERE id=1", (now + seconds,))

    def release_lease(self, owner: str) -> None:
        with self._transaction() as db:
            db.execute("UPDATE lease SET owner=NULL,expires=0 WHERE id=1 AND owner=?", (owner,))

    def materialize_due(self, owner: str, *, now: float) -> list[DispatchEvent]:
        """Persist due business occurrences and advance cursors atomically.

        Downtime coalesces into one range. Heartbeats also merge into the single
        unsent occurrence. Unknown external outcomes block further occurrences.
        """
        from .scheduler import due_window

        now = _timestamp(now)
        touched: list[str] = []
        with self._transaction() as db:
            self._require_lease(db, owner, now)
            if self.is_autonomy_paused():
                return []
            for row in db.execute("SELECT definition FROM jobs ORDER BY id").fetchall():
                job = self._job(row[0])
                if not job.enabled or job.next_due is None or job.next_due > now:
                    continue
                if db.execute(
                    "SELECT 1 FROM events WHERE job_id=? AND status='unknown' LIMIT 1", (job.id,)
                ).fetchone():
                    continue
                first, last, following = due_window(job, now)
                existing = (
                    db.execute(
                        "SELECT id,due_at FROM events WHERE job_id=? AND status='pending' ORDER BY due_at LIMIT 1",
                        (job.id,),
                    ).fetchone()
                    if job.kind == "heartbeat"
                    else None
                )
                if existing is not None:
                    event_id = existing[0]
                    db.execute(
                        "UPDATE events SET through_at=?,catch_up=? WHERE id=?",
                        (last, int(job.catch_up and last > existing["due_at"]), event_id),
                    )
                else:
                    event_id = str(
                        uuid5(NAMESPACE_URL, f"anima:{job.id}:{job.revision}:{first.hex()}")
                    )
                    db.execute(
                        "INSERT INTO events VALUES (?,?,?,?,?,?,'pending',NULL,NULL)",
                        (
                            event_id,
                            job.id,
                            json.dumps(asdict(job)),
                            first,
                            last,
                            int(job.catch_up and last > first),
                        ),
                    )
                db.execute(
                    "UPDATE jobs SET definition=? WHERE id=?",
                    (json.dumps(asdict(replace(job, next_due=following))), job.id),
                )
                touched.append(event_id)
        return [self.get_event(event_id) for event_id in touched]

    def claim_event(self, event_id: str, owner: str, *, now: float) -> bool:
        with self._transaction() as db:
            self._require_lease(db, owner, _timestamp(now))
            if self.is_autonomy_paused():
                return False
            row = db.execute(
                "SELECT job_id,definition,status FROM events WHERE id=?", (event_id,)
            ).fetchone()
            if row is None or row["status"] != "pending":
                return False
            if db.execute(
                "SELECT 1 FROM events WHERE job_id=? AND status='unknown' LIMIT 1", (row["job_id"],)
            ).fetchone():
                return False
            job_row = db.execute(
                "SELECT definition FROM jobs WHERE id=?", (row["job_id"],)
            ).fetchone()
            if job_row is None:
                return False
            current, planned = self._job(job_row[0]), self._job(row["definition"])
            if not current.enabled or current.revision != planned.revision:
                return False
            db.execute("UPDATE events SET status='claimed',owner=? WHERE id=?", (owner, event_id))
            return True

    def mark_sending(self, event_id: str, owner: str, *, now: float) -> bool:
        with self._transaction() as db:
            self._require_lease(db, owner, _timestamp(now))
            if self.is_autonomy_paused():
                db.execute(
                    "UPDATE events SET status='pending',owner=NULL WHERE id=? AND status='claimed' AND owner=?",
                    (event_id, owner),
                )
                return False
            return (
                db.execute(
                    "UPDATE events SET status='sending' WHERE id=? AND status='claimed' AND owner=?",
                    (event_id, owner),
                ).rowcount
                == 1
            )

    def release_claim(self, event_id: str, owner: str) -> None:
        with self._transaction() as db:
            db.execute(
                "UPDATE events SET status='pending',owner=NULL WHERE id=? AND status='claimed' AND owner=?",
                (event_id, owner),
            )

    def defer_event(self, event_id: str, owner: str, *, now: float) -> DispatchEvent:
        """Return a definitively unsent dispatch to the same pending occurrence.

        Only its current leased owner may attest that sending had no external
        effect. Changed or deleted definitions cancel the old occurrence; an
        unknown result or a durable receipt can never be reset for replay.
        """
        with self._transaction() as db:
            self._require_lease(db, owner, _timestamp(now))
            row = db.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
            if row is None:
                raise KeyError(event_id)
            if row["status"] != "sending" or row["owner"] != owner:
                raise ValueError("only an owned sending event can be deferred")
            job_row = db.execute(
                "SELECT definition FROM jobs WHERE id=?", (row["job_id"],)
            ).fetchone()
            current = self._job(job_row[0]) if job_row is not None else None
            planned = self._job(row["definition"])
            status = (
                "pending"
                if current is not None and current.enabled and current.revision == planned.revision
                else "cancelled"
            )
            db.execute(
                "UPDATE events SET status=?,owner=NULL,receipt=NULL WHERE id=?",
                (status, event_id),
            )
        return self.get_event(event_id)

    def record_receipt(self, event_id: str, receipt: DispatchReceipt) -> DispatchEvent:
        """Record explicit acknowledgement/completion or reconcile unknown state.

        No status here causes an automatic retry. Failed is a confirmed failure;
        unknown requires external inspection. Accepted may later complete/fail.
        """
        if not isinstance(receipt, DispatchReceipt) or receipt.status not in {
            "accepted",
            "completed",
            "failed",
            "unknown",
        }:
            raise ValueError("invalid dispatch receipt")
        with self._transaction() as db:
            row = db.execute("SELECT status,receipt FROM events WHERE id=?", (event_id,)).fetchone()
            if row is None:
                raise KeyError(event_id)
            old = row[0]
            if old in {"completed", "failed", "cancelled"} and old != receipt.status:
                raise ValueError(f"cannot change terminal receipt {old} to {receipt.status}")
            if old in {"pending", "claimed", "cancelled"}:
                raise ValueError("unsent event cannot have a delivery receipt")
            if row["receipt"]:
                prior = DispatchReceipt(**json.loads(row["receipt"]))
                receipt = replace(
                    receipt,
                    thread_id=receipt.thread_id or prior.thread_id,
                    turn_id=receipt.turn_id or prior.turn_id,
                )
            db.execute(
                "UPDATE events SET status=?,receipt=?,owner=NULL WHERE id=?",
                (receipt.status, json.dumps(asdict(receipt)), event_id),
            )
        return self.get_event(event_id)

    def get_event(self, event_id: str) -> DispatchEvent:
        rows = self._rows("SELECT * FROM events WHERE id=?", (event_id,))
        if not rows:
            raise KeyError(event_id)
        return self._event(rows[0])

    def list_events(
        self, *, status: str | None = None, job_id: str | None = None
    ) -> list[DispatchEvent]:
        clauses, args = [], []
        if status is not None:
            clauses.append("status=?")
            args.append(status)
        if job_id is not None:
            clauses.append("job_id=?")
            args.append(job_id)
        suffix = " WHERE " + " AND ".join(clauses) if clauses else ""
        return [
            self._event(row)
            for row in self._rows(
                "SELECT * FROM events" + suffix + " ORDER BY due_at,id", tuple(args)
            )
        ]
