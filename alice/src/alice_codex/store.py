"""Durable business schedules and dispatch receipts, not a Codex input queue.

All mutations are SQLite transactions. Corrupt/unknown databases fail closed and
are never replaced by an empty store. Sending records are deliberately ambiguous
after lease loss: only reconciliation may resolve them, never automatic replay.
"""

from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
import json
import math
from pathlib import Path
import sqlite3
import time
from typing import Any, Iterator
from uuid import uuid4, uuid5, NAMESPACE_URL


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
