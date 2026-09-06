"""Timezone-aware timing and one leased dispatcher for business schedules."""

import asyncio
from datetime import datetime, timedelta
import math
import time
from typing import Callable, Protocol
from uuid import uuid4
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import croniter

from .store import DispatchEvent, DispatchReceipt, Job, LeaseLost, Store


def _at(job: Job) -> float:
    if isinstance(job.schedule_value, (int, float)):
        value = float(job.schedule_value)
    else:
        try:
            value = float(job.schedule_value)
        except ValueError:
            date = datetime.fromisoformat(job.schedule_value.replace("Z", "+00:00"))
            if date.tzinfo is None:
                date = date.replace(tzinfo=ZoneInfo(job.timezone))
                # Nonexistent DST local times must not silently move to another hour.
                if datetime.fromtimestamp(date.timestamp(), date.tzinfo).replace(
                    tzinfo=None
                ) != date.replace(tzinfo=None):
                    raise ValueError("at time does not exist in the selected timezone")
            value = date.timestamp()
    if not math.isfinite(value):
        raise ValueError("at timestamp must be finite")
    return value


def validate_job(job: Job) -> None:
    if (
        not isinstance(job.id, str)
        or not job.id
        or not isinstance(job.name, str)
        or not job.name.strip()
    ):
        raise ValueError("job id and name are required")
    if job.kind not in {"task", "heartbeat"} or job.schedule_type not in {"at", "every", "cron"}:
        raise ValueError("unsupported job kind or schedule type")
    if not isinstance(job.target, str) or not job.target or not isinstance(job.prompt, str):
        raise ValueError("target and prompt must be strings; target cannot be empty")
    if (
        type(job.enabled) is not bool
        or type(job.catch_up) is not bool
        or type(job.revision) is not int
        or job.revision < 1
    ):
        raise ValueError("invalid flags or revision")
    if isinstance(job.schedule_value, bool) or not isinstance(
        job.schedule_value, (str, float, int)
    ):
        raise ValueError("invalid schedule value")
    try:
        ZoneInfo(job.timezone)
    except (ZoneInfoNotFoundError, ValueError, TypeError) as exc:
        raise ValueError(f"unknown timezone: {job.timezone}") from exc
    if job.schedule_type == "at":
        _at(job)
    elif job.schedule_type == "every":
        seconds = float(job.schedule_value)
        if not math.isfinite(seconds) or seconds <= 0:
            raise ValueError("every interval must be finite and positive")
    elif (
        not isinstance(job.schedule_value, str)
        or len(job.schedule_value.split()) != 5
        or not croniter.is_valid(job.schedule_value)
    ):
        raise ValueError("cron must be a valid five-field expression")


def next_due(job: Job, now: float, *, initial: bool = False) -> float | None:
    """At is one-shot; every is phase-anchored; cron follows its IANA timezone."""
    validate_job(job)
    if not math.isfinite(now):
        raise ValueError("clock must be finite")
    if job.schedule_type == "at":
        return _at(job) if initial else None
    if job.schedule_type == "every":
        interval = float(job.schedule_value)
        base = now if initial or job.next_due is None else job.next_due
        if not initial and now < base:
            return base
        return (
            base + interval
            if initial
            else base + (max(0, math.floor((now - base) / interval)) + 1) * interval
        )
    return (
        croniter(job.schedule_value, datetime.fromtimestamp(now, ZoneInfo(job.timezone)))
        .get_next(datetime)
        .timestamp()
    )


def due_window(job: Job, now: float) -> tuple[float, float, float | None]:
    if job.next_due is None or job.next_due > now:
        raise ValueError("job is not due")
    first = float(job.next_due)
    following = next_due(job, now)
    if job.schedule_type == "every":
        interval = float(job.schedule_value)
        last = first + math.floor((now - first) / interval) * interval
    elif job.schedule_type == "cron":
        local = datetime.fromtimestamp(now, ZoneInfo(job.timezone)) + timedelta(microseconds=1)
        last = max(first, croniter(job.schedule_value, local).get_prev(datetime).timestamp())
    else:
        last = first
    return first, last, following


class Dispatcher(Protocol):
    async def is_busy(self, target: str) -> bool: ...

    async def dispatch(self, event: DispatchEvent) -> DispatchReceipt:
        """Return accepted only with a durable Codex acknowledgement/reference.

        completed requires observed terminal success. A transport exception is
        unknown, even if a request likely did not arrive. RejectedDispatch may
        be raised only when non-acceptance is positively known.
        """
        ...


class RejectedDispatch(Exception):
    """The receiver definitively did not accept the operation."""


class Scheduler:
    def __init__(
        self,
        store: Store,
        dispatcher: Dispatcher,
        *,
        clock: Callable[[], float] = time.time,
        owner: str | None = None,
        lease_seconds: float = 30,
        dispatch_timeout: float = 120,
    ):
        if (
            not math.isfinite(lease_seconds)
            or lease_seconds <= 0
            or not math.isfinite(dispatch_timeout)
            or dispatch_timeout <= 0
        ):
            raise ValueError("lease and dispatch timeouts must be finite and positive")
        self.store, self.dispatcher, self.clock = store, dispatcher, clock
        self.owner = owner or str(uuid4())
        self.lease_seconds, self.dispatch_timeout = lease_seconds, dispatch_timeout
        self._poll_lock = asyncio.Lock()

    async def _renew(self) -> None:
        while True:
            await asyncio.sleep(self.lease_seconds / 3)
            self.store.renew_lease(self.owner, now=self.clock(), seconds=self.lease_seconds)

    async def _deliver(self, event: DispatchEvent) -> DispatchReceipt:
        sending = asyncio.create_task(self.dispatcher.dispatch(event))
        renew = asyncio.create_task(self._renew())
        try:
            done, _ = await asyncio.wait(
                {sending, renew}, timeout=self.dispatch_timeout, return_when=asyncio.FIRST_COMPLETED
            )
            if renew in done:
                await renew  # A renewal failure must terminate dispatch and fail closed.
                raise LeaseLost("lease renewal ended unexpectedly")
            if sending not in done:
                raise TimeoutError("dispatch acknowledgement timed out")
            return await sending
        finally:
            sending.cancel()
            renew.cancel()
            await asyncio.gather(sending, renew, return_exceptions=True)

    async def poll(self) -> list[DispatchEvent]:
        """Run one bounded timing pass; callers own daemon sleep/lifecycle.

        Pausing blocks new dispatch but does not claim to interrupt existing
        Codex turns. This module owns only business plans and delivery receipts.
        """
        async with self._poll_lock:
            if not self.store.acquire_lease(
                self.owner, now=self.clock(), seconds=self.lease_seconds
            ):
                return []
            outcomes = []
            try:
                self.store.materialize_due(self.owner, now=self.clock())
                for event in self.store.list_events(status="pending"):
                    self.store.renew_lease(self.owner, now=self.clock(), seconds=self.lease_seconds)
                    if not self.store.claim_event(event.id, self.owner, now=self.clock()):
                        continue
                    try:
                        busy = await asyncio.wait_for(
                            self.dispatcher.is_busy(event.target),
                            timeout=min(self.dispatch_timeout, self.lease_seconds / 2),
                        )
                    except BaseException:
                        self.store.release_claim(event.id, self.owner)
                        raise
                    if busy:
                        self.store.release_claim(event.id, self.owner)
                        continue
                    if not self.store.mark_sending(event.id, self.owner, now=self.clock()):
                        continue
                    try:
                        receipt = await self._deliver(event)
                        if not isinstance(receipt, DispatchReceipt) or receipt.status not in {
                            "accepted",
                            "completed",
                            "failed",
                            "unknown",
                        }:
                            raise TypeError(
                                "dispatcher must return DispatchReceipt, never an implicit success"
                            )
                    except asyncio.CancelledError:
                        self.store.record_receipt(
                            event.id,
                            DispatchReceipt(
                                "unknown",
                                detail="dispatch cancelled before acknowledgement; reconcile",
                            ),
                        )
                        raise
                    except RejectedDispatch as exc:
                        receipt = DispatchReceipt("failed", detail=str(exc))
                    except Exception as exc:
                        receipt = DispatchReceipt("unknown", detail=f"{type(exc).__name__}: {exc}")
                    current = self.store.get_event(event.id)
                    # A completion notification may be persisted before the RPC
                    # acknowledgement returns. Never overwrite that stronger fact
                    # with late acceptance; unrelated receipt/storage errors still fail.
                    if receipt.status == "accepted" and current.status in {"completed", "failed"}:
                        outcomes.append(current)
                    else:
                        outcomes.append(self.store.record_receipt(event.id, receipt))
            finally:
                self.store.release_lease(self.owner)
            return outcomes
