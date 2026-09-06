"""Idempotent L0 observations of owned native history, not reconstructed notifications.

App-server item pages omit lifecycle timestamps; turn pages can have null times
and different hydration from notifications. Keep live IDs/bodies unchanged and
archive canonical variants under content-addressed snapshot IDs. A completed
scan means pagination ended, not that all historical notifications were recovered.
Reads are bounded scans; new L0 content is appended incrementally. The host should call
this at recovery/shutdown or for deliberately selected dirty threads, not each tick.
"""

from collections.abc import Callable, Iterable
import hashlib
import json

from .memory import MemoryError, MemoryStore
from .rpc import RpcClient, RpcError


LIVE_METHODS = {"turn/started", "turn/completed", "item/completed"}


class _Incomplete(Exception):
    def __init__(self, reason: str):
        self.reason = reason


def _digest(value: dict) -> str:
    raw = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()
    return hashlib.sha256(raw).hexdigest()


def _live_record(event: dict) -> tuple[str, dict]:
    method, params = event["method"], event["params"]
    thread = params["threadId"]
    entity = params.get("turn") or params.get("item") or {}
    if not isinstance(entity, dict) or not isinstance(entity.get("id"), str) or not entity["id"]:
        raise ValueError("Native event requires an entity ID")
    if method == "item/completed" and not isinstance(params.get("turnId"), str):
        raise ValueError("Native item requires its containing turn ID")
    event_id = ":".join([method, thread, params.get("turnId", ""), entity["id"]])
    return event_id, {
        "kind": method,
        "thread_id": thread,
        "turn_id": params.get("turnId", entity["id"]),
        "content": entity,
    }


class NativeJournal:
    def __init__(
        self,
        memory: MemoryStore,
        rpc: RpcClient,
        owns: Callable[[str], bool],
        *,
        page_size: int = 100,
        max_pages: int = 100,
        max_records: int = 10000,
    ):
        if any(type(n) is not int or n < 1 for n in (page_size, max_pages, max_records)):
            raise ValueError("Journal bounds must be positive integers")
        if page_size > 100:
            raise ValueError("Journal page size cannot exceed 100")
        self.memory, self.rpc, self.owns = memory, rpc, owns
        self.page_size, self.max_pages, self.max_records = page_size, max_pages, max_records

    def record_live(self, event: dict) -> dict | None:
        """Preserve the pre-journal notification identity and exact content.

        The existing event timestamp is archival observation time, not the
        native completedAtMs field. Resource/account notifications stay separate.
        """
        if event.get("method") not in LIVE_METHODS:
            return None
        thread = event.get("params", {}).get("threadId")
        if not isinstance(thread, str) or not self.owns(thread):
            return None
        event_id, body = _live_record(event)
        return self.memory.append_event(event_id, body)

    def _matching_live(self, event: dict) -> bool:
        event_id, body = _live_record(event)
        # This is a read of MemoryStore schema-1 event addressing, not a second
        # journal/index. append_event then verifies/re-materializes the exact
        # record and its receipt; search previews cannot prove exact equality.
        name = hashlib.sha256(event_id.encode()).hexdigest() + ".jsonl"
        path = self.memory.state / "events" / "raw" / name
        for part in (path, *path.parents):
            if part.is_symlink():
                raise MemoryError("Event archive symlinks are not allowed")
            if part == self.memory.state:
                break
        if not path.exists():
            return False
        try:
            record = json.loads(path.read_text())
            if record["event_id"] != event_id or record["_anima_ingest"]["schema_version"] != 1:
                raise ValueError()
        except (ValueError, TypeError, KeyError) as error:
            raise MemoryError("Event archive is invalid") from error
        original = {
            k: v for k, v in record.items() if k not in {"event_id", "timestamp", "_anima_ingest"}
        }
        # Do not hide archive corruption behind a content-variant classification.
        self.memory.append_event(event_id, original)
        return original == body

    def _archive(self, thread: str, turn_id: str, entity: dict, kind: str, report: dict) -> None:
        if not self.owns(thread):
            raise _Incomplete("ownership_changed")
        native_times = (
            {
                "started_at": entity.get("startedAt"),
                "completed_at": entity.get("completedAt"),
                "unit": "unix_seconds",
            }
            if kind == "turn"
            else {"occurred_at": None}
        )
        if kind == "item":
            notification = {
                "method": "item/completed",
                "params": {"threadId": thread, "turnId": turn_id, "item": entity},
            }
        elif entity.get("status") in {"completed", "failed", "interrupted"}:
            notification = {
                "method": "turn/completed",
                "params": {"threadId": thread, "turn": entity},
            }
        else:
            notification = None
        if notification is not None and self._matching_live(notification):
            report["matched_live"] += 1
            return
        links = (
            [f"item/completed:{thread}:{turn_id}:{entity['id']}"]
            if kind == "item"
            else [f"turn/started:{thread}::{turn_id}", f"turn/completed:{thread}::{turn_id}"]
        )
        body = {
            "kind": f"native/history/{kind}",
            "thread_id": thread,
            "turn_id": turn_id,
            "content": entity,
            "provenance": {
                "record_type": "canonical_snapshot",
                "source": "codex_app_server_history",
                "timestamp_meaning": "archive_observation",
                "native_times": native_times,
                "related_notification_ids": links,
                "notification_reconstructed": False,
            },
        }
        receipt = self.memory.append_event("native/history:" + _digest(body), body)
        report["already_archived" if receipt["already_recorded"] else "appended"] += 1

    async def _pages(self, method: str, params: dict, report: dict):
        cursor, seen = None, set()
        while True:
            if report["pages"] >= self.max_pages:
                raise _Incomplete("page_limit")
            if not self.owns(params["threadId"]):
                raise _Incomplete("ownership_changed")
            report["pages"] += 1
            page = await self.rpc.request(
                method, {**params, "cursor": cursor, "limit": self.page_size}
            )
            if not isinstance(page, dict) or not isinstance(page.get("data"), list):
                raise _Incomplete("malformed_page")
            if len(page["data"]) > self.page_size:
                raise _Incomplete("page_size_exceeded")
            for entity in page["data"]:
                if report["turns"] + report["items"] >= self.max_records:
                    raise _Incomplete("record_limit")
                yield entity
            cursor = page.get("nextCursor")
            if cursor is None:
                return
            if not isinstance(cursor, str) or not cursor or cursor in seen:
                raise _Incomplete("invalid_or_repeated_cursor")
            seen.add(cursor)

    def _turn(self, value: dict) -> dict:
        if (
            not isinstance(value, dict)
            or not isinstance(value.get("id"), str)
            or not value["id"]
            or value.get("status") not in {"inProgress", "completed", "failed", "interrupted"}
        ):
            raise _Incomplete("malformed_turn")
        return value

    def _item(self, thread: str, entry: dict, report: dict, seen: dict) -> None:
        if (
            not isinstance(entry, dict)
            or not isinstance(entry.get("turnId"), str)
            or not entry["turnId"]
            or not isinstance(entry.get("item"), dict)
        ):
            raise _Incomplete("malformed_item_entry")
        item, turn_id = entry["item"], entry["turnId"]
        if (
            not isinstance(item.get("id"), str)
            or not item["id"]
            or not isinstance(item.get("type"), str)
        ):
            raise _Incomplete("malformed_item")
        report["items"] += 1
        key, digest = (turn_id, item["id"]), _digest(item)
        if key in seen and seen[key] != digest:
            report["gaps"].append({"reason": "item_changed_during_scan", "turn_id": turn_id})
        seen[key] = digest
        self._archive(thread, turn_id, item, "item", report)

    async def backfill(self, thread_ids: Iterable[str]) -> dict:
        """Archive bounded canonical observations only for caller-selected owned IDs.

        Ownership discovery stays with CodexClient. No rollouts, unmaterialized
        threads, unavailable methods and pagination boundaries are per-thread
        gaps. Archive corruption/write errors propagate and must stop dispatch.
        Caller cancellation also propagates; a retry safely replays prior pages.
        """
        result = {
            "schema_version": 1,
            "scope": "requested_owned_threads",
            "traversal_complete": True,
            "notification_log_reconstructed": False,
            "timestamp_basis": "archive_observation",
            "scan_atomic": False,
            "limitations": [
                "Only currently persisted native representations are available",
                "Item lifecycle timestamps are absent from canonical pages",
                "Concurrent history changes can require another scan",
                "A snapshot and a later live notification are distinct observations",
            ],
            "threads": {},
        }
        for thread in dict.fromkeys(thread_ids):
            if not isinstance(thread, str) or not thread:
                raise ValueError("Journal thread IDs must be nonempty strings")
            report = {
                "status": "scanned",
                "traversal_complete": False,
                "turns": 0,
                "items": 0,
                "pages": 0,
                "appended": 0,
                "already_archived": 0,
                "matched_live": 0,
                "gaps": [],
            }
            result["threads"][thread] = report
            if not self.owns(thread):
                report.update(status="not_owned", gaps=[{"reason": "not_owned"}])
                result["traversal_complete"] = False
                continue
            try:
                seen_turns, seen_items = {}, {}
                # Recover recent content before metadata/old history. A bounded
                # scan must not repeatedly spend its entire allowance on the
                # oldest turn headers while missing the crash's newest tail.
                try:
                    async for entry in self._pages(
                        "thread/items/list", {"threadId": thread, "sortDirection": "desc"}, report
                    ):
                        self._item(thread, entry, report, seen_items)
                except RpcError as error:
                    if error.code != -32601 or report["items"]:
                        raise
                    # Legacy rollout stores can expose full Turn items while
                    # thread/items/list remains unsupported. Never accept summary.
                    report["items_source"] = "thread/turns/list:full"
                    async for raw in self._pages(
                        "thread/turns/list",
                        {"threadId": thread, "sortDirection": "desc", "itemsView": "full"},
                        report,
                    ):
                        turn = self._turn(raw)
                        if turn.get("itemsView") != "full" or not isinstance(
                            turn.get("items"), list
                        ):
                            raise _Incomplete("full_items_unavailable")
                        for item in turn["items"]:
                            if report["turns"] + report["items"] >= self.max_records:
                                raise _Incomplete("record_limit")
                            self._item(
                                thread, {"turnId": turn["id"], "item": item}, report, seen_items
                            )
                async for raw in self._pages(
                    "thread/turns/list",
                    {"threadId": thread, "sortDirection": "desc", "itemsView": "notLoaded"},
                    report,
                ):
                    turn = self._turn(raw)
                    report["turns"] += 1
                    digest = _digest(turn)
                    if turn["id"] in seen_turns and seen_turns[turn["id"]] != digest:
                        report["gaps"].append(
                            {"reason": "turn_changed_during_scan", "turn_id": turn["id"]}
                        )
                    seen_turns[turn["id"]] = digest
                    self._archive(thread, turn["id"], turn, "turn", report)
                report["traversal_complete"] = not report["gaps"]
                if report["gaps"]:
                    report["status"] = "partial"
            except _Incomplete as error:
                report.update(status="partial")
                report["gaps"].append({"reason": error.reason})
            except (RpcError, ConnectionError, TimeoutError) as error:
                reason = (
                    "no_rollout"
                    if isinstance(error, RpcError) and "no rollout found" in str(error)
                    else "native_unavailable"
                )
                report.update(status="unavailable")
                report["gaps"].append(
                    {
                        "reason": reason,
                        "error_type": type(error).__name__,
                        "rpc_code": error.code if isinstance(error, RpcError) else None,
                    }
                )
            result["traversal_complete"] &= report["traversal_complete"]
        return result
