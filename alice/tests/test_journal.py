"""Synthetic native protocol fixtures; no real model, rollout or production data."""

import asyncio
import copy
import json

import pytest

from alice_codex.journal import NativeJournal
from alice_codex.memory import MemoryConflictError, MemoryError, MemoryStore
from alice_codex.rpc import RpcDisconnected, RpcError


def turn(identifier="turn-1", **changes):
    return {
        "id": identifier,
        "items": [],
        "itemsView": "notLoaded",
        "status": "completed",
        "error": None,
        "startedAt": 1788652800,
        "completedAt": 1788652802,
        "durationMs": 2000,
        **changes,
    }


def entry(identifier="message-1", text="Observed result", turn_id="turn-1"):
    return {
        "turnId": turn_id,
        "item": {"id": identifier, "type": "agentMessage", "text": text, "phase": "final"},
    }


def notification(value, thread="root"):
    return {
        "method": "item/completed",
        "params": {"threadId": thread, **value, "completedAtMs": 1788652802000},
    }


def records(memory):
    return [
        json.loads(p.read_text()) for p in sorted((memory.state / "events/raw").glob("*.jsonl"))
    ]


class NativePages:
    def __init__(self, pages=None):
        self.pages = pages or {}
        self.calls = []

    async def request(self, method, params):
        self.calls.append((method, copy.deepcopy(params)))
        value = self.pages.get(
            (params["threadId"], method, params.get("itemsView"), params["cursor"]),
            {"data": [], "nextCursor": None},
        )
        if isinstance(value, BaseException):
            raise value
        return copy.deepcopy(value)


def native(items=None, turns=None, thread="root"):
    return NativePages(
        {
            (thread, "thread/turns/list", "notLoaded", None): {
                "data": turns if turns is not None else [turn()],
                "nextCursor": None,
            },
            (thread, "thread/items/list", None, None): {
                "data": items if items is not None else [entry()],
                "nextCursor": None,
            },
        }
    )


def journal(tmp_path, rpc, **bounds):
    memory = MemoryStore(tmp_path / "alice")
    return NativeJournal(memory, rpc, lambda value: value in {"root", "child", "broken"}, **bounds)


def test_live_preserves_old_id_and_body_and_excludes_resources(tmp_path):
    subject = journal(tmp_path, native())
    value = entry()
    old_id = "item/completed:root:turn-1:message-1"
    old = subject.memory.append_event(
        old_id,
        {
            "kind": "item/completed",
            "thread_id": "root",
            "turn_id": "turn-1",
            "content": value["item"],
        },
    )
    receipt = subject.record_live(notification(value))
    assert receipt == {**old, "already_recorded": True}
    assert subject.record_live(notification(value, "unowned")) is None
    assert (
        subject.record_live(
            {
                "method": "thread/tokenUsage/updated",
                "params": {"threadId": "root", "tokenUsage": {"total": {"totalTokens": 42}}},
            }
        )
        is None
    )
    assert len(records(subject.memory)) == 1
    with pytest.raises(MemoryConflictError):
        subject.record_live(notification(entry(text="A changed notification")))


async def test_restart_backfills_missing_tail_once_without_fabricating_times(tmp_path):
    rpc = native(
        items=[entry("user", "Input"), entry("answer", "Tail after crash")],
        turns=[turn(startedAt=None, completedAt=None)],
    )
    subject = journal(tmp_path, rpc)
    subject.record_live(notification(entry("user", "Input")))
    subject = journal(tmp_path, rpc)  # restart; no journal checkpoint is required
    first = await subject.backfill(["root", "root"])
    assert first["threads"]["root"] == {
        "status": "scanned",
        "traversal_complete": True,
        "turns": 1,
        "items": 2,
        "pages": 2,
        "appended": 2,
        "already_archived": 0,
        "matched_live": 1,
        "gaps": [],
    }
    before = records(subject.memory)
    assert len(before) == 3
    snapshots = [r for r in before if r["kind"].startswith("native/history/")]
    assert all(r["provenance"]["timestamp_meaning"] == "archive_observation" for r in snapshots)
    assert all(r["provenance"]["notification_reconstructed"] is False for r in snapshots)
    recovered_turn = next(r for r in snapshots if r["kind"] == "native/history/turn")
    assert recovered_turn["provenance"]["native_times"] == {
        "started_at": None,
        "completed_at": None,
        "unit": "unix_seconds",
    }
    recovered_item = next(r for r in snapshots if r["kind"] == "native/history/item")
    assert recovered_item["provenance"]["native_times"] == {"occurred_at": None}
    subject = journal(tmp_path, rpc)
    second = await subject.backfill(["root"])
    assert second["threads"]["root"]["already_archived"] == 2
    assert second["threads"]["root"]["appended"] == 0
    assert records(subject.memory) == before
    assert first["traversal_complete"] and not first["notification_log_reconstructed"]


async def test_live_variant_and_later_canonical_revision_never_reuse_original_id(tmp_path):
    rpc = native()
    subject = journal(tmp_path, rpc)
    subject.record_live(notification(entry(text="Live text with delivery metadata")))
    await subject.backfill(["root"])
    first = records(subject.memory)
    subject.rpc = native(items=[entry(text="Canonical content was updated")])
    assert (await subject.backfill(["root"]))["threads"]["root"]["appended"] == 1
    after = records(subject.memory)
    original = next(r for r in after if r["kind"] == "item/completed")
    assert original["content"]["text"] == "Live text with delivery metadata"
    assert len(after) == len(first) + 1
    assert len({r["event_id"] for r in after}) == len(after)
    assert (await subject.backfill(["root"]))["threads"]["root"]["appended"] == 0


async def test_in_progress_snapshots_do_not_claim_completion(tmp_path):
    raw = entry()
    raw["item"] = {
        "id": "exec-1",
        "type": "commandExecution",
        "status": "inProgress",
        "command": "sleep 10",
        "aggregatedOutput": "partial",
    }
    subject = journal(tmp_path, native([raw], [turn(status="inProgress", completedAt=None)]))
    await subject.backfill(["root"])
    assert {r["kind"] for r in records(subject.memory)} == {
        "native/history/turn",
        "native/history/item",
    }
    assert all(r["content"]["status"] == "inProgress" for r in records(subject.memory))


async def test_all_pages_and_owned_children_are_scanned_but_no_unowned_rpc(tmp_path):
    rpc = native()
    rpc.pages[("root", "thread/turns/list", "notLoaded", None)]["nextCursor"] = "t2"
    rpc.pages[("root", "thread/turns/list", "notLoaded", "t2")] = {
        "data": [turn("turn-2")],
        "nextCursor": None,
    }
    rpc.pages[("root", "thread/items/list", None, None)]["nextCursor"] = "i2"
    rpc.pages[("root", "thread/items/list", None, "i2")] = {
        "data": [entry("m2", turn_id="turn-2")],
        "nextCursor": None,
    }
    rpc.pages.update(native(thread="child").pages)
    subject = journal(tmp_path, rpc)
    result = await subject.backfill(["root", "child", "stranger"])
    assert result["threads"]["root"]["turns"] == 2
    assert result["threads"]["root"]["items"] == 2
    assert result["threads"]["root"]["pages"] == 4
    assert result["threads"]["child"]["traversal_complete"]
    assert result["threads"]["stranger"]["status"] == "not_owned"
    assert all(params["threadId"] != "stranger" for _, params in rpc.calls)
    assert {r["thread_id"] for r in records(subject.memory)} == {"root", "child"}


@pytest.mark.parametrize(
    "error,reason",
    [
        (RpcError("no rollout found for thread id broken", -32600), "no_rollout"),
        (RpcDisconnected("socket closed"), "native_unavailable"),
        (TimeoutError("bounded query"), "native_unavailable"),
    ],
)
async def test_missing_history_does_not_block_other_root(tmp_path, error, reason):
    rpc = native()
    rpc.pages[("broken", "thread/turns/list", "notLoaded", None)] = error
    subject = journal(tmp_path, rpc)
    result = await subject.backfill(["broken", "root"])
    assert result["threads"]["broken"]["gaps"][0]["reason"] == reason
    assert not result["traversal_complete"]
    assert result["threads"]["root"]["appended"] == 2


@pytest.mark.parametrize(
    "bounds,reason", [({"max_pages": 1}, "page_limit"), ({"max_records": 1}, "record_limit")]
)
async def test_hard_bounds_report_partial_not_success(tmp_path, bounds, reason):
    result = await journal(tmp_path, native(), **bounds).backfill(["root"])
    assert result["threads"]["root"]["gaps"] == [{"reason": reason}]
    assert not result["traversal_complete"]


async def test_repeated_cursor_and_changed_page_are_reported(tmp_path):
    rpc = native()
    rpc.pages[("root", "thread/items/list", None, None)]["nextCursor"] = "again"
    rpc.pages[("root", "thread/items/list", None, "again")] = {
        "data": [entry(text="changed during traversal")],
        "nextCursor": "again",
    }
    subject = journal(tmp_path, rpc)
    report = (await subject.backfill(["root"]))["threads"]["root"]
    assert report["gaps"] == [
        {"reason": "item_changed_during_scan", "turn_id": "turn-1"},
        {"reason": "invalid_or_repeated_cursor"},
    ]
    assert report["status"] == "partial" and len(records(subject.memory)) == 2


@pytest.mark.parametrize("view,complete", [("full", True), ("summary", False)])
async def test_legacy_fallback_requires_explicit_full_items(tmp_path, view, complete):
    rpc = native()
    rpc.pages[("root", "thread/items/list", None, None)] = RpcError("unsupported", -32601)
    rpc.pages[("root", "thread/turns/list", "full", None)] = {
        "data": [turn(itemsView=view, items=[entry()["item"]])],
        "nextCursor": None,
    }
    subject = journal(tmp_path, rpc)
    report = (await subject.backfill(["root"]))["threads"]["root"]
    assert report["traversal_complete"] == complete
    assert report["items_source"] == "thread/turns/list:full"
    assert report["items"] == int(complete)
    if not complete:
        assert report["gaps"] == [{"reason": "full_items_unavailable"}]


async def test_write_failure_is_raised_and_restart_safely_retries(tmp_path, monkeypatch):
    subject = journal(tmp_path, native())
    original = subject.memory.append_event

    def fail_after_one(event_id, body, **kwargs):
        if body["kind"] == "native/history/turn":
            raise OSError("disk full")
        return original(event_id, body, **kwargs)

    monkeypatch.setattr(subject.memory, "append_event", fail_after_one)
    with pytest.raises(OSError, match="disk full"):
        await subject.backfill(["root"])
    restarted = journal(tmp_path, native())
    report = (await restarted.backfill(["root"]))["threads"]["root"]
    assert report["already_archived"] == 1 and report["appended"] == 1


async def test_archive_corruption_is_not_misclassified_as_content_variant(tmp_path):
    subject = journal(tmp_path, native())
    subject.record_live(notification(entry()))
    archive = next((subject.memory.state / "events/raw").glob("*.jsonl"))
    original = archive.read_bytes()
    archive.write_text('{"broken":true}')
    with pytest.raises(MemoryError, match="invalid"):
        await subject.backfill(["root"])
    assert archive.read_bytes() != original


async def test_cancellation_does_not_become_a_success_report(tmp_path):
    rpc = native()
    rpc.pages[("root", "thread/items/list", None, None)] = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await journal(tmp_path, rpc).backfill(["root"])


async def test_bounded_scan_prioritizes_newest_content_over_old_headers(tmp_path):
    rpc = native(items=[entry("latest", "Latest tail")])
    rpc.pages[("root", "thread/items/list", None, None)]["nextCursor"] = "older"
    rpc.pages[("root", "thread/items/list", None, "older")] = {
        "data": [entry("old", "Old content")],
        "nextCursor": None,
    }
    subject = journal(tmp_path, rpc, max_records=1)
    result = await subject.backfill(["root"])
    assert result["threads"]["root"]["gaps"] == [{"reason": "record_limit"}]
    assert [r["content"]["id"] for r in records(subject.memory)] == ["latest"]
    assert rpc.calls[0][0] == "thread/items/list"
    assert all(params["sortDirection"] == "desc" for _, params in rpc.calls)
