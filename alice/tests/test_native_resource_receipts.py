"""Independent receipt judge plus synthetic rejection tests; no native runtime.

Native acceptance imports ``validate_native_resource_receipts`` and supplies
child IDs and epoch coverage derived from public native thread evidence. The
wrapper records actual received frames; this judge reads the installed ledger
with SQLite read-only access and does not import resource-accounting code.
"""

from collections import Counter
import hashlib
import json
from pathlib import Path
import sqlite3

import pytest


class ReceiptEvidenceError(AssertionError):
    """Incomplete or inconsistent native receipt evidence cannot pass."""


def _require(condition, message):
    if not condition:
        raise ReceiptEvidenceError(message)


def _canonical(value):
    # JSON payload identity is UTF-8, Unicode preserved, lexicographically sorted
    # object keys, compact separators and finite numbers. Receipt identity is
    # SHA256 of the JSON array [epoch ID, payload SHA256], under the same encoding.
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )


def _sha(value):
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def validate_native_resource_receipts(
    log_path, db_path, *, expected_child_thread_ids, expected_epoch_threads
):
    """Require exact observed-frame/ledger agreement across a service restart.

    ``expected_epoch_threads`` maps independently known epoch IDs to nonempty
    sets of threads that must have observations in that epoch. It must cover all
    expected children and at least two epochs. Repeated identical native frames
    legitimately share one event and one default receipt within their epoch.
    Returned data contains counts and hashes, never native thread/epoch IDs.
    """
    children = set(expected_child_thread_ids)
    coverage = {epoch: set(threads) for epoch, threads in expected_epoch_threads.items()}
    _require(bool(children), "Expected native children must be nonempty")
    _require(len(coverage) >= 2, "Restart evidence requires at least two expected epochs")
    _require(all(coverage.values()), "Every expected epoch needs independent thread coverage")
    _require(children <= set().union(*coverage.values()), "Epoch expectations omit children")
    log_bytes = Path(log_path).read_bytes()
    _require(bool(log_bytes), "Native token frame evidence is empty")
    records = [json.loads(line) for line in log_bytes.splitlines()]
    frames, final, per_epoch, threads = {}, {}, Counter(), {}
    for record in records:
        epoch = record.get("epoch_id")
        _require(epoch in coverage, "Unexpected native observation epoch")
        _require(epoch not in final, "Native observations continue after epoch close")
        if record.get("kind") == "close":
            _require("observer_error" in record and record["observer_error"] is None,
                     "Native resource observer error witness is missing or failed")
            _require(record.get("write_failures") == 0, "Native frame capture had write failures")
            _require(record.get("pending_count") == 0 and record.get("unresolved") == [],
                     "Native resource listener closed with unresolved observations")
            _require(record.get("token_frames") == per_epoch[epoch],
                     "Captured frame count does not match observer close witness")
            final[epoch] = record
            continue
        _require(record.get("kind") == "token", "Unexpected native evidence record kind")
        event = record.get("event", {})
        _require(event.get("method") == "thread/tokenUsage/updated", "Evidence is not a token event")
        params = event.get("params")
        _require(isinstance(params, dict), "Native token parameters are not an object")
        thread, turn = params.get("threadId"), params.get("turnId")
        _require(isinstance(thread, str) and bool(thread), "Native token frame lacks thread ID")
        _require(isinstance(turn, str) and bool(turn), "Native token frame lacks turn ID")
        total = params.get("tokenUsage", {}).get("total", {})
        _require(isinstance(total, dict) and all(
            type(total.get(field)) is int and 0 <= total[field] <= 2**53
            for field in ("inputTokens", "cachedInputTokens", "outputTokens",
                          "reasoningOutputTokens", "totalTokens")
        ), "Native token usage is unknown")
        fingerprint = _sha(params)
        frames[(epoch, fingerprint)] = (thread, turn, _canonical(params))
        per_epoch[epoch] += 1
        threads.setdefault(epoch, set()).add(thread)
    _require(set(final) == set(coverage), "Expected epoch close evidence is missing")
    _require(set(threads) == set(coverage), "Expected epoch has no token observations")
    _require(children <= set().union(*threads.values()), "Expected child token observations missing")
    for epoch, required in coverage.items():
        _require(required <= threads[epoch], "Independent thread coverage missing in expected epoch")

    uri = Path(db_path).resolve().as_uri() + "?mode=ro"
    with sqlite3.connect(uri, uri=True) as database:
        database.execute("PRAGMA query_only=ON")
        event_rows = database.execute(
            "SELECT epoch,fingerprint,thread,turn,payload,state FROM token_epoch_events"
        ).fetchall()
        receipt_rows = database.execute(
            "SELECT id,epoch,fingerprint FROM token_epoch_receipts"
        ).fetchall()
    events = {(epoch, fingerprint): (thread, turn, payload, status)
              for epoch, fingerprint, thread, turn, payload, status in event_rows}
    _require(len(events) == len(event_rows), "Duplicate ledger event identities")
    _require(set(events) == set(frames), "Ledger events differ from captured epoch/payload pairs")
    for key, (thread, turn, payload) in frames.items():
        actual_thread, actual_turn, actual_payload, status = events[key]
        _require(status == "known", "Ledger token observation is not known")
        _require((actual_thread, actual_turn) == (thread, turn), "Ledger thread/turn differs from frame")
        _require(actual_payload == payload, "Ledger payload differs from canonical native frame")
        _require(hashlib.sha256(actual_payload.encode("utf-8")).hexdigest() == key[1],
                 "Ledger payload fingerprint mismatch")
    expected_receipts = {(_sha([epoch, fingerprint]), epoch, fingerprint)
                         for epoch, fingerprint in frames}
    _require(len(set(receipt_rows)) == len(receipt_rows), "Duplicate ledger receipt identities")
    _require(set(receipt_rows) == expected_receipts, "Ledger receipts missing, extra or misbound")
    inventory = sorted([epoch, fingerprint, _sha([epoch, fingerprint])]
                       for epoch, fingerprint in frames)
    return {
        "native_frames": sum(per_epoch.values()),
        "unique_epoch_payloads": len(frames),
        "duplicate_frames": sum(per_epoch.values()) - len(frames),
        "ledger_events": len(events),
        "ledger_receipts": len(receipt_rows),
        "epochs": len(coverage),
        "threads": len(set().union(*threads.values())),
        "expected_children": len(children),
        "unknown_events": 0,
        "unresolved_frames": 0,
        "per_epoch": [
            {"epoch_sha256": hashlib.sha256(epoch.encode("utf-8")).hexdigest(),
             "native_frames": per_epoch[epoch], "threads": len(threads[epoch]),
             "unique_epoch_payloads": sum(key[0] == epoch for key in frames)}
            for epoch in sorted(coverage)
        ],
        "source_log_sha256": hashlib.sha256(log_bytes).hexdigest(),
        "receipt_inventory_sha256": _sha(inventory),
    }


def _fixture(tmp_path):
    log_path, database_path = tmp_path / "received.jsonl", tmp_path / "resources.sqlite3"
    records, event_rows, receipt_rows = [], [], []
    for epoch, child in (("epoch-before", "child-first"), ("epoch-after", "child-second")):
        params = {
            "threadId": child, "turnId": "turn-synthetic",
            "tokenUsage": {"total": {
                "inputTokens": 0, "cachedInputTokens": 0, "outputTokens": 0,
                "reasoningOutputTokens": 0, "totalTokens": 0,
            }},
            "syntheticLabel": "归属",
        }
        payload = json.dumps(params, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        fingerprint = hashlib.sha256(payload.encode()).hexdigest()
        receipt = hashlib.sha256(
            json.dumps([epoch, fingerprint], separators=(",", ":")).encode()
        ).hexdigest()
        # Match the real regression's 35 repeated frames, without calling the ledger.
        records.extend({"kind": "token", "epoch_id": epoch,
                        "event": {"method": "thread/tokenUsage/updated", "params": params}}
                       for _ in range(35))
        records.append({"kind": "close", "epoch_id": epoch, "token_frames": 35,
                        "write_failures": 0, "pending_count": 0, "unresolved": [],
                        "observer_error": None})
        event_rows.append((epoch, fingerprint, child, "turn-synthetic", payload, "known"))
        receipt_rows.append((receipt, epoch, fingerprint))
    log_path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in records))
    with sqlite3.connect(database_path) as database:
        database.execute("CREATE TABLE token_epoch_events(epoch, fingerprint, thread, turn, payload, state)")
        database.execute("CREATE TABLE token_epoch_receipts(id, epoch, fingerprint)")
        database.executemany("INSERT INTO token_epoch_events VALUES (?,?,?,?,?,?)", event_rows)
        database.executemany("INSERT INTO token_epoch_receipts VALUES (?,?,?)", receipt_rows)
    expectations = {
        "expected_child_thread_ids": {"child-first", "child-second"},
        "expected_epoch_threads": {"epoch-before": {"child-first"}, "epoch-after": {"child-second"}},
    }
    return log_path, database_path, expectations


def test_native_receipt_judge_accepts_replay_dedup_across_restart(tmp_path):
    log_path, database_path, expectations = _fixture(tmp_path)
    result = validate_native_resource_receipts(log_path, database_path, **expectations)
    assert result["native_frames"] == 70
    assert result["unique_epoch_payloads"] == result["ledger_receipts"] == 2
    assert result["duplicate_frames"] == 68
    assert result["epochs"] == result["expected_children"] == 2
    assert "child-first" not in json.dumps(result)
    assert "epoch-before" not in json.dumps(result)


@pytest.mark.parametrize("mutation", [
    "drop_receipt", "missing_child", "wrong_epoch", "wrong_receipt_hash",
    "unknown", "extra_event", "wrong_payload", "empty_log", "missing_epoch",
    "missing_observer_error", "observer_failed",
])
def test_native_receipt_judge_rejects_incomplete_evidence(tmp_path, mutation):
    log_path, database_path, expectations = _fixture(tmp_path)
    with sqlite3.connect(database_path) as database:
        if mutation == "drop_receipt":
            database.execute("DELETE FROM token_epoch_receipts WHERE epoch='epoch-before'")
        elif mutation == "missing_child":
            expectations["expected_child_thread_ids"].add("missing-child")
            expectations["expected_epoch_threads"]["epoch-after"].add("missing-child")
        elif mutation == "wrong_epoch":
            database.execute("UPDATE token_epoch_events SET epoch='wrong-epoch' WHERE epoch='epoch-before'")
        elif mutation == "wrong_receipt_hash":
            database.execute("UPDATE token_epoch_receipts SET id='wrong-hash' WHERE epoch='epoch-before'")
        elif mutation == "unknown":
            database.execute("UPDATE token_epoch_events SET state='unknown' WHERE epoch='epoch-before'")
        elif mutation == "extra_event":
            database.execute("INSERT INTO token_epoch_events VALUES ('extra','hash','thread','turn','{}','known')")
        elif mutation == "wrong_payload":
            database.execute("UPDATE token_epoch_events SET payload='{}' WHERE epoch='epoch-before'")
        elif mutation == "empty_log":
            log_path.write_text("")
        elif mutation == "missing_epoch":
            rows = [json.loads(line) for line in log_path.read_text().splitlines()]
            log_path.write_text("".join(json.dumps(row) + "\n" for row in rows
                                        if row["epoch_id"] != "epoch-after"))
        elif mutation in {"missing_observer_error", "observer_failed"}:
            rows = [json.loads(line) for line in log_path.read_text().splitlines()]
            close = next(row for row in rows if row["kind"] == "close")
            if mutation == "missing_observer_error":
                del close["observer_error"]
            else:
                close["observer_error"] = "Synthetic unresolved observer failure"
            log_path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    with pytest.raises(ReceiptEvidenceError):
        validate_native_resource_receipts(log_path, database_path, **expectations)
