"""Resource schema migration acceptance using only literal synthetic v1 databases."""

import hashlib
import json
import sqlite3

import pytest

from alice_codex.resources import ResourceError, ResourceLedger


LEGACY_TABLES = ("settings", "tokens", "checkpoints", "observations", "money", "settlements")
MIGRATION_KEY = "token_epoch_migration_v1_to_v2"
V1_SCHEMA = """
CREATE TABLE settings(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE tokens(id TEXT PRIMARY KEY, fingerprint TEXT UNIQUE NOT NULL,
    thread TEXT NOT NULL, turn TEXT NOT NULL, payload TEXT NOT NULL, state TEXT NOT NULL);
CREATE TABLE checkpoints(thread TEXT PRIMARY KEY, counters TEXT NOT NULL);
CREATE TABLE observations(id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL,
    period TEXT NOT NULL, summary TEXT NOT NULL, observed_at REAL NOT NULL);
CREATE TABLE money(seq INTEGER PRIMARY KEY, id TEXT UNIQUE NOT NULL,
    kind TEXT NOT NULL, amount INTEGER NOT NULL, source TEXT NOT NULL);
CREATE TABLE settlements(period TEXT PRIMARY KEY, receipt_id TEXT NOT NULL,
    count INTEGER NOT NULL, amount INTEGER NOT NULL);
PRAGMA user_version=1;
"""


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def usage(total=100, *, turn="fixture-turn"):
    return {
        "threadId": "fixture-thread",
        "turnId": turn,
        "tokenUsage": {
            "total": {
                "inputTokens": total - 10,
                "cachedInputTokens": 5,
                "outputTokens": 10,
                "reasoningOutputTokens": 3,
                "totalTokens": total,
            },
            "last": {"totalTokens": 10},
        },
    }


def database_snapshot(path):
    """Compare persisted contents and schema without normalizing stored JSON."""
    with sqlite3.connect(path) as db:
        tables = {
            name: statement
            for name, statement in db.execute(
                "SELECT name,sql FROM sqlite_master WHERE type='table' ORDER BY name"
            )
        }
        return {
            "version": db.execute("PRAGMA user_version").fetchone()[0],
            "schema": tables,
            "rows": {
                name: db.execute(f'SELECT * FROM "{name}" ORDER BY rowid').fetchall()
                for name in tables
            },
        }


@pytest.fixture
def v1(tmp_path):
    path = tmp_path / "synthetic-v1.sqlite3"
    with sqlite3.connect(path) as db:
        db.executescript(V1_SCHEMA)
        db.execute("INSERT INTO settings VALUES (?,?)", ("fixture_setting", '{ "keep" : true }'))
        for identity, total, turn in (
            ("legacy-receipt-a", 100, "fixture-turn"),
            ("legacy-receipt-b", 120, "fixture-next-turn"),
        ):
            event = usage(total, turn=turn)
            payload = encode(event)
            db.execute(
                "INSERT INTO tokens VALUES (?,?,?,?,?,?)",
                (
                    identity,
                    hashlib.sha256(payload.encode()).hexdigest(),
                    event["threadId"],
                    event["turnId"],
                    payload,
                    "known",
                ),
            )
        counters = {**usage(120)["tokenUsage"]["total"], "cacheWriteInputTokens": None}
        db.execute("INSERT INTO checkpoints VALUES (?,?)", ("fixture-thread", encode(counters)))
        db.execute(
            "INSERT INTO observations VALUES (?,?,?,?,?)",
            ("fixture-observation", "a" * 64, "2026-06-01", '{ "fixture" : "保留原文" }', 100),
        )
        db.execute(
            "INSERT INTO money VALUES (?,?,?,?,?)",
            (1, "legacy-money", "cost", 7, "synthetic invoice"),
        )
        db.execute(
            "INSERT INTO settlements VALUES (?,?,?,?)",
            ("2026-06-01", "fixture-observation", 3, 11),
        )
    return path


def test_v1_open_requires_explicit_migration_and_preserves_database(v1):
    before = v1.read_bytes()
    with pytest.raises(ResourceError, match="migrate_v1"):
        ResourceLedger(v1)
    assert v1.read_bytes() == before


def test_migration_retains_original_tables_rows_ids_and_json_without_guessing_epochs(v1):
    before = database_snapshot(v1)
    result = ResourceLedger.migrate_v1(v1)
    assert result["migrated"] is True
    receipt = result["receipt"]
    assert receipt["migration"] == "token_epochs_v1_to_v2"
    assert receipt["from_schema"] == 1 and receipt["to_schema"] == 2
    assert receipt["retained_rows"] == {name: len(before["rows"][name]) for name in LEGACY_TABLES}
    after = database_snapshot(v1)
    assert after["version"] == 2
    for name in LEGACY_TABLES:
        assert after["schema"][name] == before["schema"][name]
        preserved = after["rows"][name]
        if name == "settings":
            preserved = [row for row in preserved if row[0] != MIGRATION_KEY]
        assert preserved == before["rows"][name]
    stored_receipts = [row[1] for row in after["rows"]["settings"] if row[0] == MIGRATION_KEY]
    assert len(stored_receipts) == 1 and json.loads(stored_receipts[0]) == receipt
    assert after["rows"]["token_epoch_events"] == []
    assert after["rows"]["token_epoch_checkpoints"] == []
    assert ResourceLedger(v1).status()["virtual_budget_enabled"] is False


def test_fresh_v2_migration_is_a_noop_without_fabricating_a_receipt(tmp_path):
    path = tmp_path / "fresh-v2.sqlite3"
    ResourceLedger(path)
    before = database_snapshot(path)
    assert ResourceLedger.migrate_v1(path) == {"migrated": False, "receipt": None}
    assert database_snapshot(path) == before


def test_repeated_migration_keeps_original_receipt_and_all_new_records(v1):
    first = ResourceLedger.migrate_v1(v1)
    ledger = ResourceLedger(v1)
    ledger.record_money(
        "new-money", kind="income", amount_microusd=9, source="synthetic new receipt"
    )
    ledger.record_token_usage(usage(150, turn="legacy-new-turn"), event_id="new-legacy-token")
    ledger.record_token_usage(
        usage(40, turn="new-epoch-turn"), event_id="new-epoch-token", epoch_id="fixture-new-epoch"
    )
    before = database_snapshot(v1)
    result = ResourceLedger.migrate_v1(v1)
    assert result == {"migrated": False, "receipt": first["receipt"]}
    assert database_snapshot(v1) == before
    assert len(before["rows"]["money"]) == 2
    assert len(before["rows"]["tokens"]) == 3
    assert len(before["rows"]["token_epoch_events"]) == 1
    reopened = ResourceLedger(v1)
    assert not reopened.record_token_usage(
        usage(40, turn="new-epoch-turn"), event_id="new-epoch-token", epoch_id="fixture-new-epoch"
    )["recorded"]


def test_failure_after_epoch_table_creation_rolls_back_entire_migration(v1, monkeypatch):
    before = database_snapshot(v1)
    create_epoch_tables = ResourceLedger._create_epoch_tables

    def create_then_interrupt(db):
        create_epoch_tables(db)
        raise sqlite3.OperationalError("synthetic interrupted migration")

    with monkeypatch.context() as patch:
        patch.setattr(ResourceLedger, "_create_epoch_tables", staticmethod(create_then_interrupt))
        with pytest.raises(ResourceError, match="synthetic interrupted migration"):
            ResourceLedger.migrate_v1(v1)
    assert database_snapshot(v1) == before
    with pytest.raises(ResourceError, match="migrate_v1"):
        ResourceLedger(v1)
    assert ResourceLedger.migrate_v1(v1)["migrated"] is True


def test_migration_does_not_create_missing_database_or_parent_directory(tmp_path):
    path = tmp_path / "absent-parent" / "missing.sqlite3"
    with pytest.raises(ResourceError):
        ResourceLedger.migrate_v1(path)
    assert not path.exists() and not path.parent.exists()


def test_future_schema_rejected_without_modification(v1):
    with sqlite3.connect(v1) as db:
        db.execute("PRAGMA user_version=99")
    before = v1.read_bytes()
    for operation in (ResourceLedger, ResourceLedger.migrate_v1):
        with pytest.raises(ResourceError):
            operation(v1)
        assert v1.read_bytes() == before


@pytest.mark.parametrize("missing", LEGACY_TABLES)
def test_missing_v1_table_is_rejected_without_repair_or_partial_migration(v1, missing):
    with sqlite3.connect(v1) as db:
        db.execute(f'DROP TABLE "{missing}"')
    before = database_snapshot(v1)
    with pytest.raises(ResourceError):
        ResourceLedger.migrate_v1(v1)
    assert database_snapshot(v1) == before


def test_v1_with_partial_epoch_table_is_rejected_without_adopting_it(v1):
    with sqlite3.connect(v1) as db:
        db.execute("CREATE TABLE token_epoch_events(epoch TEXT, fingerprint TEXT)")
    before = database_snapshot(v1)
    with pytest.raises(ResourceError):
        ResourceLedger.migrate_v1(v1)
    assert database_snapshot(v1) == before


@pytest.mark.parametrize(
    "missing", ["token_epoch_events", "token_epoch_checkpoints", "token_epoch_receipts"]
)
def test_incomplete_v2_cannot_be_opened_or_reported_as_migrated(v1, missing):
    ResourceLedger.migrate_v1(v1)
    with sqlite3.connect(v1) as db:
        db.execute(f'DROP TABLE "{missing}"')
    before = database_snapshot(v1)
    for operation in (ResourceLedger, ResourceLedger.migrate_v1):
        with pytest.raises(ResourceError):
            operation(v1)
        assert database_snapshot(v1) == before


def test_migrated_receipt_ids_cannot_be_rebound_between_legacy_and_scoped_events(v1):
    ResourceLedger.migrate_v1(v1)
    ledger = ResourceLedger(v1)
    before = database_snapshot(v1)
    with pytest.raises(ResourceError, match="conflicting"):
        ledger.record_token_usage(usage(), event_id="legacy-receipt-a", epoch_id="fixture-epoch")
    assert database_snapshot(v1) == before
    assert not ledger.record_token_usage(usage(), event_id="legacy-receipt-a")["recorded"]
    ledger.record_token_usage(usage(140), event_id="new-scoped-id", epoch_id="fixture-epoch")
    before = database_snapshot(v1)
    with pytest.raises(ResourceError, match="conflicting"):
        ledger.record_token_usage(usage(140), event_id="new-scoped-id")
    assert database_snapshot(v1) == before


@pytest.mark.parametrize("version", [1, 2])
def test_same_named_table_with_missing_columns_is_not_a_valid_schema(v1, version):
    if version == 2:
        ResourceLedger.migrate_v1(v1)
    with sqlite3.connect(v1) as db:
        db.execute("DROP TABLE tokens")
        db.execute("CREATE TABLE tokens(id TEXT PRIMARY KEY)")
        db.execute("INSERT INTO tokens VALUES ('synthetic-incomplete-row')")
    before = database_snapshot(v1)
    for operation in (ResourceLedger, ResourceLedger.migrate_v1):
        with pytest.raises(ResourceError):
            operation(v1)
        assert database_snapshot(v1) == before


@pytest.mark.parametrize("value", [" null ", '{"synthetic":"existing"}', "not-json"])
def test_existing_migration_key_is_never_overwritten_even_with_null_value(v1, value):
    with sqlite3.connect(v1) as db:
        db.execute("INSERT INTO settings VALUES (?,?)", ("token_epoch_migration_v1_to_v2", value))
    before = database_snapshot(v1)
    with pytest.raises(ResourceError, match="conflict"):
        ResourceLedger.migrate_v1(v1)
    assert database_snapshot(v1) == before
