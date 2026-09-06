"""Host heartbeat evidence uses real loopback HTTP, synthetic data, and no model."""

from contextlib import contextmanager
from dataclasses import FrozenInstanceError, replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading

import pytest

from alice_codex.heartbeat import CollectionSpec, HostHeartbeatAdapter, compare_receipts


@contextmanager
def endpoint(routes):
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append({"path": self.path, "authorization": "Authorization" in self.headers})
            status, value, headers = routes[self.path]
            body = value if isinstance(value, bytes) else json.dumps(value).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            for key, value in headers.items():
                self.send_header(key, str(value))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive()


def page(items, next_link=None, **headers):
    return 200, {"data": items, "paging": {"is_end": next_link is None, "next": next_link}}, headers


def spec(url, **changes):
    return CollectionSpec(
        source_id="fixture-answers", url=url, subject="fixture-member", collection="answers",
        auth_context_version="fixture-account-v1", max_age_seconds=60,
        required_metrics=("voteup_count",), **changes,
    )


def adapter_for(source):
    adapter = HostHeartbeatAdapter()
    adapter.register("monitor", source)
    return adapter


def test_registration_is_explicit_and_unconfigured_is_not_an_empty_known_snapshot():
    receipt = HostHeartbeatAdapter().observe("monitor", now=100)
    assert receipt["state"] == "unconfigured"
    assert receipt["source_id"] is None and receipt["scope_sha256"] is None
    assert receipt["content_sha256"] is None
    assert compare_receipts(None, receipt) == {
        "state": "unconfigured", "accept": True, "update_last_good": False,
        "wake": False, "reason": "source_unconfigured",
    }
    with pytest.raises(ValueError, match="CollectionSpec"):
        HostHeartbeatAdapter().register("monitor", {"trusted": True, "complete": True})


@pytest.mark.parametrize(
    "change",
    [
        {"url": "http://public.example/answers"},
        {"url": "https://user:secret@example.invalid/answers"},
        {"url": "https://example.invalid/answers#fragment"},
        {"required_metrics": ["voteup_count"]},
        {"required_metrics": ("voteup_count", "voteup_count")},
        {"header_env": {"Authorization": "FIXTURE_AUTH"}},
        {"header_env": (("Host", "FIXTURE_HOST"),)},
        {"header_env": (("Authorization", "Bearer literal-secret"),)},
        {"header_env": (("Authorization", "FIXTURE_AUTH"), ("authorization", "FIXTURE_AUTH2"))},
        {"auth_context_version": ""},
        {"max_age_seconds": float("nan")},
        {"request_timeout": True},
        {"total_seconds": 0},
        {"max_pages": 1001},
    ],
)
def test_collection_spec_rejects_invalid_or_mutable_contract_before_collection(change):
    with pytest.raises(ValueError):
        replace(spec("https://example.invalid/answers"), **change)


def test_collection_spec_is_frozen_and_query_is_not_in_repr():
    source = spec("https://example.invalid/answers?filter=private-query")
    with pytest.raises(FrozenInstanceError):
        source.subject = "different-member"
    assert "private-query" not in repr(source)


def test_real_http_normalizes_order_duplicate_rows_and_observation_time():
    routes = {"/answers": page([{"id": "a", "voteup_count": 2}, {"id": "b", "voteup_count": 3}])}
    with endpoint(routes) as (base, requests):
        adapter = adapter_for(spec(base + "/answers"))
        first = adapter.observe("monitor", now=100)
        routes["/answers"] = page([
            {"id": "b", "voteup_count": 3}, {"id": "a", "voteup_count": 2},
            {"id": "a", "voteup_count": 2},
        ])
        second = adapter.observe("monitor", now=101)
    assert len(requests) == 2
    assert first["state"] == second["state"] == "known"
    assert first["content_sha256"] == second["content_sha256"]
    assert first["evidence_sha256"] != second["evidence_sha256"]
    assert first["observed_at"] != second["observed_at"]
    assert compare_receipts(None, first)["state"] == "baseline"
    assert compare_receipts(first, second) == {
        "state": "unchanged", "accept": True, "update_last_good": True,
        "wake": False, "reason": None,
    }


@pytest.mark.parametrize(
    "changed",
    [
        [{"id": "c", "voteup_count": 2}, {"id": "d", "voteup_count": 3}],
        [{"id": "a", "voteup_count": 1}, {"id": "b", "voteup_count": 4}],
    ],
)
def test_same_total_different_ids_or_per_item_metrics_is_new_evidence(changed):
    routes = {"/answers": page([{"id": "a", "voteup_count": 2}, {"id": "b", "voteup_count": 3}])}
    with endpoint(routes) as (base, _):
        adapter = adapter_for(spec(base + "/answers"))
        first = adapter.observe("monitor", now=100)
        routes["/answers"] = page(changed)
        second = adapter.observe("monitor", now=101)
    assert second["state"] == "known"
    assert first["content_sha256"] != second["content_sha256"]
    result = compare_receipts(first, second)
    assert result["state"] == "new_evidence" and result["wake"]
    assert "complete" not in result  # A changed snapshot is not a business-goal verdict.


@pytest.mark.parametrize("change", ["query", "authentication", "subject", "source"])
def test_scope_change_starts_new_baseline_despite_identical_items(change):
    routes = {"/answers?filter=first": page([]), "/answers?filter=second": page([])}
    with endpoint(routes) as (base, _):
        source = spec(base + "/answers?filter=first")
        adapter = adapter_for(source)
        first = adapter.observe("monitor", now=100)
        changes = {
            "query": {"url": base + "/answers?filter=second"},
            "authentication": {"auth_context_version": "fixture-account-v2"},
            "subject": {"subject": "different-member"},
            "source": {"source_id": "another-fixture"},
        }
        adapter.register("monitor", replace(source, **changes[change]))
        second = adapter.observe("monitor", now=101)
    assert first["state"] == second["state"] == "known"
    assert first["scope_sha256"] != second["scope_sha256"]
    assert compare_receipts(first, second)["state"] == "baseline"
    assert not compare_receipts(first, second)["wake"]


@pytest.mark.parametrize(
    "failed_page",
    [
        (403, b"private forbidden response", {}),
        (200, b"invalid response", {}),
        page([{"id": "missing-metric"}]),
        page([{"id": "duplicate", "voteup_count": 1}, {"id": "duplicate", "voteup_count": 2}]),
        page([], **{"Age": 3600, "Cache-Control": "max-age=60"}),
        page([], "/answers?missing-page=true"),
    ],
)
def test_failed_or_incomplete_collection_is_unknown_and_preserves_last_good(failed_page):
    routes = {"/answers": page([{"id": "a", "voteup_count": 2}])}
    with endpoint(routes) as (base, _):
        adapter = adapter_for(spec(base + "/answers", max_pages=1))
        good = adapter.observe("monitor", now=100)
        routes["/answers"] = failed_page
        unknown = adapter.observe("monitor", now=101)
        routes["/answers"] = page([{"id": "a", "voteup_count": 2}])
        recovered = adapter.observe("monitor", now=102)
    assert good["state"] == recovered["state"] == "known"
    assert unknown["state"] == "unknown" and unknown["content_sha256"] is None
    assert "private forbidden response" not in json.dumps(unknown)
    result = compare_receipts(good, unknown)
    assert result["accept"] and not result["update_last_good"] and not result["wake"]
    assert compare_receipts(unknown, recovered, last_good=good)["state"] == "unchanged"
    assert compare_receipts(unknown, recovered)["state"] == "baseline"


def test_credentials_are_read_only_from_environment_and_never_enter_receipts(monkeypatch):
    routes = {"/answers?filter=private-query": page([])}
    with endpoint(routes) as (base, requests):
        adapter = adapter_for(spec(
            base + "/answers?filter=private-query", header_env=(("Authorization", "FIXTURE_AUTH"),)
        ))
        monkeypatch.delenv("FIXTURE_AUTH", raising=False)
        missing = adapter.observe("monitor", now=100)
        assert missing["state"] == "unknown" and not requests
        monkeypatch.setenv("FIXTURE_AUTH", "Bearer fixture-secret")
        known = adapter.observe("monitor", now=101)
    assert known["state"] == "known"
    assert requests == [{"path": "/answers?filter=private-query", "authorization": True}]
    assert "fixture-secret" not in json.dumps(known) and "private-query" not in json.dumps(known)


def test_repeat_conflict_and_old_receipts_cannot_extend_wait_or_regress_baseline():
    with endpoint({"/answers": page([])}) as (base, _):
        adapter = adapter_for(spec(base + "/answers"))
        good = adapter.observe("monitor", now=100)
        old = adapter.observe("monitor", now=99)
        equal = adapter.observe("monitor", now=100)
    repeat = compare_receipts(good, dict(good))
    assert repeat["state"] == "duplicate" and not repeat["accept"]
    assert not repeat["update_last_good"] and not repeat["wake"]
    for receipt in (old, equal):
        stale = compare_receipts(good, receipt)
        assert stale["state"] == "stale" and not stale["accept"]
        assert not stale["update_last_good"] and not stale["wake"]
    with pytest.raises(ValueError, match="conflicting"):
        compare_receipts(good, {**good, "observed_at": 101})
    with pytest.raises(ValueError, match="same target"):
        compare_receipts(good, {**old, "target": "another-monitor"})


def test_unknown_receipt_cannot_reuse_a_known_digest_and_validator_change_resets_scope():
    with endpoint({"/answers": page([])}) as (base, _):
        adapter = adapter_for(spec(base + "/answers"))
        first = adapter.observe("monitor", now=100)
        later = adapter.observe("monitor", now=101)
    with pytest.raises(ValueError, match="must be absent"):
        compare_receipts(first, {**later, "state": "unknown", "reason": "collection_failed"})
    changed_validator = {**later, "validator_version": "collector-summary-v2"}
    assert compare_receipts(first, changed_validator)["state"] == "baseline"
