"""Real localhost HTTP fixtures, never production Zhihu or cookie stores."""

from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
from urllib.request import urlopen

import pytest

from alice_codex.collector import collect_collection, collect_zhihu_answers
from alice_codex.business import reconcile_publication


@contextmanager
def endpoint(routes):
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(
                {"path": self.path, "authorization_present": "Authorization" in self.headers}
            )
            status, value, extra = routes[self.path]
            body = value if isinstance(value, bytes) else json.dumps(value).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            for name, content in extra.items():
                self.send_header(name, str(content))
            if "Content-Length" not in extra:
                self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            self.close_connection = True

        def log_message(self, format, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
    )
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive()


def page(items, next_link=None):
    return 200, {"data": items, "paging": {"is_end": next_link is None, "next": next_link}}, {}


def collect(base, **options):
    return collect_collection(
        base + "/answers", subject="test-member", collection="answers", **options
    )


def test_real_http_follows_page_two_and_retains_true_zero(monkeypatch):
    monkeypatch.setenv("ALICE_FIXTURE_AUTH", "Bearer fixture-only-value")
    with endpoint(
        {
            "/answers": page(
                [{"id": "a", "voteup_count": 0, "comment_count": 0}],
                "/answers?cursor=private-cursor",
            ),
            "/answers?cursor=private-cursor": page(
                [{"id": "b", "voteup_count": 7, "comment_count": 2}]
            ),
        }
    ) as (base, requests):
        result = collect(base, header_env={"Authorization": "ALICE_FIXTURE_AUTH"})
    assert requests == [
        {"path": "/answers", "authorization_present": True},
        {"path": "/answers?cursor=private-cursor", "authorization_present": True},
    ]
    assert result["summary"]["complete"] is True
    assert result["summary"]["metrics"]["voteup_count"] == {
        "state": "known",
        "value": 7,
        "observed_sum": 7,
        "known_items": 2,
        "unknown_items": [],
    }
    assert result["observation"]["pages"][0]["items"][0]["voteup_count"] == 0
    serialized = json.dumps(result)
    assert "private-cursor" not in serialized
    assert "fixture-only-value" not in serialized


@pytest.mark.parametrize(
    "missing",
    [
        {"id": "missing"},
        {"id": "missing", "voteup_count": None},
        {"id": "missing", "voteup_count": "0"},
    ],
)
def test_missing_null_or_string_counter_is_unknown_even_beside_real_zero(missing):
    with endpoint({"/answers": page([missing, {"id": "zero", "voteup_count": 0}])}) as (base, _):
        result = collect(base, required_metrics=("voteup_count",))
    assert result["summary"]["coverage"]["pagination_complete"] is True
    assert result["summary"]["metrics"]["voteup_count"] == {
        "state": "unknown",
        "value": None,
        "observed_sum": 0,
        "known_items": 1,
        "unknown_items": ["missing"],
    }
    assert result["summary"]["complete"] is False


def test_forbidden_second_page_keeps_observation_but_total_is_unknown():
    with endpoint(
        {
            "/answers": page([{"id": "a", "voteup_count": 4}], "/answers?page=2"),
            "/answers?page=2": (403, b"private response body", {}),
        }
    ) as (base, requests):
        result = collect(base, required_metrics=("voteup_count",))
    assert len(requests) == 2
    assert result["summary"]["metrics"]["voteup_count"]["value"] is None
    assert result["summary"]["metrics"]["voteup_count"]["observed_sum"] == 4
    assert result["fetches"][-1]["http_status"] == 403
    assert result["fetches"][-1]["error"] == "http_error"
    assert "private response body" not in json.dumps(result)


def test_http_body_truncation_is_not_a_successful_empty_collection():
    body = json.dumps({"data": [], "paging": {"is_end": True}}).encode()
    with endpoint({"/answers": (200, body, {"Content-Length": len(body) + 12})}) as (base, _):
        result = collect(base)
    assert result["fetches"][0]["error"] == "truncated_response"
    assert result["summary"]["complete"] is False


def test_cumulative_response_budget_stops_many_small_pages():
    first = page([{"id": "a", "voteup_count": 1}], "/answers?page=2")
    with endpoint(
        {"/answers": first, "/answers?page=2": page([{"id": "b", "voteup_count": 2}])}
    ) as (base, _):
        result = collect(
            base,
            required_metrics=("voteup_count",),
            max_total_bytes=len(json.dumps(first[1]).encode()) + 4,
        )
    assert result["fetches"][-1]["error"] == "total_byte_limit"
    assert result["summary"]["metrics"]["voteup_count"]["observed_sum"] == 1
    assert result["summary"]["metrics"]["voteup_count"]["value"] is None


def test_invalid_counter_does_not_copy_arbitrary_remote_body_to_output():
    with endpoint(
        {"/answers": page([{"id": "a", "voteup_count": {"private": "untrusted remote body"}}])}
    ) as (base, _):
        result = collect(base, required_metrics=("voteup_count",))
    assert result["observation"]["pages"][0]["items"] == [
        {"id": "a", "voteup_count": {"invalid_type": "dict"}}
    ]
    assert "untrusted remote body" not in json.dumps(result)
    assert result["summary"]["metrics"]["voteup_count"]["state"] == "unknown"


@pytest.mark.parametrize(
    "option, expected_error",
    [
        ({"max_pages": 1}, "page_limit"),
        ({"max_items": 1}, "item_limit"),
        ({"max_response_bytes": 8}, "response_too_large"),
    ],
)
def test_resource_limit_never_claims_complete(option, expected_error):
    with endpoint({"/answers": page([{"id": "a", "voteup_count": 0}], "/answers?page=2")}) as (
        base,
        requests,
    ):
        result = collect(base, required_metrics=("voteup_count",), **option)
    assert len(requests) == 1
    assert result["summary"]["metrics"]["voteup_count"]["state"] == "unknown"
    assert result["fetches"][-1]["error"] == expected_error


def test_item_cap_in_the_final_page_is_still_incomplete():
    with endpoint(
        {"/answers": page([{"id": "a", "voteup_count": 0}, {"id": "b", "voteup_count": 9}])}
    ) as (base, _):
        result = collect(base, required_metrics=("voteup_count",), max_items=1)
    assert result["fetches"][-1]["error"] == "item_limit"
    assert result["summary"]["complete"] is False


@pytest.mark.parametrize(
    "body",
    [
        {"data": []},
        {"data": [], "paging": {"is_end": False}},
        {"data": [], "paging": {"is_end": "true"}},
    ],
)
def test_missing_pagination_metadata_does_not_prove_empty(body):
    with endpoint({"/answers": (200, body, {})}) as (base, _):
        result = collect(base)
    assert result["summary"]["complete"] is False
    assert result["summary"]["metrics"]["voteup_count"]["value"] is None


def test_independent_page_only_record_is_reported():
    independent = {
        "subject": "test-member",
        "collection": "answers",
        "source": "rendered-test-profile",
        "observed_at": "2026-09-06T00:00:00Z",
        "coverage": "complete",
        "items": [{"id": "web-only", "voteup_count": 3}],
    }
    with endpoint({"/answers": page([])}) as (base, _):
        result = collect(base, required_metrics=("voteup_count",), independent=independent)
    assert result["summary"]["coverage"]["pagination_complete"] is True
    assert result["summary"]["differences"] == [{"code": "independent_only", "id": "web-only"}]
    assert result["summary"]["metrics"]["voteup_count"]["value"] is None


def test_next_url_cannot_forward_credentials_to_another_origin(monkeypatch):
    monkeypatch.setenv("ALICE_FIXTURE_AUTH", "fixture-only-value")
    with endpoint({"/steal": page([])}) as (other, other_requests):
        with endpoint({"/answers": page([], other + "/steal")}) as (base, _):
            result = collect(base, header_env={"Authorization": "ALICE_FIXTURE_AUTH"})
    assert other_requests == []
    assert result["fetches"][-1]["error"] == "next_page_origin_rejected"
    assert result["summary"]["complete"] is False


def test_redirects_are_not_followed_even_on_the_same_origin():
    with endpoint({"/answers": (302, b"", {"Location": "/login"}), "/login": page([])}) as (
        base,
        requests,
    ):
        result = collect(base)
    assert len(requests) == 1
    assert result["fetches"][0]["error"] == "redirect_rejected"
    assert result["summary"]["complete"] is False


def test_next_link_cannot_silently_change_the_declared_collection_path():
    with endpoint(
        {"/answers": page([], "/different-member/answers"), "/different-member/answers": page([])}
    ) as (base, requests):
        result = collect(base)
    assert len(requests) == 1
    assert result["fetches"][-1]["error"] == "next_page_scope_rejected"
    assert result["summary"]["complete"] is False


def test_pagination_cycle_does_not_refetch_forever():
    with endpoint({"/answers": page([], "/answers")}) as (base, requests):
        result = collect(base)
    assert len(requests) == 1
    assert result["fetches"][-1]["error"] == "pagination_cycle"
    assert result["summary"]["complete"] is False


def test_zhihu_wrapper_uses_only_the_explicit_member_collection():
    with endpoint({"/api/v4/members/member-id/answers?offset=0&limit=20": page([])}) as (
        base,
        requests,
    ):
        result = collect_zhihu_answers("member-id", base_url=base)
    assert requests == [
        {
            "path": "/api/v4/members/member-id/answers?offset=0&limit=20",
            "authorization_present": False,
        }
    ]
    assert result["summary"]["subject"] == "member-id"
    assert result["summary"]["collection"] == "answers"
    assert result["summary"]["scope"] == "declared_api_collection"
    assert result["summary"]["coverage"]["independent_comparison"] == "not_provided"


def test_invalid_config_is_rejected_before_http_or_credential_lookup(monkeypatch):
    monkeypatch.delenv("ALICE_MISSING_FIXTURE_AUTH", raising=False)
    with endpoint({"/answers": page([])}) as (base, requests):
        with pytest.raises(ValueError, match="credential environment variable"):
            collect(base, header_env={"Cookie": "ALICE_MISSING_FIXTURE_AUTH"})
        with pytest.raises(ValueError, match="subject"):
            collect_zhihu_answers("../me", base_url=base)
        with pytest.raises(ValueError, match="loopback"):
            collect_collection("http://example.com/answers", subject="x", collection="answers")
    assert requests == []


@pytest.mark.parametrize(
    "headers,code,state",
    [
        ({"Age": "120", "Cache-Control": "public, max-age=60"}, "stale_cache", "stale"),
        ({"Age": "0", "Cache-Control": "max-age=0"}, "stale_cache", "stale"),
        ({"Warning": '110 synthetic-cache "Response is stale"'}, "stale_cache", "stale"),
        ({"Age": "10"}, "cache_freshness_unknown", "unknown"),
        ({"Age": "private-header-value"}, "cache_metadata_invalid", "unknown"),
        ({"Cache-Control": "max-age=private-header-value"}, "cache_metadata_invalid", "unknown"),
        ({"Cache-Control": 'max-age="60'}, "cache_metadata_invalid", "unknown"),
    ],
)
def test_http_cache_evidence_never_turns_stale_or_unknown_into_empty_zero(headers, code, state):
    status, value, _ = page([])
    with endpoint({"/answers": (status, value, headers)}) as (base, requests):
        result = collect(base)
    assert len(requests) == 1
    assert result["summary"]["coverage"]["freshness"] == state
    assert code in {issue["code"] for issue in result["summary"]["issues"]}
    assert result["summary"]["metrics"]["voteup_count"]["value"] is None
    assert result["fetches"][0]["bytes_received"] == len(json.dumps(value).encode())
    assert "private-header-value" not in json.dumps(result)


def test_fresh_http_cache_preserves_counts_but_respects_explicit_stricter_age():
    status, value, _ = page([{"id": "a", "voteup_count": 7}])
    with endpoint(
        {"/answers": (status, value, {"Age": "10", "Cache-Control": 'max-age="60"'})}
    ) as (base, requests):
        fresh = collect(base, required_metrics=("voteup_count",))
        stale = collect(base, required_metrics=("voteup_count",), max_age_seconds=5)
    assert len(requests) == 2
    assert fresh["summary"]["complete"]
    assert fresh["summary"]["coverage"]["freshness"] == "fresh"
    assert fresh["summary"]["metrics"]["voteup_count"]["value"] == 7
    assert stale["summary"]["coverage"]["freshness"] == "stale"
    assert stale["summary"]["metrics"]["voteup_count"]["value"] is None
    assert stale["summary"]["metrics"]["voteup_count"]["observed_sum"] == 7


def test_forbidden_and_truncated_http_failures_are_retained_in_observation_contract():
    with endpoint({"/answers": (403, b"unavailable", {})}) as (base, _):
        forbidden = collect(base)
    page_evidence = forbidden["observation"]["pages"][0]
    assert page_evidence["http_status"] == 403 and page_evidence["error"] == "http_error"
    assert "permission_denied" in {issue["code"] for issue in forbidden["summary"]["issues"]}
    with endpoint({"/answers": (200, b'{"data":[]}', {"Content-Length": 100})}) as (base, _):
        truncated = collect(base)
    assert truncated["observation"]["pages"][0]["truncated"] is True
    assert "observation_truncated" in {issue["code"] for issue in truncated["summary"]["issues"]}


@pytest.mark.parametrize("independent_status", [200, 403])
def test_two_real_http_sources_expose_disagreement_or_unavailable_comparison(independent_status):
    other = page([{"id": "a", "voteup_count": 9}]) if independent_status == 200 else (403, b"", {})
    with endpoint({"/answers": page([{"id": "a", "voteup_count": 0}]), "/rendered": other}) as (
        base,
        requests,
    ):
        separately_read = collect_collection(
            base + "/rendered",
            subject="test-member",
            collection="answers",
            required_metrics=("voteup_count",),
        )
        independent = {
            **separately_read["observation"]["pages"][0],
            "subject": "test-member",
            "collection": "answers",
            "coverage": "complete",
        }
        result = collect(base, required_metrics=("voteup_count",), independent=independent)
    assert len(requests) == 2
    assert result["summary"]["metrics"]["voteup_count"]["value"] is None
    if independent_status == 200:
        assert result["summary"]["differences"] == [
            {"code": "metric_disagreement", "id": "a", "metric": "voteup_count"}
        ]
    else:
        assert result["summary"]["coverage"]["independent_comparison"] == "unavailable"
        assert result["summary"]["differences"] == []


def test_business_result_is_checked_by_separate_http_readback_of_actual_visible_body():
    intended = "Synthetic public answer with complete evidence."
    intent = {
        "action_id": "synthetic-action",
        "subject": "synthetic-account",
        "target": "question",
        "status": "unknown",
        "external_id": "synthetic-answer",
        "content_sha256": hashlib.sha256(intended.encode()).hexdigest(),
        "sent_at": "2026-09-06T00:00:00Z",
    }
    binding = {key: intent[key] for key in ("action_id", "subject", "target", "external_id")}
    routes = {
        "/submission-ack": (
            201,
            {
                **binding,
                "kind": "submission_ack",
                "visible": True,
                "content_sha256": intent["content_sha256"],
            },
            {},
        ),
        "/public-object": (
            200,
            {**binding, "body": "Synthetic incomplete answer.", "visible": True},
            {},
        ),
    }
    with endpoint(routes) as (base, requests):

        def read_evidence(path, is_readback):
            with urlopen(base + path, timeout=2) as response:
                actual = json.load(response)
            if is_readback:
                actual["kind"] = "read_back"
                actual["content_sha256"] = hashlib.sha256(actual.pop("body").encode()).hexdigest()
            return {
                "source": base + path,
                "observed_at": datetime.now(timezone.utc).isoformat(),
                "receipts": [actual],
            }

        assert (
            reconcile_publication(intent, read_evidence("/submission-ack", False))["status"]
            == "needs_reconciliation"
        )
        wrong = reconcile_publication(intent, read_evidence("/public-object", True))
        assert wrong["status"] == "conflict" and not wrong["retry_allowed"]
        routes["/public-object"][1]["body"] = intended
        corrected = reconcile_publication(intent, read_evidence("/public-object", True))
        assert corrected["status"] == "confirmed" and corrected["temporal_check"] == "checked"
    assert len(requests) == 3  # All requests were loopback GET; no publication was attempted.


def test_empty_remote_identity_is_a_missing_record_not_a_collector_exception():
    with endpoint({"/answers": page([{"id": "", "voteup_count": 0}])}) as (base, _):
        result = collect(base, required_metrics=("voteup_count",))
    assert not result["summary"]["complete"]
    assert "item_missing_id" in {issue["code"] for issue in result["summary"]["issues"]}


@pytest.mark.parametrize("max_age", [True, float("nan"), 10**400])
def test_invalid_freshness_limit_is_rejected_before_fetch(max_age):
    with endpoint({"/answers": page([])}) as (base, requests):
        with pytest.raises(ValueError, match="finite nonnegative"):
            collect(base, max_age_seconds=max_age)
    assert requests == []
