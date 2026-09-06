"""Real localhost HTTP fixtures, never production Zhihu or cookie stores."""

from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading

import pytest

from alice_codex.collector import collect_collection, collect_zhihu_answers


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
