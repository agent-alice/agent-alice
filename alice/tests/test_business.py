"""Held-out shapes for incomplete observations and outgoing draft leakage."""

import copy
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import pytest

from alice_codex.business import check_draft, reconcile_publication, summarize_observation


def observation(items, **overrides):
    return {
        "schema_version": 1,
        "subject": "test-account",
        "collection": "answers",
        "required_metrics": ["voteup_count", "comment_count"],
        "pages": [
            {
                "source": "api/answers",
                "observed_at": "2026-09-06T10:00:00Z",
                "cursor": None,
                "next_cursor": None,
                "status": "ok",
                "items": items,
            }
        ],
        **overrides,
    }


def independent(items, coverage="partial"):
    return {
        "subject": "test-account",
        "collection": "answers",
        "source": "rendered-profile",
        "observed_at": "2026-09-06T10:00:01Z",
        "coverage": coverage,
        "items": items,
    }


def test_missing_field_remains_unknown_and_real_zero_stays_zero():
    report = summarize_observation(observation([{"id": "a", "comment_count": 0}]))
    assert report["metrics"]["voteup_count"] == {
        "state": "unknown",
        "value": None,
        "observed_sum": 0,
        "known_items": 0,
        "unknown_items": ["a"],
    }
    assert report["metrics"]["comment_count"]["value"] == 0
    assert report["field_issues"] == [{"id": "a", "metric": "voteup_count", "reason": "missing"}]
    assert not report["complete"]


def test_all_observed_counts_can_genuinely_be_zero():
    report = summarize_observation(
        observation([{"id": "a", "voteup_count": 0, "comment_count": 0}])
    )
    assert report["complete"]
    assert report["metrics"]["voteup_count"]["value"] == 0
    assert report["scope"] == "declared_api_collection"
    assert report["coverage"]["independent_comparison"] == "not_provided"


def test_second_page_changes_unknown_total_to_observed_positive_total():
    document = observation([{"id": "a", "voteup_count": 0, "comment_count": 0}])
    document["pages"][0]["next_cursor"] = "offset=20"
    partial = summarize_observation(document)
    assert partial["metrics"]["voteup_count"]["value"] is None
    document["pages"].append(
        {
            "source": "api/answers",
            "observed_at": "2026-09-06T10:00:00Z",
            "cursor": "offset=20",
            "next_cursor": None,
            "status": "ok",
            "items": [{"id": "b", "voteup_count": 7, "comment_count": 1}],
        }
    )
    complete = summarize_observation(document)
    assert complete["complete"]
    assert complete["metrics"]["voteup_count"]["value"] == 7


def test_page_only_item_exposes_an_api_blind_spot():
    document = observation(
        [{"id": "a", "voteup_count": 0, "comment_count": 0}],
        independent=independent([{"id": "page-only", "voteup_count": 9}]),
    )
    report = summarize_observation(document)
    assert report["differences"] == [{"code": "independent_only", "id": "page-only"}]
    assert report["metrics"]["voteup_count"]["value"] is None
    assert report["metrics"]["voteup_count"]["observed_sum"] == 0


def test_partial_browser_view_does_not_claim_api_only_items_are_missing():
    document = observation(
        [{"id": "a", "voteup_count": 0, "comment_count": 0}], independent=independent([])
    )
    assert summarize_observation(document)["differences"] == []
    document["independent"]["coverage"] = "complete"
    assert summarize_observation(document)["differences"] == [{"code": "api_only", "id": "a"}]


def test_independent_field_conflict_prevents_a_false_known_total():
    report = summarize_observation(
        observation(
            [{"id": "a", "voteup_count": 0, "comment_count": 0}],
            independent=independent([{"id": "a", "voteup_count": 2}]),
        )
    )
    assert report["differences"] == [
        {"code": "metric_disagreement", "id": "a", "metric": "voteup_count"}
    ]
    assert report["metrics"]["voteup_count"]["state"] == "unknown"


@pytest.mark.parametrize(
    "value,reason", [(None, "null"), ("0", "invalid_count"), (False, "invalid_count")]
)
def test_three_additional_missing_or_misparsed_counter_variants(value, reason):
    report = summarize_observation(
        observation([{"id": "a", "voteup_count": value, "comment_count": 0}])
    )
    assert report["metrics"]["voteup_count"]["value"] is None
    assert report["field_issues"][0]["reason"] == reason


@pytest.mark.parametrize("mutation", ["missing_metadata", "failed_page", "gap", "count_mismatch"])
def test_collection_completeness_requires_evidence(mutation):
    document = observation([])
    if mutation == "missing_metadata":
        document["pages"][0].pop("next_cursor")
    elif mutation == "failed_page":
        document["pages"][0]["status"] = "error"
    elif mutation == "gap":
        document["pages"][0]["cursor"] = "skipped-first-page"
    else:
        document["expected_count"] = 1
    result = summarize_observation(document)
    assert not result["coverage"]["pagination_complete"]
    assert result["metrics"]["voteup_count"]["value"] is None


@pytest.mark.parametrize(
    "body,code",
    [
        ("公开正文\n<!-- 内部推演，不要发 -->", "html_comment"),
        ("公开正文\n&lt;!-- 后面补材料 --&gt;", "html_comment"),
        ("公开正文\n[//]: # (private planning)", "markdown_comment"),
    ],
)
def test_three_comment_leakage_variants_are_blocked(body, code):
    result = check_draft(body)
    assert not result["passed"]
    assert code in {finding["code"] for finding in result["findings"]}
    assert result["content_sha256"] == hashlib.sha256(body.encode()).hexdigest()


@pytest.mark.parametrize(
    "body",
    [
        "答案：ＴＯＤＯ",
        "答案：T\u200bODO",
        "## 内部备忘\n先不要给用户看",
        "---\nstatus: draft\n---\n正文",
        "图片 ![](./private.png)",
        "插图 ![说明](images/figure.png)",
    ],
)
def test_placeholders_internal_notes_metadata_and_local_images_are_blocked(body):
    assert not check_draft(body)["passed"]


def test_clean_draft_is_only_a_static_check_not_a_rendered_or_link_verification():
    result = check_draft("# 公开说明\n答案是零，这是本次确实观测到的值。")
    assert result["passed"]
    assert result["findings"] == []
    assert not result["rendering_checked"] and not result["remote_links_checked"]


def publication():
    intent = {
        "action_id": "act-1",
        "subject": "account",
        "target": "question-1",
        "status": "unknown",
        "content_sha256": hashlib.sha256(b"final payload").hexdigest(),
        "external_id": "answer-1",
    }
    receipt = {**intent, "kind": "read_back", "visible": True}
    return intent, {
        "source": "public-answer-page",
        "observed_at": "2026-09-06T10:00:00Z",
        "receipts": [receipt],
    }


def test_independent_receipt_confirms_the_exact_external_object_and_payload():
    intent, evidence = publication()
    assert reconcile_publication(intent, evidence)["status"] == "confirmed"
    evidence["receipts"][0]["content_sha256"] = hashlib.sha256(b"truncated payload").hexdigest()
    assert reconcile_publication(intent, evidence)["status"] == "conflict"


def test_http_success_response_does_not_replace_readback_or_allow_a_retry():
    intent, evidence = publication()
    evidence["receipts"][0]["kind"] = "submission_ack"
    evidence["receipts"][0]["http_status"] = 201
    result = reconcile_publication(intent, evidence)
    assert result["status"] == "needs_reconciliation" and result["external_id"] is None
    assert not result["retry_allowed"]


def test_existing_identical_post_without_action_binding_is_only_a_candidate():
    intent, evidence = publication()
    intent.pop("external_id")
    evidence["receipts"][0].pop("action_id")
    result = reconcile_publication(intent, evidence)
    assert result["status"] == "needs_reconciliation"
    assert result["candidate_ids"] == ["answer-1"]


def test_missing_readback_field_stays_unknown_instead_of_claiming_a_conflict():
    intent, evidence = publication()
    evidence["receipts"][0].pop("visible")
    assert reconcile_publication(intent, evidence)["status"] == "needs_reconciliation"


def test_two_external_objects_for_one_action_are_not_accepted_as_one_success():
    intent, evidence = publication()
    other = copy.deepcopy(evidence["receipts"][0])
    other["external_id"] = "answer-2"
    evidence["receipts"].append(other)
    assert reconcile_publication(intent, evidence)["status"] == "conflict"


def test_real_module_cli_fails_a_leaking_draft_without_rewriting_it(tmp_path):
    draft = tmp_path / "draft.md"
    body = "Public body\n<!-- internal annotation -->"
    draft.write_text(body)
    result = subprocess.run(
        [sys.executable, "-m", "alice_codex.business", "check-draft", str(draft)],
        cwd=Path(__file__).parents[1],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 2
    assert not json.loads(result.stdout)["passed"]
    assert draft.read_text() == body


@pytest.mark.parametrize(
    "field,value,code",
    [
        ("subject", "different-account", "independent_scope_mismatch"),
        ("source", "api/answers", "independent_source_not_independent"),
    ],
)
def test_unusable_independent_view_cannot_leave_totals_known(field, value, code):
    document = observation(
        [{"id": "a", "voteup_count": 0, "comment_count": 0}],
        independent=independent([]),
    )
    document["independent"][field] = value
    result = summarize_observation(document)
    assert code in {issue["code"] for issue in result["issues"]}
    assert result["metrics"]["voteup_count"]["value"] is None
    assert result["coverage"]["pagination_complete"]


@pytest.mark.parametrize(
    "metadata,code",
    [
        ({"truncated": True}, "observation_truncated"),
        ({"http_status": 403}, "permission_denied"),
        ({"cache_age_seconds": 60, "cache_max_age_seconds": 60}, "stale_cache"),
        ({"cache_age_seconds": 12}, "cache_freshness_unknown"),
        ({"cache_max_age_seconds": 60}, "cache_freshness_unknown"),
        ({"error": "private remote diagnostic"}, "collection_error"),
    ],
)
def test_explicit_quality_evidence_prevents_false_complete_zero(metadata, code):
    document = observation([])
    document["pages"][0].update(metadata)
    result = summarize_observation(document)
    assert code in {issue["code"] for issue in result["issues"]}
    assert not result["complete"]
    assert result["metrics"]["voteup_count"]["value"] is None
    assert "private remote diagnostic" not in json.dumps(result)


@pytest.mark.parametrize(
    "metadata",
    [
        {"status": "error"},
        {"http_status": 401},
        {"truncated": True},
        {"cache_age_seconds": 90, "cache_max_age_seconds": 60},
    ],
)
def test_failed_independent_evidence_is_unavailable_not_an_empty_disagreement(metadata):
    document = observation(
        [{"id": "a", "voteup_count": 7, "comment_count": 0}],
        independent=independent([], coverage="complete"),
    )
    document["independent"].update(metadata)
    result = summarize_observation(document)
    assert result["coverage"]["independent_comparison"] == "unavailable"
    assert result["differences"] == []
    assert result["metrics"]["voteup_count"]["value"] is None
    assert result["metrics"]["voteup_count"]["observed_sum"] == 7
    assert result["independent_source"] == {
        "source": "rendered-profile",
        "observed_at": "2026-09-06T10:00:01Z",
    }


@pytest.mark.parametrize(
    "as_of,cache_age,state,code",
    [
        ("2026-09-06T10:00:30Z", 0, "fresh", None),
        ("2026-09-06T10:01:01Z", 0, "stale", "stale_observation"),
        ("2026-09-06T10:00:30Z", 40, "stale", "stale_observation"),
        ("2026-09-06T09:59:59Z", 0, "unknown", "observation_from_future"),
        ("2026-09-06T18:00:30+08:00", 0, "fresh", None),
    ],
)
def test_explicit_freshness_checks_elapsed_snapshot_and_cache_age(as_of, cache_age, state, code):
    document = observation([], freshness={"as_of": as_of, "max_age_seconds": 60})
    document["pages"][0]["cache_age_seconds"] = cache_age
    result = summarize_observation(document)
    assert result["coverage"]["freshness"] == state
    assert result["complete"] is (code is None)
    if code:
        assert code in {issue["code"] for issue in result["issues"]}
        assert result["metrics"]["voteup_count"]["value"] is None


def test_legacy_snapshot_remains_readable_without_claiming_freshness():
    result = summarize_observation(observation([]))
    assert result["complete"] and result["metrics"]["voteup_count"]["value"] == 0
    assert result["coverage"]["freshness"] == "not_checked"


@pytest.mark.parametrize(
    "metadata",
    [
        {"truncated": "false"},
        {"http_status": True},
        {"cache_age_seconds": None},
        {"cache_age_seconds": float("nan")},
        {"cache_max_age_seconds": -1},
    ],
)
def test_invalid_quality_metadata_cannot_be_silently_ignored(metadata):
    document = observation([])
    document["pages"][0].update(metadata)
    with pytest.raises(ValueError):
        summarize_observation(document)


def test_freshness_requires_a_zoned_clock_and_bounded_numeric_age():
    document = observation([], freshness={"as_of": "2026-09-06T10:00:30", "max_age_seconds": 60})
    with pytest.raises(ValueError, match="timezone"):
        summarize_observation(document)
    document["freshness"] = {"as_of": "2026-09-06T10:00:30Z", "max_age_seconds": True}
    with pytest.raises(ValueError, match="finite nonnegative"):
        summarize_observation(document)


@pytest.mark.parametrize(
    "metadata",
    [
        {"http_status": 403},
        {"truncated": True},
        {"status": "error"},
        {"cache_age_seconds": 90, "cache_max_age_seconds": 60},
        {"observed_at": "2026-09-06T09:59:59Z"},
    ],
)
def test_incomplete_or_pre_action_readback_does_not_confirm(metadata):
    intent, evidence = publication()
    intent["sent_at"] = "2026-09-06T10:00:00Z"
    evidence.update(metadata)
    result = reconcile_publication(intent, evidence)
    assert result["status"] == "needs_reconciliation"
    assert result["issues"] and not result["retry_allowed"]


def test_new_envelope_time_cannot_refresh_an_old_receipt():
    intent, evidence = publication()
    intent["sent_at"] = "2026-09-06T10:00:00Z"
    evidence["observed_at"] = "2026-09-06T10:00:10Z"
    evidence["receipts"][0]["observed_at"] = "2026-09-06T09:59:59Z"
    result = reconcile_publication(intent, evidence)
    assert result["status"] == "needs_reconciliation"
    assert result["issues"] == [{"code": "evidence_before_action", "external_id": "answer-1"}]
    evidence["receipts"][0]["observed_at"] = evidence["observed_at"]
    result = reconcile_publication(intent, evidence)
    assert result["status"] == "confirmed" and result["temporal_check"] == "checked"


def test_readback_without_attempt_time_does_not_claim_temporal_verification():
    intent, evidence = publication()
    assert reconcile_publication(intent, evidence)["temporal_check"] == "not_checked"


def test_elapsed_snapshot_time_can_expire_a_previously_fresh_cache():
    document = observation([], freshness={"as_of": "2026-09-06T10:00:50Z", "max_age_seconds": 3600})
    document["pages"][0].update(cache_age_seconds=10, cache_max_age_seconds=60)
    result = summarize_observation(document)
    assert result["coverage"]["freshness"] == "stale"
    assert result["metrics"]["voteup_count"]["value"] is None
    assert "stale_cache" in {issue["code"] for issue in result["issues"]}


def test_fresh_api_does_not_override_a_stale_independent_snapshot():
    document = observation(
        [],
        independent=independent([]),
        freshness={"as_of": "2026-09-06T10:00:30Z", "max_age_seconds": 60},
    )
    document["independent"]["observed_at"] = "2026-09-06T09:58:00Z"
    result = summarize_observation(document)
    assert result["coverage"]["independent_comparison"] == "unavailable"
    assert result["metrics"]["voteup_count"]["value"] is None
    assert {"code": "stale_observation", "source_role": "independent"} in result["issues"]


def test_module_cli_rejects_stale_observation_then_accepts_a_fresh_read(tmp_path):
    document = observation([{"id": "a", "voteup_count": 7, "comment_count": 0}])
    document["pages"][0].update(cache_age_seconds=90, cache_max_age_seconds=60)
    path = tmp_path / "synthetic-observation.json"
    for age, expected_exit, expected_value in [(90, 2, None), (10, 0, 7)]:
        document["pages"][0]["cache_age_seconds"] = age
        path.write_text(json.dumps(document))
        result = subprocess.run(
            [sys.executable, "-m", "alice_codex.business", "summarize-observation", str(path)],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
        assert result.returncode == expected_exit
        report = json.loads(result.stdout)
        assert report["metrics"]["voteup_count"]["value"] == expected_value
        assert report["metrics"]["voteup_count"]["observed_sum"] == 7
