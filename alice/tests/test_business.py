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
