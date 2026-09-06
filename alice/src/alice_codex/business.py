"""Offline checks over supplied evidence; no website client or publisher.

Observation v1 wraps raw collector pages, retaining absent fields and cursors.
It cannot repair old reports which already converted missing values to zero.
Independent browser observations are supplied separately, not inferred from API
data. Counts describe the declared collection, never an entire account.
"""

import argparse
from datetime import datetime
import hashlib
import html
import json
import math
from pathlib import Path
import re
import sys
import unicodedata


MAX_INPUT_BYTES = 2 * 1024 * 1024
MAX_ITEMS = 20000
COLLECTION_ERRORS = frozenset(
    {
        "pagination_cycle",
        "time_budget",
        "unexpected_http_status",
        "unsupported_content_encoding",
        "response_too_large",
        "total_byte_limit",
        "truncated_response",
        "redirect_rejected",
        "http_error",
        "transport_error",
        "invalid_json",
        "invalid_collection_shape",
        "item_limit",
        "pagination_metadata_missing",
        "next_page_missing",
        "next_page_origin_rejected",
        "next_page_scope_rejected",
        "page_limit",
        "permission_denied",
        "stale_cache",
        "cache_metadata_invalid",
    }
)


def _text(value, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a nonempty string")
    return value


def _count(value) -> bool:
    return type(value) is int and value >= 0


def _seconds(value, field: str) -> float:
    try:
        valid = type(value) in (int, float) and math.isfinite(value) and value >= 0
    except OverflowError:
        valid = False
    if not valid:
        raise ValueError(f"{field} must be a finite nonnegative number")
    return value


def _timestamp(value, field: str) -> datetime:
    _text(value, field)
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError(f"{field} must be an ISO 8601 timestamp with a timezone") from None
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
    return result


def _freshness_policy(value):
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("freshness must be an object")
    return (
        _timestamp(value.get("as_of"), "freshness.as_of"),
        _seconds(value.get("max_age_seconds"), "freshness.max_age_seconds"),
    )


def _evidence_quality(evidence: dict, freshness=None) -> tuple[list[str], str]:
    """Only supplied metadata is checked; absent legacy metadata stays not_checked."""
    problems = []
    if "truncated" in evidence:
        if type(evidence["truncated"]) is not bool:
            raise ValueError("truncated must be a boolean")
        if evidence["truncated"]:
            problems.append("observation_truncated")
    status = evidence.get("http_status")
    if status is not None:
        if type(status) is not int or not 100 <= status <= 599:
            raise ValueError("http_status must be an integer from 100 to 599")
        if status in {401, 403}:
            problems.append("permission_denied")
        elif not 200 <= status < 300:
            problems.append("page_failed")
    if evidence.get("status", "ok") != "ok":
        problems.append("page_failed")
    if "error" in evidence:
        error = _text(evidence["error"], "error")
        # Never copy a remote body or caller-authored diagnostic into a report.
        problems.append(error if error in COLLECTION_ERRORS else "collection_error")
    age = evidence.get("cache_age_seconds")
    lifetime = evidence.get("cache_max_age_seconds")
    for field in ("cache_age_seconds", "cache_max_age_seconds"):
        if field in evidence:
            _seconds(evidence[field], field)
    state, elapsed = "not_checked", 0
    if freshness is not None:
        observed = _timestamp(evidence.get("observed_at"), "observed_at")
        elapsed = (freshness[0] - observed).total_seconds()
        state = "fresh"
        if elapsed < 0:
            problems.append("observation_from_future")
            state = "unknown"
        elif elapsed + (age or 0) > freshness[1]:
            problems.append("stale_observation")
            state = "stale"
    if age is not None or lifetime is not None:
        if age is not None and lifetime is not None:
            if age + max(elapsed, 0) >= lifetime:
                problems.append("stale_cache")
                state = "stale"
            elif state == "not_checked":
                state = "fresh"
        elif freshness is None or age is None:
            problems.append("cache_freshness_unknown")
            state = "unknown" if state != "stale" else state
    if "stale_cache" in problems:
        state = "stale"
    if "cache_metadata_invalid" in problems:
        state = "unknown"
    return list(dict.fromkeys(problems)), state


def summarize_observation(document: dict) -> dict:
    """Summarize v1 collector evidence without changing unknown counts into zero.

    Required: schema_version=1, subject, collection, required_metrics and pages.
    Each page: source, observed_at, cursor, next_cursor, status='ok'|'error', items.
    The first cursor is null; each next_cursor must match the following cursor;
    only explicit null terminates pagination. Optional expected_count is a count
    of this exact collection. Optional independent has source, observed_at, subject,
    collection, coverage='partial'|'complete', items from a separate browser view.
    Optional freshness={as_of,max_age_seconds} checks snapshot and cache age.
    Pages and independent evidence may declare truncated, http_status, error,
    cache_age_seconds and cache_max_age_seconds. Without freshness/cache metadata,
    a known count describes the supplied snapshot, not current remote state.
    """
    if not isinstance(document, dict) or document.get("schema_version") != 1:
        raise ValueError("observation schema_version must be 1")
    subject = _text(document.get("subject"), "subject")
    collection = _text(document.get("collection"), "collection")
    metrics = document.get("required_metrics")
    if (
        not isinstance(metrics, list)
        or not 1 <= len(metrics) <= 8
        or any(
            not isinstance(m, str) or not re.fullmatch(r"[a-zA-Z_][a-zA-Z_0-9]{0,63}", m)
            for m in metrics
        )
        or len(set(metrics)) != len(metrics)
    ):
        raise ValueError("required_metrics must contain 1..8 distinct field names")
    pages = document.get("pages")
    if not isinstance(pages, list) or len(pages) > 1000:
        raise ValueError("pages must be a list with at most 1000 entries")
    issues, sources, observed = [], [], {}
    freshness = _freshness_policy(document.get("freshness"))
    freshness_states = []
    evidence_usable = True
    unknown = {metric: set() for metric in metrics}
    field_issues = []
    expected_cursor, seen_cursors, raw_count = None, set(), 0
    pagination_complete = bool(pages)
    if not pages:
        issues.append({"code": "no_pages"})
    for number, page in enumerate(pages):
        if not isinstance(page, dict):
            raise ValueError("each page must be an object")
        sources.append(
            {
                "source": _text(page.get("source"), "page.source"),
                "observed_at": _text(page.get("observed_at"), "page.observed_at"),
            }
        )
        quality, state = _evidence_quality(page, freshness)
        freshness_states.append(state)
        if quality:
            evidence_usable = False
            issues.extend({"code": code, "page": number + 1} for code in quality)
        if any(
            code in quality
            for code in ("observation_truncated", "permission_denied", "page_failed")
        ):
            pagination_complete = False
        cursor = page.get("cursor")
        if cursor is not None and not isinstance(cursor, str):
            raise ValueError("page cursor must be a string or null")
        if (
            "cursor" not in page
            or cursor != expected_cursor
            or cursor in seen_cursors
            or (number > 0 and expected_cursor is None)
        ):
            pagination_complete = False
            issues.append({"code": "pagination_gap_or_repeat", "page": number + 1})
        seen_cursors.add(cursor)
        expected_cursor = page.get("next_cursor")
        if "next_cursor" not in page or (
            expected_cursor is not None and not isinstance(expected_cursor, str)
        ):
            pagination_complete = False
            issues.append({"code": "pagination_unknown", "page": number + 1})
        if page.get("status") != "ok":
            pagination_complete = False
            if "page_failed" not in quality:
                issues.append({"code": "page_failed", "page": number + 1})
            continue
        items = page.get("items")
        if not isinstance(items, list):
            raise ValueError("successful page.items must be a list")
        raw_count += len(items)
        if raw_count > MAX_ITEMS:
            raise ValueError("observation exceeds item limit")
        for item in items:
            if (
                not isinstance(item, dict)
                or not isinstance(item.get("id"), (str, int))
                or isinstance(item.get("id"), bool)
            ):
                pagination_complete = False
                issues.append({"code": "item_missing_id", "page": number + 1})
                continue
            identity = str(item["id"])
            if not identity:
                raise ValueError("item id must not be empty")
            if identity in observed:
                for metric in metrics:
                    if observed[identity].get(metric) != item.get(metric):
                        unknown[metric].add(identity)
                        issues.append(
                            {"code": "duplicate_item_conflict", "id": identity, "metric": metric}
                        )
            else:
                observed[identity] = item
            for metric in metrics:
                if not _count(item.get(metric)):
                    unknown[metric].add(identity)
                    reason = (
                        "missing"
                        if metric not in item
                        else "null"
                        if item[metric] is None
                        else "invalid_count"
                    )
                    field_issues.append({"id": identity, "metric": metric, "reason": reason})
    if expected_cursor is not None:
        pagination_complete = False
        issues.append({"code": "pagination_unfinished"})
    expected_count = document.get("expected_count")
    if expected_count is not None:
        if not _count(expected_count):
            raise ValueError("expected_count must be a nonnegative integer or null")
        if expected_count != len(observed):
            pagination_complete = False
            issues.append(
                {
                    "code": "expected_count_mismatch",
                    "expected": expected_count,
                    "observed": len(observed),
                }
            )
    differences = []
    independent = document.get("independent")
    comparison = "not_provided"
    independent_source = None
    if independent is not None:
        if not isinstance(independent, dict):
            raise ValueError("independent observation must be an object")
        independent_source = {
            "source": _text(independent.get("source"), "independent.source"),
            "observed_at": _text(independent.get("observed_at"), "independent.observed_at"),
        }
        quality, state = _evidence_quality(independent, freshness)
        freshness_states.append(state)
        if independent.get("coverage") not in {"partial", "complete"}:
            raise ValueError("independent.coverage must be partial or complete")
        rows = independent.get("items")
        if not isinstance(rows, list) or len(rows) + raw_count > MAX_ITEMS:
            raise ValueError("independent.items must be a bounded list")
        comparable = (
            independent.get("subject") == subject and independent.get("collection") == collection
        )
        comparison = "compared" if comparable else "scope_mismatch"
        if not comparable:
            issues.append({"code": "independent_scope_mismatch"})
            evidence_usable = False
        if independent["source"] in {source["source"] for source in sources}:
            quality.append("independent_source_not_independent")
        if quality:
            evidence_usable = False
            comparison = "unavailable"
            issues.extend({"code": code, "source_role": "independent"} for code in quality)
        if comparable and not quality:
            other = {}
            for item in rows:
                if (
                    not isinstance(item, dict)
                    or not isinstance(item.get("id"), (str, int))
                    or isinstance(item.get("id"), bool)
                ):
                    raise ValueError("independent items require an id")
                identity = str(item["id"])
                if not identity:
                    raise ValueError("independent item id must not be empty")
                if identity in other:
                    raise ValueError("independent items must have unique ids")
                other[identity] = item
            for identity in sorted(other.keys() - observed.keys()):
                differences.append({"code": "independent_only", "id": identity})
            if independent["coverage"] == "complete":
                for identity in sorted(observed.keys() - other.keys()):
                    differences.append({"code": "api_only", "id": identity})
            for identity in sorted(other.keys() & observed.keys()):
                for metric in metrics:
                    external = other[identity].get(metric)
                    if _count(external) and external != observed[identity].get(metric):
                        differences.append(
                            {"code": "metric_disagreement", "id": identity, "metric": metric}
                        )
    totals = {}
    for metric in metrics:
        known = [
            item[metric]
            for identity, item in observed.items()
            if identity not in unknown[metric] and _count(item.get(metric))
        ]
        complete = (
            pagination_complete and evidence_usable and not unknown[metric] and not differences
        )
        totals[metric] = {
            "state": "known" if complete else "unknown",
            "value": sum(known) if complete else None,
            "observed_sum": sum(known),
            "known_items": len(known),
            "unknown_items": sorted(unknown[metric]),
        }
    return {
        "schema_version": 1,
        "subject": subject,
        "collection": collection,
        "scope": "declared_api_collection",
        "sources": sources,
        "independent_source": independent_source,
        "coverage": {
            "pagination_complete": pagination_complete,
            "observed_items": len(observed),
            "independent_comparison": comparison,
            "independent_coverage": independent.get("coverage") if independent else None,
            "freshness": (
                "stale"
                if "stale" in freshness_states
                else "unknown"
                if "unknown" in freshness_states
                else "not_checked"
                if not freshness_states or "not_checked" in freshness_states
                else "fresh"
            ),
        },
        "metrics": totals,
        "issues": issues,
        "field_issues": field_issues,
        "differences": differences,
        "complete": pagination_complete
        and not issues
        and not differences
        and all(metric["state"] == "known" for metric in totals.values()),
    }


def check_draft(content: str) -> dict:
    """Check exact outgoing text without silently stripping or rewriting it.

    This is deterministic lint, not a semantic, rendering, link-reachability or
    publication authorization check. Run against both source and final payload.
    """
    if not isinstance(content, str) or len(content.encode("utf-8")) > MAX_INPUT_BYTES:
        raise ValueError("draft must be text no larger than 2 MiB")
    canonical = unicodedata.normalize("NFKC", html.unescape(html.unescape(content)))
    canonical = re.sub("[\u200b\u200c\u200d\ufeff]", "", canonical)
    checks = [
        ("html_comment", r"<!--", "Remove internal HTML comments from the outgoing payload."),
        (
            "markdown_comment",
            r"(?m)^\s*\[(?://|comment)\]:\s*(?:#|<>)",
            "Remove Markdown comment directives.",
        ),
        (
            "placeholder",
            r"(?i)\b(?:TODO|TBD|FIXME|XXX|PLACEHOLDER)\b|\{\{[^}\n]+\}\}|待补充|待填写|此处插入|稍后完善|\[insert\b",
            "Resolve unfinished placeholders before preview.",
        ),
        (
            "internal_note",
            r"(?im)(?:^|[>#\s])(?:内部备忘|内部备注|内部笔记|内部思考|仅供内部|不要发布|发送前删除|note to self|internal note)|<(?:analysis|think)>|状态\s*[:：]\s*草稿|目标问题\s*[:：]",
            "Separate private notes and draft metadata from the public body.",
        ),
        (
            "local_asset",
            r"(?i)(?:\]\(|(?:src|href)\s*=\s*['\"])(?:file:|/Users/|/tmp/|\.\.?/)",
            "Resolve local links or images to their intended public assets.",
        ),
        (
            "unresolved_image",
            r"!\[[^]\n]*\]\((?!https?://|//)[^)\n]+\)",
            "Resolve the image to its intended public URL before checking the final payload.",
        ),
        ("empty_link", r"\]\(\s*\)", "Supply the missing link or image destination."),
    ]
    findings = []
    if not canonical.strip():
        findings.append({"code": "empty_body", "line": 1, "message": "The outgoing body is empty."})
    lines = canonical.lstrip().splitlines()
    if lines and lines[0].strip() == "---":
        metadata = []
        for line in lines[1:]:
            if line.strip() == "---":
                if any(re.match(r"^[\w-]+\s*:", entry) for entry in metadata):
                    findings.append(
                        {
                            "code": "frontmatter",
                            "line": 1,
                            "message": "Prepare a separate public payload without frontmatter.",
                        }
                    )
                break
            metadata.append(line)
    for code, pattern, message in checks:
        lines = set()
        for match in re.finditer(pattern, canonical):
            line = canonical.count("\n", 0, match.start()) + 1
            if line not in lines:
                findings.append({"code": code, "line": line, "message": message})
                lines.add(line)
            if len(findings) >= 100:
                break
        if len(findings) >= 100:
            break
    return {
        "schema_version": 1,
        "passed": not findings,
        "scope": "static_text_only",
        "content_sha256": hashlib.sha256(content.encode("utf-8")).hexdigest(),
        "findings": findings,
        "findings_limited": len(findings) >= 100,
        "rendering_checked": False,
        "remote_links_checked": False,
    }


def reconcile_publication(intent: dict, evidence: dict) -> dict:
    """Match a post-attempt intent against separately fetched external evidence.

    Intent: action_id, subject, target, content_sha256, status=sent|unknown|confirmed,
    optional external_id and sent_at. Evidence: source, observed_at, receipts; each receipt has
    kind=read_back, external_id, subject, target, content_sha256, visible and optional
    action_id. HTTP submission responses alone cannot confirm publication. A matching
    body without an ID/action binding is only a candidate, not proof of this action.
    No outcome from this function authorizes automatically repeating a submission.
    Optional sent_at requires readback timestamps at or after the attempt; optional
    freshness and observation quality metadata reject stale/failed readbacks.
    Without sent_at, the temporal relationship remains explicitly not_checked.
    """
    if not isinstance(intent, dict) or not isinstance(evidence, dict):
        raise ValueError("intent and evidence must be objects")
    for field in ("action_id", "subject", "target", "content_sha256"):
        _text(intent.get(field), f"intent.{field}")
    if not re.fullmatch(r"[0-9a-f]{64}", intent["content_sha256"]):
        raise ValueError("intent.content_sha256 must be a SHA-256 hex digest of the final payload")
    if intent.get("status") not in {"sent", "unknown", "confirmed"}:
        raise ValueError("reconciliation requires a sent, unknown or confirmed intent")
    if intent.get("external_id") is not None:
        _text(intent["external_id"], "intent.external_id")
    source = _text(evidence.get("source"), "evidence.source")
    observed_at = _text(evidence.get("observed_at"), "evidence.observed_at")
    freshness = _freshness_policy(evidence.get("freshness"))
    evidence_problems, _ = _evidence_quality(evidence, freshness)
    sent_at = _timestamp(intent["sent_at"], "intent.sent_at") if "sent_at" in intent else None
    if sent_at is not None and _timestamp(observed_at, "evidence.observed_at") < sent_at:
        evidence_problems.append("evidence_before_action")
    receipts = evidence.get("receipts")
    if not isinstance(receipts, list) or len(receipts) > 1000:
        raise ValueError("receipts must be a list of at most 1000 entries")
    confirmed, candidates, conflicts = set(), set(), set()
    issues = [{"code": code, "source_role": "evidence"} for code in evidence_problems]
    for receipt in receipts:
        if not isinstance(receipt, dict):
            raise ValueError("receipt must be an object")
        if receipt.get("kind") != "read_back" or not receipt.get("external_id"):
            continue
        if not isinstance(receipt["external_id"], (str, int)) or isinstance(
            receipt["external_id"], bool
        ):
            raise ValueError("receipt external_id must identify an external object")
        identity = str(receipt["external_id"])
        quality_receipt = {"observed_at": observed_at, **receipt}
        # Legacy receipts may include the copied intent's lifecycle status.
        if quality_receipt.get("status") in {"sent", "unknown", "confirmed"}:
            quality_receipt.pop("status")
        receipt_problems, _ = _evidence_quality(quality_receipt, freshness)
        if (
            sent_at is not None
            and _timestamp(receipt.get("observed_at", observed_at), "receipt.observed_at") < sent_at
        ):
            receipt_problems.append("evidence_before_action")
        issues.extend({"code": code, "external_id": identity} for code in receipt_problems)
        if evidence_problems or receipt_problems:
            continue
        same_target = all(receipt.get(field) == intent[field] for field in ("subject", "target"))
        bound = (
            identity == intent.get("external_id") or receipt.get("action_id") == intent["action_id"]
        )
        if bound and (
            (intent.get("external_id") is not None and identity != intent["external_id"])
            or (
                receipt.get("action_id") is not None and receipt["action_id"] != intent["action_id"]
            )
        ):
            conflicts.add(identity)
            continue
        matches = (
            same_target
            and receipt.get("content_sha256") == intent["content_sha256"]
            and receipt.get("visible") is True
        )
        if bound and matches:
            confirmed.add(identity)
        elif bound and (
            receipt.get("visible") is False
            or any(
                receipt.get(field) is not None and receipt[field] != intent[field]
                for field in ("subject", "target", "content_sha256")
            )
        ):
            conflicts.add(identity)
        elif matches:
            candidates.add(identity)
    status = "confirmed" if len(confirmed) == 1 and not conflicts else "needs_reconciliation"
    if conflicts or len(confirmed) > 1:
        status = "conflict"
    return {
        "schema_version": 1,
        "action_id": intent["action_id"],
        "status": status,
        "external_id": next(iter(confirmed)) if status == "confirmed" else None,
        "candidate_ids": sorted(candidates | confirmed) if status != "confirmed" else [],
        "conflicting_ids": sorted(conflicts),
        "retry_allowed": False,
        "temporal_check": "checked" if sent_at is not None else "not_checked",
        "issues": issues,
        "evidence": {"source": source, "observed_at": observed_at},
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Offline Alice business evidence checks")
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("check-draft", "summarize-observation"):
        commands.add_parser(command).add_argument("path", type=Path)
    reconcile = commands.add_parser("reconcile-publication")
    reconcile.add_argument("--intent", required=True, type=Path)
    reconcile.add_argument("--evidence", required=True, type=Path)
    args = parser.parse_args(argv)

    def read(path):
        with path.open("rb") as handle:
            data = handle.read(MAX_INPUT_BYTES + 1)
        if len(data) > MAX_INPUT_BYTES:
            raise ValueError("input file exceeds 2 MiB")
        return data.decode("utf-8")

    try:
        if args.command == "check-draft":
            result = check_draft(read(args.path))
            passed = result["passed"]
        elif args.command == "summarize-observation":
            result = summarize_observation(json.loads(read(args.path)))
            passed = result["complete"]
        else:
            result = reconcile_publication(
                json.loads(read(args.intent)), json.loads(read(args.evidence))
            )
            passed = result["status"] == "confirmed"
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0 if passed else 2
    except (OSError, UnicodeError, ValueError) as error:
        print(json.dumps({"error": str(error)}, ensure_ascii=False), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
