"""Offline, deterministic task scoring; never inference, publication or promotion.

Tasks and the reference oracle are separate versioned files. The candidate only
receives a copy of task input. Reports retain actual results so comparison can
re-score them rather than trust a caller-supplied `passed` field. This is an
auditable test harness, not a security boundary against code with local access.
"""

import argparse
import copy
import hashlib
import json
from pathlib import Path
import platform
import subprocess
import time

from alice_codex import business


SCORER_VERSION = 1
MAX_FILE_BYTES = 2 * 1024 * 1024
OPERATIONS = {
    "observation": business.summarize_observation,
    "reconciliation": lambda value: business.reconcile_publication(
        value["intent"], value["evidence"]
    ),
}


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False).encode()
    ).hexdigest()


def read_json(path):
    with Path(path).open("rb") as handle:
        raw = handle.read(MAX_FILE_BYTES + 1)
    if len(raw) > MAX_FILE_BYTES:
        raise ValueError("evaluation input exceeds 2 MiB")
    return json.loads(raw)


def validate_suite(tasks, oracle):
    if not isinstance(tasks, dict) or not isinstance(oracle, dict):
        raise ValueError("tasks and oracle must be objects")
    if tasks.get("schema_version") != 1 or oracle.get("schema_version") != 1:
        raise ValueError("unsupported task/oracle schema")
    version = tasks.get("fixture_version")
    if not isinstance(version, str) or not version or version != oracle.get("fixture_version"):
        raise ValueError("fixture versions must match")
    cases, answers = tasks.get("cases"), oracle.get("answers")
    if not isinstance(cases, list) or not 5 <= len(cases) <= 200 or not isinstance(answers, dict):
        raise ValueError("a nonempty bounded suite with training and variants is required")
    identities, phases = set(), []
    for case in cases:
        if not isinstance(case, dict):
            raise ValueError("case must be an object")
        identity = case.get("id")
        if not isinstance(identity, str) or not identity or identity in identities:
            raise ValueError("case IDs must be distinct nonempty strings")
        identities.add(identity)
        if case.get("operation") not in OPERATIONS or not isinstance(case.get("input"), dict):
            raise ValueError("case operation/input is invalid")
        phase = case.get("phase")
        if phase not in {"training", "unseen", "counterexample"}:
            raise ValueError("unknown case phase")
        phases.append(phase)
        expected = answers.get(identity)
        if (
            not isinstance(expected, dict)
            or type(expected.get("gap_expected")) is not bool
            or not isinstance(expected.get("checks"), dict)
            or not expected["checks"]
        ):
            raise ValueError("each case requires independent checks and a gap expectation")
        if any(not isinstance(key, str) or not key for key in expected["checks"]):
            raise ValueError("oracle check paths must be nonempty strings")
    if set(answers) != identities:
        raise ValueError("oracle and task IDs differ")
    if (
        phases.count("training") < 1
        or phases.count("unseen") < 3
        or phases.count("counterexample") < 1
    ):
        raise ValueError("training, three unseen variants and a counterexample are required")
    if not any(
        case["phase"] == "counterexample"
        and case["operation"] == "observation"
        and not answers[case["id"]]["gap_expected"]
        for case in cases
    ):
        raise ValueError("a complete observation counterexample is required")


def grade_result(operation, result, expected):
    """Check independent expected paths, ignoring any self-reported success."""
    mismatches = []
    for path, target in expected["checks"].items():
        value = result
        for part in path.split("."):
            if not isinstance(value, dict) or part not in value:
                mismatches.append(path)
                break
            value = value[part]
        else:
            # JSON booleans must not pass as 0/1 counts.
            if digest(value) != digest(target):
                mismatches.append(path)
    gap = isinstance(result, dict) and (
        result.get("complete") is False
        if operation == "observation"
        else result.get("status") in {"needs_reconciliation", "conflict"}
    )
    if gap != expected["gap_expected"]:
        mismatches.append("gap_detected")
    return {
        "status": "failed" if mismatches else "passed",
        "mismatches": mismatches,
        "gap_detected": gap,
    }


def _counts(rows):
    return {
        status: sum(row["status"] == status for row in rows)
        for status in ("passed", "failed", "error", "not_run")
    }


def _source_identity():
    module = Path(business.__file__).resolve()
    revision, dirty = None, None
    # The installed package hash remains available outside a source checkout.
    source = next((parent for parent in module.parents if (parent / ".git").exists()), None)
    if source:
        try:
            revision = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=source,
                capture_output=True,
                text=True,
                check=True,
                timeout=5,
            ).stdout.strip()
            dirty = bool(
                subprocess.run(
                    ["git", "status", "--porcelain", "--", "alice/src"],
                    cwd=source,
                    capture_output=True,
                    text=True,
                    check=True,
                    timeout=5,
                ).stdout.strip()
            )
        except (OSError, subprocess.SubprocessError):
            pass
    return {
        "source_commit": revision,
        "source_dirty": dirty,
        "business_sha256": hashlib.sha256(module.read_bytes()).hexdigest(),
        "scorer_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "python_version": platform.python_version(),
        "platform": platform.system(),
    }


def evaluate(tasks, oracle):
    validate_suite(tasks, oracle)
    started, rows = time.monotonic(), []
    for case in tasks["cases"]:
        before = time.monotonic()
        expected = oracle["answers"][case["id"]]
        row = {
            "id": case["id"],
            "phase": case["phase"],
            "input_sha256": digest(case["input"]),
            "expected_sha256": digest(expected),
        }
        try:
            actual = OPERATIONS[case["operation"]](copy.deepcopy(case["input"]))
            row.update(
                result=actual,
                result_sha256=digest(actual),
                **grade_result(case["operation"], actual, expected),
            )
        except (ValueError, TypeError, KeyError, OverflowError) as error:
            # No exception messages: caller inputs may contain private paths/text.
            row.update(status="error", error_type=type(error).__name__, gap_detected=False)
        row["elapsed_seconds"] = round(time.monotonic() - before, 6)
        rows.append(row)
    counts = _counts(rows)
    gaps = [row for row in rows if oracle["answers"][row["id"]]["gap_expected"]]
    return {
        "schema_version": 1,
        "scorer_version": SCORER_VERSION,
        "fixture_version": tasks["fixture_version"],
        "tasks_sha256": digest(tasks),
        "oracle_sha256": digest(oracle),
        "implementation": _source_identity(),
        "evidence_layer": "offline_rules",
        "counts": counts,
        "samples": len(rows),
        "status": "passed" if counts["passed"] == len(rows) else "failed",
        "gap_detection": {
            "expected": len(gaps),
            "detected": sum(row["gap_detected"] for row in gaps),
            "missed": sum(not row["gap_detected"] for row in gaps),
            "correctly_scored": sum(row["status"] == "passed" for row in gaps),
            "false_positives": sum(row["gap_detected"] for row in rows if row not in gaps),
        },
        "human_interventions_during_run": 0,
        "elapsed_seconds": round(time.monotonic() - started, 6),
        "consumption": {
            "model_calls": 0,
            "native_tokens_observed": None,
            "token_usage_state": "not_applicable_no_model_calls",
            "money_receipts": [],
            "inference_cost_incurred": False,
            "virtual_budget_enabled": False,
        },
        "rows": rows,
    }


def _regrade(report, tasks, oracle):
    """Validate binding and recompute grades; never trust report counts/status."""
    if not isinstance(report, dict):
        raise ValueError("report must be an object")
    if (
        report.get("schema_version") != 1
        or report.get("scorer_version") != SCORER_VERSION
        or report.get("tasks_sha256") != digest(tasks)
        or report.get("oracle_sha256") != digest(oracle)
        or report.get("evidence_layer") != "offline_rules"
    ):
        raise ValueError("report is not bound to this suite, oracle and scoring version")
    rows = report.get("rows")
    if not isinstance(rows, list) or len(rows) != len(tasks["cases"]):
        raise ValueError("report must contain every case exactly once")
    indexed = {row["id"]: row for row in rows}
    if set(indexed) != {case["id"] for case in tasks["cases"]}:
        raise ValueError("report case IDs differ")
    verified = {}
    for case in tasks["cases"]:
        row = indexed[case["id"]]
        expected = oracle["answers"][case["id"]]
        if row.get("input_sha256") != digest(case["input"]) or row.get("expected_sha256") != digest(
            expected
        ):
            raise ValueError("report case evidence binding differs")
        if "result" not in row:
            verified[case["id"]] = {"status": "error", "mismatches": ["execution_error"]}
            continue
        if row.get("result_sha256") != digest(row["result"]):
            raise ValueError("report result hash differs")
        verified[case["id"]] = grade_result(case["operation"], row["result"], expected)
    return verified


def compare_correction(tasks, oracle, baseline, candidate, correction):
    validate_suite(tasks, oracle)
    if not isinstance(correction, dict):
        raise ValueError("correction must be an object")
    before, after = _regrade(baseline, tasks, oracle), _regrade(candidate, tasks, oracle)
    for key in ("cause_hypothesis", "change", "scope"):
        if not isinstance(correction.get(key), str) or not correction[key].strip():
            raise ValueError("correction requires cause hypothesis, change and scope")
    failure = correction.get("failure_case")
    training = {case["id"] for case in tasks["cases"] if case["phase"] == "training"}
    if failure not in training or before[failure]["status"] != "failed":
        raise ValueError("correction must cite an observed training mismatch")
    if (
        baseline["implementation"]["business_sha256"]
        == candidate["implementation"]["business_sha256"]
    ):
        raise ValueError("correction requires a changed implementation")
    baseline_failed = [identity for identity, row in before.items() if row["status"] != "passed"]
    remaining = [identity for identity, row in after.items() if row["status"] != "passed"]
    unresolved = sorted(set(remaining) & set(baseline_failed))
    new_regressions = sorted(set(remaining) - set(baseline_failed))
    repeated = [
        identity
        for identity in baseline_failed
        if set(before[identity]["mismatches"]) & set(after[identity]["mismatches"])
    ]
    phases = {}
    for phase in ("training", "unseen", "counterexample"):
        rows = [after[case["id"]] for case in tasks["cases"] if case["phase"] == phase]
        phases[phase] = {"samples": len(rows), **_counts(rows)}
    success = all(row["status"] == "passed" for row in after.values())
    return {
        "schema_version": 1,
        "evidence_layer": "offline_rules",
        "status": "regression_passed_transfer_not_tested" if success else "regression_failed",
        "failure_evidence": {
            "case": failure,
            "baseline_report_sha256": digest(baseline),
            "mismatches": before[failure]["mismatches"],
        },
        "correction": {key: correction[key] for key in ("cause_hypothesis", "change", "scope")},
        "candidate_evaluation_sha256": digest(
            {key: value for key, value in candidate.items() if key != "correction_chain"}
        ),
        "implementation": candidate["implementation"],
        "phases": phases,
        "baseline_failures": len(baseline_failed),
        "remaining_failures": len(remaining),
        "unresolved_baseline_failures": len(unresolved),
        "new_regressions": new_regressions,
        "repeated_failures": len(repeated),
        "repeated_failure_ids": repeated,
        "human_interventions_during_run": 0,
        "correction_origin": "developer_authored_code_change",
        "fresh_model_task_reuse": "not_run",
        "skill_promotion_allowed": False,
        "limits": [
            "Fixed synthetic shapes only; unseen means held out from the training case.",
            "Development can inspect this public suite; it is not a blind model evaluation.",
            "No inference, installed-artifact, live website or general-learning claim.",
        ],
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tasks", type=Path, required=True)
    parser.add_argument("--oracle", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--baseline-report", type=Path)
    parser.add_argument("--correction", type=Path)
    args = parser.parse_args(argv)
    if bool(args.baseline_report) != bool(args.correction):
        parser.error("baseline-report and correction are required together")
    tasks, oracle = read_json(args.tasks), read_json(args.oracle)
    report = evaluate(tasks, oracle)
    if args.baseline_report:
        try:
            report["correction_chain"] = compare_correction(
                tasks, oracle, read_json(args.baseline_report), report, read_json(args.correction)
            )
        except (OSError, ValueError, TypeError, KeyError) as error:
            report["correction_error"] = type(error).__name__
            report["status"] = "failed"
    # New reports only: don't overwrite previous failures or recovery evidence.
    with args.report.open("x", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
        handle.write("\n")
    summary = {
        key: report[key] for key in ("status", "samples", "counts", "gap_detection", "consumption")
    }
    if "correction_error" in report:
        summary["correction_error"] = report["correction_error"]
    print(json.dumps(summary))
    return 0 if report["status"] == "passed" and "correction_error" not in report else 2


if __name__ == "__main__":
    raise SystemExit(main())
