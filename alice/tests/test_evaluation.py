"""Exercise the independent oracle with deliberately wrong synthetic candidates."""

import copy
import json
from pathlib import Path
import subprocess
import sys

import pytest

from alice_codex import business, evaluation


FIXTURES = Path(__file__).parent / "fixtures" / "observation_learning"


@pytest.fixture
def suite():
    return tuple(
        evaluation.read_json(FIXTURES / name)
        for name in ("tasks.json", "oracle.json", "correction.json")
    )


def test_fixed_suite_scores_real_outputs_including_held_out_shapes_and_counterexamples(suite):
    tasks, oracle, _ = suite
    report = evaluation.evaluate(tasks, oracle)
    assert report["counts"] == {"passed": 20, "failed": 0, "error": 0, "not_run": 0}
    assert report["gap_detection"] == {
        "expected": 15,
        "detected": 15,
        "missed": 0,
        "correctly_scored": 15,
        "false_positives": 0,
    }
    assert report["human_interventions_during_run"] == 0
    assert report["consumption"]["model_calls"] == 0
    assert report["consumption"]["native_tokens_observed"] is None
    assert not report["consumption"]["virtual_budget_enabled"]
    assert all(row["result_sha256"] == evaluation.digest(row["result"]) for row in report["rows"])


def test_always_unknown_cannot_pass_complete_empty_zero_and_multipage_counterexamples(
    suite, monkeypatch
):
    tasks, oracle, _ = suite

    def always_unknown(raw):
        result = business.summarize_observation(raw)
        result["complete"] = False
        for metric in result["metrics"].values():
            metric.update(state="unknown", value=None)
        return result

    monkeypatch.setitem(evaluation.OPERATIONS, "observation", always_unknown)
    report = evaluation.evaluate(tasks, oracle)
    assert report["status"] == "failed"
    assert report["counts"]["failed"] == 4
    assert report["gap_detection"]["false_positives"] == 4


def test_first_page_only_candidate_fails_independent_multipage_oracle(suite, monkeypatch):
    tasks, oracle, _ = suite

    def first_page(raw):
        raw["pages"] = raw["pages"][:1]
        raw["pages"][0]["next_cursor"] = None
        return business.summarize_observation(raw)

    monkeypatch.setitem(evaluation.OPERATIONS, "observation", first_page)
    report = evaluation.evaluate(tasks, oracle)
    row = next(row for row in report["rows"] if row["id"] == "counterexample-complete-pages")
    assert row["status"] == "failed"
    assert "metrics.count.value" in row["mismatches"]


def test_self_reported_passed_and_boolean_zero_are_not_accepted(suite):
    _, oracle, _ = suite
    expected = oracle["answers"]["counterexample-true-zero"]
    assert evaluation.grade_result("observation", {"passed": True}, expected)["status"] == "failed"
    actual = {
        "complete": True,
        "metrics": {
            "count": {
                "state": "known",
                "value": False,
                "observed_sum": 0,
                "unknown_items": [],
            }
        },
    }
    score = evaluation.grade_result("observation", actual, expected)
    assert score["mismatches"] == ["metrics.count.value"]


@pytest.mark.parametrize(
    "damage", ["empty", "missing_answer", "no_unseen", "no_complete_counterexample"]
)
def test_missing_or_uninformative_suite_evidence_is_rejected(suite, damage):
    tasks, oracle, _ = copy.deepcopy(suite)
    if damage == "empty":
        tasks["cases"] = []
    elif damage == "missing_answer":
        oracle["answers"].pop(tasks["cases"][0]["id"])
    elif damage == "no_unseen":
        for case in tasks["cases"]:
            case["phase"] = "training"
    else:
        for expected in oracle["answers"].values():
            expected["gap_expected"] = True
    with pytest.raises(ValueError):
        evaluation.evaluate(tasks, oracle)


def synthetic_baseline(candidate):
    """Unit-test an old defective result; this is not the real main run evidence."""
    report = copy.deepcopy(candidate)
    report["implementation"]["business_sha256"] = "0" * 64
    row = next(row for row in report["rows"] if row["id"] == "training-expired-cache")
    row["result"]["complete"] = True
    row["result"]["metrics"]["count"].update(state="known", value=4)
    row["result_sha256"] = evaluation.digest(row["result"])
    # Intentionally leave status/counts claiming success: the comparer must re-grade.
    return report


def test_correction_chain_regrades_false_success_and_never_promotes_a_skill(suite):
    tasks, oracle, correction = suite
    candidate = evaluation.evaluate(tasks, oracle)
    baseline = synthetic_baseline(candidate)
    chain = evaluation.compare_correction(tasks, oracle, baseline, candidate, correction)
    assert chain["baseline_failures"] == 1
    assert chain["remaining_failures"] == chain["repeated_failures"] == 0
    assert chain["status"] == "regression_passed_transfer_not_tested"
    assert chain["phases"]["unseen"]["samples"] == 14
    assert not chain["skill_promotion_allowed"]
    assert chain["fresh_model_task_reuse"] == "not_run"
    candidate["correction_chain"] = chain
    assert chain["candidate_evaluation_sha256"] == evaluation.digest(
        {key: value for key, value in candidate.items() if key != "correction_chain"}
    )


def test_new_regression_cannot_disappear_from_remaining_failure_count(suite):
    tasks, oracle, correction = suite
    candidate = evaluation.evaluate(tasks, oracle)
    baseline = synthetic_baseline(candidate)
    row = next(row for row in candidate["rows"] if row["id"] == "counterexample-true-zero")
    row["result"]["metrics"]["count"]["value"] = 99
    row["result_sha256"] = evaluation.digest(row["result"])
    chain = evaluation.compare_correction(tasks, oracle, baseline, candidate, correction)
    assert chain["status"] == "regression_failed"
    assert chain["remaining_failures"] == 1
    assert chain["unresolved_baseline_failures"] == 0
    assert chain["new_regressions"] == ["counterexample-true-zero"]


@pytest.mark.parametrize("damage", ["oracle", "input", "output", "missing_case", "no_change"])
def test_changed_or_missing_correction_evidence_is_rejected(suite, damage):
    tasks, oracle, correction = suite
    candidate = evaluation.evaluate(tasks, oracle)
    baseline = synthetic_baseline(candidate)
    if damage == "oracle":
        baseline["oracle_sha256"] = "1" * 64
    elif damage == "input":
        baseline["rows"][0]["input_sha256"] = "1" * 64
    elif damage == "output":
        baseline["rows"][0]["result_sha256"] = "1" * 64
    elif damage == "missing_case":
        baseline["rows"].pop()
    else:
        baseline["implementation"] = candidate["implementation"]
    with pytest.raises(ValueError):
        evaluation.compare_correction(tasks, oracle, baseline, candidate, correction)


def test_execution_error_is_recorded_and_other_cases_still_run(suite, monkeypatch):
    tasks, oracle, _ = suite

    def failing(raw):
        if raw["pages"][0]["items"] == [{"id": "training-cached", "count": 4}]:
            raise ValueError("must not copy sensitive exception messages")
        return business.summarize_observation(raw)

    monkeypatch.setitem(evaluation.OPERATIONS, "observation", failing)
    report = evaluation.evaluate(tasks, oracle)
    assert report["counts"]["error"] == 1
    assert report["counts"]["passed"] == 19
    assert "sensitive" not in json.dumps(report)


@pytest.mark.parametrize("bad_value", ["{}", "[]", "null"])
def test_module_entrypoint_preserves_results_when_correction_binding_fails(tmp_path, bad_value):
    report = tmp_path / "report.json"
    bad_baseline = tmp_path / "bad.json"
    bad_baseline.write_text(bad_value)
    command = [
        sys.executable,
        "-m",
        "alice_codex.evaluation",
        "--tasks",
        str(FIXTURES / "tasks.json"),
        "--oracle",
        str(FIXTURES / "oracle.json"),
        "--report",
        str(report),
        "--baseline-report",
        str(bad_baseline),
        "--correction",
        str(FIXTURES / "correction.json"),
    ]
    result = subprocess.run(command, capture_output=True, text=True, timeout=15)
    assert result.returncode == 2
    evidence = json.loads(report.read_text())
    assert evidence["counts"]["passed"] == 20
    assert evidence["correction_error"] == "ValueError"
    assert json.loads(result.stdout)["status"] == "failed"
    assert json.loads(result.stdout)["correction_error"] == "ValueError"
    before = report.read_bytes()
    result = subprocess.run(command, capture_output=True, text=True, timeout=15)
    assert result.returncode != 0
    assert report.read_bytes() == before
