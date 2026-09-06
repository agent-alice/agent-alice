"""Temporary CI rejection rehearsal. This branch must never be merged."""


def test_required_gate_must_reject_a_failing_candidate():
    raise AssertionError("Intentional failure to verify the required merge gate")
