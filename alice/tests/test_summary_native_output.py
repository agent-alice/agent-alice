"""Actual native failure envelopes remain failures, with a useful diagnostic."""

import pytest

from test_native_summary_partitions import MARKER, NativeResourceRuntime, PartitionRuntime


@pytest.mark.parametrize(
    "output,kind",
    [
        ([
            {"type": "input_text", "text": "Script failed\nWall time 0.0 seconds\nOutput:\n"},
            {"type": "input_text", "text": 'Script error:\nError: {"content":[{"type":"text",'
             '"text":"Error executing tool memory_read: [Errno 2] No such file or directory"}],'
             '"isError":true}'},
        ], "script_failed"),
        ([{"type": "input_text", "text": "Script running with cell ID 123"}], "script_running"),
        ([{"type": "input_text", "text": MARKER + '{"content":"truncated'}],
         "missing_or_invalid_tagged_result"),
    ],
)
def test_unsuccessful_native_page_never_advances_or_commits(output, kind):
    runtime = object.__new__(PartitionRuntime)
    runtime.children = {"node": {"stage": "page", "call_id": "owned-call", "offset": 0}}
    runtime.node_results = {}
    body = {"input": [{"type": "custom_tool_call_output", "call_id": "owned-call",
                       "output": output}]}

    with pytest.raises(AssertionError, match=f"native_output={kind}, call_id=owned-call"):
        runtime.child_step(body, "node")

    assert runtime.children["node"] == {"stage": "page", "call_id": "owned-call", "offset": 0}
    assert runtime.node_results == {}


@pytest.mark.parametrize("diagnostics,receipts", [(False, False), (True, False), (False, True), (True, True)])
def test_summary_wrappers_are_selected_without_silent_override(tmp_path, monkeypatch, diagnostics, receipts):
    # Pure fixture configuration: no binary probe, service or model is run.
    monkeypatch.setenv("ALICE_SUMMARY_DIAGNOSTICS", "1" if diagnostics else "0")
    monkeypatch.setenv("ALICE_SUMMARY_RESOURCE_RECEIPTS", "1" if receipts else "0")
    for key, value in {
        "ALICE_ARTIFACT_PYTHON": str(tmp_path / "candidate/python"),
        "ALICE_TEST_CODEX_BINARY": str(tmp_path / "pair/codex"),
        "ALICE_TEST_CODEX_SHA256": "a" * 64,
        "ALICE_TEST_CODEX_HOST_BINARY": str(tmp_path / "pair/codex-code-mode-host"),
        "ALICE_TEST_CODEX_HOST_SHA256": "b" * 64,
    }.items():
        monkeypatch.setenv(key, value)
    checks = []
    monkeypatch.setattr(NativeResourceRuntime, "verify_pair", lambda self: checks.append("pair"))
    monkeypatch.setattr(NativeResourceRuntime, "verify_installed_source", lambda self: checks.append("installed"))
    if diagnostics and receipts:
        with pytest.raises(ValueError, match="wrappers must run separately"):
            PartitionRuntime(tmp_path)
        assert checks == []
        return
    runtime = PartitionRuntime(tmp_path)
    if receipts:
        assert checks == ["pair", "installed"]
        assert runtime.host_command[2].endswith("/native_resource_receipt_service.py")
        assert runtime.env["ALICE_NATIVE_RESOURCE_RECEIPTS"] == str(runtime.receipt_log)
    elif diagnostics:
        assert checks == []
        assert runtime.host_command[2].endswith("/native_summary_diagnostic_service.py")
        assert runtime.host_command[-1] == str(tmp_path / "service-failures.jsonl")
    else:
        assert checks == [] and runtime.host_command is None
