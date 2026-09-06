"""Actual native failure envelopes remain failures, with a useful diagnostic."""

import pytest

from test_native_summary_partitions import MARKER, PartitionRuntime


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
