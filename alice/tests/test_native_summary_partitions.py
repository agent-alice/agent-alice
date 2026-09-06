"""Real Codex children + real Code Mode/MCP, with an owned Responses fixture.

This checks a manually continued native coordinator, not calendar redispatch or
semantic model quality. ALICE_ARTIFACT_PYTHON makes every Alice operation run
from the same installed wheel; otherwise this is source-runtime native evidence.
No inference provider, credentials, user home, or private materials are used.
"""

import asyncio
from contextlib import suppress
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess

import pytest

from alice_codex.rpc import RpcError
from test_native_service_resource_epochs import NativeResourceRuntime, required_environment
from test_native_task_policy import NativePolicyRuntime


pytestmark = pytest.mark.native
MARKER = "SYNTHETIC_PARTITION_RESULT:"
PAGE_CHARS = 4096


def advertised(body):
    values = list(body.get("tools", []))
    for row in body.get("input", []):
        if row.get("type") in {"additional_tools", "tool_search_output"}:
            values.extend(row.get("tools", []))
    result = []
    for tool in values:
        if tool.get("type") == "namespace":
            result.extend({**item, "namespace": tool["name"]} for item in tool.get("tools", []))
        else:
            result.append(tool)
    return result


def tagged_result(value):
    if isinstance(value, dict):
        for item in value.values():
            found = tagged_result(item)
            if found is not None:
                return found
    elif isinstance(value, list):
        for item in value:
            found = tagged_result(item)
            if found is not None:
                return found
    elif isinstance(value, str):
        try:
            return tagged_result(json.loads(value))
        except (ValueError, TypeError):
            offset = value.find(MARKER)
            if offset >= 0:
                try:
                    return json.JSONDecoder().raw_decode(value[offset + len(MARKER):])[0]
                except ValueError:
                    pass
    return None


def output_kind(value):
    """Describe the actual native envelope without accepting a failed or partial page."""
    output = value.get("output", []) if isinstance(value, dict) else value
    if isinstance(output, str):
        output = [output]
    for item in output if isinstance(output, list) else []:
        text = item.get("text", "") if isinstance(item, dict) else item
        if not isinstance(text, str):
            continue
        if text.startswith("Script failed\n"):
            return "script_failed"
        if text.startswith("Script running with cell ID "):
            return "script_running"
    return "missing_or_invalid_tagged_result"


class PartitionRuntime(NativePolicyRuntime):
    def __init__(self, root):
        super().__init__(root)
        self.controllers = {}
        self.children = {}
        self.batch = None
        self.node_results = {}
        self.native_reads = {}
        self.sequence = 0
        self.raw_source = b""
        diagnostics = os.environ.get("ALICE_SUMMARY_DIAGNOSTICS") == "1"
        self.resource_receipts = os.environ.get("ALICE_SUMMARY_RESOURCE_RECEIPTS") == "1"
        if diagnostics and self.resource_receipts:
            raise ValueError("Summary diagnostics and resource receipt wrappers must run separately")
        if diagnostics:
            self.host_command = [
                self.runtime_python, "-I",
                str(Path(__file__).parent / "fixtures/native_summary_diagnostic_service.py"),
                str(self.home), str(self.root / "service-failures.jsonl"),
            ]
        if self.resource_receipts:
            # This stricter evidence mode always starts a new installed candidate
            # with its explicit Codex/Code Mode pair, before any native work.
            required_environment("ALICE_ARTIFACT_PYTHON")
            self.binary = Path(required_environment("ALICE_TEST_CODEX_BINARY")).resolve()
            self.host_binary = Path(required_environment("ALICE_TEST_CODEX_HOST_BINARY")).resolve()
            self.expected_hashes = {
                "codex": required_environment("ALICE_TEST_CODEX_SHA256"),
                "codex-code-mode-host": required_environment("ALICE_TEST_CODEX_HOST_SHA256"),
            }
            NativeResourceRuntime.verify_pair(self)
            NativeResourceRuntime.verify_installed_source(self)
            self.receipt_log = self.root / "native-resource-receipts.jsonl"
            self.env["ALICE_NATIVE_RESOURCE_RECEIPTS"] = str(self.receipt_log)
            wrapper = Path(__file__).parent / "fixtures/native_resource_receipt_service.py"
            self.host_command = [self.runtime_python, "-I", str(wrapper), str(self.home)]

    def resource_epoch(self):
        return json.loads((self.home / "state/runtime.json").read_text())["server"]["resource_epoch_id"]

    def call(self, body, name, arguments=None, code=None):
        matches = [item for item in advertised(body) if item.get("name") == name]
        assert len(matches) == 1, f"Native tool {name} absent or ambiguous"
        tool = matches[0]
        self.sequence += 1
        item = {
            "type": "custom_tool_call" if code is not None else "function_call",
            "name": tool["name"], "call_id": f"partition-call-{self.sequence}",
        }
        if "namespace" in tool:
            item["namespace"] = tool["namespace"]
        if code is not None:
            item["input"] = '// @exec: {"max_output_tokens": 100000}\n' + code
        else:
            item["arguments"] = json.dumps(arguments or {})
        return item

    @staticmethod
    def js_helpers():
        return """
function unpack(result) {
  if (result.isError) throw new Error(JSON.stringify(result));
  if (result.structuredContent) return result.structuredContent;
  return JSON.parse(result.content.filter(x => x.type === 'text').map(x => x.text).join(''));
}
async function mcp(suffix, args) {
  const matches = ALL_TOOLS.filter(tool => tool.name.endsWith('__' + suffix));
  if (matches.length !== 1) throw new Error('Missing or ambiguous Alice tool: ' + suffix);
  return unpack(await tools[matches[0].name](args));
}
"""

    def mcp_call(self, body, method, args):
        code = self.js_helpers() + (
            "const result = await mcp(" + json.dumps(method) + ", " + json.dumps(args) + ");"
            + "text(" + json.dumps(MARKER) + " + JSON.stringify(result));"
        )
        return self.call(body, "exec", code=code)

    @staticmethod
    def final(text):
        return {"type": "message", "role": "assistant", "id": "synthetic-summary-final",
                "content": [{"type": "output_text", "text": text}]}

    @staticmethod
    def latest_output(body, call_id):
        matches = [row for row in body["input"] if row.get("call_id") == call_id
                   and row.get("type") in {"function_call_output", "custom_tool_call_output"}]
        assert matches, f"Native did not return actual output for {call_id}"
        return matches[-1]

    def flow(self, body):
        # Tool arguments in a parent contain child prompts, so inspect input
        # messages only. fork_turns=none gives each native child bounded input.
        text = "\n".join(json.dumps(row, ensure_ascii=False) for row in body["input"]
                         if (row.get("role") in {"user", "developer"}
                             or row.get("type") == "agent_message")
                         and row.get("type") != "additional_tools")
        child = re.search(r"SUMMARY_CHILD:(n[0-9]+-[0-9]+)", text)
        if child:
            return "child", child[1]
        controller = re.search(r"SUMMARY_CONTROLLER:(first|remaining)", text)
        assert controller, ("Unexpected native input outside the owned synthetic workflow: "
                            + json.dumps([(row.get("type"), row.get("role")) for row in body["input"]]))
        # Both same-root controller turns remain in native history.
        return "controller", re.findall(r"SUMMARY_CONTROLLER:(first|remaining)", text)[-1]

    async def step(self, body):
        assert not self.errors, self.errors
        kind, key = self.flow(body)
        if kind == "child":
            return self.child_step(body, key)
        state = self.controllers.setdefault(key, {"stage": "initial"})
        if state["stage"] == "initial":
            if key == "first":
                item = self.mcp_call(body, "memory_prepare_summary", {
                    "level": "L1", "period": "2026-09-01T00:00", "timezone": "Asia/Shanghai",
                })
                state.update(stage="prepared", call_id=item["call_id"])
                return item
            return self.next_call(body, state)
        if state["stage"] == "prepared":
            self.batch = tagged_result(self.latest_output(body, state["call_id"]))
            assert self.batch and self.batch["strategy"] == "partitioned-v1"
            assert self.batch["source_count"] == 1
            return self.next_call(body, state)
        if state["stage"] == "next":
            page = tagged_result(self.latest_output(body, state["call_id"]))
            assert page is not None
            if page["complete"]:
                assert key == "remaining" and page["ready"] == []
                assert page["completed_nodes"] == page["total_nodes"] == len(self.node_results)
                self.complete_page = page
                return self.final("The host confirmed the whole partition root commit.")
            assert page["ready"] and len(page["ready"]) <= 4
            if key == "remaining" and not state.get("restarted_checked"):
                assert page["completed_nodes"] == 1
                assert not {node["node_id"] for node in page["ready"]} & self.node_results.keys()
                state["restarted_checked"] = True
            node = page["ready"][0]
            assert node["node_id"] not in self.children, "The coordinator must not duplicate workers"
            self.children[node["node_id"]] = {"node": node, "stage": "initial"}
            prompt = f"SUMMARY_CHILD:{node['node_id']}\n" + node["prompt"]
            assert len(prompt.encode()) < 16 * 1024
            item = self.call(body, "spawn_agent", {
                "task_name": "partition_" + node["node_id"].replace("-", "_"),
                "message": prompt, "fork_turns": "none",
            })
            state.update(stage="spawned", call_id=item["call_id"], node_id=node["node_id"])
            return item
        if state["stage"] == "spawned":
            output = self.latest_output(body, state["call_id"])
            assert "error" not in json.dumps(output).lower(), output
            self.children[state["node_id"]]["spawn_output"] = output
            state["stage"] = "waiting"
        if state["stage"] == "waiting":
            if state["node_id"] not in self.node_results:
                return self.call(body, "wait_agent", {"timeout_ms": 10000})
            if key == "first":
                assert len(self.node_results) == 1
                assert not self.node_results[state["node_id"]]["complete"]
                return self.final("One leaf is committed; the root is still incomplete.")
            return self.next_call(body, state)
        raise AssertionError(f"Unexpected synthetic controller state {state}")

    def next_call(self, body, state):
        item = self.mcp_call(body, "memory_summary_partition_next", {
            "batch_id": self.batch["batch_id"], "limit": 4,
        })
        state.update(stage="next", call_id=item["call_id"])
        return item

    def child_step(self, body, node_id):
        state = self.children[node_id]
        if state["stage"] == "initial":
            command = {
                "cmd": "cat " + shlex.quote(state["node"]["manifest_path"]),
                "login": False, "max_output_tokens": 40000,
            }
            code = self.js_helpers() + "const file = await tools.exec_command(" + json.dumps(command) + ");\n"
            code += """
if (file.exit_code !== 0) throw new Error(file.output);
const manifest = JSON.parse(file.output);
"""
            code += "text(" + json.dumps(MARKER) + " + JSON.stringify(manifest));"
            item = self.call(body, "exec", code=code)
            state.update(stage="manifest", call_id=item["call_id"])
            return item
        if state["stage"] == "manifest":
            manifest = tagged_result(self.latest_output(body, state["call_id"]))
            assert manifest and manifest["node_id"] == node_id
            state.update(sources=manifest["sources"], readings=[], source_index=0, offset=0, chunks=[])
            return self.read_call(body, state)
        if state["stage"] == "page":
            output = self.latest_output(body, state["call_id"])
            value = tagged_result(output)
            assert value is not None, (
                "A bounded native MCP page must be fully visible; "
                f"native_output={output_kind(output)}, call_id={state['call_id']}"
            )
            assert value["source_id"] == state["sources"][state["source_index"]]["source_id"]
            assert value["offset_chars"] == state["offset"]
            assert len(value["content"]) == min(PAGE_CHARS, value["total_chars"] - state["offset"])
            state["chunks"].append(value["content"])
            if value["next_offset"] is not None:
                assert value["next_offset"] == state["offset"] + len(value["content"])
                state["offset"] = value["next_offset"]
                return self.read_call(body, state)
            state["readings"].append({**value, "content": "".join(state["chunks"])})
            state.update(source_index=state["source_index"] + 1, offset=0, chunks=[])
            if state["source_index"] < len(state["sources"]):
                return self.read_call(body, state)
            sources, readings = state["sources"], state["readings"]
            assert len(sources) == len(readings) <= 64
            assert {row["source_id"] for row in sources} == {row["source_id"] for row in readings}
            total = 0
            for value in readings:
                content = value["content"].encode()
                assert not value["truncated"]
                total += len(content)
                if "byte_start" in value:
                    assert value["byte_end"] - value["byte_start"] <= 64 * 1024
                    assert content == self.raw_source[value["byte_start"]:value["byte_end"]]
                else:
                    assert len(content) <= 16 * 1024
            assert 0 < total <= 128 * 1024
            self.native_reads[node_id] = {"source_count": len(readings), "total_bytes": total}
            good = [row["source_id"] for row in sources if not row["parse_error"]]
            bad = [row["source_id"] for row in sources if row["parse_error"]]
            candidate = {
                "content": "Controlled native structural result. " + " ".join(f"[source:{sid}]" for sid in good[:1]),
                "source_ids": good[:1], "covered_source_ids": good,
                "missing": [{"source_id": sid, "reason": "Synthetic explicit gap"} for sid in bad],
            }
            item = self.mcp_call(body, "memory_commit_summary_partition", {
                "batch_id": self.batch["batch_id"], "node_id": node_id, "candidate": candidate,
            })
            state.update(stage="committed", call_id=item["call_id"], candidate=candidate)
            return item
        if state["stage"] == "committed":
            result = tagged_result(self.latest_output(body, state["call_id"]))
            assert result and result["node_id"] == node_id
            self.node_results[node_id] = result
            return self.final(f"Synthetic node {node_id} received its host commit receipt.")
        raise AssertionError("Unexpected synthetic child state")

    def read_call(self, body, state):
        item = self.mcp_call(body, "memory_read", {
            "source_id": state["sources"][state["source_index"]]["source_id"],
            "offset_chars": state["offset"], "max_chars": PAGE_CHARS,
        })
        state.update(stage="page", call_id=item["call_id"])
        return item

    async def model(self, reader, writer):
        self.peers.add(writer)
        handler = asyncio.current_task()
        self.handlers.add(handler)
        try:
            headers = (await reader.readuntil(b"\r\n\r\n")).decode().split("\r\n")
            values = {line.split(":", 1)[0].lower(): line.split(":", 1)[1].strip()
                      for line in headers[1:] if ":" in line}
            assert headers[0] == "POST /responses HTTP/1.1"
            assert values["host"].startswith("127.0.0.1:")
            assert "authorization" not in values and "content-encoding" not in values
            body = json.loads(await reader.readexactly(int(values["content-length"])))
            self.requests.append(body)
            item = await self.step(body)
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n")
            response_id = f"synthetic-partition-{len(self.requests)}"
            for event in (
                {"type": "response.created", "response": {"id": response_id}},
                {"type": "response.output_item.done", "item": item},
                {"type": "response.completed", "response": {"id": response_id,
                 "usage": {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}}},
            ):
                writer.write(("data: " + json.dumps(event) + "\n\n").encode())
            await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        except Exception as error:
            if not self.errors:
                (self.root / "fixture-error.json").write_text(json.dumps({
                    "error": repr(error), "input": body["input"],
                    "service_pid": self.service.pid if self.service else None,
                    "service_returncode": self.service.returncode if self.service else None,
                    "control_socket_exists": self.config.control_socket.exists() if self.config else None,
                }, ensure_ascii=False))
                self.errors.append(repr(error))
        finally:
            writer.close()
            with suppress(ConnectionError):
                await writer.wait_closed()
            self.peers.discard(writer)
            self.handlers.discard(handler)


async def native_threads(runtime):
    entries = {}
    for archived in (False, True):
        page = await runtime.rpc.request("thread/list", {
            "limit": 100, "archived": archived, "modelProviders": [],
            "sourceKinds": ["cli", "vscode", "exec", "appServer", "subAgent", "unknown"],
        })
        assert not page.get("nextCursor"), "The bounded fixture must inspect all native threads"
        entries.update({row["id"]: row for row in page["data"]})
    loaded = await runtime.rpc.request("thread/loaded/list", {"limit": 100})
    assert not loaded.get("nextCursor")
    entries.update({thread_id: {"id": thread_id} for thread_id in loaded["data"]})
    result = {}
    pending = list(entries)
    while pending:
        thread_id = pending.pop()
        if thread_id in result:
            continue
        try:
            found = await runtime.rpc.request("thread/read", {"threadId": thread_id, "includeTurns": True})
        except RpcError as error:
            if "not loaded" not in str(error):
                raise
            await runtime.rpc.request("thread/resume", {"threadId": thread_id})
            found = await runtime.rpc.request("thread/read", {"threadId": thread_id, "includeTurns": True})
        turns = await runtime.rpc.request("thread/turns/list", {
            "threadId": thread_id, "limit": 100, "itemsView": "full",
        })
        assert not turns.get("nextCursor")
        result[thread_id] = {**found["thread"], "turns": turns["data"]}
        # V2 child threads share a session with their parent and may be absent
        # from thread/list after restart. The parent's durable native activity
        # supplies explicit child IDs; verify those through public read APIs.
        pending.extend(item["agentThreadId"] for turn in turns["data"] for item in turn["items"]
                       if item.get("type") == "subAgentActivity" and item.get("kind") == "started"
                       and item["agentThreadId"] not in result)
    (runtime.root / "native-thread-evidence.json").write_text(json.dumps(result))
    return result


def child_references(threads, parent_id):
    result = {}
    started = {item["agentThreadId"]: item["agentPath"]
               for turn in threads[parent_id]["turns"] for item in turn["items"]
               if item.get("type") == "subAgentActivity" and item.get("kind") == "started"}
    for thread_id, thread in threads.items():
        if thread_id == parent_id:
            continue
        for turn in thread["turns"]:
            commits = [item for item in turn["items"] if item.get("type") == "mcpToolCall"
                       and item.get("server") == "alice"
                       and item.get("tool") == "memory_commit_summary_partition"]
            if commits:
                assert len(commits) == 1
                commit = commits[0]
                node_id = commit["arguments"]["node_id"]
                assert commit["status"] == "completed" and commit["error"] is None
                assert turn["status"] == "completed"
                source = json.dumps(thread["source"])
                assert thread.get("parentThreadId") == parent_id or ("subAgent" in source and parent_id in source)
                assert started[thread_id].endswith("partition_" + node_id.replace("-", "_"))
                assert node_id not in result, "A node must have one durable native worker turn"
                result[node_id] = {"thread_id": thread_id, "turn_id": turn["id"]}
    return result


async def test_native_children_complete_large_partition_and_resume_after_leaf_restart(tmp_path, record_testsuite_property):
    runtime = PartitionRuntime(tmp_path)
    try:
        await runtime.configure()
        package = subprocess.run(
            [runtime.runtime_python, "-I", "-c", "import alice_codex; print(alice_codex.__file__)"],
            cwd=tmp_path, env=runtime.env, text=True, capture_output=True, check=True, timeout=10,
        ).stdout.strip()
        artifact = os.environ.get("ALICE_ARTIFACT_PYTHON")
        if artifact:
            assert Path(package).resolve().is_relative_to(Path(artifact).absolute().parent.parent.resolve()), (
                "Installed-wheel native gate must not import Alice from the source checkout"
            )
        record_testsuite_property("partition_alice_runtime_package", package)
        record_testsuite_property("partition_same_installed_wheel", bool(artifact))
        record_testsuite_property("partition_native_binary_sha256", hashlib.sha256(runtime.binary.read_bytes()).hexdigest())
        runtime.raw_source = (json.dumps({"content": "x" * (1024 * 1024 + 1),
                              "timestamp": "2026-09-01T00:15:00+08:00"}) + "\n").encode()
        source = runtime.config.workspace / "memory/chronicle/traces/2026-09-01.jsonl"
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_bytes(runtime.raw_source)
        first_prompt = "SUMMARY_CONTROLLER:first\nPrepare the complete window and delegate one bounded leaf to a native child."
        first = await runtime.cli("ask", first_prompt, "--request-id", "partition-native-first", "--wait", "90", timeout=100)
        assert not runtime.errors, runtime.errors
        assert first["intent"]["status"] == "completed", first
        parent_id = first["intent"]["thread_id"]
        assert len(runtime.node_results) == 1
        target = runtime.config.workspace / "memory/chronicle/hourly/2026-09-01.jsonl"
        assert not target.exists(), "One native leaf must not create a whole-window result"
        assert not (Path(runtime.batch["manifest_path"]).parent / "coverage.jsonl").exists()
        first_refs = child_references(await native_threads(runtime), parent_id)
        assert set(first_refs) == set(runtime.node_results)
        first_epoch = runtime.resource_epoch()
        runtime.remember_mcp()
        request_count = len(runtime.requests)
        await runtime.stop()
        restarted = await runtime.launch()
        second_epoch = runtime.resource_epoch()
        assert second_epoch != first_epoch
        assert restarted["tasks"]["main"]["thread_id"] == parent_id
        assert len(runtime.requests) == request_count
        assert child_references(await native_threads(runtime), parent_id) == first_refs

        remaining_prompt = "SUMMARY_CONTROLLER:remaining\nContinue the existing host plan through bounded native children; inspect next without repeating original sources."
        second = await runtime.cli("ask", remaining_prompt, "--request-id", "partition-native-remaining", "--wait", "180", timeout=190)
        assert not runtime.errors, runtime.errors
        assert second["intent"]["status"] == "completed", second
        assert second["intent"]["thread_id"] == parent_id
        assert runtime.complete_page["complete"]
        assert len(runtime.node_results) > 3
        assert sum(result["complete"] for result in runtime.node_results.values()) == 1
        assert set(runtime.native_reads) == set(runtime.node_results)
        native = await native_threads(runtime)
        references = child_references(native, parent_id)
        assert set(references) == set(runtime.node_results)
        assert {node: references[node] for node in first_refs} == first_refs
        assert len({row["thread_id"] for row in references.values()}) == len(references)
        parent_turns = {turn["id"]: turn for turn in native[parent_id]["turns"]}
        assert parent_turns[first["intent"]["turn_id"]]["status"] == "completed"
        assert parent_turns[second["intent"]["turn_id"]]["status"] == "completed"
        rows = target.read_text().splitlines()
        assert len(rows) == 1
        summary = json.loads(rows[0])
        assert summary["batch_id"] == runtime.batch["batch_id"]
        reference = summary["coverage_ref"]
        coverage = Path(runtime.batch["manifest_path"]).parent / reference["path"]
        proof = coverage.read_bytes()
        proof_rows = [json.loads(line) for line in proof.splitlines()]
        assert reference["sha256"] == hashlib.sha256(proof).hexdigest()
        assert reference["records"] == 1 and reference["fragments"] > 16
        assert reference["missing_fragments"] == 0
        position = 0
        for row in proof_rows:
            assert row["status"] == "covered" and row["byte_start"] == position
            position = row["byte_end"]
        assert position == len(runtime.raw_source)
        assert source.read_bytes() == runtime.raw_source
        before = target.read_bytes()
        request_count = len(runtime.requests)
        replay = await runtime.cli("ask", remaining_prompt, "--request-id", "partition-native-remaining")
        assert replay == second["intent"]
        assert len(runtime.requests) == request_count and target.read_bytes() == before
        record_testsuite_property("native_partition_nodes", len(references))
        record_testsuite_property("native_controller_turns", 2)
        record_testsuite_property("partition_coverage_sha256", reference["sha256"])
        record_testsuite_property("partition_native_references_sha256", hashlib.sha256(json.dumps(references, sort_keys=True).encode()).hexdigest())
        runtime.remember_mcp()
        await runtime.stop()
        if runtime.resource_receipts:
            from test_native_resource_receipts import validate_native_resource_receipts

            assert len(references) == 12
            children = {row["thread_id"] for row in references.values()}
            first_children = {row["thread_id"] for row in first_refs.values()}
            proof = validate_native_resource_receipts(
                runtime.receipt_log, runtime.home / "state/resources.sqlite3",
                expected_child_thread_ids=children,
                expected_epoch_threads={
                    first_epoch: {parent_id, *first_children},
                    second_epoch: {parent_id, *(children - first_children)},
                },
            )
            proof["localhost_responses"] = len(runtime.requests)
            proof["paid_model_calls"] = 0
            proof["native_code_mode_host_sha256"] = runtime.expected_hashes["codex-code-mode-host"]
            resources = await runtime.cli("resources", "status")
            assert resources["virtual_budget_enabled"] is False
            assert resources["money_receipts"] == {}
            assert resources["tokens"]["actual_usage_total"] is None
            assert resources["tokens"]["cost_microusd"] is None
            assert resources["tokens"]["unknown_or_out_of_order_events"] == 0
            proof["sum_epoch_high_water_marks"] = resources["tokens"]["sum_epoch_high_water_marks"]
            proof["actual_usage_total"] = resources["tokens"]["actual_usage_total"]
            proof["money_receipts"] = resources["money_receipts"]
            proof["virtual_budget_enabled"] = resources["virtual_budget_enabled"]
            proof["account_limits"] = resources["account_limits"]
            (runtime.root / "native-resource-receipt-summary.json").write_text(json.dumps(proof))
            record_testsuite_property("partition_resource_receipts", json.dumps(proof, sort_keys=True))
    finally:
        await runtime.close()
