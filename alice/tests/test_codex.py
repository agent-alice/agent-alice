"""Native request contracts and Alice ownership/stop boundaries."""

import asyncio
import copy
import unittest

from alice_codex.codex import CodexClient, OwnershipError
from alice_codex.rpc import RpcError


class FakeRpc:
    def __init__(self):
        self.calls = []
        self.listeners = []
        self.threads = {
            "main": {"id": "main", "status": {"type": "active"}},
            "child": {
                "id": "child",
                "parentThreadId": "main",
                "status": {"type": "active"},
                "canAcceptDirectInput": False,
            },
            "other": {"id": "other", "status": {"type": "active"}},
        }
        self.goals = {"main": {"status": "active"}, "child": {"status": "complete"}}
        self.interrupted = asyncio.Event()
        self.settle_on_interrupt = True
        self.terminals = []
        self.item_pages = {None: {"data": [], "nextCursor": None}}

    def add_listener(self, listener):
        self.listeners.append(listener)
        return lambda: self.listeners.remove(listener)

    async def request(self, method, params):
        self.calls.append((method, copy.deepcopy(params)))
        thread_id = params.get("threadId")
        if method == "thread/start":
            return {"thread": {"id": "created", "status": {"type": "idle"}}}
        if method == "thread/resume":
            return {"thread": copy.deepcopy(self.threads[thread_id])}
        if method == "thread/list":
            return {"data": copy.deepcopy(list(self.threads.values())), "nextCursor": None}
        if method == "thread/loaded/list":
            return {
                "data": [
                    key
                    for key, thread in self.threads.items()
                    if thread["status"]["type"] != "notLoaded"
                ],
                "nextCursor": None,
            }
        if method == "thread/read":
            return {"thread": copy.deepcopy(self.threads[thread_id])}
        if method == "thread/turns/list":
            return {"data": [{"id": f"turn-{thread_id}", "status": "inProgress"}]}
        if method == "thread/items/list":
            return copy.deepcopy(self.item_pages[params.get("cursor")])
        if method == "thread/goal/get":
            return {"goal": copy.deepcopy(self.goals.get(thread_id))}
        if method == "thread/goal/set":
            if self.threads[thread_id].get("canAcceptDirectInput") is False:
                raise RpcError(
                    "direct app-server input is not allowed for multi-agent v2 sub-agents",
                    code=-32600,
                )
            self.goals[thread_id] = {"status": params["status"]}
            return {"goal": self.goals[thread_id]}
        if method == "turn/interrupt":
            self.interrupted.set()
            if self.settle_on_interrupt:
                self.threads[thread_id]["status"] = {"type": "idle"}
            return {}
        if method == "thread/backgroundTerminals/list":
            return {"data": self.terminals, "nextCursor": None}
        if method == "turn/start":
            return {"turn": {"id": "new-turn"}}
        if method == "turn/steer":
            return {"turnId": params["expectedTurnId"]}
        if method == "thread/queue/add":
            return {
                "queuedSubmission": {
                    "id": "queued",
                    "clientUserMessageId": params["clientUserMessageId"],
                }
            }
        return {}


class CodexTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.rpc = FakeRpc()
        self.codex = CodexClient(self.rpc, owned_root_ids=["main"])

    async def asyncTearDown(self):
        self.codex.close()

    async def test_thread_start_registers_new_owned_root(self):
        result = await self.codex.thread_start({"cwd": "/isolated", "approvalPolicy": "on-request"})
        self.assertTrue(self.codex.owns(result["thread"]["id"]))
        self.assertEqual(self.rpc.calls[0][1]["approvalPolicy"], "on-request")

    async def test_resume_and_mutation_refuse_unowned_threads(self):
        for action in (
            self.codex.thread_resume("other"),
            self.codex.turn_start("other", "do something"),
            self.codex.turn_interrupt("other", "active"),
            self.codex.pause_goal("other"),
            self.codex.stop_tree("other"),
        ):
            with self.assertRaises(OwnershipError):
                await action
        self.assertEqual(self.rpc.calls, [])

    async def test_queue_keeps_host_occurrence_id_and_never_auto_starts(self):
        result = await self.codex.queue_add(
            "main", "scheduled check", client_message_id="occurrence-001"
        )
        self.assertEqual(result["queuedSubmission"]["clientUserMessageId"], "occurrence-001")
        self.assertEqual([m for m, _ in self.rpc.calls], ["thread/queue/add"])
        self.assertEqual(
            self.rpc.calls[0][1]["input"], [{"type": "text", "text": "scheduled check"}]
        )

    async def test_steer_requires_exact_active_turn_id(self):
        await self.codex.turn_steer(
            "main", "turn-before-race", "correction", client_message_id="message-1"
        )
        self.assertEqual(
            self.rpc.calls[-1],
            (
                "turn/steer",
                {
                    "threadId": "main",
                    "expectedTurnId": "turn-before-race",
                    "input": [{"type": "text", "text": "correction"}],
                    "clientUserMessageId": "message-1",
                },
            ),
        )

    async def test_message_reconciliation_pages_native_history(self):
        self.rpc.item_pages = {
            None: {
                "data": [
                    {"turnId": "new-turn", "item": {"type": "userMessage", "clientId": "other"}}
                ],
                "nextCursor": "older",
            },
            "older": {
                "data": [
                    {
                        "turnId": "matched-turn",
                        "item": {"type": "userMessage", "clientId": "intent"},
                    }
                ],
                "nextCursor": None,
            },
        }
        result = await self.codex.find_turn_by_client_id("main", "intent", page_size=1)
        self.assertEqual(result, "matched-turn")
        self.assertEqual(
            self.rpc.calls,
            [
                (
                    "thread/items/list",
                    {"threadId": "main", "cursor": None, "limit": 1, "sortDirection": "desc"},
                ),
                (
                    "thread/items/list",
                    {"threadId": "main", "cursor": "older", "limit": 1, "sortDirection": "desc"},
                ),
            ],
        )

    async def test_missing_message_does_not_submit_anything(self):
        self.assertIsNone(await self.codex.find_turn_by_client_id("main", "unobserved"))
        self.assertEqual([method for method, _ in self.rpc.calls], ["thread/items/list"])

    async def test_reconciliation_does_not_misreport_truncated_scan_as_missing(self):
        self.rpc.item_pages[None] = {"data": [], "nextCursor": "older"}
        with self.assertRaisesRegex(RpcError, "scan limit"):
            await self.codex.find_turn_by_client_id("main", "intent", max_pages=1)

    async def test_reconciliation_rejects_repeated_cursor(self):
        self.rpc.item_pages = {
            None: {"data": [], "nextCursor": "loop"},
            "loop": {"data": [], "nextCursor": "loop"},
        }
        with self.assertRaisesRegex(RpcError, "repeated cursor"):
            await self.codex.find_turn_by_client_id("main", "intent")

    async def test_reconciliation_detects_duplicate_message_in_distinct_turns(self):
        self.rpc.item_pages = {
            None: {
                "data": [
                    {"turnId": "turn-two", "item": {"type": "userMessage", "clientId": "intent"}}
                ],
                "nextCursor": "older",
            },
            "older": {
                "data": [
                    {"turnId": "turn-one", "item": {"type": "userMessage", "clientId": "intent"}}
                ],
                "nextCursor": None,
            },
        }
        with self.assertRaisesRegex(RpcError, "multiple Codex turns"):
            await self.codex.find_turn_by_client_id("main", "intent")

    async def test_stop_tree_pauses_goal_and_excludes_unrelated_thread(self):
        result = await self.codex.stop_tree("main")
        self.assertEqual(result["stopped"], ["child", "main"])
        self.assertEqual(result["interrupted"], 2)
        self.assertEqual(self.rpc.threads["other"]["status"]["type"], "active")
        self.assertEqual(self.rpc.goals["child"]["status"], "complete")
        mutations = [
            (m, p)
            for m, p in self.rpc.calls
            if m in {"turn/interrupt", "thread/goal/set", "thread/backgroundTerminals/clean"}
        ]
        self.assertEqual(
            mutations[0], ("thread/goal/set", {"threadId": "main", "status": "paused"})
        )
        self.assertFalse(any(p["threadId"] == "other" for _, p in mutations))
        self.assertFalse(any(m == "thread/queue/start" for m, _ in self.rpc.calls))

    async def test_stop_ack_is_not_completion(self):
        self.rpc.settle_on_interrupt = False
        stop = asyncio.create_task(self.codex.stop_tree("main", timeout=2))
        await asyncio.wait_for(self.rpc.interrupted.wait(), 1)
        self.assertFalse(stop.done())
        self.rpc.threads["main"]["status"] = {"type": "idle"}
        self.rpc.threads["child"]["status"] = {"type": "idle"}
        self.assertEqual((await stop)["stopped"], ["child", "main"])

    async def test_unsettled_stop_times_out_instead_of_claiming_stopped(self):
        self.rpc.settle_on_interrupt = False
        with self.assertRaises(asyncio.TimeoutError):
            await self.codex.stop_tree("main", timeout=0.08)

    async def test_cold_descendant_is_not_loaded_just_to_clean_terminals(self):
        self.rpc.threads["child"]["status"] = {"type": "notLoaded"}
        result = await self.codex.stop_tree("main")
        self.assertEqual(result["backgroundTerminalsCleaned"], ["main"])
        self.assertFalse(any(m == "thread/resume" for m, _ in self.rpc.calls))

    async def test_remaining_background_terminal_blocks_success(self):
        self.rpc.terminals = [{"processId": "still-running"}]
        with self.assertRaises(asyncio.TimeoutError):
            await self.codex.stop_tree("main", timeout=0.08)

    async def test_native_child_lineage_is_discovered_from_source(self):
        self.rpc.threads["child"].pop("parentThreadId")
        self.rpc.threads["child"]["source"] = {
            "subAgent": {"thread_spawn": {"parent_thread_id": "main"}}
        }
        # Current wire representation has camelCase outer key and snake_case spawn variant.
        result = await self.codex.stop_tree("main")
        self.assertIn("child", result["stopped"])

    async def test_parent_owned_child_goal_blocks_durable_stop_after_interrupt(self):
        self.rpc.goals["child"] = {"status": "active"}
        with self.assertRaisesRegex(RpcError, "child goals remain active"):
            await self.codex.stop_tree("main")
        self.assertEqual(self.rpc.threads["child"]["status"]["type"], "idle")
        self.assertEqual(self.rpc.goals["main"]["status"], "paused")
        self.assertFalse(
            any(m == "thread/goal/set" and p["threadId"] == "child" for m, p in self.rpc.calls)
        )

    async def test_interrupt_race_is_reconciled_with_read_not_ignored(self):
        original = self.rpc.request

        async def raced_request(method, params):
            if method == "turn/interrupt" and params["threadId"] == "child":
                self.rpc.threads["child"]["status"] = {"type": "idle"}
                raise RpcError("no active turn to interrupt", code=-32600)
            return await original(method, params)

        self.rpc.request = raced_request
        self.assertEqual((await self.codex.stop_tree("main"))["stopped"], ["child", "main"])

    async def test_stop_discovers_live_child_before_persistent_list(self):
        original = self.rpc.request

        async def lagging_list(method, params):
            if method == "thread/list":
                return {"data": [copy.deepcopy(self.rpc.threads["main"])], "nextCursor": None}
            return await original(method, params)

        self.rpc.request = lagging_list
        self.assertEqual((await self.codex.stop_tree("main"))["stopped"], ["child", "main"])


if __name__ == "__main__":
    unittest.main()
