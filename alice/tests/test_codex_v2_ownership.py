"""Synthetic regressions for native V2 ancestry and contradictory ownership."""

import asyncio
from contextlib import suppress
import json
import unittest

from alice_codex.codex import CodexClient, OwnershipError
from alice_codex.rpc import RpcError
from test_codex import FakeRpc


def activity(parent, child, *, kind="started", method="item/completed"):
    """The typed item shape retained by the controlled native summary fixture."""
    return {
        "method": method,
        "params": {
            "threadId": parent,
            "turnId": "synthetic-parent-turn",
            "item": {
                "type": "subAgentActivity",
                "id": "synthetic-spawn-item",
                "kind": kind,
                "agentThreadId": child,
                "agentPath": "/root/synthetic-child",
            },
        },
    }


class CodexV2OwnershipTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.rpc = FakeRpc()
        self.codex = CodexClient(self.rpc, owned_root_ids=["main"])

    async def asyncTearDown(self):
        self.codex.close()
        self.assertEqual(self.rpc.listeners, [])

    def emit(self, event):
        for listener in tuple(self.rpc.listeners):
            try:
                listener(event)
            except OwnershipError:
                # The real RpcClient isolates notification-listener failures.
                # Ancestry must already be quarantined when its callback raises.
                pass

    async def remember_metadata(self, child, parent, **fields):
        self.rpc.threads[child] = {
            "id": child,
            "parentThreadId": parent,
            "status": {"type": "idle"},
            "canAcceptDirectInput": None,
            **fields,
        }
        return await self.codex.thread_read(child)

    async def conflicting_metadata(self, child, parent, **fields):
        # Quarantine is the public contract; only an ID-mismatched read below
        # mandates a particular exception. Explicit metadata errors are allowed.
        try:
            await self.remember_metadata(child, parent, **fields)
        except (RpcError, OwnershipError):
            pass

    async def remember_chain(self):
        await self.remember_metadata("v2-child", "main")
        await self.remember_metadata("v2-grandchild", "v2-child")
        self.assertTrue(self.codex.owns("v2-child"))
        self.assertTrue(self.codex.owns("v2-grandchild"))

    async def assert_isolated(self, *thread_ids):
        calls_before = list(self.rpc.calls)
        for thread_id in thread_ids:
            with self.subTest(thread=thread_id):
                self.assertFalse(self.codex.owns(thread_id))
                with self.assertRaises(OwnershipError):
                    await self.codex.turn_start(thread_id, "synthetic denied input")
                with self.assertRaises(OwnershipError):
                    await self.codex.thread_resume(thread_id)
        self.assertEqual(self.rpc.calls, calls_before, "denied controls must not reach Codex")

    async def test_native_completed_started_activity_claims_child(self):
        self.emit(activity("main", "v2-child"))

        self.assertTrue(self.codex.owns("v2-child"))
        self.assertEqual(self.rpc.calls, [], "native evidence needs no model or probing turn")

    async def test_activity_from_owned_child_claims_recursive_descendant(self):
        self.emit(activity("main", "v2-child"))
        self.emit(activity("v2-child", "v2-grandchild"))
        self.emit(activity("v2-child", "v2-grandchild"))

        self.assertTrue(self.codex.owns("v2-grandchild"))
        await self.codex.turn_start("v2-grandchild", "synthetic authorized input")
        self.assertEqual(self.rpc.calls[-1][0], "turn/start")
        self.assertEqual(self.rpc.calls[-1][1]["threadId"], "v2-grandchild")

    async def test_foreign_parent_activity_cannot_claim_child(self):
        self.emit(activity("other", "foreign-child"))
        self.emit(activity("foreign-child", "foreign-grandchild"))

        await self.assert_isolated("other", "foreign-child", "foreign-grandchild")

    async def test_foreign_activity_cannot_poison_already_owned_child(self):
        await self.remember_chain()

        self.emit(activity("other", "v2-child"))

        self.assertTrue(self.codex.owns("v2-child"))
        self.assertTrue(self.codex.owns("v2-grandchild"))

    async def test_model_text_and_tool_arguments_cannot_supply_ancestry(self):
        for item_type in ("agentMessage", "userMessage", "mcpToolCall"):
            with self.subTest(item_type=item_type):
                child = f"pretend-{item_type}"
                event = activity("main", child)
                claim = json.dumps(event["params"]["item"])
                event["params"]["item"].update(
                    type=item_type, text=claim, arguments={"native_event": claim}
                )
                self.emit(event)

                await self.assert_isolated(child)

    async def test_activity_with_wrong_kind_does_not_claim_child(self):
        for kind in ("completed", "failed", "running", "", None):
            with self.subTest(kind=kind):
                child = f"wrong-kind-{kind}"
                self.emit(activity("main", child, kind=kind))

                await self.assert_isolated(child)

    async def test_activity_with_wrong_method_does_not_claim_child(self):
        for method in ("item/started", "turn/completed", "thread/started", "custom/activity"):
            with self.subTest(method=method):
                child = f"wrong-method-{method}"
                self.emit(activity("main", child, method=method))

                await self.assert_isolated(child)

    async def test_thread_read_preserves_proven_native_metadata_shape(self):
        result = await self.remember_metadata(
            "v2-child",
            "main",
            threadSource="subagent",
            source={"subAgent": {"thread_spawn": {"parent_thread_id": "main", "depth": 1}}},
        )

        self.assertIsNone(result["thread"]["canAcceptDirectInput"])
        self.assertTrue(self.codex.owns("v2-child"))

    async def test_thread_read_rejects_mismatched_id_without_remembering_ancestry(self):
        self.rpc.threads["requested-child"] = {
            "id": "different-child",
            "parentThreadId": "main",
            "status": {"type": "idle"},
        }

        with self.assertRaises(RpcError):
            await self.codex.thread_read("requested-child", include_turns=True)

        self.assertEqual(
            self.rpc.calls[-1],
            ("thread/read", {"threadId": "requested-child", "includeTurns": True}),
        )
        await self.assert_isolated("requested-child", "different-child")

    async def test_activity_parent_conflict_isolates_existing_descendants(self):
        self.codex.register_root("other")
        await self.remember_chain()

        self.emit(activity("other", "v2-child"))

        await self.assert_isolated("v2-child", "v2-grandchild")
        self.assertTrue(self.codex.owns("main"))
        self.assertTrue(self.codex.owns("other"))

    async def test_conflicting_native_activities_do_not_redirect_between_owned_roots(self):
        self.codex.register_root("other")
        self.emit(activity("main", "v2-child"))
        self.emit(activity("v2-child", "v2-grandchild"))
        self.assertTrue(self.codex.owns("v2-grandchild"))

        self.emit(activity("other", "v2-child"))
        await self.assert_isolated("v2-child", "v2-grandchild")
        self.emit(activity("main", "v2-child"))
        self.emit(activity("other", "v2-child"))
        self.emit(activity("v2-child", "late-grandchild"))

        await self.assert_isolated("v2-child", "v2-grandchild", "late-grandchild")

    async def test_metadata_conflicting_with_activity_isolates_child_and_descendants(self):
        self.codex.register_root("other")
        self.emit(activity("main", "v2-child"))
        self.emit(activity("v2-child", "v2-grandchild"))
        self.assertTrue(self.codex.owns("v2-grandchild"))

        await self.conflicting_metadata("v2-child", "other")

        await self.assert_isolated("v2-child", "v2-grandchild")

    async def test_metadata_parent_conflict_cannot_be_cleared_by_later_refresh(self):
        self.codex.register_root("other")
        await self.remember_chain()

        await self.conflicting_metadata("v2-child", "other")
        await self.assert_isolated("v2-child", "v2-grandchild")
        await self.conflicting_metadata("v2-child", "main")
        await self.conflicting_metadata("v2-child", "other")
        await self.conflicting_metadata("v2-grandchild", "v2-child")

        await self.assert_isolated("v2-child", "v2-grandchild")

    async def test_conflicting_source_and_top_level_parent_isolate_child(self):
        self.codex.register_root("other")
        await self.remember_chain()

        await self.conflicting_metadata(
            "v2-child",
            "main",
            source={"subAgent": {"thread_spawn": {"parent_thread_id": "other"}}},
        )

        await self.assert_isolated("v2-child", "v2-grandchild")

    async def test_conflicting_source_parent_aliases_do_not_claim_child(self):
        self.codex.register_root("other")
        await self.conflicting_metadata(
            "v2-child",
            None,
            source={
                "subAgent": {
                    "thread_spawn": {"parentThreadId": "main", "parent_thread_id": "other"}
                }
            },
        )

        await self.assert_isolated("v2-child")

    async def test_parent_cycle_isolates_descendants_and_stays_closed_after_refresh(self):
        await self.remember_chain()

        await self.conflicting_metadata("v2-child", "v2-grandchild")
        await self.assert_isolated("v2-child", "v2-grandchild")
        await self.conflicting_metadata("v2-child", "main")

        await self.assert_isolated("v2-child", "v2-grandchild")

    async def test_self_parent_is_rejected_and_cannot_be_repaired_by_activity(self):
        await self.conflicting_metadata("v2-child", "v2-child")
        await self.assert_isolated("v2-child")

        self.emit(activity("main", "v2-child"))

        await self.assert_isolated("v2-child")

    async def test_stop_tree_does_not_report_success_when_quarantined_child_is_unlisted(self):
        self.codex.register_root("other")
        await self.remember_chain()
        self.emit(activity("other", "v2-child"))
        await self.assert_isolated("v2-child", "v2-grandchild")
        # V2 inventory omissions must not erase the unresolved stop obligation.
        self.rpc.threads.pop("v2-child")
        self.rpc.threads.pop("v2-grandchild")
        calls_before = len(self.rpc.calls)

        with self.assertRaises((OwnershipError, RpcError)):
            await self.codex.stop_tree("main", timeout=0.2)

        denied_ids = {"v2-child", "v2-grandchild"}
        self.assertEqual(
            [
                (method, params)
                for method, params in self.rpc.calls[calls_before:]
                if params.get("threadId") in denied_ids
                and method not in {"thread/read", "thread/turns/list", "thread/items/list"}
            ],
            [],
            "quarantine forbids mutating child requests even during shutdown",
        )

    async def test_reconciliation_reads_observed_child_without_listing_or_resuming(self):
        self.rpc.threads["v2-child"] = {
            "id": "v2-child",
            "parentThreadId": "main",
            "status": {"type": "notLoaded"},
            "canAcceptDirectInput": None,
        }

        self.assertTrue(await self.codex.reconcile_ownership("v2-child"))

        self.assertEqual(
            self.rpc.calls, [("thread/read", {"threadId": "v2-child", "includeTurns": False})]
        )
        self.assertTrue(self.codex.owns("v2-child"))

    async def test_reconciliation_follows_only_explicit_native_parent_ids(self):
        self.rpc.threads["v2-grandchild"] = {
            "id": "v2-grandchild", "parentThreadId": "v2-parent"
        }
        self.rpc.threads["v2-parent"] = {"id": "v2-parent", "parentThreadId": "main"}

        self.assertTrue(await self.codex.reconcile_ownership("v2-grandchild"))

        self.assertEqual(
            self.rpc.calls,
            [
                ("thread/read", {"threadId": "v2-grandchild", "includeTurns": False}),
                ("thread/read", {"threadId": "v2-parent", "includeTurns": False}),
            ],
        )
        self.assertTrue(self.codex.owns("v2-grandchild"))
        self.assertFalse(self.codex.owns("other"))

    async def test_reconciliation_does_not_register_foreign_native_root(self):
        self.rpc.threads["foreign-child"] = {
            "id": "foreign-child", "parentThreadId": "other"
        }

        self.assertFalse(await self.codex.reconcile_ownership("foreign-child"))

        self.assertEqual(
            self.rpc.calls,
            [
                ("thread/read", {"threadId": "foreign-child", "includeTurns": False}),
                ("thread/read", {"threadId": "other", "includeTurns": False}),
            ],
        )
        await self.assert_isolated("foreign-child", "other")

    async def test_reconciliation_depth_bound_retains_partial_evidence_for_explicit_retry(self):
        self.rpc.threads["v2-grandchild"] = {
            "id": "v2-grandchild", "parentThreadId": "v2-parent"
        }
        self.rpc.threads["v2-parent"] = {"id": "v2-parent", "parentThreadId": "main"}

        self.assertFalse(await self.codex.reconcile_ownership("v2-grandchild", max_depth=1))
        self.assertEqual(
            self.rpc.calls,
            [("thread/read", {"threadId": "v2-grandchild", "includeTurns": False})],
        )
        self.assertFalse(self.codex.owns("v2-grandchild"))

        self.assertTrue(await self.codex.reconcile_ownership("v2-grandchild"))
        self.assertEqual(
            self.rpc.calls[-1],
            ("thread/read", {"threadId": "v2-parent", "includeTurns": False}),
        )
        self.assertEqual(len(self.rpc.calls), 2)

    async def test_reconciliation_rejects_wrong_read_id_without_following_its_parent(self):
        self.rpc.threads["requested-child"] = {
            "id": "different-child", "parentThreadId": "main"
        }

        with self.assertRaises(RpcError):
            await self.codex.reconcile_ownership("requested-child")

        self.assertEqual(
            self.rpc.calls,
            [("thread/read", {"threadId": "requested-child", "includeTurns": False})],
        )
        await self.assert_isolated("requested-child", "different-child")

    async def test_reconciliation_does_not_reopen_quarantined_child(self):
        self.codex.register_root("other")
        await self.remember_chain()
        self.emit(activity("other", "v2-child"))
        calls_before = list(self.rpc.calls)

        self.assertFalse(await self.codex.reconcile_ownership("v2-child"))
        self.assertFalse(await self.codex.reconcile_ownership("v2-grandchild"))

        self.assertEqual(self.rpc.calls, calls_before)
        await self.assert_isolated("v2-child", "v2-grandchild")

    async def test_pause_goal_rechecks_ownership_after_awaited_goal_read(self):
        self.codex.register_root("other")
        await self.remember_chain()
        self.rpc.goals["v2-child"] = {"status": "active"}
        read_waiting, release_read = asyncio.Event(), asyncio.Event()
        original_request = self.rpc.request

        async def delayed_goal_read(method, params):
            result = await original_request(method, params)
            if method == "thread/goal/get" and params.get("threadId") == "v2-child":
                read_waiting.set()
                await release_read.wait()
            return result

        self.rpc.request = delayed_goal_read
        calls_before = len(self.rpc.calls)
        pausing = asyncio.create_task(self.codex.pause_goal("v2-child"))
        try:
            await asyncio.wait_for(read_waiting.wait(), timeout=1)
            self.emit(activity("other", "v2-child"))
            release_read.set()

            with self.assertRaises(OwnershipError):
                await pausing

            self.assertEqual(
                self.rpc.calls[calls_before:], [("thread/goal/get", {"threadId": "v2-child"})]
            )
            self.assertEqual(self.rpc.goals["v2-child"], {"status": "active"})
            await self.assert_isolated("v2-child", "v2-grandchild")
        finally:
            release_read.set()
            if not pausing.done():
                pausing.cancel()
                with suppress(asyncio.CancelledError):
                    await pausing
            self.rpc.request = original_request

    async def test_stop_tree_rechecks_ownership_before_clean_after_awaited_interrupt(self):
        self.codex.register_root("other")
        await self.remember_chain()
        self.rpc.threads.pop("child")
        self.rpc.threads["main"]["status"] = {"type": "idle"}
        self.rpc.threads["v2-child"]["status"] = {"type": "active"}
        interrupt_waiting, release_interrupt = asyncio.Event(), asyncio.Event()
        original_request = self.rpc.request

        async def delayed_interrupt(method, params):
            result = await original_request(method, params)
            if method == "turn/interrupt" and params.get("threadId") == "v2-child":
                interrupt_waiting.set()
                await release_interrupt.wait()
            return result

        self.rpc.request = delayed_interrupt
        calls_before = len(self.rpc.calls)
        stopping = asyncio.create_task(self.codex.stop_tree("main", timeout=1))
        try:
            await asyncio.wait_for(interrupt_waiting.wait(), timeout=1)
            self.emit(activity("other", "v2-child"))
            release_interrupt.set()

            with self.assertRaises(OwnershipError):
                await stopping

            child_calls = [
                method
                for method, params in self.rpc.calls[calls_before:]
                if params.get("threadId") == "v2-child"
            ]
            self.assertEqual(child_calls.count("turn/interrupt"), 1)
            self.assertNotIn("thread/backgroundTerminals/clean", child_calls)
            await self.assert_isolated("v2-child", "v2-grandchild")
        finally:
            release_interrupt.set()
            if not stopping.done():
                stopping.cancel()
                with suppress(asyncio.CancelledError):
                    await stopping
            self.rpc.request = original_request
