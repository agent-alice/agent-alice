"""Codex thread controls without a second model loop or transcript store."""

import asyncio
from typing import Any
import uuid

from .rpc import RpcClient, RpcError


class OwnershipError(ValueError):
    """A mutating operation targeted a thread outside Alice's registered roots."""


class CodexClient:
    def __init__(self, rpc: RpcClient, *, owned_root_ids: list[str] | None = None):
        self.rpc = rpc
        self.owned_root_ids = set(owned_root_ids or [])
        self._parents: dict[str, str] = {}
        self._active_turns: dict[str, str] = {}
        self._statuses: dict[str, str] = {}
        self._direct_input: dict[str, bool | None] = {}
        self._remove_listener = rpc.add_listener(self._on_event)

    def close(self) -> None:
        self._remove_listener()

    def register_root(self, thread_id: str) -> None:
        """Register an ID from host-owned persistent state, never from model text."""
        if not isinstance(thread_id, str) or not thread_id:
            raise ValueError("thread_id must be a nonempty string")
        self.owned_root_ids.add(thread_id)

    def owns(self, thread_id: str) -> bool:
        seen = set()
        while thread_id not in seen:
            if thread_id in self.owned_root_ids:
                return True
            seen.add(thread_id)
            thread_id = self._parents.get(thread_id, "")
        return False

    def _require_owned(self, thread_id: str) -> None:
        if not self.owns(thread_id):
            raise OwnershipError(f"Thread {thread_id} is not owned by this Alice runtime")

    def _remember_thread(self, thread: dict) -> None:
        thread_id, parent = thread.get("id"), thread.get("parentThreadId")
        if not thread_id:
            return
        # Older persisted subagent records express their parent through source.
        source = thread.get("source")
        if not parent and isinstance(source, dict):
            subagent = source.get("subAgent", {})
            if isinstance(subagent, dict):
                spawn = subagent.get("thread_spawn", {})
                if isinstance(spawn, dict):
                    parent = spawn.get("parentThreadId") or spawn.get("parent_thread_id")
        if parent:
            self._parents[thread_id] = parent
        if self.owns(thread_id):
            self._direct_input[thread_id] = thread.get("canAcceptDirectInput")
            status = thread.get("status", {})
            self._statuses[thread_id] = (
                status.get("type", "unknown") if isinstance(status, dict) else status
            )
            if isinstance(status, dict) and status.get("type") in {"idle", "notLoaded"}:
                self._active_turns.pop(thread_id, None)
            for turn in thread.get("turns", []):
                if turn.get("status") == "inProgress":
                    self._active_turns[thread_id] = turn["id"]

    def _on_event(self, event: dict) -> None:
        method, params = event.get("method"), event.get("params", {})
        if method == "thread/started":
            self._remember_thread(params.get("thread", {}))
        thread_id = params.get("threadId")
        if not thread_id or not self.owns(thread_id):
            return
        if method == "turn/started":
            self._active_turns[thread_id] = params["turn"]["id"]
        elif method == "turn/completed":
            if self._active_turns.get(thread_id) == params.get("turn", {}).get("id"):
                self._active_turns.pop(thread_id, None)

    async def thread_start(self, params: dict | None = None, **overrides: Any) -> dict:
        """Start an owned thread; its ID is not durable until a turn materializes.

        Codex may return an ID for an empty thread with no persisted rollout. The host
        must record this distinction; a later failed resume must not silently replace
        a thread that could already have executed work.
        """
        result = await self.rpc.request("thread/start", {**(params or {}), **overrides})
        thread = result["thread"]
        self.register_root(thread["id"])
        self._remember_thread(thread)
        return result

    async def thread_resume(self, thread_id: str, **overrides: Any) -> dict:
        self._require_owned(thread_id)
        result = await self.rpc.request("thread/resume", {**overrides, "threadId": thread_id})
        if result["thread"]["id"] != thread_id:
            raise RpcError("Codex resumed a different thread")
        self._remember_thread(result["thread"])
        return result

    async def thread_read(self, thread_id: str, *, include_turns: bool = False) -> dict:
        result = await self.rpc.request(
            "thread/read", {"threadId": thread_id, "includeTurns": include_turns}
        )
        self._remember_thread(result["thread"])
        return result

    async def thread_list(self, **params: Any) -> dict:
        result = await self.rpc.request("thread/list", params)
        for thread in result.get("data", []):
            self._remember_thread(thread)
        return result

    async def find_turn_by_client_id(
        self, thread_id: str, client_message_id: str, *, page_size: int = 100, max_pages: int = 1000
    ) -> str | None:
        """Find the native turn containing a host intent's stable message ID.

        The wire request uses clientUserMessageId; persisted userMessage items use
        clientId. Scan every page to detect ambiguous duplicate submissions. None means
        not currently observed, not proof the operation never reached Codex; it does
        not authorize resubmission. Incomplete or contradictory evidence raises.
        """
        self._require_owned(thread_id)
        if not isinstance(client_message_id, str) or not client_message_id:
            raise ValueError("client_message_id must be a nonempty string")
        if not 1 <= page_size <= 100 or max_pages < 1:
            raise ValueError("page_size must be 1..100 and max_pages must be positive")
        cursor = None
        seen_cursors = set()
        found = None
        for _ in range(max_pages):
            result = await self.rpc.request(
                "thread/items/list",
                {
                    "threadId": thread_id,
                    "cursor": cursor,
                    "limit": page_size,
                    "sortDirection": "desc",
                },
            )
            entries = result.get("data")
            if not isinstance(entries, list):
                raise RpcError("Codex returned invalid history; message outcome is unknown")
            for entry in entries:
                if not isinstance(entry, dict) or not isinstance(entry.get("item"), dict):
                    raise RpcError("Codex returned an invalid history item")
                item = entry["item"]
                if item.get("type") != "userMessage" or item.get("clientId") != client_message_id:
                    continue
                turn_id = entry.get("turnId")
                if not isinstance(turn_id, str) or not turn_id:
                    raise RpcError("Matching Codex message has no turn ID")
                if found is not None and found != turn_id:
                    raise RpcError(
                        "A client message ID appears in multiple Codex turns; reconcile manually"
                    )
                found = turn_id
            cursor = result.get("nextCursor")
            if cursor is None:
                return found
            if not isinstance(cursor, str) or not cursor or cursor in seen_cursors:
                raise RpcError("Codex history pagination returned an invalid or repeated cursor")
            seen_cursors.add(cursor)
        raise RpcError("Codex history scan limit reached; message outcome is still unknown")

    async def queue_add(self, thread_id: str, text: str, *, client_message_id: str) -> dict:
        """Caller supplies the durable occurrence ID; a timeout requires reconciliation."""
        self._require_owned(thread_id)
        if not client_message_id:
            raise ValueError("queue additions require a stable client_message_id")
        return await self.rpc.request(
            "thread/queue/add",
            {
                "threadId": thread_id,
                "input": self._input(text),
                "clientUserMessageId": client_message_id,
            },
        )

    async def queue_list(self, thread_id: str, **params: Any) -> dict:
        self._require_owned(thread_id)
        return await self.rpc.request("thread/queue/list", {**params, "threadId": thread_id})

    async def queue_start(self, thread_id: str, *, submission_id: str | None = None) -> dict:
        """Explicit resume only; a heartbeat must never unpause an interrupted queue."""
        self._require_owned(thread_id)
        params = {"threadId": thread_id}
        if submission_id:
            params["queuedSubmissionId"] = submission_id
        return await self.rpc.request("thread/queue/start", params)

    async def turn_start(
        self, thread_id: str, text: str, *, client_message_id: str | None = None, **overrides: Any
    ) -> dict:
        self._require_owned(thread_id)
        result = await self.rpc.request(
            "turn/start",
            {
                **overrides,
                "threadId": thread_id,
                "input": self._input(text),
                "clientUserMessageId": client_message_id or str(uuid.uuid4()),
            },
        )
        self._active_turns[thread_id] = result["turn"]["id"]
        return result

    async def turn_steer(
        self, thread_id: str, turn_id: str, text: str, *, client_message_id: str | None = None
    ) -> dict:
        self._require_owned(thread_id)
        return await self.rpc.request(
            "turn/steer",
            {
                "threadId": thread_id,
                "expectedTurnId": turn_id,
                "input": self._input(text),
                "clientUserMessageId": client_message_id or str(uuid.uuid4()),
            },
        )

    async def turn_interrupt(self, thread_id: str, turn_id: str) -> dict:
        self._require_owned(thread_id)
        # Acceptance is not proof of quiescence. stop_tree verifies subsequent state.
        return await self.rpc.request("turn/interrupt", {"threadId": thread_id, "turnId": turn_id})

    async def pause_goal(self, thread_id: str) -> dict:
        self._require_owned(thread_id)
        result = await self.rpc.request("thread/goal/get", {"threadId": thread_id})
        goal = result.get("goal")
        if goal and goal.get("status") == "active":
            return await self.rpc.request(
                "thread/goal/set", {"threadId": thread_id, "status": "paused"}
            )
        return result

    async def _discover_tree(self, root_id: str) -> set[str]:
        # An empty sourceKinds list means interactive sources, NOT all sources.
        cursor = None
        seen_cursors = set()
        while True:
            page = await self.thread_list(
                limit=100,
                cursor=cursor,
                modelProviders=[],
                sourceKinds=["cli", "vscode", "exec", "appServer", "subAgent", "unknown"],
            )
            cursor = page.get("nextCursor")
            if not cursor:
                break
            if cursor in seen_cursors:
                raise RpcError("Codex thread pagination repeated a cursor")
            seen_cursors.add(cursor)
        # Newly spawned V2 children can be running before persistent thread/list
        # exposes them. Query the runtime inventory as well, without resuming any
        # stored thread or assuming thread/started was broadcast to this client.
        cursor = None
        seen_cursors = set()
        while True:
            page = await self.rpc.request("thread/loaded/list", {"limit": 100, "cursor": cursor})
            for thread_id in page.get("data", []):
                await self.thread_read(thread_id)
            cursor = page.get("nextCursor")
            if not cursor:
                break
            if cursor in seen_cursors:
                raise RpcError("Codex loaded-thread pagination repeated a cursor")
            seen_cursors.add(cursor)
        descendants = {root_id}
        while True:
            expanded = descendants | {
                child for child, parent in self._parents.items() if parent in descendants
            }
            if expanded == descendants:
                return descendants
            descendants = expanded

    async def discover_owned_threads(self) -> set[str]:
        """Refresh native ancestry, then return only registered roots and their children."""
        if not self.owned_root_ids:
            return set()
        await self._discover_tree(next(iter(self.owned_root_ids)))
        return {thread for thread in self.owned_root_ids | set(self._parents) if self.owns(thread)}

    async def _active_turn(self, thread_id: str) -> str | None:
        thread = (await self.thread_read(thread_id))["thread"]
        status = thread.get("status", {})
        status_type = status.get("type") if isinstance(status, dict) else status
        if status_type in {"idle", "notLoaded"}:
            return None
        result = await self.rpc.request(
            "thread/turns/list",
            {
                "threadId": thread_id,
                "limit": 5,
                "sortDirection": "desc",
                "itemsView": "summary",
            },
        )
        for turn in result.get("data", []):
            if turn.get("status") == "inProgress":
                return turn["id"]
        if status_type == "active":
            raise RpcError(f"Thread {thread_id} is active but its active turn is unavailable")
        return None

    async def stop_tree(
        self, root_id: str, *, timeout: float = 20, clean_background_terminals: bool = True
    ) -> dict:
        """Pause the root, interrupt descendants, and verify their turns stopped.

        The host must persist its autonomy pause BEFORE this call and keep all new
        dispatch disabled. Unknown/failed outcomes raise; they never report success.
        This does not claim to undo external side effects or stop detached OS processes.
        Parent-owned V2 children reject direct goal changes. If one retains an active
        goal after interruption, fail explicitly so the host can stop its owned server.
        """
        if root_id not in self.owned_root_ids:
            raise OwnershipError("stop_tree requires an explicitly registered Alice root")

        async def stop() -> dict:
            interrupted: set[tuple[str, str]] = set()
            cleaned: set[str] = set()
            await self.pause_goal(root_id)
            while True:
                ids = await self._discover_tree(root_id)
                # Stop root first so it cannot keep creating new descendants.
                ordered = [root_id, *sorted(ids - {root_id})]
                active = []
                unpaused_child_goals = []
                for thread_id in ordered:
                    turn_id = await self._active_turn(thread_id)
                    if thread_id != root_id and self._direct_input.get(thread_id) is True:
                        await self.pause_goal(thread_id)
                    if turn_id:
                        active.append(thread_id)
                        key = (thread_id, turn_id)
                        if key not in interrupted:
                            try:
                                await self.turn_interrupt(thread_id, turn_id)
                            except RpcError as error:
                                # The parent can finish/cancel a child before our
                                # interrupt arrives. Confirm state, never swallow
                                # arbitrary errors or a still-active same turn.
                                race = error.code == -32600 and (
                                    str(error) == "no active turn to interrupt"
                                    or str(error).startswith("expected active turn id ")
                                )
                                if not race or await self._active_turn(thread_id) == turn_id:
                                    raise
                            else:
                                interrupted.add(key)
                    if (
                        clean_background_terminals
                        and thread_id not in cleaned
                        and self._statuses.get(thread_id) != "notLoaded"
                    ):
                        await self.rpc.request(
                            "thread/backgroundTerminals/clean", {"threadId": thread_id}
                        )
                        cleaned.add(thread_id)
                    if clean_background_terminals and thread_id in cleaned:
                        terminals = await self.rpc.request(
                            "thread/backgroundTerminals/list",
                            {
                                "threadId": thread_id,
                                "limit": 1,
                            },
                        )
                        if terminals.get("data"):
                            active.append(thread_id)
                    if thread_id != root_id and self._direct_input.get(thread_id) is not True:
                        goal = (
                            await self.rpc.request("thread/goal/get", {"threadId": thread_id})
                        ).get("goal")
                        if goal and goal.get("status") == "active":
                            unpaused_child_goals.append(thread_id)
                if not active:
                    # Discover again after interrupts, including late-published children.
                    if await self._discover_tree(root_id) == ids:
                        if unpaused_child_goals:
                            raise RpcError(
                                "Parent-owned child goals remain active after interruption; "
                                "stop the owned App Server before claiming a durable stop: "
                                + ", ".join(sorted(unpaused_child_goals))
                            )
                        return {
                            "stopped": sorted(ids),
                            "interrupted": len(interrupted),
                            "backgroundTerminalsCleaned": sorted(cleaned),
                        }
                await asyncio.sleep(0.05)

        return await asyncio.wait_for(stop(), timeout)

    @staticmethod
    def _input(text: str) -> list[dict]:
        if not isinstance(text, str) or not text.strip():
            raise ValueError("Codex input must contain text")
        return [{"type": "text", "text": text}]


Codex = CodexClient
