"""One owned Codex App Server and a small, durable scheduling/control adapter.

No model calls, prompt history reconstruction, or context compression live here.
Codex remains the executor and authoritative store for conversation state.
"""

import asyncio
from contextlib import suppress
from dataclasses import asdict
import hashlib
import json
import os
import signal
import subprocess
import time
import uuid

from .codex import CodexClient
from .calendar import prepare_dispatch, dispatch_completion, summary_level
from .config import RuntimeConfig
from .control import MAX_MESSAGE
from .files import SingletonLock, read_json, write_json
from .memory import MemoryStore
from .journal import NativeJournal
from .resources import ResourceLedger
from .rpc import RpcClient, RpcError
from .scheduler import Scheduler, RejectedDispatch
from .store import DispatchReceipt, Store


def process_identity(pid: int) -> str | None:
    result = subprocess.run(
        ["ps", "-p", str(pid), "-o", "lstart=", "-o", "command="],
        capture_output=True,
        text=True,
        timeout=5,
    )
    return result.stdout.strip() or None


def process_birth(pid: int) -> str | None:
    result = subprocess.run(
        ["ps", "-p", str(pid), "-o", "lstart="], capture_output=True, text=True, timeout=5
    )
    return result.stdout.strip() or None


class Service:
    def __init__(self, config: RuntimeConfig):
        self.config = config
        self.path = config.root / "state/runtime.json"
        self.state = (
            read_json(self.path)
            if self.path.exists()
            else {
                "version": 1,
                "tasks": {},
                "intents": {},
                "lifecycle": "new",
                "server": None,
            }
        )
        if (
            not isinstance(self.state, dict)
            or self.state.get("version") != 1
            or not isinstance(self.state.get("tasks"), dict)
            or not isinstance(self.state.get("intents"), dict)
        ):
            raise ValueError("Unsupported or damaged runtime state; preserved for recovery")
        self.store = Store(config.database)
        self.memory = MemoryStore(config.root)
        self.resources = ResourceLedger(config.root / "state/resources.sqlite3")
        self._resource_refresh_at = 0.0
        self.stop_event = asyncio.Event()
        self.rpc: RpcClient | None = None
        self.codex: CodexClient | None = None
        self.journal: NativeJournal | None = None
        self.process: asyncio.subprocess.Process | None = None
        self.ready = False
        self.stopping = False
        self.error: str | None = None
        self._task_locks: dict[str, asyncio.Lock] = {}
        self._notifications: asyncio.Queue = asyncio.Queue(maxsize=2048)
        self._background: list[asyncio.Task] = []
        self._handlers: set[asyncio.Task] = set()
        self._native_interrupts: dict[str, str] = {}
        self.scheduler = Scheduler(self.store, self)

    def save(self) -> None:
        write_json(self.path, self.state)

    def _lock(self, key: str) -> asyncio.Lock:
        return self._task_locks.setdefault(key, asyncio.Lock())

    async def _recover_orphan(self) -> None:
        previous = self.state.get("server")
        if not previous:
            return
        pid = previous["pid"]
        current = process_identity(pid)
        if not current:
            return
        born = process_birth(pid)
        same_identity = (
            born == previous["birth"] if previous.get("birth") else current == previous["identity"]
        )
        if (
            not same_identity
            or os.getpgid(pid) != pid
            or str(self.config.codex_socket) not in current
        ):
            raise RuntimeError(
                "Recorded server identity changed; refusing to signal another process"
            )
        # An orphan can still own terminals in other process groups. Ask its
        # native runtime to stop them before terminating our original group.
        recovered_rpc = None
        try:
            recovered_rpc = await RpcClient.connect_unix(
                self.config.codex_socket, timeout=2, request_timeout=5
            )
            await recovered_rpc.initialize(name="alice_recovery")
            recovered = CodexClient(
                recovered_rpc,
                owned_root_ids=[task["thread_id"] for task in self.state["tasks"].values()],
            )
            try:
                for task in list(self.state["tasks"].values()):
                    await recovered.stop_tree(task["thread_id"], timeout=5)
            finally:
                recovered.close()
        except Exception:
            # Transport failure doesn't authorize another invocation. The only
            # fallback is termination of this verified, owned process group.
            pass
        finally:
            if recovered_rpc:
                await recovered_rpc.close()
        if process_birth(pid) != born or os.getpgid(pid) != pid:
            raise RuntimeError("Owned server identity changed during recovery")
        os.killpg(pid, signal.SIGTERM)
        for _ in range(100):
            if process_birth(pid) != born:
                return
            await asyncio.sleep(0.05)
        # Only the same verified process group, never a PID found by name alone.
        if process_birth(pid) == born and os.getpgid(pid) == pid:
            os.killpg(pid, signal.SIGKILL)
        for _ in range(100):
            if process_birth(pid) != born:
                return
            await asyncio.sleep(0.05)
        raise RuntimeError("Owned orphan server did not exit")

    async def run(self) -> None:
        self.config.prepare_directories()
        self.config.verify_binary()
        with SingletonLock(self.config.root / "state/service.lock"):
            if self.state["lifecycle"] in {"new", "running", "starting", "stopping"}:
                self.store.set_autonomy_paused(True)
            await self._recover_orphan()
            self.state["lifecycle"] = "starting"
            self.state["server"] = None
            self.save()
            self.config.codex_socket.unlink(missing_ok=True)
            self.config.control_socket.unlink(missing_ok=True)
            log = open(self.config.root / "logs/codex-server.log", "ab", buffering=0)
            try:
                self.process = await asyncio.create_subprocess_exec(
                    self.config.codex_binary,
                    "app-server",
                    "--listen",
                    f"unix://{self.config.codex_socket}",
                    env=self.config.environment(),
                    cwd=self.config.workspace,
                    stdin=subprocess.DEVNULL,
                    stdout=log,
                    stderr=log,
                    start_new_session=True,
                )
                self.state["server"] = {
                    "pid": self.process.pid,
                    "identity": process_identity(self.process.pid),
                    "birth": process_birth(self.process.pid),
                }
                self.save()
                deadline = time.monotonic() + 30
                while True:
                    if self.process.returncode is not None:
                        raise RuntimeError(
                            "Owned Codex App Server exited during startup; inspect private log"
                        )
                    if self.config.codex_socket.exists():
                        try:
                            self.rpc = await RpcClient.connect_unix(
                                self.config.codex_socket, timeout=2
                            )
                            break
                        except (OSError, TimeoutError):
                            pass
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Owned Codex App Server did not become ready")
                    await asyncio.sleep(0.1)
                await self.rpc.initialize()
                self.codex = CodexClient(
                    self.rpc,
                    owned_root_ids=[task["thread_id"] for task in self.state["tasks"].values()],
                )
                self.journal = NativeJournal(self.memory, self.rpc, self.codex.owns)
                self.rpc.add_listener(self._on_notification)
                await self.archive_native_history("startup")
                server = await asyncio.start_unix_server(
                    self._control_client, str(self.config.control_socket), limit=MAX_MESSAGE
                )
                self.config.control_socket.chmod(0o600)
                async with server:
                    self._background = [
                        asyncio.create_task(self._tick()),
                        asyncio.create_task(self._record_events()),
                    ]
                    self.state["lifecycle"] = "running"
                    self.save()
                    self.ready = True
                    await self.stop_event.wait()
                    self.ready = False
            finally:
                self.stopping = True
                self.ready = False
                for task in [*self._background, *self._handlers]:
                    task.cancel()
                await asyncio.gather(*self._background, *self._handlers, return_exceptions=True)
                await self._shutdown(log)

    async def _shutdown(self, log) -> None:
        try:
            # Shutdown never means that externally published work was undone.
            try:
                self.store.set_autonomy_paused(True)
                self.state["lifecycle"] = "stopping"
                self.save()
            except Exception as error:
                self.error = f"Pause persistence failed: {type(error).__name__}"
            if self.codex:
                for task in list(self.state["tasks"].values()):
                    try:
                        await self.codex.stop_tree(task["thread_id"], timeout=8)
                    except Exception as error:
                        self.error = (
                            f"Shutdown required owned-server termination: {type(error).__name__}"
                        )
                if self.journal:
                    await self.archive_native_history("shutdown")
                self.codex.close()
        finally:
            try:
                if self.rpc:
                    with suppress(Exception):
                        await self.rpc.close()
                if self.process and self.process.returncode is None:
                    with suppress(ProcessLookupError):
                        os.killpg(self.process.pid, signal.SIGTERM)
                    try:
                        await asyncio.wait_for(self.process.wait(), 10)
                    except TimeoutError:
                        with suppress(ProcessLookupError):
                            os.killpg(self.process.pid, signal.SIGKILL)
                        await self.process.wait()
                self.state["lifecycle"] = "stopped"
                self.state["server"] = None
                self.save()
            finally:
                try:
                    self.config.control_socket.unlink(missing_ok=True)
                    self.config.codex_socket.unlink(missing_ok=True)
                finally:
                    log.close()
                    self.store.close()

    def _fail(self, message: str) -> None:
        self.error = message
        self.stopping = True
        self.ready = False
        self.stop_event.set()
        with suppress(Exception):
            self.store.set_autonomy_paused(True)

    def _on_notification(self, event: dict) -> None:
        if event.get("method") not in {
            "turn/started",
            "turn/completed",
            "item/completed",
            "thread/tokenUsage/updated",
            "account/rateLimits/updated",
        }:
            return
        params = event.get("params", {})
        if event["method"] in {"thread/tokenUsage/updated", "account/rateLimits/updated"}:
            try:
                # Keep receiving accounting through native interruption, even
                # after the asynchronous transcript worker has been cancelled.
                if event["method"] == "thread/tokenUsage/updated":
                    if self.codex and self.codex.owns(params.get("threadId")):
                        self.resources.record_token_usage(params)
                else:
                    snapshot = params.get("rateLimits")
                    limit_id = snapshot.get("limitId") if isinstance(snapshot, dict) else None
                    observed = {"rateLimits": snapshot}
                    if limit_id:
                        observed["rateLimitsByLimitId"] = {limit_id: snapshot}
                    self.resources.record_rate_limits(observed)
            except Exception:
                self._fail("Native resource observations could not be persisted")
            return
        if (
            event.get("method") == "turn/completed"
            and params.get("turn", {}).get("status") == "interrupted"
        ):
            try:
                key = self._mark_native_interrupt(params.get("threadId"))
                if key is not None:
                    self._native_interrupts[params["threadId"]] = key
            except Exception:
                self._fail("Native interruption could not be persisted")
                return
        try:
            self._notifications.put_nowait(event)
        except asyncio.QueueFull:
            self._fail("Event archive backpressure; autonomous dispatch has stopped")

    def _mark_native_interrupt(self, thread: str) -> str | None:
        # The TUI can interrupt a turn that never passed through Alice.submit.
        # Persist before the next await so a due heartbeat cannot revive it.
        for key, task in list(self.state["tasks"].items()):
            if task["thread_id"] == thread and not task.get("paused"):
                task["paused"] = True
                if key == "main":
                    self.store.set_autonomy_paused(True)
                self.save()
                return key
        return None

    async def _record_events(self) -> None:
        if self.journal is None:
            self.journal = NativeJournal(self.memory, self.rpc, self.codex.owns)
        try:
            while True:
                event = await self._notifications.get()
                params = event.get("params", {})
                thread = params.get("threadId")
                if not thread or not self.codex.owns(thread):
                    continue
                entity = params.get("turn") or params.get("item") or {}
                if event["method"] == "turn/started":
                    for task in self.state["tasks"].values():
                        if task["thread_id"] == thread:
                            task["has_input"] = True
                    self.save()
                self.journal.record_live(event)
                if event["method"] == "item/completed" and entity.get("type") == "userMessage":
                    intent = self.state["intents"].get(entity.get("clientId"))
                    if (
                        intent
                        and intent.get("thread_id") == thread
                        and intent["status"] not in {"completed", "failed"}
                    ):
                        intent.update(turn_id=params["turnId"], status="accepted")
                        self.save()
                if event["method"] == "turn/completed":
                    self._complete_turn(thread, entity)
                    key = self._native_interrupts.pop(thread, None)
                    if key is not None:
                        await self.pause(None if key == "main" else key)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._fail(f"Event recording failed: {type(error).__name__}")

    async def archive_native_history(self, reason: str) -> None:
        try:
            async with asyncio.timeout(15):
                threads = await self.codex.discover_owned_threads()
                report = await self.journal.backfill(threads)
        except TimeoutError:
            report = {
                "traversal_complete": False,
                "notification_log_reconstructed": False,
                "error": "native_archive_deadline_exceeded",
            }
        except RpcError:
            report = {
                "traversal_complete": False,
                "notification_log_reconstructed": False,
                "error": "native_inventory_unavailable",
            }
        self.state["journal"] = {"reason": reason, "observed_at": time.time(), **report}
        self.save()

    def _complete_turn(self, thread: str, turn: dict) -> None:
        for intent_id, intent in self.state["intents"].items():
            if intent.get("thread_id") != thread or intent.get("turn_id") != turn["id"]:
                continue
            # This is executor completion, not independent business verification.
            status = "completed" if turn["status"] == "completed" else "failed"
            if intent.get("summary_plan") and status == "completed":
                evidence = dispatch_completion(read_json(intent["summary_plan"]), self.memory)
                status = "completed" if evidence["complete"] else "unknown"
                intent["summary_evidence"] = evidence
            intent.update(status=status, outcome=turn["status"])
            if intent.get("event_id"):
                receipt = DispatchReceipt(
                    status,
                    thread,
                    turn["id"],
                    detail="Codex turn ended; external outcome requires task evidence",
                )
                old = self.store.get_event(intent["event_id"])
                if old.status in {"sending", "accepted", "unknown"}:
                    self.store.record_receipt(intent["event_id"], receipt)
        self.save()

    async def _tick(self) -> None:
        try:
            while True:
                if self.process.returncode is not None or not self.rpc.connected:
                    raise RuntimeError("Owned Codex connection was lost")
                if time.monotonic() >= self._resource_refresh_at:
                    await self.refresh_resources()
                await self.reconcile()
                await self.scheduler.poll()
                await asyncio.sleep(self.config.poll_seconds)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._fail(f"Dispatch stopped: {type(error).__name__}: {error}")

    async def refresh_resources(self) -> dict:
        self._resource_refresh_at = time.monotonic() + 60
        try:
            result = await self.rpc.request(
                "account/rateLimits/read", {"excludeResetCreditDetails": True}, timeout=5
            )
        except (RpcError, TimeoutError):
            # Missing quota data cannot clear an earlier explicit exhaustion.
            self.resources.record_rate_limits(None)
        else:
            self.resources.record_rate_limits(result)
        return self.resources.status()

    async def ensure_thread(self, key: str) -> dict:
        if not isinstance(key, str) or not key or len(key) > 150:
            raise ValueError("Task name must contain 1–150 characters")
        async with self._lock("thread:" + key):
            task = self.state["tasks"].get(key)
            if task:
                try:
                    result = await self.codex.thread_read(task["thread_id"])
                    status = result["thread"].get("status", {}).get("type")
                    if status == "notLoaded":
                        await self.codex.thread_resume(
                            task["thread_id"],
                            cwd=str(self.config.workspace),
                            model=self.config.model,
                            **self.config.native_permission_params(),
                        )
                    return task
                except RpcError as error:
                    untouched = not task.get("has_input") and not any(
                        intent["thread_id"] == task["thread_id"]
                        for intent in self.state["intents"].values()
                    )
                    if not untouched or "no rollout found" not in str(error):
                        raise
                    # Codex intentionally doesn't persist an unused empty thread.
                    # Never use this fallback after any acknowledged/unknown input.
                    self.state.setdefault("replaced_empty_threads", []).append(task["thread_id"])
                    del self.state["tasks"][key]
                    self.save()
            if self.stopping:
                raise RejectedDispatch("Service is stopping")
            result = await self.codex.thread_start(
                cwd=str(self.config.workspace),
                model=self.config.model,
                approvalPolicy="never",
                **self.config.native_permission_params(),
                developerInstructions=(
                    "You are Alice. Read AGENTS.md, SOUL.md, USER.md and "
                    "memory/MEMORY.md before substantive work. Retrieve older experience on demand "
                    "using alice memory tools; do not fill context with the archive. "
                    "Use native Codex execution, compaction and collaboration. "
                    "Only explicit user goals authorize creating a persistent Goal."
                ),
            )
            task = {
                "thread_id": result["thread"]["id"],
                "paused": False,
                "created_at": time.time(),
                "has_input": False,
            }
            self.state["tasks"][key] = task
            self.save()
            return task

    async def is_busy(self, target: str) -> bool:
        if self.stopping or not self.ready or self.store.is_autonomy_paused():
            return True
        if not self.resources.can_dispatch(automatic=True)["allowed"]:
            return True
        task = self.state["tasks"].get(target)
        if task and task.get("paused"):
            return True
        if target.startswith("summary:L"):
            rank = {"L1": 1, "L2": 2, "L3": 3, "L4": 4}
            level = target.removeprefix("summary:")
            if level not in rank:
                raise ValueError("Unknown summary level")
            for event in self.store.list_events():
                lower = summary_level(event.job)
                if lower and rank[lower] < rank[level] and event.status != "completed":
                    return True  # Staggering clocks is not proof that lower summaries committed.
        active = 0
        task_snapshot = list(self.state["tasks"].items())
        task_ids = {(name, item["thread_id"]) for name, item in task_snapshot}
        for name, item in task_snapshot:
            if item.get("reconcile_error") == "native_history_missing":
                continue
            try:
                thread = (await self.codex.thread_read(item["thread_id"]))["thread"]
            except RpcError as error:
                if "no rollout found" in str(error):
                    if item.get("has_input"):
                        item.update(paused=True, reconcile_error="native_history_missing")
                        self.save()
                        if name == target:
                            return True
                    continue
                raise
            if thread.get("status", {}).get("type") == "active":
                active += 1
                if name == target:
                    return True
        return active >= self.config.max_active_tasks or task_ids != {
            (name, item["thread_id"]) for name, item in self.state["tasks"].items()
        }

    async def submit(
        self,
        target: str,
        text: str,
        *,
        intent_id: str | None = None,
        event_id: str | None = None,
        automatic: bool = False,
        summary_plan: str | None = None,
    ) -> dict:
        if not isinstance(text, str) or not text.strip():
            raise ValueError("Task input cannot be empty")
        intent_id = intent_id or str(uuid.uuid4())
        fingerprint = hashlib.sha256((target + "\0" + text).encode()).hexdigest()
        async with self._lock("input:" + target):
            if self.stopping or (
                automatic
                and (
                    self.store.is_autonomy_paused()
                    or not self.resources.can_dispatch(automatic=True)["allowed"]
                )
            ):
                raise RejectedDispatch("Autonomy is paused")
            if intent_id in self.state["intents"]:
                prior = self.state["intents"][intent_id]
                if prior.get("input_sha256") != fingerprint:
                    raise ValueError("Request ID already identifies different input")
                return prior
            task = await self.ensure_thread(target)
            if task.get("paused"):
                raise RejectedDispatch("Task is paused; resume it explicitly")
            intent = {
                "id": intent_id,
                "target": target,
                "thread_id": task["thread_id"],
                "status": "sending",
                "event_id": event_id,
                "created_at": time.time(),
                "input_sha256": fingerprint,
            }
            if summary_plan:
                intent["summary_plan"] = summary_plan
            self.state["intents"][intent_id] = intent
            self.save()  # Save intent before any RPC can start side effects.
            try:
                native = (await self.codex.thread_read(task["thread_id"]))["thread"]
                if (
                    self.stopping
                    or task.get("paused")
                    or (
                        automatic
                        and (
                            self.store.is_autonomy_paused()
                            or not self.resources.can_dispatch(automatic=True)["allowed"]
                        )
                    )
                ):
                    intent["status"] = "failed"
                    self.save()
                    raise RejectedDispatch("Dispatch paused before input was sent")
                if native.get("status", {}).get("type") == "active":
                    result = await self.codex.queue_add(
                        task["thread_id"], text, client_message_id=intent_id
                    )
                    if intent["status"] not in {"completed", "failed", "accepted"}:
                        intent.update(status="queued", queue_receipt=result)
                else:
                    result = await self.codex.turn_start(
                        task["thread_id"], text, client_message_id=intent_id, approvalPolicy="never"
                    )
                    if intent["status"] not in {"completed", "failed"}:
                        intent.update(status="accepted", turn_id=result["turn"]["id"])
                task["has_input"] = True
            except BaseException:
                if intent["status"] != "failed":
                    intent["status"] = "unknown"
                self.save()
                raise
            self.save()
            return intent

    async def dispatch(self, event) -> DispatchReceipt:
        prompt = (
            f"Scheduled occurrence {event.id}; due_at={event.due_at}; "
            f"through_at={event.through_at}; catch_up={event.catch_up}.\n" + event.prompt
        )
        summary_plan = None
        if summary_level(event.job):
            plan = prepare_dispatch(event, self.memory)
            if not plan["prepared"]:
                return DispatchReceipt(
                    "completed", detail=f"No source records; explicit skips in {plan['plan_path']}"
                )
            prompt, summary_plan = plan["prompt"], plan["plan_path"]
        if event.job.kind == "heartbeat":
            heartbeat = self.config.workspace / "HEARTBEAT.md"
            prompt += (
                "\nRead HEARTBEAT.md and .alice/prompts/autonomy-review.md. If nothing warrants "
                "action, wait without creating a Goal or inventing work."
            )
            if not heartbeat.exists():
                raise RejectedDispatch("HEARTBEAT.md is missing")
        target = f"scheduled:{event.id}" if event.target == "new" else event.target
        result = await self.submit(
            target,
            prompt,
            intent_id=event.id,
            event_id=event.id,
            automatic=True,
            summary_plan=summary_plan,
        )
        return DispatchReceipt(
            "accepted" if result["status"] in {"queued", "accepted"} else result["status"],
            result["thread_id"],
            result.get("turn_id"),
        )

    async def reconcile(self) -> None:
        # Re-read native state for acknowledged turns, including completion missed
        # during disconnect. An unacknowledged RPC is never blindly replayed.
        for intent in list(self.state["intents"].values()):
            if intent["status"] not in {"accepted", "unknown", "sending", "queued"}:
                continue
            if intent.get("reconcile_after", 0) > time.time():
                continue
            try:
                await self._reconcile_intent(intent)
            except RpcError as error:
                if "no rollout found" not in str(error):
                    raise
                # An ambiguous send to unavailable history blocks that task,
                # not every unrelated root. Absence never authorizes replay.
                intent.update(
                    status="unknown",
                    reconcile_error="native_history_missing",
                    reconcile_after=time.time() + 60,
                )
                for task in self.state["tasks"].values():
                    if task["thread_id"] == intent["thread_id"]:
                        task.update(paused=True, reconcile_error="native_history_missing")
                self.save()

    async def _reconcile_intent(self, intent: dict) -> None:
        if not intent.get("turn_id"):
            turn_id = await self.codex.find_turn_by_client_id(intent["thread_id"], intent["id"])
            if turn_id is None:
                return
            intent.update(turn_id=turn_id, status="accepted")
            self.save()
        cursor, seen = None, set()
        while True:
            page = await self.rpc.request(
                "thread/turns/list",
                {
                    "threadId": intent["thread_id"],
                    "cursor": cursor,
                    "limit": 100,
                    "sortDirection": "desc",
                    "itemsView": "summary",
                },
            )
            match = next((t for t in page["data"] if t["id"] == intent["turn_id"]), None)
            if match:
                if match["status"] != "inProgress":
                    self._complete_turn(intent["thread_id"], match)
                    if match["status"] == "interrupted":
                        key = self._mark_native_interrupt(intent["thread_id"])
                        if key is not None:
                            await self.pause(None if key == "main" else key)
                return
            cursor = page.get("nextCursor")
            if not cursor:
                return
            if cursor in seen or len(seen) >= 1000:
                raise RpcError("Turn reconciliation exceeded its pagination boundary")
            seen.add(cursor)

    async def pause(self, target: str | None = None) -> dict:
        if target is None:
            self.store.set_autonomy_paused(True)
            keys = list(self.state["tasks"])
        else:
            if target not in self.state["tasks"]:
                raise ValueError("Unknown task")
            keys = [target]
        for key in keys:
            self.state["tasks"][key]["paused"] = True
        self.save()
        results = {}
        for key in keys:
            async with self._lock("input:" + key):
                try:
                    results[key] = await self.codex.stop_tree(self.state["tasks"][key]["thread_id"])
                except Exception:
                    self._fail("Task tree pause could not be proven")
                    raise RuntimeError(
                        "Could not prove task tree paused; stopping the owned Codex server"
                    )
        return {"paused": True, "tasks": results}

    async def handle(self, action: str, params: dict) -> dict:
        if action == "status":
            return {
                "ready": self.ready,
                "pid": os.getpid(),
                "codex_pid": self.process.pid,
                "codex_version": self.config.codex_version,
                "autonomy_paused": self.store.is_autonomy_paused(),
                "tasks": self.state["tasks"],
                "resources": self.resources.status(),
                "error": self.error,
            }
        if not self.ready or self.stopping:
            raise RuntimeError("Service is not accepting work")
        if action == "shutdown":
            self._fail("Shutdown requested")
            return {"stopping": True}
        if action == "pause":
            return await self.pause(params.get("target"))
        if action == "resume":
            target = params.get("target")
            keys = (
                [target]
                if target
                else [name for name, task in self.state["tasks"].items() if task.get("paused")]
            )
            for key in keys:
                task = await self.ensure_thread(key)
                queue = await self.codex.queue_list(task["thread_id"], limit=1)
                if queue.get("data"):
                    await self.codex.queue_start(task["thread_id"])
                goal = (
                    await self.rpc.request("thread/goal/get", {"threadId": task["thread_id"]})
                ).get("goal")
                if goal and goal.get("status") == "paused":
                    await self.rpc.request(
                        "thread/goal/set", {"threadId": task["thread_id"], "status": "active"}
                    )
                task["paused"] = False
                task.pop("reconcile_error", None)
                self.save()
            if target is None:
                self.store.set_autonomy_paused(False)
            return {"resumed": target or "autonomy"}
        if action == "thread":
            return await self.ensure_thread(params.get("target", "main"))
        if action == "ask":
            return await self.submit(
                params.get("target", "main"),
                params["text"],
                intent_id=params.get("request_id"),
                automatic=params.get("automatic", False),
            )
        if action == "task_status":
            task = self.state["tasks"][params.get("target", "main")]
            return await self.codex.thread_read(task["thread_id"], include_turns=True)
        if action == "intents":
            return {"intents": list(self.state["intents"].values())}
        if action == "cron_list":
            return {"jobs": [asdict(job) for job in self.store.list_jobs()]}
        if action == "cron_create":
            params.setdefault("timezone", self.config.timezone)
            return asdict(self.store.create_job(**params))
        if action == "cron_update":
            return asdict(self.store.update_job(params["job_id"], **params["changes"]))
        if action == "cron_delete":
            return {"deleted": self.store.delete_job(params["job_id"])}
        if action == "events":
            return {"events": [asdict(event) for event in self.store.list_events(**params)]}
        if action == "memory_search":
            return {"sources": self.memory.search(**params), "index": self.memory.index_status()}
        if action == "memory_read":
            return self.memory.read_source(**params)
        if action == "memory_prepare":
            return self.memory.prepare_summary(**params)
        if action == "memory_commit":
            return self.memory.commit_summary(**params)
        if action == "resources_status":
            return self.resources.status()
        if action == "resources_refresh":
            return await self.refresh_resources()
        if action == "resources_observation":
            return self.resources.record_observation(**params)
        if action == "resources_money":
            return self.resources.record_money(**params)
        raise ValueError(f"Unknown Alice operation: {action}")

    async def _control_client(self, reader, writer) -> None:
        handler = asyncio.current_task()
        self._handlers.add(handler)
        try:
            try:
                raw = await asyncio.wait_for(reader.readline(), 10)
                if not raw or len(raw) >= MAX_MESSAGE:
                    raise ValueError("Invalid control request length")
                message = json.loads(raw)
                if not isinstance(message, dict) or not isinstance(message.get("params", {}), dict):
                    raise ValueError("Invalid control request")
                result = await self.handle(message["action"], message.get("params", {}))
                payload = {"ok": True, "result": result}
            except Exception as error:
                payload = {"ok": False, "error": f"{type(error).__name__}: {error}"}
            encoded = json.dumps(payload, ensure_ascii=False).encode() + b"\n"
            if len(encoded) > MAX_MESSAGE:
                encoded = b'{"ok":false,"error":"Response exceeds local limit; narrow the query"}\n'
            writer.write(encoded)
            await writer.drain()
        finally:
            self._handlers.discard(handler)
            writer.close()
            with suppress(ConnectionError):
                await writer.wait_closed()


async def serve(config: RuntimeConfig) -> None:
    service = Service(config)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, service.stop_event.set)
    await service.run()
