"""One owned Codex App Server and a small, durable scheduling/control adapter.

No model calls, prompt history reconstruction, or context compression live here.
Codex remains the executor and authoritative store for conversation state.
"""

import asyncio
from contextlib import suppress
from copy import copy
from dataclasses import asdict
import hashlib
import json
import os
import signal
import subprocess
import sys
import time
import uuid

from .codex import CodexClient
from .calendar import (
    prepare_dispatch,
    dispatch_completion,
    summary_level,
    summary_dependencies,
    UnclosedSummaryDependency,
)
from .config import RuntimeConfig
from .control import MAX_MESSAGE
from .files import SingletonLock, read_json, write_json
from .memory import MemoryStore
from .journal import NativeJournal
from .heartbeat import (
    CollectionSpec,
    HostHeartbeatAdapter,
    compare_receipts,
    parse_heartbeat_sources,
)
from .identity import (
    IDENTITY_HOOK_COMPAT_VERSION,
    build_identity_bundle,
    validate_identity_hooks,
    validate_identity_runtime_manifest,
)
from .resources import ResourceLedger
from .rpc import RpcClient, RpcError
from .scheduler import Scheduler, RejectedDispatch, DeferredDispatch
from .store import DispatchReceipt, Store


# Release guards must inspect the installed candidate, not infer compatibility
# from runtime.version or ResourceLedger's schema alone.
RESOURCE_EPOCH_CAPABILITY = 1
RESOURCE_SHUTDOWN_DRAIN_TIMEOUT = 1.0
NATIVE_SHUTDOWN_TERM_TIMEOUT = 10.0


def validate_resource_epoch_journal(state: dict) -> None:
    """Validate only the additive journal and its current-server reference.

    No I/O or mutation; unrelated runtime fields and unknown additive keys are
    left to their owners. A valid prepared record is accepted structurally;
    Service startup separately refuses another spawn until it is reconciled.
    """
    if not isinstance(state, dict):
        raise ValueError("Resource epoch validation requires a runtime object")
    epochs = state.get("resource_epochs", {})
    if not isinstance(epochs, dict):
        raise ValueError("Invalid resource epoch journal; preserved for reconciliation")

    def valid_server(server):
        return (
            isinstance(server, dict)
            and type(server.get("pid")) is int
            and server["pid"] > 0
            and all(
                isinstance(server.get(key), str) and server[key].strip()
                for key in ("birth", "identity")
            )
        )

    for epoch, record in epochs.items():
        if (
            not isinstance(epoch, str)
            or not epoch.strip()
            or len(epoch) > 2000
            or not isinstance(record, dict)
            or not isinstance(record.get("state"), str)
            or record["state"] not in {"prepared", "bound", "aborted"}
            or "server" not in record
        ):
            raise ValueError("Invalid resource epoch record; preserved for reconciliation")
        if record["state"] == "bound":
            if not valid_server(record["server"]):
                raise ValueError("Invalid resource epoch binding; preserved for reconciliation")
        elif record["server"] is not None:
            raise ValueError("Unbound resource epoch has a server identity")
    current = state.get("server")
    if current is not None and not isinstance(current, dict):
        raise ValueError("Invalid active server in resource epoch journal")
    if isinstance(current, dict) and "resource_epoch_id" in current:
        epoch_id = current["resource_epoch_id"]
        record = epochs.get(epoch_id) if isinstance(epoch_id, str) else None
        if (
            not record
            or record["state"] != "bound"
            or not valid_server(current)
            or any(record["server"][key] != current[key] for key in ("pid", "birth", "identity"))
        ):
            raise ValueError("Active resource epoch binding conflicts with server identity")


def process_identity(pid: int) -> str | None:
    result = subprocess.run(
        ["ps", "-ww", "-p", str(pid), "-o", "lstart=", "-o", "command="],
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


async def _stop_native_root(
    client: CodexClient, config: RuntimeConfig, thread_id: str, *, timeout: float | None = None
) -> dict:
    options = {} if timeout is None else {"timeout": timeout}
    try:
        return await client.stop_tree(thread_id, **options)
    except RpcError as error:
        if error.code != -32600 or str(error) not in {
            f"thread not loaded: {thread_id}",
            f"thread not found: {thread_id}",
        }:
            raise
        await client.thread_resume(
            thread_id,
            cwd=str(config.workspace),
            model=config.model,
            **config.native_permission_params(),
        )
        return await client.stop_tree(thread_id, **options)


async def recover_owned_server(config: RuntimeConfig, state: dict) -> None:
    """Stop a recorded orphan without constructing Service or opening business stores.

    The caller must hold service.lock and ensure the previous daemon has exited.
    Only the server identity and valid task root IDs are read; no runtime schema
    migration, root replacement, pause change or Alice state write occurs. Native stop
    failures fall back to termination of the verified owned group; returning does
    not prove that detached processes or external business effects were undone.
    """
    previous = state.get("server")
    if previous is None:
        return
    if not isinstance(previous, dict) or type(previous.get("pid")) is not int:
        raise RuntimeError("Invalid recorded server identity; refusing recovery")
    pid = previous["pid"]
    if pid <= 0:
        raise RuntimeError("Invalid recorded server identity; refusing recovery")
    current = process_identity(pid)
    if not current:
        return
    born = process_birth(pid)
    if born is None:
        return  # The owned process can exit between observations.
    same_identity = (
        born == previous["birth"] if previous.get("birth") else current == previous.get("identity")
    )
    try:
        pgid = os.getpgid(pid)
    except ProcessLookupError:
        return
    if not same_identity or pgid != pid or str(config.codex_socket) not in current:
        raise RuntimeError("Recorded server identity changed; refusing to signal another process")

    tasks = state.get("tasks")
    roots = list(
        dict.fromkeys(
            task["thread_id"]
            for task in (tasks.values() if isinstance(tasks, dict) else [])
            if isinstance(task, dict)
            and isinstance(task.get("thread_id"), str)
            and task["thread_id"]
        )
    )
    # An orphan can own native terminals outside its original process group.
    # Try every known root, even if an earlier empty alias has no saved rollout.
    recovered_rpc = None
    try:
        recovered_rpc = await RpcClient.connect_unix(
            config.codex_socket, timeout=2, request_timeout=5
        )
        await recovered_rpc.initialize(name="alice_recovery")
        recovered = CodexClient(recovered_rpc, owned_root_ids=roots)
        try:
            for thread_id in roots:
                try:
                    await _stop_native_root(recovered, config, thread_id, timeout=5)
                except Exception:
                    # No failure authorizes a new root or another input attempt.
                    continue
        finally:
            recovered.close()
    except Exception:
        pass  # Transport failure still requires verified owned-group cleanup.
    finally:
        if recovered_rpc:
            with suppress(Exception):
                await recovered_rpc.close()

    def same_group_alive() -> bool:
        observed_birth = process_birth(pid)
        if observed_birth is None:
            return False
        if observed_birth != born:
            raise RuntimeError("Owned server identity changed during recovery")
        try:
            if os.getpgid(pid) != pid:
                raise RuntimeError("Owned server process group changed during recovery")
        except ProcessLookupError:
            return False
        return True

    for termination in (signal.SIGTERM, signal.SIGKILL):
        if not same_group_alive():
            return
        try:
            os.killpg(pid, termination)
        except ProcessLookupError:
            return
        for _ in range(100):
            if process_birth(pid) != born:
                return
            await asyncio.sleep(0.05)
    raise RuntimeError("Owned orphan server did not exit")


class _ResourceEpochListener:
    """One connection's bounded observations, with immutable source attribution."""

    MAX_PENDING = 256
    MAX_BYTES = 1024 * 1024

    def __init__(self, service, rpc, codex, epoch_id, durable_roots):
        self.codex, self.epoch_id = codex, epoch_id
        self._roots = set(durable_roots)
        self._pending = []
        self._bytes = 0
        self._service = service

        # These closures capture this process's epoch/client, never the next
        # values of Service.rpc, Service.codex or runtime.json's active server.
        self._record = lambda params: service.resources.record_token_usage(
            params, epoch_id=epoch_id
        )
        # Read-only view: share the captured client's native ancestry, but only
        # trust roots whose host aliases were durably saved. Call owns() only;
        # this view creates no listener and never performs control or close().
        ownership = copy(codex)
        ownership.owned_root_ids = self._roots
        self._owned = ownership.owns
        self._current = lambda: service.rpc is rpc and service.codex is codex

    @property
    def pending_count(self):
        return len(self._pending)

    def confirm_roots(self, roots):
        """Called only after the host has successfully saved these root aliases."""
        self._roots.update(roots)
        self.flush()

    def flush(self):
        remaining = []
        for index, (params, size) in enumerate(self._pending):
            try:
                if self._owned(params.get("threadId")):
                    self._record(params)
                    self._bytes -= size
                else:
                    remaining.append((params, size))
            except Exception:
                # Retain this notification and the unprocessed suffix for a
                # possible shutdown delivery; never discard a failed write.
                self._pending = remaining + self._pending[index:]
                self._service._fail("Native resource observations could not be persisted")
                return
        self._pending = remaining

    def receive(self, event):
        try:
            method = event.get("method")
            if method == "thread/tokenUsage/updated":
                payload = json.dumps(
                    event.get("params", {}), allow_nan=False, separators=(",", ":")
                )
                size = len(payload.encode())
                if len(self._pending) >= self.MAX_PENDING or self._bytes + size > self.MAX_BYTES:
                    raise RuntimeError("Unresolved token observation buffer is full")
                self._pending.append((json.loads(payload), size))
                self._bytes += size
                self.flush()
            elif method == "thread/started":
                # CodexClient's listener runs first and remembers native ancestry.
                self.flush()
            elif self._current():
                # Stale transport callbacks must not update a newer task lifecycle.
                self._service._on_notification(event)
        except Exception:
            # RpcClient isolates listener exceptions; explicitly stop dispatch.
            self._service._fail("Native resource observation buffer could not be preserved")


class Service:
    def __init__(self, config: RuntimeConfig):
        # Validate the complete source configuration before opening business
        # stores or starting native work, including direct embedded callers.
        heartbeat_sources = parse_heartbeat_sources(config.heartbeat_sources)
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
        self._validate_resource_epochs()
        self.store = Store(config.database)
        self.memory = MemoryStore(config.root)
        self.resources = ResourceLedger(config.root / "state/resources.sqlite3")
        self._resource_refresh_at = 0.0
        self._heartbeat = HostHeartbeatAdapter()
        self._heartbeat_intervals: dict[str, float] = {}
        self._heartbeat_scopes: dict[str, str] = {}
        self._heartbeat_next: dict[str, float] = {}
        self._heartbeat_collection_rejected: dict[str, bool] = {}
        for source in heartbeat_sources:
            self.register_heartbeat_source(
                source.target, source.spec, wait_seconds=source.wait_seconds
            )
        consumed = self.state.get("heartbeat_consumed", {})
        if not isinstance(consumed, dict):
            raise ValueError("Damaged heartbeat consumption state; preserved for recovery")
        for target, receipt in consumed.items():
            compare_receipts(None, receipt)
            if receipt["target"] != target or receipt["state"] != "known":
                raise ValueError("Heartbeat consumption requires a matching known host receipt")
        self.stop_event = asyncio.Event()
        self.rpc: RpcClient | None = None
        self.codex: CodexClient | None = None
        self.journal: NativeJournal | None = None
        self.process: asyncio.subprocess.Process | None = None
        self._bound_resource_epoch: str | None = None
        self._resource_observer: _ResourceEpochListener | None = None
        self._resource_durable_roots = {task["thread_id"] for task in self.state["tasks"].values()}
        self._starting_resource_epoch: str | None = None
        self._spawn_attempted = False
        self.ready = False
        self.stopping = False
        self.error: str | None = None
        self._task_locks: dict[str, asyncio.Lock] = {}
        self._notifications: asyncio.Queue = asyncio.Queue(maxsize=2048)
        self._background: list[asyncio.Task] = []
        self._handlers: set[asyncio.Task] = set()
        self._native_interrupts: dict[str, str] = {}
        self._resuming: set[str] = set()
        self._pause_revision = 0  # In-flight resume fence; no resume survives process exit.
        self._task_pause_revisions: dict[str, int] = {}
        self._native_turn_revisions: dict[str, int] = {}
        self._policy_wakeup = asyncio.Event()
        self.scheduler = Scheduler(self.store, self)

    def save(self) -> None:
        write_json(self.path, self.state)

    def _validate_resource_epochs(self):
        validate_resource_epoch_journal(self.state)

    def _prepare_resource_epoch(self):
        epochs = self.state.get("resource_epochs", {})
        if any(record["state"] == "prepared" for record in epochs.values()):
            raise RuntimeError(
                "Unbound resource epoch requires reconciliation before another spawn"
            )
        epoch = str(uuid.uuid4())
        previous = {key: self.state.get(key) for key in ("server", "lifecycle", "resource_epochs")}
        self.state["resource_epochs"] = {**epochs, epoch: {"state": "prepared", "server": None}}
        self.state["lifecycle"], self.state["server"] = "starting", None
        try:
            self.save()
        except BaseException:
            for key, value in previous.items():
                if key == "resource_epochs" and value is None:
                    self.state.pop(key, None)
                else:
                    self.state[key] = value
            raise
        self._starting_resource_epoch = epoch
        self._bound_resource_epoch = None
        self._spawn_attempted = False
        return epoch

    def _bind_resource_epoch(self, epoch_id):
        if self.state.get("resource_epochs", {}).get(epoch_id) != {
            "state": "prepared",
            "server": None,
        }:
            raise RuntimeError("Resource epoch was not prepared for this spawn")
        if self.process is None or self.process.returncode is not None:
            raise RuntimeError("Cannot bind a resource epoch without a live owned child")
        server = {
            "pid": self.process.pid,
            "identity": process_identity(self.process.pid),
            "birth": process_birth(self.process.pid),
        }
        if (
            not server["identity"]
            or not server["birth"]
            or os.getpgid(self.process.pid) != self.process.pid
        ):
            raise RuntimeError("Cannot verify the resource epoch's owned process identity")
        self.state["server"] = {**server, "resource_epoch_id": epoch_id}
        self.state["resource_epochs"][epoch_id] = {"state": "bound", "server": server}
        try:
            self.save()
        except BaseException:
            # Keep the known child identity for cleanup, without claiming an
            # acknowledged epoch binding on a subsequent shutdown save.
            self.state["server"] = server
            self.state["resource_epochs"][epoch_id] = {"state": "prepared", "server": None}
            raise
        self._bound_resource_epoch = epoch_id
        self._resource_durable_roots.update(
            task["thread_id"] for task in self.state["tasks"].values()
        )

    def _attach_resource_listener(self, rpc, codex, epoch_id):
        active = self.state.get("server") or {}
        record = self.state.get("resource_epochs", {}).get(epoch_id, {})
        if (
            not epoch_id
            or epoch_id != self._bound_resource_epoch
            or active.get("resource_epoch_id") != epoch_id
            or record.get("state") != "bound"
            or record.get("server")
            != {key: active.get(key) for key in ("pid", "birth", "identity")}
            or self.process is None
            or self.process.returncode is not None
            or self.process.pid != active.get("pid")
        ):
            raise RuntimeError("Resource listener requires a durably bound process epoch")
        observer = _ResourceEpochListener(self, rpc, codex, epoch_id, self._resource_durable_roots)
        # No await between subscribing and replaying the connection's retained
        # prefix. A missing prefix cannot be silently treated as no observations.
        rpc.add_listener(observer.receive)
        self._resource_observer = observer
        retained = list(rpc.events)
        if rpc.event_sequence and (not retained or retained[0].sequence != 1):
            self._fail("Native resource notifications have an unobserved connection prefix")
            raise RuntimeError("Resource notification prefix is unavailable")
        for event in retained:
            codex._on_event(event.message)  # Replay native ancestry through its existing owner.
            observer.receive(event.message)
        return observer

    def _confirm_resource_roots(self):
        self._resource_durable_roots.update(
            task["thread_id"] for task in self.state["tasks"].values()
        )
        observer = self._resource_observer
        if observer is not None and observer.codex is self.codex:
            observer.confirm_roots(self._resource_durable_roots)

    def _lock(self, key: str) -> asyncio.Lock:
        return self._task_locks.setdefault(key, asyncio.Lock())

    async def _recover_orphan(self) -> None:
        await recover_owned_server(self.config, self.state)

    async def run(self) -> None:
        self.config.prepare_directories()
        self.config.verify_binary()
        with SingletonLock(self.config.root / "state/service.lock"):
            if self.state["lifecycle"] in {"new", "running", "starting", "stopping"}:
                self.store.set_autonomy_paused(True)
            await self._recover_orphan()
            self._recover_task_policy_usage()
            identity_manifest = self.config.root / "state/identity-runtime.json"
            if identity_manifest.exists():
                prior_identity = read_json(identity_manifest)
                validate_identity_runtime_manifest(prior_identity)
            identity = build_identity_bundle(self.config.workspace)
            self.config.write_identity_config(identity)
            epoch_id = self._prepare_resource_epoch()
            self.config.codex_socket.unlink(missing_ok=True)
            self.config.control_socket.unlink(missing_ok=True)
            log = open(self.config.root / "logs/codex-server.log", "ab", buffering=0)
            try:
                self._spawn_attempted = True
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
                self._bind_resource_epoch(epoch_id)
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
                self.codex = CodexClient(
                    self.rpc,
                    owned_root_ids=list(self._resource_durable_roots),
                )
                self._attach_resource_listener(self.rpc, self.codex, epoch_id)
                self.journal = NativeJournal(self.memory, self.rpc, self.codex.owns)
                await self.rpc.initialize()
                if self.stopping:
                    raise RuntimeError(self.error or "Native resource startup failed")
                listing = await self.rpc.request(
                    "hooks/list", {"cwds": [str(self.config.workspace)]}
                )
                self.config.trust_identity_hooks(listing)
                trusted_hashes = validate_identity_hooks(
                    await self.rpc.request("hooks/list", {"cwds": [str(self.config.workspace)]}),
                    self.config.codex_home / "config.toml",
                    self.config.workspace,
                    self.config.identity_hooks(),
                    require_trusted=True,
                )
                write_json(
                    identity_manifest,
                    {
                        "version": 1,
                        "hook_compat_version": IDENTITY_HOOK_COMPAT_VERSION,
                        "python": sys.executable,
                        "workspace": str(self.config.workspace),
                        "config_path": str(self.config.codex_home / "config.toml"),
                        "bundle": identity.metadata(),
                        "hook_state": trusted_hashes,
                    },
                )
                self.state["identity"] = {
                    **identity.metadata(),
                    "canonical": "configured",
                    "hooks": "trusted_before_thread_load",
                }
                self.save()
                await self.archive_native_history("startup")
                if self.stopping:
                    raise RuntimeError(self.error or "Native resource startup failed")
                server = await asyncio.start_unix_server(
                    self._control_client, str(self.config.control_socket), limit=MAX_MESSAGE
                )
                self.config.control_socket.chmod(0o600)
                async with server:
                    self._background = [
                        asyncio.create_task(self._tick()),
                        asyncio.create_task(self._record_events()),
                        asyncio.create_task(self._watch_task_policies()),
                        asyncio.create_task(self._watch_heartbeats()),
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
        # Shutdown must retain this epoch's transport and ancestry listener,
        # even if a later connection replaces the Service's current fields.
        rpc, codex, process = self.rpc, self.codex, self.process
        starting_epoch = self._starting_resource_epoch
        spawn_attempted = self._spawn_attempted
        cancelled_while_reaping = False
        try:
            # Shutdown never means that externally published work was undone.
            try:
                self.store.set_autonomy_paused(True)
                self.state["lifecycle"] = "stopping"
                self.save()
            except Exception as error:
                self.error = f"Pause persistence failed: {type(error).__name__}"
            if codex:
                for key in list(self.state["tasks"]):
                    try:
                        await self._stop_task(key, codex=codex, timeout=8)
                    except Exception as error:
                        self.error = (
                            f"Shutdown required owned-server termination: {type(error).__name__}"
                        )
                if self.journal and self.rpc is rpc and self.codex is codex:
                    await self.archive_native_history("shutdown")
        finally:
            try:
                try:
                    if process and process.returncode is None:

                        async def terminate_owned_child():
                            with suppress(ProcessLookupError):
                                os.killpg(process.pid, signal.SIGTERM)
                            try:
                                await asyncio.wait_for(process.wait(), NATIVE_SHUTDOWN_TERM_TIMEOUT)
                            except TimeoutError:
                                with suppress(ProcessLookupError):
                                    os.killpg(process.pid, signal.SIGKILL)
                                await process.wait()

                        termination = asyncio.create_task(terminate_owned_child())
                        while True:
                            try:
                                await asyncio.shield(termination)
                                break
                            except asyncio.CancelledError:
                                if termination.cancelled():
                                    raise
                                # Cancellation cannot orphan the child by
                                # cancelling its only wait. Escalate this owned
                                # process, finish reaping, then propagate below.
                                cancelled_while_reaping = True
                                if process.returncode is None:
                                    with suppress(ProcessLookupError):
                                        os.killpg(process.pid, signal.SIGKILL)
                    # The OS child is already reaped. Buffered frames can still
                    # be waiting for the RPC reader; leave both listeners alive
                    # until that reader finishes or this bounded wait expires.
                    if rpc:
                        try:
                            drained = await rpc.wait_reader_closed(
                                timeout=RESOURCE_SHUTDOWN_DRAIN_TIMEOUT
                            )
                            reason = None if drained else "Native notification tail drain timed out"
                        except Exception as error:
                            reason = (
                                f"Native notification tail drain failed: {type(error).__name__}"
                            )
                        if reason:
                            self.error = f"{self.error}; {reason}" if self.error else reason
                            # A fixed, content-free diagnostic remains in the
                            # private server log after this Service exits.
                            with suppress(OSError):
                                log.write(f"Alice resource shutdown: {reason}\n".encode())
                finally:
                    try:
                        if codex:
                            codex.close()
                    finally:
                        if rpc:
                            with suppress(Exception):
                                await rpc.close()
                self.state["lifecycle"] = "stopped"
                self.state["server"] = None
                epoch = self.state.get("resource_epochs", {}).get(starting_epoch)
                if (
                    epoch
                    and epoch["state"] == "prepared"
                    and (
                        not spawn_attempted
                        or (process is not None and process.returncode is not None)
                    )
                ):
                    epoch["state"] = "aborted"
                self.save()
                if cancelled_while_reaping:
                    raise asyncio.CancelledError
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
        if event.get("method") == "thread/tokenUsage/updated":
            self._fail("Native resource notification has no bound epoch listener")
            return
        if event.get("method") not in {
            "turn/started",
            "turn/completed",
            "item/completed",
            "thread/tokenUsage/updated",
            "account/rateLimits/updated",
        }:
            return
        params = event.get("params", {})
        if event["method"] == "turn/started" or (
            event["method"] == "item/completed"
            and params.get("item", {}).get("type") == "userMessage"
        ):
            try:
                changed = False
                for task in self.state["tasks"].values():
                    if task["thread_id"] == params.get("threadId"):
                        if event["method"] == "turn/started":
                            thread_id = task["thread_id"]
                            self._native_turn_revisions[thread_id] = (
                                self._native_turn_revisions.get(thread_id, 0) + 1
                            )
                        if not task.get("has_input"):
                            task["has_input"] = True
                            changed = True
                        if event["method"] == "turn/started" and task.get(
                            "policy_deadline_stopped"
                        ):
                            # Another native client can start after a prior stop.
                            # Re-arm the obligation; never treat the old stop as
                            # proof that this new execution has also ended.
                            task.pop("policy_deadline_stopped")
                            self._policy_wakeup.set()
                            changed = True
                if changed:
                    self.save()  # Direct TUI input must survive before archive-worker scheduling.
            except Exception:
                self._fail("Native input ownership could not be persisted")
                return
        if event["method"] == "account/rateLimits/updated":
            try:
                # Keep receiving accounting through native interruption, even
                # after the asynchronous transcript worker has been cancelled.
                snapshot = params.get("rateLimits")
                limit_id = snapshot.get("limitId") if isinstance(snapshot, dict) else None
                observed = {"rateLimits": snapshot}
                if limit_id:
                    observed["rateLimitsByLimitId"] = {limit_id: snapshot}
                self.resources.record_rate_limits(observed)
                self._mark_resource_pauses()
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
            if task["thread_id"] == thread and (not task.get("paused") or key in self._resuming):
                task["paused"] = True
                if key == "main":
                    self._pause_revision += 1
                    self.store.set_autonomy_paused(True)
                else:
                    self._task_pause_revisions[key] = self._task_pause_revisions.get(key, 0) + 1
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
        if self._resource_observer is not None and self._resource_observer.codex is self.codex:
            self._resource_observer.flush()

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
            self._finish_policy_intent(
                intent, "unknown" if turn["status"] == "completed" else "failed"
            )
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
                await self._stop_resource_paused_tasks()
                await self.reconcile()
                await self.scheduler.poll()
                await asyncio.sleep(self.config.poll_seconds)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._fail(f"Dispatch stopped: {type(error).__name__}: {error}")

    @staticmethod
    def _policy_target(target: str) -> None:
        if (
            not isinstance(target, str)
            or not target
            or len(target) > 150
            or target == "new"
            or target.startswith(("summary:", "scheduled:"))
        ):
            raise ValueError(
                "Task policy requires a stable named target, not recurring summary roots"
            )

    def _default_task_policy(self, target: str, *, summary_plan: str | None = None) -> None:
        # A configured default only applies to explicit named work. Main and
        # recurring summaries need their own explicit operator decision.
        policy = self.config.task_policy
        if (
            policy is None
            or target in {"main", "new"}
            or target.startswith(("summary:", "scheduled:"))
            or summary_plan is not None
            or self.store.get_task_policy(target) is not None
        ):
            return
        fingerprint = hashlib.sha256(
            json.dumps(policy, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        self.store.set_task_policy(
            target, policy, f"config-policy:{target}:{fingerprint}", now=time.time()
        )

    def _policy_intent_busy(self, target: str) -> bool:
        root = self.state["tasks"].get(target, {}).get("thread_id")
        return any(
            (item.get("target") == target or (root and item.get("thread_id") == root))
            and item["status"] in {"sending", "accepted", "queued", "unknown"}
            for item in self.state["intents"].values()
        )

    async def set_task_policy(self, target: str, policy: dict, request_id: str) -> dict:
        self._policy_target(target)
        async with self._lock("admission"):
            if request_id in self.state["intents"]:
                raise ValueError("Request ID already identifies task input")
            # Do not pretend to know when pre-existing unbudgeted work began.
            if self.store.get_task_policy(target) is None:
                if self._policy_intent_busy(target):
                    raise RejectedDispatch(
                        "Reconcile existing work before assigning its first policy"
                    )
                task = self.state["tasks"].get(target)
                if task:
                    native = (await self.codex.thread_read(task["thread_id"]))["thread"]
                    if native.get("status", {}).get("type") == "active":
                        raise RejectedDispatch(
                            "Stop existing native work before assigning its first policy"
                        )
            receipt = self.store.set_task_policy(target, policy, request_id, now=time.time())
            self._policy_wakeup.set()
            return receipt

    async def task_policy_status(self, target: str) -> dict:
        self._policy_target(target)
        busy = self._policy_intent_busy(target)
        task = self.state["tasks"].get(target)
        if task and not busy:
            try:
                native = (await self.codex.thread_read(task["thread_id"]))["thread"]
                busy = native.get("status", {}).get("type") == "active"
            except RpcError as error:
                if error.code != -32600 or str(error) != f"thread not loaded: {task['thread_id']}":
                    raise
        report = self.store.task_policy_status(target, now=time.time(), busy=busy)
        return {
            **(report or {"target": target, "policy": None, "usage": None, "decision": None}),
            "enforcement_scope": "alice_admission",
        }

    def _finish_policy_intent(self, intent: dict, outcome: str) -> None:
        if not intent.get("policy_charged"):
            return
        record = self.store.get_task_policy(intent["target"])
        if record is None or intent["id"] not in record["attempts"]:
            raise RuntimeError("Task policy receipt missing; consumption cannot be reconstructed")
        attempt = record["attempts"][intent["id"]]
        if attempt["outcome"] not in {"running", "unknown"}:
            return  # Late native notifications cannot overwrite independently confirmed facts.
        if record["current_attempt_id"] != intent["id"]:
            raise RuntimeError("Unresolved old policy attempt requires reconciliation")
        self.store.finish_task_attempt(intent["target"], intent["id"], outcome, now=time.time())
        self._policy_wakeup.set()

    def _restore_policy_intents(self) -> None:
        """Reconcile the SQLite-before-runtime crash boundary without resending input."""
        changed = False
        for record in self.store.list_task_policies():
            intent_id = record["current_attempt_id"]
            if not intent_id:
                continue
            attempt = record["attempts"][intent_id]
            if attempt["outcome"] not in {"running", "unknown"}:
                continue
            task = self.state["tasks"].get(record["target"])
            if (
                not task
                or not attempt.get("thread_id")
                or task["thread_id"] != attempt["thread_id"]
            ):
                raise RuntimeError(
                    "Charged policy attempt has no matching owned root; reconcile manually"
                )
            intent = self.state["intents"].get(intent_id)
            if intent is not None:
                if (
                    intent.get("thread_id") != attempt["thread_id"]
                    or intent.get("target") != record["target"]
                    or intent.get("input_sha256") != attempt["input_sha256"]
                    or intent.get("policy_charged") is not True
                ):
                    raise RuntimeError("Runtime intent conflicts with its charged policy receipt")
                continue
            self.store.finish_task_attempt(record["target"], intent_id, "unknown", now=time.time())
            self.state["intents"][intent_id] = {
                "id": intent_id,
                "target": record["target"],
                "thread_id": attempt["thread_id"],
                "input_sha256": attempt["input_sha256"],
                "created_at": attempt["admitted_at"],
                "status": "unknown",
                "event_id": None,
                "policy_charged": True,
                "recovered_policy_receipt": True,
            }
            changed = True
        if changed:
            self.save()

    def _recover_task_policy_usage(self) -> None:
        # This runs once after old-server cleanup, never on the running watcher.
        # A persisted acceptance alone cannot prove what survived process death.
        self._restore_policy_intents()
        for record in self.store.list_task_policies():
            intent_id = record["current_attempt_id"]
            if not intent_id or record["usage"]["last_outcome"] not in {"running", "unknown"}:
                continue
            self.store.finish_task_attempt(record["target"], intent_id, "unknown", now=time.time())
            intent = self.state["intents"].get(intent_id)
            if intent is not None:
                intent["status"] = "unknown"
        self.save()

    def register_heartbeat_source(
        self, target: str, spec: CollectionSpec, *, wait_seconds: float
    ) -> None:
        """Internal host registration; no control/MCP operation accepts sources.

        The interval is explicit and independent of a task's lifetime budget.
        Construction restores explicit local configuration once per process;
        persisted receipts alone never authorize contacting an old source.
        """
        import math

        self._policy_target(target)
        if (
            type(wait_seconds) not in (int, float)
            or not math.isfinite(wait_seconds)
            or wait_seconds <= 0
        ):
            raise ValueError("Heartbeat wait must be finite and positive")
        self._heartbeat.register(target, spec)
        self._heartbeat_intervals[target] = wait_seconds
        self._heartbeat_scopes[target] = spec.scope_sha256
        self._heartbeat_next.pop(target, None)

    async def observe_heartbeat(self, target: str) -> dict:
        """Collect only from host-registered sources and durably compare them."""
        async with self._lock("heartbeat:" + target):
            now = time.time()
            interval = self._heartbeat_intervals.get(target, self.config.poll_seconds)
            previous = self.store.get_heartbeat_state(target)
            # On restart an old wait cannot masquerade as a configured source.
            configured = target in self._heartbeat_intervals
            if (
                previous is not None
                and self._heartbeat_next.get(target, 0) > time.monotonic()
                and (configured or previous["latest"]["state"] == "unconfigured")
            ):
                return {
                    **previous,
                    "configured": configured,
                    "collection_rejected": self._heartbeat_collection_rejected.get(target, False),
                }
            receipt = await asyncio.to_thread(self._heartbeat.observe, target, now=now)
            state = self.store.record_heartbeat(target, receipt, wait_seconds=interval)
            scope_changed = receipt["scope_sha256"] != self._heartbeat_scopes.get(target)
            rejected = state["latest"]["id"] != receipt["id"] or scope_changed
            self._heartbeat_collection_rejected[target] = rejected
            self._heartbeat_next[target] = time.monotonic() + (
                0 if scope_changed else self._heartbeat_intervals.get(target, interval)
            )
            return {**state, "configured": configured, "collection_rejected": rejected}

    async def _watch_heartbeats(self) -> None:
        try:
            while True:
                # Recheck evidence even while policy/native ambiguity blocks
                # dispatch. New evidence never clears those independent blocks.
                for target in list(self._heartbeat_intervals):
                    await self.observe_heartbeat(target)
                await asyncio.sleep(self.config.poll_seconds)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._fail(f"Heartbeat evidence stopped: {type(error).__name__}: {error}")

    def _heartbeat_candidate(self, target: str, receipt: dict) -> bool:
        consumed = self.state.get("heartbeat_consumed", {}).get(target)
        return consumed is None or any(
            consumed[key] != receipt[key]
            for key in ("source_id", "scope_sha256", "validator_version", "content_sha256")
        )

    async def _enforce_task_deadlines(self) -> float:
        """Check the same persisted policy independently of scheduler/RPC latency."""
        self._restore_policy_intents()
        delay = self.config.poll_seconds
        async with self._lock("policy-stops"):
            for record in self.store.list_task_policies():
                usage = record["usage"]
                task = self.state["tasks"].get(record["target"])
                if usage is None or task is None:
                    continue
                # Running/unknown work may still own native execution after a
                # lost acknowledgement; treat it as reserved until reconciled.
                busy = usage["last_outcome"] in {"running", "unknown"} or self._policy_intent_busy(
                    record["target"]
                )
                report = self.store.task_policy_status(record["target"], now=time.time(), busy=busy)
                decision = report["decision"]
                if decision["state"] != "complete":
                    seconds = decision["remaining"]["seconds"]
                    if seconds > 0:
                        delay = min(delay, seconds)
                if "task_time_exhausted" in decision["reasons"] and not task.get(
                    "policy_deadline_stopped"
                ):
                    task.update(paused=True, policy_pause_pending=True)
                    self._task_pause_revisions[record["target"]] = (
                        self._task_pause_revisions.get(record["target"], 0) + 1
                    )
                    self.save()  # Preserve the stop obligation before waiting on native state.
                if task.get("policy_pause_pending"):
                    thread_id = task["thread_id"]
                    stopped_revision = self._native_turn_revisions.get(thread_id, 0)
                    await asyncio.wait_for(self.pause(record["target"]), timeout=5)
                    if self._native_turn_revisions.get(thread_id, 0) != stopped_revision:
                        # A received start raced with the final native stop
                        # proof. Keep the durable obligation for that execution.
                        delay = 0.01
                        self._policy_wakeup.set()
                        continue
                    task.pop("policy_pause_pending", None)
                    task["policy_deadline_stopped"] = True
                    self.save()
        return max(0.01, delay)

    async def _watch_task_policies(self) -> None:
        try:
            while True:
                self._policy_wakeup.clear()
                delay = await self._enforce_task_deadlines()
                try:
                    await asyncio.wait_for(self._policy_wakeup.wait(), delay)
                except TimeoutError:
                    pass
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._fail(f"Task policy enforcement stopped: {type(error).__name__}: {error}")

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
        self._mark_resource_pauses()
        await self._stop_resource_paused_tasks()
        return self.resources.status()

    def _mark_resource_pauses(self) -> None:
        if self.resources.can_dispatch(automatic=True)["allowed"]:
            return
        changed = False
        for key, task in self.state["tasks"].items():
            if (
                task.get("automatic") is True
                and (not task.get("paused") or key in self._resuming)
                and not task.get("resource_pause_pending")
            ):
                task.update(paused=True, resource_pause_pending=True)
                self._task_pause_revisions[key] = self._task_pause_revisions.get(key, 0) + 1
                changed = True
        if changed:
            self.save()  # Persist admission and unfinished stop before the next await.

    async def _stop_resource_paused_tasks(self) -> None:
        async with self._lock("resource-stops"):
            for key, task in list(self.state["tasks"].items()):
                if task.get("resource_pause_pending"):
                    await self.pause(key)  # Includes native Goal, input queue and descendants.
                    task.pop("resource_pause_pending", None)
                    self.save()

    def _thread_has_input(self, task: dict) -> bool:
        return bool(task.get("has_input")) or any(
            intent["thread_id"] == task["thread_id"] for intent in self.state["intents"].values()
        )

    async def _bootstrap_thread(
        self, task: dict, native_thread: dict, *, fresh: bool = False
    ) -> None:
        """Make an unused owned root resumable without manufacturing a model turn."""
        thread_id = task["thread_id"]
        bootstrap = task.get("bootstrap")
        if bootstrap is not None and (
            not isinstance(bootstrap, dict)
            or bootstrap.get("version") != 1
            or bootstrap.get("thread_id") != thread_id
            or bootstrap.get("state") not in {"pending", "sending", "unknown", "ready"}
        ):
            raise RpcError("Unsupported native bootstrap state; reconcile without replacing root")
        if bootstrap and bootstrap["state"] == "ready":
            return
        if not bootstrap and self._thread_has_input(task):
            return  # Existing real work must not acquire another initialization entry.

        async def confirm_rollout():
            await self.codex.thread_resume(
                thread_id,
                cwd=str(self.config.workspace),
                model=self.config.model,
                **self.config.native_permission_params(),
            )

        if not fresh:
            try:
                await confirm_rollout()
            except RpcError as error:
                if (
                    error.code != -32600
                    or str(error) != f"no rollout found for thread id {thread_id}"
                ):
                    raise
            else:
                task["bootstrap"] = {"version": 1, "thread_id": thread_id, "state": "ready"}
                self.save()
                return  # Raw developer entries are not necessarily exposed by items/list.

        if bootstrap and bootstrap["state"] in {"sending", "unknown"}:
            raise RpcError("Native bootstrap outcome is unknown; reconcile without replay")
        if self._thread_has_input(task) or native_thread.get("status", {}).get("type") != "idle":
            raise RpcError("Native bootstrap requires an unused idle owned thread")
        # Do not request full history here: Codex's unmaterialized paginated
        # roots cannot service turns/list or read(includeTurns=True). A new root
        # is not returned to an attach caller until bootstrap has finished;
        # legacy roots also require the exact no-rollout response above.
        current = native_thread if fresh else (await self.codex.thread_read(thread_id))["thread"]
        if (
            self._thread_has_input(task)
            or current.get("status", {}).get("type") != "idle"
            or current.get("turns")
        ):
            raise RpcError("Native input arrived during bootstrap; initialization not sent")
        if self.stopping:
            raise DeferredDispatch("Service is stopping")
        task["bootstrap"] = {"version": 1, "thread_id": thread_id, "state": "sending"}
        self.save()  # An uncertain inject must never be sent a second time automatically.
        try:
            await self.codex.record_runtime_initialization(thread_id)
            await confirm_rollout()
        except Exception:
            task["bootstrap"]["state"] = "unknown"
            self.save()
            raise
        task["bootstrap"]["state"] = "ready"  # Rollout observed, not a user-input receipt.
        self.save()

    async def ensure_thread(self, key: str) -> dict:
        if not isinstance(key, str) or not key or len(key) > 150:
            raise ValueError("Task name must contain 1–150 characters")
        async with self._lock("thread:" + key):
            identity = build_identity_bundle(self.config.workspace)
            task = self.state["tasks"].get(key)
            if task:
                try:
                    try:
                        result = await self.codex.thread_read(task["thread_id"])
                    except RpcError as error:
                        if (
                            error.code != -32600
                            or str(error) != f"thread not loaded: {task['thread_id']}"
                        ):
                            raise
                        # This is not proof of absent history. Resume the exact
                        # ID first; only an explicit no-rollout error below can
                        # authorize replacement of an untouched empty thread.
                        result = await self.codex.thread_resume(
                            task["thread_id"],
                            cwd=str(self.config.workspace),
                            model=self.config.model,
                            developerInstructions=identity.developer_instructions,
                            **self.config.native_permission_params(),
                        )
                    status = result["thread"].get("status", {}).get("type")
                    if status == "notLoaded":
                        await self.codex.thread_resume(
                            task["thread_id"],
                            cwd=str(self.config.workspace),
                            model=self.config.model,
                            developerInstructions=identity.developer_instructions,
                            **self.config.native_permission_params(),
                        )
                except RpcError as error:
                    bootstrap = task.get("bootstrap")
                    if (
                        self._thread_has_input(task)
                        or (
                            bootstrap is not None
                            and (
                                not isinstance(bootstrap, dict)
                                or bootstrap.get("version") != 1
                                or bootstrap.get("thread_id") != task["thread_id"]
                                or bootstrap.get("state") != "pending"
                            )
                        )
                        or error.code != -32600
                        or str(error) != f"no rollout found for thread id {task['thread_id']}"
                    ):
                        raise
                    # Codex intentionally doesn't persist an unused empty thread.
                    # Never use this fallback after any acknowledged/unknown input.
                    # Retain the old alias and its pause flags until replacement
                    # succeeds, including if this process dies during thread/start.
                else:
                    await self._bootstrap_thread(task, result["thread"])
                    return task
            if self.stopping:
                raise DeferredDispatch("Service is stopping")
            result = await self.codex.thread_start(
                cwd=str(self.config.workspace),
                model=self.config.model,
                approvalPolicy="never",
                **self.config.native_permission_params(),
                developerInstructions=identity.developer_instructions,
            )
            if task:
                if self._thread_has_input(task):
                    raise RpcError("Input was observed while replacing an empty thread")
                self.state.setdefault("replaced_empty_threads", []).append(task["thread_id"])
            task = {
                **(task or {}),
                "thread_id": result["thread"]["id"],
                "paused": bool(task and task.get("paused")),
                "created_at": time.time(),
                "identity": {**identity.metadata(), "canonical": "new_thread_requested"},
                "has_input": False,
                "bootstrap": {
                    "version": 1,
                    "thread_id": result["thread"]["id"],
                    "state": "pending",
                },
            }
            self.state["tasks"][key] = task
            try:
                self.save()  # Preserve the alias before admitting its early token observations.
            except Exception:
                self._fail("Native resource root ownership could not be persisted")
                raise
            self._confirm_resource_roots()
            await self._bootstrap_thread(task, result["thread"], fresh=True)
            return task

    async def is_busy(self, target: str) -> bool:
        if self.stopping or not self.ready or self.store.is_autonomy_paused():
            return True
        if not self.resources.can_dispatch(automatic=True)["allowed"]:
            return True
        task = self.state["tasks"].get(target)
        if task and task.get("paused"):
            return True
        policy = self.store.task_policy_status(
            target, now=time.time(), busy=self._policy_intent_busy(target)
        )
        if policy and not policy["decision"]["allowed"]:
            return True
        # An acknowledgement can precede native status visibility; ambiguous
        # inputs also reserve their root until reconciliation, never a new ID.
        stopped_roots = {
            item["thread_id"]
            for item in self.state["tasks"].values()
            if item.get("paused") and item.get("policy_deadline_stopped") is True
        }
        active = {
            intent["thread_id"]
            for intent in self.state["intents"].values()
            if intent["status"] in {"sending", "unknown", "accepted", "queued"}
            and intent["thread_id"] not in stopped_roots
        }
        if task and task["thread_id"] in active:
            return True
        task_snapshot = list(self.state["tasks"].items())
        task_ids = {(name, item["thread_id"]) for name, item in task_snapshot}
        for name, item in task_snapshot:
            if item.get("reconcile_error") == "native_history_missing":
                continue
            try:
                try:
                    thread = (await self.codex.thread_read(item["thread_id"]))["thread"]
                except RpcError as error:
                    if (
                        error.code != -32600
                        or str(error) != f"thread not loaded: {item['thread_id']}"
                    ):
                        raise
                    if item.get("paused"):
                        # No loaded executor exists here. An automatic capacity
                        # query must not resume a different manually paused root.
                        continue
                    previous_id = item["thread_id"]
                    item = await self.ensure_thread(name)
                    task_ids.discard((name, previous_id))
                    task_ids.add((name, item["thread_id"]))
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
                active.add(item["thread_id"])
                if name == target:
                    return True
        return len(active) >= self.config.max_active_tasks or task_ids != {
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
        heartbeat_receipt: dict | None = None,
    ) -> dict:
        if not isinstance(text, str) or not text.strip():
            raise ValueError("Task input cannot be empty")
        if type(automatic) is not bool:
            raise ValueError("automatic must be boolean")
        if not isinstance(target, str) or not target or len(target) > 150:
            raise ValueError("Task name must contain 1–150 characters")
        if intent_id is not None and (not isinstance(intent_id, str) or not intent_id):
            raise ValueError("Request ID must be a nonempty string")
        intent_id = intent_id or str(uuid.uuid4())
        fingerprint = hashlib.sha256((target + "\0" + text).encode()).hexdigest()
        # All entry points share the request namespace and capacity boundary.
        # Hold admission only through the bounded native acknowledgement, never
        # through model execution. Pause persists flags before waiting for input.
        async with self._lock("admission"), self._lock("input:" + target):
            if intent_id in self.state["intents"]:
                prior = self.state["intents"][intent_id]
                if prior.get("input_sha256") != fingerprint:
                    raise ValueError("Request ID already identifies different input")
                return prior
            if self.stopping or (automatic and await self.is_busy(target)):
                raise DeferredDispatch("Automatic dispatch is paused or busy")
            task = await self.ensure_thread(target)
            if task.get("paused"):
                raise DeferredDispatch("Task is paused; resume it explicitly")
            native = (await self.codex.thread_read(task["thread_id"]))["thread"]
            if (
                self.stopping
                or task.get("paused")
                or (
                    automatic
                    and (
                        self.store.is_autonomy_paused()
                        or not self.resources.can_dispatch(automatic=True)["allowed"]
                        or native.get("status", {}).get("type") == "active"
                    )
                )
            ):
                # No input was sent, so retain the occurrence without inventing
                # a failed durable intent that would poison its stable retry ID.
                raise DeferredDispatch("Dispatch paused or busy before input was sent")
            self._default_task_policy(target, summary_plan=summary_plan)
            if heartbeat_receipt is not None:
                latest = self.store.get_heartbeat_state(target)
                if (
                    target not in self._heartbeat_intervals
                    or self._heartbeat_collection_rejected.get(target, False)
                    or latest is None
                    or latest["latest"]["state"] != "known"
                    or latest["latest"] != heartbeat_receipt
                    or heartbeat_receipt["scope_sha256"] != self._heartbeat_scopes.get(target)
                    or not self._heartbeat_candidate(target, heartbeat_receipt)
                ):
                    raise DeferredDispatch("Heartbeat evidence was consumed or superseded")
            charge = self.store.admit_task_attempt(
                target,
                intent_id,
                fingerprint,
                now=time.time(),
                busy=native.get("status", {}).get("type") == "active"
                or self._policy_intent_busy(target),
                thread_id=task["thread_id"],
            )
            if charge and charge["replayed"]:
                raise RejectedDispatch(
                    "Task attempt was already charged; reconcile its receipt without replay"
                )
            if charge and not charge["admitted"]:
                raise DeferredDispatch(
                    "Task policy blocked dispatch: " + ", ".join(charge["decision"]["reasons"])
                )
            intent = {
                "id": intent_id,
                "target": target,
                "thread_id": task["thread_id"],
                "status": "sending",
                "event_id": event_id,
                "created_at": time.time(),
                "input_sha256": fingerprint,
                "automatic": automatic,
                "policy_charged": charge is not None,
            }
            if summary_plan:
                intent["summary_plan"] = summary_plan
            self.state["intents"][intent_id] = intent
            if heartbeat_receipt is not None:
                intent["heartbeat_receipt_id"] = heartbeat_receipt["id"]
                self.state.setdefault("heartbeat_consumed", {})[target] = heartbeat_receipt
            # A manual follow-up can share a native root with an autonomous
            # Goal/queue. It cannot revoke ownership of that still-running tree.
            task["automatic"] = automatic or task.get("automatic") is True
            self._policy_wakeup.set()
            self.save()  # Save intent before any RPC can start side effects.
            try:
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
                self._finish_policy_intent(intent, "unknown")
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
        heartbeat_receipt = None
        if summary_level(event.job):
            try:
                dependencies = summary_dependencies(event, self.store.list_events())
            except UnclosedSummaryDependency as error:
                raise DeferredDispatch(str(error)) from error
            if dependencies:
                raise DeferredDispatch("Required lower summary windows are not completed")
            plan = prepare_dispatch(event, self.memory)
            if not plan["prepared"]:
                return DispatchReceipt(
                    "completed", detail=f"No source records; explicit skips in {plan['plan_path']}"
                )
            prompt, summary_plan = plan["prompt"], plan["plan_path"]
        if event.job.kind == "heartbeat":
            evidence = await self.observe_heartbeat(event.target)
            heartbeat_receipt = evidence["latest"]
            if not evidence["configured"]:
                # General periodic self-review remains usable without an
                # external business source. It gets ordinary admission, never
                # an invented unchanged verdict or evidence-based sleep.
                heartbeat_receipt = None
                prompt += "\nExternal heartbeat evidence is unconfigured; no external unchanged verdict is available."
            elif evidence["collection_rejected"]:
                raise DeferredDispatch(
                    "Heartbeat observation did not advance its evidence watermark"
                )
            elif heartbeat_receipt["state"] != "known":
                raise DeferredDispatch("Heartbeat evidence is " + heartbeat_receipt["state"])
            # Compare to the evidence actually admitted, not just the preceding
            # poll: an actionable change survives pauses and unchanged rechecks.
            if heartbeat_receipt is not None and not self._heartbeat_candidate(
                event.target, heartbeat_receipt
            ):
                raise DeferredDispatch(
                    "Heartbeat source is unchanged; waiting for the next host check"
                )
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
            heartbeat_receipt=heartbeat_receipt,
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
                self._finish_policy_intent(intent, "unknown")
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
            self._pause_revision += 1
            self.store.set_autonomy_paused(True)
            keys = list(self.state["tasks"])
        else:
            if target not in self.state["tasks"]:
                raise ValueError("Unknown task")
            self._task_pause_revisions[target] = self._task_pause_revisions.get(target, 0) + 1
            keys = [target]
        for key in keys:
            self.state["tasks"][key]["paused"] = True
        self.save()
        results = {}
        for key in keys:
            async with self._lock("input:" + key):
                try:
                    results[key] = await self._stop_task(key)
                except Exception:
                    self._fail("Task tree pause could not be proven")
                    raise RuntimeError(
                        "Could not prove task tree paused; stopping the owned Codex server"
                    )
        return {"paused": True, "tasks": results}

    async def _stop_task(self, key: str, *, codex=None, timeout: float | None = None) -> dict:
        client = codex or self.codex
        task = self.state["tasks"][key]
        thread_id = task["thread_id"]
        try:
            return await _stop_native_root(client, self.config, thread_id, timeout=timeout)
        except RpcError as error:
            untouched = not task.get("has_input") and not any(
                intent["thread_id"] == thread_id for intent in self.state["intents"].values()
            )
            if (
                not untouched
                or error.code != -32600
                or str(error) != f"no rollout found for thread id {thread_id}"
            ):
                raise
            # Native explicitly confirms an unused alias has no execution to
            # stop. Keep that paused alias; stopping must not create a new root.
            return {"stopped": [], "absent_empty_thread": thread_id}

    async def resume(self, target: str | None = None) -> dict:
        await self._stop_resource_paused_tasks()
        await self._enforce_task_deadlines()
        blocked_tasks = {}
        async with self._lock("admission"):
            revision = self._pause_revision
            keys = (
                [target]
                if target
                else [name for name, task in self.state["tasks"].items() if task.get("paused")]
            )
            task_revisions = {key: self._task_pause_revisions.get(key, 0) for key in keys}
            for key in keys:
                blocked = self._policy_resume_block(key)
                if target is None and blocked:
                    blocked_tasks[key] = blocked
                    continue
                async with self._lock("input:" + key):
                    self._resuming.add(key)
                    try:

                        def check():
                            self._mark_resource_pauses()
                            if (
                                self.stopping
                                or self._pause_revision != revision
                                or self._task_pause_revisions.get(key, 0) != task_revisions[key]
                            ):
                                raise RejectedDispatch("Resume superseded by a pause")
                            if (
                                self.state["tasks"].get(key, {}).get("automatic") is True
                                and not self.resources.can_dispatch(automatic=True)["allowed"]
                            ):
                                raise RejectedDispatch("Automatic task resource limit is active")
                            blocked = self._policy_resume_block(key)
                            if blocked:
                                raise RejectedDispatch(
                                    "Task policy blocks resume: " + ", ".join(blocked)
                                )

                        check()
                        task = await self.ensure_thread(key)
                        check()
                        queue = await self.codex.queue_list(task["thread_id"], limit=1)
                        check()
                        goal = (
                            await self.rpc.request(
                                "thread/goal/get", {"threadId": task["thread_id"]}
                            )
                        ).get("goal")
                        check()
                        if queue.get("data") or (goal and goal.get("status") == "paused"):
                            try:
                                self._check_policy_native_resume(key)
                            except RejectedDispatch:
                                if target is not None:
                                    raise
                                blocked_tasks[key] = [
                                    "native_work_has_no_confirmed_charged_attempt"
                                ]
                                continue
                        if queue.get("data"):
                            await self.codex.queue_start(task["thread_id"])
                            check()
                        if goal and goal.get("status") == "paused":
                            self._check_policy_native_resume(key)
                            await self.rpc.request(
                                "thread/goal/set",
                                {"threadId": task["thread_id"], "status": "active"},
                            )
                            check()
                        task["paused"] = False
                        task.pop("reconcile_error", None)
                        task.pop("policy_deadline_stopped", None)
                        self.save()
                    finally:
                        self._resuming.discard(key)
            if target is None:
                if self.stopping or self._pause_revision != revision:
                    raise RejectedDispatch("Resume superseded by a pause")
                self.store.set_autonomy_paused(False)
        return {
            "resumed": target or "autonomy",
            **({"blocked_tasks": blocked_tasks} if blocked_tasks else {}),
        }

    def _policy_resume_block(self, target: str) -> list[str]:
        policy = self.store.task_policy_status(
            target, now=time.time(), busy=self._policy_intent_busy(target)
        )
        if policy is None or policy["decision"]["allowed"]:
            return []
        # An already admitted final attempt can continue; no new input is bought.
        if (
            policy["usage"] is not None
            and policy["usage"]["last_outcome"] == "running"
            and policy["decision"]["reasons"] == ["task_busy"]
        ):
            return []
        return policy["decision"]["reasons"] or ["task_" + policy["decision"]["state"]]

    def _check_policy_native_resume(self, target: str) -> None:
        record = self.store.get_task_policy(target)
        if record is None:
            return
        usage = record["usage"]
        current = self.state["intents"].get(record["current_attempt_id"])
        if (
            usage is None
            or usage["last_outcome"] != "running"
            or current is None
            or not current.get("policy_charged")
            or current.get("status") not in {"accepted", "queued"}
            or current.get("thread_id") != self.state["tasks"][target]["thread_id"]
        ):
            raise RejectedDispatch(
                "Native queue/Goal has no confirmed charged attempt; submit new work through task admission"
            )

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
            return await self.resume(params.get("target"))
        if action == "thread":
            return await self.ensure_thread(params.get("target", "main"))
        if action == "ask":
            return await self.submit(
                params.get("target", "main"),
                params["text"],
                intent_id=params.get("request_id"),
                automatic=params.get("automatic", False),
            )
        if action == "task_policy_set":
            return await self.set_task_policy(**params)
        if action == "task_policy_status":
            return await self.task_policy_status(params.get("target", "main"))
        if action == "task_status":
            target = params.get("target", "main")
            task = self.state["tasks"][target]
            native = await self.codex.thread_read(task["thread_id"], include_turns=True)
            policy = self.store.task_policy_status(
                target,
                now=time.time(),
                busy=self._policy_intent_busy(target)
                or native["thread"].get("status", {}).get("type") == "active",
            )
            return {
                **native,
                **(policy or {"target": target, "policy": None, "usage": None, "decision": None}),
                "enforcement_scope": "alice_admission",
            }
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
        if action == "summary_partition_next":
            return self.memory.summary_partition_next(**params)
        if action == "commit_summary_partition":
            return self.memory.commit_summary_partition(**params)
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
