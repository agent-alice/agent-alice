"""Bounded JSON-RPC client for an explicitly selected Codex control socket.

Codex's Unix socket carries WebSocket frames, not JSONL. This module never
starts a daemon, changes its configuration, or terminates its process.
"""

import asyncio
from collections import deque
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Awaitable, Callable

from websockets.asyncio.client import unix_connect


class RpcError(RuntimeError):
    def __init__(self, message: str, code: int | None = None, data: Any = None):
        super().__init__(message)
        self.code, self.data = code, data


class RpcDisconnected(RpcError):
    """The connection ended; in-flight operations may have reached Codex."""


class RpcTimeout(TimeoutError):
    """Only the response wait timed out. The remote operation was not cancelled."""


class EventGapError(RpcError):
    """Retained notifications no longer cover the requested cursor."""


@dataclass(frozen=True)
class Event:
    sequence: int
    message: dict[str, Any]
    size: int


UNHANDLED = object()
RequestHandler = Callable[[str, dict[str, Any]], Awaitable[Any]]


class RpcClient:
    """One bidirectional connection, with passive approval handling by default.

    When another client (the TUI) handles interaction, this client must not race it
    by declining its approvals. Interactive requests remain visible as events;
    without a TUI or an explicit handler they remain pending, never auto-approved.
    """

    def __init__(
        self,
        connection: Any,
        *,
        request_timeout: float = 30,
        request_handler: RequestHandler | None = None,
        approval_policy: str = "defer",
        max_events: int = 512,
        max_event_bytes: int = 4 * 1024 * 1024,
        max_pending: int = 128,
        max_handlers: int = 32,
    ):
        if approval_policy not in {"defer", "deny"}:
            raise ValueError("approval_policy must be defer or deny")
        if min(max_events, max_event_bytes, max_pending, max_handlers) < 1:
            raise ValueError("RPC bounds must be positive")
        self.connection = connection
        self.request_timeout = request_timeout
        self.request_handler = request_handler
        self.approval_policy = approval_policy
        self.max_events, self.max_event_bytes = max_events, max_event_bytes
        self.max_pending, self.max_handlers = max_pending, max_handlers
        self.events: deque[Event] = deque()
        self.diagnostics: deque[dict[str, Any]] = deque(maxlen=128)
        self.event_sequence = 0
        self._event_bytes = 0
        self._sequence = 0
        self._pending: dict[int, asyncio.Future[Any]] = {}
        self._handlers: set[asyncio.Task[Any]] = set()
        self._listeners: list[Callable[[dict[str, Any]], None]] = []
        self._changed = asyncio.Event()
        self._send_lock = asyncio.Lock()
        self._failure: RpcDisconnected | None = None
        self._reader = asyncio.create_task(self._read(), name="alice-codex-rpc")

    @classmethod
    async def connect_unix(
        cls, socket_path: str | Path, *, timeout: float = 10, **kwargs: Any
    ) -> "RpcClient":
        path = Path(socket_path)
        if not path.is_absolute():
            raise ValueError("Codex socket path must be absolute")
        connection = await unix_connect(
            str(path),
            uri="ws://localhost/rpc",
            open_timeout=timeout,
            close_timeout=3,
            max_size=16 * 1024 * 1024,
            max_queue=16,
            compression=None,
            proxy=None,
        )
        return cls(connection, **kwargs)

    @property
    def connected(self) -> bool:
        return self._failure is None and not self._reader.done()

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    def add_listener(self, listener: Callable[[dict[str, Any]], None]) -> Callable[[], None]:
        """Subscribe a short synchronous callback; return its disposer."""
        self._listeners.append(listener)

        def remove() -> None:
            if listener in self._listeners:
                self._listeners.remove(listener)

        return remove

    async def initialize(self, *, name: str = "alice_codex", version: str = "0.1.0") -> dict:
        result = await self.request(
            "initialize",
            {
                "clientInfo": {"name": name, "title": "Alice", "version": version},
                "capabilities": {"experimentalApi": True},
            },
        )
        await self.notify("initialized", {})
        return result

    async def _send(self, message: dict[str, Any]) -> None:
        if self._failure:
            raise self._failure
        raw = json.dumps(message, ensure_ascii=False, separators=(",", ":"))
        async with self._send_lock:
            try:
                await self.connection.send(raw)
            except Exception as exc:
                self._disconnect(f"Codex send failed: {type(exc).__name__}")
                raise self._failure from exc
        # Never retain prompts, tool bodies, credentials, or complete responses in logs.
        self.diagnostics.append(
            {
                "direction": "send",
                "id": message.get("id"),
                "method": message.get("method"),
                "bytes": len(raw),
            }
        )

    async def request(
        self, method: str, params: dict | None = None, *, timeout: float | None = None
    ) -> Any:
        if len(self._pending) >= self.max_pending:
            raise RpcError("Too many pending Codex requests")
        if self._failure:
            raise self._failure
        self._sequence += 1
        request_id = self._sequence
        future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        try:
            await self._send({"id": request_id, "method": method, "params": params or {}})
            try:
                return await asyncio.wait_for(
                    asyncio.shield(future), self.request_timeout if timeout is None else timeout
                )
            except asyncio.TimeoutError as exc:
                raise RpcTimeout(
                    f"{method} response timed out; remote outcome is unknown, not cancelled"
                ) from exc
        finally:
            self._pending.pop(request_id, None)
            if not future.done():
                future.cancel()
            elif not future.cancelled():
                future.exception()  # Consume an EOF failure even if send itself failed.

    async def notify(self, method: str, params: dict | None = None) -> None:
        await self._send({"method": method, "params": params or {}})

    async def respond(
        self, request_id: int | str, *, result: Any = None, error: dict | None = None
    ) -> None:
        message = {"id": request_id}
        message["error" if error is not None else "result"] = error if error is not None else result
        await self._send(message)

    def _publish(self, message: dict[str, Any], size: int) -> None:
        self.event_sequence += 1
        self.events.append(Event(self.event_sequence, message, size))
        self._event_bytes += size
        while len(self.events) > self.max_events or self._event_bytes > self.max_event_bytes:
            self._event_bytes -= self.events.popleft().size
        for listener in tuple(self._listeners):
            try:
                listener(message)
            except Exception as exc:
                self.diagnostics.append({"listener_error": type(exc).__name__})
        self._changed.set()

    def _disconnect(self, reason: str) -> None:
        if self._failure is None:
            self._failure = RpcDisconnected(reason)
        for future in self._pending.values():
            if not future.done():
                future.set_exception(self._failure)
        self._changed.set()

    async def _read(self) -> None:
        try:
            async for raw in self.connection:
                if not isinstance(raw, str):
                    raise ValueError("Codex JSON-RPC must use WebSocket text messages")
                message = json.loads(raw)
                if not isinstance(message, dict):
                    raise ValueError("Codex JSON-RPC message must be an object")
                self.diagnostics.append(
                    {
                        "direction": "receive",
                        "id": message.get("id"),
                        "method": message.get("method"),
                        "bytes": len(raw),
                    }
                )
                if "method" in message:
                    self._publish(message, len(raw.encode("utf-8")))
                    if "id" in message:
                        if len(self._handlers) >= self.max_handlers:
                            await self.respond(
                                message["id"],
                                error={
                                    "code": -32001,
                                    "message": "Alice client request handler overloaded",
                                },
                            )
                        else:
                            task = asyncio.create_task(self._handle_request(message))
                            self._handlers.add(task)
                            task.add_done_callback(self._handlers.discard)
                else:
                    future = self._pending.get(message.get("id"))
                    if future is not None and not future.done():
                        if "error" in message:
                            error = message["error"]
                            future.set_exception(
                                RpcError(
                                    error.get("message", "Codex error"),
                                    error.get("code"),
                                    error.get("data"),
                                )
                            )
                        else:
                            future.set_result(message.get("result"))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._disconnect(f"Codex connection failed: {type(exc).__name__}")
        finally:
            self._disconnect("Codex connection closed; pending outcomes may be unknown")

    async def _handle_request(self, message: dict) -> None:
        method, params = message["method"], message.get("params", {})
        try:
            result = UNHANDLED
            if self.request_handler is not None:
                result = await self.request_handler(method, params)
            if result is not UNHANDLED:
                await self.respond(message["id"], result=result)
                return
            interactive = method.endswith("requestApproval") or method in {
                "mcpServer/elicitation/request",
                "item/tool/requestUserInput",
            }
            if interactive and self.approval_policy == "defer":
                return
            if method == "item/permissions/requestApproval":
                result = {"permissions": {}, "scope": "turn"}
            elif method.endswith("requestApproval"):
                result = {"decision": "decline"}
            elif method == "mcpServer/elicitation/request":
                result = {"action": "cancel"}
            else:
                await self.respond(
                    message["id"], error={"code": -32601, "message": "Client method not handled"}
                )
                return
            await self.respond(message["id"], result=result)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.diagnostics.append({"handler_error": type(exc).__name__, "method": method})
            if self.connected:
                try:
                    await self.respond(
                        message["id"],
                        error={"code": -32603, "message": "Alice client handler failed"},
                    )
                except RpcDisconnected:
                    pass

    async def wait_event(
        self, predicate: Callable[[dict], bool], *, after: int = 0, timeout: float = 30
    ) -> dict:
        async def wait() -> dict:
            cursor = after
            while True:
                self._changed.clear()
                oldest = self.events[0].sequence if self.events else self.event_sequence + 1
                if cursor < oldest - 1:
                    raise EventGapError("Codex events were evicted; reconcile with thread/read")
                for event in self.events:
                    if event.sequence > cursor:
                        cursor = event.sequence
                        if predicate(event.message):
                            return event.message
                if self._failure:
                    raise self._failure
                await self._changed.wait()

        return await asyncio.wait_for(wait(), timeout)

    async def close(self) -> None:
        """Disconnect this client only. The shared App Server keeps running."""
        self._disconnect("Alice client disconnected")
        await self.connection.close()
        self._reader.cancel()
        for task in tuple(self._handlers):
            task.cancel()
        await asyncio.gather(self._reader, *tuple(self._handlers), return_exceptions=True)
        self._listeners.clear()


JsonRpcClient = RpcClient
