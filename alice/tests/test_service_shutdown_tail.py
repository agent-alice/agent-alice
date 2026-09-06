"""Shutdown tails through the real RPC reader; process/transport are synthetic.

No native binary or model is started. The process boundary deliberately emits
notifications asynchronously after wait() observes exit, as a socket can retain
already-produced messages after the child has terminated.
"""

import asyncio
import json
import signal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from alice_codex.codex import CodexClient
from alice_codex.rpc import RpcClient
import alice_codex.service as service_module
from test_service_resource_epochs import host as host, started, token


EOF = object()


class QueueTransport:
    def __init__(self, order):
        self.queue = asyncio.Queue()
        self.order = order

    def __aiter__(self):
        return self

    async def __anext__(self):
        message = await self.queue.get()
        if message is EOF:
            self.order.append("reader_eof")
            raise StopAsyncIteration
        if isinstance(message, Exception):
            self.order.append("reader_failure")
            raise message
        return message

    async def send(self, raw):
        raise AssertionError("This test must not send native protocol requests")

    async def close(self):
        self.order.append("transport_close")
        self.queue.put_nowait(EOF)

    def publish(self, message):
        self.queue.put_nowait(json.dumps(message))


class ExitingProcess:
    pid = 424242

    def __init__(self, order, after_exit):
        self.returncode = None
        self.order = order
        self.after_exit = after_exit
        self.tail_task = None

    async def wait(self):
        self.order.append("process_exit")
        self.returncode = 0
        self.tail_task = asyncio.create_task(self.after_exit())
        return self.returncode


class KillRequiredProcess(ExitingProcess):
    def __init__(self, order, after_exit):
        super().__init__(order, after_exit)
        self.wait_entered = asyncio.Event()
        self.killed = asyncio.Event()

    async def wait(self):
        self.wait_entered.set()
        await self.killed.wait()
        return await super().wait()


async def no_tail():
    await asyncio.sleep(0)


def bind(host, process, transport):
    epoch = host._prepare_resource_epoch()
    host.process = process
    host._bind_resource_epoch(epoch)
    rpc = RpcClient(transport)
    codex = CodexClient(rpc, owned_root_ids=["root"])
    host.rpc, host.codex = rpc, codex
    host._attach_resource_listener(rpc, codex, epoch)
    host.archive_native_history = AsyncMock()
    host._stop_task = AsyncMock(side_effect=RuntimeError("synthetic stop_tree failure"))
    return epoch, rpc


@pytest.mark.parametrize("requires_kill", [False, True])
async def test_forced_termination_drains_child_tail_after_process_exit(
    host, monkeypatch, requires_kill
):
    order = []
    transport = QueueTransport(order)

    async def tail():
        await asyncio.sleep(0)
        order.append("tail_published")
        transport.publish(started("tail-child", "root"))
        transport.publish(token(130, thread="tail-child", turn="tail"))
        transport.publish(token(130, thread="tail-child", turn="tail"))
        transport.queue.put_nowait(EOF)

    process_class = KillRequiredProcess if requires_kill else ExitingProcess
    process = process_class(order, tail)
    epoch, rpc = bind(host, process, transport)
    monkeypatch.setattr(service_module, "NATIVE_SHUTDOWN_TERM_TIMEOUT", 0.03)

    def terminate(pid, sig):
        assert pid == process.pid
        assert sig in (signal.SIGTERM, signal.SIGKILL)
        order.append(sig.name)
        if sig == signal.SIGKILL:
            assert requires_kill
            process.killed.set()

    monkeypatch.setattr(service_module.os, "killpg", terminate)
    log = (host.config.root / "synthetic-shutdown.log").open("ab")
    try:
        await host._shutdown(log)
        await process.tail_task
        tokens = host.resources.status()["tokens"]
        child = tokens["epochs"].get(epoch, {}).get("tail-child")
        assert child is not None, "Forced-termination tail disappeared before RPC reader EOF"
        assert child["high_water"]["totalTokens"] == 130
        assert child["event_count"] == 1
        assert tokens["threads"] == {} and tokens["actual_usage_total"] is None
        assert order.index("SIGTERM") < order.index("process_exit")
        if requires_kill:
            assert order.index("SIGTERM") < order.index("SIGKILL") < order.index("process_exit")
        assert order.index("process_exit") < order.index("tail_published")
        assert order.index("reader_eof") < order.index("transport_close")
        assert log.closed
    finally:
        await rpc.close()
        if process.tail_task:
            await process.tail_task
        log.close()


@pytest.mark.parametrize("disconnect", [EOF, ConnectionError("synthetic transport failure")])
async def test_early_reader_disconnect_still_terminates_and_reaps_child(
    host, monkeypatch, disconnect
):
    order = []
    transport = QueueTransport(order)
    process = ExitingProcess(order, no_tail)
    _, rpc = bind(host, process, transport)
    transport.queue.put_nowait(disconnect)
    assert await rpc.wait_reader_closed(timeout=1)
    assert not rpc.connected

    def terminate(pid, sig):
        assert pid == process.pid and sig == signal.SIGTERM
        order.append("SIGTERM")

    monkeypatch.setattr(service_module.os, "killpg", terminate)
    log = (host.config.root / "synthetic-shutdown.log").open("ab")
    try:
        await host._shutdown(log)
        await process.tail_task
        assert order.index("SIGTERM") < order.index("process_exit")
        assert order.index("process_exit") < order.index("transport_close")
        assert process.returncode is not None and log.closed
        assert host.state["lifecycle"] == "stopped" and host.state["server"] is None
    finally:
        await rpc.close()
        if process.tail_task:
            await process.tail_task
        log.close()


async def test_missing_eof_has_bounded_drain_and_preserves_error(host, monkeypatch):
    order = []
    transport = QueueTransport(order)
    process = ExitingProcess(order, no_tail)
    _, rpc = bind(host, process, transport)
    monkeypatch.setattr(service_module, "RESOURCE_SHUTDOWN_DRAIN_TIMEOUT", 0.03)
    monkeypatch.setattr(service_module.os, "killpg", lambda pid, sig: order.append(sig.name))
    log = (host.config.root / "synthetic-shutdown.log").open("ab")
    try:
        await asyncio.wait_for(host._shutdown(log), 1)
        await process.tail_task
        assert "tail drain timed out" in host.error
        assert "owned-server termination" in host.error
        assert b"tail drain timed out" in (host.config.root / "synthetic-shutdown.log").read_bytes()
        assert order.index("process_exit") < order.index("transport_close")
        assert "reader_eof" not in order
        assert process.returncode is not None and rpc._reader.done() and log.closed
    finally:
        await rpc.close()
        if process.tail_task:
            await process.tail_task
        log.close()


async def test_cancellation_during_drain_closes_captured_resources(host, monkeypatch):
    order = []
    transport = QueueTransport(order)
    process = ExitingProcess(order, no_tail)
    _, rpc = bind(host, process, transport)
    codex = host.codex
    close_codex = Mock(wraps=codex.close)
    monkeypatch.setattr(codex, "close", close_codex)
    monkeypatch.setattr(service_module, "RESOURCE_SHUTDOWN_DRAIN_TIMEOUT", 0.03)
    monkeypatch.setattr(service_module.os, "killpg", lambda pid, sig: order.append(sig.name))
    drain_entered = asyncio.Event()
    wait_reader_closed = rpc.wait_reader_closed

    async def observed_drain(*, timeout):
        drain_entered.set()
        return await wait_reader_closed(timeout=timeout)

    monkeypatch.setattr(rpc, "wait_reader_closed", observed_drain)
    log = (host.config.root / "synthetic-shutdown.log").open("ab")
    shutdown = asyncio.create_task(host._shutdown(log))
    try:
        await asyncio.wait_for(drain_entered.wait(), 1)
        assert process.returncode is not None and not rpc._reader.done()
        shutdown.cancel()
        with pytest.raises(asyncio.CancelledError):
            await shutdown
        close_codex.assert_called_once()
        assert order.index("process_exit") < order.index("transport_close")
        assert rpc._reader.done() and log.closed
    finally:
        if not shutdown.done():
            shutdown.cancel()
        await asyncio.gather(shutdown, return_exceptions=True)
        await rpc.close()
        if process.tail_task:
            await process.tail_task
        log.close()


async def test_shutdown_keeps_original_client_process_and_epoch_after_fields_change(
    host, monkeypatch
):
    """Capture stability only; concurrent replacement is not a supported new run."""
    order = []
    transport = QueueTransport(order)

    async def tail():
        await asyncio.sleep(0)
        transport.publish(started("tail-child", "root"))
        transport.publish(token(130, thread="tail-child", turn="tail"))
        transport.queue.put_nowait(EOF)

    process = ExitingProcess(order, tail)
    epoch, rpc = bind(host, process, transport)
    codex = host.codex
    replacement_rpc = SimpleNamespace(close=AsyncMock())
    replacement_codex = SimpleNamespace(close=Mock())
    replacement_process = SimpleNamespace(pid=989898, returncode=None)

    async def replace_fields(key, *, codex: CodexClient, timeout):
        assert codex is original_codex
        host.rpc = replacement_rpc
        host.codex = replacement_codex
        host.process = replacement_process
        host._starting_resource_epoch = "unrelated-epoch"
        raise RuntimeError("synthetic stop_tree failure after field replacement")

    original_codex = codex
    host._stop_task = AsyncMock(side_effect=replace_fields)

    def terminate(pid, sig):
        assert pid == process.pid and sig == signal.SIGTERM
        order.append("SIGTERM")

    monkeypatch.setattr(service_module.os, "killpg", terminate)
    log = (host.config.root / "synthetic-shutdown.log").open("ab")
    try:
        await host._shutdown(log)
        await process.tail_task
        child = host.resources.status()["tokens"]["epochs"][epoch]["tail-child"]
        assert child["high_water"]["totalTokens"] == 130 and child["event_count"] == 1
        replacement_rpc.close.assert_not_awaited()
        replacement_codex.close.assert_not_called()
        assert replacement_process.returncode is None
        assert order.index("reader_eof") < order.index("transport_close")
        assert rpc._reader.done() and log.closed
    finally:
        await rpc.close()
        if process.tail_task:
            await process.tail_task
        log.close()


async def test_cancellation_during_process_wait_still_reaps_owned_child(host, monkeypatch):
    order = []
    transport = QueueTransport(order)

    async def tail():
        await asyncio.sleep(0)
        transport.queue.put_nowait(EOF)

    process = KillRequiredProcess(order, tail)
    _, rpc = bind(host, process, transport)
    monkeypatch.setattr(service_module, "NATIVE_SHUTDOWN_TERM_TIMEOUT", 0.03)

    def terminate(pid, sig):
        assert pid == process.pid
        order.append(sig.name)
        if sig == signal.SIGKILL:
            process.killed.set()

    monkeypatch.setattr(service_module.os, "killpg", terminate)
    log = (host.config.root / "synthetic-shutdown.log").open("ab")
    shutdown = asyncio.create_task(host._shutdown(log))
    try:
        await asyncio.wait_for(process.wait_entered.wait(), 1)
        shutdown.cancel()
        with pytest.raises(asyncio.CancelledError):
            await shutdown
        assert process.returncode is not None, "Cancellation left the owned native child running"
        assert order.index("process_exit") < order.index("transport_close")
        assert rpc._reader.done() and log.closed
    finally:
        process.killed.set()
        if not shutdown.done():
            shutdown.cancel()
        await asyncio.gather(shutdown, return_exceptions=True)
        await rpc.close()
        if process.tail_task:
            await process.tail_task
        log.close()
