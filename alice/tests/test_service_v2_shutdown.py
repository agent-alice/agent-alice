"""Service shutdown awaits real RPC ownership work after the reader tail drains.

Only the transport and owned-process boundary are synthetic. Gates expose
response/continuation and cancellation races without native or model execution.
"""

import asyncio
import json
import signal

import alice_codex.service as service_module
from test_service_resource_epochs import epoch_tokens, host as host, token
from test_service_shutdown_tail import EOF, ExitingProcess, QueueTransport, bind
from test_service_v2_attribution import activity


class ReadTransport(QueueTransport):
    def __init__(self, order, *, hold_send=False):
        super().__init__(order)
        self.request_received = asyncio.Event()
        self.send_released = asyncio.Event()
        if not hold_send:
            self.send_released.set()
        self.request = None

    async def send(self, raw):
        request = json.loads(raw)
        assert request["method"] == "thread/read"
        assert request["params"]["threadId"] == "child"
        self.request = request
        self.order.append("read_sent")
        self.request_received.set()
        await self.send_released.wait()

    def answer(self):
        self.order.append("response_queued")
        self.publish(
            {
                "id": self.request["id"],
                "result": {
                    "thread": {
                        "id": "child",
                        "parentThreadId": "root",
                        "status": {"type": "idle"},
                    },
                },
            }
        )


def observe_cleanup(host, observer, order, monkeypatch):
    entered = asyncio.Event()
    close_observer = observer.close
    close_codex = host.codex.close
    close_store = host.store.close
    record = host.resources.record_token_usage

    async def observed_close():
        order.append("observer_close_enter")
        entered.set()
        await close_observer()
        order.append("observer_close_return")

    def observed_codex_close():
        order.append("codex_close")
        close_codex()

    def observed_store_close():
        order.append("store_close")
        close_store()

    def observed_record(*args, **kwargs):
        result = record(*args, **kwargs)
        order.append("ledger_write")
        return result

    def terminate(pid, sig):
        assert pid == ExitingProcess.pid and sig == signal.SIGTERM
        order.append("SIGTERM")

    monkeypatch.setattr(observer, "close", observed_close)
    monkeypatch.setattr(host.codex, "close", observed_codex_close)
    monkeypatch.setattr(host.store, "close", observed_store_close)
    monkeypatch.setattr(host.resources, "record_token_usage", observed_record)
    monkeypatch.setattr(service_module.os, "killpg", terminate)
    return entered


async def test_shutdown_accounts_read_response_before_closing_after_reader_eof(host, monkeypatch):
    order = []
    transport = ReadTransport(order, hold_send=True)

    async def tail():
        await transport.request_received.wait()
        transport.answer()
        transport.queue.put_nowait(EOF)

    process = ExitingProcess(order, tail)
    epoch, rpc = bind(host, process, transport)
    observer = host._resource_observer
    closing = observe_cleanup(host, observer, order, monkeypatch)
    observer.start_reconciliation()
    log = (host.config.root / "synthetic-v2-shutdown.log").open("ab")
    shutdown = None
    try:
        transport.publish(token(130, thread="child"))
        await asyncio.wait_for(transport.request_received.wait(), 1)
        shutdown = asyncio.create_task(host._shutdown(log))
        await asyncio.wait_for(closing.wait(), 1)
        # The actual reader already fulfilled the public request, while its
        # sending coroutine has not returned to remember the parent relation.
        assert rpc._reader.done()
        assert rpc._pending[transport.request["id"]].done()
        assert observer.pending_count == 1 and not host.codex.owns("child")
        assert epoch_tokens(host, epoch, "child") is None
        assert "codex_close" not in order and "store_close" not in order
        transport.send_released.set()
        await asyncio.wait_for(shutdown, 1)
        await process.tail_task
        child = epoch_tokens(host, epoch, "child")
        assert child["high_water"]["totalTokens"] == 130 and child["event_count"] == 1
        assert observer.pending_count == 0 and rpc.pending_count == 0
        sequence = [
            "SIGTERM",
            "process_exit",
            "response_queued",
            "reader_eof",
            "observer_close_enter",
            "ledger_write",
            "observer_close_return",
            "codex_close",
            "transport_close",
            "store_close",
        ]
        assert [order.index(name) for name in sequence] == sorted(
            order.index(name) for name in sequence
        )
        assert log.closed
    finally:
        transport.send_released.set()
        if shutdown is not None and not shutdown.done():
            shutdown.cancel()
        if shutdown is not None:
            await asyncio.gather(shutdown, return_exceptions=True)
        await observer.close()
        await rpc.close()
        if process.tail_task:
            await process.tail_task
        log.close()


async def test_shutdown_cancels_unanswered_read_and_awaits_cleanup_before_client_close(
    host, monkeypatch
):
    order = []
    transport = ReadTransport(order)
    exited = asyncio.Event()

    async def tail():
        exited.set()  # Deliberately no response and no EOF.

    process = ExitingProcess(order, tail)
    epoch, rpc = bind(host, process, transport)
    observer = host._resource_observer
    observe_cleanup(host, observer, order, monkeypatch)
    monkeypatch.setattr(service_module, "RESOURCE_SHUTDOWN_DRAIN_TIMEOUT", 0.03)
    monkeypatch.setattr(observer, "RECONCILE_CLOSE_TIMEOUT", 0.03)
    read_cancelled, release_cleanup = asyncio.Event(), asyncio.Event()
    request = rpc.request

    async def observed_request(*args, **kwargs):
        try:
            return await request(*args, **kwargs)
        except asyncio.CancelledError:
            order.append("read_cancelled")
            read_cancelled.set()
            await release_cleanup.wait()
            order.append("read_cleanup_done")
            raise

    monkeypatch.setattr(rpc, "request", observed_request)
    observer.start_reconciliation()
    log = (host.config.root / "synthetic-v2-shutdown.log").open("ab")
    shutdown = None
    try:
        transport.publish(token(80, thread="child"))
        await asyncio.wait_for(transport.request_received.wait(), 1)
        shutdown = asyncio.create_task(host._shutdown(log))
        await asyncio.wait_for(read_cancelled.wait(), 1)
        assert exited.is_set() and process.returncode is not None
        assert not shutdown.done()
        assert "codex_close" not in order and "transport_close" not in order
        assert "store_close" not in order and "ledger_write" not in order
        release_cleanup.set()
        await asyncio.wait_for(shutdown, 1)
        await process.tail_task
        sequence = [
            "SIGTERM",
            "process_exit",
            "observer_close_enter",
            "read_cancelled",
            "read_cleanup_done",
            "observer_close_return",
            "codex_close",
            "transport_close",
            "store_close",
        ]
        assert [order.index(name) for name in sequence] == sorted(
            order.index(name) for name in sequence
        )
        assert rpc.pending_count == 0 and rpc._reader.done() and log.closed
        assert "tail drain timed out" in host.error
        assert observer.pending_count == 1
        # A late captured callback cannot write after its client/store closed.
        host.codex._on_event(activity("child"))
        observer.receive(activity("child"))
        observer.receive(token(99, thread="child"))
        observer.flush()
        assert epoch_tokens(host, epoch, "child") is None and "ledger_write" not in order
    finally:
        release_cleanup.set()
        if shutdown is not None and not shutdown.done():
            shutdown.cancel()
        if shutdown is not None:
            await asyncio.gather(shutdown, return_exceptions=True)
        await observer.close()
        await rpc.close()
        if process.tail_task:
            await process.tail_task
        log.close()
