"""Exercise the wire protocol with real sockets; no model or user state."""

import asyncio
import json
import os
from pathlib import Path
import tempfile
import unittest
import pytest

from websockets.asyncio.server import unix_serve

from alice_codex.rpc import EventGapError, RpcClient, RpcDisconnected, RpcError, RpcTimeout


class RpcTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="alice-rpc-", dir="/tmp")
        self.path = Path(self.temp.name) / "control.sock"
        self.clients = []
        self.received = asyncio.Queue()
        self.peer = None
        self.joined = asyncio.Event()

        async def server(peer):
            self.peer = peer
            self.joined.set()
            async for raw in peer:
                message = json.loads(raw)
                await self.received.put(message)
                if message.get("method") == "initialize":
                    await peer.send(
                        json.dumps({"id": message["id"], "result": {"version": "test"}})
                    )

        self.server = await unix_serve(server, str(self.path))

    async def connect(self, **kwargs):
        rpc = await RpcClient.connect_unix(self.path, **kwargs)
        self.clients.append(rpc)
        await self.joined.wait()
        return rpc

    async def asyncTearDown(self):
        for rpc in self.clients:
            await rpc.close()
        self.server.close()
        await self.server.wait_closed()
        self.temp.cleanup()

    async def send(self, message):
        await self.peer.send(json.dumps(message))

    async def test_initialize_and_notifications_use_websocket(self):
        rpc = await self.connect()
        self.assertEqual(await rpc.initialize(), {"version": "test"})
        initialize = await self.received.get()
        self.assertTrue(initialize["params"]["capabilities"]["experimentalApi"])
        self.assertEqual((await self.received.get())["method"], "initialized")
        await self.send({"method": "turn/completed", "params": {"threadId": "t"}})
        event = await rpc.wait_event(lambda e: e["method"] == "turn/completed")
        self.assertEqual(event["params"]["threadId"], "t")

    async def test_concurrent_requests_correlate_out_of_order(self):
        rpc = await self.connect()
        a = asyncio.create_task(rpc.request("first"))
        b = asyncio.create_task(rpc.request("second"))
        messages = [await self.received.get(), await self.received.get()]
        for m in reversed(messages):
            await self.send({"id": m["id"], "result": m["method"]})
        self.assertEqual(await asyncio.gather(a, b), ["first", "second"])

    async def test_timeout_does_not_interrupt_remote_turn(self):
        rpc = await self.connect()
        pending = asyncio.create_task(rpc.request("turn/start", timeout=0.03))
        message = await self.received.get()
        with self.assertRaises(RpcTimeout):
            await pending
        self.assertEqual(rpc.pending_count, 0)
        self.assertTrue(self.received.empty())
        await self.send({"id": message["id"], "result": {"turn": {"id": "late"}}})
        await self.send({"method": "turn/completed", "params": {"turn": {"id": "late"}}})
        self.assertEqual(
            (await rpc.wait_event(lambda e: e["method"] == "turn/completed"))["params"]["turn"][
                "id"
            ],
            "late",
        )
        self.assertTrue(rpc.connected)

    async def test_eof_fails_all_pending_requests(self):
        rpc = await self.connect()
        tasks = [asyncio.create_task(rpc.request(name)) for name in ("one", "two")]
        await self.received.get()
        await self.received.get()
        await self.peer.close()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        self.assertTrue(all(isinstance(r, RpcDisconnected) for r in results))
        self.assertFalse(rpc.connected)

    async def test_error_preserves_native_code(self):
        rpc = await self.connect()
        task = asyncio.create_task(rpc.request("bad"))
        message = await self.received.get()
        await self.send(
            {
                "id": message["id"],
                "error": {"code": -32001, "message": "busy", "data": {"retry": True}},
            }
        )
        with self.assertRaises(RpcError) as raised:
            await task
        self.assertEqual(raised.exception.code, -32001)
        self.assertEqual(raised.exception.data, {"retry": True})

    async def test_approval_is_passive_without_explicit_handler(self):
        rpc = await self.connect()
        await self.send(
            {"id": "approval", "method": "item/commandExecution/requestApproval", "params": {}}
        )
        await rpc.wait_event(lambda e: e.get("id") == "approval")
        # A later initialize response proves the reader processed the prior request.
        await rpc.initialize()
        methods = [(await self.received.get())["method"], (await self.received.get())["method"]]
        self.assertEqual(methods, ["initialize", "initialized"])
        self.assertTrue(self.received.empty())

    async def test_explicit_headless_denial_grants_no_permissions(self):
        await self.connect(approval_policy="deny")
        await self.send(
            {
                "id": 22,
                "method": "item/permissions/requestApproval",
                "params": {"permissions": {"network": {"enabled": True}}},
            }
        )
        response = await asyncio.wait_for(self.received.get(), 1)
        self.assertEqual(response, {"id": 22, "result": {"permissions": {}, "scope": "turn"}})

    async def test_event_eviction_is_explicit_and_logs_are_bounded(self):
        rpc = await self.connect(max_events=2)
        for n in range(140):
            await self.send({"method": "progress", "params": {"n": n, "private": "secret"}})
        await rpc.wait_event(lambda e: e["params"]["n"] == 139, after=138)
        with self.assertRaises(EventGapError):
            await rpc.wait_event(lambda e: True, after=0)
        self.assertEqual(len(rpc.events), 2)
        self.assertEqual(len(rpc.diagnostics), 128)
        self.assertNotIn("secret", json.dumps(list(rpc.diagnostics)))

    async def test_closing_client_does_not_close_server(self):
        first = await self.connect()
        await first.close()
        second = await self.connect()
        self.assertEqual(await second.initialize(), {"version": "test"})


@pytest.mark.native
class NativeCodexTests(unittest.IsolatedAsyncioTestCase):
    async def test_isolated_native_unix_initialize(self):
        binary = Path(
            os.environ.get(
                "ALICE_TEST_CODEX_BINARY", "/Applications/ChatGPT.app/Contents/Resources/codex"
            )
        )
        if not binary.is_file():
            self.fail(f"Required native Codex binary unavailable: {binary}")
        with tempfile.TemporaryDirectory(prefix="alice-native-", dir="/tmp") as temporary:
            root = Path(temporary)
            home = root / "home"
            home.mkdir()
            state = root / "codex"
            state.mkdir()
            control = root / "control"
            control.mkdir(mode=0o700)
            sock = control / "rpc.sock"
            env = {
                "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
                "HOME": str(home),
                "CODEX_HOME": str(state),
                "LANG": "en_US.UTF-8",
                "RUST_LOG": "error",
            }
            proc = await asyncio.create_subprocess_exec(
                str(binary),
                "app-server",
                "--listen",
                f"unix://{sock}",
                "-c",
                "features.apps=false",
                "-c",
                "features.plugins=false",
                "-c",
                "features.memories=false",
                "-c",
                "check_for_update_on_startup=false",
                cwd=root,
                env=env,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            clients = []
            try:

                async def wait_ready():
                    while not sock.exists():
                        if proc.returncode is not None:
                            self.fail(f"isolated App Server exited: {proc.returncode}")
                        await asyncio.sleep(0.02)

                await asyncio.wait_for(wait_ready(), 10)
                rpc = await RpcClient.connect_unix(sock)
                clients.append(rpc)
                response = await rpc.initialize()
                self.assertIsInstance(response, dict)
                from alice_codex.codex import CodexClient

                adapter = CodexClient(rpc)
                try:
                    # Read-only native validation of the sourceKinds used by stop.
                    page = await adapter.thread_list(
                        sourceKinds=["cli", "vscode", "exec", "appServer", "subAgent", "unknown"],
                        modelProviders=[],
                        limit=100,
                    )
                    self.assertEqual(page["data"], [])
                finally:
                    adapter.close()
                self.assertIsNone(proc.returncode)
                await rpc.close()
                # Same explicitly owned server accepts another client; no restart.
                other = await RpcClient.connect_unix(sock)
                clients.append(other)
                self.assertIsInstance(await other.initialize(), dict)
                self.assertIsNone(proc.returncode)
            finally:
                for client in clients:
                    await client.close()
                if proc.returncode is None:
                    proc.terminate()
                    try:
                        await asyncio.wait_for(proc.wait(), 8)
                    except asyncio.TimeoutError:
                        proc.kill()
                        await proc.wait()
                self.assertIsNotNone(proc.returncode)

    async def test_native_v2_child_stop_with_local_recorded_responses(self):
        """Real Codex executes a recorded spawn call; no external model requests."""
        from alice_codex.codex import CodexClient

        binary = Path(
            os.environ.get(
                "ALICE_TEST_CODEX_BINARY", "/Applications/ChatGPT.app/Contents/Resources/codex"
            )
        )
        if not binary.is_file():
            self.fail(f"Required native Codex binary unavailable: {binary}")
        peers, handlers = set(), set()
        first_request = True
        held_requests = asyncio.Event()
        request_count = 0
        request_paths = []
        streams_closed = asyncio.Event()
        release_late_response = asyncio.Event()

        async def responses(reader, writer):
            nonlocal first_request, request_count
            peers.add(writer)
            task = asyncio.current_task()
            handlers.add(task)
            try:
                header = await reader.readuntil(b"\r\n\r\n")
                request_paths.append(header.split(b"\r\n", 1)[0].decode())
                content_length = next(
                    (
                        int(line.split(b":", 1)[1])
                        for line in header.split(b"\r\n")
                        if line.lower().startswith(b"content-length:")
                    ),
                    0,
                )
                await reader.readexactly(content_length)
                request_count += 1
                writer.write(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n"
                )
                if first_request:
                    first_request = False
                    events = [
                        {"type": "response.created", "response": {"id": "recorded-parent"}},
                        {
                            "type": "response.output_item.done",
                            "item": {
                                "type": "function_call",
                                "call_id": "recorded-spawn",
                                "namespace": "collaboration",
                                "name": "spawn_agent",
                                "arguments": json.dumps(
                                    {
                                        "task_name": "worker",
                                        "message": "Wait for the isolated test to interrupt this task.",
                                    }
                                ),
                            },
                        },
                        {
                            "type": "response.completed",
                            "response": {
                                "id": "recorded-parent",
                                "usage": {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
                            },
                        },
                    ]
                    for event in events:
                        writer.write(("data: " + json.dumps(event) + "\n\n").encode())
                    await writer.drain()
                else:
                    writer.write(
                        b'data: {"type":"response.created","response":{"id":"recorded-held"}}\n\n'
                    )
                    await writer.drain()
                    if request_count >= 3:
                        held_requests.set()
                    await release_late_response.wait()
                    # A stop cancels execution, but need not close the pooled HTTP
                    # transport immediately. Release a late tool result and make
                    # sure it doesn't start another child after the stop.
                    events = [
                        {
                            "type": "response.output_item.done",
                            "item": {
                                "type": "function_call",
                                "call_id": f"late-spawn-{id(writer)}",
                                "namespace": "collaboration",
                                "name": "spawn_agent",
                                "arguments": json.dumps(
                                    {"task_name": "late_worker", "message": "Late ignored work"}
                                ),
                            },
                        },
                        {
                            "type": "response.completed",
                            "response": {
                                "id": "recorded-held",
                                "usage": {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
                            },
                        },
                    ]
                    for event in events:
                        writer.write(("data: " + json.dumps(event) + "\n\n").encode())
                    await writer.drain()
            except (ConnectionError, asyncio.IncompleteReadError):
                pass
            finally:
                writer.close()
                await writer.wait_closed()
                peers.discard(writer)
                handlers.discard(task)
                if not peers and request_count >= 3:
                    streams_closed.set()

        http = await asyncio.start_server(responses, "127.0.0.1", 0)
        port = http.sockets[0].getsockname()[1]
        try:
            with tempfile.TemporaryDirectory(prefix="alice-child-", dir="/tmp") as temporary:
                root = Path(temporary)
                state, home = root / "codex", root / "home"
                state.mkdir(mode=0o700)
                home.mkdir(mode=0o700)
                (state / "config.toml").write_text(f"""
model = "gpt-5.4"
model_provider = "recorded"
approval_policy = "never"
sandbox_mode = "read-only"
check_for_update_on_startup = false
[features]
apps = false
plugins = false
memories = false
goals = true
multi_agent_v2 = true
[model_providers.recorded]
name = "Isolated recorded responses"
base_url = "http://127.0.0.1:{port}"
wire_api = "responses"
supports_websockets = false
requires_openai_auth = false
request_max_retries = 0
stream_max_retries = 0
""")
                env = {
                    "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
                    "HOME": str(home),
                    "CODEX_HOME": str(state),
                    "LANG": "en_US.UTF-8",
                    "RUST_LOG": "error",
                    "NO_PROXY": "127.0.0.1,localhost,::1",
                    "no_proxy": "127.0.0.1,localhost,::1",
                }
                sock = root / "rpc.sock"
                proc = await asyncio.create_subprocess_exec(
                    str(binary),
                    "app-server",
                    "--listen",
                    f"unix://{sock}",
                    cwd=root,
                    env=env,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                rpc = adapter = None
                try:

                    async def ready():
                        while not sock.exists():
                            if proc.returncode is not None:
                                self.fail(f"isolated App Server exited: {proc.returncode}")
                            await asyncio.sleep(0.02)

                    await asyncio.wait_for(ready(), 10)
                    rpc = await RpcClient.connect_unix(sock)
                    await rpc.initialize()
                    adapter = CodexClient(rpc)
                    start = await adapter.thread_start(
                        cwd=str(root), model="gpt-5.4", modelProvider="recorded"
                    )
                    self.assertEqual(start["modelProvider"], "recorded")
                    thread = start["thread"]
                    root_id = thread["id"]
                    with self.assertRaises(RpcError) as empty_queue:
                        await adapter.queue_start(root_id)
                    self.assertEqual(empty_queue.exception.code, -32600)
                    self.assertIn("queue is empty", str(empty_queue.exception))
                    turn = await adapter.turn_start(
                        root_id,
                        "Run the recorded child task.",
                        client_message_id="recorded-client-id",
                    )
                    try:
                        await asyncio.wait_for(held_requests.wait(), 20)
                    except asyncio.TimeoutError:
                        observed = [
                            event.message
                            for event in rpc.events
                            if event.message.get("method")
                            in {"error", "item/completed", "turn/completed"}
                        ]
                        self.fail(
                            f"Recorded spawn did not reach both held turns; requests={request_count}; "
                            f"events={json.dumps(observed)[-8000:]}"
                        )
                    ids = await adapter._discover_tree(root_id)
                    self.assertEqual(
                        len(ids),
                        2,
                        f"requests={request_paths}; events="
                        + json.dumps(
                            [
                                event.message
                                for event in rpc.events
                                if event.message.get("method")
                                in {"thread/started", "item/completed", "error"}
                            ]
                        )[-10000:],
                    )
                    child_id = next(iter(ids - {root_id}))
                    child = (await adapter.thread_read(child_id))["thread"]
                    self.assertIs(child["canAcceptDirectInput"], False)
                    result = await adapter.stop_tree(root_id, timeout=15)
                    self.assertEqual(set(result["stopped"]), ids)
                    release_late_response.set()
                    await asyncio.wait_for(streams_closed.wait(), 2)
                    self.assertEqual(await adapter._discover_tree(root_id), ids)
                    for thread_id in ids:
                        status = (await adapter.thread_read(thread_id))["thread"]["status"]["type"]
                        self.assertIn(status, {"idle", "notLoaded"})
                    self.assertIsNone(proc.returncode)
                    self.assertEqual(request_count, 3)
                    history = await rpc.request(
                        "thread/items/list",
                        {
                            "threadId": root_id,
                            "turnId": turn["turn"]["id"],
                            "limit": 100,
                        },
                    )
                    matched = [
                        entry
                        for entry in history["data"]
                        if entry["item"].get("clientId") == "recorded-client-id"
                    ]
                    self.assertEqual(len(matched), 1)
                    self.assertEqual(matched[0]["turnId"], turn["turn"]["id"])
                    empty_id = (
                        await adapter.thread_start(
                            cwd=str(root), model="gpt-5.4", modelProvider="recorded"
                        )
                    )["thread"]["id"]
                    adapter.close()
                    adapter = None
                    await rpc.close()
                    rpc = None
                    proc.terminate()
                    await asyncio.wait_for(proc.wait(), 8)
                    proc = await asyncio.create_subprocess_exec(
                        str(binary),
                        "app-server",
                        "--listen",
                        f"unix://{sock}",
                        cwd=root,
                        env=env,
                        stdout=asyncio.subprocess.DEVNULL,
                        stderr=asyncio.subprocess.DEVNULL,
                    )
                    await asyncio.wait_for(ready(), 10)
                    rpc = await RpcClient.connect_unix(sock)
                    await rpc.initialize()
                    adapter = CodexClient(rpc, owned_root_ids=[root_id, empty_id])
                    # Native 0.153.4 does not persist a thread/start-only session.
                    # Keep this error observable; the adapter must not silently
                    # create a replacement thread during resume/reconciliation.
                    with self.assertRaisesRegex(RpcError, "no rollout found for thread id"):
                        await adapter.thread_resume(empty_id)
                    resumed = await adapter.thread_resume(root_id)
                    self.assertEqual(resumed["thread"]["id"], root_id)
                    self.assertEqual(
                        await adapter.find_turn_by_client_id(root_id, "recorded-client-id"),
                        turn["turn"]["id"],
                    )
                    self.assertEqual(
                        request_count, 3, "A plain resume must not request another model turn"
                    )
                finally:
                    if adapter:
                        adapter.close()
                    if rpc:
                        await rpc.close()
                    if proc.returncode is None:
                        proc.terminate()
                        try:
                            await asyncio.wait_for(proc.wait(), 8)
                        except asyncio.TimeoutError:
                            proc.kill()
                            await proc.wait()
        finally:
            http.close()
            await http.wait_closed()
            for peer in tuple(peers):
                peer.close()
            for task in tuple(handlers):
                task.cancel()
            await asyncio.gather(*tuple(handlers), return_exceptions=True)


if __name__ == "__main__":
    unittest.main()
