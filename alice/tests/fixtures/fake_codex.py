#!/usr/bin/env python3
"""Deterministic App Server fixture; never imports or calls a model provider.

It uses actual Unix WebSockets and durable private state. Its one scripted tool
effect appends evidence in the caller's temporary workspace. This validates the
Alice process/protocol path, and is deliberately not labelled native Codex.
"""

import asyncio
from contextlib import suppress
import hashlib
import json
import os
from pathlib import Path
import signal
import sys
import tomllib
from uuid import uuid4


class FakeServer:
    def __init__(self, home):
        self.path = home / "fixture-state.json"
        home.mkdir(parents=True, exist_ok=True)
        self.state = json.loads(self.path.read_text()) if self.path.exists() else {"threads": {}}
        self.peers = set()
        self.tasks = set()
        for thread in self.state["threads"].values():
            thread["status"] = {"type": "notLoaded"}
        self.save()

    def save(self):
        temporary = self.path.with_suffix(".tmp")
        temporary.write_text(json.dumps(self.state))
        temporary.replace(self.path)

    async def notify(self, method, params):
        for peer in list(self.peers):
            with suppress(Exception):
                await peer.send(json.dumps({"jsonrpc": "2.0", "method": method, "params": params}))

    def spawn(self, awaitable):
        task = asyncio.create_task(awaitable)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def finish(self, thread, turn, text):
        await self.notify("turn/started", {"threadId": thread["id"], "turn": turn})
        await self.notify(
            "item/completed",
            {"threadId": thread["id"], "turnId": turn["id"], "item": turn["items"][0]},
        )
        evidence = Path(thread["cwd"]) / "fixture-evidence.jsonl"
        with evidence.open("a") as stream:
            stream.write(
                json.dumps(
                    {
                        "thread_id": thread["id"],
                        "turn_id": turn["id"],
                        "client_id": turn["items"][0]["clientId"],
                        "text": text,
                    }
                )
                + "\n"
            )
            stream.flush()
            os.fsync(stream.fileno())
        assistant = {
            "type": "agentMessage",
            "id": str(uuid4()),
            "text": "Fixture wrote external evidence.",
        }
        turn["items"].append(assistant)
        turn["status"] = "completed"
        thread["status"] = {"type": "idle"}
        self.save()
        await self.notify(
            "item/completed", {"threadId": thread["id"], "turnId": turn["id"], "item": assistant}
        )
        await self.notify("turn/completed", {"threadId": thread["id"], "turn": turn})

    async def call(self, method, params):
        if method == "initialize":
            return {"userAgent": "alice-fixture/0", "version": "fixture"}
        if method == "hooks/list":
            config_path = self.path.parent / "config.toml"
            hooks = tomllib.loads(config_path.read_text()).get("hooks", {})
            values = []
            for event, groups in hooks.items():
                if event == "state":
                    continue
                for i, group in enumerate(groups):
                    for j, handler in enumerate(group["hooks"]):
                        key = f"{config_path}:{event}:{i}:{j}"
                        current_hash = "sha256:" + hashlib.sha256(
                            json.dumps(handler, sort_keys=True).encode()
                        ).hexdigest()
                        trusted = hooks.get("state", {}).get(key, {}).get("trusted_hash") == current_hash
                        values.append({
                            "key": key, "eventName": event[0].lower() + event[1:],
                            "handlerType": "command", "command": handler["command"],
                            "async": handler.get("async", False), "matcher": group.get("matcher"),
                            "timeoutSec": handler.get("timeout"),
                            "statusMessage": handler.get("statusMessage"),
                            "additionalContextLimit": handler.get("additionalContextLimit"),
                            "sourcePath": str(config_path), "source": "user", "enabled": True,
                            "currentHash": current_hash, "trustStatus": "trusted" if trusted else "untrusted",
                        })
            return {"data": [{"cwd": cwd, "hooks": values, "warnings": [], "errors": []}
                             for cwd in params["cwds"]]}
        if method == "thread/start":
            thread = {
                "id": str(uuid4()),
                "cwd": params["cwd"],
                "status": {"type": "idle"},
                "turns": [],
                "source": "appServer",
                "canAcceptDirectInput": True,
            }
            self.state["threads"][thread["id"]] = thread
            self.save()
            return {"thread": thread}
        if method == "thread/list":
            return {"data": list(self.state["threads"].values()), "nextCursor": None}
        if method == "thread/loaded/list":
            return {
                "data": [
                    key
                    for key, row in self.state["threads"].items()
                    if row["status"]["type"] != "notLoaded"
                ],
                "nextCursor": None,
            }
        thread = self.state["threads"][params["threadId"]]
        if method == "thread/inject_items":
            thread.setdefault("injected_items", []).extend(params["items"])
            self.save()
            return {}
        if method == "thread/resume":
            thread["status"] = {"type": "idle"}
            self.save()
            return {"thread": thread}
        if method == "thread/read":
            return {"thread": thread}
        if method == "thread/turns/list":
            return {"data": list(reversed(thread["turns"])), "nextCursor": None}
        if method == "thread/goal/get":
            return {"goal": None}
        if method == "thread/backgroundTerminals/list":
            return {"data": [], "nextCursor": None}
        if method in {"thread/backgroundTerminals/clean", "thread/queue/start"}:
            return {}
        if method == "thread/queue/list":
            return {"data": [], "nextCursor": None}
        if method in {"turn/start", "thread/queue/add"}:
            text = "\n".join(part["text"] for part in params["input"] if part["type"] == "text")
            turn = {
                "id": str(uuid4()),
                "status": "inProgress",
                "error": None,
                "items": [
                    {
                        "type": "userMessage",
                        "id": str(uuid4()),
                        "clientId": params["clientUserMessageId"],
                        "content": params["input"],
                    }
                ],
            }
            thread["turns"].append(turn)
            thread["status"] = {"type": "active"}
            self.save()
            # Event completion may arrive before the caller finishes persisting its ack.
            self.spawn(self.finish(thread, turn, text))
            return {"turn": turn} if method == "turn/start" else {"queuedSubmissionId": turn["id"]}
        if method == "turn/interrupt":
            for turn in thread["turns"]:
                if turn["id"] == params["turnId"]:
                    turn["status"] = "interrupted"
            thread["status"] = {"type": "idle"}
            self.save()
            return {}
        raise ValueError(f"Unsupported fixture RPC: {method}")

    async def connection(self, peer):
        self.peers.add(peer)
        try:
            async for raw in peer:
                message = json.loads(raw)
                if "id" not in message:
                    continue
                try:
                    result = await self.call(message["method"], message.get("params", {}))
                    response = {"jsonrpc": "2.0", "id": message["id"], "result": result}
                except Exception as error:
                    response = {
                        "jsonrpc": "2.0",
                        "id": message["id"],
                        "error": {"code": -32602, "message": str(error)},
                    }
                await peer.send(json.dumps(response))
        finally:
            self.peers.discard(peer)


async def serve(socket):
    from websockets.asyncio.server import unix_serve

    server = FakeServer(Path(os.environ["CODEX_HOME"]))
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stopped.set)
    async with unix_serve(server.connection, socket):
        await stopped.wait()
    for task in list(server.tasks):
        task.cancel()
    await asyncio.gather(*server.tasks, return_exceptions=True)


if __name__ == "__main__":
    if "--version" in sys.argv:
        print("codex-cli fixture-0.153.4")
    elif "features" in sys.argv:
        print("goals\texperimental\ttrue")
    elif "app-server" in sys.argv:
        endpoint = sys.argv[sys.argv.index("--listen") + 1]
        if not endpoint.startswith("unix://"):
            raise SystemExit("fixture requires a private Unix socket")
        asyncio.run(serve(endpoint.removeprefix("unix://")))
    else:
        raise SystemExit("unsupported fixture invocation")
