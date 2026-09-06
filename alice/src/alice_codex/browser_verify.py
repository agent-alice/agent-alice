"""Validate a browser MCP using an owned native server and synthetic local pages."""

import asyncio
import hashlib
import os
from pathlib import Path
import re
import signal
import tempfile
from uuid import uuid4

import tomlkit

from .codex import CodexClient
from .rpc import RpcClient


async def _processes():
    process = await asyncio.create_subprocess_exec(
        "ps",
        "-axo",
        "pid=,ppid=,pgid=,stat=,lstart=",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    output, _ = await asyncio.wait_for(process.communicate(), 5)
    if process.returncode:
        raise RuntimeError("Cannot inspect owned browser process identities")
    result = {}
    for line in output.decode().splitlines():
        parts = line.split()
        if len(parts) >= 9:
            result[int(parts[0])] = (int(parts[1]), int(parts[2]), parts[3], " ".join(parts[4:]))
    return result


def verify(codex: Path, settings: dict) -> dict:
    """No inference or account access; callers invoke this synchronous API off-loop."""
    return asyncio.run(_verify(codex, settings))


async def _verify(codex, settings):
    marker = "alice-browser-" + uuid4().hex
    record = {"status": "failed", "model_calls": 0, "snapshots": [], "requests": []}
    owned, peers = {}, set()
    process = rpc = client = None
    identity = None
    closed = False

    async def page(reader, writer):
        peers.add(writer)
        try:
            header = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 5)
            route = header.split(b" ", 2)[1].decode()
            record["requests"].append(route)
            content = (
                f"<title>Owned browser fixture</title><h1>{marker}</h1>"
                "<p>Known count: 0</p><p>Missing count: unknown</p>"
                '<a href="/page2">Second page</a>'
                if route == "/"
                else f"<title>Owned second page</title><h1>{marker}-second</h1><p>Count: 11</p>"
            ).encode()
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n"
                b"Connection: close\r\nContent-Length: "
                + str(len(content)).encode()
                + b"\r\n\r\n"
                + content
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
            peers.discard(writer)

    async def remember():
        snapshot = await _processes()
        found = {process.pid}
        while True:
            expanded = found | {
                pid for pid, item in snapshot.items() if item[0] in found or item[1] == process.pid
            }
            if expanded == found:
                break
            found = expanded
        for pid in found & snapshot.keys():
            owned[pid] = snapshot[pid][3]
        return snapshot

    http = await asyncio.start_server(page, "127.0.0.1", 0)
    url = f"http://127.0.0.1:{http.sockets[0].getsockname()[1]}/"
    with tempfile.TemporaryDirectory(prefix="alice-browser-", dir="/tmp") as temporary:
        root = Path(temporary)
        for name in ("home", "codex", "workspace"):
            (root / name).mkdir(mode=0o700)
        document = {
            "check_for_update_on_startup": False,
            "features": {"apps": False, "plugins": False},
            "mcp_servers": {"alice_browser": settings},
        }
        (root / "codex/config.toml").write_text(tomlkit.dumps(document))
        environment = {
            "HOME": str(root / "home"),
            "CODEX_HOME": str(root / "codex"),
            "PATH": "/usr/bin:/bin",
            "RUST_LOG": "error",
        }
        socket = root / "rpc.sock"
        try:
            async with asyncio.timeout(60):
                process = await asyncio.create_subprocess_exec(
                    str(codex),
                    "app-server",
                    "--listen",
                    f"unix://{socket}",
                    cwd=root / "workspace",
                    env=environment,
                    start_new_session=True,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                while not socket.exists():
                    if process.returncode is not None:
                        raise RuntimeError("Owned browser verification server exited before ready")
                    await asyncio.sleep(0.03)
                rpc = await RpcClient.connect_unix(socket)
                await rpc.initialize(name="alice_browser_verification")
                client = CodexClient(rpc)
                thread = await client.thread_start(
                    cwd=str(root / "workspace"), approvalPolicy="never", sandbox="read-only"
                )
                identity = thread["thread"]["id"]
                catalog = await rpc.request("mcpServerStatus/list", {"threadId": identity})
                servers = catalog["data"]
                if (
                    len(servers) != 1
                    or servers[0]["name"] != "alice_browser"
                    or servers[0].get("runtimeStatus") != "connected"
                    or servers[0].get("toolsError")
                    or set(servers[0]["tools"]) != set(settings["enabled_tools"])
                ):
                    raise RuntimeError(
                        "Browser tool discovery does not match the installed allowlist"
                    )
                record["tools"] = sorted(servers[0]["tools"])

                async def call(tool, arguments):
                    value = await rpc.request(
                        "mcpServer/tool/call",
                        {
                            "threadId": identity,
                            "server": "alice_browser",
                            "tool": tool,
                            "arguments": arguments,
                        },
                        timeout=20,
                    )
                    await remember()
                    if value.get("isError"):
                        record["tool_error"] = {
                            "tool": tool,
                            "text": "\n".join(
                                item.get("text", "") for item in value.get("content", [])
                            )[:3000],
                        }
                        raise RuntimeError(f"Native browser tool failed: {tool}")
                    return "\n".join(item.get("text", "") for item in value.get("content", []))

                def snapshot(value):
                    match = re.search(r"\[Snapshot\]\(([^)]+)\)", value)
                    if not match:
                        raise RuntimeError("Native browser did not return a snapshot file")
                    output = Path(settings["args"][settings["args"].index("--output-dir") + 1])
                    path = output / Path(match.group(1)).name
                    if path.is_symlink() or not path.is_file() or path.stat().st_size > 65536:
                        raise RuntimeError("Invalid browser snapshot artifact")
                    data = path.read_bytes()
                    record["snapshots"].append(
                        {"sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}
                    )
                    return data.decode()

                first = snapshot(await call("browser_navigate", {"url": url}))
                link = re.search(r'link "Second page" \[ref=([^]]+)\]', first)
                if not (
                    marker in first
                    and "Known count: 0" in first
                    and "Missing count: unknown" in first
                    and link
                ):
                    raise RuntimeError("First browser snapshot failed fixture validation")
                second = snapshot(
                    await call("browser_click", {"target": link[1], "element": "Owned second page"})
                )
                if marker + "-second" not in second or "Count: 11" not in second:
                    raise RuntimeError("Second browser snapshot failed fixture validation")
                traffic = await call("browser_network_requests", {"static": True})
                urls = re.findall(r"\[GET\] (\S+) =>", traffic)
                if urls != [url, url + "page2"] or "/page2" not in record["requests"]:
                    raise RuntimeError("Browser page traffic failed localhost validation")
                await call("browser_close", {})
                closed = True
                record["status"] = "passed"
        except Exception as error:
            record["error_type"] = type(error).__name__
            record["error"] = str(error)
        finally:
            cleanup_errors = []
            if process:
                try:
                    await remember()
                except Exception:
                    cleanup_errors.append("process_inventory")
            if rpc and client and rpc.connected:
                if identity and not closed:
                    try:
                        await rpc.request(
                            "mcpServer/tool/call",
                            {
                                "threadId": identity,
                                "server": "alice_browser",
                                "tool": "browser_close",
                                "arguments": {},
                            },
                            timeout=5,
                        )
                    except Exception:
                        cleanup_errors.append("browser_close")
                for owned_root in client.owned_root_ids:
                    try:
                        await client.stop_tree(owned_root, timeout=5)
                    except Exception:
                        cleanup_errors.append("stop_tree")
            if process and process.returncode is None:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    await asyncio.wait_for(process.wait(), 8)
                except asyncio.TimeoutError:
                    os.killpg(process.pid, signal.SIGKILL)
                    await process.wait()
            if client:
                client.close()
            if rpc:
                await rpc.close()
            try:
                state = await _processes()
            except Exception:
                state = {}
                cleanup_errors.append("process_inventory_after_exit")
            remaining = [
                pid
                for pid, birth in owned.items()
                if pid in state and state[pid][3] == birth and not state[pid][2].startswith("Z")
            ]
            for pid in remaining:
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            for _ in range(40):
                try:
                    state = await _processes()
                except Exception:
                    cleanup_errors.append("process_inventory_after_cleanup")
                    break
                remaining = [
                    pid
                    for pid in remaining
                    if pid in state
                    and state[pid][3] == owned[pid]
                    and not state[pid][2].startswith("Z")
                ]
                if not remaining:
                    break
                await asyncio.sleep(0.05)
            for peer in tuple(peers):
                peer.close()
            http.close()
            await http.wait_closed()
            record.update(
                owned_processes=sorted(owned),
                remaining_owned_processes=remaining,
                cleanup_errors=cleanup_errors,
                server_exit_code=process.returncode if process else None,
            )
            if remaining or cleanup_errors or record["server_exit_code"] != 0:
                record["status"] = "failed"
    return record
