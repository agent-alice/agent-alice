"""Private local control socket. Codex itself uses the separate WebSocket RPC."""

import asyncio
import json
from pathlib import Path

MAX_MESSAGE = 2 * 1024 * 1024


class ControlError(RuntimeError):
    pass


async def request(
    socket: Path, action: str, params: dict | None = None, *, timeout: float = 60
) -> dict:
    async def exchange():
        reader, writer = await asyncio.open_unix_connection(str(socket), limit=MAX_MESSAGE)
        try:
            payload = (
                json.dumps({"action": action, "params": params or {}}, ensure_ascii=False).encode()
                + b"\n"
            )
            if len(payload) > MAX_MESSAGE:
                raise ControlError("Request exceeds local control limit")
            writer.write(payload)
            await writer.drain()
            raw = await reader.readline()
            if not raw:
                raise ControlError("Service disconnected; outcome may be unknown")
            response = json.loads(raw)
            if not response.get("ok"):
                raise ControlError(response.get("error", "Control request failed"))
            return response["result"]
        finally:
            writer.close()
            await writer.wait_closed()

    return await asyncio.wait_for(exchange(), timeout)
