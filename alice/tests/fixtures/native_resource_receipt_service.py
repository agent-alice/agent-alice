"""Observe the installed service's token frames without changing their delivery.

Run with the selected installed Python, ``-I``, this script and the Alice home.
ALICE_NATIVE_RESOURCE_RECEIPTS must name a private, run-specific JSONL file.
The source file is synthetic instrumentation; captured native data stays outside Git.
"""

import asyncio
import hashlib
import json
import os
from pathlib import Path
import sys
from weakref import WeakKeyDictionary

from alice_codex.config import load_config
from alice_codex.service import _ResourceEpochListener, serve


def canonical(value):
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )


def main():
    config = load_config(Path(sys.argv[1]))
    destination = Path(os.environ["ALICE_NATIVE_RESOURCE_RECEIPTS"])
    descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    original_receive = _ResourceEpochListener.receive
    original_close = _ResourceEpochListener.close
    observed = WeakKeyDictionary()

    def state(listener):
        return observed.setdefault(listener, {"token_frames": 0, "write_failures": 0})

    def append(listener, record):
        # Instrumentation must not replace the service's exception semantics.
        # Any short/failed write is exposed at close and rejected by the judge.
        try:
            data = (canonical(record) + "\n").encode("utf-8")
            if os.write(descriptor, data) != len(data):
                state(listener)["write_failures"] += 1
        except (OSError, ValueError, TypeError):
            state(listener)["write_failures"] += 1

    def observed_receive(self, event):
        if event.get("method") == "thread/tokenUsage/updated":
            state(self)["token_frames"] += 1
            append(self, {"kind": "token", "epoch_id": self.epoch_id, "event": event})
        return original_receive(self, event)

    async def observed_close(self):
        try:
            return await original_close(self)
        finally:
            try:
                append(self, {
                    "kind": "close",
                    "epoch_id": self.epoch_id,
                    **state(self),
                "pending_count": self.pending_count,
                "observer_error": self.error,
                    "unresolved": [
                        {
                            "thread_id": params.get("threadId"),
                            "payload_sha256": hashlib.sha256(
                                canonical(params).encode("utf-8")
                            ).hexdigest(),
                        }
                        for params, _ in self._pending
                    ],
                })
            except Exception:
                # Missing close evidence is a judge failure, never a reason to
                # replace a return or exception from the installed close().
                pass

    _ResourceEpochListener.receive = observed_receive
    _ResourceEpochListener.close = observed_close
    try:
        asyncio.run(serve(config))
    finally:
        os.close(descriptor)


if __name__ == "__main__":
    main()
