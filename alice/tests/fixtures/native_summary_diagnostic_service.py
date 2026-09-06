"""Private diagnostic wrapper; delegates every operation to the selected runtime.

This records fail-closed causes without changing admission, limits, timing waits,
native ancestry or error handling. It is not a release acceptance entry point.
"""

import asyncio
from collections import Counter
import json
from pathlib import Path
import sys
import time
import traceback

from alice_codex.config import load_config
from alice_codex.service import Service, _ResourceEpochListener, serve


def main():
    config = load_config(Path(sys.argv[1]))
    destination = Path(sys.argv[2])
    original_fail = Service._fail
    original_receive = _ResourceEpochListener.receive

    def observed_receive(self, event):
        self._diagnostic_event = {
            "method": event.get("method"),
            "thread_id": event.get("params", {}).get("threadId"),
        }
        return original_receive(self, event)

    def observed_fail(self, message):
        try:
            listener = self._resource_observer
            pending = listener._pending if listener else []
            record = {
                "time": time.time(), "message": message,
                "pending_count": len(pending),
                "pending_bytes": listener._bytes if listener else None,
                "pending_threads": dict(Counter(p.get("threadId") for p, _ in pending)),
                "distinct_pending": len({json.dumps(p, sort_keys=True) for p, _ in pending}),
                "last_resource_event": getattr(listener, "_diagnostic_event", None),
                "known_parents": self.codex._parents if self.codex else {},
                "durable_roots": sorted(listener._roots) if listener else [],
                "stack": traceback.format_stack(),
            }
            with destination.open("a") as output:
                output.write(json.dumps(record, sort_keys=True) + "\n")
        except Exception:
            # Diagnostics must never replace the original failure semantics.
            pass
        return original_fail(self, message)

    _ResourceEpochListener.receive = observed_receive
    Service._fail = observed_fail
    asyncio.run(serve(config))


if __name__ == "__main__":
    main()
