"""Stable, bounded process startup and verified fallback; never a model loop."""

import argparse
import asyncio
from contextlib import suppress
import os
from pathlib import Path
import signal
import sys
import time

from .bootstrap import checked_runtime
from .config import load_config
from .control import ControlError, request
from .files import SingletonLock, read_json, write_json
from .releases import ReleaseError, ReleaseManager
from .service import Service, process_birth, process_identity

BOOTSTRAP_PROTOCOL = 1


class Supervisor:
    def __init__(
        self,
        config,
        *,
        startup_timeout: float = 30,
        max_failures: int = 2,
        healthy_seconds: float = 60,
        stop_timeout: float = 40,
        retry_delay: float = 1,
    ):
        if min(startup_timeout, max_failures, healthy_seconds, stop_timeout) <= 0:
            raise ValueError("supervisor bounds must be positive")
        self.config = config
        self.manager = ReleaseManager(config.root)
        self.path = config.root / "state/bootstrap-state.json"
        self.startup_timeout, self.max_failures = startup_timeout, max_failures
        self.healthy_seconds, self.stop_timeout = healthy_seconds, stop_timeout
        self.retry_delay = retry_delay
        self.stop_event = asyncio.Event()
        self.process = None
        self.state = (
            read_json(self.path)
            if self.path.exists()
            else {
                "version": 1,
                "activation_epoch": None,
                "attempts": {},
                "failed": [],
                "child": None,
            }
        )
        if (
            not isinstance(self.state, dict)
            or self.state.get("version") != 1
            or not isinstance(self.state.get("attempts"), dict)
            or not isinstance(self.state.get("failed"), list)
        ):
            raise ValueError("Invalid supervisor state; preserved for recovery")

    def save(self, lifecycle: str, **values) -> None:
        self.state.update(lifecycle=lifecycle, observed_at=time.time(), **values)
        write_json(self.path, self.state)

    async def status(self):
        try:
            return await request(self.config.control_socket, "status", timeout=0.5)
        except (OSError, TimeoutError, ControlError):
            # This is a read-only health probe. EOF does not authorize retrying input.
            return None

    async def delay(self, seconds: float) -> None:
        with suppress(TimeoutError):
            await asyncio.wait_for(self.stop_event.wait(), seconds)

    def _same_child(self, child: dict) -> bool:
        return process_birth(child["pid"]) == child["birth"]

    async def terminate_child(self) -> None:
        """Only the exact child identity recorded by this supervisor is signalable."""
        child = self.state.get("child")
        if not child or not self._same_child(child):
            if self.process is not None:
                await self.process.wait()
            self.state["child"] = None
            return
        pid = child["pid"]
        identity = process_identity(pid) or ""
        if (
            os.getpgid(pid) != pid
            or str(self.config.root) not in identity
            or "alice_codex" not in identity
        ):
            raise RuntimeError("Recorded daemon identity changed; refusing to signal")
        # First allow normal journal draining and native shutdown. A status probe
        # must name the same daemon; never send shutdown to an unrelated service.
        state = await self.status()
        if state and state.get("pid") == pid:
            with suppress(OSError, TimeoutError, ControlError):
                await request(self.config.control_socket, "shutdown", timeout=2)
        if self._same_child(child):
            os.kill(pid, signal.SIGTERM)
        deadline = time.monotonic() + self.stop_timeout
        while self._same_child(child) and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        if self._same_child(child):
            if os.getpgid(pid) != pid:
                raise RuntimeError("Recorded daemon process group changed")
            os.kill(pid, signal.SIGKILL)
        deadline = time.monotonic() + 5
        while self._same_child(child) and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        if self._same_child(child):
            raise RuntimeError("Owned daemon did not exit")
        if self.process is not None:
            await self.process.wait()
        self.state["child"] = None

    async def clean_orphan(self) -> None:
        """Reuse the Service's birth/socket/group checks, while no daemon owns its lock."""
        with SingletonLock(self.config.root / "state/service.lock"):
            service = Service(self.config)
            try:
                service.store.set_autonomy_paused(True)
                await service._recover_orphan()
                service.state["server"] = None
                service.state["lifecycle"] = "stopped"
                service.save()
            finally:
                service.store.close()

    def launch_command(self, pointer: dict) -> list[str]:
        return [pointer["python"], "-I", "-m", "alice_codex", "--home", self.config.home, "serve"]

    async def attempt(self, pointer: dict) -> str:
        candidate = pointer["current"]
        self.config.write_codex_config(python=pointer["python"])
        self.save("starting", candidate=candidate)
        self.process = await asyncio.create_subprocess_exec(
            *self.launch_command(pointer),
            cwd=self.config.workspace,
            env=self.config.environment(),
            stdin=asyncio.subprocess.DEVNULL,
            start_new_session=True,
        )
        born = process_birth(self.process.pid)
        if born:
            self.state["child"] = {
                "pid": self.process.pid,
                "birth": born,
                "identity": process_identity(self.process.pid),
            }
        self.save("starting")
        deadline = time.monotonic() + self.startup_timeout
        ready_at, reset = None, False
        while not self.stop_event.is_set():
            if self.process.returncode is not None:
                return "stopped" if self.process.returncode == 0 else "failed"
            now = time.monotonic()
            if ready_at is None:
                observed = await self.status()
                if observed and observed.get("ready"):
                    if observed.get("pid") != self.process.pid:
                        raise RuntimeError("Health socket belongs to another service")
                    ready_at = now
                    self.save("running", ready_at=time.time())
                elif now >= deadline:
                    self.save("startup_timeout", error="candidate readiness deadline exceeded")
                    return "failed"
            elif not reset and now - ready_at >= self.healthy_seconds:
                # A later, isolated crash does not inherit ancient startup failures.
                self.state["attempts"][candidate] = 0
                self.save("healthy")
                reset = True
            await self.delay(0.1)
        return "stopped"

    def fallback(self, candidate: str, error: str) -> bool:
        failed = set(self.state["failed"]) | {candidate}
        self.state["failed"] = sorted(failed)
        self.save("candidate_failed", error=error)
        try:
            pointer = self.manager.automatic_rollback(
                candidate,
                failed,
                expected_epoch=self.state["activation_epoch"],
            )
        except ReleaseError as failure:
            self.save("blocked", error=str(failure), candidate_error=error)
            return False
        self.save("rolled_back", candidate=pointer["current"], rejected_candidate=candidate)
        return True

    async def run(self) -> int:
        # The supervisor lock is distinct from Service's lock and held across
        # child lifetimes. The service lock is held only between child processes.
        with SingletonLock(self.config.root / "state/bootstrap.lock"):
            await self.terminate_child()
            await self.clean_orphan()
            while not self.stop_event.is_set():
                raw = self.manager.current()
                if raw is None:
                    self.save("blocked", error="no active candidate")
                    return 0
                candidate = raw["current"]
                epoch = raw.get("activation_epoch", "legacy:" + candidate)
                if self.state["activation_epoch"] != epoch:
                    self.state.update(activation_epoch=epoch, attempts={}, failed=[])
                    self.save("activation_observed")
                if candidate in self.state["failed"]:
                    self.save("blocked", error="candidate already failed in this activation")
                    return 0
                try:
                    pointer = self.manager.checked_current()
                except (ReleaseError, OSError, ValueError) as error:
                    # Hash/schema failures are deterministic; do not execute them twice.
                    if not self.fallback(candidate, str(error)):
                        return 0
                    continue
                count = self.state["attempts"].get(candidate, 0)
                if count >= self.max_failures:
                    if not self.fallback(candidate, "candidate exceeded bounded startup failures"):
                        return 0
                    continue
                self.state["attempts"][candidate] = count + 1
                self.save("attempt_recorded", candidate=candidate)
                try:
                    outcome = await self.attempt(pointer)
                except (OSError, TimeoutError) as error:
                    self.save("attempt_error", error=str(error))
                    outcome = "failed"
                finally:
                    # Complete cleanup before switching code or starting another copy.
                    await self.terminate_child()
                    await self.clean_orphan()
                if outcome == "stopped":
                    self.save("stopped", error=None)
                    return 0
                self.save("retry_wait", error="candidate exited unsuccessfully")
                await self.delay(self.retry_delay)
            self.save("stopped", error=None)
            return 0


async def _main(home: Path) -> int:
    # This independent package must match the installed bootstrap pointer, even
    # when the current candidate is broken. No current-candidate import is used.
    runtime = checked_runtime(home)
    if Path(sys.executable).absolute() != Path(runtime["python"]).absolute():
        raise ReleaseError("supervisor must run from its independent bootstrap environment")
    config = load_config(home)
    manager = ReleaseManager(home)
    manager._check_data_schema(runtime["manifest"], None)
    supervisor = Supervisor(config)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, supervisor.stop_event.set)
    return await supervisor.run()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--home", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        return asyncio.run(_main(args.home))
    except (Exception, KeyboardInterrupt) as error:
        # A startup-control failure must not become launchd's infinite crash loop.
        with suppress(Exception):
            write_json(
                args.home / "state/bootstrap-failure.json",
                {
                    "observed_at": time.time(),
                    "error": f"{type(error).__name__}: {error}",
                },
            )
        print(f"alice supervisor stopped safely: {type(error).__name__}: {error}", file=sys.stderr)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
