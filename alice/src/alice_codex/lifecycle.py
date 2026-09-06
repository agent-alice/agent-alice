"""Serialize offline maintenance with startup, shutdown and process ownership."""

import asyncio
from contextlib import contextmanager

from .control import request
from .files import SingletonLock


@contextmanager
def offline_maintenance(config):
    """Hold lifecycle -> bootstrap -> service locks for the entire operation.

    This synchronous context must run off an asyncio event loop. It never stops
    a service: loaded supervision, live recorded owners and uncertain sockets
    must be resolved by an explicit stop before maintenance. Callers must not
    reacquire these locks or invoke public launchd transitions inside the yield.
    """
    from .launchd import _assert_owned_stopped, status as supervisor_status

    state = config.root / "state"
    paths = [state / name for name in ("lifecycle.lock", "bootstrap.lock", "service.lock")]
    if state.is_symlink() or any(path.is_symlink() for path in paths):
        raise ValueError("Offline maintenance lock paths must not be symbolic links")
    with SingletonLock(paths[0]), SingletonLock(paths[1]), SingletonLock(paths[2]):
        if (state / "supervisor.json").exists() and supervisor_status(config)["loaded"]:
            raise ValueError("Stop the installed supervisor before offline maintenance")
        _assert_owned_stopped(config)
        if config.control_socket.exists():
            try:
                asyncio.run(request(config.control_socket, "status", timeout=2))
            except (OSError, TimeoutError) as error:
                raise RuntimeError("Alice control state is uncertain; stop/diagnose first") from error
            raise ValueError("Stop Alice before offline maintenance")
        if config.codex_socket.exists():
            raise RuntimeError("Alice native socket remains; stop/diagnose first")
        yield
