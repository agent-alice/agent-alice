"""Host epoch wiring with synthetic transport and real resource persistence.

No Codex process or model is started. One cleanup case owns a sleeping Python
child; the fake transport retains notifications
and swallows listener exceptions like RpcClient, so persistence failures must
explicitly stop the service rather than disappear into transport diagnostics.
"""

from collections import deque
from copy import deepcopy
import asyncio
import inspect
import json
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from alice_codex.codex import CodexClient
from alice_codex.config import RuntimeConfig
from alice_codex.rpc import Event
from alice_codex.service import Service, validate_resource_epoch_journal
import alice_codex.service as service_module


class FakeRpc:
    def __init__(self):
        self.listeners = []
        self.events = deque()
        self.event_sequence = 0
        self.listener_errors = []
        self.connected = True
        self.on_request = None
        self.on_initialize = None

    def add_listener(self, listener):
        self.listeners.append(listener)

        def remove():
            if listener in self.listeners:
                self.listeners.remove(listener)

        return remove

    def publish(self, event):
        event = deepcopy(event)
        self.event_sequence += 1
        self.events.append(Event(self.event_sequence, event, len(json.dumps(event).encode())))
        for listener in tuple(self.listeners):
            try:
                listener(event)
            except Exception as error:
                self.listener_errors.append(type(error).__name__)

    async def initialize(self):
        if self.on_initialize:
            result = self.on_initialize()
            if inspect.isawaitable(result):
                await result
        return {"version": "synthetic"}

    async def request(self, method, params):
        if self.on_request:
            result = self.on_request(method, params)
            return await result if inspect.isawaitable(result) else result
        if method == "thread/start":
            return {"thread": {"id": "new-root", "status": {"type": "idle"}}}
        if method in {"thread/read", "thread/resume"}:
            return {"thread": {"id": params["threadId"], "status": {"type": "idle"}}}
        return {"data": []}

    async def close(self):
        self.connected = False
        self.listeners.clear()

    async def wait_reader_closed(self, *, timeout):
        return True


def token(total=100, *, thread="root", turn="turn", last=None):
    counters = {
        "inputTokens": total,
        "cachedInputTokens": 0,
        "outputTokens": 0,
        "reasoningOutputTokens": 0,
        "totalTokens": total,
    }
    return {
        "method": "thread/tokenUsage/updated",
        "params": {
            "threadId": thread,
            "turnId": turn,
            "tokenUsage": {
                "total": counters,
                "last": {"totalTokens": total if last is None else last},
            },
        },
    }


def started(thread, parent=None):
    value = {"id": thread, "status": {"type": "idle"}}
    if parent is not None:
        value["parentThreadId"] = parent
    return {"method": "thread/started", "params": {"thread": value}}


@pytest.fixture
def host(tmp_path, monkeypatch):
    config = RuntimeConfig(str(tmp_path), "/usr/bin/true", "codex-cli fixture", "unused")
    config.prepare_directories()
    monkeypatch.setattr(config, "environment", lambda: {})
    service = Service(config)
    service.state["tasks"]["main"] = {"thread_id": "root", "paused": False}
    service.save()
    monkeypatch.setattr(service_module, "process_identity", lambda pid: f"fixture-process-{pid}")
    monkeypatch.setattr(service_module, "process_birth", lambda pid: f"fixture-birth-{pid}")
    monkeypatch.setattr(service_module.os, "getpgid", lambda pid: pid)
    yield service
    service.store.close()
    for socket in config.socket_dir.iterdir():
        socket.unlink()
    config.socket_dir.rmdir()


def attach(host, *, rpc=None, roots=("root",), epoch=None):
    if epoch is None:
        epoch = host._prepare_resource_epoch()
        host.process = SimpleNamespace(pid=424242, returncode=None)
        host._bind_resource_epoch(epoch)
    rpc = rpc or FakeRpc()
    codex = CodexClient(rpc, owned_root_ids=list(roots))
    host.rpc, host.codex = rpc, codex
    observer = host._attach_resource_listener(rpc, codex, epoch)
    return epoch, rpc, codex, observer


def epoch_tokens(host, epoch, thread="root"):
    return host.resources.status()["tokens"]["epochs"].get(epoch, {}).get(thread)


def assert_no_tokens(host):
    tokens = host.resources.status()["tokens"]
    assert tokens["threads"] == {} and tokens["epochs"] == {}


@pytest.mark.parametrize(
    "state",
    [
        {},
        {"resource_epochs": {}},
        {"resource_epochs": {"epoch": {"state": "prepared", "server": None}}},
        {"resource_epochs": {"epoch": {"state": "aborted", "server": None}}},
        {
            "resource_epochs": {
                "epoch": {
                    "state": "bound",
                    "server": {"pid": 1, "identity": "fixture", "birth": "fixture"},
                }
            },
            "server": {
                "pid": 1,
                "identity": "fixture",
                "birth": "fixture",
                "resource_epoch_id": "epoch",
            },
            "unrelated_additive_data": {"preserve": True},
        },
    ],
)
def test_shared_journal_validator_accepts_without_mutation_or_io(state, monkeypatch):
    before = deepcopy(state)
    for name in ("Store", "ResourceLedger", "read_json", "write_json"):
        monkeypatch.setattr(
            service_module, name, Mock(side_effect=AssertionError("Unexpected I/O"))
        )
    assert validate_resource_epoch_journal(state) is None
    assert state == before


@pytest.mark.parametrize(
    "state",
    [
        [],
        {"resource_epochs": None},
        {"resource_epochs": []},
        {"resource_epochs": {"": {"state": "prepared", "server": None}}},
        {"resource_epochs": {"epoch": []}},
        {"resource_epochs": {"epoch": {"state": [], "server": None}}},
        {"resource_epochs": {"epoch": {"state": "unknown", "server": None}}},
        {"resource_epochs": {"epoch": {"state": "prepared"}}},
        {"resource_epochs": {"epoch": {"state": "prepared", "server": {}}}},
        {"resource_epochs": {"epoch": {"state": "bound", "server": None}}},
        {
            "resource_epochs": {
                "epoch": {
                    "state": "bound",
                    "server": {"pid": True, "identity": "fixture", "birth": "fixture"},
                }
            }
        },
        {"server": []},
        {"server": {"resource_epoch_id": []}},
        {"server": {"resource_epoch_id": "missing"}},
    ],
)
def test_shared_journal_validator_rejects_corruption_with_value_error(state):
    before = deepcopy(state)
    with pytest.raises(ValueError):
        validate_resource_epoch_journal(state)
    assert state == before


def test_listener_cannot_attach_before_process_binding_is_persisted(host):
    epoch = host._prepare_resource_epoch()
    rpc = FakeRpc()
    codex = CodexClient(rpc, owned_root_ids=["root"])
    with pytest.raises((RuntimeError, ValueError)):
        host._attach_resource_listener(rpc, codex, epoch)
    assert getattr(host, "_resource_observer", None) is None
    assert_no_tokens(host)


async def test_prepare_save_failure_prevents_spawn(host, monkeypatch):
    host.config.verify_binary = Mock()
    host._recover_orphan = AsyncMock()
    save = host.save
    injected = False

    def save_except_prepared():
        nonlocal injected
        if any(
            entry["state"] == "prepared" for entry in host.state.get("resource_epochs", {}).values()
        ):
            injected = True
            raise OSError("synthetic disk full at prepare")
        save()

    host.save = save_except_prepared
    spawn = AsyncMock()
    monkeypatch.setattr(service_module.asyncio, "create_subprocess_exec", spawn)
    with pytest.raises(OSError, match="synthetic disk full"):
        await host.run()
    assert injected
    spawn.assert_not_awaited()
    assert "resource_epochs" not in json.loads(host.path.read_text())
    assert_no_tokens(host)


def test_bind_save_failure_does_not_authorize_an_observer(host):
    epoch = host._prepare_resource_epoch()
    host.process = SimpleNamespace(pid=424242, returncode=None)
    host.save = Mock(side_effect=OSError("synthetic disk full"))
    with pytest.raises(OSError, match="synthetic disk full"):
        host._bind_resource_epoch(epoch)
    rpc = FakeRpc()
    codex = CodexClient(rpc, owned_root_ids=["root"])
    with pytest.raises((RuntimeError, ValueError)):
        host._attach_resource_listener(rpc, codex, epoch)
    assert getattr(host, "_resource_observer", None) is None
    assert_no_tokens(host)


async def test_bind_save_failure_stops_and_waits_for_owned_child(host, monkeypatch):
    host.config.verify_binary = Mock()
    host._recover_orphan = AsyncMock()
    save = host.save
    injected = False

    def save_except_binding():
        nonlocal injected
        if any(
            entry["state"] == "bound" for entry in host.state.get("resource_epochs", {}).values()
        ):
            injected = True
            raise OSError("synthetic disk full at bind")
        save()

    child = await asyncio.create_subprocess_exec(
        sys.executable, "-c", "import time; time.sleep(120)", start_new_session=True
    )
    try:
        host.save = save_except_binding
        monkeypatch.setattr(
            service_module.asyncio, "create_subprocess_exec", AsyncMock(return_value=child)
        )
        connect = AsyncMock()
        monkeypatch.setattr(service_module.RpcClient, "connect_unix", connect)
        with pytest.raises(OSError, match="synthetic disk full at bind"):
            await host.run()
        assert injected and child.returncode is not None
        connect.assert_not_awaited()
        assert host._resource_observer is None
        saved = json.loads(host.path.read_text())
        assert saved["server"] is None and saved["lifecycle"] == "stopped"
        assert list(saved["resource_epochs"].values()) == [{"state": "aborted", "server": None}]
        assert_no_tokens(host)
    finally:
        if child.returncode is None:
            child.kill()
        await child.wait()


def test_unbound_notification_fails_closed_without_legacy_fallback(host):
    host.codex = CodexClient(FakeRpc(), owned_root_ids=["root"])
    host._on_notification(token())
    assert host.stopping and host.stop_event.is_set()
    assert_no_tokens(host)


def test_known_tokens_deduplicate_and_compaction_last_does_not_add_usage(host):
    epoch, rpc, _, _ = attach(host)
    rpc.publish(token(100))
    rpc.publish(token(100))
    rpc.publish(token(100, turn="compaction", last=6844))
    rpc.publish(token(120, turn="next", last=900000))
    recorded = epoch_tokens(host, epoch)
    assert recorded["event_count"] == 3
    assert recorded["first_observed"]["totalTokens"] == 100
    assert recorded["high_water"]["totalTokens"] == 120
    assert recorded["observed_increase_after_first"]["totalTokens"] == 20
    status = host.resources.status()
    assert status["tokens"]["threads"] == {}
    assert status["tokens"]["actual_usage_total"] is None
    assert status["money_receipts"] == {} and not status["virtual_budget_enabled"]


def test_new_connection_to_same_process_reuses_bound_epoch(host):
    epoch, first_rpc, _, _ = attach(host)
    first_rpc.publish(token(100))
    before = deepcopy(host.state["resource_epochs"])
    _, second_rpc, _, observer = attach(host, rpc=FakeRpc(), epoch=epoch)
    second_rpc.publish(token(100))
    second_rpc.publish(token(140, turn="next"))
    assert observer.epoch_id == epoch
    assert host.state["resource_epochs"] == before
    assert list(host.resources.status()["tokens"]["epochs"]) == [epoch]
    assert epoch_tokens(host, epoch)["event_count"] == 2
    assert epoch_tokens(host, epoch)["observed_increase_after_first"]["totalTokens"] == 40


def test_old_callback_retains_original_process_and_ownership_context(host):
    epoch, _, old_codex, observer = attach(host)
    old_callback = observer.receive
    old_callback(token(100))
    host.rpc = FakeRpc()
    host.codex = CodexClient(host.rpc, owned_root_ids=["unrelated-root"])
    host.state["server"] = {
        "pid": 434343,
        "identity": "another-process",
        "birth": "another-birth",
        "resource_epoch_id": "another-epoch",
    }
    old_callback(token(150, turn="tail"))
    assert observer.codex is old_codex and observer.epoch_id == epoch
    assert epoch_tokens(host, epoch)["high_water"]["totalTokens"] == 150
    assert "another-epoch" not in host.resources.status()["tokens"]["epochs"]


def test_early_root_and_child_wait_for_durable_host_confirmation(host):
    epoch, rpc, codex, observer = attach(host)
    rpc.publish(token(40, thread="new-root"))
    rpc.publish(token(60, thread="new-root", turn="later"))
    rpc.publish(started("new-child", "new-root"))
    rpc.publish(token(10, thread="new-child"))
    codex.register_root("new-root")  # Native response alone does not persist the host alias.
    observer.flush()
    assert epoch_tokens(host, epoch, "new-root") is None
    assert epoch_tokens(host, epoch, "new-child") is None
    host.state["tasks"]["new"] = {"thread_id": "new-root", "paused": False}
    host.save()
    observer.confirm_roots(["new-root"])
    observer.flush()
    recorded = epoch_tokens(host, epoch, "new-root")
    assert recorded["first_observed"]["totalTokens"] == 40
    assert recorded["high_water"]["totalTokens"] == 60
    assert recorded["observed_increase_after_first"]["totalTokens"] == 20
    assert epoch_tokens(host, epoch, "new-child")["high_water"]["totalTokens"] == 10


def test_child_token_preceding_trusted_ancestry_is_flushed_on_thread_started(host):
    epoch, rpc, _, _ = attach(host)
    rpc.publish(token(20, thread="child"))
    rpc.publish(token(30, thread="child", turn="later"))
    assert epoch_tokens(host, epoch, "child") is None
    rpc.publish(started("child", "root"))
    recorded = epoch_tokens(host, epoch, "child")
    assert recorded["first_observed"]["totalTokens"] == 20
    assert recorded["high_water"]["totalTokens"] == 30


def test_uncommitted_other_root_does_not_block_a_durable_roots_child(host):
    epoch, rpc, codex, _ = attach(host)
    codex.register_root("uncommitted-other")
    rpc.publish(started("durable-child", "root"))
    rpc.publish(token(20, thread="durable-child"))
    recorded = epoch_tokens(host, epoch, "durable-child")
    assert recorded is not None, "An unrelated uncommitted root cannot hide durable-root usage"
    assert recorded["high_water"]["totalTokens"] == 20
    assert not host.stopping


@pytest.mark.parametrize("save_fails", [False, True])
async def test_ensure_thread_confirms_early_root_only_after_alias_save(
    host, monkeypatch, save_fails
):
    epoch, rpc, _, observer = attach(host)
    host._bootstrap_thread = AsyncMock()

    def start_request(method, params):
        assert method == "thread/start"
        rpc.publish(token(40, thread="new-root"))
        rpc.publish(started("new-child", "new-root"))
        rpc.publish(token(10, thread="new-child"))
        assert epoch_tokens(host, epoch, "new-root") is None
        assert epoch_tokens(host, epoch, "new-child") is None
        return {"thread": {"id": "new-root", "status": {"type": "idle"}}}

    rpc.on_request = start_request
    if save_fails:
        monkeypatch.setattr(host, "save", Mock(side_effect=OSError("alias save failed")))
        with pytest.raises(OSError, match="alias save failed"):
            await host.ensure_thread("new")
        observer.flush()
        rpc.publish(token(60, thread="new-root", turn="later"))
        assert epoch_tokens(host, epoch, "new-root") is None
        assert epoch_tokens(host, epoch, "new-child") is None
    else:
        task = await host.ensure_thread("new")
        assert json.loads(host.path.read_text())["tasks"]["new"]["thread_id"] == task["thread_id"]
        assert epoch_tokens(host, epoch, "new-root")["high_water"]["totalTokens"] == 40
        assert epoch_tokens(host, epoch, "new-child")["high_water"]["totalTokens"] == 10


async def test_startup_discovery_flushes_tokens_whose_ancestry_arrives_in_inventory(host):
    epoch, rpc, _, _ = attach(host)
    rpc.publish(token(20, thread="child"))
    assert epoch_tokens(host, epoch, "child") is None

    def inventory(method, params):
        if method == "thread/list":
            return {"data": [{"id": "child", "parentThreadId": "root"}]}
        return {"data": []}

    rpc.on_request = inventory
    host.journal = service_module.NativeJournal(host.memory, rpc, host.codex.owns)
    await host.archive_native_history("startup")
    assert epoch_tokens(host, epoch, "child")["high_water"]["totalTokens"] == 20


def test_retained_notifications_replay_ancestry_before_scoring_pending_tokens(host):
    rpc = FakeRpc()
    rpc.publish(token(20, thread="child"))
    rpc.publish(started("child", "root"))
    rpc.publish(token(30, thread="child", turn="later"))
    epoch, _, _, _ = attach(host, rpc=rpc)
    recorded = epoch_tokens(host, epoch, "child")
    assert recorded["first_observed"]["totalTokens"] == 20
    assert recorded["high_water"]["totalTokens"] == 30
    assert recorded["event_count"] == 2


def test_retained_notification_gap_stops_startup_instead_of_claiming_complete_capture(host):
    rpc = FakeRpc()
    rpc.publish(token(100))
    rpc.publish(token(120, turn="later"))
    rpc.events.popleft()
    try:
        attach(host, rpc=rpc)
    except (RuntimeError, ValueError):
        pass
    assert host.stopping and host.stop_event.is_set()


def test_resource_persistence_failure_is_not_swallowed_by_transport(host, monkeypatch):
    _, rpc, _, _ = attach(host)
    monkeypatch.setattr(
        host.resources, "record_token_usage", Mock(side_effect=OSError("synthetic disk full"))
    )
    rpc.publish(token())
    assert host.stopping and host.stop_event.is_set()
    assert host.error is not None
    assert_no_tokens(host)


def test_malformed_native_token_envelope_stops_service_without_legacy_write(host):
    _, rpc, _, _ = attach(host)
    rpc.publish({"method": "thread/tokenUsage/updated", "params": []})
    assert host.stopping and host.stop_event.is_set()
    assert_no_tokens(host)


@pytest.mark.parametrize("boundary", ["count", "bytes"])
def test_pending_ownership_buffer_overflow_stops_service(host, monkeypatch, boundary):
    _, rpc, _, observer = attach(host)
    if boundary == "count":
        monkeypatch.setattr(type(observer), "MAX_PENDING", 1)
        rpc.publish(token(10, thread="unknown-a"))
        rpc.publish(token(20, thread="unknown-b"))
    else:
        monkeypatch.setattr(type(observer), "MAX_BYTES", 32)
        rpc.publish(token(10, thread="unknown-a"))
    assert host.stopping and host.stop_event.is_set()
    assert_no_tokens(host)


async def test_unresolved_prepared_generation_prevents_new_process_after_restart(host, monkeypatch):
    pending = host._prepare_resource_epoch()
    restarted = Service(host.config)
    try:
        restarted.config.verify_binary = Mock()
        restarted._recover_orphan = AsyncMock()
        spawn = AsyncMock()
        monkeypatch.setattr(service_module.asyncio, "create_subprocess_exec", spawn)
        with pytest.raises((RuntimeError, ValueError)):
            await restarted.run()
        spawn.assert_not_awaited()
        saved = json.loads(host.path.read_text())
        assert list(saved["resource_epochs"]) == [pending]
        assert saved["resource_epochs"][pending]["state"] == "prepared"
    finally:
        restarted.store.close()


def test_valid_legacy_runtime_and_unscoped_token_rows_remain_separate(host):
    old_tasks = deepcopy(host.state["tasks"])
    assert "resource_epochs" not in json.loads(host.path.read_text())
    host.resources.record_token_usage(token(200)["params"], event_id="legacy-receipt")
    epoch, rpc, _, _ = attach(host)
    rpc.publish(token(50))
    status = host.resources.status()["tokens"]
    assert host.state["tasks"] == old_tasks
    assert status["threads"]["root"]["totalTokens"] == 200
    assert status["epochs"][epoch]["root"]["high_water"]["totalTokens"] == 50
    assert status["legacy_unscoped"] is True and status["actual_usage_total"] is None


async def test_run_persists_prepared_and_bound_identity_before_initialize_notifications(
    host, monkeypatch
):
    host.config.verify_binary = Mock()
    host._recover_orphan = AsyncMock()
    process = SimpleNamespace(pid=424242, returncode=None)
    rpc = FakeRpc()
    witnessed = {}

    async def spawn(*args, **kwargs):
        saved = json.loads(host.path.read_text())
        prepared = [
            key for key, value in saved["resource_epochs"].items() if value["state"] == "prepared"
        ]
        assert len(prepared) == 1 and saved["server"] is None
        witnessed["epoch"] = prepared[0]
        host.config.codex_socket.touch()
        return process

    def initialize():
        epoch = witnessed["epoch"]
        saved = json.loads(host.path.read_text())
        assert saved["server"]["resource_epoch_id"] == epoch
        assert saved["resource_epochs"][epoch]["state"] == "bound"
        assert saved["resource_epochs"][epoch]["server"]["pid"] == process.pid
        assert host._resource_observer.epoch_id == epoch
        rpc.publish(token(100))
        assert epoch_tokens(host, epoch)["high_water"]["totalTokens"] == 100

    rpc.on_initialize = initialize
    monkeypatch.setattr(service_module.asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setattr(service_module.RpcClient, "connect_unix", AsyncMock(return_value=rpc))
    host.archive_native_history = AsyncMock()

    async def stop(*args, **kwargs):
        rpc.publish(token(130, turn="shutdown-tail"))
        process.returncode = 0

    host._stop_task = stop

    class ControlServer:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

    async def listen(*args, **kwargs):
        host.config.control_socket.touch()
        host.stop_event.set()
        return ControlServer()

    monkeypatch.setattr(service_module.asyncio, "start_unix_server", listen)
    await host.run()
    epoch = witnessed["epoch"]
    assert epoch_tokens(host, epoch)["high_water"]["totalTokens"] == 130
    assert epoch_tokens(host, epoch)["observed_increase_after_first"]["totalTokens"] == 30
    assert host.resources.status()["tokens"]["threads"] == {}
    assert rpc.connected is False
