"""Synthetic RPC faults for durable empty-root initialization; no model calls."""

import asyncio
from copy import deepcopy
import json
from unittest.mock import AsyncMock, Mock

import pytest

from alice_codex.codex import CodexClient
from alice_codex.config import RuntimeConfig
from alice_codex.rpc import RpcError, RpcTimeout
from alice_codex.service import Service


class BootstrapRuntime:
    """A temporary host paired with a fake native rollout boundary."""

    def __init__(self, config):
        self.config = config
        self.instances = []
        self.threads = {}
        self.persisted = set()
        self.calls = []
        self.resume_error = None
        self.read_error = None
        self.inject_error = None
        self.persist_on_inject = True
        self.returned_resume_id = None
        self.service = self.restart()

    def restart(self):
        service = Service(self.config)
        rpc = Mock()
        rpc.add_listener.return_value = lambda: None
        rpc.request = AsyncMock(side_effect=self.request)
        service.rpc = rpc
        service.codex = CodexClient(
            rpc, owned_root_ids=[task["thread_id"] for task in service.state["tasks"].values()]
        )
        service.ready = True
        service.submit = AsyncMock(side_effect=AssertionError("bootstrap must not submit input"))
        self.instances.append(service)
        self.service = service
        return service

    @property
    def disk(self):
        return json.loads(self.service.path.read_text()) if self.service.path.exists() else None

    def seed(self, *, state=None, paused=False, has_input=False, status="idle", thread_id="root"):
        task = {"thread_id": thread_id, "paused": paused, "has_input": has_input}
        if state is not None:
            task["bootstrap"] = {"version": 1, "thread_id": thread_id, "state": state}
        self.threads[thread_id] = {
            "id": thread_id,
            "status": {"type": status},
            "canAcceptDirectInput": True,
            "turns": [],
        }
        self.service.state["tasks"]["main"] = task
        self.service.codex.register_root(thread_id)
        self.service.save()
        return task

    def matching(self, method):
        return [call for call in self.calls if call[0] == method]

    async def request(self, method, params=None, **kwargs):
        params = params or {}
        self.calls.append((method, deepcopy(params), self.disk))
        thread_id = params.get("threadId")
        if method == "thread/start":
            self.threads["new-root"] = {
                "id": "new-root", "status": {"type": "idle"},
                "canAcceptDirectInput": True, "turns": [],
            }
            return {"thread": deepcopy(self.threads["new-root"])}
        if method == "thread/read":
            if self.read_error:
                raise self.read_error
            if thread_id not in self.threads:
                raise RpcError(f"thread not loaded: {thread_id}", -32600)
            return {"thread": deepcopy(self.threads[thread_id])}
        if method == "thread/resume":
            if self.resume_error:
                raise self.resume_error
            if thread_id not in self.persisted:
                raise RpcError(f"no rollout found for thread id {thread_id}", -32600)
            thread = deepcopy(self.threads[thread_id])
            thread["id"] = self.returned_resume_id or thread_id
            return {"thread": thread}
        if method == "thread/loaded/list":
            return {"data": list(self.threads), "nextCursor": None}
        if method == "thread/inject_items":
            if self.persist_on_inject:
                self.persisted.add(thread_id)
            if self.inject_error:
                raise self.inject_error
            return {}
        raise AssertionError(f"Unexpected RPC during bootstrap: {method}")


@pytest.fixture
def runtime(tmp_path):
    config = RuntimeConfig(str(tmp_path), "/usr/bin/true", "codex-cli fixture", "unused")
    config.prepare_directories()
    item = BootstrapRuntime(config)
    yield item
    for service in item.instances:
        service.codex.close()
        service.store.close()
    for socket in config.socket_dir.iterdir():
        socket.unlink()
    config.socket_dir.rmdir()


async def test_new_root_alias_is_durable_before_injection_and_same_id_resume(runtime):
    durable_states = []
    save = runtime.service.save

    def tracked_save():
        save()
        durable_states.append(runtime.disk["tasks"]["main"])

    runtime.service.save = tracked_save
    task = await runtime.service.ensure_thread("main")
    probes = runtime.matching("thread/resume")
    injections = runtime.matching("thread/inject_items")
    assert len(probes) == 1 and len(injections) == 1
    assert [state["bootstrap"]["state"] for state in durable_states] == ["pending", "sending", "ready"]
    assert durable_states[0]["thread_id"] == "new-root"
    assert durable_states[0]["bootstrap"] == {
        "version": 1, "thread_id": "new-root", "state": "pending",
    }
    assert injections[0][2]["tasks"]["main"]["bootstrap"]["state"] == "sending"
    assert probes[0][1]["threadId"] == "new-root"
    assert runtime.calls.index(injections[0]) < runtime.calls.index(probes[0])
    items = injections[0][1]["items"]
    assert len(items) == 1 and items[0]["type"] == "message" and items[0]["role"] == "developer"
    assert task["thread_id"] == "new-root" and task["bootstrap"]["state"] == "ready"
    assert runtime.disk["tasks"]["main"] == task
    assert task["has_input"] is False and not runtime.disk["intents"]
    runtime.service.submit.assert_not_called()
    assert all(method not in {"turn/start", "thread/input/queue/add"} for method, _, _ in runtime.calls)


async def test_existing_rollout_resumes_same_root_without_adding_initialization(runtime):
    task = runtime.seed(state="pending", paused=True)
    runtime.persisted.add("root")
    assert await runtime.service.ensure_thread("main") == task
    assert task["bootstrap"]["state"] == "ready" and task["paused"] is True
    assert not runtime.matching("thread/start") and not runtime.matching("thread/inject_items")


async def test_lost_injection_ack_recovers_by_same_id_resume_without_duplicate(runtime):
    runtime.inject_error = RpcTimeout("injection acknowledgement lost")
    with pytest.raises(RpcTimeout):
        await runtime.service.ensure_thread("main")
    before = runtime.disk["tasks"]["main"]
    assert before["thread_id"] == "new-root"
    assert before["bootstrap"]["state"] in {"sending", "unknown"}
    assert before["has_input"] is False
    restarted = runtime.restart()
    recovered = await restarted.ensure_thread("main")
    assert recovered["thread_id"] == "new-root" and recovered["bootstrap"]["state"] == "ready"
    assert len(runtime.matching("thread/inject_items")) == 1
    assert len(runtime.matching("thread/start")) == 1


async def test_unacknowledged_missing_rollout_cannot_reinject_or_replace_after_restart(runtime):
    runtime.persist_on_inject = False
    runtime.inject_error = RpcTimeout("unresolved native outcome")
    with pytest.raises(RpcTimeout):
        await runtime.service.ensure_thread("main")
    restarted = runtime.restart()
    with pytest.raises(RpcError):
        await restarted.ensure_thread("main")
    assert runtime.disk["tasks"]["main"]["thread_id"] == "new-root"
    assert runtime.disk["tasks"]["main"]["bootstrap"]["state"] in {"sending", "unknown"}
    assert len(runtime.matching("thread/inject_items")) == 1
    assert len(runtime.matching("thread/start")) == 1


@pytest.mark.parametrize("state", ["sending", "unknown"])
async def test_ambiguous_initialization_never_replays_even_with_empty_turn_metadata(runtime, state):
    runtime.seed(state=state)
    with pytest.raises(RpcError):
        await runtime.service.ensure_thread("main")
    assert runtime.disk["tasks"]["main"]["thread_id"] == "root"
    assert not runtime.matching("thread/inject_items") and not runtime.matching("thread/start")
    runtime.persisted.add("root")
    recovered = await runtime.restart().ensure_thread("main")
    assert recovered["bootstrap"]["state"] == "ready"
    assert not runtime.matching("thread/inject_items")


@pytest.mark.parametrize("evidence", ["has_input", "completed_intent", "unknown_intent", "failed_intent"])
async def test_local_input_evidence_forbids_empty_root_initialization(runtime, evidence):
    runtime.seed(state="pending", has_input=evidence == "has_input")
    if evidence.endswith("_intent"):
        runtime.service.state["intents"]["request"] = {
            "id": "request", "thread_id": "root", "target": "main",
            "status": evidence.removesuffix("_intent"),
        }
        runtime.service.save()
    with pytest.raises(RpcError):
        await runtime.service.ensure_thread("main")
    assert runtime.disk["tasks"]["main"]["thread_id"] == "root"
    assert not runtime.matching("thread/inject_items") and not runtime.matching("thread/start")


async def test_legacy_loaded_empty_root_initializes_original_id_and_preserves_pause(runtime):
    runtime.seed(paused=True)
    task = await runtime.service.ensure_thread("main")
    assert task["thread_id"] == "root" and task["paused"] is True
    assert task["bootstrap"]["state"] == "ready" and task["has_input"] is False
    assert len(runtime.matching("thread/inject_items")) == 1
    assert not runtime.matching("thread/start")
    runtime.service.submit.assert_not_called()


async def test_normal_used_task_never_receives_initialization_record(runtime):
    runtime.seed(has_input=True)
    runtime.persisted.add("root")
    task = await runtime.service.ensure_thread("main")
    assert task["thread_id"] == "root" and task["has_input"] is True
    assert not runtime.matching("thread/inject_items") and not runtime.matching("thread/start")


@pytest.mark.parametrize(
    "error",
    [
        RpcError("no rollout found for thread id root", -32603),
        RpcError("no rollout found for thread id another-root", -32600),
        RpcError("proxy failure: no rollout found for thread id root", -32600),
        RpcError("no rollout found for thread id root"),
    ],
)
async def test_only_exact_missing_rollout_error_for_owned_id_authorizes_initialization(runtime, error):
    runtime.seed(state="pending")
    runtime.resume_error = error
    with pytest.raises(RpcError):
        await runtime.service.ensure_thread("main")
    assert runtime.disk["tasks"]["main"]["thread_id"] == "root"
    assert not runtime.matching("thread/inject_items") and not runtime.matching("thread/start")


async def test_active_native_root_is_not_treated_as_empty(runtime):
    runtime.seed(state="pending", status="active")
    with pytest.raises(RpcError):
        await runtime.service.ensure_thread("main")
    assert not runtime.matching("thread/inject_items") and not runtime.matching("thread/start")


async def test_existing_native_turn_forbids_initialization_despite_no_local_input(runtime):
    runtime.seed(state="pending")
    runtime.threads["root"]["turns"] = [{"id": "native-turn", "status": "completed", "items": []}]
    with pytest.raises(RpcError):
        await runtime.service.ensure_thread("main")
    assert not runtime.matching("thread/inject_items") and not runtime.matching("thread/start")


async def test_cancelled_initialization_retains_sending_and_never_replays(runtime):
    runtime.persist_on_inject = False
    runtime.inject_error = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await runtime.service.ensure_thread("main")
    assert runtime.disk["tasks"]["main"]["bootstrap"]["state"] == "sending"
    with pytest.raises(RpcError):
        await runtime.restart().ensure_thread("main")
    assert len(runtime.matching("thread/inject_items")) == 1
    assert len(runtime.matching("thread/start")) == 1


async def test_bootstrap_receipt_for_another_root_does_not_authorize_current_root(runtime):
    task = runtime.seed(state="ready")
    task["bootstrap"]["thread_id"] = "another-root"
    runtime.service.save()
    with pytest.raises(RpcError, match="bootstrap state"):
        await runtime.service.ensure_thread("main")
    assert not runtime.matching("thread/inject_items") and not runtime.matching("thread/start")


async def test_resume_response_for_another_root_cannot_mark_original_ready(runtime):
    runtime.seed(state="pending")
    runtime.persisted.add("root")
    runtime.returned_resume_id = "another-root"
    with pytest.raises(RpcError, match="different thread"):
        await runtime.service.ensure_thread("main")
    assert runtime.disk["tasks"]["main"]["thread_id"] == "root"
    assert runtime.disk["tasks"]["main"]["bootstrap"]["state"] != "ready"
    assert not runtime.matching("thread/inject_items") and not runtime.matching("thread/start")
