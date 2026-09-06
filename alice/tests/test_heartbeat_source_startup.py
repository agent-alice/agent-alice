"""Configured Service startup uses actual loopback collection, never forged receipts.

Only the native turn and scheduling boundary uses the existing isolated fixture.
No test registers a source manually or connects to Codex/an active Alice runtime.
"""

from copy import deepcopy
from dataclasses import asdict
import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from alice_codex.config import RuntimeConfig, load_config
from alice_codex.heartbeat import CollectionSpec, HostHeartbeatAdapter
from alice_codex.memory import MemoryStore
from alice_codex.scheduler import RejectedDispatch
from alice_codex.service import Service
import alice_codex.service as service_module

from test_heartbeat import endpoint, page
from test_heartbeat_service import HeartbeatRuntime


def source_config(url, *, wait=10, **changes):
    spec = CollectionSpec(
        source_id="startup-fixture",
        url=url,
        subject="synthetic-member",
        collection="answers",
        auth_context_version="synthetic-account-v1",
        max_age_seconds=60,
        required_metrics=("voteup_count",),
    )
    spec_json = json.loads(json.dumps(asdict(spec)))
    spec_json.update(changes)
    return {
        "version": 1,
        "sources": [{"target": "monitor", "wait_seconds": wait, "spec": spec_json}],
    }


@pytest.fixture
def startup(tmp_path, monkeypatch):
    clock = SimpleNamespace(now=100.0, monotonic=1000.0)
    monkeypatch.setattr(
        service_module,
        "time",
        SimpleNamespace(time=lambda: clock.now, monotonic=lambda: clock.monotonic),
    )
    config = RuntimeConfig(str(tmp_path / "alice"), "/usr/bin/true", "synthetic-fixture", "unused")
    config.prepare_directories()
    MemoryStore(config.root).install_workspace_templates()
    runtimes, registrations = [], []
    real_register = HostHeartbeatAdapter.register

    def observed_register(adapter, target, spec):
        registrations.append((id(adapter), target, spec))
        real_register(adapter, target, spec)

    # Observe only the calls made by Service construction; keep the actual
    # adapter and collector intact, including their receipt generation.
    monkeypatch.setattr(HostHeartbeatAdapter, "register", observed_register)

    def start(document=None, *, absent=False):
        config.heartbeat_sources = document
        config.save()
        if absent:
            path = config.root / "config.json"
            raw = json.loads(path.read_text())
            raw.pop("heartbeat_sources", None)
            path.write_text(json.dumps(raw))
        runtime = HeartbeatRuntime(load_config(config.root), clock)
        runtimes.append(runtime)
        return runtime

    yield SimpleNamespace(start=start, config=config, registrations=registrations)
    for runtime in runtimes:
        for service in runtime.instances:
            service.store.close()
    for socket in config.socket_dir.iterdir():
        socket.unlink()
    config.socket_dir.rmdir()


def restart_from_config(runtime, document, *, advance=10):
    runtime.config.heartbeat_sources = document
    runtime.config.save()
    runtime.config = load_config(runtime.config.root)
    runtime.advance(advance)
    return runtime.restart()


def items(votes=2):
    return [{"id": "synthetic-answer", "voteup_count": votes}]


async def test_configured_source_collects_after_startup_and_restart_without_manual_registration(
    startup,
):
    with endpoint({"/answers": page(items())}) as (base, requests):
        document = source_config(base + "/answers")
        runtime = startup.start(document)
        assert requests == []  # Construction registers; it does not perform network I/O.
        first = await runtime.dispatch()
        runtime.complete(first)
        consumed = runtime.disk["heartbeat_consumed"]["monitor"]
        assert consumed["state"] == "known" and first.status == "accepted"
        assert len(requests) == 1
        restart_from_config(runtime, document)
        with pytest.raises(RejectedDispatch, match="unchanged"):
            await runtime.dispatch()
        latest = runtime.service.store.get_heartbeat_state("monitor")["latest"]
        assert latest["id"] != consumed["id"]
        assert latest["content_sha256"] == consumed["content_sha256"]
        assert runtime.disk["heartbeat_consumed"]["monitor"] == consumed
        assert len(requests) == 2
        assert [target for _, target, _ in startup.registrations] == ["monitor", "monitor"]
        assert startup.registrations[0][0] != startup.registrations[1][0]
        runtime.codex.turn_start.assert_awaited_once()


@pytest.mark.parametrize("mode", ["absent", "null", "empty"])
async def test_no_configured_sources_registers_nothing_and_preserves_unconfigured_semantics(
    startup, mode
):
    document = {"version": 1, "sources": []} if mode == "empty" else None
    runtime = startup.start(document, absent=mode == "absent")
    # save() intentionally omits None; explicitly exercise reading a JSON null too.
    if mode == "null":
        path = runtime.config.root / "config.json"
        raw = json.loads(path.read_text())
        raw["heartbeat_sources"] = None
        path.write_text(json.dumps(raw))
        runtime.config = load_config(runtime.config.root)
        runtime.restart()
    observed = await runtime.service.observe_heartbeat("monitor")
    assert not observed["configured"] and observed["latest"]["state"] == "unconfigured"
    assert observed["last_good"] is None
    assert startup.registrations == []
    assert (await runtime.dispatch()).status == "accepted"
    assert not runtime.disk.get("heartbeat_consumed")


async def test_removing_source_stops_collection_and_restoring_same_scope_keeps_consumption(startup):
    with endpoint({"/answers": page(items())}) as (base, requests):
        document = source_config(base + "/answers")
        runtime = startup.start(document)
        first = await runtime.dispatch()
        runtime.complete(first)
        consumed = runtime.disk["heartbeat_consumed"]["monitor"]
        restart_from_config(runtime, {"version": 1, "sources": []})
        observed = await runtime.service.observe_heartbeat("monitor")
        assert not observed["configured"] and observed["latest"]["state"] == "unconfigured"
        assert observed["last_good"] == consumed
        assert len(requests) == 1 and len(startup.registrations) == 1
        ordinary_review = await runtime.dispatch()
        runtime.complete(ordinary_review)
        assert runtime.disk["heartbeat_consumed"]["monitor"] == consumed
        restart_from_config(runtime, document)
        with pytest.raises(RejectedDispatch, match="unchanged"):
            await runtime.dispatch()
        assert len(requests) == 2 and len(startup.registrations) == 2
        assert runtime.disk["heartbeat_consumed"]["monitor"] == consumed
        assert runtime.codex.turn_start.await_count == 2


@pytest.mark.parametrize("change", ["query", "source_id"])
async def test_changed_startup_source_creates_new_scope_even_for_identical_items(startup, change):
    routes = {"/answers?scope=one": page(items()), "/answers?scope=two": page(items())}
    with endpoint(routes) as (base, requests):
        document = source_config(base + "/answers?scope=one")
        runtime = startup.start(document)
        first = await runtime.dispatch()
        runtime.complete(first)
        consumed = runtime.disk["heartbeat_consumed"]["monitor"]
        changed = deepcopy(document)
        spec = changed["sources"][0]["spec"]
        if change == "query":
            spec["url"] = base + "/answers?scope=two"
        else:
            spec["source_id"] = "replacement-startup-source"
        restart_from_config(runtime, changed)
        second = await runtime.dispatch()
        current = runtime.disk["heartbeat_consumed"]["monitor"]
        assert second.status == "accepted" and current["scope_sha256"] != consumed["scope_sha256"]
        assert (
            runtime.service.store.get_heartbeat_state("monitor")["comparison"]["state"]
            == "baseline"
        )
        assert requests[-1]["path"] == (
            "/answers?scope=two" if change == "query" else "/answers?scope=one"
        )
        assert runtime.codex.turn_start.await_count == 2


async def test_changed_wait_is_restored_after_restart_without_resetting_consumed_content(startup):
    with endpoint({"/answers": page(items())}) as (base, requests):
        document = source_config(base + "/answers", wait=10)
        runtime = startup.start(document)
        first = await runtime.dispatch()
        runtime.complete(first)
        consumed = runtime.disk["heartbeat_consumed"]["monitor"]
        runtime.advance(10)
        original = await runtime.service.observe_heartbeat("monitor")
        assert original["waiting_until"] == runtime.clock.now + 10
        changed = deepcopy(document)
        changed["sources"][0]["wait_seconds"] = 60
        restart_from_config(runtime, changed, advance=1)
        observed = await runtime.service.observe_heartbeat("monitor")
        assert observed["waiting_until"] == runtime.clock.now + 60
        assert observed["latest"]["id"] != original["latest"]["id"]
        assert len(requests) == 3
        runtime.advance(59)
        assert (await runtime.service.observe_heartbeat("monitor"))["latest"] == observed["latest"]
        assert len(requests) == 3
        runtime.advance(1)
        with pytest.raises(RejectedDispatch, match="unchanged"):
            await runtime.dispatch()
        assert len(requests) == 4
        assert runtime.disk["heartbeat_consumed"]["monitor"] == consumed
        runtime.codex.turn_start.assert_awaited_once()


async def test_configured_restart_preserves_pause_and_pending_changed_evidence(startup):
    routes = {"/answers": page(items())}
    with endpoint(routes) as (base, requests):
        document = source_config(base + "/answers")
        runtime = startup.start(document)
        first = await runtime.dispatch()
        runtime.complete(first)
        consumed = runtime.disk["heartbeat_consumed"]["monitor"]
        await runtime.service.pause("monitor")
        routes["/answers"] = page(items(votes=7))
        restart_from_config(runtime, document)
        assert runtime.disk["tasks"]["monitor"]["paused"] is True
        event = runtime.event()
        with pytest.raises(RejectedDispatch):
            await runtime.dispatch(event)
        pending = runtime.service.store.get_heartbeat_state("monitor")["latest"]
        assert (
            pending["state"] == "known" and pending["content_sha256"] != consumed["content_sha256"]
        )
        assert runtime.disk["heartbeat_consumed"]["monitor"] == consumed
        runtime.codex.turn_start.assert_awaited_once()
        await runtime.service.resume("monitor")
        assert (await runtime.dispatch(event)).status == "accepted"
        assert runtime.disk["heartbeat_consumed"]["monitor"] == pending
        assert len(requests) == 2


async def test_configured_collection_failure_after_restart_is_unknown_not_unconfigured(startup):
    routes = {"/answers": page(items())}
    with endpoint(routes) as (base, requests):
        document = source_config(base + "/answers")
        runtime = startup.start(document)
        first = await runtime.dispatch()
        runtime.complete(first)
        consumed = runtime.disk["heartbeat_consumed"]["monitor"]
        routes["/answers"] = (403, {"error": "synthetic permission denial"}, {})
        restart_from_config(runtime, document)
        observed = await runtime.service.observe_heartbeat("monitor")
        assert observed["configured"] and observed["latest"]["state"] == "unknown"
        assert observed["last_good"] == consumed
        with pytest.raises(RejectedDispatch, match="unknown"):
            await runtime.dispatch()
        assert runtime.disk["heartbeat_consumed"]["monitor"] == consumed
        assert len(requests) == 2
        runtime.codex.turn_start.assert_awaited_once()


@pytest.mark.parametrize(
    "invalid", ["later_binding", "future_version", "target_surrogate", "spec_surrogate"]
)
def test_invalid_source_batch_fails_before_any_service_component_or_persistent_write(
    tmp_path, monkeypatch, invalid
):
    config = RuntimeConfig(str(tmp_path / "alice"), "/usr/bin/true", "synthetic-fixture", "unused")
    config.prepare_directories()
    document = source_config("https://example.invalid/answers")
    if invalid == "later_binding":
        second = deepcopy(document["sources"][0])
        second.update(target="other-monitor", wait_seconds=0)
        document["sources"].append(second)
    elif invalid == "future_version":
        document["version"] = 99
    elif invalid == "target_surrogate":
        document["sources"][0]["target"] = "monitor-\ud800"
    else:
        document["sources"][0]["spec"]["subject"] = "member-\udfff"
    config.heartbeat_sources = document  # Direct construction must validate too.
    config.database.write_bytes(b"synthetic unopened business state\x00")
    (config.root / "state/resources.sqlite3").write_bytes(b"synthetic resource state\x00")
    originals = {path: path.read_bytes() for path in config.root.rglob("*") if path.is_file()}
    constructors = {}
    for name in (
        "Store",
        "MemoryStore",
        "ResourceLedger",
        "HostHeartbeatAdapter",
        "CodexClient",
        "RpcClient",
        "NativeJournal",
    ):
        constructors[name] = Mock(side_effect=AssertionError(f"invalid config constructed {name}"))
        monkeypatch.setattr(service_module, name, constructors[name])
    try:
        with pytest.raises(ValueError):
            Service(config)
        for constructor in constructors.values():
            constructor.assert_not_called()
        assert {
            path: path.read_bytes() for path in config.root.rglob("*") if path.is_file()
        } == originals
    finally:
        for socket in config.socket_dir.iterdir():
            socket.unlink()
        config.socket_dir.rmdir()
