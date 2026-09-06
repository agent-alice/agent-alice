"""Host source configuration is pure; fixtures never contact an account or model."""

from copy import deepcopy
from dataclasses import FrozenInstanceError
import json

import pytest

from alice_codex.config import RuntimeConfig, load_config
from alice_codex.heartbeat import (
    HEARTBEAT_SOURCES_CONFIG_VERSION,
    HeartbeatSourceBinding,
    parse_heartbeat_sources,
)
import alice_codex.collector as collector_module
import alice_codex.heartbeat as heartbeat_module


def source_config(*, target="monitor"):
    return {
        "version": 1,
        "sources": [
            {
                "target": target,
                "wait_seconds": 30,
                "spec": {
                    "source_id": "fixture-answers",
                    "url": "https://example.invalid/answers?filter=published",
                    "subject": "fixture-member",
                    "collection": "answers",
                    "auth_context_version": "fixture-account-v1",
                    "max_age_seconds": 60,
                    "required_metrics": ["voteup_count"],
                    "header_env": [["Authorization", "FIXTURE_HEARTBEAT_AUTH"]],
                },
            }
        ],
    }


@pytest.fixture
def runtime(tmp_path):
    # Validation and save do not need a native process or a prepared runtime.
    return RuntimeConfig(str(tmp_path), "/usr/bin/true", "synthetic", "unused")


def test_absent_and_explicit_empty_configuration_are_valid_without_sources():
    assert HEARTBEAT_SOURCES_CONFIG_VERSION == 1
    assert parse_heartbeat_sources(None) == ()
    assert parse_heartbeat_sources({"version": 1, "sources": []}) == ()


def test_parser_never_fetches_or_resolves_credentials(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("source parsing must not collect data or resolve header credentials")

    monkeypatch.delenv("FIXTURE_HEARTBEAT_AUTH", raising=False)
    monkeypatch.setattr(heartbeat_module, "collect_collection", forbidden)
    monkeypatch.setattr(collector_module, "_headers", forbidden)
    result = parse_heartbeat_sources(source_config())
    assert len(result) == 1 and isinstance(result[0], HeartbeatSourceBinding)
    assert result[0].spec.header_env == (("Authorization", "FIXTURE_HEARTBEAT_AUTH"),)
    assert result[0].wait_seconds == 30.0 and type(result[0].wait_seconds) is float


def test_parser_copies_nested_arrays_and_returns_frozen_bindings():
    raw = source_config()
    before = deepcopy(raw)
    result = parse_heartbeat_sources(raw)
    assert raw == before
    assert isinstance(result, tuple)
    binding = result[0]
    digest = binding.spec.scope_sha256
    raw["sources"][0]["spec"]["required_metrics"].append("comment_count")
    raw["sources"][0]["spec"]["header_env"][0][1] = "OTHER_ENV"
    raw["sources"][0]["target"] = "other"
    assert binding.target == "monitor" and binding.spec.scope_sha256 == digest
    with pytest.raises(FrozenInstanceError):
        binding.wait_seconds = 999
    with pytest.raises(FrozenInstanceError):
        binding.spec.subject = "other"


def test_optional_spec_fields_reuse_existing_defaults_and_multiple_targets_remain_distinct():
    raw = source_config(target="main")
    for name in ("required_metrics", "header_env"):
        raw["sources"][0]["spec"].pop(name)
    other = deepcopy(raw["sources"][0])
    other["target"] = "具名任务"
    raw["sources"].append(other)
    first, second = parse_heartbeat_sources(raw)
    assert (first.target, second.target) == ("main", "具名任务")
    assert first.spec.required_metrics == ("voteup_count", "comment_count")
    assert first.spec.header_env == ()
    assert first.spec.max_pages == 20 and first.spec.max_items == 2000
    assert first.spec.request_timeout == 10 and first.spec.total_seconds == 60
    assert first.spec.scope_sha256 == second.spec.scope_sha256


@pytest.mark.parametrize(
    "raw",
    [
        False,
        [],
        "configured",
        {},
        {"version": 1},
        {"sources": []},
        {"version": True, "sources": []},
        {"version": 1.0, "sources": []},
        {"version": 2, "sources": []},
        {"version": 1, "sources": None},
        {"version": 1, "sources": ()},
        {"version": 1, "sources": [], "trusted": True},
    ],
)
def test_invalid_envelope_never_becomes_an_empty_configuration(raw):
    with pytest.raises(ValueError, match="heartbeat_sources"):
        parse_heartbeat_sources(raw)


@pytest.mark.parametrize(
    "entry",
    [
        None,
        [],
        {},
        {"target": "monitor", "wait_seconds": 30},
        {"target": "monitor", "wait_seconds": 30, "spec": {}, "trusted": True},
        {"target": "monitor", "wait_seconds": 30, "spec": {}, "receipt": {"state": "known"}},
    ],
)
def test_entries_require_exact_configuration_fields(entry):
    with pytest.raises(ValueError, match="heartbeat source requires"):
        parse_heartbeat_sources({"version": 1, "sources": [entry]})


@pytest.mark.parametrize(
    "target",
    [None, True, "", "  ", "line\nbreak", "x" * 151, "new", "summary:L1", "scheduled:daily"],
)
def test_target_must_be_accepted_by_host_registration(target):
    with pytest.raises(ValueError, match="target"):
        parse_heartbeat_sources(source_config(target=target))


def test_duplicate_target_is_rejected_instead_of_overwriting_its_source():
    raw = source_config()
    other = deepcopy(raw["sources"][0])
    other["spec"]["auth_context_version"] = "different-account"
    raw["sources"].append(other)
    with pytest.raises(ValueError, match="unique"):
        parse_heartbeat_sources(raw)


@pytest.mark.parametrize(
    "wait", [None, True, False, 0, -1, "30", float("inf"), float("nan"), 10**400]
)
def test_wait_is_explicit_positive_finite_and_not_boolean(wait):
    raw = source_config()
    raw["sources"][0]["wait_seconds"] = wait
    with pytest.raises(ValueError, match="wait_seconds"):
        parse_heartbeat_sources(raw)


@pytest.mark.parametrize(
    "field",
    ["source_id", "url", "subject", "collection", "auth_context_version", "max_age_seconds"],
)
def test_required_spec_fields_cannot_be_omitted(field):
    raw = source_config()
    raw["sources"][0]["spec"].pop(field)
    with pytest.raises(ValueError, match="invalid CollectionSpec"):
        parse_heartbeat_sources(raw)


@pytest.mark.parametrize(
    "spec", [None, [], "url", {"trusted": True}, {"state": "known", "content_sha256": "0" * 64}]
)
def test_spec_is_configuration_not_a_raw_observation(spec):
    raw = source_config()
    raw["sources"][0]["spec"] = spec
    with pytest.raises(ValueError, match="CollectionSpec"):
        parse_heartbeat_sources(raw)


@pytest.mark.parametrize(
    "field,value",
    [
        ("required_metrics", "voteup_count"),
        ("required_metrics", ("voteup_count",)),
        ("required_metrics", None),
        ("header_env", {}),
        ("header_env", None),
        ("header_env", (("Authorization", "FIXTURE_AUTH"),)),
        ("header_env", [("Authorization", "FIXTURE_AUTH")]),
        ("header_env", ["Authorization=FIXTURE_AUTH"]),
        ("header_env", [["Authorization"]]),
        ("header_env", [["Authorization", "FIXTURE_AUTH", "extra"]]),
    ],
)
def test_optional_containers_must_be_json_arrays_before_freezing(field, value):
    raw = source_config()
    raw["sources"][0]["spec"][field] = value
    with pytest.raises(ValueError, match="JSON list"):
        parse_heartbeat_sources(raw)


@pytest.mark.parametrize(
    "field,value",
    [
        ("required_metrics", []),
        ("required_metrics", ["voteup_count", "voteup_count"]),
        ("required_metrics", [{}]),
        ("header_env", [["Host", "FIXTURE_HOST"]]),
        ("header_env", [["Authorization", "ENV1"], ["authorization", "ENV2"]]),
        ("header_env", [["Authorization", "Bearer fixture-secret"]]),
        ("max_pages", 1001),
        ("max_items", 20001),
        ("request_timeout", True),
        ("total_seconds", 0),
        ("max_age_seconds", float("nan")),
        ("url", "https://user:fixture-secret@example.invalid/answers"),
    ],
)
def test_constructor_validation_is_preserved_and_errors_do_not_echo_values(field, value):
    raw = source_config()
    raw["sources"][0]["spec"][field] = value
    with pytest.raises(ValueError, match="invalid CollectionSpec") as caught:
        parse_heartbeat_sources(raw)
    assert "fixture-secret" not in str(caught.value)
    assert "example.invalid" not in str(caught.value)


def test_unknown_spec_field_is_rejected_without_echoing_its_name_or_value():
    raw = source_config()
    raw["sources"][0]["spec"]["private-secret-field"] = "private-secret-value"
    with pytest.raises(ValueError) as caught:
        parse_heartbeat_sources(raw)
    assert "private-secret" not in str(caught.value)


def test_scope_preserves_authentication_binding_but_excludes_wait_and_fetch_budgets():
    raw = source_config()
    initial = parse_heartbeat_sources(raw)[0]
    raw["sources"][0]["wait_seconds"] = 45
    raw["sources"][0]["spec"].update(max_pages=40, request_timeout=20)
    tuned = parse_heartbeat_sources(raw)[0]
    assert tuned.spec.scope_sha256 == initial.spec.scope_sha256
    raw["sources"][0]["spec"]["auth_context_version"] = "fixture-account-v2"
    rotated = parse_heartbeat_sources(raw)[0]
    assert rotated.spec.scope_sha256 != initial.spec.scope_sha256


def test_legacy_configuration_loads_without_rewrite_and_save_omits_absent_sources(runtime):
    runtime.save()
    path = runtime.root / "config.json"
    before = path.read_bytes()
    assert "heartbeat_sources" not in json.loads(before)
    loaded = load_config(runtime.root)
    assert loaded.heartbeat_sources is None and path.read_bytes() == before
    loaded.save()
    assert path.read_bytes() == before


@pytest.mark.parametrize("raw", [None, {"version": 1, "sources": []}, source_config()])
def test_runtime_configuration_roundtrip_preserves_explicit_source_structure(runtime, raw):
    original = deepcopy(raw)
    runtime.heartbeat_sources = raw
    runtime.save()
    path = runtime.root / "config.json"
    before = path.read_bytes()
    saved = json.loads(before)
    assert saved.get("heartbeat_sources") == original
    assert ("heartbeat_sources" in saved) is (raw is not None)
    restored = load_config(runtime.root)
    assert restored.heartbeat_sources == original and raw == original
    assert parse_heartbeat_sources(restored.heartbeat_sources) == parse_heartbeat_sources(original)
    assert path.read_bytes() == before
    restored.save()
    assert path.read_bytes() == before


def test_runtime_roundtrip_retains_all_optional_limits_and_scope(runtime):
    raw = source_config()
    raw["sources"][0]["spec"].update(
        required_metrics=["comment_count", "voteup_count"],
        header_env=[["X-Account", "FIXTURE_ACCOUNT"], ["Authorization", "FIXTURE_AUTH"]],
        max_pages=23,
        max_items=301,
        request_timeout=7.5,
        total_seconds=45.5,
    )
    expected = parse_heartbeat_sources(raw)
    runtime.heartbeat_sources = raw
    runtime.save()
    actual = parse_heartbeat_sources(load_config(runtime.root).heartbeat_sources)
    assert actual == expected
    assert actual[0].spec.scope_sha256 == expected[0].spec.scope_sha256


@pytest.mark.parametrize(
    "raw", [False, {"version": 99, "sources": []}, {"version": 1, "sources": [{"trusted": True}]}]
)
def test_invalid_source_save_preserves_existing_config_and_load_never_rewrites(runtime, raw):
    runtime.save()
    path = runtime.root / "config.json"
    before = path.read_bytes()
    runtime.heartbeat_sources = raw
    with pytest.raises(ValueError):
        runtime.save()
    assert path.read_bytes() == before
    damaged = json.loads(before)
    damaged["heartbeat_sources"] = raw
    path.write_text(json.dumps(damaged))
    damaged_bytes = path.read_bytes()
    with pytest.raises(ValueError):
        load_config(runtime.root)
    assert path.read_bytes() == damaged_bytes
