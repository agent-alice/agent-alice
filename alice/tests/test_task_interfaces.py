"""Public configuration compatibility and rejection before task admission."""

from dataclasses import asdict
import json

import pytest

from alice_codex import cli
from alice_codex.config import RuntimeConfig, UNATTENDED_TOOLS, load_config
from alice_codex.files import write_json


POLICY = {
    "max_elapsed_seconds": 120,
    "max_attempts": 4,
    "max_retries": 1,
    "retry_wait_seconds": 5,
    "unchanged_wait_seconds": 30,
}


def fixture_config(tmp_path):
    config = RuntimeConfig(str(tmp_path / "alice"), "/usr/bin/true", "fixture", "unused")
    config.prepare_directories()
    write_json(config.root / "config.json", asdict(config))
    return config


def test_legacy_config_load_does_not_enable_policy_or_rewrite_file(tmp_path):
    config = fixture_config(tmp_path)
    path = config.root / "config.json"
    legacy = asdict(config)
    legacy.pop("task_policy")
    write_json(path, legacy)
    original = path.read_bytes()
    assert load_config(config.root).task_policy is None
    assert path.read_bytes() == original


def test_explicit_policy_round_trip_and_existing_unknown_field_rejection(tmp_path):
    config = fixture_config(tmp_path)
    config.task_policy = POLICY
    config.save()
    assert load_config(config.root).task_policy == POLICY
    path = config.root / "config.json"
    document = json.loads(path.read_text())
    document["future_policy"] = "preserve me"
    write_json(path, document)
    original = path.read_bytes()
    with pytest.raises(ValueError, match="Unsupported or missing"):
        load_config(config.root)
    assert path.read_bytes() == original


@pytest.mark.parametrize(
    "value",
    [
        [],
        {},
        {**POLICY, "extra": 1},
        {**POLICY, "max_attempts": 0},
        {**POLICY, "max_retries": True},
        {**POLICY, "max_elapsed_seconds": float("nan")},
        {**POLICY, "unchanged_wait_seconds": -1},
    ],
)
def test_invalid_default_policy_preserves_saved_configuration(tmp_path, value):
    config = fixture_config(tmp_path)
    path = config.root / "config.json"
    original = path.read_bytes()
    config.task_policy = value
    with pytest.raises(ValueError):
        config.save()
    assert path.read_bytes() == original


def test_cli_invalid_limit_rejected_before_contacting_unavailable_service(tmp_path, capsys):
    config = fixture_config(tmp_path)
    assert not config.control_socket.exists()
    result = cli.main(
        [
            "--home",
            config.home,
            "task-policy",
            "set",
            "--target",
            "fixture",
            "--request-id",
            "fixture-policy",
            "--max-elapsed-seconds",
            "120",
            "--max-attempts",
            "0",
            "--max-retries",
            "1",
            "--retry-wait-seconds",
            "5",
            "--unchanged-wait-seconds",
            "30",
        ]
    )
    assert result == 1
    assert "max_attempts must be positive" in capsys.readouterr().err
    assert "task_policy_set" not in UNATTENDED_TOOLS
