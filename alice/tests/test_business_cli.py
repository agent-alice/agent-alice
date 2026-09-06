"""Exercise public collection/accounting commands with only local evidence."""

from dataclasses import asdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import subprocess
import sys
from threading import Thread

from alice_codex.config import RuntimeConfig
from alice_codex.files import write_json


def run(*args):
    result = subprocess.run(
        [sys.executable, "-m", "alice_codex", *map(str, args)],
        text=True,
        capture_output=True,
        timeout=15,
    )
    assert result.returncode in (0, 2), result.stderr
    return result.returncode, json.loads(result.stdout)


def test_collection_cli_keeps_partial_observation_and_returns_failure_status(tmp_path):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            content = json.dumps(
                {"data": [{"id": "missing", "comment_count": 0}], "paging": {"is_end": True}}
            ).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    worker = Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        output = tmp_path / "receipt.json"
        code, result = run(
            "collect",
            f"http://127.0.0.1:{server.server_port}/answers",
            "--subject",
            "fixture",
            "--collection",
            "answers",
            "--output",
            output,
        )
        assert code == 2 and result["complete"] is False
        document = json.loads(output.read_text())
        assert "voteup_count" not in document["observation"]["pages"][0]["items"][0]
        assert document["summary"]["metrics"]["voteup_count"]["state"] == "unknown"
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=5)


def test_offline_resource_receipts_are_idempotent_and_virtual_budget_stays_disabled(tmp_path):
    config = RuntimeConfig(str(tmp_path / "alice"), "/usr/bin/true", "fixture", "unused")
    config.prepare_directories()
    write_json(config.root / "config.json", asdict(config))
    command = (
        "--home",
        config.home,
        "resources",
        "money",
        "--receipt-id",
        "invoice-1",
        "--kind",
        "income",
        "--amount-microusd",
        "1000000",
        "--source",
        "synthetic receipt",
    )
    assert run(*command)[1]["recorded"] is True
    assert run(*command)[1]["recorded"] is False
    status = run("--home", config.home, "resources", "status")[1]
    assert status["money_receipts"] == {"income": {"amount_microusd": 1000000, "receipts": 1}}
    assert status["virtual_budget_enabled"] is False
    assert status["tokens"]["cost_microusd"] is None
    assert status["virtual_budget"]["balance_microusd"] is None


def test_cli_freshness_limit_rejects_an_otherwise_complete_cached_collection(tmp_path):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            content = json.dumps(
                {
                    "data": [{"id": "zero", "voteup_count": 0, "comment_count": 0}],
                    "paging": {"is_end": True},
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(content)))
            self.send_header("Age", "120")
            self.send_header("Cache-Control", "max-age=300")
            self.end_headers()
            self.wfile.write(content)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    worker = Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        output = tmp_path / "receipt.json"
        args = [
            "collect",
            f"http://127.0.0.1:{server.server_port}/answers",
            "--subject",
            "fixture",
            "--collection",
            "answers",
            "--output",
            output,
        ]
        code, result = run(*args)
        assert code == 0 and result["complete"] is True
        assert result["summary"]["metrics"]["voteup_count"]["value"] == 0
        code, result = run(*args, "--max-age-seconds", "60")
        assert code == 2 and result["complete"] is False
        assert result["summary"]["metrics"]["voteup_count"]["value"] is None
        assert result["summary"]["coverage"]["freshness"] == "stale"
        assert (
            json.loads(output.read_text())["observation"]["pages"][0]["items"][0]["voteup_count"]
            == 0
        )
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=5)
