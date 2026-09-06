#!/usr/bin/env python3
"""One fail-closed source + installed-artifact regression entry point."""

import argparse
import json
from pathlib import Path
import sys
import tempfile

# The verifier runs from this explicit source tree; artifact subprocesses use the
# candidate's isolated interpreter, supplied through ALICE_ARTIFACT_PYTHON.
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from alice_codex.releases import ReleaseError, ReleaseManager  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=ROOT)
    parser.add_argument("--codex-binary", type=Path)
    parser.add_argument("--release-home", type=Path)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--native", action="store_true", help="require real Codex protocol tests")
    parser.add_argument(
        "--live", action="store_true", help="explicitly run and require budgeted model tests"
    )
    parser.add_argument("--timeout", type=float, default=600)
    options = parser.parse_args(argv)
    source = options.source.resolve()
    binary = options.codex_binary or source / "tests" / "fixtures" / "fake_codex.py"
    if options.native and options.codex_binary is None:
        parser.error("--native requires an explicit real --codex-binary")
    if options.live and not options.native:
        parser.error("--live requires --native and its explicit real Codex binary")
    temporary = (
        tempfile.TemporaryDirectory(prefix="alice-check-") if options.release_home is None else None
    )
    home = Path(temporary.name) if temporary else options.release_home.resolve()
    report: dict = {}
    exit_code = 1
    try:
        manager = ReleaseManager(home)
        candidate = manager.build(source, codex_binary=binary, timeout=options.timeout)
        report = manager.verify(
            candidate,
            source_root=source,
            native=options.native,
            live=options.live,
            timeout=options.timeout,
        )
        report["candidate_retained"] = temporary is None
        if temporary is None:
            report["release_home"] = str(home)
        exit_code = 0 if report["passed"] else 1
    except (ReleaseError, OSError, ValueError) as exc:
        report = {"passed": False, "phase": "build_or_verify", "error": str(exc)}
    except KeyboardInterrupt:
        report = {"passed": False, "phase": "build_or_verify", "error": "cancelled"}
        exit_code = 130
    finally:
        text = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
        if options.report is not None:
            options.report.parent.mkdir(parents=True, exist_ok=True)
            options.report.write_text(text)
        print(text, end="")
        if temporary:
            temporary.cleanup()
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
