"""Select verified command code without opening the business databases."""

import argparse
import os
from pathlib import Path
import sys

from .releases import ReleaseError, ReleaseManager


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--home", type=Path, required=True)
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    try:
        home = args.home.expanduser().resolve()
        release = ReleaseManager(home).resolve_current()
        if release is None:
            raise ReleaseError("No verified Alice release is active")
        python = release["python"]
        environment = os.environ.copy()
        for name in ("PYTHONPATH", "PYTHONHOME"):
            environment.pop(name, None)
        environment["PYTHONNOUSERSITE"] = "1"
        os.chdir(home)
        os.execve(
            python,
            [python, "-I", "-m", "alice_codex", "--home", str(home), *(args.arguments or ["chat"])],
            environment,
        )
    except (ReleaseError, OSError, ValueError) as error:
        print(f"Alice command could not be launched: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
