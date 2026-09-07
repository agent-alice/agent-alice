"""Finite deployment settings shared by the CLI and process supervisor."""

import argparse
import math

DEFAULT_STARTUP_TIMEOUT = 30.0
MAX_STARTUP_TIMEOUT = 600.0


def validate_startup_timeout(value: object) -> float:
    if (
        type(value) not in (int, float)
        or not math.isfinite(value)
        or not 0 < value <= MAX_STARTUP_TIMEOUT
    ):
        raise ValueError("startup timeout must be finite, positive and at most 600 seconds")
    return float(value)


def parse_startup_timeout(value: str) -> float:
    try:
        return validate_startup_timeout(float(value))
    except (ValueError, OverflowError) as error:
        raise argparse.ArgumentTypeError(
            "startup timeout must be finite, positive and at most 600 seconds"
        ) from error


def startup_wait_seconds(timeout: float) -> float:
    # Two attempts for each of current/previous, with the supervisor's bounded
    # child cleanup, plus observation overhead. A CLI observation timeout never
    # resets the supervisor's attempt counters or starts another owner.
    return max(180.0, 4 * (validate_startup_timeout(timeout) + 40) + 60)
