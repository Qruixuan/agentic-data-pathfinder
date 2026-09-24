"""Compatibility CLI for the 28-route pilot; execution uses the shared runner."""

import sys

from experiments.multiq_pilot_20260924.compat import runner_main


if __name__ == "__main__":
    try:
        raise SystemExit(runner_main(28))
    except Exception as exc:
        print("EXPERIMENT_BATCH_FAILED", type(exc).__name__, file=sys.stderr)
        raise SystemExit(2) from None
