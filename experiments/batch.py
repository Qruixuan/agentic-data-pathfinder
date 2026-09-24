"""Stable configuration-driven entry point for supported experiment families."""

import sys

from experiments.interleaved_batch import main


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("EXPERIMENT_BATCH_FAILED", type(exc).__name__, file=sys.stderr)
        raise SystemExit(2) from None
