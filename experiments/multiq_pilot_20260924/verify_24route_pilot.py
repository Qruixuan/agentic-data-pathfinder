"""Historical 24-route verifier API, backed by the common offline verifier."""

from pathlib import Path

from experiments.multiq_pilot_20260924.compat import verify_legacy, verifier_main


def verify(artifact_root: Path, output_dir: Path, *, seal: bool = False) -> dict:
    return verify_legacy(24, artifact_root, output_dir, seal=seal)


if __name__ == "__main__":
    verifier_main(24)
