"""Historical 28-route verifier API, backed by the common offline verifier."""

from pathlib import Path

from experiments.multiq_pilot_20260924.compat import verify_legacy, verifier_main


def verify(
    artifact_root: Path, baseline_spec_dir: Path, output_dir: Path,
    *, seal: bool = False,
) -> dict:
    return verify_legacy(
        28, artifact_root, output_dir,
        baseline_spec_dir=baseline_spec_dir, seal=seal,
    )


if __name__ == "__main__":
    verifier_main(28)
