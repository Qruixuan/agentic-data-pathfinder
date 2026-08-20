"""Shared markers identifying a generated synthetic Oracle fixture.

This module deliberately depends on nothing else in the package. Both the
fixture generator and the real Reduced Oracle analysis need these constants,
and a shared leaf module is what keeps that from becoming an import cycle.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path


SYNTHETIC_FLAG = "synthetic"
SCIENTIFIC_CLAIM_FLAG = "eligible_for_scientific_claims"
SYNTHETIC_MANIFEST_FILENAME = "synthetic_oracle_manifest.json"
SYNTHETIC_TRUTH_FILENAME = "synthetic_truth.json"
ORACLE_TABLE_FILENAME = "oracle_table.csv"

SYNTHETIC_FIXTURE_CONSUMERS = ("evaluate-awm", "run-oed-replay")


class SyntheticFixtureRefusal(RuntimeError):
    """Raised when a real Oracle command is aimed at a synthetic fixture."""


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().casefold() in {"true", "1", "yes"}
    return False


def _manifest_says_synthetic(path: Path) -> bool:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        # A manifest we cannot parse is still a manifest: its presence is
        # handled by the filename check, which cannot be defeated this way.
        return False
    return isinstance(payload, dict) and _truthy(payload.get(SYNTHETIC_FLAG))


def _table_says_synthetic(path: Path) -> bool:
    try:
        with path.open(encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if SYNTHETIC_FLAG not in (reader.fieldnames or ()):
                return False
            return any(_truthy(row.get(SYNTHETIC_FLAG)) for row in reader)
    except (OSError, UnicodeDecodeError, csv.Error):
        return False


def _records_say_synthetic(path: Path) -> bool:
    try:
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                record = json.loads(line)
                return isinstance(record, dict) and _truthy(
                    record.get(SYNTHETIC_FLAG)
                )
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return False
    return False


def synthetic_fixture_evidence(directory: str | Path) -> list[str]:
    """Return every reason this directory looks like a synthetic fixture.

    Read-only and best-effort: an unreadable or malformed file contributes no
    evidence rather than raising, because this runs as a guard in front of
    commands that must fail for a clear reason or not at all.
    """
    root = Path(directory)
    reasons: list[str] = []
    manifest = root / SYNTHETIC_MANIFEST_FILENAME
    if manifest.is_file():
        reasons.append(f"{SYNTHETIC_MANIFEST_FILENAME} is present")
        if _manifest_says_synthetic(manifest):
            reasons.append(
                f"{SYNTHETIC_MANIFEST_FILENAME} declares {SYNTHETIC_FLAG}=true"
            )
    if (root / SYNTHETIC_TRUTH_FILENAME).is_file():
        reasons.append(f"{SYNTHETIC_TRUTH_FILENAME} is present")
    table = root / ORACLE_TABLE_FILENAME
    if table.is_file() and _table_says_synthetic(table):
        reasons.append(
            f"{ORACLE_TABLE_FILENAME} has rows marked {SYNTHETIC_FLAG}=true"
        )
    designs = root / "designs"
    if designs.is_dir():
        for records in sorted(designs.glob("*/runs.jsonl")):
            if _records_say_synthetic(records):
                reasons.append(
                    f"designs/{records.parent.name}/runs.jsonl contains "
                    f"{SYNTHETIC_FLAG}=true records"
                )
    return reasons


def assert_not_synthetic_fixture(
    directory: str | Path,
    *,
    command: str,
) -> None:
    """Fail closed before a real Oracle command touches a fixture directory.

    The analysis commands rewrite ``oracle_table.csv`` in place. Run against a
    generated fixture they would silently replace declared synthetic values
    with zeros recomputed from a transition journal the fixture does not have,
    leaving a file that no longer says it is synthetic.
    """
    reasons = synthetic_fixture_evidence(directory)
    if not reasons:
        return
    raise SyntheticFixtureRefusal(
        f"{command} refuses to run against a synthetic Oracle fixture "
        f"directory ({Path(directory)}): "
        + "; ".join(reasons)
        + ". Nothing was written. A synthetic fixture may only be consumed "
        "by " + " and ".join(SYNTHETIC_FIXTURE_CONSUMERS) + ", and is never "
        "physical Reduced Oracle evidence."
    )
