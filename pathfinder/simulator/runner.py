"""Atomic publication and verification for simulator runs."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from hashlib import sha256
from pathlib import Path
from typing import Any

from .config import SimulatorScenario, load_simulator_scenario
from .engine import SimulationResult, run_discrete_event_simulation


SIMULATOR_RUN_MANIFEST_SCHEMA_VERSION = (
    "pathfinder.flowmesh-infra-simulator-run/v1alpha1"
)


def _json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _jsonl_bytes(values: list[dict[str, Any]]) -> bytes:
    return "".join(
        json.dumps(
            value,
            sort_keys=True,
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
        for value in values
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return sha256(value).hexdigest()


def _documents(
    scenario: SimulatorScenario,
    result: SimulationResult,
) -> dict[str, bytes]:
    documents = {
        "simulator_plan.json": _json_bytes(result.plan),
        "events.jsonl": _jsonl_bytes([
            event.to_public_dict() for event in result.events
        ]),
        "canonical_records.jsonl": _jsonl_bytes(
            list(result.canonical_records)
        ),
        "summary.json": _json_bytes(result.summary),
    }
    manifest = {
        "schema_version": SIMULATOR_RUN_MANIFEST_SCHEMA_VERSION,
        "status": "COMPLETE",
        "scenario_id": scenario.scenario_id,
        "scenario_sha256": scenario.source_sha256,
        "plan_sha256": result.plan["plan_sha256"],
        "rate_card_id": scenario.rate_card.rate_card_id,
        "rate_card_provenance": scenario.rate_card.provenance,
        "calibration_provenance": scenario.calibration_provenance,
        "planned_trials": scenario.planned_trial_count,
        "canonical_records": len(result.canonical_records),
        "events": len(result.events),
        "output_sha256": {
            name: _sha256_bytes(value)
            for name, value in sorted(documents.items())
        },
        "simulated": True,
        "flowmesh_deployed": False,
        "external_services_called": False,
        "llm_called": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    documents["run_manifest.json"] = _json_bytes(manifest)
    documents["SHA256SUMS"] = "".join(
        f"{_sha256_bytes(value)}  {name}\n"
        for name, value in sorted(documents.items())
    ).encode("utf-8")
    return documents


def _verify_documents(root: Path, documents: dict[str, bytes]) -> None:
    actual = {
        path.name
        for path in root.iterdir()
        if path.is_file()
    }
    if actual != set(documents):
        raise RuntimeError("simulator output file set changed during publication")
    for name, expected in documents.items():
        if (root / name).read_bytes() != expected:
            raise RuntimeError(f"simulator output verification failed: {name}")
    checksums = (root / "SHA256SUMS").read_text(encoding="utf-8").splitlines()
    expected_names = set(documents) - {"SHA256SUMS"}
    found_names: set[str] = set()
    for line in checksums:
        digest, separator, name = line.partition("  ")
        if separator != "  " or not name or name in found_names:
            raise RuntimeError("simulator SHA256SUMS is malformed")
        found_names.add(name)
        if _sha256_bytes((root / name).read_bytes()) != digest:
            raise RuntimeError(f"simulator checksum mismatch: {name}")
    if found_names != expected_names:
        raise RuntimeError("simulator SHA256SUMS does not bind every output")


def run_simulator_scenario(
    scenario_path: str | Path,
    *,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Run one scenario and atomically publish deterministic evidence.

    Existing output is refused.  This keeps two configurations or attempts
    from being silently mixed into a directory that looks like one run.
    """

    scenario = load_simulator_scenario(scenario_path)
    target = Path(output_dir).resolve()
    if target.exists():
        raise RuntimeError(
            f"simulator output directory already exists: {target}"
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    result = run_discrete_event_simulation(scenario)
    documents = _documents(scenario, result)
    temporary = Path(tempfile.mkdtemp(
        prefix=f".{target.name}.tmp-",
        dir=target.parent,
    ))
    try:
        for name, value in documents.items():
            path = temporary / name
            with path.open("wb") as handle:
                handle.write(value)
                handle.flush()
                os.fsync(handle.fileno())
        _verify_documents(temporary, documents)
        temporary.replace(target)
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise

    manifest = json.loads((target / "run_manifest.json").read_text(encoding="utf-8"))
    return {
        **manifest,
        "output_dir": str(target),
        "plan_path": str(target / "simulator_plan.json"),
        "events_path": str(target / "events.jsonl"),
        "canonical_records_path": str(target / "canonical_records.jsonl"),
        "summary_path": str(target / "summary.json"),
        "checksums_path": str(target / "SHA256SUMS"),
    }


def verify_simulator_run(output_dir: str | Path) -> dict[str, Any]:
    """Read-only verification of a published simulator output directory."""

    root = Path(output_dir).resolve()
    if not root.is_dir():
        raise RuntimeError(f"simulator output directory does not exist: {root}")
    checksum_path = root / "SHA256SUMS"
    if not checksum_path.is_file():
        raise RuntimeError("simulator output has no SHA256SUMS")
    checked = 0
    for line in checksum_path.read_text(encoding="utf-8").splitlines():
        digest, separator, name = line.partition("  ")
        if separator != "  " or not name or Path(name).name != name:
            raise RuntimeError("simulator SHA256SUMS is malformed")
        path = root / name
        if not path.is_file() or _sha256_bytes(path.read_bytes()) != digest:
            raise RuntimeError(f"simulator checksum mismatch: {name}")
        checked += 1
    manifest = json.loads((root / "run_manifest.json").read_text(encoding="utf-8"))
    if manifest.get("status") != "COMPLETE":
        raise RuntimeError("simulator run manifest is not complete")
    return {
        "status": "VERIFIED",
        "scenario_id": manifest["scenario_id"],
        "checked_files": checked,
        "flowmesh_deployed": manifest["flowmesh_deployed"],
        "eligible_for_scientific_claims": manifest[
            "eligible_for_scientific_claims"
        ],
    }
