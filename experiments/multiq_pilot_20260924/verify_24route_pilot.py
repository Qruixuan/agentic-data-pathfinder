"""Offline, source-bound verification of the interleaved 24-route receipt."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from pathfinder.integrations.flowmesh.semantic_matrix_trial import (
    verify_semantic_route_evidence,
)
from pathfinder.simulator.full_flow_matrix_runner import _assert_public_evidence

from run_24route_pilot import _canonical, _inputs


def verify(artifact_root: Path, output_dir: Path, *, seal: bool = False) -> dict:
    report, trials, stages, routes, episodes = _inputs(artifact_root)
    root = output_dir.resolve()
    if not root.is_dir() or (root / "failure.json").exists():
        raise ValueError("route output is missing or failed")
    expected_files = {
        "admission-sha256.txt", "summary.json", "SHA256SUMS",
        *(f"route-{index:02d}.json" for index in range(24)),
    }
    actual_files = {p.name for p in root.iterdir()}
    if actual_files != (expected_files - {"SHA256SUMS"} if seal else expected_files):
        raise ValueError("24-route output file set differs")
    if (root / "admission-sha256.txt").read_bytes() != (
        report["admission_sha256"] + "\n"
    ).encode("ascii"):
        raise ValueError("receipt binds another admission")
    manifest = json.loads((root / "summary.json").read_bytes())
    if (
        manifest.get("status") != "VERIFIED_24_ROUTE_EXECUTION"
        or manifest.get("route_count") != 24
        or manifest.get("admission_sha256") != report["admission_sha256"]
        or manifest.get("credentials_recorded") is not False
        or manifest.get("eligible_for_scientific_claims") is not False
    ):
        raise ValueError("24-route summary is invalid")
    stage_by_key = {row["stage_key"]: row for row in stages}
    success_by_arm = {arm: {True: 0, False: 0} for arm in ("R", "D", "DC", "I")}
    for ordinal, trial in enumerate(trials):
        result = json.loads((root / f"route-{ordinal:02d}.json").read_bytes())
        _assert_public_evidence(result)
        route = routes[trial["trial_key"]]
        episode = episodes.get(trial["trial_key"])
        stages_for_trial = [stage_by_key[key]
                            for key in trial["semantic_stage_keys"]]
        evidence = verify_semantic_route_evidence(
            result["semantic_route_evidence"], run_id=route["run_id"],
            bound_trial=trial, bound_stages=stages_for_trial,
            cache_episode_id=(episode["cache_episode_id"] if episode else None),
        )
        expected_key = hashlib.sha256(_canonical({
            "domain": "interleaved-admission-idempotency/v1",
            "run_id": route["run_id"], "trial_key": trial["trial_key"],
        })).hexdigest()
        if (
            result.get("status") != "COMPLETE"
            or result.get("trial_key") != trial["trial_key"]
            or result.get("idempotency_key") != expected_key
            or result.get("route_evidence_sha256") != evidence["evidence_sha256"]
            or result.get("execution_transport") != "flowmesh"
            or result.get("n1_score_authenticity_verified") is not True
            or result.get("telemetry_complete") is not True
            or result.get("llm_called") is not True
            or result.get("credentials_recorded") is not False
            or result.get("eligible_for_scientific_claims") is not False
            or type(result.get("task_success")) is not bool
        ):
            raise ValueError(f"route {ordinal} result contract differs")
        success_by_arm[trial["design_id"]][result["task_success"]] += 1
    checksum_lines = []
    for path in sorted(p for p in root.iterdir() if p.name != "SHA256SUMS"):
        checksum_lines.append(
            f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n"
        )
    checksum_bytes = "".join(checksum_lines).encode("ascii")
    if seal:
        with (root / "SHA256SUMS").open("xb") as handle:
            handle.write(checksum_bytes)
            handle.flush()
    if (root / "SHA256SUMS").read_bytes() != checksum_bytes:
        raise ValueError("route output checksum manifest differs")
    return {
        "status": "VERIFIED_24_ROUTE_OUTPUT",
        "admission_sha256": report["admission_sha256"],
        "route_count": 24,
        "success_by_arm": {
            arm: {"correct": values[True], "incorrect": values[False]}
            for arm, values in success_by_arm.items()
        },
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--seal", action="store_true")
    args = parser.parse_args()
    print(json.dumps(verify(args.artifact_root, args.output_dir,
                            seal=args.seal), sort_keys=True))


if __name__ == "__main__":
    main()
