"""Independently verify and seal one frozen seven-question route batch."""

from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path

from pathfinder.integrations.flowmesh.semantic_matrix_trial import (
    verify_semantic_route_evidence,
)
from pathfinder.simulator.full_flow_matrix_runner import _assert_public_evidence

from experiments.multiq_pilot_20260924.run_24route_pilot import _canonical
from experiments.multiq_pilot_20260924.run_28route_sealed import (
    _baseline, _inputs,
)


def _read(path: Path) -> dict:
    return json.loads(path.read_bytes())


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("route timestamp has no UTC offset")
    return parsed


def verify(
    artifact_root: Path, baseline_spec_dir: Path, output_dir: Path,
    *, seal: bool = False,
) -> dict:
    report, trials, stages, routes, episodes = _inputs(artifact_root)
    baseline_digest = _baseline(baseline_spec_dir, report)
    root = output_dir.resolve()
    if not root.is_dir() or (root / "failure.json").exists():
        raise ValueError("route output is missing or failed")
    expected = {
        "start.json", "summary.json", "SHA256SUMS",
        *(f"route-{i:02d}.json" for i in range(28)),
        *(f"timing-{i:02d}.json" for i in range(28)),
    }
    actual = {path.name for path in root.iterdir()}
    if actual != (expected - {"SHA256SUMS"} if seal else expected):
        raise ValueError("seven-question output file set differs")
    start = _read(root / "start.json")
    summary = _read(root / "summary.json")
    for record, required_status in (
        (start, "STARTED"),
        (summary, "VERIFIED_28_ROUTE_EXECUTION"),
    ):
        if (
            record.get("status") != required_status
            or record.get("admission_sha256") != report["admission_sha256"]
            or record.get("baseline_spec_sha256") != baseline_digest
            or record.get("worker_alias")
            != "pathfinder_costaware_20260815a"
            or record.get("credentials_recorded") is not False
        ):
            raise ValueError("batch start or summary binding differs")
    if (
        summary.get("route_count") != 28
        or summary.get("eligible_for_scientific_claims") is not False
        or summary.get("started_utc") != start.get("started_utc")
    ):
        raise ValueError("batch summary differs")
    batch_start = _utc(start["started_utc"])
    batch_end = _utc(summary["ended_utc"])
    if batch_end < batch_start:
        raise ValueError("batch time order differs")
    stage_by_key = {row["stage_key"]: row for row in stages}
    success = {arm: {True: 0, False: 0} for arm in ("R", "D", "DC", "I")}
    seen_execution = set()
    seen_flowmesh_evidence = set()
    prior_end = batch_start
    for ordinal, trial in enumerate(trials):
        result = _read(root / f"route-{ordinal:02d}.json")
        timing = _read(root / f"timing-{ordinal:02d}.json")
        _assert_public_evidence(result)
        route = routes[trial["trial_key"]]
        episode = episodes.get(trial["trial_key"])
        bound_stages = [stage_by_key[key]
                        for key in trial["semantic_stage_keys"]]
        evidence = verify_semantic_route_evidence(
            result["semantic_route_evidence"],
            run_id=route["run_id"], bound_trial=trial,
            bound_stages=bound_stages,
            cache_episode_id=(episode["cache_episode_id"] if episode else None),
        )
        key = hashlib.sha256(_canonical({
            "domain": "interleaved-admission-idempotency/v1",
            "run_id": route["run_id"], "trial_key": trial["trial_key"],
        })).hexdigest()
        if (
            result.get("status") != "COMPLETE"
            or result.get("trial_key") != trial["trial_key"]
            or result.get("idempotency_key") != key
            or result.get("route_evidence_sha256") != evidence["evidence_sha256"]
            or result.get("execution_transport") != "flowmesh"
            or result.get("n1_score_authenticity_verified") is not True
            or result.get("telemetry_complete") is not True
            or result.get("llm_called") is not True
            or result.get("credentials_recorded") is not False
            or result.get("eligible_for_scientific_claims") is not False
            or type(result.get("task_success")) is not bool
        ):
            raise ValueError(f"route {ordinal} public evidence differs")
        if (
            timing.get("ordinal") != ordinal
            or timing.get("trial_key") != trial["trial_key"]
            or type(timing.get("elapsed_ms")) is not int
            or timing["elapsed_ms"] < 0
        ):
            raise ValueError(f"route {ordinal} timing differs")
        route_start = _utc(timing["route_started_utc"])
        route_end = _utc(timing["route_ended_utc"])
        if not (prior_end <= route_start <= route_end <= batch_end):
            raise ValueError(f"route {ordinal} is not serial within batch")
        prior_end = route_end
        for identity, seen in (
            (result["semantic_route_evidence"].get("execution_id"),
             seen_execution),
            (result.get("flowmesh_workflow_evidence_sha256"),
             seen_flowmesh_evidence),
        ):
            if not isinstance(identity, str) or not identity or identity in seen:
                raise ValueError(f"route {ordinal} identity missing or reused")
            seen.add(identity)
        success[trial["design_id"]][result["task_success"]] += 1
    if any(sum(values.values()) != 7 for values in success.values()):
        raise ValueError("seven-question arm coverage differs")
    checksums = "".join(
        f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n"
        for path in sorted(root.iterdir()) if path.name != "SHA256SUMS"
    ).encode("ascii")
    if seal:
        with (root / "SHA256SUMS").open("xb") as handle:
            handle.write(checksums)
    if (root / "SHA256SUMS").read_bytes() != checksums:
        raise ValueError("route checksum manifest differs")
    return {
        "status": "VERIFIED_28_ROUTE_OUTPUT",
        "route_count": 28,
        "question_count": 7,
        "admission_sha256": report["admission_sha256"],
        "baseline_spec_sha256": baseline_digest,
        "success_by_arm": {
            arm: {"correct": values[True], "incorrect": values[False]}
            for arm, values in success.items()
        },
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--baseline-spec-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--seal", action="store_true")
    args = parser.parse_args()
    print(json.dumps(verify(
        args.artifact_root, args.baseline_spec_dir, args.output_dir,
        seal=args.seal,
    ), sort_keys=True))


if __name__ == "__main__":
    main()
