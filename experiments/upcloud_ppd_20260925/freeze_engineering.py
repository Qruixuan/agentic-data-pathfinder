"""Freeze an isolated, one-object UpCloud PPD engineering deployment.

This reuses verified media from an existing portable N3/N4 package. It does
not call a model, submit FlowMesh work, or modify the source packages.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Mapping

from pathfinder.config import load_config
from pathfinder.data_agent_manifest import load_data_agent_manifest
from pathfinder.distributed.registry import load_endpoint_registry
from pathfinder.simulator.n4_derived_data_plane import (
    N4ArtifactProvenance,
    N4DerivedArtifactInput,
    build_n4_derived_data_package,
    verify_n4_derived_data_package,
)
from pathfinder.simulator.raw_cold_data_plane import (
    RawColdObjectBinding,
    build_raw_cold_data_plane_package,
    verify_raw_cold_data_plane_package,
)

PLANS = ("PPD_LOCAL_DIGEST", "PPD_REMOTE_DIGEST")
REPS = ("raw_video", "sampled_frame_bundle", "multimodal_digest")


def _json(path: Path) -> dict:
    return json.loads(path.read_bytes())


def _write_json(path: Path, value: dict) -> None:
    path.write_bytes(
        (json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False)
         + "\n").encode("utf-8")
    )


def _read_bound_artifact(root: Path, row: dict) -> Path:
    relative = Path(row["artifact_package_path"])
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError("source artifact escapes its package")
    path = root / relative
    data = path.read_bytes()
    if (len(data) != row["artifact_size_bytes"] or
            hashlib.sha256(data).hexdigest() != row["artifact_sha256"]):
        raise ValueError("source artifact identity differs")
    return path


def _verify_source_sums(root: Path) -> None:
    checksums = (root / "SHA256SUMS").read_text(encoding="utf-8")
    for line in checksums.splitlines():
        if not line:
            continue
        digest, relative = line.split("  ", 1)
        relative = relative.lstrip("*")
        path = Path(relative)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError("checksum path escapes its package")
        actual = hashlib.sha256((root / path).read_bytes()).hexdigest()
        if actual != digest:
            raise ValueError("source package checksum differs")


def _config(sizes: dict[str, int]) -> dict:
    representations = [
        {"id": rep, "description": "Verified PPD engineering artifact",
         "size_bytes": sizes[rep], "task_quality": {"video_qa": 0.5}}
        for rep in REPS
    ]
    designs = []
    for design_id in PLANS:
        paths = {}
        for rep in REPS:
            # These are engineering quote knobs, not measured costs/latencies.
            quote = {"raw_video": 4, "sampled_frame_bundle": 3,
                     "multimodal_digest": 2}[rep]
            paths[rep] = {
                "available": True,
                # This is the Data Agent's logical binding location, not
                # the endpoint's physical placement. The registry below
                # records N4 remote versus N7 replica separately.
                "location": "origin-cold" if rep == "raw_video"
                else "origin-warm",
                "latency_ms": 1, "latency_jitter_ms": 0,
                "realized_cost": 0,
                "quotes": {"video_qa": quote},
            }
        designs.append({"id": design_id, "description":
                        "Isolated PPD engineering placement", "paths": paths})
    return {
        "schema_version": "1.0",
        "price_universe_version": "ppd-engineering-quotes-v1-not-measured",
        "objective": {"resource_cost_weight": 1.0},
        "representations": representations,
        "task_classes": [{"id": "video_qa", "description":
                          "One public video question; engineering only",
                          "candidate_representations": list(REPS),
                          "access_budget": 6, "max_accesses": 1,
                          "task_value": 10, "quality_weight": 1,
                          "price_weight": 1, "outside_option_utility": 0,
                          "choice_noise": 0, "success_midpoint": 0.5,
                          "success_temperature": 0.1,
                          "latency_reference_ms": 1,
                          "latency_success_penalty_per_ms": 0}],
        "price_universes": {"video_qa": {
            "raw_video": [4], "sampled_frame_bundle": [3],
            "multimodal_digest": [2]}},
        "physical_designs": designs,
        "quote_profiles": [{"id": "as_designed", "description":
                            "Engineering quotes only", "overrides": {}}],
        "pilot": {"design_ids": list(PLANS),
                  "task_class_ids": ["video_qa"],
                  "quote_profile_ids": ["as_designed"],
                  "latency_multipliers": [1.0],
                  "trials_per_cell": 1, "base_seed": 5201},
    }


def _registry() -> dict:
    endpoints = [
        ("n3_raw", "N3", "n3-remote-origin", "remote", "N3"),
        ("n4_remote", "N4", "n4-remote-origin", "remote", "N4"),
        ("n4_n7_replica", "N4", "n7-local-replica", "local", "N7"),
    ]
    return {
        "schema_version": "pathfinder.data-agent-endpoint-registry/v1alpha1",
        "registry_id": "upcloud-ppd-engineering-v1",
        "execution_node_id": "N7",
        "endpoints": [{
            "endpoint_id": name, "node_id": node, "location": location,
            "description": location, "network_transport": transport,
            "network_zero_justification": (
                "Data Agent replica and Gateway share the N7 host"
                if transport == "local" else None),
            "base_url_env": f"PATHFINDER_PPD_{suffix}_URL",
            "token_env": f"PATHFINDER_PPD_{suffix}_TOKEN",
            "private_http_service_name":
                f"pathfinder-full-flow-ppd-{name.replace('_', '-')}",
            "timeout_seconds": 30, "max_retries": 1,
            "max_artifact_bytes": 16 * 1024 * 1024,
            "telemetry_capabilities": ["access", "artifact-download"],
        } for name, node, location, transport, suffix in endpoints],
        "placement": [
            {"design_id": design, "representation_id": rep,
             "endpoint_id": (
                 "n3_raw" if rep == "raw_video" else
                 "n4_n7_replica" if design == PLANS[0]
                 and rep == "multimodal_digest" else "n4_remote")}
            for design in PLANS for rep in REPS
        ],
    }


def _verify_access_bindings(
    config_path: Path,
    registry_path: Path,
    package_by_endpoint: Mapping[str, Path],
    object_id: str,
) -> None:
    """Fail before sealing if any routed access would fail manifest lookup."""
    config = load_config(config_path)
    registry = load_endpoint_registry(registry_path)
    if set(package_by_endpoint) != set(registry.endpoint_ids):
        raise ValueError("PPD endpoint package set differs from registry")
    manifests = {
        endpoint_id: load_data_agent_manifest(
            package / "config" / "data-agent-manifest.json"
        )
        for endpoint_id, package in package_by_endpoint.items()
    }
    for design_id, design in config.designs.items():
        for representation_id, path in design.paths.items():
            route = registry.route(
                design_id=design_id,
                representation_id=representation_id,
            )
            manifests[route.endpoint_id].resolve(
                plan_id=design_id,
                object_id=object_id,
                representation_id=representation_id,
                requested_location=path.location,
            )


def freeze(n3: Path, n4: Path, object_id: str, output: Path) -> None:
    if output.exists():
        raise ValueError("output already exists; use a new immutable directory")
    _verify_source_sums(n3)
    _verify_source_sums(n4)
    raw_package = _json(n3 / "raw-cold-data-plane.json")
    raw_rows = raw_package.get("raw_objects", raw_package.get("objects"))
    if not isinstance(raw_rows, list):
        raise ValueError("N3 source package has no raw object list")
    raw = next(row for row in raw_rows if row["object_id"] == object_id)
    derived_rows = _json(n4 / "n4-derived-data-package.json")["objects"]
    derived = [row for row in derived_rows if row["object_id"] == object_id]
    if {row["representation_id"] for row in derived} != set(REPS[1:]):
        raise ValueError("N4 source lacks required digest and frame bundle")
    raw_path = _read_bound_artifact(n3, raw)
    artifacts = [
        N4DerivedArtifactInput.from_path(
            object_id=object_id, representation_id=row["representation_id"],
            artifact_path=_read_bound_artifact(n4, row), plan_ids=PLANS,
            provenance=N4ArtifactProvenance.from_dict(row["provenance"]),
            expected_sha256=row["artifact_sha256"],
            expected_size_bytes=row["artifact_size_bytes"],
        ) for row in derived
    ]
    output.mkdir(parents=True)
    build_raw_cold_data_plane_package([
        RawColdObjectBinding(
            object_id=object_id, artifact_path=raw_path,
            catalog_version="upcloud-ppd-engineering-n3-v1", plan_ids=PLANS,
            dataset_id=raw["provenance"]["dataset_id"],
            dataset_revision=raw["provenance"]["dataset_revision"],
            source_object_id=raw["provenance"]["source_object_id"],
            artifact_sha256=raw["artifact_sha256"],
            artifact_size_bytes=raw["artifact_size_bytes"],
        )], output_dir=output / "n3", package_id="upcloud-ppd-engineering-n3-v1")
    build_n4_derived_data_package(
        artifacts, output_dir=output / "n4",
        package_id="upcloud-ppd-engineering-n4-v1",
        catalog_version="upcloud-ppd-engineering-n4-v1",
    )
    verify_raw_cold_data_plane_package(output / "n3")
    verify_n4_derived_data_package(output / "n4")
    sizes = {"raw_video": raw["artifact_size_bytes"]}
    sizes.update({row["representation_id"]: row["artifact_size_bytes"]
                  for row in derived})
    _write_json(output / "system.json", _config(sizes))
    _write_json(output / "endpoint-registry.json", _registry())
    _verify_access_bindings(
        output / "system.json",
        output / "endpoint-registry.json",
        {
            "n3_raw": output / "n3",
            "n4_remote": output / "n4",
            "n4_n7_replica": output / "n4",
        },
        object_id,
    )
    _write_json(output / "receipt.json", {
        "status": "FROZEN_PPD_ENGINEERING_INPUTS_NOT_DEPLOYED",
        "object_id": object_id, "plan_ids": list(PLANS),
        "representations": list(REPS),
        "source_n3_package_sha256": hashlib.sha256(
            (n3 / "SHA256SUMS").read_bytes()).hexdigest(),
        "source_n4_package_sha256": hashlib.sha256(
            (n4 / "SHA256SUMS").read_bytes()).hexdigest(),
        "quote_and_latency_measured": False,
        "workflow_submitted": False, "llm_called": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    })
    names = ("system.json", "endpoint-registry.json", "receipt.json")
    (output / "SHA256SUMS").write_bytes("".join(
        f"{hashlib.sha256((output / name).read_bytes()).hexdigest()}  {name}\n"
        for name in names).encode("ascii"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-n3", type=Path, required=True)
    parser.add_argument("--source-n4", type=Path, required=True)
    parser.add_argument("--object-id", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    freeze(args.source_n3, args.source_n4, args.object_id, args.output_dir)
