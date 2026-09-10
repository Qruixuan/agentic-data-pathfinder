"""Fit measured infrastructure evidence into a simulator scenario.

Only directly identified parameters are changed.  Storage observations update
storage latency/throughput, network observations update link RTT/bandwidth,
and model-timing observations update one explicitly named operation.  FlowMesh
route observations are retained as validation evidence and never decomposed
into unobserved internal components.
"""

from __future__ import annotations

import json
import math
import os
import shutil
import tempfile
from hashlib import sha256
from pathlib import Path
from statistics import median
from typing import Any, Mapping

from .config import load_simulator_scenario
from .evidence import (
    EVIDENCE_MANIFEST_SCHEMA_VERSION,
    EVIDENCE_OBSERVATION_SCHEMA_VERSION,
    verify_simulator_evidence_bundle,
)


FIT_REPORT_SCHEMA_VERSION = "pathfinder.flowmesh-infra-fit-report/v1alpha1"
FIT_MANIFEST_SCHEMA_VERSION = "pathfinder.flowmesh-infra-fit-run/v1alpha1"


class SimulatorFitError(ValueError):
    """Raised when evidence cannot be uniquely bound to scenario parameters."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise SimulatorFitError(message)


def _unique_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        _require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _invalid_number(value: str) -> None:
    raise SimulatorFitError(f"non-finite JSON number: {value}")


def _read_json(path: Path, name: str) -> tuple[bytes, Any]:
    try:
        raw = path.read_bytes()
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_unique_keys,
            parse_constant=_invalid_number,
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SimulatorFitError(f"cannot read valid {name}: {path}") from exc
    return raw, value


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise SimulatorFitError(f"cannot read evidence observations: {path}") from exc
    result: list[dict[str, Any]] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(
                line,
                object_pairs_hook=_unique_keys,
                parse_constant=_invalid_number,
            )
        except (json.JSONDecodeError, SimulatorFitError) as exc:
            raise SimulatorFitError(
                f"invalid evidence observation at line {line_number}: {exc}"
            ) from exc
        _require(isinstance(value, dict), "evidence observation is not an object")
        result.append(value)
    _require(bool(result), "evidence observation file is empty")
    return result


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    _require(isinstance(value, Mapping), f"{name} must be an object")
    return value


def _text(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and bool(value.strip()),
        f"{name} must be a non-empty string",
    )
    return value.strip()


def _sha256_bytes(value: bytes) -> str:
    return sha256(value).hexdigest()


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


def _metric(
    observation: Mapping[str, Any],
    name: str,
    unit: str,
) -> tuple[list[float], Mapping[str, Any]]:
    metrics = _mapping(observation.get("metrics"), "observation.metrics")
    item = _mapping(metrics.get(name), f"observation.metrics.{name}")
    _require(item.get("unit") == unit, f"{name} has an unexpected unit")
    values = item.get("samples")
    _require(isinstance(values, list) and values, f"{name} has no samples")
    samples: list[float] = []
    for value in values:
        _require(
            type(value) in (int, float)
            and math.isfinite(float(value))
            and float(value) > 0.0,
            f"{name} contains an invalid sample",
        )
        samples.append(float(value))
    return samples, _mapping(item.get("summary"), f"{name}.summary")


def _relative_p95_jitter(
    primary: tuple[list[float], Mapping[str, Any]],
    secondary: tuple[list[float], Mapping[str, Any]] | None = None,
) -> float:
    values = [primary]
    if secondary is not None:
        values.append(secondary)
    deviations = []
    for samples, summary in values:
        center = median(samples)
        p95 = float(summary["p95"])
        deviations.append(max(0.0, p95 / center - 1.0))
    return min(0.999999, max(deviations))


def _resources(scenario: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for node in scenario.get("nodes", []):
        node_id = _text(node.get("node_id"), "scenario node_id")
        for resource in node.get("resources", []):
            resource_id = _text(resource.get("resource_id"), "resource_id")
            _require(resource_id not in result, f"duplicate resource: {resource_id}")
            resource["_fit_node_id"] = node_id
            result[resource_id] = resource
    return result


def _links(scenario: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for link in scenario.get("links", []):
        link_id = _text(link.get("link_id"), "link_id")
        _require(link_id not in result, f"duplicate link: {link_id}")
        result[link_id] = link
    return result


def _templates(scenario: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for template in scenario.get("operation_templates", []):
        template_id = _text(template.get("template_id"), "template_id")
        _require(template_id not in result, f"duplicate template: {template_id}")
        result[template_id] = template
    return result


def _fit_storage(
    observation: Mapping[str, Any],
    resources: Mapping[str, dict[str, Any]],
) -> dict[str, Any]:
    target = _mapping(observation.get("target"), "storage target")
    resource_id = _text(target.get("resource_id"), "storage resource_id")
    _require(resource_id in resources, f"unknown storage resource: {resource_id}")
    resource = resources[resource_id]
    _require(resource.get("kind") == "storage", f"{resource_id} is not storage")
    _require(
        resource.pop("_fit_node_id") == target.get("node_id"),
        f"storage node mismatch: {resource_id}",
    )
    latency = _metric(observation, "base_latency_ms", "ms")
    throughput = _metric(
        observation,
        "throughput_bytes_per_second",
        "bytes/second",
    )
    previous = {
        "base_latency_ms": resource.get("base_latency_ms", 0.0),
        "throughput_bytes_per_second": resource.get(
            "throughput_bytes_per_second"
        ),
        "jitter_fraction": resource.get("jitter_fraction", 0.0),
    }
    fitted = {
        "base_latency_ms": median(latency[0]),
        "throughput_bytes_per_second": median(throughput[0]),
        "jitter_fraction": _relative_p95_jitter(latency, throughput),
    }
    resource.update(fitted)
    return {
        "measurement_kind": "storage",
        "target": dict(target),
        "source_observation_id": observation["observation_id"],
        "sample_count": min(len(latency[0]), len(throughput[0])),
        "previous_parameters": previous,
        "fitted_parameters": fitted,
        "uncertainty_summary": {
            "latency": dict(latency[1]),
            "throughput": dict(throughput[1]),
        },
    }


def _fit_network(
    observation: Mapping[str, Any],
    links: Mapping[str, dict[str, Any]],
) -> dict[str, Any]:
    target = _mapping(observation.get("target"), "network target")
    link_id = _text(target.get("link_id"), "network link_id")
    _require(link_id in links, f"unknown network link: {link_id}")
    link = links[link_id]
    _require(
        link.get("source_node_id") == target.get("source_node_id")
        and link.get("destination_node_id") == target.get("destination_node_id"),
        f"network endpoint mismatch: {link_id}",
    )
    bandwidth = _metric(
        observation,
        "bandwidth_bytes_per_second",
        "bytes/second",
    )
    rtt = _metric(observation, "round_trip_time_ms", "ms")
    previous = {
        "bandwidth_bytes_per_second": link["bandwidth_bytes_per_second"],
        "round_trip_time_ms": link["round_trip_time_ms"],
        "jitter_fraction": link.get("jitter_fraction", 0.0),
    }
    fitted = {
        "bandwidth_bytes_per_second": median(bandwidth[0]),
        "round_trip_time_ms": median(rtt[0]),
        "jitter_fraction": _relative_p95_jitter(bandwidth, rtt),
    }
    link.update(fitted)
    return {
        "measurement_kind": "network",
        "target": dict(target),
        "source_observation_id": observation["observation_id"],
        "sample_count": min(len(bandwidth[0]), len(rtt[0])),
        "previous_parameters": previous,
        "fitted_parameters": fitted,
        "uncertainty_summary": {
            "bandwidth": dict(bandwidth[1]),
            "round_trip_time": dict(rtt[1]),
        },
    }


def _fit_compute(
    observation: Mapping[str, Any],
    templates: Mapping[str, dict[str, Any]],
    resources: Mapping[str, dict[str, Any]],
) -> dict[str, Any]:
    target = _mapping(observation.get("target"), "compute target")
    template_id = _text(target.get("template_id"), "compute template_id")
    op_id = _text(target.get("op_id"), "compute op_id")
    resource_id = _text(target.get("resource_id"), "compute resource_id")
    _require(template_id in templates, f"unknown template: {template_id}")
    _require(resource_id in resources, f"unknown compute resource: {resource_id}")
    resource = resources[resource_id]
    node = resource.pop("_fit_node_id")
    _require(node == target.get("node_id"), f"compute node mismatch: {resource_id}")
    _require(
        resource.get("kind") in ("cpu", "gpu"),
        f"{resource_id} is not a compute resource",
    )
    operations = [
        operation
        for operation in templates[template_id].get("operations", [])
        if operation.get("op_id") == op_id
    ]
    _require(len(operations) == 1, f"compute operation is not unique: {op_id}")
    operation = operations[0]
    _require(operation.get("kind") == "compute", f"{op_id} is not compute")
    configured = operation.get("resource_id")
    _require(
        configured == resource_id or str(configured).startswith("$"),
        f"compute resource mismatch: {template_id}/{op_id}",
    )
    timing = _metric(observation, "service_time_ms", "ms")
    previous = {"service_ms": operation.get("service_ms", 0.0)}
    fitted = {"service_ms": median(timing[0])}
    operation.update(fitted)
    return {
        "measurement_kind": "compute-operation",
        "target": dict(target),
        "source_observation_id": observation["observation_id"],
        "sample_count": len(timing[0]),
        "previous_parameters": previous,
        "fitted_parameters": fitted,
        "uncertainty_summary": {"service_time": dict(timing[1])},
    }


def _strip_internal_fields(scenario: Mapping[str, Any]) -> None:
    for node in scenario.get("nodes", []):
        for resource in node.get("resources", []):
            resource.pop("_fit_node_id", None)


def _verify_output(root: Path) -> dict[str, Any]:
    expected = {
        "calibrated_scenario.json",
        "fit_report.json",
        "fit_manifest.json",
    }
    actual = {path.name for path in root.iterdir() if path.is_file()}
    _require(actual == expected | {"SHA256SUMS"}, "fit output set changed")
    checksums: dict[str, str] = {}
    for line in (root / "SHA256SUMS").read_text(encoding="utf-8").splitlines():
        digest, separator, name = line.partition("  ")
        _require(separator == "  " and name in expected, "malformed SHA256SUMS")
        _require(name not in checksums, f"duplicate fit checksum: {name}")
        _require(
            _sha256_bytes((root / name).read_bytes()) == digest,
            f"fit checksum mismatch: {name}",
        )
        checksums[name] = digest
    _require(set(checksums) == expected, "fit checksums are incomplete")
    _, value = _read_json(root / "fit_manifest.json", "fit manifest")
    manifest = _mapping(value, "fit manifest")
    _require(
        manifest.get("schema_version") == FIT_MANIFEST_SCHEMA_VERSION,
        "unsupported fit manifest schema_version",
    )
    _require(manifest.get("status") == "COMPLETE", "fit run incomplete")
    _require(
        manifest.get("output_sha256")
        == {
            name: digest
            for name, digest in checksums.items()
            if name != "fit_manifest.json"
        },
        "fit manifest digests disagree",
    )
    load_simulator_scenario(root / "calibrated_scenario.json")
    return dict(manifest)


def fit_simulator_scenario(
    scenario_path: str | Path,
    evidence_dir: str | Path,
    *,
    output_scenario_id: str,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Fit direct evidence and publish a new validated scenario."""
    scenario_source = Path(scenario_path).resolve()
    scenario_model = load_simulator_scenario(scenario_source)
    scenario_raw, scenario_value = _read_json(scenario_source, "scenario")
    scenario = dict(_mapping(scenario_value, "scenario"))
    new_id = _text(output_scenario_id, "output_scenario_id")
    _require(new_id != scenario_model.scenario_id, "output scenario_id must change")
    evidence_root = Path(evidence_dir).resolve()
    evidence_manifest = verify_simulator_evidence_bundle(evidence_root)
    _require(
        evidence_manifest.get("schema_version")
        == EVIDENCE_MANIFEST_SCHEMA_VERSION,
        "evidence manifest schema mismatch",
    )
    observations = _read_jsonl(evidence_root / "evidence_observations.jsonl")
    resources = _resources(scenario)
    links = _links(scenario)
    templates = _templates(scenario)
    reports: list[dict[str, Any]] = []
    validation: list[dict[str, Any]] = []
    fitted_targets: set[tuple[str, str]] = set()
    for observation in observations:
        _require(
            observation.get("schema_version")
            == EVIDENCE_OBSERVATION_SCHEMA_VERSION,
            "unsupported evidence observation schema_version",
        )
        kind = _text(observation.get("measurement_kind"), "measurement_kind")
        if observation.get("calibration_role") != "direct":
            validation.append({
                "observation_id": observation["observation_id"],
                "measurement_kind": kind,
                "target": observation["target"],
                "role": observation["calibration_role"],
            })
            continue
        target = _mapping(observation.get("target"), "observation.target")
        if kind == "storage":
            identity = (kind, str(target.get("resource_id")))
        elif kind == "network":
            identity = (kind, str(target.get("link_id")))
        elif kind == "compute-operation":
            identity = (
                kind,
                f"{target.get('template_id')}/{target.get('op_id')}",
            )
        else:
            raise SimulatorFitError(f"unsupported direct evidence kind: {kind}")
        _require(identity not in fitted_targets, f"duplicate fit target: {identity}")
        fitted_targets.add(identity)
        if kind == "storage":
            reports.append(_fit_storage(observation, resources))
        elif kind == "network":
            reports.append(_fit_network(observation, links))
        else:
            reports.append(_fit_compute(observation, templates, resources))
    _strip_internal_fields(scenario)
    _require(bool(reports), "evidence bundle has no direct calibration evidence")
    scenario["scenario_id"] = new_id
    scenario["calibration_provenance"] = (
        "measured-evidence-bundle:"
        + str(evidence_manifest["evidence_id"])
    )
    scenario_bytes = _json_bytes(scenario)
    report = {
        "schema_version": FIT_REPORT_SCHEMA_VERSION,
        "status": "COMPLETE",
        "base_scenario_id": scenario_model.scenario_id,
        "output_scenario_id": new_id,
        "evidence_id": evidence_manifest["evidence_id"],
        "fitted_target_count": len(reports),
        "validation_only_observation_count": len(validation),
        "fitted_targets": reports,
        "validation_observations": validation,
        "retained_provisional_parameter_classes": [
            "arrival_process",
            "cache_capacity_and_initial_state",
            "resource_and_link_slots",
            "rate_card",
            "task_success_and_quality",
            "unmeasured_resources_links_and_operations",
        ],
        "point_estimates_use_sample_medians": True,
        "p95_retained_in_uncertainty_summary": True,
        "flowmesh_end_to_end_latency_decomposed": False,
        "rate_card_calibrated": False,
        "simulator_results_are_not_real_measurements": True,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    documents = {
        "calibrated_scenario.json": scenario_bytes,
        "fit_report.json": _json_bytes(report),
    }
    manifest = {
        "schema_version": FIT_MANIFEST_SCHEMA_VERSION,
        "status": "COMPLETE",
        "base_scenario_sha256": _sha256_bytes(scenario_raw),
        "output_scenario_id": new_id,
        "evidence_id": evidence_manifest["evidence_id"],
        "evidence_manifest_sha256": _sha256_bytes(
            (evidence_root / "evidence_manifest.json").read_bytes()
        ),
        "fitted_target_count": len(reports),
        "external_services_called": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
        "output_sha256": {
            name: _sha256_bytes(content)
            for name, content in sorted(documents.items())
        },
    }
    documents["fit_manifest.json"] = _json_bytes(manifest)
    documents["SHA256SUMS"] = "".join(
        f"{_sha256_bytes(content)}  {name}\n"
        for name, content in sorted(documents.items())
    ).encode("utf-8")
    target_dir = Path(output_dir).resolve()
    _require(not target_dir.exists(), f"fit output already exists: {target_dir}")
    target_dir.parent.mkdir(parents=True, exist_ok=True)
    staging_parent = Path(tempfile.mkdtemp(prefix=".fit-", dir=target_dir.parent))
    staging = staging_parent / "output"
    try:
        staging.mkdir()
        for name, content in documents.items():
            (staging / name).write_bytes(content)
        _verify_output(staging)
        _require(not target_dir.exists(), f"fit output already exists: {target_dir}")
        os.replace(staging, target_dir)
    finally:
        shutil.rmtree(staging_parent, ignore_errors=True)
    return {
        "status": "COMPLETE",
        "output_scenario_id": new_id,
        "evidence_id": evidence_manifest["evidence_id"],
        "fitted_target_count": len(reports),
        "validation_only_observation_count": len(validation),
        "output_dir": str(target_dir),
        "external_services_called": False,
        "eligible_for_scientific_claims": False,
    }


def verify_simulator_fit(output_dir: str | Path) -> dict[str, Any]:
    """Verify the exact immutable output set of an evidence fit."""
    root = Path(output_dir).resolve()
    _require(root.is_dir(), f"fit output does not exist: {root}")
    manifest = _verify_output(root)
    return {
        "status": "VERIFIED",
        "output_scenario_id": manifest["output_scenario_id"],
        "evidence_id": manifest["evidence_id"],
        "fitted_target_count": manifest["fitted_target_count"],
        "checked_files": 3,
        "eligible_for_scientific_claims": False,
    }
