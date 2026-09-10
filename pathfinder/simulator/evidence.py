"""Unified, immutable calibration evidence for the infrastructure simulator.

The evidence bundle normalizes a deliberately small subset of fio, iperf3,
model-timing, and already-sanitized FlowMesh trace output.  It preserves raw
sample vectors and units; it does not turn end-to-end latency into fictional
disk, network, or model components.
"""

from __future__ import annotations

import json
import math
import os
import shutil
import tempfile
from collections import Counter, defaultdict
from hashlib import sha256
from pathlib import Path, PurePosixPath
from statistics import mean, median
from typing import Any, Iterable, Mapping

from .trace_import import (
    FLOWMESH_ACCESS_OBSERVATION_SCHEMA_VERSION,
    verify_flowmesh_trace_import,
)


EVIDENCE_SPEC_SCHEMA_VERSION = (
    "pathfinder.flowmesh-infra-evidence-spec/v1alpha1"
)
EVIDENCE_OBSERVATION_SCHEMA_VERSION = (
    "pathfinder.flowmesh-infra-evidence-observation/v1alpha1"
)
EVIDENCE_SUMMARY_SCHEMA_VERSION = (
    "pathfinder.flowmesh-infra-evidence-summary/v1alpha1"
)
EVIDENCE_MANIFEST_SCHEMA_VERSION = (
    "pathfinder.flowmesh-infra-evidence-bundle/v1alpha1"
)
MODEL_TIMING_SCHEMA_VERSION = (
    "pathfinder.model-timing-observation/v1alpha1"
)

SOURCE_KINDS = (
    "fio",
    "iperf3",
    "model-timing-jsonl",
    "flowmesh-trace-import",
)
DIRECT_KINDS = ("storage", "network", "compute-operation")
_SENSITIVE_KEY_PARTS = (
    "api_key",
    "authorization",
    "bearer",
    "credential",
    "password",
    "private_key",
    "secret",
    "token",
)


class SimulatorEvidenceError(ValueError):
    """Raised when measurement evidence is unsafe or cannot be normalized."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise SimulatorEvidenceError(message)


def _unique_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        _require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _invalid_number(value: str) -> None:
    raise SimulatorEvidenceError(f"non-finite JSON number: {value}")


def _read_json(path: Path, name: str) -> tuple[bytes, Any]:
    try:
        raw = path.read_bytes()
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_unique_keys,
            parse_constant=_invalid_number,
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SimulatorEvidenceError(f"cannot read valid {name}: {path}") from exc
    return raw, value


def _read_jsonl(path: Path, name: str) -> tuple[bytes, list[dict[str, Any]]]:
    try:
        raw = path.read_bytes()
        text = raw.decode("utf-8")
    except (OSError, UnicodeError) as exc:
        raise SimulatorEvidenceError(f"cannot read UTF-8 {name}: {path}") from exc
    values: list[dict[str, Any]] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(
                line,
                object_pairs_hook=_unique_keys,
                parse_constant=_invalid_number,
            )
        except (json.JSONDecodeError, SimulatorEvidenceError) as exc:
            raise SimulatorEvidenceError(
                f"invalid {name} at line {line_number}: {exc}"
            ) from exc
        _require(isinstance(value, dict), f"{name}:{line_number} is not an object")
        values.append(value)
    _require(bool(values), f"{name} is empty")
    return raw, values


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    _require(isinstance(value, Mapping), f"{name} must be an object")
    return value


def _array(value: Any, name: str) -> list[Any]:
    _require(isinstance(value, list), f"{name} must be an array")
    return value


def _text(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and bool(value.strip()),
        f"{name} must be a non-empty string",
    )
    return value.strip()


def _number(value: Any, name: str, *, positive: bool = True) -> float:
    _require(
        type(value) in (int, float) and math.isfinite(float(value)),
        f"{name} must be a finite number",
    )
    result = float(value)
    _require(
        result > 0.0 if positive else result >= 0.0,
        f"{name} is out of range",
    )
    return result


def _samples(value: Any, name: str) -> list[float]:
    result = [
        _number(item, f"{name}[{index}]")
        for index, item in enumerate(_array(value, name))
    ]
    _require(bool(result), f"{name} must not be empty")
    return result


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


def _jsonl_bytes(values: Iterable[Mapping[str, Any]]) -> bytes:
    return "".join(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
        for value in values
    ).encode("utf-8")


def _contained(root: Path, relative: str, name: str) -> Path:
    candidate = PurePosixPath(relative)
    _require(
        not candidate.is_absolute() and ".." not in candidate.parts,
        f"{name} must be a contained relative path",
    )
    base = root.resolve()
    resolved = base.joinpath(*candidate.parts).resolve()
    _require(
        resolved == base or base in resolved.parents,
        f"{name} escapes the evidence-spec directory",
    )
    return resolved


def _reject_sensitive_keys(value: Any, name: str = "input") -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            lowered = str(key).casefold()
            if lowered == "credentials_recorded":
                _require(
                    item is False,
                    f"{name}.credentials_recorded must be literal false",
                )
                continue
            _require(
                not any(part in lowered for part in _SENSITIVE_KEY_PARTS),
                f"{name} contains credential-like field {key!r}",
            )
            _reject_sensitive_keys(item, f"{name}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _reject_sensitive_keys(item, f"{name}[{index}]")


def _stats(values: list[float]) -> dict[str, Any]:
    ordered = sorted(values)

    def quantile(fraction: float) -> float:
        position = (len(ordered) - 1) * fraction
        lower = math.floor(position)
        upper = math.ceil(position)
        return ordered[lower] + (
            ordered[upper] - ordered[lower]
        ) * (position - lower)

    return {
        "count": len(ordered),
        "minimum": ordered[0],
        "mean": mean(ordered),
        "median": median(ordered),
        "p95": quantile(0.95),
        "maximum": ordered[-1],
    }


def _observation(
    *,
    evidence_id: str,
    source_id: str,
    source_kind: str,
    source_sha256: str,
    measurement_kind: str,
    target: Mapping[str, Any],
    metrics: Mapping[str, tuple[str, list[float]]],
    calibration_role: str = "direct",
) -> dict[str, Any]:
    identity = json.dumps(
        [evidence_id, source_id, measurement_kind, dict(target)],
        sort_keys=True,
        separators=(",", ":"),
    )
    return {
        "schema_version": EVIDENCE_OBSERVATION_SCHEMA_VERSION,
        "observation_id": sha256(identity.encode("utf-8")).hexdigest(),
        "evidence_id": evidence_id,
        "source_id": source_id,
        "source_kind": source_kind,
        "source_file_sha256": source_sha256,
        "measurement_kind": measurement_kind,
        "target": dict(target),
        "metrics": {
            key: {
                "unit": unit,
                "samples": values,
                "summary": _stats(values),
            }
            for key, (unit, values) in sorted(metrics.items())
        },
        "calibration_role": calibration_role,
        "real_measurement": True,
        "simulated": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def _fio_source(
    evidence_id: str,
    source: Mapping[str, Any],
    path: Path,
) -> tuple[str, dict[str, Any]]:
    raw, value = _read_json(path, "fio JSON")
    _reject_sensitive_keys(value, "fio")
    root = _mapping(value, "fio")
    operation = _text(source.get("operation"), "fio.operation")
    _require(operation in ("read", "write"), "fio.operation is unsupported")
    throughputs: list[float] = []
    latencies: list[float] = []
    for index, item in enumerate(_array(root.get("jobs"), "fio.jobs")):
        job = _mapping(item, f"fio.jobs[{index}]")
        result = _mapping(job.get(operation), f"fio.jobs[{index}].{operation}")
        if result.get("bw_bytes") is not None:
            throughput = _number(result["bw_bytes"], "fio.bw_bytes")
        else:
            throughput = _number(result.get("bw"), "fio.bw") * 1024.0
        latency_block = result.get("clat_ns", result.get("lat_ns"))
        latency = _mapping(latency_block, "fio latency")
        latency_ms = _number(latency.get("mean"), "fio latency mean") / 1e6
        throughputs.append(throughput)
        latencies.append(latency_ms)
    target = {
        "node_id": _text(source.get("node_id"), "fio.node_id"),
        "resource_id": _text(source.get("resource_id"), "fio.resource_id"),
        "operation": operation,
    }
    return _sha256_bytes(raw), _observation(
        evidence_id=evidence_id,
        source_id=_text(source.get("source_id"), "source_id"),
        source_kind="fio",
        source_sha256=_sha256_bytes(raw),
        measurement_kind="storage",
        target=target,
        metrics={
            "base_latency_ms": ("ms", latencies),
            "throughput_bytes_per_second": (
                "bytes/second",
                throughputs,
            ),
        },
    )


def _iperf_source(
    evidence_id: str,
    source: Mapping[str, Any],
    path: Path,
) -> tuple[str, dict[str, Any]]:
    raw, value = _read_json(path, "iperf3 JSON")
    _reject_sensitive_keys(value, "iperf3")
    root = _mapping(value, "iperf3")
    bandwidth: list[float] = []
    for index, interval in enumerate(_array(root.get("intervals"), "iperf3.intervals")):
        interval_root = _mapping(interval, f"iperf3.intervals[{index}]")
        total = interval_root.get("sum", interval_root.get("sum_received"))
        total_root = _mapping(total, "iperf3 interval sum")
        bandwidth.append(
            _number(total_root.get("bits_per_second"), "iperf3 bits_per_second")
            / 8.0
        )
    _require(bool(bandwidth), "iperf3 JSON contains no interval bandwidth")
    rtt = _samples(
        source.get("round_trip_time_ms_samples"),
        "iperf3.round_trip_time_ms_samples",
    )
    target = {
        "link_id": _text(source.get("link_id"), "iperf3.link_id"),
        "source_node_id": _text(
            source.get("source_node_id"),
            "iperf3.source_node_id",
        ),
        "destination_node_id": _text(
            source.get("destination_node_id"),
            "iperf3.destination_node_id",
        ),
    }
    return _sha256_bytes(raw), _observation(
        evidence_id=evidence_id,
        source_id=_text(source.get("source_id"), "source_id"),
        source_kind="iperf3",
        source_sha256=_sha256_bytes(raw),
        measurement_kind="network",
        target=target,
        metrics={
            "bandwidth_bytes_per_second": ("bytes/second", bandwidth),
            "round_trip_time_ms": ("ms", rtt),
        },
    )


def _model_sources(
    evidence_id: str,
    source: Mapping[str, Any],
    path: Path,
) -> tuple[str, list[dict[str, Any]]]:
    raw, rows = _read_jsonl(path, "model timing JSONL")
    _reject_sensitive_keys(rows, "model timing")
    grouped: dict[tuple[str, str, str, str], list[float]] = defaultdict(list)
    for index, row in enumerate(rows):
        _require(
            row.get("schema_version") == MODEL_TIMING_SCHEMA_VERSION,
            f"model timing row {index} has an unsupported schema_version",
        )
        _require(row.get("outcome_type") == "completed", "model timing incomplete")
        _require(
            row.get("telemetry_complete") is True,
            "model timing telemetry must be literal true",
        )
        _require(
            row.get("real_measurement") is True,
            "model timing must be a real measurement",
        )
        key = (
            _text(row.get("node_id"), "model timing node_id"),
            _text(row.get("resource_id"), "model timing resource_id"),
            _text(row.get("template_id"), "model timing template_id"),
            _text(row.get("op_id"), "model timing op_id"),
        )
        grouped[key].append(
            _number(row.get("service_time_ms"), "model timing service_time_ms")
        )
    digest = _sha256_bytes(raw)
    observations = []
    for node_id, resource_id, template_id, op_id in sorted(grouped):
        observations.append(_observation(
            evidence_id=evidence_id,
            source_id=_text(source.get("source_id"), "source_id"),
            source_kind="model-timing-jsonl",
            source_sha256=digest,
            measurement_kind="compute-operation",
            target={
                "node_id": node_id,
                "resource_id": resource_id,
                "template_id": template_id,
                "op_id": op_id,
            },
            metrics={
                "service_time_ms": ("ms", grouped[
                    (node_id, resource_id, template_id, op_id)
                ])
            },
        ))
    return digest, observations


def _flowmesh_sources(
    evidence_id: str,
    source: Mapping[str, Any],
    path: Path,
) -> tuple[str, list[dict[str, Any]]]:
    verify_flowmesh_trace_import(path)
    _, manifest_value = _read_json(
        path / "import_manifest.json",
        "FlowMesh trace import manifest",
    )
    manifest = _mapping(manifest_value, "FlowMesh trace import manifest")
    access_path = path / "access_observations.jsonl"
    raw, rows = _read_jsonl(access_path, "FlowMesh access observations")
    _reject_sensitive_keys(rows, "FlowMesh access observations")
    groups: dict[tuple[Any, ...], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        _require(
            row.get("schema_version")
            == FLOWMESH_ACCESS_OBSERVATION_SCHEMA_VERSION,
            "unsupported FlowMesh access observation",
        )
        key = (
            row.get("endpoint_id"),
            row.get("source_node_id"),
            row.get("destination_execution_node_id"),
            row.get("representation_id"),
        )
        groups[key].append(row)
    digest = _sha256_bytes(raw)
    observations: list[dict[str, Any]] = []
    for key, items in sorted(groups.items(), key=lambda item: repr(item[0])):
        latency = [
            _number(row["felt_latency_ms"], "felt_latency_ms")
            for row in items
            if row.get("felt_latency_ms") is not None
        ]
        payload = [
            _number(row["payload_bytes"], "payload_bytes")
            for row in items
            if row.get("payload_bytes") is not None
        ]
        _require(latency and payload, "FlowMesh route lacks latency or byte samples")
        endpoint_id, source_node, destination_node, representation = key
        observations.append(_observation(
            evidence_id=evidence_id,
            source_id=_text(source.get("source_id"), "source_id"),
            source_kind="flowmesh-trace-import",
            source_sha256=digest,
            measurement_kind="flowmesh-route",
            target={
                "endpoint_id": endpoint_id,
                "source_node_id": source_node,
                "destination_node_id": destination_node,
                "representation_id": representation,
            },
            metrics={
                "end_to_end_latency_ms": ("ms", latency),
                "payload_bytes": ("bytes", payload),
            },
            calibration_role="validation-only",
        ))
    _require(
        manifest.get("credentials_recorded") is False,
        "FlowMesh trace import is unsafe",
    )
    return digest, observations


def _verify_output(root: Path) -> dict[str, Any]:
    expected = {
        "evidence_observations.jsonl",
        "evidence_summary.json",
        "evidence_manifest.json",
    }
    actual = {path.name for path in root.iterdir() if path.is_file()}
    _require(actual == expected | {"SHA256SUMS"}, "evidence output set changed")
    checksums: dict[str, str] = {}
    for line in (root / "SHA256SUMS").read_text(encoding="utf-8").splitlines():
        digest, separator, name = line.partition("  ")
        _require(separator == "  " and name in expected, "malformed SHA256SUMS")
        _require(name not in checksums, f"duplicate checksum: {name}")
        _require(
            _sha256_bytes((root / name).read_bytes()) == digest,
            f"evidence checksum mismatch: {name}",
        )
        checksums[name] = digest
    _require(set(checksums) == expected, "evidence checksums are incomplete")
    _, value = _read_json(root / "evidence_manifest.json", "evidence manifest")
    manifest = _mapping(value, "evidence manifest")
    _require(
        manifest.get("schema_version") == EVIDENCE_MANIFEST_SCHEMA_VERSION,
        "unsupported evidence manifest schema_version",
    )
    _require(manifest.get("status") == "COMPLETE", "evidence bundle incomplete")
    _require(
        manifest.get("output_sha256")
        == {
            name: digest
            for name, digest in checksums.items()
            if name != "evidence_manifest.json"
        },
        "evidence manifest digests disagree",
    )
    return dict(manifest)


def build_simulator_evidence_bundle(
    spec_path: str | Path,
    *,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Normalize measurement files into one content-bound evidence bundle."""
    source_path = Path(spec_path).resolve()
    spec_raw, spec_value = _read_json(source_path, "evidence spec")
    spec = _mapping(spec_value, "evidence spec")
    _reject_sensitive_keys(spec, "evidence spec")
    _require(
        spec.get("schema_version") == EVIDENCE_SPEC_SCHEMA_VERSION,
        "unsupported evidence spec schema_version",
    )
    evidence_id = _text(spec.get("evidence_id"), "evidence_id")
    _require(
        spec.get("credentials_recorded") is False,
        "evidence spec must record credentials_recorded=false",
    )
    sources = _array(spec.get("sources"), "sources")
    _require(bool(sources), "evidence sources must not be empty")
    observations: list[dict[str, Any]] = []
    source_digests: dict[str, str] = {}
    source_ids: set[str] = set()
    for index, item in enumerate(sources):
        source = _mapping(item, f"sources[{index}]")
        source_id = _text(source.get("source_id"), f"sources[{index}].source_id")
        _require(source_id not in source_ids, f"duplicate source_id: {source_id}")
        source_ids.add(source_id)
        kind = _text(source.get("kind"), f"sources[{index}].kind")
        _require(kind in SOURCE_KINDS, f"unsupported source kind: {kind}")
        path = _contained(
            source_path.parent,
            _text(source.get("path"), f"sources[{index}].path"),
            f"sources[{index}].path",
        )
        if kind == "fio":
            digest, observation = _fio_source(evidence_id, source, path)
            additions = [observation]
        elif kind == "iperf3":
            digest, observation = _iperf_source(evidence_id, source, path)
            additions = [observation]
        elif kind == "model-timing-jsonl":
            digest, additions = _model_sources(evidence_id, source, path)
        else:
            _require(path.is_dir(), "FlowMesh trace source must be a directory")
            digest, additions = _flowmesh_sources(evidence_id, source, path)
        source_digests[source_id] = digest
        observations.extend(additions)
    observations.sort(key=lambda item: item["observation_id"])
    ids = [item["observation_id"] for item in observations]
    _require(len(ids) == len(set(ids)), "duplicate normalized observation ID")
    counts = Counter(item["measurement_kind"] for item in observations)
    direct = sum(item["calibration_role"] == "direct" for item in observations)
    summary = {
        "schema_version": EVIDENCE_SUMMARY_SCHEMA_VERSION,
        "status": "COMPLETE",
        "evidence_id": evidence_id,
        "source_count": len(sources),
        "observation_count": len(observations),
        "observation_counts_by_kind": dict(sorted(counts.items())),
        "direct_calibration_observation_count": direct,
        "validation_only_observation_count": len(observations) - direct,
        "raw_samples_preserved": True,
        "units_preserved": True,
        "unobserved_components_inferred": False,
        "rate_card_calibrated": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    documents = {
        "evidence_observations.jsonl": _jsonl_bytes(observations),
        "evidence_summary.json": _json_bytes(summary),
    }
    manifest = {
        "schema_version": EVIDENCE_MANIFEST_SCHEMA_VERSION,
        "status": "COMPLETE",
        "evidence_id": evidence_id,
        "spec_sha256": _sha256_bytes(spec_raw),
        "source_sha256": dict(sorted(source_digests.items())),
        "source_count": len(sources),
        "observation_count": len(observations),
        "direct_calibration_observation_count": direct,
        "external_services_called": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
        "output_sha256": {
            name: _sha256_bytes(content)
            for name, content in sorted(documents.items())
        },
    }
    documents["evidence_manifest.json"] = _json_bytes(manifest)
    documents["SHA256SUMS"] = "".join(
        f"{_sha256_bytes(content)}  {name}\n"
        for name, content in sorted(documents.items())
    ).encode("utf-8")
    target = Path(output_dir).resolve()
    _require(not target.exists(), f"evidence output already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging_parent = Path(tempfile.mkdtemp(prefix=".evidence-", dir=target.parent))
    staging = staging_parent / "output"
    try:
        staging.mkdir()
        for name, content in documents.items():
            (staging / name).write_bytes(content)
        _verify_output(staging)
        _require(not target.exists(), f"evidence output already exists: {target}")
        os.replace(staging, target)
    finally:
        shutil.rmtree(staging_parent, ignore_errors=True)
    return {
        "status": "COMPLETE",
        "evidence_id": evidence_id,
        "source_count": len(sources),
        "observation_count": len(observations),
        "direct_calibration_observation_count": direct,
        "output_dir": str(target),
        "external_services_called": False,
        "eligible_for_scientific_claims": False,
    }


def verify_simulator_evidence_bundle(output_dir: str | Path) -> dict[str, Any]:
    """Verify a unified simulator evidence bundle."""
    root = Path(output_dir).resolve()
    _require(root.is_dir(), f"evidence output does not exist: {root}")
    manifest = _verify_output(root)
    return {
        "schema_version": manifest["schema_version"],
        "status": "VERIFIED",
        "evidence_id": manifest["evidence_id"],
        "source_count": manifest["source_count"],
        "observation_count": manifest["observation_count"],
        "checked_files": 3,
        "eligible_for_scientific_claims": False,
    }
