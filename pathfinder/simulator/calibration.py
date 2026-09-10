"""Evidence-bound partial calibration for infrastructure scenarios.

The first calibration source is deliberately narrow: a frozen workload cohort
and its real raw-video, digest, and frame-bundle files.  These data identify
object sizes, but they do not identify storage throughput, network capacity,
queueing, GPU service time, or physical prices.  The calibrator updates only
the identifiable object-size parameters and records every retained provisional
parameter class explicitly.
"""

from __future__ import annotations

import json
import math
import os
import shutil
import tempfile
from hashlib import sha256
from pathlib import Path, PurePosixPath
from statistics import median
from typing import Any, Mapping

from .config import load_simulator_scenario
from .retrieval import (
    RETRIEVAL_COHORT_SCHEMA_VERSION,
    verify_simulator_retrieval,
)


CALIBRATION_CONFIG_SCHEMA_VERSION = (
    "pathfinder.flowmesh-infra-calibration-config/v1alpha1"
)
CALIBRATION_REPORT_SCHEMA_VERSION = (
    "pathfinder.flowmesh-infra-calibration-report/v1alpha1"
)
CALIBRATION_MANIFEST_SCHEMA_VERSION = (
    "pathfinder.flowmesh-infra-calibration-run/v1alpha1"
)


class SimulatorCalibrationError(ValueError):
    """Raised when evidence cannot support the declared calibration."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise SimulatorCalibrationError(message)


def _unique_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        _require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _invalid_number(value: str) -> None:
    raise SimulatorCalibrationError(f"non-finite JSON number: {value}")


def _read_json(path: Path, name: str) -> tuple[bytes, Any]:
    try:
        raw = path.read_bytes()
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_unique_keys,
            parse_constant=_invalid_number,
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SimulatorCalibrationError(f"cannot read valid {name}: {path}") from exc
    return raw, value


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


def _integer(value: Any, name: str, *, positive: bool = False) -> int:
    _require(type(value) is int, f"{name} must be an integer")
    _require(value > 0 if positive else value >= 0, f"{name} is out of range")
    return value


def _sha256_bytes(value: bytes) -> str:
    return sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = sha256()
    try:
        with path.open("rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
    except OSError as exc:
        raise SimulatorCalibrationError(f"cannot read evidence file: {path}") from exc
    return digest.hexdigest()


def _safe_segment(value: str, name: str) -> str:
    _require(
        value not in (".", "..")
        and "/" not in value
        and "\\" not in value,
        f"{name} must be one safe path segment",
    )
    return value


def _resolve_beneath(root: Path, relative: str, name: str) -> Path:
    candidate = PurePosixPath(relative)
    _require(
        not candidate.is_absolute() and ".." not in candidate.parts,
        f"{name} must be a contained relative path",
    )
    resolved_root = root.resolve()
    resolved = resolved_root.joinpath(*candidate.parts).resolve()
    _require(
        resolved == resolved_root or resolved_root in resolved.parents,
        f"{name} escapes its evidence root",
    )
    return resolved


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


def _load_calibration_config(path: Path) -> tuple[bytes, Mapping[str, Any]]:
    raw, value = _read_json(path, "calibration config")
    root = _mapping(value, "calibration config")
    _require(
        root.get("schema_version") == CALIBRATION_CONFIG_SCHEMA_VERSION,
        "unsupported calibration config schema_version",
    )
    _text(root.get("calibration_id"), "calibration_id")
    _text(root.get("output_scenario_id"), "output_scenario_id")
    profiles = _array(root.get("object_profiles"), "object_profiles")
    _require(bool(profiles), "object_profiles must not be empty")
    seen_classes: set[str] = set()
    seen_targets: set[str] = set()
    for index, value in enumerate(profiles):
        profile = _mapping(value, f"object_profiles[{index}]")
        workload_class = _text(
            profile.get("workload_class"),
            f"object_profiles[{index}].workload_class",
        )
        target = _text(
            profile.get("target_object_id"),
            f"object_profiles[{index}].target_object_id",
        )
        _require(workload_class not in seen_classes, "duplicate workload_class")
        _require(target not in seen_targets, "duplicate target_object_id")
        seen_classes.add(workload_class)
        seen_targets.add(target)
        mode = _text(profile.get("mode"), f"object_profiles[{index}].mode")
        _require(
            mode in (
                "cohort-representative",
                "retrieval-relevance-representative",
                "retain-base",
            ),
            f"object_profiles[{index}].mode is unsupported",
        )
        if mode == "cohort-representative":
            _text(
                profile.get("stratum_id"),
                f"object_profiles[{index}].stratum_id",
            )
            _require(
                profile.get("selection_rule")
                == "nearest-object-to-raw-size-median",
                f"object_profiles[{index}].selection_rule is unsupported",
            )
        elif mode == "retrieval-relevance-representative":
            _require(
                profile.get("selection_rule")
                == "nearest-object-to-raw-size-median",
                f"object_profiles[{index}].selection_rule is unsupported",
            )
        else:
            _text(profile.get("reason"), f"object_profiles[{index}].reason")
    return raw, root


def _load_generation_objects(
    path: Path,
) -> tuple[bytes, Mapping[str, Mapping[str, Any]]]:
    raw, value = _read_json(path, "representation generation manifest")
    root = _mapping(value, "representation generation manifest")
    _require(
        root.get("credentials_recorded") is False,
        "generation manifest is unsafe",
    )
    objects: dict[str, Mapping[str, Any]] = {}
    for index, item in enumerate(_array(root.get("objects"), "generation objects")):
        obj = _mapping(item, f"generation objects[{index}]")
        object_id = _text(obj.get("object_id"), f"generation objects[{index}].object_id")
        _require(object_id not in objects, f"duplicate generated object: {object_id}")
        source = _mapping(obj.get("source_video"), f"generation object {object_id}.source_video")
        _integer(source.get("size_bytes"), f"{object_id}.source_video.size_bytes", positive=True)
        _text(source.get("sha256"), f"{object_id}.source_video.sha256")
        reps = _mapping(
            obj.get("representations"),
            f"generation object {object_id}.representations",
        )
        digest = _mapping(reps.get("multimodal_digest"), f"{object_id}.multimodal_digest")
        _integer(digest.get("size_bytes"), f"{object_id}.digest.size_bytes", positive=True)
        _text(digest.get("sha256"), f"{object_id}.digest.sha256")
        objects[object_id] = obj
    _require(bool(objects), "generation manifest contains no objects")
    return raw, objects


def _cohort_objects_by_stratum(
    value: Any,
) -> dict[str, tuple[str, ...]]:
    workloads = _mapping(value, "workload manifest")
    grouped: dict[str, set[str]] = {}
    for workload_id, item in workloads.items():
        workload = _mapping(item, f"workloads.{workload_id}")
        stratum = _text(workload.get("stratum_id"), f"{workload_id}.stratum_id")
        object_id = _text(workload.get("object_id"), f"{workload_id}.object_id")
        grouped.setdefault(stratum, set()).add(object_id)
    return {key: tuple(sorted(values)) for key, values in sorted(grouped.items())}


def _retrieval_relevant_objects(
    output_dir: Path,
) -> tuple[bytes, tuple[str, ...], str]:
    verify_simulator_retrieval(output_dir)
    manifest_path = output_dir / "retrieval_manifest.json"
    manifest_raw, manifest_value = _read_json(
        manifest_path,
        "retrieval manifest",
    )
    manifest = _mapping(manifest_value, "retrieval manifest")
    cohort_path = output_dir / "retrieval_cohort.json"
    _, cohort_value = _read_json(cohort_path, "retrieval cohort")
    cohort = _mapping(cohort_value, "retrieval cohort")
    _require(
        cohort.get("schema_version") == RETRIEVAL_COHORT_SCHEMA_VERSION,
        "unsupported retrieval cohort schema_version",
    )
    relevant: set[str] = set()
    for index, item in enumerate(_array(cohort.get("queries"), "retrieval queries")):
        query = _mapping(item, f"retrieval queries[{index}]")
        for object_id in _array(
            query.get("relevant_object_ids"),
            f"retrieval queries[{index}].relevant_object_ids",
        ):
            relevant.add(_text(object_id, "retrieval relevant object_id"))
    _require(bool(relevant), "retrieval cohort contains no relevant objects")
    return (
        manifest_raw,
        tuple(sorted(relevant)),
        _text(cohort.get("annotation_status"), "retrieval annotation_status"),
    )


def _checksum_index(path: Path) -> tuple[bytes, dict[str, str]]:
    try:
        raw = path.read_bytes()
        lines = raw.decode("utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise SimulatorCalibrationError(f"cannot read frame bundle checksums: {path}") from exc
    result: dict[str, str] = {}
    for line in lines:
        digest, separator, name = line.partition("  ")
        _require(separator == "  " and bool(name), "frame bundle SHA256SUMS is malformed")
        _require(
            len(digest) == 64
            and all(character in "0123456789abcdef" for character in digest),
            "frame bundle SHA256SUMS contains an invalid digest",
        )
        normalized = PurePosixPath(name).as_posix()
        _require(normalized not in result, f"duplicate frame bundle checksum: {normalized}")
        result[normalized] = digest
    return raw, result


def _verified_selected_vector(
    *,
    object_id: str,
    generation: Mapping[str, Any],
    generation_manifest_sha256: str,
    representation_root: Path,
    frame_bundle_root: Path,
    video_root: Path,
    frame_checksums: Mapping[str, str],
) -> tuple[dict[str, int], dict[str, str]]:
    source = _mapping(generation["source_video"], f"{object_id}.source_video")
    video_name = _text(source.get("filename"), f"{object_id}.source_video.filename")
    _safe_segment(video_name, f"{object_id}.source_video.filename")
    video_path = video_root / video_name
    raw_size = _integer(source.get("size_bytes"), f"{object_id}.raw size", positive=True)
    _require(
        video_path.is_file() and video_path.stat().st_size == raw_size,
        f"raw video size mismatch: {object_id}",
    )
    raw_sha = _sha256_file(video_path)
    _require(raw_sha == source.get("sha256"), f"raw video checksum mismatch: {object_id}")

    reps = _mapping(generation["representations"], f"{object_id}.representations")
    digest = _mapping(reps["multimodal_digest"], f"{object_id}.multimodal_digest")
    digest_path = _resolve_beneath(
        representation_root,
        _text(digest.get("path"), f"{object_id}.digest.path"),
        f"{object_id}.digest.path",
    )
    digest_size = _integer(
        digest.get("size_bytes"),
        f"{object_id}.digest size",
        positive=True,
    )
    _require(
        digest_path.is_file() and digest_path.stat().st_size == digest_size,
        f"digest size mismatch: {object_id}",
    )
    digest_sha = _sha256_file(digest_path)
    _require(digest_sha == digest.get("sha256"), f"digest checksum mismatch: {object_id}")

    _safe_segment(object_id, "object_id")
    bundle_dir = frame_bundle_root / object_id
    bundle_path = bundle_dir / "sampled_frame_bundle.tar"
    bundle_manifest_path = bundle_dir / "frame_bundle_manifest.json"
    _, bundle_manifest_value = _read_json(bundle_manifest_path, "frame bundle manifest")
    bundle_manifest = _mapping(bundle_manifest_value, "frame bundle manifest")
    _require(
        bundle_manifest.get("object_id") == object_id,
        f"frame bundle object mismatch: {object_id}",
    )
    _require(
        bundle_manifest.get("generation_manifest_sha256")
        == generation_manifest_sha256,
        f"frame bundle generation binding mismatch: {object_id}",
    )
    _require(
        bundle_manifest.get("source_video_sha256") == raw_sha
        and bundle_manifest.get("source_video_size_bytes") == raw_size,
        f"frame bundle source binding mismatch: {object_id}",
    )
    _require(
        bundle_path.is_file() and bundle_path.stat().st_size > 0,
        f"frame bundle is missing: {object_id}",
    )
    bundle_sha = _sha256_file(bundle_path)
    checksum_name = f"{object_id}/sampled_frame_bundle.tar"
    _require(
        frame_checksums.get(checksum_name) == bundle_sha,
        f"frame bundle checksum mismatch: {object_id}",
    )
    return (
        {
            "raw_video": raw_size,
            "sampled_frame_bundle": bundle_path.stat().st_size,
            "multimodal_digest": digest_size,
        },
        {
            "raw_video": raw_sha,
            "sampled_frame_bundle": bundle_sha,
            "multimodal_digest": digest_sha,
        },
    )


def _select_representative(
    object_ids: tuple[str, ...],
    generation: Mapping[str, Mapping[str, Any]],
) -> tuple[str, float]:
    _require(bool(object_ids), "cohort stratum contains no objects")
    sized: list[tuple[str, int]] = []
    for object_id in object_ids:
        _require(
            object_id in generation,
            f"cohort object is absent from generation manifest: {object_id}",
        )
        source = _mapping(
            generation[object_id]["source_video"],
            f"{object_id}.source_video",
        )
        sized.append(
            (
                object_id,
                _integer(
                    source["size_bytes"],
                    f"{object_id}.size",
                    positive=True,
                ),
            )
        )
    target = float(median(size for _, size in sized))
    selected = min(sized, key=lambda item: (abs(item[1] - target), item[0]))[0]
    return selected, target


def _replace_object_sizes(
    scenario: dict[str, Any],
    *,
    target_object_id: str,
    sizes: Mapping[str, int],
) -> dict[str, int]:
    objects = _array(scenario.get("objects"), "scenario.objects")
    matches = [obj for obj in objects if obj.get("object_id") == target_object_id]
    _require(
        len(matches) == 1,
        f"scenario target object is not unique: {target_object_id}",
    )
    representations = _mapping(
        matches[0].get("representations"),
        f"{target_object_id}.representations",
    )
    old: dict[str, int] = {}
    for representation_id, size in sizes.items():
        _require(
            representation_id in representations,
            f"scenario object lacks {representation_id}",
        )
        old[representation_id] = _integer(
            representations[representation_id],
            f"{target_object_id}.{representation_id}",
            positive=True,
        )
        representations[representation_id] = size
    for node in _array(scenario.get("nodes"), "scenario.nodes"):
        for cache in node.get("caches", []):
            for entry in cache.get("initial_entries", []):
                if entry.get("object_id") != target_object_id:
                    continue
                representation_id = entry.get("representation_id")
                if representation_id in sizes:
                    entry["size_bytes"] = sizes[representation_id]
    return old


def _verify_output(root: Path) -> dict[str, Any]:
    expected = {
        "calibrated_scenario.json",
        "calibration_report.json",
        "calibration_manifest.json",
    }
    actual = {path.name for path in root.iterdir() if path.is_file()}
    _require(actual == expected | {"SHA256SUMS"}, "calibration output file set changed")
    checksums: dict[str, str] = {}
    for line in (root / "SHA256SUMS").read_text(encoding="utf-8").splitlines():
        digest, separator, name = line.partition("  ")
        _require(
            separator == "  " and name in expected,
            "calibration SHA256SUMS is malformed",
        )
        _require(name not in checksums, f"duplicate calibration checksum: {name}")
        _require(_sha256_file(root / name) == digest, f"calibration checksum mismatch: {name}")
        checksums[name] = digest
    _require(set(checksums) == expected, "calibration checksums are incomplete")
    _, value = _read_json(root / "calibration_manifest.json", "calibration manifest")
    manifest = _mapping(value, "calibration manifest")
    _require(
        manifest.get("schema_version") == CALIBRATION_MANIFEST_SCHEMA_VERSION,
        "unsupported calibration manifest",
    )
    _require(manifest.get("status") == "COMPLETE", "calibration is not complete")
    _require(
        manifest.get("output_sha256")
        == {
            name: digest
            for name, digest in checksums.items()
            if name != "calibration_manifest.json"
        },
        "calibration manifest digests disagree",
    )
    load_simulator_scenario(root / "calibrated_scenario.json")
    return dict(manifest)


def calibrate_simulator_scenario(
    scenario_path: str | Path,
    calibration_config_path: str | Path,
    *,
    workload_manifest_path: str | Path,
    representation_manifest_path: str | Path,
    frame_bundle_root: str | Path,
    video_root: str | Path,
    output_dir: str | Path,
    retrieval_output_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Apply only evidence-identifiable object-size calibrations."""

    scenario_source = Path(scenario_path).resolve()
    base = load_simulator_scenario(scenario_source)
    scenario_raw, scenario_value = _read_json(scenario_source, "base scenario")
    scenario = dict(_mapping(scenario_value, "base scenario"))
    config_path = Path(calibration_config_path).resolve()
    config_raw, config = _load_calibration_config(config_path)
    output_scenario_id = _text(config["output_scenario_id"], "output_scenario_id")
    _require(output_scenario_id != base.scenario_id, "calibrated scenario_id must change")

    workload_path = Path(workload_manifest_path).resolve()
    workload_raw, workload_value = _read_json(workload_path, "workload manifest")
    cohort = _cohort_objects_by_stratum(workload_value)
    representation_path = Path(representation_manifest_path).resolve()
    generation_raw, generation = _load_generation_objects(representation_path)
    generation_sha = _sha256_bytes(generation_raw)
    bundle_root = Path(frame_bundle_root).resolve()
    frame_checksums_raw, frame_checksums = _checksum_index(bundle_root / "SHA256SUMS")
    videos = Path(video_root).resolve()
    retrieval_manifest_raw: bytes | None = None
    retrieval_candidates: tuple[str, ...] | None = None
    retrieval_annotation_status: str | None = None
    if retrieval_output_dir is not None:
        (
            retrieval_manifest_raw,
            retrieval_candidates,
            retrieval_annotation_status,
        ) = _retrieval_relevant_objects(Path(retrieval_output_dir).resolve())

    reports: list[dict[str, Any]] = []
    calibrated_parameters = 0
    retained_parameters = 0
    for index, value in enumerate(config["object_profiles"]):
        profile = _mapping(value, f"object_profiles[{index}]")
        workload_class = _text(profile["workload_class"], "workload_class")
        target_object_id = _text(profile["target_object_id"], "target_object_id")
        mode = _text(profile["mode"], "mode")
        if mode == "retain-base":
            matches = [
                obj for obj in scenario["objects"]
                if obj["object_id"] == target_object_id
            ]
            _require(
                len(matches) == 1,
                f"scenario target object is not unique: {target_object_id}",
            )
            target = matches[0]
            sizes = dict(target["representations"])
            retained_parameters += len(sizes)
            reports.append({
                "workload_class": workload_class,
                "target_object_id": target_object_id,
                "mode": mode,
                "reason": _text(profile["reason"], "reason"),
                "retained_sizes_bytes": sizes,
                "parameter_provenance": "retained-from-base-scenario",
            })
            continue
        if mode == "retrieval-relevance-representative":
            _require(
                retrieval_candidates is not None,
                "retrieval_output_dir is required by the calibration config",
            )
            candidates = retrieval_candidates
            stratum = None
        else:
            stratum = _text(profile["stratum_id"], "stratum_id")
            _require(stratum in cohort, f"workload stratum is absent: {stratum}")
            candidates = cohort[stratum]
        selected, raw_median = _select_representative(candidates, generation)
        sizes, digests = _verified_selected_vector(
            object_id=selected,
            generation=generation[selected],
            generation_manifest_sha256=generation_sha,
            representation_root=representation_path.parent,
            frame_bundle_root=bundle_root,
            video_root=videos,
            frame_checksums=frame_checksums,
        )
        previous = _replace_object_sizes(
            scenario,
            target_object_id=target_object_id,
            sizes=sizes,
        )
        calibrated_parameters += len(sizes)
        reports.append({
            "workload_class": workload_class,
            "target_object_id": target_object_id,
            "mode": mode,
            "stratum_id": stratum,
            "selection_rule": profile["selection_rule"],
            "candidate_object_count": len(candidates),
            "cohort_raw_size_median_bytes": raw_median,
            "selected_source_object_id": selected,
            "previous_sizes_bytes": previous,
            "calibrated_sizes_bytes": sizes,
            "evidence_sha256": digests,
            "parameter_provenance": "verified-existing-cohort-files",
            "retrieval_annotation_status": (
                retrieval_annotation_status
                if mode == "retrieval-relevance-representative"
                else None
            ),
        })

    scenario["scenario_id"] = output_scenario_id
    calibration_id = _text(config["calibration_id"], "calibration_id")
    scenario["calibration_provenance"] = (
        f"partial-existing-evidence:{calibration_id};"
        "object-sizes-only;other-parameters-retained"
    )
    scenario_bytes = _json_bytes(scenario)
    report = {
        "schema_version": CALIBRATION_REPORT_SCHEMA_VERSION,
        "status": "COMPLETE",
        "calibration_id": calibration_id,
        "calibration_class": "partial-mixed-evidence",
        "base_scenario_id": base.scenario_id,
        "output_scenario_id": output_scenario_id,
        "source_sha256": {
            "base_scenario": _sha256_bytes(scenario_raw),
            "calibration_config": _sha256_bytes(config_raw),
            "workload_manifest": _sha256_bytes(workload_raw),
            "representation_manifest": generation_sha,
            "frame_bundle_checksums": _sha256_bytes(frame_checksums_raw),
            "retrieval_manifest": (
                _sha256_bytes(retrieval_manifest_raw)
                if retrieval_manifest_raw is not None
                else None
            ),
        },
        "object_profiles": reports,
        "calibrated_object_size_parameter_count": calibrated_parameters,
        "retained_object_size_parameter_count": retained_parameters,
        "resource_parameter_count_calibrated": 0,
        "link_parameter_count_calibrated": 0,
        "rate_parameter_count_calibrated": 0,
        "quality_parameter_count_calibrated": 0,
        "retained_provisional_parameter_classes": [
            "arrival_process",
            "cache_capacity_and_initial_state",
            "control_service_time",
            "cpu_service_time",
            "gpu_service_time",
            "index_service_time",
            "link_bandwidth_rtt_slots_and_jitter",
            "rate_card",
            "storage_latency_throughput_slots_and_jitter",
            "synthetic_task_success",
        ],
        "required_next_evidence": [
            "frozen_real_flowmesh_canonical_records",
            "endpoint_to_simulator_resource_binding",
            "fio_storage_calibration",
            "iperf3_network_calibration",
            "worker_and_model_service_timing",
            *(
                []
                if retrieval_candidates is not None
                else ["retrieval_workload_representation_manifest"]
            ),
        ],
        "simulator_results_are_not_real_measurements": True,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    report_bytes = _json_bytes(report)
    manifest = {
        "schema_version": CALIBRATION_MANIFEST_SCHEMA_VERSION,
        "status": "COMPLETE",
        "calibration_id": calibration_id,
        "output_scenario_id": output_scenario_id,
        "calibration_class": "partial-mixed-evidence",
        "calibrated_object_size_parameter_count": calibrated_parameters,
        "resource_parameter_count_calibrated": 0,
        "link_parameter_count_calibrated": 0,
        "external_services_called": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
        "output_sha256": {
            "calibrated_scenario.json": _sha256_bytes(scenario_bytes),
            "calibration_report.json": _sha256_bytes(report_bytes),
        },
    }
    documents = {
        "calibrated_scenario.json": scenario_bytes,
        "calibration_report.json": report_bytes,
        "calibration_manifest.json": _json_bytes(manifest),
    }
    checksum_bytes = "".join(
        f"{_sha256_bytes(content)}  {name}\n"
        for name, content in sorted(documents.items())
    ).encode("utf-8")
    documents["SHA256SUMS"] = checksum_bytes

    target = Path(output_dir).resolve()
    _require(not target.exists(), f"calibration output directory already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{target.name}.tmp-", dir=target.parent))
    try:
        for name, content in documents.items():
            path = temporary / name
            with path.open("wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
        _verify_output(temporary)
        temporary.replace(target)
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    verified = _verify_output(target)
    return {
        **verified,
        "output_dir": str(target),
        "calibrated_scenario_path": str(target / "calibrated_scenario.json"),
        "calibration_report_path": str(target / "calibration_report.json"),
        "checksums_path": str(target / "SHA256SUMS"),
    }


def verify_simulator_calibration(output_dir: str | Path) -> dict[str, Any]:
    """Read-only verification of one published calibration directory."""

    root = Path(output_dir).resolve()
    _require(root.is_dir(), f"calibration output directory does not exist: {root}")
    manifest = _verify_output(root)
    return {
        "status": "VERIFIED",
        "calibration_id": manifest["calibration_id"],
        "output_scenario_id": manifest["output_scenario_id"],
        "calibrated_object_size_parameter_count": manifest[
            "calibrated_object_size_parameter_count"
        ],
        "checked_files": 3,
        "eligible_for_scientific_claims": False,
    }
