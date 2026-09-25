"""Freeze the multi-case runtime foundation for the RSI-Exam collection.

The public cohort plan deliberately contains no labels and no deployable
derived store.  This module completes that offline boundary in one atomic
package: N1 public/private task inputs, the N2 visible index, a question-
independent N4 derived store, and a scenario containing exactly the selected
video-disjoint workloads.  Hidden answers are processed only while building
the N1-private subtree and are never returned by this API.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from importlib import metadata
from pathlib import Path, PurePosixPath
from typing import Any, Mapping, Sequence

from ..frame_bundle import deterministic_frame_bundle_tar
from ..simulator.config import load_simulator_scenario
from ..simulator.full_flow_tasks import (
    build_full_flow_task_plane,
    verify_full_flow_task_plane,
)
from ..simulator.index_service import (
    build_n2_index_package,
    verify_n2_index_package,
)
from ..simulator.n3_indexed_data_plane import (
    verify_n3_indexed_data_plane_package,
)
from ..simulator.n4_derived_data_plane import (
    N4ArtifactProvenance,
    N4DerivedArtifactInput,
    build_n4_derived_data_package,
    verify_n4_derived_data_package,
)
from ..simulator.raw_cold_data_plane import (
    verify_raw_cold_data_plane_package,
)
from .collection_plan import verify_collection_plan
from .temporal_index_collection import (
    caption_search_text,
    verify_formal_temporal_caption_package,
    verify_formal_temporal_index_preparation,
)


FOUNDATION_SCHEMA_VERSION = "pathfinder.rsi-exam-formal-foundation/v1alpha1"
FOUNDATION_MANIFEST = "formal-foundation-manifest.json"
SCENARIO_NAME = "formal-scenario.json"
CHECKSUMS_NAME = "SHA256SUMS"
TASK_PLANE_DIR = "task-plane"
N2_PACKAGE_DIR = "n2-package"
N4_PACKAGE_DIR = "n4-package"
FRAME_BUNDLE_SCHEMA_VERSION = "pathfinder.sampled-frame-bundle/v0.1"
SEMANTIC_SPEC_SCHEMA_VERSION = (
    "pathfinder.data-agent-frame-bundle-semantic-spec/v1alpha1"
)
N2_SOURCE_SCHEMA_VERSION = "pathfinder.n2-index-source/v1alpha1"
PLAN_IDS = ("D2", "D3", "D6", "D7")
FORMAL_INDEXED_DESIGN_IDS = frozenset({"D1", "D5"})
STRATUM_TO_WORKLOAD_CLASS = {
    "descriptive": "W1",
    "temporal": "W2",
    "causal": "W3",
}
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_COMMIT = re.compile(r"[0-9a-f]{40}\Z")


class FormalFoundationError(ValueError):
    """Raised when the formal runtime foundation cannot be proven."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FormalFoundationError(message)


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            indent=2,
        )
        + "\n"
    ).encode("utf-8")


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FormalFoundationError(f"cannot read {label}") from exc
    _require(isinstance(value, dict), f"{label} must be an object")
    return value


def _read_jsonl(path: Path, label: str) -> list[dict[str, Any]]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise FormalFoundationError(f"cannot read {label}") from exc
    _require(b"\r" not in raw, f"{label} contains CR bytes")
    rows: list[dict[str, Any]] = []
    for index, line in enumerate(raw.splitlines(), start=1):
        try:
            value = json.loads(line)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise FormalFoundationError(
                f"cannot read {label} row {index}"
            ) from exc
        _require(isinstance(value, dict), f"{label} row {index} is invalid")
        rows.append(value)
    return rows


def _relative_files(root: Path) -> list[str]:
    return sorted(
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file() and path != root / CHECKSUMS_NAME
    )


def _checksum_bytes(root: Path) -> bytes:
    return b"".join(
        f"{_sha256((root / PurePosixPath(name)).read_bytes())}  {name}\n".encode(
            "utf-8"
        )
        for name in _relative_files(root)
    )


def _verify_checksums(root: Path) -> dict[str, str]:
    try:
        raw = (root / CHECKSUMS_NAME).read_bytes()
    except OSError as exc:
        raise FormalFoundationError("foundation SHA256SUMS is missing") from exc
    _require(b"\r" not in raw, "foundation SHA256SUMS contains CR bytes")
    entries: dict[str, str] = {}
    for line in raw.decode("utf-8").splitlines():
        digest, separator, name = line.partition("  ")
        _require(separator == "  ", "foundation checksum line is malformed")
        _require(_SHA256.fullmatch(digest) is not None, "checksum is invalid")
        _require(name not in entries, "foundation checksum path repeats")
        entries[name] = digest
    _require(set(entries) == set(_relative_files(root)), "foundation file set changed")
    for name, digest in entries.items():
        _require(
            _sha256((root / PurePosixPath(name)).read_bytes()) == digest,
            f"foundation checksum failed: {name}",
        )
    return entries


def _software_versions() -> dict[str, str | None]:
    values: dict[str, str | None] = {}
    for name in ("av", "Pillow", "pathfinder-minimal"):
        try:
            values[name] = metadata.version(name)
        except metadata.PackageNotFoundError:
            values[name] = None
    return values


def _public_tasks(path: Path) -> tuple[bytes, dict[str, dict[str, Any]]]:
    raw = path.read_bytes()
    document = _read_json(path, "candidate public task set")
    _require(
        document.get("schema_version") == "pathfinder.public-task-set/v1alpha1",
        "unsupported public task set",
    )
    _require(document.get("label_values_included") is False, "public labels leak")
    rows = document.get("tasks")
    _require(isinstance(rows, list), "public tasks are missing")
    by_object: dict[str, dict[str, Any]] = {}
    for row in rows:
        _require(isinstance(row, dict), "public task is invalid")
        object_id = str(row.get("object_id", ""))
        _require(object_id and object_id not in by_object, "public object repeats")
        by_object[object_id] = row
    return raw, by_object


def _raw_rows(root: Path) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    report = _read_json(root / "raw-cold-data-plane.json", "N3 raw manifest")
    rows = {
        str(row["object_id"]): row
        for row in report["objects"]
        if row["representation_id"] == "raw_video"
    }
    return report, rows


def _frame_bundle(
    *,
    object_id: str,
    object_row: Mapping[str, Any],
    frame_rows: Sequence[Mapping[str, Any]],
    preparation_root: Path,
    preparation_sha256: str,
    frame_description_path: str = "caption-frames.jsonl",
) -> bytes:
    frames: list[dict[str, Any]] = []
    members: list[tuple[str, bytes]] = []
    for expected_index, row in enumerate(frame_rows):
        _require(row.get("frame_index") == expected_index, "caption frames reorder")
        source = preparation_root / PurePosixPath(str(row["package_path"]))
        payload = source.read_bytes()
        _require(_sha256(payload) == row["jpeg_sha256"], "caption frame digest differs")
        _require(len(payload) == row["jpeg_size_bytes"], "caption frame size differs")
        member = f"frames/{expected_index:03d}.jpg"
        frames.append({
            "frame_index": expected_index,
            "timestamp_seconds": row["timestamp_seconds"],
            "width": row["width"],
            "height": row["height"],
            "path": member,
            "jpeg_size_bytes": len(payload),
            "jpeg_sha256": _sha256(payload),
        })
        members.append((member, payload))
    source_description = b"".join(
        _canonical(dict(row)) + b"\n" for row in frame_rows
    )
    manifest = {
        "schema_version": FRAME_BUNDLE_SCHEMA_VERSION,
        "representation_id": "sampled_frame_bundle",
        "object_id": object_id,
        "source_video_id": object_id,
        "source_video_filename": f"{object_id}.mp4",
        "source_video_size_bytes": object_row["source_video_size_bytes"],
        "source_video_sha256": object_row["source_video_sha256"],
        "source_duration_seconds": object_row["duration_seconds"],
        "sampling": {
            "method": "uniform-midpoint",
            "frame_count": len(frames),
            "jpeg_max_dimension": 768,
            "jpeg_quality": 82,
            "jpeg_optimize": True,
        },
        "source_frame_descriptions": {
            "representation_id": "sampled_frames",
            "path": frame_description_path,
            "sha256": _sha256(source_description),
        },
        "generation_manifest_sha256": preparation_sha256,
        "frames": frames,
        "frame_count": len(frames),
        "total_jpeg_bytes": sum(row["jpeg_size_bytes"] for row in frames),
        "software_versions": _software_versions(),
        "historical_visual_bytes_retained": False,
        "sampling_alignment_statement": (
            "The frames are aligned with the frozen sampling metadata and "
            "this artifact does not claim byte identity with the historical "
            "visual input."
        ),
        "claims_byte_identity_with_historical_visual_input": False,
        "credentials_recorded": False,
        "llm_called": False,
        "network_calls_performed": False,
    }
    members.append(("frame_bundle_manifest.json", _json_bytes(manifest)))
    return deterministic_frame_bundle_tar(members)


def _digest_text(
    object_id: str,
    captions: Sequence[Mapping[str, Any]],
) -> bytes:
    lines = [f"Object: {object_id}", "Question-independent temporal observations:"]
    for row in sorted(captions, key=lambda item: int(item["ordinal"])):
        text = caption_search_text(row["structured_caption"])
        lines.append(
            f"[{int(row['ordinal']):02d} {float(row['start_seconds']):.6f}-"
            f"{float(row['end_seconds']):.6f}s] {text}"
        )
    return ("\n".join(lines) + "\n").encode("utf-8")


def _correct_option_id(
    task: Mapping[str, Any],
    pilot_workload: Mapping[str, Any],
) -> str:
    _require(
        str(pilot_workload.get("id")) == str(task.get("workload_id")),
        "pilot and public workload ids differ",
    )
    prompt = str(pilot_workload.get("question", ""))
    marker = " Answer with the best option text: "
    _, separator, option_text = prompt.partition(marker)
    _require(separator == marker, "pilot question option marker is missing")
    _require(option_text.endswith("."), "pilot option list terminator is missing")
    pilot_options = option_text[:-1].split("; ")
    public_options = task.get("answer_options")
    _require(isinstance(public_options, list), "public answer options are missing")
    _require(
        len(pilot_options) == len(public_options),
        "pilot and public option counts differ",
    )
    _require(
        len(set(pilot_options)) == len(pilot_options),
        "pilot option text repeats",
    )
    accepted = pilot_workload.get("accepted_answer_substrings")
    _require(isinstance(accepted, list) and bool(accepted), "pilot label is missing")
    accepted_values = {str(value) for value in accepted}
    matches = [
        index
        for index, option in enumerate(pilot_options)
        if option in accepted_values
    ]
    if not matches:
        # ``accepted_answer_substrings`` is a legacy scoring contract, not a
        # guarantee that one entry repeats the complete option verbatim.
        # Resolve it with the same direction its name declares: after generic
        # case/punctuation/whitespace normalization, an accepted phrase must
        # occur inside exactly one full option.  Never use fuzzy similarity or
        # break a tie by order; ambiguous labels remain fail-closed.
        normalize = lambda value: " ".join(
            re.findall(r"[a-z0-9]+", str(value).casefold())
        )
        normalized_options = [normalize(option) for option in pilot_options]
        normalized_accepted = {
            normalize(value) for value in accepted_values if normalize(value)
        }
        matches = [
            index
            for index, option in enumerate(normalized_options)
            if any(value in option for value in normalized_accepted)
        ]
    _require(len(matches) == 1, "pilot label does not identify exactly one option")
    return str(public_options[matches[0]]["option_id"])


def _formal_scenario(
    base: Mapping[str, Any],
    cases: Sequence[Mapping[str, Any]],
    tasks: Mapping[str, Mapping[str, Any]],
    raw_rows: Mapping[str, Mapping[str, Any]],
    n4_rows: Mapping[tuple[str, str], Mapping[str, Any]],
    *,
    package_id: str,
) -> dict[str, Any]:
    scenario = json.loads(json.dumps(base))
    scenario["scenario_id"] = f"{package_id}-scenario"
    scenario["seed"] = 20260922
    scenario["repetitions"] = 2
    for node in scenario["nodes"]:
        for cache in node.get("caches", []):
            cache["initial_entries"] = []
    scenario["objects"] = []
    scenario["workloads"] = []
    design_ids = [str(row["design_id"]) for row in scenario["designs"]]
    for case in sorted(cases, key=lambda row: row["object_id"]):
        object_id = str(case["object_id"])
        task = tasks[object_id]
        scenario["objects"].append({
            "object_id": object_id,
            "representations": {
                "raw_video": raw_rows[object_id]["artifact_size_bytes"],
                "sampled_frame_bundle": n4_rows[
                    (object_id, "sampled_frame_bundle")
                ]["artifact_size_bytes"],
                "multimodal_digest": n4_rows[
                    (object_id, "multimodal_digest")
                ]["artifact_size_bytes"],
            },
        })
        stratum = str(case["stratum"])
        _require(stratum in STRATUM_TO_WORKLOAD_CLASS, "unsupported stratum")
        scenario["workloads"].append({
            "workload_id": task["workload_id"],
            "workload_class": STRATUM_TO_WORKLOAD_CLASS[stratum],
            "object_id": object_id,
            "task_type": f"flowmesh.agent.video_qa.{stratum}",
            "quality_provenance": (
                "planning-placeholder-not-used-for-live-hidden-scoring"
            ),
            "task_success_by_design": {
                design_id: True for design_id in design_ids
            },
        })
    workload_classes = {
        str(workload["workload_class"])
        for workload in scenario["workloads"]
    }
    for design in scenario["designs"]:
        design_id = str(design["design_id"])
        if design_id in FORMAL_INDEXED_DESIGN_IDS:
            # The base simulator scenario predates the formal collection and
            # maps descriptive W1 D1/D5 trials to ``raw-full``.  That is a
            # valid historical smoke shape, but it collapses the formal
            # collection's indexed action into its raw action for an entire
            # stratum.  Every selected formal object has a frozen,
            # content-bound temporal selection, so D1/D5 must denote that
            # indexed action for every included workload class.
            design["route_templates"] = {
                workload_class: "raw-indexed"
                for workload_class in sorted(workload_classes)
            }
        else:
            design["route_templates"] = {
                workload_class: template_id
                for workload_class, template_id
                in design["route_templates"].items()
                if workload_class in workload_classes
            }
    return scenario


def _verify_formal_indexed_designs(scenario: Mapping[str, Any]) -> None:
    """Require the formal D1/D5 action to stay indexed in every stratum."""

    workloads = scenario.get("workloads")
    designs = scenario.get("designs")
    _require(isinstance(workloads, list) and bool(workloads),
             "formal scenario workloads are missing")
    _require(isinstance(designs, list), "formal scenario designs are missing")
    workload_classes = {
        str(row.get("workload_class"))
        for row in workloads
        if isinstance(row, Mapping)
    }
    indexed = {
        str(row.get("design_id")): row
        for row in designs
        if isinstance(row, Mapping)
        and str(row.get("design_id")) in FORMAL_INDEXED_DESIGN_IDS
    }
    _require(
        set(indexed) == set(FORMAL_INDEXED_DESIGN_IDS),
        "formal indexed designs D1/D5 are missing",
    )
    for design_id, design in sorted(indexed.items()):
        routes = design.get("route_templates")
        _require(
            isinstance(routes, Mapping)
            and set(routes) == workload_classes,
            f"formal {design_id} workload coverage changed",
        )
        _require(
            set(routes.values()) == {"raw-indexed"},
            f"formal {design_id} collapses an indexed action into raw",
        )


def build_formal_runtime_foundation(
    collection_plan_dir: str | Path,
    public_task_set: str | Path,
    pilot_config: str | Path,
    n3_raw_package_dir: str | Path,
    n3_indexed_package_dir: str | Path,
    temporal_index_dir: str | Path,
    preparation_dir: str | Path,
    caption_dir: str | Path,
    base_scenario: str | Path,
    *,
    output_dir: str | Path,
    package_id: str,
    source_commit: str,
    expected_model: str,
) -> dict[str, Any]:
    """Build all offline inputs required before source-bound promotion."""

    _require(_COMMIT.fullmatch(source_commit) is not None, "source commit invalid")
    _require(bool(package_id), "package_id is required")
    plan_root = Path(collection_plan_dir).resolve()
    public_path = Path(public_task_set).resolve()
    pilot_path = Path(pilot_config).resolve()
    raw_root = Path(n3_raw_package_dir).resolve()
    indexed_root = Path(n3_indexed_package_dir).resolve()
    index_root = Path(temporal_index_dir).resolve()
    prep_root = Path(preparation_dir).resolve()
    captions_root = Path(caption_dir).resolve()
    base_path = Path(base_scenario).resolve()
    target = Path(output_dir).resolve()
    _require(not target.exists(), "foundation output directory already exists")

    plan = verify_collection_plan(plan_root)
    verify_raw_cold_data_plane_package(raw_root)
    indexed = verify_n3_indexed_data_plane_package(indexed_root)
    prep = verify_formal_temporal_index_preparation(prep_root)
    captions = verify_formal_temporal_caption_package(captions_root, prep_root)
    public_raw, tasks = _public_tasks(public_path)
    pilot = _read_json(pilot_path, "pilot config")
    base_raw = base_path.read_bytes()
    base = _read_json(base_path, "base scenario")
    index_manifest = _read_json(index_root / "temporal-index-package.json", "index")
    _verify_checksums(index_root)
    cases = _read_jsonl(plan_root / "selected-cases.jsonl", "selected cases")
    case_ids = {str(row["object_id"]) for row in cases}
    _require(case_ids <= set(tasks), "selected public task is missing")
    pilot_rows = pilot.get("workloads")
    _require(isinstance(pilot_rows, list), "pilot workloads are missing")
    pilot_by_object = {
        str(row["object_id"]): row for row in pilot_rows if isinstance(row, dict)
    }
    _require(case_ids <= set(pilot_by_object), "selected pilot workload is missing")
    raw_report, raw_rows = _raw_rows(raw_root)
    _require(case_ids == set(raw_rows), "raw package objects differ from cohort")
    prep_manifest = _read_json(
        prep_root / "temporal-index-preparation.json", "preparation manifest"
    )
    prep_objects = {str(row["object_id"]): row for row in prep_manifest["objects"]}
    frame_rows: dict[str, list[dict[str, Any]]] = {value: [] for value in case_ids}
    for row in _read_jsonl(prep_root / "caption-frames.jsonl", "caption frames"):
        frame_rows[str(row["object_id"])].append(row)
    caption_rows: dict[str, list[dict[str, Any]]] = {value: [] for value in case_ids}
    for row in _read_jsonl(captions_root / "fine-captions.jsonl", "captions"):
        caption_rows[str(row["object_id"])].append(row)

    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.", dir=target.parent))
    private_specs = staging / ".private-specs"
    try:
        n4_inputs: list[N4DerivedArtifactInput] = []
        for object_id in sorted(case_ids):
            raw_row = raw_rows[object_id]
            object_row = prep_objects[object_id]
            bundle = _frame_bundle(
                object_id=object_id,
                object_row=object_row,
                frame_rows=sorted(
                    frame_rows[object_id], key=lambda row: int(row["frame_index"])
                ),
                preparation_root=prep_root,
                preparation_sha256=prep["preparation_sha256"],
            )
            digest = _digest_text(object_id, caption_rows[object_id])
            for representation_id, payload, derivation_id in (
                (
                    "sampled_frame_bundle",
                    bundle,
                    "rsi-exam-question-independent-frame-bundle-v1",
                ),
                (
                    "multimodal_digest",
                    digest,
                    "rsi-exam-question-independent-temporal-digest-v1",
                ),
            ):
                derivation = {
                    "derivation_id": derivation_id,
                    "object_id": object_id,
                    "representation_id": representation_id,
                    "source_video_sha256": raw_row["artifact_sha256"],
                    "preparation_sha256": prep["preparation_sha256"],
                    "caption_package_sha256": captions["package_sha256"],
                }
                n4_inputs.append(N4DerivedArtifactInput(
                    object_id=object_id,
                    representation_id=representation_id,
                    artifact_bytes=payload,
                    plan_ids=PLAN_IDS,
                    provenance=N4ArtifactProvenance(
                        producer_node_id="N5",
                        publication_source_id=f"{package_id}-{object_id}",
                        source_representation_id="raw_video",
                        source_content_sha256=raw_row["artifact_sha256"],
                        derivation_id=derivation_id,
                        derivation_sha256=_sha256(_canonical(derivation)),
                    ),
                ))
        n4_result = build_n4_derived_data_package(
            n4_inputs,
            output_dir=staging / N4_PACKAGE_DIR,
            package_id=f"{package_id}-n4",
            catalog_version=f"{package_id}-n4-catalog",
        )
        n4_manifest = _read_json(
            staging / N4_PACKAGE_DIR / "n4-derived-data-package.json",
            "N4 package manifest",
        )
        n4_rows = {
            (str(row["object_id"]), str(row["representation_id"])): row
            for row in n4_manifest["objects"]
        }

        private_specs.mkdir()
        semantic_specs: list[Path] = []
        for case in sorted(cases, key=lambda row: row["object_id"]):
            object_id = str(case["object_id"])
            task = tasks[object_id]
            pilot_row = pilot_by_object[object_id]
            artifact = n4_rows[(object_id, "sampled_frame_bundle")]
            spec = {
                "schema_version": SEMANTIC_SPEC_SCHEMA_VERSION,
                "semantic_run_id": f"{package_id}-task-plane",
                "trial_key": (
                    f"{task['workload_id']}|formal-foundation|D2|r0000"
                ),
                "semantic_executor_node_id": "N6",
                "representation_id": "sampled_frame_bundle",
                "data_agent_route_design_id": "D2",
                "data_agent_plan_id": "D2",
                "data_agent_plan_epoch": 0,
                "workload_id": task["workload_id"],
                "task_class_id": task["task_class_id"],
                "artifact_object_id": object_id,
                "artifact_sha256": artifact["artifact_sha256"],
                "artifact_size_bytes": artifact["artifact_size_bytes"],
                "object_catalog_version": n4_manifest["catalog_version"],
                "question": task["question"],
                "success_scoring_rule": task["success_scoring_rule"],
                "answer_options": task["answer_options"],
                "correct_answer_id": _correct_option_id(task, pilot_row),
                "expected_model": expected_model,
                "credentials_recorded": False,
            }
            path = private_specs / f"{object_id}.json"
            path.write_bytes(_json_bytes(spec))
            semantic_specs.append(path)
        build_full_flow_task_plane(
            semantic_specs,
            task_plane_id=f"{package_id}-tasks",
            oracle_id=f"{package_id}-oracle",
            output_dir=staging / TASK_PLANE_DIR,
        )
        shutil.rmtree(private_specs)

        n2_source = {
            "schema_version": N2_SOURCE_SCHEMA_VERSION,
            "index_id": f"{package_id}-visible-index",
            "logical_node_id": "N2",
            "documents": [
                {
                    "object_id": object_id,
                    "source_object_group": package_id,
                    "visible_fields": {
                        "description": tasks[object_id]["question"],
                        "media_type": "video",
                        "modalities": ["text", "video"],
                        "source_collection": "nextqa-val-formal-cohort",
                        "tags": sorted({
                            option["text"]
                            for option in tasks[object_id]["answer_options"]
                        }),
                    },
                }
                for object_id in sorted(case_ids)
            ],
            "credentials_recorded": False,
        }
        n2_source_path = staging / ".n2-source.json"
        n2_source_path.write_bytes(_json_bytes(n2_source))
        build_n2_index_package(
            n2_source_path,
            output_dir=staging / N2_PACKAGE_DIR,
        )
        n2_source_path.unlink()

        scenario = _formal_scenario(
            base,
            cases,
            tasks,
            raw_rows,
            n4_rows,
            package_id=package_id,
        )
        _verify_formal_indexed_designs(scenario)
        (staging / SCENARIO_NAME).write_bytes(_json_bytes(scenario))
        load_simulator_scenario(staging / SCENARIO_NAME)

        manifest = {
            "schema_version": FOUNDATION_SCHEMA_VERSION,
            "status": "FROZEN_RSI_EXAM_FORMAL_RUNTIME_FOUNDATION",
            "package_id": package_id,
            "source_commit": source_commit,
            "case_count": len(case_ids),
            "collection_plan_sha256": plan["plan_sha256"],
            "candidate_public_task_set_sha256": _sha256(public_raw),
            "base_scenario_sha256": _sha256(base_raw),
            "n3_raw_package_sha256": _sha256(
                (raw_root / CHECKSUMS_NAME).read_bytes()
            ),
            "n3_indexed_package_sha256": _sha256(
                (indexed_root / CHECKSUMS_NAME).read_bytes()
            ),
            "temporal_index_package_sha256": index_manifest["package_sha256"],
            "preparation_sha256": prep["preparation_sha256"],
            "caption_package_sha256": captions["package_sha256"],
            "n4_package_sha256": n4_result["package_sha256"],
            "task_plane_manifest_sha256": _sha256(
                (staging / TASK_PLANE_DIR / "task-plane-manifest.json").read_bytes()
            ),
            "formal_scenario_sha256": _sha256(
                (staging / SCENARIO_NAME).read_bytes()
            ),
            "labels_confined_to": "task-plane/n1-private",
            "hidden_label_values_processed_by_builder": True,
            "hidden_label_values_displayed": False,
            "task_outcomes_read": False,
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        }
        (staging / FOUNDATION_MANIFEST).write_bytes(_json_bytes(manifest))
        (staging / CHECKSUMS_NAME).write_bytes(_checksum_bytes(staging))
        verify_formal_runtime_foundation(staging)
        os.replace(staging, target)
    finally:
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)

    return {
        "status": "FROZEN_RSI_EXAM_FORMAL_RUNTIME_FOUNDATION",
        "output_dir": str(target),
        "package_id": package_id,
        "case_count": len(case_ids),
        "n4_artifact_count": len(n4_inputs),
        "hidden_label_values_displayed": False,
        "task_outcomes_read": False,
        "credentials_recorded": False,
    }


def verify_formal_runtime_foundation(
    foundation_dir: str | Path,
) -> dict[str, Any]:
    """Verify the complete offline foundation without returning labels."""

    root = Path(foundation_dir).resolve()
    _require(root.is_dir(), "foundation directory is missing")
    entries = _verify_checksums(root)
    manifest = _read_json(root / FOUNDATION_MANIFEST, "foundation manifest")
    _require(
        manifest.get("schema_version") == FOUNDATION_SCHEMA_VERSION,
        "unsupported foundation schema",
    )
    _require(manifest.get("credentials_recorded") is False, "credentials recorded")
    _require(manifest.get("task_outcomes_read") is False, "task outcomes read")
    _require(
        manifest.get("hidden_label_values_displayed") is False,
        "hidden labels displayed",
    )
    _require(_COMMIT.fullmatch(str(manifest.get("source_commit"))) is not None,
             "foundation source commit is invalid")
    task = verify_full_flow_task_plane(root / TASK_PLANE_DIR)
    n2 = verify_n2_index_package(root / N2_PACKAGE_DIR)
    n4 = verify_n4_derived_data_package(root / N4_PACKAGE_DIR)
    scenario_document = _read_json(root / SCENARIO_NAME, "formal scenario")
    _verify_formal_indexed_designs(scenario_document)
    scenario = load_simulator_scenario(root / SCENARIO_NAME)
    _require(task["public_task_count"] == manifest["case_count"],
             "task-plane case count differs")
    _require(n4["object_count"] == manifest["case_count"],
             "N4 object count differs")
    _require(len(scenario.workloads) == manifest["case_count"],
             "scenario workload count differs")
    _require(len(scenario.objects) == manifest["case_count"],
             "scenario object count differs")
    private_root = (root / TASK_PLANE_DIR / "n1-private").resolve()
    for path in root.rglob("*.json"):
        if path.is_relative_to(private_root):
            continue
        value = path.read_text(encoding="utf-8")
        _require('"correct_answer_id"' not in value, "label leaked outside N1")
        _require('"accepted_answer_substrings"' not in value,
                 "pilot labels leaked outside N1")
    _require(f"{TASK_PLANE_DIR}/n1-private/hidden-label-source.json" in entries,
             "private label source is not checksum bound")
    return {
        "status": "VERIFIED_RSI_EXAM_FORMAL_RUNTIME_FOUNDATION",
        "package_id": manifest["package_id"],
        "foundation_sha256": _sha256((root / CHECKSUMS_NAME).read_bytes()),
        "case_count": manifest["case_count"],
        "n2_document_count": n2["document_count"],
        "n4_artifact_count": n4["artifact_count"],
        "labels_confined_to_n1": True,
        "hidden_label_values_displayed": False,
        "task_outcomes_read": False,
        "credentials_recorded": False,
    }


__all__ = [
    "FormalFoundationError",
    "build_formal_runtime_foundation",
    "verify_formal_runtime_foundation",
]
