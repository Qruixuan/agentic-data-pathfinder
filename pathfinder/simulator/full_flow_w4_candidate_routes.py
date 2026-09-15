"""Candidate-wide physical route blueprints for prospective W4 retrieval.

The historical 4x8 matrix represents W4 with one object and one multiple-
choice answer.  This compiler consumes the separate public W4 retrieval
runtime and verified N3/N4/index/range packages, then freezes candidate-wide
route blueprints for the same sixteen design/repetition coordinates.  It does
not rewrite the historical matrix and it does not execute a route.

Every candidate must have a byte-exact N3 raw artifact, N4 digest and frame
bundle, and an exact raw range.  The lexical index must cover exactly the
public candidate set.  This conservative input requirement lets a later
coordinator choose a route without inventing identities or ranges.  Hidden
relevance labels are never read or copied.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from ._full_flow_primitives import (
    W4_IDENTIFIER_PATTERN as _IDENTIFIER,
    canonical_json_bytes,
    canonical_json_lines_bytes,
    checked_identifier,
    checked_lower_sha256,
    pretty_json_bytes,
    sha256_hex,
    strict_json_loads,
)
from .full_flow_exact_range_catalog import (
    CATALOG_NAME as RANGE_CATALOG_NAME,
    verify_full_flow_exact_range_catalog,
)
from .full_flow_w4_retrieval_runtime import (
    W4_INDEX_QUERY_MAX_CANDIDATES,
    load_full_flow_w4_retrieval_runtime_inputs,
    validate_full_flow_w4_public_runtime_task,
)
from .index_service import verify_n2_index_package
from .n4_derived_data_plane import (
    PACKAGE_MANIFEST_NAME as N4_MANIFEST_NAME,
    verify_n4_derived_data_package,
)
from .raw_cold_data_plane import (
    PACKAGE_MANIFEST_NAME as N3_MANIFEST_NAME,
    verify_raw_cold_data_plane_package,
)


W4_CANDIDATE_ROUTE_PLAN_SCHEMA_VERSION = (
    "pathfinder.full-flow-w4-candidate-route-plan/v1alpha1"
)
W4_CANDIDATE_ROUTE_TRIAL_SCHEMA_VERSION = (
    "pathfinder.full-flow-w4-candidate-route-trial/v1alpha1"
)
W4_CANDIDATE_ROUTE_OPERATION_SCHEMA_VERSION = (
    "pathfinder.full-flow-w4-candidate-route-operation/v1alpha1"
)
W4_CANDIDATE_ARTIFACT_CATALOG_SCHEMA_VERSION = (
    "pathfinder.full-flow-w4-candidate-artifact-catalog/v1alpha1"
)

PLAN_NAME = "w4-candidate-route-plan.json"
TRIALS_NAME = "w4-candidate-route-trials.jsonl"
OPERATIONS_NAME = "w4-candidate-route-operations.jsonl"
ARTIFACT_CATALOG_NAME = "w4-candidate-artifact-catalog.json"
TASK_NAME = "w4-candidate-route-task.json"
CHECKSUMS_NAME = "SHA256SUMS"

_FILES = frozenset({
    PLAN_NAME,
    TRIALS_NAME,
    OPERATIONS_NAME,
    ARTIFACT_CATALOG_NAME,
    TASK_NAME,
})
_DESIGNS = {f"D{index}" for index in range(8)}
_RAW_DESIGNS = {"D0", "D4"}
_INDEXED_RAW_DESIGNS = {"D1", "D5"}
_REMOTE_DERIVED_DESIGNS = {"D2", "D6"}
_LOCAL_DERIVED_DESIGNS = {"D3", "D7"}
_ROUTE_FAMILY_BY_DESIGN = {
    "D0": "raw",
    "D1": "indexed-raw",
    "D2": "remote-derived",
    "D3": "local-cache-derived",
    "D4": "raw",
    "D5": "indexed-raw",
    "D6": "remote-derived",
    "D7": "local-cache-derived",
}
_EXECUTOR_BY_DESIGN = {
    **{design: "N7" for design in ("D0", "D1", "D2", "D3")},
    **{design: "N8" for design in ("D4", "D5", "D6", "D7")},
}


class FullFlowW4CandidateRouteError(ValueError):
    """Raised when candidate-wide route inputs are incomplete or ambiguous."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FullFlowW4CandidateRouteError(message)


def _canonical(value: Any) -> bytes:
    return canonical_json_bytes(
        value,
        error_type=FullFlowW4CandidateRouteError,
        error_message="candidate route value is not canonical JSON",
    )


def _json_bytes(value: Any) -> bytes:
    return pretty_json_bytes(value)


def _jsonl_bytes(values: Sequence[Mapping[str, Any]]) -> bytes:
    return canonical_json_lines_bytes(
        values,
        error_type=FullFlowW4CandidateRouteError,
        error_message="candidate route value is not canonical JSON",
    )


def _sha256(value: bytes) -> str:
    return sha256_hex(value)


def _identifier(value: Any, name: str) -> str:
    return str(
        checked_identifier(
            value,
            name,
            error_type=FullFlowW4CandidateRouteError,
        )
    )


def _digest(value: Any, name: str) -> str:
    return str(
        checked_lower_sha256(
            value,
            name,
            error_type=FullFlowW4CandidateRouteError,
        )
    )


def _positive(value: Any, name: str) -> int:
    _require(type(value) is int and value > 0, f"{name} must be positive")
    return int(value)


def _strict_json(path: Path, name: str) -> dict[str, Any]:
    _require(path.is_file() and not path.is_symlink(), f"{name} is missing")

    try:
        value = strict_json_loads(
            path.read_text(encoding="utf-8"),
            error_type=FullFlowW4CandidateRouteError,
            duplicate_key_message=lambda key: f"{name} repeats key {key}",
            nonfinite_number_message=(
                lambda token: f"{name} contains non-finite number {token}"
            ),
        )
    except FullFlowW4CandidateRouteError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowW4CandidateRouteError(f"cannot read {name}") from exc
    _require(isinstance(value, dict), f"{name} must be an object")
    return value


def _checksums(documents: Mapping[str, bytes]) -> bytes:
    return b"".join(
        f"{_sha256(documents[name])}  {name}\n".encode("utf-8")
        for name in sorted(documents)
    )


def _verify_files(root: Path) -> None:
    _require(root.is_dir() and not root.is_symlink(), "route package is missing")
    entries = list(root.iterdir())
    _require(
        all(path.is_file() and not path.is_symlink() for path in entries),
        "route package contains a non-regular file",
    )
    _require(
        {path.name for path in entries} == _FILES | {CHECKSUMS_NAME},
        "route package file set changed",
    )
    documents = {name: (root / name).read_bytes() for name in _FILES}
    _require(
        (root / CHECKSUMS_NAME).read_bytes() == _checksums(documents),
        "candidate route checksums failed",
    )


def _publish(target: Path, documents: Mapping[str, bytes]) -> None:
    _require(not target.exists(), f"output directory already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    parent = Path(tempfile.mkdtemp(prefix=".w4-candidate-routes-", dir=target.parent))
    stage = parent / "output"
    try:
        stage.mkdir()
        for name, payload in documents.items():
            (stage / name).write_bytes(payload)
        (stage / CHECKSUMS_NAME).write_bytes(_checksums(documents))
        _verify_files(stage)
        os.replace(stage, target)
    finally:
        shutil.rmtree(parent, ignore_errors=True)


def _require_disjoint_output(target: Path, sources: Sequence[Path]) -> None:
    for source in sources:
        _require(
            target != source
            and not target.is_relative_to(source)
            and not source.is_relative_to(target),
            f"output directory overlaps an input: {source}",
        )


def _identity(
    row: Mapping[str, Any],
    expected_representation: str,
    *,
    catalog_version: str | None = None,
) -> dict[str, Any]:
    _require(
        row.get("representation_id") == expected_representation,
        f"expected {expected_representation} artifact",
    )
    result = {
        "representation_id": expected_representation,
        "artifact_sha256": _digest(
            row.get("artifact_sha256"), "artifact SHA-256"
        ),
        "artifact_size_bytes": _positive(
            row.get("artifact_size_bytes"), "artifact size"
        ),
        "object_catalog_version": _identifier(
            (
                row.get("catalog_version")
                if catalog_version is None
                else catalog_version
            ),
            "object catalog version",
        ),
    }
    plans = row.get("plan_ids")
    _require(
        isinstance(plans, list)
        and plans == sorted(set(plans))
        and all(
            isinstance(value, str)
            and _IDENTIFIER.fullmatch(value) is not None
            for value in plans
        ),
        "artifact plan IDs are invalid",
    )
    result["data_agent_plan_ids"] = list(plans)
    return result


def _derived_provenance(
    row: Mapping[str, Any],
    *,
    raw_identity: Mapping[str, Any],
) -> dict[str, Any]:
    value = row.get("provenance")
    _require(
        isinstance(value, Mapping)
        and set(value)
        == {
            "schema_version",
            "producer_node_id",
            "publication_source_id",
            "source_representation_id",
            "source_content_sha256",
            "derivation_id",
            "derivation_sha256",
        },
        "N4 candidate provenance fields changed",
    )
    _require(
        value.get("schema_version")
        == "pathfinder.simulator-derived-artifact-provenance/v1alpha1"
        and value.get("producer_node_id") == "N5"
        and value.get("source_representation_id") == "raw_video"
        and value.get("source_content_sha256")
        == raw_identity["artifact_sha256"],
        "N4 candidate provenance does not bind the N3 raw artifact",
    )
    for name in ("publication_source_id", "derivation_id"):
        _identifier(value.get(name), f"N4 provenance {name}")
    _digest(value.get("derivation_sha256"), "N4 derivation SHA-256")
    return dict(value)


def _source_catalog(
    runtime_overlay_dir: Path,
    n3_package_dir: Path,
    n4_package_dir: Path,
    index_package_dir: Path,
    exact_range_catalog_dir: Path,
) -> tuple[dict[str, Any], Mapping[str, Any], tuple[Mapping[str, Any], ...]]:
    runtime = load_full_flow_w4_retrieval_runtime_inputs(runtime_overlay_dir)
    try:
        n3_verified = verify_raw_cold_data_plane_package(n3_package_dir)
        n4_verified = verify_n4_derived_data_package(n4_package_dir)
        index_verified = verify_n2_index_package(index_package_dir)
        range_verified = verify_full_flow_exact_range_catalog(
            exact_range_catalog_dir, n3_package_dir
        )
    except Exception as exc:
        raise FullFlowW4CandidateRouteError(
            "a candidate-route source package failed verification"
        ) from exc

    n3 = _strict_json(n3_package_dir / N3_MANIFEST_NAME, "N3 package manifest")
    n4 = _strict_json(n4_package_dir / N4_MANIFEST_NAME, "N4 package manifest")
    index = _strict_json(index_package_dir / "lexical-index.json", "N2 index")
    ranges = _strict_json(
        exact_range_catalog_dir / RANGE_CATALOG_NAME,
        "exact range catalog",
    )
    candidate_rows = runtime.public_task["candidate_objects"]
    candidate_ids = [str(row["object_id"]) for row in candidate_rows]
    _require(
        candidate_ids == sorted(set(candidate_ids)),
        "public candidate IDs are not canonical",
    )
    index_ids = index.get("candidate_object_ids")
    _require(
        index_ids == candidate_ids,
        "N2 index does not exactly cover the public W4 candidate corpus",
    )
    _require(
        index_verified.get("document_count") == len(candidate_ids),
        "N2 verifier candidate count differs from W4",
    )
    n3_catalog_version = _identifier(
        n3_verified.get("catalog_version"), "N3 catalog version"
    )
    n4_catalog_version = _identifier(
        n4_verified.get("catalog_version"), "N4 catalog version"
    )
    _require(
        n3.get("catalog_version") == n3_catalog_version
        and n4.get("catalog_version") == n4_catalog_version,
        "candidate source manifest catalog version differs from verification",
    )
    n3_checksums = n3_package_dir / CHECKSUMS_NAME
    _require(
        n3_checksums.is_file() and not n3_checksums.is_symlink(),
        "N3 package checksum commitment is missing",
    )

    n3_rows = n3.get("objects")
    n4_rows = n4.get("objects")
    range_rows = ranges.get("entries")
    _require(isinstance(n3_rows, list), "N3 object list is invalid")
    _require(isinstance(n4_rows, list), "N4 object list is invalid")
    _require(isinstance(range_rows, list), "exact range list is invalid")
    raw_candidates = [row for row in n3_rows if isinstance(row, Mapping)]
    derived_candidates = [row for row in n4_rows if isinstance(row, Mapping)]
    range_candidates = [row for row in range_rows if isinstance(row, Mapping)]
    raw_by_id = {
        str(row.get("object_id")): row
        for row in raw_candidates
    }
    derived_by_key = {
        (str(row.get("object_id")), str(row.get("representation_id"))): row
        for row in derived_candidates
    }
    range_by_id = {
        str(row.get("object_id")): row
        for row in range_candidates
    }
    _require(
        len(raw_by_id) == len(raw_candidates), "N3 candidate identity repeats"
    )
    _require(
        len(derived_by_key) == len(derived_candidates),
        "N4 candidate representation identity repeats",
    )
    _require(
        len(range_by_id) == len(range_candidates),
        "exact range candidate identity repeats",
    )
    _require(set(raw_by_id) == set(candidate_ids), "N3 candidate coverage changed")
    _require(set(range_by_id) == set(candidate_ids), "range coverage changed")
    _require(
        set(derived_by_key)
        == {
            (object_id, representation_id)
            for object_id in candidate_ids
            for representation_id in (
                "multimodal_digest",
                "sampled_frame_bundle",
            )
        },
        "N4 candidate representation coverage changed",
    )

    public_digest_by_id = {
        str(row["object_id"]): row for row in candidate_rows
    }
    objects: list[dict[str, Any]] = []
    for object_id in candidate_ids:
        raw = _identity(raw_by_id[object_id], "raw_video")
        digest = _identity(
            derived_by_key[(object_id, "multimodal_digest")],
            "multimodal_digest",
            catalog_version=n4_catalog_version,
        )
        frames = _identity(
            derived_by_key[(object_id, "sampled_frame_bundle")],
            "sampled_frame_bundle",
            catalog_version=n4_catalog_version,
        )
        committed = public_digest_by_id[object_id]
        _require(
            digest["artifact_sha256"] == committed["artifact_sha256"]
            and digest["artifact_size_bytes"]
            == committed["artifact_size_bytes"],
            f"public W4 digest identity differs from N4 for {object_id}",
        )
        range_row = range_by_id[object_id]
        exact_range = {
            "object_id": _identifier(range_row.get("object_id"), "range object"),
            "representation_id": "raw_video",
            "object_catalog_version": _identifier(
                range_row.get("object_catalog_version"), "range catalog version"
            ),
            "full_artifact_size_bytes": _positive(
                range_row.get("full_artifact_size_bytes"), "range full size"
            ),
            "full_artifact_sha256": _digest(
                range_row.get("full_artifact_sha256"), "range full digest"
            ),
            "range_start": range_row.get("range_start"),
            "range_end": range_row.get("range_end"),
            "range_size_bytes": range_row.get("range_size_bytes"),
            "range_sha256": _digest(
                range_row.get("range_sha256"), "range digest"
            ),
            "selection_semantics": range_row.get("selection_semantics"),
        }
        _require(
            type(exact_range["range_start"]) is int
            and type(exact_range["range_end"]) is int
            and exact_range["range_start"] == 0
            and exact_range["range_end"] == raw["artifact_size_bytes"] - 1
            and exact_range["range_size_bytes"]
            == raw["artifact_size_bytes"]
            and exact_range["full_artifact_sha256"] == raw["artifact_sha256"]
            and exact_range["range_sha256"] == raw["artifact_sha256"]
            and exact_range["full_artifact_size_bytes"]
            == raw["artifact_size_bytes"]
            and exact_range["object_catalog_version"]
            == raw["object_catalog_version"]
            and exact_range["selection_semantics"]
            == "exact-full-object-fallback",
            f"exact range does not bind N3 raw artifact {object_id}",
        )
        required_raw = {"D0", "D1", "D4", "D5"}
        required_derived = {"D2", "D3", "D6", "D7"}
        _require(
            required_raw <= set(raw["data_agent_plan_ids"]),
            f"N3 raw artifact lacks W4 design bindings for {object_id}",
        )
        for representation in (digest, frames):
            _require(
                required_derived <= set(representation["data_agent_plan_ids"]),
                f"N4 artifact lacks W4 design bindings for {object_id}",
            )
        provenance = {
            representation_id: _derived_provenance(
                derived_by_key[(object_id, representation_id)],
                raw_identity=raw,
            )
            for representation_id in (
                "multimodal_digest",
                "sampled_frame_bundle",
            )
        }
        objects.append({
            "object_id": object_id,
            "representations": {
                "raw_video": raw,
                "multimodal_digest": digest,
                "sampled_frame_bundle": frames,
            },
            "derived_provenance": provenance,
            "exact_raw_range": exact_range,
        })

    catalog: dict[str, Any] = {
        "schema_version": W4_CANDIDATE_ARTIFACT_CATALOG_SCHEMA_VERSION,
        "status": "FROZEN_W4_CANDIDATE_ARTIFACT_CATALOG",
        "catalog_id": "w4-candidate-artifacts-" + _sha256(
            _canonical({
                "runtime_plan_sha256": runtime.plan["plan_sha256"],
                "candidate_set_sha256": runtime.public_task[
                    "candidate_set_sha256"
                ],
            })
        )[:32],
        "runtime_plan_sha256": runtime.plan["plan_sha256"],
        "retrieval_task_binding_sha256": runtime.public_task[
            "task_binding_sha256"
        ],
        "candidate_set_sha256": runtime.public_task["candidate_set_sha256"],
        "index_binding": {
            "logical_author_node_id": "N2",
            "serving_node_ids": ["N2", "N7", "N8"],
            "index_id": _identifier(index_verified.get("index_id"), "index_id"),
            "index_sha256": _digest(
                index_verified.get("index_sha256"), "index SHA-256"
            ),
            "source_manifest_sha256": _digest(
                index.get("source_manifest_sha256"),
                "index source manifest SHA-256",
            ),
            "candidate_object_ids": candidate_ids,
        },
        "objects": objects,
        "source_package_sha256": {
            "n3_package": _digest(
                _sha256(n3_checksums.read_bytes()), "N3 package SHA-256"
            ),
            "n4_package": _digest(
                n4_verified.get("package_sha256"), "N4 package SHA-256"
            ),
            "exact_range_catalog": _digest(
                range_verified.get("catalog_sha256"), "range catalog SHA-256"
            ),
            "n2_index": _digest(
                index_verified.get("index_sha256"), "N2 index SHA-256"
            ),
        },
        "hidden_relevance_values_included": False,
        "artifact_bytes_copied": False,
        "endpoints_included": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    catalog["catalog_sha256"] = _sha256(_canonical(catalog))
    _validate_artifact_catalog(catalog, runtime.public_task)
    return catalog, runtime.public_task, runtime.trials


def _operation(
    *,
    plan_id: str,
    trial: Mapping[str, Any],
    operations: list[dict[str, Any]],
    suffix: str,
    action: str,
    service_contract_id: str,
    node_ids: list[str],
    dependencies: list[str],
    object_id: str | None = None,
    representation_identity: Mapping[str, Any] | None = None,
    exact_content_range: Mapping[str, Any] | None = None,
    activation: Mapping[str, Any] | None = None,
    data_agent_plan_id: str | None = None,
    index_query_template: Mapping[str, Any] | None = None,
) -> str:
    key = f"{trial['trial_key']}|w4-retrieval|{suffix}"
    operations.append({
        "schema_version": W4_CANDIDATE_ROUTE_OPERATION_SCHEMA_VERSION,
        "physical_plan_id": plan_id,
        "trial_key": trial["trial_key"],
        "operation_key": key,
        "operation_index": len(operations),
        "design_id": trial["design_id"],
        "repetition": trial["repetition"],
        "action": action,
        "service_contract_id": service_contract_id,
        "logical_node_ids": node_ids,
        "dependency_operation_keys": dependencies,
        "object_id": object_id,
        "representation_identity": (
            None if representation_identity is None else dict(representation_identity)
        ),
        "exact_content_range": (
            None if exact_content_range is None else dict(exact_content_range)
        ),
        "activation": (
            {"kind": "always"} if activation is None else dict(activation)
        ),
        "data_agent_plan_id": data_agent_plan_id,
        "index_query_template": (
            None if index_query_template is None else dict(index_query_template)
        ),
        "credentials_recorded": False,
    })
    return key


def _object_operation_token(object_id: str) -> str:
    """Injectively bind an arbitrary public object ID into operation keys."""

    return "object-" + _sha256(object_id.encode("utf-8"))


def _compile_trial(
    plan_id: str,
    trial: Mapping[str, Any],
    task: Mapping[str, Any],
    catalog: Mapping[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    design = str(trial["design_id"])
    executor = str(trial["executor_node_id"])
    _require(
        design in _DESIGNS
        and trial.get("route_family") == _ROUTE_FAMILY_BY_DESIGN[design]
        and executor == _EXECUTOR_BY_DESIGN[design]
        and trial.get("repetition") in {0, 1},
        "candidate route source trial has incompatible design semantics",
    )
    objects = catalog["objects"]
    rerank_count = max(task["quality_metrics"]["top_k"])
    operations: list[dict[str, Any]] = []
    admit = _operation(
        plan_id=plan_id,
        trial=trial,
        operations=operations,
        suffix="admit",
        action="admit-public-retrieval",
        service_contract_id="N1.trial-control",
        node_ids=["N1"],
        dependencies=[],
    )
    index_key: str | None = None
    fallback_ranking_key: str | None = None
    fallback_ranking_node: str | None = None
    if design not in _RAW_DESIGNS:
        index_node = executor if design in _LOCAL_DERIVED_DESIGNS else "N2"
        object_ids = [row["object_id"] for row in objects]
        shard_keys: list[str] = []
        for shard_index, start in enumerate(
            range(0, len(object_ids), W4_INDEX_QUERY_MAX_CANDIDATES)
        ):
            shard = object_ids[start : start + W4_INDEX_QUERY_MAX_CANDIDATES]
            shard_keys.append(_operation(
                plan_id=plan_id,
                trial=trial,
                operations=operations,
                suffix=f"query-index-shard-{shard_index:04d}",
                action="query-candidate-index-shard",
                service_contract_id=(
                    f"{index_node}.local-index"
                    if index_node in {"N7", "N8"}
                    else "N2.global-index"
                ),
                node_ids=[index_node],
                dependencies=[admit],
                index_query_template={
                    "query_id": task["query_id"],
                    "query_text": task["query_text"],
                    "retrieval_task_binding_sha256": task[
                        "task_binding_sha256"
                    ],
                    "index_id": catalog["index_binding"]["index_id"],
                    "index_sha256": catalog["index_binding"]["index_sha256"],
                    "source_manifest_sha256": catalog["index_binding"][
                        "source_manifest_sha256"
                    ],
                    "requested_node_id": index_node,
                    "shard_index": shard_index,
                    "top_k": len(shard),
                    "candidate_object_ids": shard,
                },
            ))
        index_key = _operation(
            plan_id=plan_id,
            trial=trial,
            operations=operations,
            suffix="merge-index-shards",
            action="merge-complete-candidate-index",
            service_contract_id=f"{index_node}.index-merge",
            node_ids=[index_node],
            dependencies=shard_keys,
        )
        fallback_ranking_key = index_key
        fallback_ranking_node = index_node

    prepared: list[str] = []
    coarse_inputs: list[str] = []
    for candidate in objects:
        object_id = str(candidate["object_id"])
        identities = candidate["representations"]
        safe = _object_operation_token(object_id)
        if design in _RAW_DESIGNS | _INDEXED_RAW_DESIGNS:
            activation = None
            exact_range = None
            if design in _INDEXED_RAW_DESIGNS:
                activation = {
                    "kind": "ranking-prefix",
                    "ranking_operation_key": index_key,
                    "limit": rerank_count,
                    "object_id": object_id,
                }
                exact_range = candidate["exact_raw_range"]
            access = _operation(
                plan_id=plan_id,
                trial=trial,
                operations=operations,
                suffix=f"candidate-{safe}-access-raw",
                action=("access-exact-raw-range" if exact_range else "access-raw"),
                service_contract_id="N3.raw-data-agent",
                node_ids=["N3"],
                dependencies=[admit] if index_key is None else [index_key],
                object_id=object_id,
                representation_identity=identities["raw_video"],
                exact_content_range=exact_range,
                activation=activation,
                data_agent_plan_id=design,
            )
            transfer = _operation(
                plan_id=plan_id,
                trial=trial,
                operations=operations,
                suffix=f"candidate-{safe}-transfer-raw",
                action="transfer-artifact-bytes",
                service_contract_id=f"transport.N3-{executor}-raw",
                node_ids=["N3", executor],
                dependencies=[access],
                object_id=object_id,
                representation_identity=identities["raw_video"],
                exact_content_range=exact_range,
                activation=activation,
            )
            prepared.append(_operation(
                plan_id=plan_id,
                trial=trial,
                operations=operations,
                suffix=f"candidate-{safe}-prepare-raw",
                action="prepare-retrieval-candidate",
                service_contract_id=f"{executor}.execution-compute",
                node_ids=[executor],
                dependencies=[transfer],
                object_id=object_id,
                representation_identity=identities["raw_video"],
                exact_content_range=exact_range,
                activation=activation,
            ))
        else:
            prefix = {
                "kind": "ranking-prefix",
                "ranking_operation_key": index_key,
                "limit": rerank_count,
                "object_id": object_id,
            }
            if design in _REMOTE_DERIVED_DESIGNS:
                access_digest = _operation(
                    plan_id=plan_id,
                    trial=trial,
                    operations=operations,
                    suffix=f"candidate-{safe}-access-digest",
                    action="access-derived-artifact",
                    service_contract_id="N4.derived-data-agent",
                    node_ids=["N4"],
                    dependencies=[index_key],
                    object_id=object_id,
                    representation_identity=identities["multimodal_digest"],
                    activation=prefix,
                    data_agent_plan_id=design,
                )
                coarse_inputs.append(_operation(
                    plan_id=plan_id,
                    trial=trial,
                    operations=operations,
                    suffix=f"candidate-{safe}-transfer-digest",
                    action="transfer-artifact-bytes",
                    service_contract_id=f"transport.N4-{executor}-derived",
                    node_ids=["N4", executor],
                    dependencies=[access_digest],
                    object_id=object_id,
                    representation_identity=identities["multimodal_digest"],
                    activation=prefix,
                ))
            else:
                lookup = _operation(
                    plan_id=plan_id,
                    trial=trial,
                    operations=operations,
                    suffix=f"candidate-{safe}-cache-lookup-digest",
                    action="lookup",
                    service_contract_id=f"{executor}.persistent-cache",
                    node_ids=[executor],
                    dependencies=[index_key],
                    object_id=object_id,
                    representation_identity=identities["multimodal_digest"],
                    activation=prefix,
                )
                hit = {
                    "kind": "ranking-prefix-and-cache-branch",
                    "ranking_operation_key": index_key,
                    "limit": rerank_count,
                    "object_id": object_id,
                    "cache_lookup_operation_key": lookup,
                    "equals": "hit",
                }
                miss = dict(hit)
                miss["equals"] = "miss"
                read_local = _operation(
                    plan_id=plan_id,
                    trial=trial,
                    operations=operations,
                    suffix=f"candidate-{safe}-read-local-digest",
                    action="read",
                    service_contract_id=f"{executor}.persistent-cache",
                    node_ids=[executor],
                    dependencies=[lookup],
                    object_id=object_id,
                    representation_identity=identities["multimodal_digest"],
                    activation=hit,
                )
                read_remote = _operation(
                    plan_id=plan_id,
                    trial=trial,
                    operations=operations,
                    suffix=f"candidate-{safe}-access-remote-digest",
                    action="access-derived-artifact",
                    service_contract_id="N4.derived-data-agent",
                    node_ids=["N4"],
                    dependencies=[lookup],
                    object_id=object_id,
                    representation_identity=identities["multimodal_digest"],
                    activation=miss,
                    data_agent_plan_id=design,
                )
                transfer_remote = _operation(
                    plan_id=plan_id,
                    trial=trial,
                    operations=operations,
                    suffix=f"candidate-{safe}-transfer-remote-digest",
                    action="transfer-artifact-bytes",
                    service_contract_id=f"transport.N4-{executor}-derived",
                    node_ids=["N4", executor],
                    dependencies=[read_remote],
                    object_id=object_id,
                    representation_identity=identities["multimodal_digest"],
                    activation=miss,
                )
                inserted = _operation(
                    plan_id=plan_id,
                    trial=trial,
                    operations=operations,
                    suffix=f"candidate-{safe}-insert-digest",
                    action="insert",
                    service_contract_id=f"{executor}.persistent-cache",
                    node_ids=[executor],
                    dependencies=[transfer_remote],
                    object_id=object_id,
                    representation_identity=identities["multimodal_digest"],
                    activation=miss,
                )
                coarse_inputs.append(_operation(
                    plan_id=plan_id,
                    trial=trial,
                    operations=operations,
                    suffix=f"candidate-{safe}-digest-ready",
                    action="join-hit-or-miss-branch",
                    service_contract_id=f"{executor}.branch-join",
                    node_ids=[executor],
                    dependencies=[read_local, inserted],
                    object_id=object_id,
                    representation_identity=identities["multimodal_digest"],
                    activation=prefix,
                ))

    if design in _REMOTE_DERIVED_DESIGNS | _LOCAL_DERIVED_DESIGNS:
        coarse = _operation(
            plan_id=plan_id,
            trial=trial,
            operations=operations,
            suffix="coarse-rank-digests",
            action="rank-digest-prefix-and-append-index-tail",
            service_contract_id=f"{executor}.execution-compute",
            node_ids=[executor],
            dependencies=[index_key, *coarse_inputs],
        )
        fallback_ranking_key = coarse
        fallback_ranking_node = executor
        for candidate in objects:
            object_id = str(candidate["object_id"])
            safe = _object_operation_token(object_id)
            identity = candidate["representations"]["sampled_frame_bundle"]
            activation = {
                "kind": "ranking-prefix",
                "ranking_operation_key": coarse,
                "limit": 1,
                "object_id": object_id,
            }
            if design in _REMOTE_DERIVED_DESIGNS:
                access = _operation(
                    plan_id=plan_id,
                    trial=trial,
                    operations=operations,
                    suffix=f"candidate-{safe}-access-frames",
                    action="access-derived-artifact",
                    service_contract_id="N4.derived-data-agent",
                    node_ids=["N4"],
                    dependencies=[coarse],
                    object_id=object_id,
                    representation_identity=identity,
                    activation=activation,
                    data_agent_plan_id=design,
                )
                ready = _operation(
                    plan_id=plan_id,
                    trial=trial,
                    operations=operations,
                    suffix=f"candidate-{safe}-transfer-frames",
                    action="transfer-artifact-bytes",
                    service_contract_id=f"transport.N4-{executor}-derived",
                    node_ids=["N4", executor],
                    dependencies=[access],
                    object_id=object_id,
                    representation_identity=identity,
                    activation=activation,
                )
            else:
                lookup = _operation(
                    plan_id=plan_id,
                    trial=trial,
                    operations=operations,
                    suffix=f"candidate-{safe}-cache-lookup-frames",
                    action="lookup",
                    service_contract_id=f"{executor}.persistent-cache",
                    node_ids=[executor],
                    dependencies=[coarse],
                    object_id=object_id,
                    representation_identity=identity,
                    activation=activation,
                )
                hit = {
                    "kind": "ranking-prefix-and-cache-branch",
                    "ranking_operation_key": coarse,
                    "limit": 1,
                    "object_id": object_id,
                    "cache_lookup_operation_key": lookup,
                    "equals": "hit",
                }
                miss = dict(hit)
                miss["equals"] = "miss"
                read_local = _operation(
                    plan_id=plan_id,
                    trial=trial,
                    operations=operations,
                    suffix=f"candidate-{safe}-read-local-frames",
                    action="read",
                    service_contract_id=f"{executor}.persistent-cache",
                    node_ids=[executor],
                    dependencies=[lookup],
                    object_id=object_id,
                    representation_identity=identity,
                    activation=hit,
                )
                read_remote = _operation(
                    plan_id=plan_id,
                    trial=trial,
                    operations=operations,
                    suffix=f"candidate-{safe}-access-remote-frames",
                    action="access-derived-artifact",
                    service_contract_id="N4.derived-data-agent",
                    node_ids=["N4"],
                    dependencies=[lookup],
                    object_id=object_id,
                    representation_identity=identity,
                    activation=miss,
                    data_agent_plan_id=design,
                )
                transfer_remote = _operation(
                    plan_id=plan_id,
                    trial=trial,
                    operations=operations,
                    suffix=f"candidate-{safe}-transfer-remote-frames",
                    action="transfer-artifact-bytes",
                    service_contract_id=f"transport.N4-{executor}-derived",
                    node_ids=["N4", executor],
                    dependencies=[read_remote],
                    object_id=object_id,
                    representation_identity=identity,
                    activation=miss,
                )
                inserted = _operation(
                    plan_id=plan_id,
                    trial=trial,
                    operations=operations,
                    suffix=f"candidate-{safe}-insert-frames",
                    action="insert",
                    service_contract_id=f"{executor}.persistent-cache",
                    node_ids=[executor],
                    dependencies=[transfer_remote],
                    object_id=object_id,
                    representation_identity=identity,
                    activation=miss,
                )
                ready = _operation(
                    plan_id=plan_id,
                    trial=trial,
                    operations=operations,
                    suffix=f"candidate-{safe}-frames-ready",
                    action="join-hit-or-miss-branch",
                    service_contract_id=f"{executor}.branch-join",
                    node_ids=[executor],
                    dependencies=[read_local, inserted],
                    object_id=object_id,
                    representation_identity=identity,
                    activation=activation,
                )
            prepared.append(_operation(
                plan_id=plan_id,
                trial=trial,
                operations=operations,
                suffix=f"candidate-{safe}-prepare-frames",
                action="prepare-retrieval-candidate",
                service_contract_id=f"{executor}.execution-compute",
                node_ids=[executor],
                dependencies=[ready],
                object_id=object_id,
                representation_identity=identity,
                activation=activation,
            ))

    inference_inputs: list[str] = []
    operations_by_key = {
        operation["operation_key"]: operation for operation in operations
    }
    for prepared_key in prepared:
        source = operations_by_key[prepared_key]
        object_id = str(source["object_id"])
        safe = _object_operation_token(object_id)
        inference_inputs.append(_operation(
            plan_id=plan_id,
            trial=trial,
            operations=operations,
            suffix=f"candidate-{safe}-send-model-input",
            action="transfer-prepared-model-input",
            service_contract_id=f"transport.{executor}-N6-model-input",
            node_ids=[executor, "N6"],
            dependencies=[prepared_key],
            object_id=object_id,
            activation=source["activation"],
        ))

    ranking_dependencies = list(inference_inputs)
    if fallback_ranking_key is not None:
        assert fallback_ranking_node is not None
        ranking_dependencies.insert(0, _operation(
            plan_id=plan_id,
            trial=trial,
            operations=operations,
            suffix="send-ranking-fallback",
            action="transfer-ranking-fallback",
            service_contract_id=(
                f"transport.{fallback_ranking_node}-N6-ranking-state"
            ),
            node_ids=[fallback_ranking_node, "N6"],
            dependencies=[fallback_ranking_key],
        ))
    rank = _operation(
        plan_id=plan_id,
        trial=trial,
        operations=operations,
        suffix="rank-candidate-set",
        action="rank-complete-candidate-set",
        service_contract_id="N6.semantic-inference",
        node_ids=["N6"],
        dependencies=ranking_dependencies,
    )
    returned = _operation(
        plan_id=plan_id,
        trial=trial,
        operations=operations,
        suffix="return-ranking",
        action="return-public-ranking-to-n1",
        service_contract_id="transport.N6-N1-retrieval-evaluation",
        node_ids=["N6", "N1"],
        dependencies=[rank],
    )
    for position, operation in enumerate(operations):
        operation["operation_index"] = position
    row = {
        "schema_version": W4_CANDIDATE_ROUTE_TRIAL_SCHEMA_VERSION,
        "physical_plan_id": plan_id,
        "trial_key": trial["trial_key"],
        "order_index": trial["order_index"],
        "design_id": design,
        "repetition": trial["repetition"],
        "route_family": trial["route_family"],
        "executor_node_id": executor,
        "candidate_object_count": len(objects),
        "rerank_candidate_count": rerank_count,
        "operation_count": len(operations),
        "operation_keys": [operation["operation_key"] for operation in operations],
        "terminal_operation_key": returned,
        "complete_ranking_rule": (
            "semantic-rank-complete-raw-corpus"
            if design in _RAW_DESIGNS
            else (
                "semantic-selected-frame-then-append-coarse-digest-tail"
                if design
                in _REMOTE_DERIVED_DESIGNS | _LOCAL_DERIVED_DESIGNS
                else "semantic-rerank-prefix-then-append-index-tail"
            )
        ),
        "hidden_relevance_values_included": False,
        "credentials_recorded": False,
    }
    return row, operations


def _documents(
    runtime_overlay_dir: Path,
    n3_package_dir: Path,
    n4_package_dir: Path,
    index_package_dir: Path,
    exact_range_catalog_dir: Path,
    physical_plan_id: str,
) -> dict[str, bytes]:
    plan_id = _identifier(physical_plan_id, "physical_plan_id")
    catalog, task, runtime_trials = _source_catalog(
        runtime_overlay_dir,
        n3_package_dir,
        n4_package_dir,
        index_package_dir,
        exact_range_catalog_dir,
    )
    trials: list[dict[str, Any]] = []
    operations: list[dict[str, Any]] = []
    for runtime_trial in runtime_trials:
        trial, rows = _compile_trial(plan_id, runtime_trial, task, catalog)
        trials.append(trial)
        operations.extend(rows)
    _require(
        len(trials) == 16
        and {(row["design_id"], row["repetition"]) for row in trials}
        == {(design, repetition) for design in _DESIGNS for repetition in (0, 1)},
        "candidate route trial coverage changed",
    )
    trial_bytes = _jsonl_bytes(trials)
    operation_bytes = _jsonl_bytes(operations)
    catalog_bytes = _json_bytes(catalog)
    task_bytes = _json_bytes(task)
    plan: dict[str, Any] = {
        "schema_version": W4_CANDIDATE_ROUTE_PLAN_SCHEMA_VERSION,
        "status": "FROZEN_W4_CANDIDATE_ROUTE_BLUEPRINTS",
        "physical_plan_id": plan_id,
        "runtime_overlay_id": runtime_trials[0]["runtime_overlay_id"],
        "runtime_plan_sha256": catalog["runtime_plan_sha256"],
        "retrieval_task_binding_sha256": task["task_binding_sha256"],
        "candidate_set_sha256": task["candidate_set_sha256"],
        "candidate_object_count": len(catalog["objects"]),
        "trial_count": len(trials),
        "operation_count": len(operations),
        "design_ids": sorted(_DESIGNS),
        "output_sha256": {
            ARTIFACT_CATALOG_NAME: _sha256(catalog_bytes),
            TASK_NAME: _sha256(task_bytes),
            TRIALS_NAME: _sha256(trial_bytes),
            OPERATIONS_NAME: _sha256(operation_bytes),
        },
        "claim_boundary": {
            "candidate_wide_physical_route_blueprints_compiled": True,
            "candidate_artifact_bindings_complete": True,
            "exact_raw_ranges_bound": True,
            "complete_index_candidate_coverage_bound": True,
            "index_source_lineage_to_representation_manifest_verified": False,
            "route_execution_performed": False,
            "multi_candidate_route_coordinator_implemented": False,
            "route_bound_ranking_evidence_present": False,
            "physical_design_retrieval_comparison_ready": False,
            "current_64_trial_matrix_modified": False,
            "current_64_trial_matrix_native_w4_semantics": (
                "multiple-choice-placeholder"
            ),
            "remaining_local_implementation": [
                "multi-candidate-physical-route-coordinator",
                "route-bound-semantic-ranker",
                "candidate-cache-branch-evidence",
            ],
            "remaining_operator_inputs": [
                "operator-verified-hidden-relevance-labels-at-n1",
                "real-candidate-artifact-packages-for-a-non-fixture-run",
                "operator-verified-index-source-to-representation-manifest-crosswalk",
            ],
        },
        "hidden_relevance_values_included": False,
        "services_started": False,
        "workflow_submitted": False,
        "llm_called": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    plan["plan_sha256"] = _sha256(_canonical(plan))
    return {
        PLAN_NAME: _json_bytes(plan),
        TRIALS_NAME: trial_bytes,
        OPERATIONS_NAME: operation_bytes,
        ARTIFACT_CATALOG_NAME: catalog_bytes,
        TASK_NAME: task_bytes,
    }


def freeze_full_flow_w4_candidate_routes(
    runtime_overlay_dir: str | Path,
    n3_package_dir: str | Path,
    n4_package_dir: str | Path,
    index_package_dir: str | Path,
    exact_range_catalog_dir: str | Path,
    *,
    physical_plan_id: str,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Freeze all sixteen public W4 candidate-wide route blueprints."""

    sources = tuple(
        Path(value).resolve()
        for value in (
            runtime_overlay_dir,
            n3_package_dir,
            n4_package_dir,
            index_package_dir,
            exact_range_catalog_dir,
        )
    )
    target = Path(output_dir).resolve()
    _require_disjoint_output(target, sources)
    documents = _documents(*sources, physical_plan_id)
    _publish(target, documents)
    verified = verify_full_flow_w4_candidate_routes(target)
    return {**verified, "output_dir": str(target)}


def _validate_artifact_catalog(
    catalog: Mapping[str, Any],
    task: Mapping[str, Any],
) -> list[str]:
    _require(
        set(catalog)
        == {
            "schema_version",
            "status",
            "catalog_id",
            "runtime_plan_sha256",
            "retrieval_task_binding_sha256",
            "candidate_set_sha256",
            "index_binding",
            "objects",
            "source_package_sha256",
            "hidden_relevance_values_included",
            "artifact_bytes_copied",
            "endpoints_included",
            "credentials_recorded",
            "eligible_for_scientific_claims",
            "catalog_sha256",
        },
        "candidate artifact catalog fields changed",
    )
    _require(
        catalog.get("schema_version")
        == W4_CANDIDATE_ARTIFACT_CATALOG_SCHEMA_VERSION
        and catalog.get("status") == "FROZEN_W4_CANDIDATE_ARTIFACT_CATALOG",
        "candidate artifact catalog schema or status changed",
    )
    catalog_id = _identifier(catalog.get("catalog_id"), "artifact catalog_id")
    runtime_plan_sha = _digest(
        catalog.get("runtime_plan_sha256"), "runtime plan SHA-256"
    )
    _require(
        catalog.get("retrieval_task_binding_sha256")
        == task["task_binding_sha256"]
        and catalog.get("candidate_set_sha256") == task["candidate_set_sha256"],
        "candidate artifact catalog binds a different public task",
    )
    _require(
        catalog_id
        == "w4-candidate-artifacts-"
        + _sha256(_canonical({
            "runtime_plan_sha256": runtime_plan_sha,
            "candidate_set_sha256": task["candidate_set_sha256"],
        }))[:32],
        "candidate artifact catalog identity changed",
    )
    index = catalog.get("index_binding")
    _require(
        isinstance(index, Mapping)
        and set(index)
        == {
            "logical_author_node_id",
            "serving_node_ids",
            "index_id",
            "index_sha256",
            "source_manifest_sha256",
            "candidate_object_ids",
        }
        and index.get("logical_author_node_id") == "N2"
        and index.get("serving_node_ids") == ["N2", "N7", "N8"],
        "candidate index binding changed",
    )
    _identifier(index.get("index_id"), "index_id")
    _digest(index.get("index_sha256"), "index SHA-256")
    _digest(
        index.get("source_manifest_sha256"),
        "index source manifest SHA-256",
    )
    task_ids = [row["object_id"] for row in task["candidate_objects"]]
    _require(
        index.get("candidate_object_ids") == task_ids,
        "candidate index coverage differs from public W4 task",
    )
    sources = catalog.get("source_package_sha256")
    _require(
        isinstance(sources, Mapping)
        and set(sources)
        == {"n3_package", "n4_package", "exact_range_catalog", "n2_index"},
        "candidate source package bindings changed",
    )
    for name, digest in sources.items():
        _digest(digest, f"{name} SHA-256")
    _require(
        sources["n2_index"] == index["index_sha256"],
        "candidate catalog index package binding changed",
    )
    objects = catalog.get("objects")
    _require(isinstance(objects, list), "candidate artifact objects are invalid")
    object_ids: list[str] = []
    public_digests = {
        row["object_id"]: row for row in task["candidate_objects"]
    }
    for row in objects:
        _require(
            isinstance(row, Mapping)
            and set(row)
            == {
                "object_id",
                "representations",
                "derived_provenance",
                "exact_raw_range",
            },
            "candidate artifact object fields changed",
        )
        object_id = _identifier(row.get("object_id"), "candidate object_id")
        object_ids.append(object_id)
        representations = row.get("representations")
        _require(
            isinstance(representations, Mapping)
            and set(representations)
            == {"raw_video", "multimodal_digest", "sampled_frame_bundle"},
            f"candidate representation set changed for {object_id}",
        )
        normalized: dict[str, Mapping[str, Any]] = {}
        for representation_id, identity in representations.items():
            _require(
                isinstance(identity, Mapping)
                and set(identity)
                == {
                    "representation_id",
                    "artifact_sha256",
                    "artifact_size_bytes",
                    "object_catalog_version",
                    "data_agent_plan_ids",
                }
                and identity.get("representation_id") == representation_id,
                f"candidate representation identity changed for {object_id}",
            )
            _digest(identity.get("artifact_sha256"), "artifact SHA-256")
            _positive(identity.get("artifact_size_bytes"), "artifact size")
            _identifier(
                identity.get("object_catalog_version"), "catalog version"
            )
            plan_ids = identity.get("data_agent_plan_ids")
            _require(
                isinstance(plan_ids, list)
                and plan_ids == sorted(set(plan_ids))
                and all(
                    isinstance(value, str)
                    and _IDENTIFIER.fullmatch(value) is not None
                    for value in plan_ids
                ),
                f"candidate Data Agent plan IDs changed for {object_id}",
            )
            expected_plan_ids = (
                {"D0", "D1", "D4", "D5"}
                if representation_id == "raw_video"
                else {"D2", "D3", "D6", "D7"}
            )
            _require(
                expected_plan_ids <= set(plan_ids),
                f"candidate Data Agent plan coverage changed for {object_id}",
            )
            normalized[representation_id] = identity
        provenance = row.get("derived_provenance")
        _require(
            isinstance(provenance, Mapping)
            and set(provenance)
            == {"multimodal_digest", "sampled_frame_bundle"},
            f"candidate derived provenance set changed for {object_id}",
        )
        for representation_id, value in provenance.items():
            _require(
                isinstance(value, Mapping)
                and set(value)
                == {
                    "schema_version",
                    "producer_node_id",
                    "publication_source_id",
                    "source_representation_id",
                    "source_content_sha256",
                    "derivation_id",
                    "derivation_sha256",
                }
                and value.get("schema_version")
                == "pathfinder.simulator-derived-artifact-provenance/v1alpha1"
                and value.get("producer_node_id") == "N5"
                and value.get("source_representation_id") == "raw_video"
                and value.get("source_content_sha256")
                == normalized["raw_video"]["artifact_sha256"],
                f"candidate {representation_id} provenance changed for {object_id}",
            )
            _identifier(
                value.get("publication_source_id"),
                "provenance publication source",
            )
            _identifier(value.get("derivation_id"), "provenance derivation ID")
            _digest(value.get("derivation_sha256"), "provenance derivation SHA")
        public = public_digests.get(object_id)
        digest_identity = normalized["multimodal_digest"]
        _require(
            public is not None
            and public["artifact_sha256"]
            == digest_identity["artifact_sha256"]
            and public["artifact_size_bytes"]
            == digest_identity["artifact_size_bytes"],
            f"candidate digest is not public-task bound for {object_id}",
        )
        exact = row.get("exact_raw_range")
        raw = normalized["raw_video"]
        _require(
            isinstance(exact, Mapping)
            and set(exact)
            == {
                "object_id",
                "representation_id",
                "object_catalog_version",
                "full_artifact_size_bytes",
                "full_artifact_sha256",
                "range_start",
                "range_end",
                "range_size_bytes",
                "range_sha256",
                "selection_semantics",
            }
            and exact.get("object_id") == object_id
            and exact.get("representation_id") == "raw_video"
            and exact.get("object_catalog_version")
            == raw["object_catalog_version"]
            and exact.get("full_artifact_size_bytes")
            == raw["artifact_size_bytes"]
            and exact.get("full_artifact_sha256") == raw["artifact_sha256"],
            f"candidate exact range identity changed for {object_id}",
        )
        start = exact.get("range_start")
        end = exact.get("range_end")
        _require(
            type(start) is int
            and type(end) is int
            and start == 0
            and end == raw["artifact_size_bytes"] - 1
            and exact.get("range_size_bytes") == raw["artifact_size_bytes"]
            and exact.get("range_sha256") == raw["artifact_sha256"]
            and exact.get("selection_semantics")
            == "exact-full-object-fallback",
            f"candidate exact range bounds changed for {object_id}",
        )
        _digest(exact.get("range_sha256"), "range SHA-256")
        _require(
            isinstance(exact.get("selection_semantics"), str)
            and bool(exact["selection_semantics"]),
            "exact range selection semantics are missing",
        )
    _require(
        object_ids == task_ids == sorted(set(object_ids)),
        "candidate artifact object order or coverage changed",
    )
    _require(
        catalog.get("hidden_relevance_values_included") is False
        and catalog.get("artifact_bytes_copied") is False
        and catalog.get("endpoints_included") is False
        and catalog.get("credentials_recorded") is False
        and catalog.get("eligible_for_scientific_claims") is False,
        "candidate artifact catalog safety boundary changed",
    )
    return object_ids


def verify_full_flow_w4_candidate_routes(
    output_dir: str | Path,
) -> dict[str, Any]:
    """Verify the self-contained public route package and claim boundary."""

    root = Path(output_dir).resolve()
    _verify_files(root)
    plan = _strict_json(root / PLAN_NAME, "candidate route plan")
    catalog = _strict_json(root / ARTIFACT_CATALOG_NAME, "artifact catalog")
    task = validate_full_flow_w4_public_runtime_task(
        _strict_json(root / TASK_NAME, "candidate route task")
    )
    try:
        trials = [
            json.loads(line)
            for line in (root / TRIALS_NAME).read_text(encoding="utf-8").splitlines()
        ]
        operations = [
            json.loads(line)
            for line in (root / OPERATIONS_NAME).read_text(
                encoding="utf-8"
            ).splitlines()
        ]
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowW4CandidateRouteError(
            "cannot read candidate route rows"
        ) from exc
    _require(
        (root / TRIALS_NAME).read_bytes() == _jsonl_bytes(trials)
        and (root / OPERATIONS_NAME).read_bytes() == _jsonl_bytes(operations)
        and (root / PLAN_NAME).read_bytes() == _json_bytes(plan)
        and (root / ARTIFACT_CATALOG_NAME).read_bytes() == _json_bytes(catalog)
        and (root / TASK_NAME).read_bytes() == _json_bytes(task),
        "candidate route package is not canonically serialized",
    )
    supplied_plan_sha = _digest(plan.pop("plan_sha256", None), "plan SHA-256")
    _require(
        supplied_plan_sha == _sha256(_canonical(plan)), "plan digest mismatch"
    )
    plan["plan_sha256"] = supplied_plan_sha
    supplied_catalog_sha = _digest(
        catalog.pop("catalog_sha256", None), "catalog SHA-256"
    )
    _require(
        supplied_catalog_sha == _sha256(_canonical(catalog)),
        "artifact catalog digest mismatch",
    )
    catalog["catalog_sha256"] = supplied_catalog_sha
    _require(
        set(plan)
        == {
            "schema_version",
            "status",
            "physical_plan_id",
            "runtime_overlay_id",
            "runtime_plan_sha256",
            "retrieval_task_binding_sha256",
            "candidate_set_sha256",
            "candidate_object_count",
            "trial_count",
            "operation_count",
            "design_ids",
            "output_sha256",
            "claim_boundary",
            "hidden_relevance_values_included",
            "services_started",
            "workflow_submitted",
            "llm_called",
            "credentials_recorded",
            "eligible_for_scientific_claims",
            "plan_sha256",
        },
        "candidate route plan fields changed",
    )
    _require(
        plan.get("schema_version") == W4_CANDIDATE_ROUTE_PLAN_SCHEMA_VERSION
        and plan.get("status") == "FROZEN_W4_CANDIDATE_ROUTE_BLUEPRINTS"
        and plan.get("trial_count") == len(trials) == 16
        and plan.get("operation_count") == len(operations)
        and plan.get("candidate_object_count") == len(catalog.get("objects", []))
        and plan.get("output_sha256")
        == {
            ARTIFACT_CATALOG_NAME: _sha256(
                (root / ARTIFACT_CATALOG_NAME).read_bytes()
            ),
            TASK_NAME: _sha256((root / TASK_NAME).read_bytes()),
            TRIALS_NAME: _sha256((root / TRIALS_NAME).read_bytes()),
            OPERATIONS_NAME: _sha256((root / OPERATIONS_NAME).read_bytes()),
        },
        "candidate route plan bindings changed",
    )
    _require(
        plan.get("retrieval_task_binding_sha256")
        == task["task_binding_sha256"]
        and plan.get("candidate_set_sha256") == task["candidate_set_sha256"]
        and plan.get("runtime_plan_sha256") == catalog["runtime_plan_sha256"]
        and plan.get("design_ids") == sorted(_DESIGNS),
        "candidate route public task binding changed",
    )
    boundary = plan.get("claim_boundary")
    _require(
        isinstance(boundary, Mapping)
        and set(boundary)
        == {
            "candidate_wide_physical_route_blueprints_compiled",
            "candidate_artifact_bindings_complete",
            "exact_raw_ranges_bound",
            "complete_index_candidate_coverage_bound",
            "index_source_lineage_to_representation_manifest_verified",
            "route_execution_performed",
            "multi_candidate_route_coordinator_implemented",
            "route_bound_ranking_evidence_present",
            "physical_design_retrieval_comparison_ready",
            "current_64_trial_matrix_modified",
            "current_64_trial_matrix_native_w4_semantics",
            "remaining_local_implementation",
            "remaining_operator_inputs",
        }
        and boundary.get("candidate_wide_physical_route_blueprints_compiled")
        is True
        and boundary.get("candidate_artifact_bindings_complete") is True
        and boundary.get("exact_raw_ranges_bound") is True
        and boundary.get("complete_index_candidate_coverage_bound") is True
        and boundary.get(
            "index_source_lineage_to_representation_manifest_verified"
        )
        is False
        and boundary.get("route_execution_performed") is False
        and boundary.get("multi_candidate_route_coordinator_implemented") is False
        and boundary.get("route_bound_ranking_evidence_present") is False
        and boundary.get("physical_design_retrieval_comparison_ready") is False
        and boundary.get("current_64_trial_matrix_modified") is False,
        "candidate route claim boundary changed",
    )
    _require(
        boundary.get("current_64_trial_matrix_native_w4_semantics")
        == "multiple-choice-placeholder"
        and boundary.get("remaining_local_implementation")
        == [
            "multi-candidate-physical-route-coordinator",
            "route-bound-semantic-ranker",
            "candidate-cache-branch-evidence",
        ]
        and boundary.get("remaining_operator_inputs")
        == [
            "operator-verified-hidden-relevance-labels-at-n1",
            "real-candidate-artifact-packages-for-a-non-fixture-run",
            "operator-verified-index-source-to-representation-manifest-crosswalk",
        ],
        "candidate route remaining-work declaration changed",
    )
    candidate_ids = _validate_artifact_catalog(catalog, task)
    trial_by_key: dict[str, Mapping[str, Any]] = {}
    for trial in trials:
        _require(
            isinstance(trial, Mapping)
            and trial.get("schema_version")
            == W4_CANDIDATE_ROUTE_TRIAL_SCHEMA_VERSION
            and trial.get("physical_plan_id") == plan["physical_plan_id"]
            and trial.get("route_family")
            == _ROUTE_FAMILY_BY_DESIGN.get(trial.get("design_id"))
            and trial.get("executor_node_id")
            == _EXECUTOR_BY_DESIGN.get(trial.get("design_id"))
            and trial.get("repetition") in {0, 1}
            and type(trial.get("order_index")) is int
            and trial["order_index"] >= 0
            and trial.get("candidate_object_count") == len(candidate_ids)
            and trial.get("hidden_relevance_values_included") is False
            and trial.get("credentials_recorded") is False,
            "candidate route trial changed",
        )
        key = _identifier(trial.get("trial_key"), "trial_key")
        _require(key not in trial_by_key, "candidate route trial repeats")
        trial_by_key[key] = trial
    _require(
        {(row["design_id"], row["repetition"]) for row in trials}
        == {(design, repetition) for design in _DESIGNS for repetition in (0, 1)},
        "candidate route matrix coverage changed",
    )
    _require(
        [row["order_index"] for row in trials]
        == sorted({row["order_index"] for row in trials}),
        "candidate route trial order changed",
    )
    operations_by_trial: dict[str, list[Mapping[str, Any]]] = {
        key: [] for key in trial_by_key
    }
    seen_operations: set[str] = set()
    for operation in operations:
        _require(
            isinstance(operation, Mapping)
            and operation.get("schema_version")
            == W4_CANDIDATE_ROUTE_OPERATION_SCHEMA_VERSION
            and operation.get("physical_plan_id") == plan["physical_plan_id"]
            and operation.get("credentials_recorded") is False,
            "candidate route operation changed",
        )
        key = _identifier(operation.get("operation_key"), "operation_key")
        trial_key = _identifier(operation.get("trial_key"), "operation trial_key")
        _require(
            key not in seen_operations and trial_key in operations_by_trial,
            "candidate route operation identity changed",
        )
        seen_operations.add(key)
        operations_by_trial[trial_key].append(operation)
    for trial_key, trial in trial_by_key.items():
        rows = operations_by_trial[trial_key]
        _require(
            [row.get("operation_index") for row in rows] == list(range(len(rows)))
            and [row.get("operation_key") for row in rows]
            == trial.get("operation_keys")
            and len(rows) == trial.get("operation_count")
            and rows[-1].get("operation_key") == trial.get("terminal_operation_key"),
            f"candidate route operation order changed for {trial_key}",
        )
        prior: set[str] = set()
        for row in rows:
            dependencies = row.get("dependency_operation_keys")
            _require(
                isinstance(dependencies, list)
                and len(dependencies) == len(set(dependencies))
                and set(dependencies) <= prior,
                f"candidate route dependency changed for {trial_key}",
            )
            prior.add(str(row["operation_key"]))
    expected_trials: list[dict[str, Any]] = []
    expected_operations: list[dict[str, Any]] = []
    for trial in trials:
        expected_trial, expected_rows = _compile_trial(
            str(plan["physical_plan_id"]), trial, task, catalog
        )
        expected_trials.append(expected_trial)
        expected_operations.extend(expected_rows)
    _require(
        _canonical(expected_trials) == _canonical(trials)
        and _canonical(expected_operations) == _canonical(operations),
        "candidate route blueprint differs from deterministic recompilation",
    )
    _require(
        plan.get("hidden_relevance_values_included") is False
        and plan.get("services_started") is False
        and plan.get("workflow_submitted") is False
        and plan.get("llm_called") is False
        and plan.get("credentials_recorded") is False
        and plan.get("eligible_for_scientific_claims") is False,
        "candidate route plan overstates evidence",
    )
    return {
        "status": "VERIFIED_W4_CANDIDATE_ROUTE_BLUEPRINTS",
        "physical_plan_id": plan["physical_plan_id"],
        "trial_count": len(trials),
        "operation_count": len(operations),
        "candidate_object_count": len(candidate_ids),
        "candidate_wide_physical_route_blueprints_compiled": True,
        "multi_candidate_route_coordinator_implemented": False,
        "physical_design_retrieval_comparison_ready": False,
        "hidden_relevance_values_read": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


@dataclass(frozen=True)
class FrozenW4CandidateRouteInputs:
    plan: Mapping[str, Any]
    public_task: Mapping[str, Any]
    artifact_catalog: Mapping[str, Any]
    trials: tuple[Mapping[str, Any], ...]
    operations: tuple[Mapping[str, Any], ...]


def load_full_flow_w4_candidate_route_inputs(
    output_dir: str | Path,
) -> FrozenW4CandidateRouteInputs:
    """Load a verified public route blueprint without private relevance data."""

    root = Path(output_dir).resolve()
    verify_full_flow_w4_candidate_routes(root)
    plan = _strict_json(root / PLAN_NAME, "candidate route plan")
    task = validate_full_flow_w4_public_runtime_task(
        _strict_json(root / TASK_NAME, "candidate route task")
    )
    catalog = _strict_json(root / ARTIFACT_CATALOG_NAME, "artifact catalog")
    trials = tuple(
        json.loads(line)
        for line in (root / TRIALS_NAME).read_text(encoding="utf-8").splitlines()
    )
    operations = tuple(
        json.loads(line)
        for line in (root / OPERATIONS_NAME).read_text(encoding="utf-8").splitlines()
    )
    return FrozenW4CandidateRouteInputs(
        plan=plan,
        public_task=task,
        artifact_catalog=catalog,
        trials=trials,
        operations=operations,
    )


__all__ = [
    "ARTIFACT_CATALOG_NAME",
    "CHECKSUMS_NAME",
    "FullFlowW4CandidateRouteError",
    "FrozenW4CandidateRouteInputs",
    "OPERATIONS_NAME",
    "PLAN_NAME",
    "TASK_NAME",
    "TRIALS_NAME",
    "W4_CANDIDATE_ARTIFACT_CATALOG_SCHEMA_VERSION",
    "W4_CANDIDATE_ROUTE_OPERATION_SCHEMA_VERSION",
    "W4_CANDIDATE_ROUTE_PLAN_SCHEMA_VERSION",
    "W4_CANDIDATE_ROUTE_TRIAL_SCHEMA_VERSION",
    "freeze_full_flow_w4_candidate_routes",
    "load_full_flow_w4_candidate_route_inputs",
    "verify_full_flow_w4_candidate_routes",
]
