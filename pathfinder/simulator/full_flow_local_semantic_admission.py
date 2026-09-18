"""Promote the blocked semantic matrix for local conformance execution.

The historical v1 admission is intentionally immutable and remains blocked.
This module verifies that package against every source from which it was
derived, verifies the subsequently collected artifact/range/provisioning
evidence, and emits a *new* admission package.  Promotion is deliberately
limited to the explicitly named ``legacy-mcq-local-conformance`` semantics.

The promoted trials are executable inputs for the generic route coordinator
and FlowMesh semantic-trial executor.  They are not performance, monetary
cost, W4 retrieval-quality, scientific, or UpCloud-readiness evidence.  Ten
representative trials remain a required runtime gate before the full matrix is
run; freezing this package does not claim that any smoke was executed.
"""

from __future__ import annotations

import hashlib
import importlib
import inspect
import json
import os
import re
import shutil
import tempfile
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from .full_flow_artifact_preflight import (
    CHECKSUMS_NAME as PREFLIGHT_CHECKSUMS_NAME,
    MANIFEST_NAME as PREFLIGHT_MANIFEST_NAME,
    verify_full_flow_semantic_artifact_preflight,
)
from .full_flow_exact_range_catalog import (
    CATALOG_NAME as RANGE_CATALOG_NAME,
    CHECKSUMS_NAME as RANGE_CHECKSUMS_NAME,
    verify_full_flow_exact_range_catalog,
)
from .full_flow_provisioning_catalog import (
    CATALOG_NAME as PROVISIONING_CATALOG_NAME,
    CHECKSUMS_NAME as PROVISIONING_CHECKSUMS_NAME,
    verify_full_flow_provisioning_catalog,
)
from .full_flow_semantic_execution_admission import (
    ADMISSION_NAME as LEGACY_ADMISSION_NAME,
    CHECKSUMS_NAME as LEGACY_CHECKSUMS_NAME,
    GAPS_NAME as LEGACY_GAPS_NAME,
    SMOKES_NAME as LEGACY_SMOKES_NAME,
    STAGES_NAME as LEGACY_STAGES_NAME,
    TRIALS_NAME as LEGACY_TRIALS_NAME,
    BOUND_STAGE_SCHEMA_VERSION,
    BOUND_TRIAL_SCHEMA_VERSION,
    verify_full_flow_semantic_execution_admission,
)
from .full_flow_semantic_input_profiles import (
    build_semantic_input_profile,
    model_input_frontier_representation_ids,
    validate_semantic_input_profile,
)
from pathfinder.distributed.scoring import (
    MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
    MULTIPLE_CHOICE_EXACT_SCORING_RULE,
)


LOCAL_SEMANTICS_MODE = "legacy-mcq-local-conformance"
LOCAL_ADMISSION_SCHEMA_VERSION = (
    "pathfinder.full-flow-local-semantic-execution-admission/v1alpha1"
)
ADAPTER_INVENTORY_SCHEMA_VERSION = (
    "pathfinder.full-flow-local-runtime-adapter-inventory/v1alpha1"
)
SMOKE_GATE_SCHEMA_VERSION = (
    "pathfinder.full-flow-local-semantic-smoke-gate/v1alpha1"
)

ADMISSION_NAME = "local-semantic-execution-admission.json"
TRIALS_NAME = "semantic-execution-trials.jsonl"
STAGES_NAME = "semantic-execution-stages.jsonl"
SMOKES_NAME = "semantic-execution-smokes.jsonl"
INVENTORY_NAME = "local-semantic-runtime-adapter-inventory.json"
CHECKSUMS_NAME = "SHA256SUMS"

_CONTENT_FILES = {
    ADMISSION_NAME,
    TRIALS_NAME,
    STAGES_NAME,
    SMOKES_NAME,
    INVENTORY_NAME,
}
_ALL_FILES = _CONTENT_FILES | {CHECKSUMS_NAME}
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:+-]{0,255}\Z")
_SMOKE_CASES = {
    "n7-raw",
    "n7-indexed-raw",
    "n7-remote-derived",
    "n7-cache-miss",
    "n7-cache-hit",
    "n8-raw",
    "n8-indexed-raw",
    "n8-remote-derived",
    "n8-cache-miss",
    "n8-cache-hit",
}
_LEGACY_MCQ_SCORING_RULES = {
    MULTIPLE_CHOICE_EXACT_SCORING_RULE,
    MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
}

# Each legacy gap is closed only for this local conformance mode.  Entries
# name concrete code or separately verified evidence.  Some gaps share one
# generic coordinator implementation; that is intentional rather than an
# assertion that separate route programs exist.
_IMPLEMENTATIONS: dict[str, dict[str, Any]] = {
    "semantic-matrix-flowmesh-renderer-v1": {
        "module": "pathfinder.integrations.flowmesh.semantic_matrix_trial",
        "symbol": "FlowMeshSemanticTrialExecutor",
        "evidence_kind": "source-implementation",
    },
    "semantic-artifact-availability-preflight-v1": {
        "module": "pathfinder.simulator.full_flow_artifact_preflight",
        "symbol": "verify_full_flow_semantic_artifact_preflight",
        "evidence_kind": "source-implementation-and-frozen-preflight",
    },
    "semantic-matrix-durable-runner-v1": {
        "module": "pathfinder.simulator.full_flow_matrix_runner",
        "symbol": "run_full_flow_semantic_matrix",
        "evidence_kind": "source-implementation",
    },
    "n1-authenticated-score-handoff-v2": {
        "module": (
            "pathfinder.simulator.full_flow_n1_remote_verification"
        ),
        "symbol": "N1RemoteScoreEvidenceVerifier",
        "evidence_kind": (
            "source-implementation-and-n1-remote-attestation"
        ),
    },
    "raw-route-coordinator-v1": {
        "module": "pathfinder.simulator.full_flow_semantic_route_runtime",
        "symbol": "GenericSemanticRouteCoordinator",
        "evidence_kind": "source-implementation",
    },
    "indexed-raw-route-coordinator-v1": {
        "module": "pathfinder.simulator.full_flow_semantic_route_runtime",
        "symbol": "GenericSemanticRouteCoordinator",
        "evidence_kind": "source-implementation-and-exact-range-catalog",
    },
    "remote-derived-route-coordinator-v1": {
        "module": "pathfinder.simulator.full_flow_semantic_route_runtime",
        "symbol": "GenericSemanticRouteCoordinator",
        "evidence_kind": "source-implementation",
    },
    "conditional-cache-derived-route-coordinator-v1": {
        "module": "pathfinder.simulator.full_flow_semantic_route_runtime",
        "symbol": "GenericSemanticRouteCoordinator",
        "evidence_kind": "source-implementation",
    },
    "cache-state-lifecycle-attestation-v1": {
        "module": "pathfinder.simulator.full_flow_route_adapters",
        "symbol": "SQLiteCacheLineageStore",
        "evidence_kind": "source-implementation",
    },
    "raw-video-model-input-adapter-v1": {
        "module": "pathfinder.simulator.full_flow_n6_adapters",
        "symbol": "N6ModelInputAdapter",
        "evidence_kind": "source-implementation",
    },
    "digest-model-input-adapter-v1": {
        "module": "pathfinder.simulator.full_flow_n6_adapters",
        "symbol": "N6ModelInputAdapter",
        "evidence_kind": "source-implementation",
    },
    "multi-representation-fusion-adapter-v1": {
        "module": "pathfinder.simulator.full_flow_n6_adapters",
        "symbol": "N6ModelInputAdapter",
        "evidence_kind": "source-implementation",
    },
    "n8-full-flow-route-runtime-v1": {
        "module": "pathfinder.simulator.full_flow_semantic_route_runtime",
        "symbol": "GenericSemanticRouteCoordinator",
        "evidence_kind": "source-implementation",
    },
    "n5-materialize-publish-runtime-v1": {
        "module": "pathfinder.simulator.full_flow_route_adapters",
        "symbol": "FrozenProvisioningReferenceAdapter",
        "evidence_kind": "verified-preprovisioned-substitution-only",
    },
    "semantic-evidence-to-awm-oed-bridge-v1": {
        "module": "pathfinder.simulator.full_flow_matrix_runner",
        "symbol": "run_full_flow_semantic_matrix",
        "evidence_kind": "source-implementation-neutral-evidence",
    },
}


class FullFlowLocalSemanticAdmissionError(ValueError):
    """Raised when local semantic promotion cannot be reproduced exactly."""


@dataclass(frozen=True)
class FrozenLocalSemanticExecutionInputs:
    """Public, source-bound inputs safe to mount on N7/N8 at runtime."""

    admission: Mapping[str, Any]
    bound_trials: tuple[Mapping[str, Any], ...]
    bound_stages: tuple[Mapping[str, Any], ...]
    representative_smokes: tuple[Mapping[str, Any], ...]
    adapter_inventory: Mapping[str, Any]


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FullFlowLocalSemanticAdmissionError(message)


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise FullFlowLocalSemanticAdmissionError(
            "local semantic admission is not canonical JSON"
        ) from exc


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


def _jsonl_bytes(rows: Sequence[Mapping[str, Any]]) -> bytes:
    return b"".join(_canonical(row) + b"\n" for row in rows)


def _identifier(value: Any, label: str) -> str:
    _require(
        isinstance(value, str) and _IDENTIFIER.fullmatch(value) is not None,
        f"{label} is invalid",
    )
    return str(value)


def _digest(value: Any, label: str) -> str:
    _require(
        isinstance(value, str) and _SHA256.fullmatch(value) is not None,
        f"{label} is not lowercase SHA-256",
    )
    return str(value)


def _strict_json(path: Path, label: str) -> dict[str, Any]:
    _require(path.is_file() and not path.is_symlink(), f"{label} is missing")

    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            _require(key not in result, f"{label} repeats key {key}")
            result[key] = value
        return result

    try:
        value = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=unique,
            parse_constant=lambda token: (_ for _ in ()).throw(
                FullFlowLocalSemanticAdmissionError(
                    f"{label} contains invalid constant {token}"
                )
            ),
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowLocalSemanticAdmissionError(
            f"cannot read {label}"
        ) from exc
    _require(isinstance(value, dict), f"{label} must be an object")
    return value


def _strict_jsonl(path: Path, label: str) -> list[dict[str, Any]]:
    _require(path.is_file() and not path.is_symlink(), f"{label} is missing")
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise FullFlowLocalSemanticAdmissionError(
            f"cannot read {label}"
        ) from exc
    _require(bool(lines), f"{label} is empty")
    rows: list[dict[str, Any]] = []
    for index, line in enumerate(lines, start=1):
        _require(bool(line.strip()), f"{label} line {index} is blank")
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise FullFlowLocalSemanticAdmissionError(
                f"cannot read {label} line {index}"
            ) from exc
        _require(isinstance(value, dict), f"{label} line {index} is invalid")
        rows.append(value)
    return rows


def _assert_public_package(value: Any, path: str = "package") -> None:
    """Reject private answer material while allowing public commitments."""

    if isinstance(value, Mapping):
        for raw_key, child in value.items():
            key = str(raw_key).casefold()
            _require(
                key not in {
                    "correct_answer",
                    "correct_answer_id",
                    "hidden_label",
                    "hidden_labels",
                    "label_records",
                    "oracle_evidence_secret",
                    "oracle_token",
                },
                f"public runtime package contains private field: {path}.{raw_key}",
            )
            if key in {
                "credential_values_included",
                "credentials_recorded",
                "hidden_label_content_included",
            }:
                _require(
                    child is False,
                    f"unsafe public runtime flag is true: {path}.{raw_key}",
                )
            _assert_public_package(child, f"{path}.{raw_key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _assert_public_package(child, f"{path}[{index}]")


def _source_snapshot(root: Path) -> dict[str, str]:
    names = {
        LEGACY_ADMISSION_NAME,
        LEGACY_TRIALS_NAME,
        LEGACY_STAGES_NAME,
        LEGACY_GAPS_NAME,
        LEGACY_SMOKES_NAME,
        LEGACY_CHECKSUMS_NAME,
    }
    _require(root.is_dir(), "legacy admission directory is missing")
    actual = {path.name for path in root.iterdir()}
    _require(actual == names, "legacy admission file set changed")
    return {
        name: _sha256((root / name).read_bytes())
        for name in sorted(names)
    }


def _verify_legacy(
    *,
    legacy_root: Path,
    semantic_matrix_dir: str | Path,
    deployment_binding_dir: str | Path,
    logical_route_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    public_task_set_path: str | Path,
    artifact_binding_path: str | Path,
    n1_oracle_package_dir: str | Path,
) -> tuple[dict[str, Any], dict[str, str]]:
    before = _source_snapshot(legacy_root)
    try:
        report = verify_full_flow_semantic_execution_admission(
            legacy_root,
            semantic_matrix_dir,
            deployment_binding_dir,
            logical_route_dir,
            scenario_path,
            container_plan_dir,
            public_task_set_path,
            artifact_binding_path,
            n1_oracle_package_dir,
        )
    except Exception as exc:
        raise FullFlowLocalSemanticAdmissionError(
            "legacy admission failed source-bound verification"
        ) from exc
    _require(
        report.get("status") == "VERIFIED_BLOCKED"
        and report.get("flowmesh_submission_authorized") is False,
        "legacy admission is not the immutable blocked v1 package",
    )
    _require(
        before == _source_snapshot(legacy_root),
        "legacy admission changed during verification",
    )
    return report, before


def _implementation_inventory(gap_ids: set[str]) -> list[dict[str, Any]]:
    _require(
        gap_ids == set(_IMPLEMENTATIONS),
        "legacy runtime-gap set is not the reviewed local capability set",
    )
    source_digests: dict[str, str] = {}
    rows: list[dict[str, Any]] = []
    for adapter_id in sorted(gap_ids):
        expected = _IMPLEMENTATIONS[adapter_id]
        module_name = str(expected["module"])
        symbol_name = str(expected["symbol"])
        try:
            module = importlib.import_module(module_name)
            symbol = getattr(module, symbol_name)
            source_name = inspect.getsourcefile(symbol)
        except (ImportError, AttributeError, TypeError) as exc:
            raise FullFlowLocalSemanticAdmissionError(
                f"runtime adapter implementation is missing: {adapter_id}"
            ) from exc
        _require(source_name is not None, f"adapter has no source: {adapter_id}")
        source = Path(source_name).resolve()
        _require(
            source.is_file() and not source.is_symlink(),
            f"adapter source is invalid: {adapter_id}",
        )
        source_digest = source_digests.setdefault(
            module_name,
            _sha256(source.read_bytes()),
        )
        row = {
            "adapter_id": adapter_id,
            "implementation_module": module_name,
            "implementation_symbol": symbol_name,
            "implementation_source_sha256": source_digest,
            "evidence_kind": expected["evidence_kind"],
            "implemented_for_semantics_mode": LOCAL_SEMANTICS_MODE,
            "implemented": True,
            "requires_upcloud": False,
        }
        row["inventory_entry_sha256"] = _sha256(_canonical(row))
        rows.append(row)
    return rows


def _evidence_reports(
    *,
    legacy_root: Path,
    artifact_preflight_dir: Path,
    exact_range_catalog_dir: Path,
    n3_package_dir: Path,
    provisioning_catalog_dir: Path,
    artifact_binding_path: Path,
    n4_package_dir: Path,
) -> dict[str, dict[str, Any]]:
    try:
        preflight = verify_full_flow_semantic_artifact_preflight(
            artifact_preflight_dir,
            semantic_execution_admission_dir=legacy_root,
            n3_package_dir=n3_package_dir,
            n4_package_dir=n4_package_dir,
        )
        ranges = verify_full_flow_exact_range_catalog(
            exact_range_catalog_dir,
            n3_package_dir,
        )
        provisioning = verify_full_flow_provisioning_catalog(
            provisioning_catalog_dir,
            artifact_binding_dir=artifact_binding_path.parent,
            n4_package_dir=n4_package_dir,
        )
    except Exception as exc:
        raise FullFlowLocalSemanticAdmissionError(
            "local runtime evidence failed source-bound verification"
        ) from exc
    _require(
        preflight.get("status") == "VERIFIED"
        and preflight.get("all_content_identities_verified") is True,
        "artifact availability preflight is incomplete",
    )
    _require(
        ranges.get("status") == "VERIFIED"
        and ranges.get("partial_mp4_ranges_supported") is False
        and type(ranges.get("index_selectivity_claimed")) is bool
        and ranges.get("byte_reduction_claimed")
        is ranges.get("index_selectivity_claimed")
        and ranges.get("source_side_projection_executed")
        is ranges.get("index_selectivity_claimed"),
        "exact source-selection catalog is incomplete",
    )
    _require(
        provisioning.get("status") == "VERIFIED"
        and provisioning.get("all_artifacts_available") is True
        and provisioning.get("live_materialization_executed") is False
        and provisioning.get("live_materialization_cost_measured") is False,
        "preprovisioned N5-to-N4 evidence is incomplete",
    )
    return {
        "artifact_preflight": dict(preflight),
        "exact_range_catalog": dict(ranges),
        "preprovisioned_catalog": dict(provisioning),
    }


def _artifact_coverage(
    trials: Sequence[Mapping[str, Any]],
    range_document: Mapping[str, Any],
    provisioning_document: Mapping[str, Any],
) -> None:
    ranges = {
        (row.get("object_id"), row.get("representation_id")): row
        for row in range_document.get("entries", [])
        if isinstance(row, Mapping)
    }
    provisioned = {
        (
            row.get("artifact_identity", {}).get("object_id"),
            row.get("artifact_identity", {}).get("representation_id"),
        ): row
        for row in provisioning_document.get("entries", [])
        if isinstance(row, Mapping)
        and isinstance(row.get("artifact_identity"), Mapping)
    }
    for trial in trials:
        identities = trial.get("representation_identities")
        _require(isinstance(identities, list), "trial identities are missing")
        logical_by_representation: dict[str, str] = {}
        for binding in identities:
            _require(isinstance(binding, Mapping), "trial identity is invalid")
            identity = binding.get("representation_binding")
            _require(isinstance(identity, Mapping), "representation binding is missing")
            representation = str(identity.get("representation_id"))
            object_id = str(binding.get("artifact_object_id"))
            logical_by_representation[representation] = str(
                binding.get("logical_object_id")
            )
            if representation == "raw_video":
                row = ranges.get((object_id, representation))
                _require(row is not None, "raw trial lacks an exact N3 range")
                _require(
                    row.get("full_artifact_sha256")
                    == identity.get("artifact_sha256")
                    and row.get("full_artifact_size_bytes")
                    == identity.get("artifact_size_bytes"),
                    "exact N3 range changes a trial artifact identity",
                )
            else:
                row = provisioned.get((object_id, representation))
                _require(
                    row is not None and row.get("available") is True,
                    "derived trial lacks verified N5-to-N4 provenance",
                )
                frozen = row["artifact_identity"]
                _require(
                    frozen.get("artifact_sha256")
                    == identity.get("artifact_sha256")
                    and frozen.get("artifact_size_bytes")
                    == identity.get("artifact_size_bytes"),
                    "N5-to-N4 provenance changes a trial artifact identity",
                )
        for chain_id in trial.get("required_provisioning_chain_ids", []):
            _require(
                isinstance(chain_id, str) and chain_id.startswith("artifact|"),
                "trial provisioning chain is invalid",
            )
            _require(
                any(row.get("chain_id") == chain_id for row in provisioned.values()),
                f"trial provisioning chain is unavailable: {chain_id}",
            )
            _, logical, representation = chain_id.split("|", 2)
            _require(
                logical_by_representation.get(representation) == logical,
                "trial provisioning chain changes the logical object",
            )


def _documents(
    *,
    legacy_root: Path,
    semantic_matrix_dir: str | Path,
    deployment_binding_dir: str | Path,
    logical_route_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    public_task_set_path: str | Path,
    artifact_binding_path: str | Path,
    n1_oracle_package_dir: str | Path,
    artifact_preflight_dir: Path,
    exact_range_catalog_dir: Path,
    n3_package_dir: Path,
    provisioning_catalog_dir: Path,
    n4_package_dir: Path,
    promotion_id: str,
    semantics_mode: str,
) -> dict[str, bytes]:
    promotion_id = _identifier(promotion_id, "promotion_id")
    _require(
        semantics_mode == LOCAL_SEMANTICS_MODE,
        "only legacy-mcq-local-conformance semantics may be promoted",
    )
    artifact_path = Path(artifact_binding_path).resolve()
    _require(
        artifact_path.is_file()
        and not artifact_path.is_symlink()
        and artifact_path.parent.is_dir(),
        "artifact binding source is invalid",
    )
    legacy_report, legacy_snapshot = _verify_legacy(
        legacy_root=legacy_root,
        semantic_matrix_dir=semantic_matrix_dir,
        deployment_binding_dir=deployment_binding_dir,
        logical_route_dir=logical_route_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
        public_task_set_path=public_task_set_path,
        artifact_binding_path=artifact_path,
        n1_oracle_package_dir=n1_oracle_package_dir,
    )
    evidence = _evidence_reports(
        legacy_root=legacy_root,
        artifact_preflight_dir=artifact_preflight_dir,
        exact_range_catalog_dir=exact_range_catalog_dir,
        n3_package_dir=n3_package_dir,
        provisioning_catalog_dir=provisioning_catalog_dir,
        artifact_binding_path=artifact_path,
        n4_package_dir=n4_package_dir,
    )

    legacy_admission = _strict_json(
        legacy_root / LEGACY_ADMISSION_NAME,
        "legacy admission",
    )
    source_trials = _strict_jsonl(
        legacy_root / LEGACY_TRIALS_NAME,
        "legacy bound trials",
    )
    source_stages = _strict_jsonl(
        legacy_root / LEGACY_STAGES_NAME,
        "legacy bound stages",
    )
    source_smokes = _strict_jsonl(
        legacy_root / LEGACY_SMOKES_NAME,
        "legacy smoke selection",
    )
    gap_document = _strict_json(
        legacy_root / LEGACY_GAPS_NAME,
        "legacy runtime gaps",
    )
    gap_rows = gap_document.get("required_adapters")
    _require(isinstance(gap_rows, list), "legacy runtime gaps are missing")
    gap_ids = {
        str(row.get("adapter_id"))
        for row in gap_rows
        if isinstance(row, Mapping)
    }
    inventory_rows = _implementation_inventory(gap_ids)

    range_document = _strict_json(
        exact_range_catalog_dir / RANGE_CATALOG_NAME,
        "exact range catalog",
    )
    provisioning_document = _strict_json(
        provisioning_catalog_dir / PROVISIONING_CATALOG_NAME,
        "preprovisioned catalog",
    )
    _artifact_coverage(source_trials, range_document, provisioning_document)

    stages_by_trial: dict[str, list[dict[str, Any]]] = {}
    for stage in source_stages:
        trial_key = stage.get("trial_key")
        if isinstance(trial_key, str):
            stages_by_trial.setdefault(trial_key, []).append(stage)

    promoted_trials: list[dict[str, Any]] = []
    for source in source_trials:
        _require(
            source.get("flowmesh_submission_authorized") is False,
            "legacy trial was already authorized",
        )
        task = source.get("public_task_binding")
        _require(isinstance(task, Mapping), "public task binding is missing")
        _require(
            task.get("success_scoring_rule") in _LEGACY_MCQ_SCORING_RULES,
            "legacy-MCQ mode requires an option-ID scoring rule",
        )
        promoted = dict(source)
        trial_key = str(source["trial_key"])
        frontier = model_input_frontier_representation_ids(
            stages_by_trial.get(trial_key, [])
        )
        promoted["semantic_input_profile"] = build_semantic_input_profile(
            route_family=str(source["route_family"]),
            model_input_representation_ids=frontier,
        )
        promoted["required_runtime_adapter_ids"] = []
        promoted["flowmesh_submission_authorized"] = True
        _require(
            promoted.get("source_semantic_trial_sha256")
            == source.get("source_semantic_trial_sha256"),
            "source semantic trial identity changed during promotion",
        )
        promoted_trials.append(promoted)
    _require(len(promoted_trials) == 64, "promoted matrix is not 64 trials")
    _require(
        [row.get("order_index") for row in promoted_trials] == list(range(64)),
        "promoted trial order changed",
    )

    by_trial = {row["trial_key"]: row for row in promoted_trials}
    promoted_smokes: list[dict[str, Any]] = []
    for source in source_smokes:
        case_id = source.get("case_id")
        trial_key = source.get("trial_key")
        _require(case_id in _SMOKE_CASES, "legacy smoke case changed")
        _require(trial_key in by_trial, "legacy smoke trial is not promoted")
        promoted = dict(source)
        promoted["required_runtime_adapter_ids"] = []
        promoted["flowmesh_submission_authorized"] = True
        promoted["semantic_execution_performed"] = False
        promoted["runtime_gate_state"] = "REQUIRED_NOT_EXECUTED"
        promoted["semantic_input_profile"] = by_trial[str(trial_key)][
            "semantic_input_profile"
        ]
        promoted_smokes.append(promoted)
    _require(
        len(promoted_smokes) == len(_SMOKE_CASES)
        and {row["case_id"] for row in promoted_smokes} == _SMOKE_CASES,
        "representative smoke gate is incomplete",
    )

    inventory: dict[str, Any] = {
        "schema_version": ADAPTER_INVENTORY_SCHEMA_VERSION,
        "status": "SOURCE_IMPLEMENTATIONS_AND_EVIDENCE_VERIFIED",
        "semantics_mode": semantics_mode,
        "adapter_count": len(inventory_rows),
        "adapters": inventory_rows,
        "artifact_availability_preflight_verified": True,
        "exact_full_object_range_catalog_verified": True,
        "preprovisioned_n5_to_n4_catalog_verified": True,
        "semantic_input_profiles_frozen": True,
        "semantic_input_profile_source_sha256": _sha256(
            Path(inspect.getsourcefile(build_semantic_input_profile)).read_bytes()
        ),
        "live_n5_materialization_executed": False,
        "live_n5_materialization_cost_measured": False,
        "n1_score_verifier": {
            "mode": "remote-n1-authenticated-verification",
            "implementation_module": (
                "pathfinder.simulator.full_flow_n1_remote_verification"
            ),
            "implementation_symbol": "N1RemoteScoreEvidenceVerifier",
            "remote_n1_verifier_implemented": True,
            "hidden_oracle_isolation_suitable_for_multihost": True,
            "upcloud_capability_evidence_required": (
                "remote-N1-deployment-and-network-attestation"
            ),
        },
        "all_legacy_runtime_adapter_requirements_satisfied_for_mode": True,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    inventory["inventory_sha256"] = _sha256(_canonical(inventory))

    source_commitments = {
        "legacy_admission_id": legacy_report["admission_id"],
        "legacy_admission_sha256": legacy_report["admission_sha256"],
        "legacy_package_file_sha256": legacy_snapshot,
        "legacy_original_source_bindings": legacy_admission[
            "source_bindings"
        ],
        "artifact_preflight_id": evidence["artifact_preflight"][
            "preflight_id"
        ],
        "artifact_preflight_sha256": evidence["artifact_preflight"][
            "preflight_sha256"
        ],
        "artifact_preflight_manifest_file_sha256": _sha256(
            (artifact_preflight_dir / PREFLIGHT_MANIFEST_NAME).read_bytes()
        ),
        "artifact_preflight_checksums_sha256": _sha256(
            (artifact_preflight_dir / PREFLIGHT_CHECKSUMS_NAME).read_bytes()
        ),
        "exact_range_catalog_id": evidence["exact_range_catalog"][
            "catalog_id"
        ],
        "exact_range_catalog_sha256": evidence["exact_range_catalog"][
            "catalog_sha256"
        ],
        "exact_range_catalog_file_sha256": _sha256(
            (exact_range_catalog_dir / RANGE_CATALOG_NAME).read_bytes()
        ),
        "exact_range_checksums_sha256": _sha256(
            (exact_range_catalog_dir / RANGE_CHECKSUMS_NAME).read_bytes()
        ),
        "preprovisioned_catalog_id": evidence["preprovisioned_catalog"][
            "catalog_id"
        ],
        "preprovisioned_catalog_sha256": evidence[
            "preprovisioned_catalog"
        ]["catalog_sha256"],
        "preprovisioned_catalog_file_sha256": _sha256(
            (provisioning_catalog_dir / PROVISIONING_CATALOG_NAME).read_bytes()
        ),
        "preprovisioned_checksums_sha256": _sha256(
            (provisioning_catalog_dir / PROVISIONING_CHECKSUMS_NAME).read_bytes()
        ),
    }
    source_commitments["source_commitments_sha256"] = _sha256(
        _canonical(source_commitments)
    )
    original_bindings = legacy_admission["source_bindings"]
    public_oracle_binding = {
        "oracle_id": _identifier(
            original_bindings.get("oracle_id"),
            "public oracle_id",
        ),
        "public_task_set_sha256": _digest(
            original_bindings.get("oracle_public_task_set_sha256"),
            "public task set SHA-256",
        ),
        "hidden_label_content_included": False,
        "n1_private_package_required_by_n7_n8_runtime": False,
    }

    trial_bytes = _jsonl_bytes(promoted_trials)
    # Stages are copied byte-for-byte so every bound_stage_sha256 in the
    # promoted trials remains valid for the existing executor and coordinator.
    stage_bytes = (legacy_root / LEGACY_STAGES_NAME).read_bytes()
    _require(
        stage_bytes == _jsonl_bytes(source_stages),
        "legacy stage serialization is not canonical",
    )
    smoke_bytes = _jsonl_bytes(promoted_smokes)
    inventory_bytes = _json_bytes(inventory)
    admission: dict[str, Any] = {
        "schema_version": LOCAL_ADMISSION_SCHEMA_VERSION,
        "status": "FROZEN_LOCAL_SEMANTIC_CONFORMANCE_INPUTS",
        "promotion_id": promotion_id,
        "semantics_mode": semantics_mode,
        "scenario_id": legacy_admission["scenario_id"],
        "deployment_id": legacy_admission["deployment_id"],
        "worker_pin": legacy_admission["worker_pin"],
        "source_commitments": source_commitments,
        "public_oracle_binding": public_oracle_binding,
        "n7_n8_runtime_mount_contract": {
            "required_public_package_files": sorted(_CONTENT_FILES),
            "task_plane_directory_required": False,
            "n1_oracle_package_directory_required": False,
            "private_mount_classes_prohibited": [
                "task-plane-root-containing-n1-private",
                "n1-private-oracle-package",
                "hidden-label-source",
            ],
        },
        "matrix_dimensions": legacy_admission["matrix_dimensions"],
        "semantic_input_profile_trial_counts": dict(sorted(Counter(
            row["semantic_input_profile"]["profile_id"]
            for row in promoted_trials
        ).items())),
        "trial_template_flowmesh_submission_authorized": True,
        "representative_smoke_gate": {
            "schema_version": SMOKE_GATE_SCHEMA_VERSION,
            "status": "REQUIRED_NOT_EXECUTED",
            "required_case_ids": sorted(_SMOKE_CASES),
            "required_smoke_count": len(_SMOKE_CASES),
            "smoke_trials_authorized": True,
            "full_matrix_runtime_gate_satisfied": False,
            "full_matrix_submission_authorized": False,
            "smoke_receipt_required_for_later_matrix_execution": True,
        },
        "claim_boundary": {
            "local_single_host_semantic_interoperability_conformance": True,
            "w4_task_semantics": "multiple-choice-placeholder",
            "w4_retrieval_quality_evaluated": False,
            "performance_measured": False,
            "monetary_cost_measured": False,
            "scientific_claim_authorized": False,
            "upcloud_ready": False,
            "remote_n1_verifier_implemented": True,
            "live_materialization_measured": False,
            "indexed_source_byte_selectivity_claimed": False,
            "direct_video_input_claimed": False,
        },
        "output_sha256": {
            TRIALS_NAME: _sha256(trial_bytes),
            STAGES_NAME: _sha256(stage_bytes),
            SMOKES_NAME: _sha256(smoke_bytes),
            INVENTORY_NAME: _sha256(inventory_bytes),
        },
        "services_started": False,
        "workflow_submitted": False,
        "semantic_execution_performed": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    admission["admission_sha256"] = _sha256(_canonical(admission))
    _assert_public_package([
        admission,
        promoted_trials,
        source_stages,
        promoted_smokes,
        inventory,
    ])
    documents = {
        ADMISSION_NAME: _json_bytes(admission),
        TRIALS_NAME: trial_bytes,
        STAGES_NAME: stage_bytes,
        SMOKES_NAME: smoke_bytes,
        INVENTORY_NAME: inventory_bytes,
    }
    documents[CHECKSUMS_NAME] = b"".join(
        f"{_sha256(documents[name])}  {name}\n".encode("utf-8")
        for name in sorted(_CONTENT_FILES)
    )
    return documents


def _verify_files(root: Path) -> dict[str, Any]:
    _require(root.is_dir(), "local semantic admission directory is missing")
    entries = list(root.iterdir())
    _require(
        all(path.is_file() and not path.is_symlink() for path in entries),
        "local semantic admission contains a non-regular file",
    )
    _require(
        {path.name for path in entries} == _ALL_FILES,
        "local semantic admission file set changed",
    )
    expected = b"".join(
        f"{_sha256((root / name).read_bytes())}  {name}\n".encode("utf-8")
        for name in sorted(_CONTENT_FILES)
    )
    _require(
        (root / CHECKSUMS_NAME).read_bytes() == expected,
        "local semantic admission checksums failed",
    )
    admission = _strict_json(root / ADMISSION_NAME, "local admission")
    _require(
        admission.get("schema_version") == LOCAL_ADMISSION_SCHEMA_VERSION
        and admission.get("status")
        == "FROZEN_LOCAL_SEMANTIC_CONFORMANCE_INPUTS"
        and admission.get("semantics_mode") == LOCAL_SEMANTICS_MODE,
        "local semantic admission status or semantics changed",
    )
    supplied = _digest(
        admission.pop("admission_sha256", None),
        "admission_sha256",
    )
    _require(
        supplied == _sha256(_canonical(admission)),
        "local semantic admission digest failed",
    )
    admission["admission_sha256"] = supplied
    trials = _strict_jsonl(root / TRIALS_NAME, "promoted trials")
    stages = _strict_jsonl(root / STAGES_NAME, "copied bound stages")
    smokes = _strict_jsonl(root / SMOKES_NAME, "promoted smokes")
    inventory = _strict_json(root / INVENTORY_NAME, "adapter inventory")
    _require(
        len(trials) == 64
        and [row.get("order_index") for row in trials] == list(range(64))
        and all(
            row.get("required_runtime_adapter_ids") == []
            and row.get("flowmesh_submission_authorized") is True
            for row in trials
        ),
        "promoted trial authorization changed",
    )
    stage_by_key: dict[str, dict[str, Any]] = {}
    for stage in stages:
        key = stage.get("stage_key")
        _require(
            stage.get("schema_version") == BOUND_STAGE_SCHEMA_VERSION
            and isinstance(key, str)
            and bool(key)
            and key not in stage_by_key,
            "copied bound stage identity changed",
        )
        stage_by_key[key] = stage
    for trial in trials:
        _require(
            trial.get("schema_version") == BOUND_TRIAL_SCHEMA_VERSION,
            "promoted bound trial schema changed",
        )
        _digest(
            trial.get("source_semantic_trial_sha256"),
            "source_semantic_trial_sha256",
        )
        keys = trial.get("semantic_stage_keys")
        hashes = trial.get("bound_stage_sha256")
        _require(
            isinstance(keys, list)
            and isinstance(hashes, list)
            and len(keys) == len(hashes)
            and all(key in stage_by_key for key in keys),
            "promoted trial stage binding is incomplete",
        )
        _require(
            all(
                _sha256(_canonical(stage_by_key[key]))
                == _digest(expected, "bound stage SHA-256")
                for key, expected in zip(keys, hashes)
            ),
            "promoted trial bound-stage digest changed",
        )
        frontier = model_input_frontier_representation_ids(
            [stage_by_key[key] for key in keys]
        )
        validate_semantic_input_profile(
            trial.get("semantic_input_profile", {}),
            route_family=str(trial.get("route_family")),
            model_input_representation_ids=frontier,
        )
    profile_counts = dict(sorted(Counter(
        row["semantic_input_profile"]["profile_id"] for row in trials
    ).items()))
    _require(
        admission.get("semantic_input_profile_trial_counts") == profile_counts,
        "semantic input profile trial counts changed",
    )
    _require(
        len(smokes) == len(_SMOKE_CASES)
        and {row.get("case_id") for row in smokes} == _SMOKE_CASES
        and all(
            row.get("runtime_gate_state") == "REQUIRED_NOT_EXECUTED"
            and row.get("semantic_execution_performed") is False
            and row.get("semantic_input_profile")
            == next(
                trial["semantic_input_profile"]
                for trial in trials
                if trial["trial_key"] == row.get("trial_key")
            )
            for row in smokes
        ),
        "representative smoke runtime gate changed",
    )
    gate = admission.get("representative_smoke_gate", {})
    boundary = admission.get("claim_boundary", {})
    public_oracle = admission.get("public_oracle_binding", {})
    mount_contract = admission.get("n7_n8_runtime_mount_contract", {})
    _require(
        gate.get("status") == "REQUIRED_NOT_EXECUTED"
        and gate.get("full_matrix_runtime_gate_satisfied") is False
        and gate.get("full_matrix_submission_authorized") is False,
        "full matrix was authorized before representative smokes",
    )
    _require(
        boundary.get("w4_task_semantics") == "multiple-choice-placeholder"
        and boundary.get("w4_retrieval_quality_evaluated") is False
        and boundary.get("performance_measured") is False
        and boundary.get("monetary_cost_measured") is False
        and boundary.get("scientific_claim_authorized") is False
        and boundary.get("upcloud_ready") is False
        and boundary.get("remote_n1_verifier_implemented") is True,
        "local-only claim boundary was weakened",
    )
    _require(
        boundary.get("indexed_source_byte_selectivity_claimed") is False
        and boundary.get("direct_video_input_claimed") is False,
        "semantic input claim boundary was weakened",
    )
    _identifier(public_oracle.get("oracle_id"), "public oracle_id")
    _digest(
        public_oracle.get("public_task_set_sha256"),
        "public task set SHA-256",
    )
    _require(
        public_oracle.get("hidden_label_content_included") is False
        and public_oracle.get("n1_private_package_required_by_n7_n8_runtime")
        is False,
        "public runtime package crossed the N1 private boundary",
    )
    _require(
        mount_contract.get("required_public_package_files")
        == sorted(_CONTENT_FILES)
        and mount_contract.get("task_plane_directory_required") is False
        and mount_contract.get("n1_oracle_package_directory_required") is False
        and mount_contract.get("private_mount_classes_prohibited")
        == [
            "task-plane-root-containing-n1-private",
            "n1-private-oracle-package",
            "hidden-label-source",
        ],
        "N7/N8 public-only mount contract changed",
    )
    _require(
        inventory.get("schema_version") == ADAPTER_INVENTORY_SCHEMA_VERSION
        and inventory.get("semantics_mode") == LOCAL_SEMANTICS_MODE
        and inventory.get("all_legacy_runtime_adapter_requirements_satisfied_for_mode")
        is True
        and inventory.get("semantic_input_profiles_frozen") is True
        and inventory.get("semantic_input_profile_source_sha256")
        == _sha256(
            Path(inspect.getsourcefile(build_semantic_input_profile)).read_bytes()
        )
        and inventory.get("n1_score_verifier", {}).get(
            "remote_n1_verifier_implemented"
        )
        is True,
        "adapter inventory scope changed",
    )
    supplied_inventory = _digest(
        inventory.pop("inventory_sha256", None),
        "inventory_sha256",
    )
    _require(
        supplied_inventory == _sha256(_canonical(inventory)),
        "adapter inventory digest failed",
    )
    inventory["inventory_sha256"] = supplied_inventory
    _require(
        admission.get("matrix_dimensions", {}).get("semantic_stage_count")
        == len(stages),
        "copied bound stage count changed",
    )
    _require(
        admission.get("output_sha256")
        == {
            TRIALS_NAME: _sha256((root / TRIALS_NAME).read_bytes()),
            STAGES_NAME: _sha256((root / STAGES_NAME).read_bytes()),
            SMOKES_NAME: _sha256((root / SMOKES_NAME).read_bytes()),
            INVENTORY_NAME: _sha256((root / INVENTORY_NAME).read_bytes()),
        },
        "local semantic admission output digests changed",
    )
    _assert_public_package([admission, trials, stages, smokes, inventory])
    return admission


def verify_full_flow_local_semantic_runtime_package(
    admission_dir: str | Path,
) -> dict[str, Any]:
    """Verify the self-contained public package without any N1 private input."""

    root = Path(admission_dir).resolve()
    admission = _verify_files(root)
    return {
        "status": "VERIFIED_PUBLIC_LOCAL_RUNTIME_INPUTS",
        "promotion_id": admission["promotion_id"],
        "admission_sha256": admission["admission_sha256"],
        "semantics_mode": admission["semantics_mode"],
        "oracle_id": admission["public_oracle_binding"]["oracle_id"],
        "public_task_set_sha256": admission["public_oracle_binding"][
            "public_task_set_sha256"
        ],
        "trial_count": 64,
        "representative_smoke_count": len(_SMOKE_CASES),
        "n1_private_package_read": False,
        "source_binding_checked_offline": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def load_full_flow_local_semantic_execution_inputs(
    admission_dir: str | Path,
) -> FrozenLocalSemanticExecutionInputs:
    """Load the verified public trials/stages without reading hidden labels."""

    root = Path(admission_dir).resolve()
    verify_full_flow_local_semantic_runtime_package(root)
    return FrozenLocalSemanticExecutionInputs(
        admission=_strict_json(root / ADMISSION_NAME, "local admission"),
        bound_trials=tuple(
            _strict_jsonl(root / TRIALS_NAME, "promoted trials")
        ),
        bound_stages=tuple(
            _strict_jsonl(root / STAGES_NAME, "copied bound stages")
        ),
        representative_smokes=tuple(
            _strict_jsonl(root / SMOKES_NAME, "promoted smokes")
        ),
        adapter_inventory=_strict_json(
            root / INVENTORY_NAME,
            "adapter inventory",
        ),
    )


def _publish(target: Path, documents: Mapping[str, bytes]) -> None:
    _require(not target.exists(), f"output directory already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    parent = Path(tempfile.mkdtemp(prefix=".local-semantic-", dir=target.parent))
    stage = parent / "output"
    try:
        stage.mkdir()
        for name, payload in documents.items():
            path = stage / name
            with path.open("wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
        _verify_files(stage)
        os.replace(stage, target)
    finally:
        shutil.rmtree(parent, ignore_errors=True)


def promote_full_flow_local_semantic_execution_admission(
    legacy_admission_dir: str | Path,
    semantic_matrix_dir: str | Path,
    deployment_binding_dir: str | Path,
    logical_route_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    public_task_set_path: str | Path,
    artifact_binding_path: str | Path,
    n1_oracle_package_dir: str | Path,
    artifact_preflight_dir: str | Path,
    exact_range_catalog_dir: str | Path,
    n3_package_dir: str | Path,
    provisioning_catalog_dir: str | Path,
    n4_package_dir: str | Path,
    *,
    semantics_mode: str,
    promotion_id: str,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Freeze local trial templates while leaving the legacy package intact."""

    legacy_root = Path(legacy_admission_dir).resolve()
    documents = _documents(
        legacy_root=legacy_root,
        semantic_matrix_dir=semantic_matrix_dir,
        deployment_binding_dir=deployment_binding_dir,
        logical_route_dir=logical_route_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
        public_task_set_path=public_task_set_path,
        artifact_binding_path=artifact_binding_path,
        n1_oracle_package_dir=n1_oracle_package_dir,
        artifact_preflight_dir=Path(artifact_preflight_dir).resolve(),
        exact_range_catalog_dir=Path(exact_range_catalog_dir).resolve(),
        n3_package_dir=Path(n3_package_dir).resolve(),
        provisioning_catalog_dir=Path(provisioning_catalog_dir).resolve(),
        n4_package_dir=Path(n4_package_dir).resolve(),
        promotion_id=promotion_id,
        semantics_mode=semantics_mode,
    )
    target = Path(output_dir).resolve()
    _publish(target, documents)
    admission = _verify_files(target)
    _require(
        _source_snapshot(legacy_root)
        == admission["source_commitments"]["legacy_package_file_sha256"],
        "legacy admission changed while publishing the promotion",
    )
    return {
        "status": admission["status"],
        "promotion_id": admission["promotion_id"],
        "admission_sha256": admission["admission_sha256"],
        "semantics_mode": admission["semantics_mode"],
        "trial_count": 64,
        "representative_smoke_count": len(_SMOKE_CASES),
        "trial_templates_authorized": True,
        "full_matrix_runtime_gate_satisfied": False,
        "w4_retrieval_quality_evaluated": False,
        "remote_n1_verifier_implemented": True,
        "output_dir": str(target),
        "services_started": False,
        "workflow_submitted": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def verify_full_flow_local_semantic_execution_admission(
    admission_dir: str | Path,
    legacy_admission_dir: str | Path,
    semantic_matrix_dir: str | Path,
    deployment_binding_dir: str | Path,
    logical_route_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    public_task_set_path: str | Path,
    artifact_binding_path: str | Path,
    n1_oracle_package_dir: str | Path,
    artifact_preflight_dir: str | Path,
    exact_range_catalog_dir: str | Path,
    n3_package_dir: str | Path,
    provisioning_catalog_dir: str | Path,
    n4_package_dir: str | Path,
) -> dict[str, Any]:
    """Reproduce a local admission from every original and later source."""

    root = Path(admission_dir).resolve()
    admission = _verify_files(root)
    expected = _documents(
        legacy_root=Path(legacy_admission_dir).resolve(),
        semantic_matrix_dir=semantic_matrix_dir,
        deployment_binding_dir=deployment_binding_dir,
        logical_route_dir=logical_route_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
        public_task_set_path=public_task_set_path,
        artifact_binding_path=artifact_binding_path,
        n1_oracle_package_dir=n1_oracle_package_dir,
        artifact_preflight_dir=Path(artifact_preflight_dir).resolve(),
        exact_range_catalog_dir=Path(exact_range_catalog_dir).resolve(),
        n3_package_dir=Path(n3_package_dir).resolve(),
        provisioning_catalog_dir=Path(provisioning_catalog_dir).resolve(),
        n4_package_dir=Path(n4_package_dir).resolve(),
        promotion_id=admission["promotion_id"],
        semantics_mode=admission["semantics_mode"],
    )
    for name in sorted(_ALL_FILES):
        _require(
            (root / name).read_bytes() == expected[name],
            f"local semantic admission does not match sources: {name}",
        )
    return {
        "status": "VERIFIED_LOCAL_SEMANTIC_CONFORMANCE_INPUTS",
        "promotion_id": admission["promotion_id"],
        "admission_sha256": admission["admission_sha256"],
        "semantics_mode": admission["semantics_mode"],
        "trial_count": 64,
        "representative_smoke_count": len(_SMOKE_CASES),
        "source_binding_checked": True,
        "legacy_admission_unchanged": True,
        "trial_templates_authorized": True,
        "full_matrix_runtime_gate_satisfied": False,
        "w4_retrieval_quality_evaluated": False,
        "performance_measured": False,
        "monetary_cost_measured": False,
        "remote_n1_verifier_implemented": True,
        "upcloud_ready": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


__all__ = [
    "ADAPTER_INVENTORY_SCHEMA_VERSION",
    "ADMISSION_NAME",
    "CHECKSUMS_NAME",
    "FullFlowLocalSemanticAdmissionError",
    "FrozenLocalSemanticExecutionInputs",
    "INVENTORY_NAME",
    "LOCAL_ADMISSION_SCHEMA_VERSION",
    "LOCAL_SEMANTICS_MODE",
    "SMOKES_NAME",
    "STAGES_NAME",
    "TRIALS_NAME",
    "load_full_flow_local_semantic_execution_inputs",
    "promote_full_flow_local_semantic_execution_admission",
    "verify_full_flow_local_semantic_runtime_package",
    "verify_full_flow_local_semantic_execution_admission",
]
