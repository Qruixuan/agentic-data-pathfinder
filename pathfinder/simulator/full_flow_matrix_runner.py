"""Durable execution boundary for the frozen 4 x 8 semantic matrix.

This module deliberately does not know how to submit a FlowMesh workflow or
call a container, Data Agent, or LLM.  Those effects belong to an injected
``SemanticTrialExecutor``.  The runner supplies a stable idempotency key and
persists an intent before invoking that executor.  It then records a
hash-chained result, a per-trial checkpoint, and completion in that order.

The resulting reduced evidence is deliberately neutral: it contains an
authenticated success bit, content/evidence digests, and typed component
measurements.  The complete public semantic-route evidence is retained in a
separate checksum-bound sidecar so a later AWM/OED adapter can revalidate the
source route without re-reading raw FlowMesh results.  Neither artifact
contains hidden labels, endpoints, credentials, or invented monetary cost.
The checksum-bound sidecar intentionally retains the model's public
prediction so an authorized verifier can replay N1 scoring against the
private package without trusting a restamped success bit.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import tempfile
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Protocol

from .full_flow_deployment import (
    DEPLOYMENT_BINDING_NAME,
    verify_full_flow_deployment_binding,
)
from .full_flow_semantic_matrix import (
    ARTIFACT_BINDINGS_NAME,
    PLAN_NAME as SEMANTIC_PLAN_NAME,
    TRIALS_NAME as SEMANTIC_TRIALS_NAME,
    verify_full_flow_semantic_matrix,
)
from .full_flow_semantic_route_evidence import (
    SEMANTIC_ROUTE_EVIDENCE_SCHEMA_VERSION,
    SemanticRouteEvidenceValidationError,
    verify_public_semantic_route_evidence,
)
from .hidden_oracle import N1_SCORE_REQUEST_SCHEMA_VERSION


RUN_CONTRACT_SCHEMA_VERSION = (
    "pathfinder.full-flow-semantic-matrix-run-contract/v1alpha1"
)
JOURNAL_ENTRY_SCHEMA_VERSION = (
    "pathfinder.full-flow-semantic-matrix-journal-entry/v1alpha1"
)
TRIAL_RESULT_SCHEMA_VERSION = (
    "pathfinder.full-flow-semantic-trial-result/v1alpha2"
)
NEUTRAL_EVIDENCE_SCHEMA_VERSION = (
    "pathfinder.full-flow-semantic-neutral-evidence/v1alpha1"
)
CHECKPOINT_SCHEMA_VERSION = (
    "pathfinder.full-flow-semantic-trial-checkpoint/v1alpha2"
)
RUN_REPORT_SCHEMA_VERSION = (
    "pathfinder.full-flow-semantic-matrix-run/v1alpha2"
)

PUBLIC_ROUTE_EVIDENCE_SCHEMA_VERSION = SEMANTIC_ROUTE_EVIDENCE_SCHEMA_VERSION

CONTRACT_NAME = "semantic-matrix-run-contract.json"
JOURNAL_NAME = "semantic-matrix-run-journal.jsonl"
EVIDENCE_NAME = "semantic-matrix-neutral-evidence.jsonl"
ROUTE_EVIDENCE_NAME = "semantic-matrix-route-evidence.jsonl"
REPORT_NAME = "semantic-matrix-run.json"
CHECKPOINT_DIR_NAME = "trial-checkpoints"
CHECKSUMS_NAME = "SHA256SUMS"

_FINAL_TOP_LEVEL_FILES = {
    CONTRACT_NAME,
    JOURNAL_NAME,
    EVIDENCE_NAME,
    ROUTE_EVIDENCE_NAME,
    REPORT_NAME,
    CHECKSUMS_NAME,
}
_SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:+-]{0,255}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_TRANSPORTS = {"direct-runtime", "flowmesh", "test-double"}
_MEASUREMENT_CLASSES = {"configured-simulator", "derived", "measured"}
_UNITS = {"bytes", "count", "milliseconds"}
_JOURNAL_STATES = {
    "TRIAL_INTENT",
    "TRIAL_RESULT",
    "TRIAL_COMPLETED",
    "RUN_FAILED",
    "FAILURE_ACKNOWLEDGED",
}
_FAILURE_CLASSES = {"infrastructure", "semantic"}
_FORBIDDEN_KEYS = {
    "access_token",
    "answer",
    "answer_text",
    "api_key",
    "authorization",
    "bearer_token",
    "correct_answer",
    "correct_answer_id",
    "endpoint",
    "endpoint_url",
    "hidden_label",
    "hidden_labels",
    "label",
    "relevance_value",
    "relevance_values",
    "password",
    "secret",
    "token",
    "url",
}


class FullFlowSemanticMatrixRunnerError(ValueError):
    """Raised when execution state or executor evidence is unsafe."""


class SemanticMatrixRunFailed(FullFlowSemanticMatrixRunnerError):
    """Raised after a trial failure has been durably journalled."""

    def __init__(
        self,
        *,
        trial_key: str,
        failure_class: str,
        failure_code: str,
        failed_entry_sha256: str,
    ) -> None:
        super().__init__(
            "semantic matrix run failed; "
            f"trial={trial_key}; class={failure_class}; code={failure_code}; "
            f"acknowledge_failed_entry_sha256={failed_entry_sha256}"
        )
        self.trial_key = trial_key
        self.failure_class = failure_class
        self.failure_code = failure_code
        self.failed_entry_sha256 = failed_entry_sha256


class SemanticTrialExecutionError(RuntimeError):
    """A sanitized executor failure suitable for the durable journal."""

    def __init__(self, failure_class: str, failure_code: str) -> None:
        _require(
            failure_class in _FAILURE_CLASSES,
            "executor failure_class is invalid",
        )
        _identifier(failure_code, "executor failure_code")
        super().__init__(failure_code)
        self.failure_class = failure_class
        self.failure_code = failure_code


class SemanticTrialExecutor(Protocol):
    """Effect boundary used by :func:`run_full_flow_semantic_matrix`.

    Implementations MUST make ``idempotency_key`` effective at their own
    external side-effect boundary.  The same key is reused after an explicitly
    acknowledged failure.  The runner never calls the executor for a trial
    whose durable checkpoint is complete.
    """

    def execute(
        self,
        *,
        trial: Mapping[str, Any],
        idempotency_key: str,
    ) -> Mapping[str, Any]:
        """Execute one frozen semantic trial and return the strict result."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FullFlowSemanticMatrixRunnerError(message)


def _identifier(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _SAFE_ID.fullmatch(value) is not None,
        f"{name} is invalid",
    )
    return str(value)


def _digest(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _SHA256.fullmatch(value) is not None,
        f"{name} must be a lowercase SHA-256 digest",
    )
    return str(value)


def _number(value: Any, name: str) -> int | float:
    _require(
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
        and float(value) >= 0,
        f"{name} must be a finite non-negative number",
    )
    return value


def _canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise FullFlowSemanticMatrixRunnerError(
            "value is not canonical JSON"
        ) from exc


def _json_bytes(value: Any) -> bytes:
    return _canonical_bytes(value) + b"\n"


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        _require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _read_json_bytes(payload: bytes, name: str) -> dict[str, Any]:
    try:
        value = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_pairs,
            parse_constant=lambda item: (_ for _ in ()).throw(
                FullFlowSemanticMatrixRunnerError(
                    f"{name} contains non-finite value {item}"
                )
            ),
        )
    except FullFlowSemanticMatrixRunnerError:
        raise
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowSemanticMatrixRunnerError(
            f"{name} is not valid JSON"
        ) from exc
    _require(isinstance(value, dict), f"{name} must be an object")
    return value


def _read_json(path: Path, name: str) -> dict[str, Any]:
    try:
        return _read_json_bytes(path.read_bytes(), name)
    except OSError as exc:
        raise FullFlowSemanticMatrixRunnerError(f"cannot read {name}") from exc


def _read_jsonl(path: Path, name: str, *, allow_empty: bool = False) -> list[dict]:
    try:
        payload = path.read_bytes()
    except OSError as exc:
        raise FullFlowSemanticMatrixRunnerError(f"cannot read {name}") from exc
    _require(
        not payload or payload.endswith(b"\n"),
        f"{name} has a torn final row",
    )
    lines = payload.splitlines()
    _require(bool(lines) or allow_empty, f"{name} cannot be empty")
    return [
        _read_json_bytes(line, f"{name} line {index}")
        for index, line in enumerate(lines, start=1)
    ]


def _assert_public_evidence(value: Any) -> None:
    """Reject answers, labels, endpoints, paths, and credential material."""

    def visit(item: Any, *, public_prediction: bool = False) -> None:
        if isinstance(item, Mapping):
            schema = item.get("schema_version")
            for key, child in item.items():
                lowered = str(key).casefold()
                if lowered == "credentials_recorded":
                    _require(child is False, "credentials_recorded must be false")
                else:
                    _require(
                        lowered not in _FORBIDDEN_KEYS,
                        f"runner evidence contains forbidden field: {key}",
                    )
                visit(
                    child,
                    public_prediction=(
                        schema == N1_SCORE_REQUEST_SCHEMA_VERSION
                        and lowered == "predicted_answer"
                    ),
                )
        elif isinstance(item, list):
            for child in item:
                visit(child)
        elif isinstance(item, str):
            if public_prediction:
                return
            lowered = item.casefold()
            _require(
                "://" not in lowered
                and not item.startswith(("/", "~", "\\\\"))
                and not re.match(r"^[A-Za-z]:[\\/]", item)
                and "bearer " not in lowered,
                "runner evidence contains endpoint, path, or credential material",
            )

    visit(value)


def _strict_fields(value: Mapping[str, Any], expected: set[str], name: str) -> None:
    _require(
        set(value) == expected,
        f"{name} fields changed: expected {sorted(expected)}, got "
        f"{sorted(value)}",
    )


def _source_file(path: str | Path, name: str) -> Path:
    resolved = Path(path).resolve()
    _require(resolved.is_file() and not resolved.is_symlink(), f"{name} is invalid")
    return resolved


def _verified_inputs(
    *,
    semantic_matrix_dir: str | Path,
    deployment_binding_dir: str | Path,
    logical_route_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    public_task_set_path: str | Path,
    artifact_binding_path: str | Path,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    semantic_root = Path(semantic_matrix_dir).resolve()
    deployment_root = Path(deployment_binding_dir).resolve()
    artifact_source = _source_file(
        artifact_binding_path,
        "artifact binding source",
    )
    semantic_report = verify_full_flow_semantic_matrix(
        semantic_root,
        logical_route_dir,
        scenario_path,
        container_plan_dir,
        public_task_set_path,
        artifact_source,
    )
    deployment_report = verify_full_flow_deployment_binding(
        deployment_root,
        logical_plan_dir=logical_route_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
    )
    _require(
        semantic_report.get("status") == "VERIFIED"
        and deployment_report.get("status") == "VERIFIED",
        "semantic matrix or deployment binding was not verified",
    )
    semantic_plan = _read_json(
        semantic_root / SEMANTIC_PLAN_NAME,
        "semantic matrix plan",
    )
    trials = _read_jsonl(
        semantic_root / SEMANTIC_TRIALS_NAME,
        "semantic matrix trials",
    )
    _require(len(trials) == 64, "semantic matrix must contain 64 trials")
    _require(
        [row.get("order_index") for row in trials] == list(range(64)),
        "semantic matrix trial order is not canonical",
    )
    _require(
        len({row.get("trial_key") for row in trials}) == 64,
        "semantic matrix trial keys are not unique",
    )
    artifact_copy = semantic_root / ARTIFACT_BINDINGS_NAME
    deployment_copy = deployment_root / DEPLOYMENT_BINDING_NAME
    source_binding = {
        "semantic_matrix_plan_sha256": semantic_report["plan_sha256"],
        "semantic_matrix_source_binding_sha256": semantic_report[
            "source_binding_sha256"
        ],
        "semantic_artifact_bindings_file_sha256": _sha256(
            artifact_copy.read_bytes()
        ),
        "artifact_binding_source_file_sha256": _sha256(
            artifact_source.read_bytes()
        ),
        "deployment_binding_sha256": deployment_report["binding_sha256"],
        "deployment_binding_file_sha256": _sha256(deployment_copy.read_bytes()),
    }
    _assert_public_evidence(source_binding)
    return semantic_plan, trials, source_binding


def _contract(
    run_id: str,
    semantic_plan: Mapping[str, Any],
    trials: Sequence[Mapping[str, Any]],
    source_binding: Mapping[str, Any],
) -> dict[str, Any]:
    trial_order = [
        {
            "order_index": row["order_index"],
            "trial_key": row["trial_key"],
            "source_logical_trial_sha256": row[
                "source_logical_trial_sha256"
            ],
        }
        for row in trials
    ]
    value = {
        "schema_version": RUN_CONTRACT_SCHEMA_VERSION,
        "status": "FROZEN_SEMANTIC_MATRIX_RUN_CONTRACT",
        "run_id": _identifier(run_id, "run_id"),
        "scenario_id": semantic_plan["scenario_id"],
        "matrix_cell_count": 32,
        "trial_count": 64,
        "trial_order_sha256": _sha256(_canonical_bytes(trial_order)),
        "source_binding": dict(source_binding),
        "executor_contract_schema_version": TRIAL_RESULT_SCHEMA_VERSION,
        "idempotency_key_derivation": (
            "sha256(run-id-nul-semantic-plan-sha256-nul-trial-key)"
        ),
        "workflow_submission_implemented_by_runner": False,
        "answer_or_hidden_label_content_included": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    value["contract_sha256"] = _sha256(_canonical_bytes(value))
    _assert_public_evidence(value)
    return value


def _idempotency_key(contract: Mapping[str, Any], trial_key: str) -> str:
    payload = (
        str(contract["run_id"]).encode("utf-8")
        + b"\0"
        + str(
            contract["source_binding"]["semantic_matrix_plan_sha256"]
        ).encode("ascii")
        + b"\0"
        + trial_key.encode("utf-8")
    )
    return _sha256(payload)


def _append_journal(path: Path, row: Mapping[str, Any]) -> dict[str, Any]:
    existing = _read_jsonl(path, "semantic matrix journal", allow_empty=True)
    previous = existing[-1]["entry_sha256"] if existing else None
    entry = {
        "schema_version": JOURNAL_ENTRY_SCHEMA_VERSION,
        "sequence": len(existing),
        "previous_entry_sha256": previous,
        **dict(row),
    }
    entry["entry_sha256"] = _sha256(_canonical_bytes(entry))
    _assert_public_evidence(entry)
    payload = _json_bytes(entry)
    with path.open("ab") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    return entry


def _audit_journal(
    rows: Sequence[Mapping[str, Any]],
    *,
    trials: Sequence[Mapping[str, Any]] | None = None,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    previous: str | None = None
    completed: list[str] = []
    state = None
    active_trial = None
    result_by_trial: dict[str, dict[str, Any]] = {}
    completion_checkpoint_by_trial: dict[str, str] = {}
    failures = Counter()
    acknowledgement_count = 0
    trial_by_key = (
        {str(row["trial_key"]): row for row in trials}
        if trials is not None
        else None
    )
    if trial_by_key is not None:
        _require(len(trial_by_key) == len(trials), "source trial keys repeat")
        _require(contract is not None, "strict journal audit requires contract")
    for sequence, raw in enumerate(rows):
        row = dict(raw)
        recorded = row.pop("entry_sha256", None)
        _digest(recorded, "journal entry_sha256")
        _require(
            recorded == _sha256(_canonical_bytes(row)),
            "journal entry digest mismatch",
        )
        row["entry_sha256"] = recorded
        _require(
            row.get("schema_version") == JOURNAL_ENTRY_SCHEMA_VERSION
            and row.get("sequence") == sequence
            and row.get("previous_entry_sha256") == previous
            and row.get("state") in _JOURNAL_STATES,
            "journal hash chain or state is invalid",
        )
        trial_key = row.get("trial_key")
        _require(isinstance(trial_key, str), "journal trial_key is invalid")
        current = row["state"]
        trial = None if trial_by_key is None else trial_by_key.get(trial_key)
        if trial_by_key is not None:
            _require(trial is not None, "journal contains an unknown trial")
            _require(
                row.get("order_index") == trial["order_index"],
                "journal trial order binding changed",
            )
        if current == "TRIAL_INTENT":
            _strict_fields(
                row,
                {
                    "schema_version",
                    "sequence",
                    "previous_entry_sha256",
                    "state",
                    "trial_key",
                    "order_index",
                    "idempotency_key",
                    "entry_sha256",
                },
                "journal intent",
            )
            _require(
                state in {None, "TRIAL_COMPLETED", "FAILURE_ACKNOWLEDGED"},
                "journal intent transition is invalid",
            )
            if state == "FAILURE_ACKNOWLEDGED":
                _require(trial_key == active_trial, "retry changed trial identity")
            if trial is not None:
                expected_trial = trials[len(completed)]
                _require(
                    trial_key == expected_trial["trial_key"],
                    "journal intent is outside the canonical trial order",
                )
                _require(
                    row.get("idempotency_key")
                    == _idempotency_key(contract, trial_key),
                    "journal idempotency key changed",
                )
            active_trial = trial_key
        elif current == "TRIAL_RESULT":
            _strict_fields(
                row,
                {
                    "schema_version",
                    "sequence",
                    "previous_entry_sha256",
                    "state",
                    "trial_key",
                    "order_index",
                    "result",
                    "entry_sha256",
                },
                "journal result",
            )
            _require(
                state == "TRIAL_INTENT" and trial_key == active_trial,
                "journal result transition is invalid",
            )
            result = row.get("result")
            _require(isinstance(result, dict), "journal result is missing")
            if trial is not None:
                result = _validated_result(
                    result,
                    trial=trial,
                    idempotency_key=_idempotency_key(contract, trial_key),
                )
            result_by_trial[trial_key] = result
        elif current == "TRIAL_COMPLETED":
            _strict_fields(
                row,
                {
                    "schema_version",
                    "sequence",
                    "previous_entry_sha256",
                    "state",
                    "trial_key",
                    "order_index",
                    "checkpoint_sha256",
                    "recovered_without_executor_call",
                    "entry_sha256",
                },
                "journal completion",
            )
            _require(
                state == "TRIAL_RESULT" and trial_key == active_trial,
                "journal completion transition is invalid",
            )
            _digest(row.get("checkpoint_sha256"), "journal checkpoint_sha256")
            _require(
                type(row.get("recovered_without_executor_call")) is bool,
                "journal recovery flag is invalid",
            )
            _require(trial_key not in completed, "trial completed twice")
            completion_checkpoint_by_trial[trial_key] = row["checkpoint_sha256"]
            completed.append(trial_key)
            active_trial = None
        elif current == "RUN_FAILED":
            _strict_fields(
                row,
                {
                    "schema_version",
                    "sequence",
                    "previous_entry_sha256",
                    "state",
                    "trial_key",
                    "order_index",
                    "failure_class",
                    "failure_code",
                    "entry_sha256",
                },
                "journal failure",
            )
            _require(
                state == "TRIAL_INTENT" and trial_key == active_trial,
                "journal failure transition is invalid",
            )
            failure_class = row.get("failure_class")
            _require(
                failure_class in _FAILURE_CLASSES,
                "journal failure class is invalid",
            )
            _identifier(row.get("failure_code"), "journal failure_code")
            failures[failure_class] += 1
        else:
            _strict_fields(
                row,
                {
                    "schema_version",
                    "sequence",
                    "previous_entry_sha256",
                    "state",
                    "trial_key",
                    "order_index",
                    "acknowledged_entry_sha256",
                    "entry_sha256",
                },
                "journal failure acknowledgement",
            )
            _require(
                state == "RUN_FAILED"
                and trial_key == active_trial
                and row.get("acknowledged_entry_sha256") == previous,
                "journal failure acknowledgement is invalid",
            )
            acknowledgement_count += 1
        state = current
        previous = recorded
    return {
        "terminal_state": state,
        "active_trial": active_trial,
        "completed": completed,
        "result_by_trial": result_by_trial,
        "completion_checkpoint_by_trial": completion_checkpoint_by_trial,
        "failure_counts": dict(failures),
        "failure_acknowledgement_count": acknowledgement_count,
        "terminal_entry_sha256": previous,
    }


_RESULT_FIELDS = {
    "schema_version",
    "status",
    "trial_key",
    "idempotency_key",
    "task_success",
    "semantic_answer_sha256",
    "n1_score_evidence_sha256",
    "n1_score_authenticity_verified",
    "route_evidence_sha256",
    "semantic_route_evidence",
    "artifact_binding_evidence_sha256",
    "measurements",
    "execution_transport",
    "flowmesh_workflow_evidence_sha256",
    "llm_called",
    "telemetry_complete",
    "credentials_recorded",
    "eligible_for_scientific_claims",
}
_MEASUREMENT_FIELDS = {
    "component_id",
    "metric_id",
    "value",
    "unit",
    "measurement_class",
}


def _validated_public_route_evidence(
    value: Any,
    *,
    trial_key: str,
    expected_sha256: str,
    required: bool,
) -> dict[str, Any] | None:
    """Validate the portable public evidence retained from the route service.

    Full semantic re-validation belongs to the source-aware FlowMesh adapter
    and AWM/OED bridge.  The runner independently enforces the content digest,
    trial identity, public-only boundary, and required presence for every
    FlowMesh result before making the record durable.
    """

    if value is None:
        _require(not required, "FlowMesh result lacks public route evidence")
        return None
    _require(
        required and isinstance(value, Mapping),
        "non-FlowMesh result cannot retain semantic route evidence",
    )
    try:
        evidence = verify_public_semantic_route_evidence(value)
    except SemanticRouteEvidenceValidationError as exc:
        raise FullFlowSemanticMatrixRunnerError(
            f"public route evidence failed strict validation: {exc}"
        ) from exc
    _require(
        evidence.get("trial_key") == trial_key,
        "public route evidence identity or status changed",
    )
    supplied = evidence.get("evidence_sha256")
    _digest(supplied, "semantic route evidence_sha256")
    _require(
        supplied == expected_sha256,
        "public route evidence digest mismatch",
    )
    _assert_public_evidence(evidence)
    return evidence


def _validated_result(
    value: Mapping[str, Any],
    *,
    trial: Mapping[str, Any],
    idempotency_key: str,
) -> dict[str, Any]:
    _require(isinstance(value, Mapping), "executor result must be an object")
    result = dict(value)
    _strict_fields(result, _RESULT_FIELDS, "executor result")
    _require(
        result.get("schema_version") == TRIAL_RESULT_SCHEMA_VERSION
        and result.get("status") == "COMPLETE"
        and result.get("trial_key") == trial["trial_key"]
        and result.get("idempotency_key") == idempotency_key,
        "executor result identity or status changed",
    )
    for name in (
        "semantic_answer_sha256",
        "n1_score_evidence_sha256",
        "route_evidence_sha256",
        "artifact_binding_evidence_sha256",
    ):
        _digest(result.get(name), name)
    _require(
        type(result.get("task_success")) is bool
        and result.get("n1_score_authenticity_verified") is True
        and type(result.get("llm_called")) is bool
        and type(result.get("telemetry_complete")) is bool
        and result.get("credentials_recorded") is False
        and result.get("eligible_for_scientific_claims") is False,
        "executor result safety or semantic flags are invalid",
    )
    transport = result.get("execution_transport")
    _require(transport in _TRANSPORTS, "execution_transport is invalid")
    result["semantic_route_evidence"] = _validated_public_route_evidence(
        result.get("semantic_route_evidence"),
        trial_key=str(trial["trial_key"]),
        expected_sha256=str(result["route_evidence_sha256"]),
        required=transport == "flowmesh",
    )
    workflow_digest = result.get("flowmesh_workflow_evidence_sha256")
    if transport == "flowmesh":
        _digest(workflow_digest, "flowmesh_workflow_evidence_sha256")
    else:
        _require(
            workflow_digest is None,
            "non-FlowMesh result cannot claim FlowMesh workflow evidence",
        )
    measurements = result.get("measurements")
    _require(isinstance(measurements, list), "measurements must be an array")
    identities: set[tuple[str, str]] = set()
    normalized: list[dict[str, Any]] = []
    for index, item in enumerate(measurements):
        _require(isinstance(item, Mapping), "measurement must be an object")
        row = dict(item)
        _strict_fields(row, _MEASUREMENT_FIELDS, "measurement")
        component = _identifier(row.get("component_id"), "component_id")
        metric = _identifier(row.get("metric_id"), "metric_id")
        identity = (component, metric)
        _require(identity not in identities, "measurement identity is duplicated")
        identities.add(identity)
        _number(row.get("value"), f"measurement[{index}].value")
        _require(row.get("unit") in _UNITS, "measurement unit is invalid")
        _require(
            row.get("measurement_class") in _MEASUREMENT_CLASSES,
            "measurement class is invalid",
        )
        normalized.append(row)
    _require(
        normalized
        == sorted(normalized, key=lambda row: (row["component_id"], row["metric_id"])),
        "measurements must be sorted by component_id and metric_id",
    )
    _assert_public_evidence(result)
    return result


def validate_semantic_trial_result(
    value: Mapping[str, Any],
    *,
    trial: Mapping[str, Any],
    idempotency_key: str,
) -> dict[str, Any]:
    """Validate one executor result without starting or resuming a run.

    Representative-smoke gates use the exact same public evidence contract as
    the 64-trial runner.  Exposing this narrow validator avoids a second,
    subtly different result schema at that gate.
    """

    return _validated_result(
        value,
        trial=trial,
        idempotency_key=idempotency_key,
    )


def _neutral_evidence(
    trial: Mapping[str, Any],
    result: Mapping[str, Any],
) -> dict[str, Any]:
    value = {
        "schema_version": NEUTRAL_EVIDENCE_SCHEMA_VERSION,
        "trial_key": trial["trial_key"],
        "order_index": trial["order_index"],
        "workload_id": trial["workload_id"],
        "workload_class": trial["workload_class"],
        "design_id": trial["design_id"],
        "repetition": trial["repetition"],
        "route_family": trial["route_family"],
        "source_logical_trial_sha256": trial["source_logical_trial_sha256"],
        "idempotency_key": result["idempotency_key"],
        "task_success": result["task_success"],
        "semantic_answer_sha256": result["semantic_answer_sha256"],
        "n1_score_evidence_sha256": result["n1_score_evidence_sha256"],
        "n1_score_authenticity_verified": True,
        "route_evidence_sha256": result["route_evidence_sha256"],
        "artifact_binding_evidence_sha256": result[
            "artifact_binding_evidence_sha256"
        ],
        "measurements": result["measurements"],
        "execution_transport": result["execution_transport"],
        "flowmesh_workflow_evidence_sha256": result[
            "flowmesh_workflow_evidence_sha256"
        ],
        "llm_called": result["llm_called"],
        "telemetry_complete": result["telemetry_complete"],
        "source_executor_result_sha256": _sha256(_canonical_bytes(result)),
        "monetary_cost_included": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    value["evidence_sha256"] = _sha256(_canonical_bytes(value))
    _assert_public_evidence(value)
    return value


def _checkpoint_path(root: Path, trial: Mapping[str, Any]) -> Path:
    suffix = _sha256(str(trial["trial_key"]).encode("utf-8"))[:16]
    return root / CHECKPOINT_DIR_NAME / f"{trial['order_index']:04d}-{suffix}.json"


def _checkpoint(
    trial: Mapping[str, Any],
    result: Mapping[str, Any],
) -> dict[str, Any]:
    evidence = _neutral_evidence(trial, result)
    value = {
        "schema_version": CHECKPOINT_SCHEMA_VERSION,
        "status": "TRIAL_COMPLETED",
        "trial_key": trial["trial_key"],
        "order_index": trial["order_index"],
        "source_logical_trial_sha256": trial["source_logical_trial_sha256"],
        "executor_result": dict(result),
        "neutral_evidence": evidence,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    value["checkpoint_sha256"] = _sha256(_canonical_bytes(value))
    _assert_public_evidence(value)
    return value


def _write_atomic_new(path: Path, payload: bytes) -> None:
    _require(not path.exists(), f"refusing to overwrite {path.name}")
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _read_checkpoint(path: Path, trial: Mapping[str, Any]) -> dict[str, Any]:
    checkpoint = _read_json(path, "semantic trial checkpoint")
    recorded = checkpoint.pop("checkpoint_sha256", None)
    _require(
        recorded == _sha256(_canonical_bytes(checkpoint)),
        "trial checkpoint digest mismatch",
    )
    checkpoint["checkpoint_sha256"] = recorded
    _require(
        checkpoint.get("schema_version") == CHECKPOINT_SCHEMA_VERSION
        and checkpoint.get("status") == "TRIAL_COMPLETED"
        and checkpoint.get("trial_key") == trial["trial_key"]
        and checkpoint.get("order_index") == trial["order_index"]
        and checkpoint.get("source_logical_trial_sha256")
        == trial["source_logical_trial_sha256"],
        "trial checkpoint identity changed",
    )
    raw_result = checkpoint.get("executor_result")
    _require(isinstance(raw_result, Mapping), "checkpoint executor result is missing")
    result = _validated_result(
        raw_result,
        trial=trial,
        idempotency_key=raw_result.get("idempotency_key"),
    )
    _require(
        checkpoint.get("neutral_evidence") == _neutral_evidence(trial, result)
        and checkpoint.get("credentials_recorded") is False
        and checkpoint.get("eligible_for_scientific_claims") is False,
        "trial checkpoint evidence changed",
    )
    _assert_public_evidence(checkpoint)
    return checkpoint


def _initialize_run(root: Path, contract: Mapping[str, Any]) -> None:
    _require(not root.exists(), f"semantic matrix run output already exists: {root}")
    root.parent.mkdir(parents=True, exist_ok=True)
    parent = Path(tempfile.mkdtemp(prefix=".semantic-run-", dir=root.parent))
    stage = parent / "output"
    try:
        stage.mkdir()
        (stage / CHECKPOINT_DIR_NAME).mkdir()
        _write_atomic_new(stage / CONTRACT_NAME, _json_bytes(contract))
        _write_atomic_new(stage / JOURNAL_NAME, b"")
        os.replace(stage, root)
    finally:
        if stage.exists():
            for path in sorted(stage.rglob("*"), reverse=True):
                if path.is_file():
                    path.unlink()
                elif path.is_dir():
                    path.rmdir()
            stage.rmdir()
        parent.rmdir()


def _validate_contract(root: Path, expected: Mapping[str, Any]) -> dict[str, Any]:
    contract = _read_json(root / CONTRACT_NAME, "semantic matrix run contract")
    recorded = contract.pop("contract_sha256", None)
    _require(
        recorded == _sha256(_canonical_bytes(contract)),
        "run contract digest mismatch",
    )
    contract["contract_sha256"] = recorded
    _require(contract == expected, "run contract does not match frozen inputs")
    return contract


def _final_documents(
    root: Path,
    contract: Mapping[str, Any],
    trials: Sequence[Mapping[str, Any]],
    journal_rows: Sequence[Mapping[str, Any]],
) -> tuple[bytes, bytes, bytes]:
    audit = _audit_journal(journal_rows, trials=trials, contract=contract)
    _require(
        audit["completed"] == [row["trial_key"] for row in trials]
        and audit["terminal_state"] == "TRIAL_COMPLETED",
        "journal does not contain the complete canonical trial prefix",
    )
    evidence: list[dict[str, Any]] = []
    route_evidence: list[dict[str, Any]] = []
    for trial in trials:
        checkpoint = _read_checkpoint(_checkpoint_path(root, trial), trial)
        _require(
            checkpoint["executor_result"]
            == audit["result_by_trial"].get(trial["trial_key"]),
            "trial checkpoint differs from the durable journal result",
        )
        _require(
            audit["completion_checkpoint_by_trial"].get(trial["trial_key"])
            == checkpoint["checkpoint_sha256"],
            "journal completion does not bind the trial checkpoint",
        )
        evidence.append(checkpoint["neutral_evidence"])
        public_route = checkpoint["executor_result"]["semantic_route_evidence"]
        if public_route is not None:
            _require(
                public_route["trial_key"] == trial["trial_key"],
                "checkpoint route evidence names another trial",
            )
            route_evidence.append(public_route)
    evidence_bytes = b"".join(_json_bytes(row) for row in evidence)
    route_evidence_bytes = b"".join(
        _json_bytes(row) for row in route_evidence
    )
    successes = Counter(row["task_success"] for row in evidence)
    route_counts = Counter(row["route_family"] for row in evidence)
    transports = Counter(row["execution_transport"] for row in evidence)
    report = {
        "schema_version": RUN_REPORT_SCHEMA_VERSION,
        "status": "COMPLETE",
        "run_id": contract["run_id"],
        "scenario_id": contract["scenario_id"],
        "contract_sha256": contract["contract_sha256"],
        "semantic_matrix_plan_sha256": contract["source_binding"][
            "semantic_matrix_plan_sha256"
        ],
        "deployment_binding_sha256": contract["source_binding"][
            "deployment_binding_sha256"
        ],
        "planned_trial_count": 64,
        "completed_trial_count": 64,
        "neutral_evidence_count": 64,
        "task_success_true_count": successes[True],
        "task_success_false_count": successes[False],
        "route_family_trial_counts": dict(sorted(route_counts.items())),
        "execution_transport_counts": dict(sorted(transports.items())),
        "llm_called_trial_count": sum(row["llm_called"] for row in evidence),
        "telemetry_complete_trial_count": sum(
            row["telemetry_complete"] for row in evidence
        ),
        "historic_infrastructure_failure_count": audit["failure_counts"].get(
            "infrastructure", 0
        ),
        "historic_semantic_failure_count": audit["failure_counts"].get(
            "semantic", 0
        ),
        "failure_acknowledgement_count": audit[
            "failure_acknowledgement_count"
        ],
        "journal_entry_count": len(journal_rows),
        "journal_terminal_entry_sha256": audit["terminal_entry_sha256"],
        "neutral_evidence_file_sha256": _sha256(evidence_bytes),
        "semantic_route_evidence_count": len(route_evidence),
        "semantic_route_evidence_file_sha256": _sha256(
            route_evidence_bytes
        ),
        "semantic_route_evidence_complete_for_flowmesh_trials": (
            len(route_evidence) == transports["flowmesh"]
        ),
        "workflow_submission_implemented_by_runner": False,
        "monetary_cost_included": False,
        "raw_answer_or_hidden_label_content_included": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    report["report_sha256"] = _sha256(_canonical_bytes(report))
    _assert_public_evidence(report)
    return evidence_bytes, route_evidence_bytes, _json_bytes(report)


def _checksum_entries(root: Path) -> list[str]:
    return sorted(
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file() and path.name != CHECKSUMS_NAME
    )


def _write_expected_or_new(path: Path, payload: bytes) -> None:
    if path.exists():
        _require(
            path.is_file()
            and not path.is_symlink()
            and path.read_bytes() == payload,
            f"existing {path.name} differs from deterministic finalization",
        )
        return
    _write_atomic_new(path, payload)


def _freeze_final(
    root: Path,
    evidence: bytes,
    route_evidence: bytes,
    report: bytes,
) -> None:
    _write_expected_or_new(root / EVIDENCE_NAME, evidence)
    _write_expected_or_new(root / ROUTE_EVIDENCE_NAME, route_evidence)
    _write_expected_or_new(root / REPORT_NAME, report)
    names = _checksum_entries(root)
    checksum_bytes = b"".join(
        f"{_sha256((root / name).read_bytes())}  {name}\n".encode("utf-8")
        for name in names
    )
    _write_expected_or_new(root / CHECKSUMS_NAME, checksum_bytes)


def _assert_output_separate(
    output: Path,
    *source_directories: str | Path,
) -> None:
    for source in source_directories:
        root = Path(source).resolve()
        _require(
            output != root and not output.is_relative_to(root),
            "run output cannot be inside a frozen source directory",
        )


def _raise_failed(entry: Mapping[str, Any]) -> None:
    raise SemanticMatrixRunFailed(
        trial_key=str(entry["trial_key"]),
        failure_class=str(entry["failure_class"]),
        failure_code=str(entry["failure_code"]),
        failed_entry_sha256=str(entry["entry_sha256"]),
    )


def _source_arguments(
    semantic_matrix_dir: str | Path,
    deployment_binding_dir: str | Path,
    logical_route_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    public_task_set_path: str | Path,
    artifact_binding_path: str | Path,
) -> dict[str, str | Path]:
    return {
        "semantic_matrix_dir": semantic_matrix_dir,
        "deployment_binding_dir": deployment_binding_dir,
        "logical_route_dir": logical_route_dir,
        "scenario_path": scenario_path,
        "container_plan_dir": container_plan_dir,
        "public_task_set_path": public_task_set_path,
        "artifact_binding_path": artifact_binding_path,
    }


def run_full_flow_semantic_matrix(
    semantic_matrix_dir: str | Path,
    deployment_binding_dir: str | Path,
    logical_route_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    public_task_set_path: str | Path,
    artifact_binding_path: str | Path,
    *,
    run_id: str,
    output_dir: str | Path,
    executor: SemanticTrialExecutor,
    acknowledge_failed_entry_sha256: str | None = None,
) -> dict[str, Any]:
    """Execute or resume the 64 trials in their frozen canonical order."""

    semantic_plan, trials, source_binding = _verified_inputs(
        **_source_arguments(
            semantic_matrix_dir,
            deployment_binding_dir,
            logical_route_dir,
            scenario_path,
            container_plan_dir,
            public_task_set_path,
            artifact_binding_path,
        )
    )
    contract = _contract(run_id, semantic_plan, trials, source_binding)
    root = Path(output_dir).resolve()
    _assert_output_separate(
        root,
        semantic_matrix_dir,
        deployment_binding_dir,
        logical_route_dir,
        container_plan_dir,
    )
    if not root.exists():
        _require(
            acknowledge_failed_entry_sha256 is None,
            "cannot acknowledge a failure for a new run",
        )
        _initialize_run(root, contract)
    else:
        _require(root.is_dir(), "semantic matrix run output is not a directory")
        _validate_contract(root, contract)
        if (root / CHECKSUMS_NAME).exists():
            return verify_full_flow_semantic_matrix_run(
                output_dir=root,
                **_source_arguments(
                    semantic_matrix_dir,
                    deployment_binding_dir,
                    logical_route_dir,
                    scenario_path,
                    container_plan_dir,
                    public_task_set_path,
                    artifact_binding_path,
                ),
            )

    journal_path = root / JOURNAL_NAME
    journal_rows = _read_jsonl(
        journal_path,
        "semantic matrix journal",
        allow_empty=True,
    )
    audit = _audit_journal(journal_rows, trials=trials, contract=contract)
    expected_keys = [row["trial_key"] for row in trials]
    _require(
        audit["completed"] == expected_keys[: len(audit["completed"])],
        "completed trials are not a canonical prefix",
    )

    # Recover only side-effect-free crash windows.  An intent without a result
    # is ambiguous and is converted to an explicit infrastructure failure.
    if audit["terminal_state"] == "TRIAL_RESULT":
        trial = trials[len(audit["completed"])]
        _require(
            audit["active_trial"] == trial["trial_key"],
            "terminal result names the wrong trial",
        )
        result = audit["result_by_trial"][trial["trial_key"]]
        checkpoint_path = _checkpoint_path(root, trial)
        expected_checkpoint = _checkpoint(trial, result)
        if checkpoint_path.exists():
            _require(
                _read_checkpoint(checkpoint_path, trial) == expected_checkpoint,
                "existing recovered checkpoint changed",
            )
        else:
            _write_atomic_new(checkpoint_path, _json_bytes(expected_checkpoint))
        _append_journal(
            journal_path,
            {
                "state": "TRIAL_COMPLETED",
                "trial_key": trial["trial_key"],
                "order_index": trial["order_index"],
                "checkpoint_sha256": expected_checkpoint["checkpoint_sha256"],
                "recovered_without_executor_call": True,
            },
        )
    elif audit["terminal_state"] == "TRIAL_INTENT":
        trial = trials[len(audit["completed"])]
        failure = _append_journal(
            journal_path,
            {
                "state": "RUN_FAILED",
                "trial_key": trial["trial_key"],
                "order_index": trial["order_index"],
                "failure_class": "infrastructure",
                "failure_code": "ambiguous-execution-outcome",
            },
        )
        _raise_failed(failure)

    journal_rows = _read_jsonl(
        journal_path,
        "semantic matrix journal",
        allow_empty=True,
    )
    audit = _audit_journal(journal_rows, trials=trials, contract=contract)
    if audit["terminal_state"] == "RUN_FAILED":
        failed = journal_rows[-1]
        if acknowledge_failed_entry_sha256 is None:
            _raise_failed(failed)
        _digest(
            acknowledge_failed_entry_sha256,
            "acknowledge_failed_entry_sha256",
        )
        _require(
            acknowledge_failed_entry_sha256 == failed["entry_sha256"],
            "failure acknowledgement does not match latest RUN_FAILED entry",
        )
        _append_journal(
            journal_path,
            {
                "state": "FAILURE_ACKNOWLEDGED",
                "trial_key": failed["trial_key"],
                "order_index": failed["order_index"],
                "acknowledged_entry_sha256": failed["entry_sha256"],
            },
        )
    else:
        _require(
            acknowledge_failed_entry_sha256 is None,
            "no latest RUN_FAILED entry exists to acknowledge",
        )

    audit = _audit_journal(
        _read_jsonl(journal_path, "semantic matrix journal", allow_empty=True),
        trials=trials,
        contract=contract,
    )
    completed_count = len(audit["completed"])
    for trial in trials[completed_count:]:
        checkpoint_path = _checkpoint_path(root, trial)
        _require(
            not checkpoint_path.exists(),
            "checkpoint exists before its journal completion",
        )
        idempotency_key = _idempotency_key(contract, trial["trial_key"])
        _append_journal(
            journal_path,
            {
                "state": "TRIAL_INTENT",
                "trial_key": trial["trial_key"],
                "order_index": trial["order_index"],
                "idempotency_key": idempotency_key,
            },
        )
        try:
            executor_trial = _read_json_bytes(
                _canonical_bytes(trial),
                "executor trial copy",
            )
            raw_result = executor.execute(
                trial=executor_trial,
                idempotency_key=idempotency_key,
            )
            result = _validated_result(
                raw_result,
                trial=trial,
                idempotency_key=idempotency_key,
            )
        except SemanticTrialExecutionError as exc:
            failure_class = exc.failure_class
            failure_code = exc.failure_code
        except FullFlowSemanticMatrixRunnerError:
            failure_class = "semantic"
            failure_code = "invalid-executor-result"
        except Exception:
            failure_class = "infrastructure"
            failure_code = "unclassified-executor-exception"
        else:
            _append_journal(
                journal_path,
                {
                    "state": "TRIAL_RESULT",
                    "trial_key": trial["trial_key"],
                    "order_index": trial["order_index"],
                    "result": result,
                },
            )
            checkpoint = _checkpoint(trial, result)
            _write_atomic_new(checkpoint_path, _json_bytes(checkpoint))
            _append_journal(
                journal_path,
                {
                    "state": "TRIAL_COMPLETED",
                    "trial_key": trial["trial_key"],
                    "order_index": trial["order_index"],
                    "checkpoint_sha256": checkpoint["checkpoint_sha256"],
                    "recovered_without_executor_call": False,
                },
            )
            continue
        failure = _append_journal(
            journal_path,
            {
                "state": "RUN_FAILED",
                "trial_key": trial["trial_key"],
                "order_index": trial["order_index"],
                "failure_class": failure_class,
                "failure_code": failure_code,
            },
        )
        _raise_failed(failure)

    journal_rows = _read_jsonl(
        journal_path,
        "semantic matrix journal",
        allow_empty=True,
    )
    evidence, route_evidence, report = _final_documents(
        root,
        contract,
        trials,
        journal_rows,
    )
    _freeze_final(root, evidence, route_evidence, report)
    return verify_full_flow_semantic_matrix_run(
        output_dir=root,
        **_source_arguments(
            semantic_matrix_dir,
            deployment_binding_dir,
            logical_route_dir,
            scenario_path,
            container_plan_dir,
            public_task_set_path,
            artifact_binding_path,
        ),
    )


def verify_full_flow_semantic_matrix_run(
    semantic_matrix_dir: str | Path,
    deployment_binding_dir: str | Path,
    logical_route_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    public_task_set_path: str | Path,
    artifact_binding_path: str | Path,
    *,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Verify a complete run and rebind it to all frozen source inputs."""

    semantic_plan, trials, source_binding = _verified_inputs(
        **_source_arguments(
            semantic_matrix_dir,
            deployment_binding_dir,
            logical_route_dir,
            scenario_path,
            container_plan_dir,
            public_task_set_path,
            artifact_binding_path,
        )
    )
    root = Path(output_dir).resolve()
    _assert_output_separate(
        root,
        semantic_matrix_dir,
        deployment_binding_dir,
        logical_route_dir,
        container_plan_dir,
    )
    _require(root.is_dir(), "semantic matrix run directory does not exist")
    contract_file = _read_json(root / CONTRACT_NAME, "run contract")
    run_id = contract_file.get("run_id")
    expected_contract = _contract(run_id, semantic_plan, trials, source_binding)
    contract = _validate_contract(root, expected_contract)
    checkpoint_dir = root / CHECKPOINT_DIR_NAME
    _require(
        checkpoint_dir.is_dir() and not checkpoint_dir.is_symlink(),
        "trial checkpoint directory is invalid",
    )
    expected_checkpoints = {_checkpoint_path(root, trial) for trial in trials}
    actual_checkpoints = set(checkpoint_dir.iterdir())
    _require(
        actual_checkpoints == expected_checkpoints
        and all(
            path.is_file() and not path.is_symlink()
            for path in actual_checkpoints
        ),
        "trial checkpoint file set changed",
    )
    _require(
        {path.name for path in root.iterdir() if path.is_dir()}
        == {CHECKPOINT_DIR_NAME}
        and all(not path.is_symlink() for path in root.iterdir()),
        "run directory structure changed",
    )
    journal_rows = _read_jsonl(root / JOURNAL_NAME, "semantic matrix journal")
    expected_evidence, expected_route_evidence, expected_report = _final_documents(
        root,
        contract,
        trials,
        journal_rows,
    )
    _require(
        (root / EVIDENCE_NAME).read_bytes() == expected_evidence
        and (root / ROUTE_EVIDENCE_NAME).read_bytes()
        == expected_route_evidence
        and (root / REPORT_NAME).read_bytes() == expected_report,
        "semantic matrix final evidence or report changed",
    )
    checksum_lines = (root / CHECKSUMS_NAME).read_text(encoding="utf-8").splitlines()
    expected_names = _checksum_entries(root)
    _require(
        len(checksum_lines) == len(expected_names),
        "semantic matrix run checksum coverage changed",
    )
    named: list[str] = []
    for line in checksum_lines:
        digest, separator, name = line.partition("  ")
        _require(
            separator == "  "
            and _SHA256.fullmatch(digest) is not None
            and name in expected_names,
            "semantic matrix run checksum line is malformed",
        )
        path = root / name
        _require(
            path.is_file()
            and not path.is_symlink()
            and _sha256(path.read_bytes()) == digest,
            f"semantic matrix run checksum mismatch: {name}",
        )
        named.append(name)
    _require(named == expected_names, "semantic matrix checksums are not canonical")
    top_level = {path.name for path in root.iterdir() if path.is_file()}
    _require(top_level == _FINAL_TOP_LEVEL_FILES, "run top-level file set changed")
    report = _read_json(root / REPORT_NAME, "semantic matrix run report")
    return {
        "status": "VERIFIED",
        "run_id": report["run_id"],
        "planned_trial_count": 64,
        "completed_trial_count": 64,
        "neutral_evidence_count": 64,
        "semantic_route_evidence_count": report[
            "semantic_route_evidence_count"
        ],
        "semantic_route_evidence_complete_for_flowmesh_trials": report[
            "semantic_route_evidence_complete_for_flowmesh_trials"
        ],
        "task_success_true_count": report["task_success_true_count"],
        "task_success_false_count": report["task_success_false_count"],
        "historic_infrastructure_failure_count": report[
            "historic_infrastructure_failure_count"
        ],
        "historic_semantic_failure_count": report[
            "historic_semantic_failure_count"
        ],
        "source_binding_checked": True,
        "workflow_submission_implemented_by_runner": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def load_full_flow_semantic_matrix_route_evidence(
    output_dir: str | Path,
) -> dict[str, Any]:
    """Load a complete public route-evidence sidecar from a frozen run.

    This verifier is intentionally source-independent.  It verifies the
    frozen run checksum set, report, checkpoints, reduced-result binding, and
    exact 64-record route-evidence sidecar.  A consumer must still rebind each
    record to the promoted semantic admission before interpreting it.
    """

    root = Path(output_dir).resolve()
    _require(root.is_dir() and not root.is_symlink(), "matrix run is missing")
    _require(
        {path.name for path in root.iterdir() if path.is_dir()}
        == {CHECKPOINT_DIR_NAME}
        and all(not path.is_symlink() for path in root.iterdir()),
        "matrix run directory structure changed",
    )
    _require(
        {path.name for path in root.iterdir() if path.is_file()}
        == _FINAL_TOP_LEVEL_FILES,
        "matrix run top-level file set changed",
    )
    expected_names = _checksum_entries(root)
    checksum_lines = (root / CHECKSUMS_NAME).read_text(
        encoding="utf-8"
    ).splitlines()
    _require(
        len(checksum_lines) == len(expected_names),
        "matrix run checksum coverage changed",
    )
    named: list[str] = []
    for line in checksum_lines:
        digest, separator, name = line.partition("  ")
        _require(
            separator == "  "
            and _SHA256.fullmatch(digest) is not None
            and name in expected_names,
            "matrix run checksum line is malformed",
        )
        path = root / name
        _require(
            path.is_file()
            and not path.is_symlink()
            and _sha256(path.read_bytes()) == digest,
            f"matrix run checksum mismatch: {name}",
        )
        named.append(name)
    _require(named == expected_names, "matrix run checksums are not canonical")

    report_path = root / REPORT_NAME
    report = _read_json(report_path, "semantic matrix run report")
    _require(
        report_path.read_bytes() == _json_bytes(report),
        "semantic matrix run report is not canonical",
    )
    supplied_report_sha256 = report.get("report_sha256")
    _digest(supplied_report_sha256, "matrix report_sha256")
    report_core = dict(report)
    del report_core["report_sha256"]
    _require(
        supplied_report_sha256 == _sha256(_canonical_bytes(report_core))
        and report.get("schema_version") == RUN_REPORT_SCHEMA_VERSION
        and report.get("status") == "COMPLETE"
        and report.get("planned_trial_count") == 64
        and report.get("completed_trial_count") == 64
        and report.get("neutral_evidence_count") == 64,
        "semantic matrix run report is incomplete or invalid",
    )

    checkpoint_root = root / CHECKPOINT_DIR_NAME
    checkpoint_paths = sorted(checkpoint_root.iterdir())
    _require(
        len(checkpoint_paths) == 64
        and all(path.is_file() and not path.is_symlink() for path in checkpoint_paths),
        "semantic matrix route evidence lacks 64 checkpoints",
    )
    checkpoint_routes: list[tuple[int, dict[str, Any]]] = []
    seen_trials: set[str] = set()
    for path in checkpoint_paths:
        checkpoint = _read_json(path, "semantic trial checkpoint")
        _require(
            path.read_bytes() == _json_bytes(checkpoint),
            "semantic trial checkpoint is not canonical",
        )
        recorded_checkpoint_sha256 = checkpoint.get("checkpoint_sha256")
        _digest(recorded_checkpoint_sha256, "checkpoint_sha256")
        checkpoint_core = dict(checkpoint)
        del checkpoint_core["checkpoint_sha256"]
        _require(
            recorded_checkpoint_sha256
            == _sha256(_canonical_bytes(checkpoint_core))
            and checkpoint.get("schema_version") == CHECKPOINT_SCHEMA_VERSION
            and checkpoint.get("status") == "TRIAL_COMPLETED"
            and checkpoint.get("credentials_recorded") is False
            and checkpoint.get("eligible_for_scientific_claims") is False,
            "semantic trial checkpoint integrity changed",
        )
        trial_key = checkpoint.get("trial_key")
        order_index = checkpoint.get("order_index")
        _require(
            isinstance(trial_key, str)
            and trial_key not in seen_trials
            and type(order_index) is int
            and 0 <= order_index < 64,
            "semantic trial checkpoint identity changed",
        )
        seen_trials.add(trial_key)
        _digest(
            checkpoint.get("source_logical_trial_sha256"),
            "source logical trial SHA-256",
        )
        raw_result = checkpoint.get("executor_result")
        _require(
            isinstance(raw_result, Mapping),
            "checkpoint executor result is missing",
        )
        result = _validated_result(
            raw_result,
            trial={"trial_key": trial_key},
            idempotency_key=raw_result.get("idempotency_key"),
        )
        route = result.get("semantic_route_evidence")
        _require(
            isinstance(route, dict),
            "checkpoint lacks required FlowMesh route evidence",
        )
        neutral = checkpoint.get("neutral_evidence")
        _require(
            isinstance(neutral, Mapping)
            and neutral.get("trial_key") == trial_key
            and neutral.get("order_index") == order_index
            and neutral.get("route_evidence_sha256")
            == result["route_evidence_sha256"]
            and neutral.get("source_executor_result_sha256")
            == _sha256(_canonical_bytes(result)),
            "checkpoint route evidence is not bound to its reduced result",
        )
        neutral_core = dict(neutral)
        recorded_neutral_sha256 = neutral_core.pop("evidence_sha256", None)
        _digest(recorded_neutral_sha256, "neutral evidence_sha256")
        _require(
            recorded_neutral_sha256
            == _sha256(_canonical_bytes(neutral_core)),
            "checkpoint neutral evidence digest mismatch",
        )
        _assert_public_evidence(checkpoint)
        checkpoint_routes.append((order_index, route))
    checkpoint_routes.sort(key=lambda item: item[0])
    _require(
        [index for index, _ in checkpoint_routes] == list(range(64)),
        "semantic matrix checkpoint order is incomplete",
    )

    route_path = root / ROUTE_EVIDENCE_NAME
    route_evidence = _read_jsonl(
        route_path,
        "semantic matrix route evidence",
    )
    _require(
        route_path.read_bytes()
        == b"".join(_json_bytes(row) for row in route_evidence)
        and route_evidence == [row for _, row in checkpoint_routes],
        "semantic matrix route-evidence sidecar differs from checkpoints",
    )
    _require(
        len(route_evidence) == 64
        and len({row["trial_key"] for row in route_evidence}) == 64
        and report.get("semantic_route_evidence_count") == 64
        and report.get("semantic_route_evidence_file_sha256")
        == _sha256(route_path.read_bytes())
        and report.get("semantic_route_evidence_complete_for_flowmesh_trials")
        is True
        and report.get("execution_transport_counts") == {"flowmesh": 64},
        "semantic matrix does not contain complete FlowMesh route evidence",
    )
    return {
        "status": "VERIFIED_PUBLIC_ROUTE_EVIDENCE",
        "run_id": report["run_id"],
        "report_sha256": report["report_sha256"],
        "report_file_sha256": _sha256(report_path.read_bytes()),
        "route_evidence_file_sha256": _sha256(route_path.read_bytes()),
        "route_evidence_count": 64,
        "evidence_records": route_evidence,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


__all__ = [
    "CHECKPOINT_DIR_NAME",
    "CHECKSUMS_NAME",
    "CONTRACT_NAME",
    "EVIDENCE_NAME",
    "JOURNAL_NAME",
    "NEUTRAL_EVIDENCE_SCHEMA_VERSION",
    "PUBLIC_ROUTE_EVIDENCE_SCHEMA_VERSION",
    "REPORT_NAME",
    "ROUTE_EVIDENCE_NAME",
    "RUN_REPORT_SCHEMA_VERSION",
    "TRIAL_RESULT_SCHEMA_VERSION",
    "validate_semantic_trial_result",
    "FullFlowSemanticMatrixRunnerError",
    "SemanticMatrixRunFailed",
    "SemanticTrialExecutionError",
    "SemanticTrialExecutor",
    "load_full_flow_semantic_matrix_route_evidence",
    "run_full_flow_semantic_matrix",
    "verify_full_flow_semantic_matrix_run",
]
