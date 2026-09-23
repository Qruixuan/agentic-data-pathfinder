"""Strict public schema for durable full-flow semantic route evidence.

The route coordinator is an effect boundary: its evidence is allowed to retain
the public model prediction sent to N1, but it must never retain credentials or
hidden relevance/answer values.  Keeping this schema in a dependency-neutral
module lets the route runtime, FlowMesh adapter, and durable matrix runner apply
the same structural and commitment checks without introducing an import cycle.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping
from typing import Any

from .full_flow_semantic_input_profiles import RAW_DIRECT_VIDEO_PROFILE_ID
from .hidden_oracle import (
    N1_SCORE_REQUEST_SCHEMA_VERSION,
    N1_SCORE_RESULT_SCHEMA_VERSION,
)


SEMANTIC_ROUTE_EVIDENCE_SCHEMA_VERSION = (
    "pathfinder.full-flow-semantic-route-evidence/v1alpha2"
)
SEMANTIC_ROUTE_EPISODE_EVIDENCE_SCHEMA_VERSION = (
    "pathfinder.full-flow-semantic-route-evidence/v1alpha3"
)

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:+-]{0,255}\Z")

_EVIDENCE_FIELDS = {
    "schema_version",
    "status",
    "execution_id",
    "request_sha256",
    "run_id",
    "trial_id",
    "trial_key",
    "trial_sha256",
    "stage_dag_sha256",
    "workload_id",
    "workload_class",
    "design_id",
    "repetition",
    "artifact_object_id",
    "public_task_binding_sha256",
    "route",
    "artifact_identities",
    "provisioning_references",
    "stage_results",
    "cache_branches",
    "model_input",
    "semantic",
    "scoring",
    "n1_score_request",
    "n1_score_result",
    "neutral_observation_candidate",
    "all_frozen_stages_accounted_for",
    "exclusive_cache_branches_verified",
    "n2_exact_range_required_for_indexed_raw",
    "n3_n4_artifact_identity_verified",
    "n5_provisioning_references_verified",
    "n6_input_mode_verified",
    "semantic_input_profile_verified",
    "n1_exactly_once_authenticated_score_verified",
    "idempotent_replay",
    "endpoint_values_included",
    "credential_values_included",
    "credentials_recorded",
    "eligible_for_scientific_claims",
    "evidence_sha256",
}
_ROUTE_FIELDS = {
    "route_family",
    "executor_node_id",
    "inference_node_id",
    "score_node_id",
}
_ARTIFACT_IDENTITY_FIELDS = {
    "logical_object_id",
    "object_id",
    "representation_id",
    "artifact_sha256",
    "artifact_size_bytes",
    "object_catalog_version",
    "identity_sha256",
}
_PROVISIONING_REFERENCE_FIELDS = {
    "chain_id",
    "logical_object_id",
    "artifact_identity_sha256",
    "n5_evidence_sha256",
    "n4_publication_sha256",
    "available",
}
_STAGE_RESULT_FIELDS = {
    "stage_key",
    "stage_index",
    "action",
    "condition",
    "state",
    "outcome_kind",
    "outcome_sha256",
    "service_time_ms",
    "bytes_read",
    "bytes_sent",
}
_CACHE_CONDITION_FIELDS = {
    "cache_operation_id",
    "cache_operation_key",
    "equals",
}
_CACHE_BRANCH_FIELDS = {
    "lookup_stage_key",
    "representation_id",
    "branch",
    "cache_node_id",
    "cache_id",
    "runtime_epoch",
    "source_insert_trial_key",
    "lookup_sha256",
}
_MODEL_INPUT_FIELDS = {
    "mode",
    "payload_sha256",
    "payload_size_bytes",
    "component_identity_sha256",
    "preparation_sha256",
    "semantic_input_profile_id",
    "semantic_input_profile_sha256",
    "semantic_input_profile_verified",
    "semantic_content_sha256",
    "frame_count",
    "frame_timestamps_seconds",
    "frame_dimensions",
    "frame_payload_bytes",
    "frame_sequence_sha256",
    "digest_input_sha256",
    "temporal_window_fraction",
    "direct_video_input",
    "direct_video_sha256",
    "direct_video_size_bytes",
}
# Evidence frozen before the direct-video path existed carries no video
# fields.  It remains verifiable, but only while it claims no direct video.
_PRE_DIRECT_VIDEO_MODEL_INPUT_FIELDS = _MODEL_INPUT_FIELDS - {
    "direct_video_sha256",
    "direct_video_size_bytes",
}
_LEGACY_EVIDENCE_FIELDS = _EVIDENCE_FIELDS - {
    "semantic_input_profile_verified"
}
_LEGACY_MODEL_INPUT_FIELDS = {
    "mode",
    "payload_sha256",
    "payload_size_bytes",
    "component_identity_sha256",
    "preparation_sha256",
}
_SEMANTIC_FIELDS = {
    "model",
    "input_sha256",
    "request_sha256",
    "result_sha256",
    "final_answer_sha256",
    "service_time_ms",
}
_SCORING_FIELDS = {
    "oracle_id",
    "score_request_id",
    "evaluation_unit_id",
    "task_binding_sha256",
    "task_success",
    "score",
    "score_evidence_hmac_sha256",
    "result_content_sha256",
    "authentication_verification_sha256",
    "authenticated_n1_v1alpha2",
}
_N1_SCORE_REQUEST_FIELDS = {
    "schema_version",
    "score_request_id",
    "evaluation_unit_id",
    "oracle_id",
    "run_id",
    "trial_id",
    "object_id",
    "task_binding_sha256",
    "predicted_answer",
    "credentials_recorded",
}
_N1_SCORE_RESULT_FIELDS = {
    "schema_version",
    "status",
    "score_request_id",
    "evaluation_unit_id",
    "oracle_id",
    "node_id",
    "run_id",
    "trial_id",
    "object_id",
    "task_binding_sha256",
    "request_sha256",
    "prediction_sha256",
    "success_scoring_rule",
    "correct",
    "score",
    "public_task_set_sha256",
    "oracle_instance_hmac_sha256",
    "score_evidence_hmac_sha256",
    "result_content_sha256",
    "idempotent_replay",
    "hidden_answer_returned",
    "credentials_recorded",
    "eligible_for_scientific_claims",
}
_NEUTRAL_OBSERVATION_FIELDS = {
    "schema_version",
    "trial_key",
    "order_index",
    "workload_id",
    "workload_class",
    "design_id",
    "repetition",
    "object_id",
    "route_family",
    "executor_node_id",
    "task_success",
    "score",
    "score_authenticity_verified",
    "score_authentication",
    "component_service_time_ms",
    "byte_measurements",
    "monetary_measurement_available",
    "monetary_values_included",
    "synthetic_monetary_inputs_consumed",
    "credentials_recorded",
    "eligible_for_scientific_claims",
}
_BYTE_MEASUREMENT_FIELDS = {
    "adapter_bytes_read",
    "adapter_bytes_sent",
    "semantic_input_bytes",
}
_FORBIDDEN_PUBLIC_KEYS = {
    "access_token",
    "api_key",
    "authorization",
    "bearer_token",
    "correct_answer",
    "correct_answer_id",
    "credential",
    "credential_value",
    "hidden_answer",
    "hidden_label",
    "hidden_labels",
    "label_package",
    "label_package_path",
    "label_values",
    "labels",
    "password",
    "relevance_label",
    "relevance_labels",
    "relevance_value",
    "relevance_values",
    "secret",
    "token",
}


class SemanticRouteEvidenceValidationError(ValueError):
    """Raised when public semantic route evidence crosses its boundary."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise SemanticRouteEvidenceValidationError(message)


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
        raise SemanticRouteEvidenceValidationError(
            "semantic route evidence is not canonical JSON"
        ) from exc


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _copy_json(value: Any) -> Any:
    return json.loads(_canonical(value))


def _verify_direct_video_claim(
    model_input: Mapping[str, Any],
    profile_id: Any,
    profile_verified: bool,
) -> None:
    """Bind the direct-video claim to the bytes and mode it asserts.

    A telemetry flag alone is never sufficient: the claim must agree with the
    frozen profile, the N6 input mode, and a committed video identity.
    """

    claimed = model_input.get("direct_video_input")
    _require(type(claimed) is bool, "direct_video_input must be boolean")
    mode = model_input.get("mode")
    _require(
        claimed is (mode == "direct-video"),
        "direct video claim disagrees with the N6 input mode",
    )
    if profile_verified:
        _require(
            claimed is (profile_id == RAW_DIRECT_VIDEO_PROFILE_ID),
            "direct video claim disagrees with its frozen profile",
        )
    video_sha256 = model_input.get("direct_video_sha256")
    video_size = model_input.get("direct_video_size_bytes")
    if not claimed:
        _require(
            video_sha256 is None and video_size is None,
            "non-direct-video input carries a video commitment",
        )
        return
    _digest(video_sha256, "direct_video_sha256")
    _require(
        type(video_size) is int and video_size > 0,
        "direct video size is invalid",
    )
    # Real encoded video is delivered whole; it is never a frame sequence and
    # never carries a sampling window or a derived digest.
    _require(
        model_input.get("frame_count") == 0
        and model_input.get("frame_sequence_sha256") is None
        and model_input.get("frame_payload_bytes") == 0
        and model_input.get("temporal_window_fraction") is None
        and model_input.get("digest_input_sha256") is None,
        "direct video input also claims a sampled representation",
    )


def _exact_fields(value: Any, expected: set[str], name: str) -> Mapping[str, Any]:
    _require(isinstance(value, Mapping), f"{name} must be an object")
    _require(set(value) == expected, f"{name} fields changed")
    return value


def _digest(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _SHA256.fullmatch(value) is not None,
        f"{name} is not lowercase SHA-256",
    )
    return str(value)


def _number(value: Any, name: str) -> float:
    _require(
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(float(value)),
        f"{name} must be finite numeric evidence",
    )
    return float(value)


def _assert_no_private_keys(value: Any, path: str = "evidence") -> None:
    if isinstance(value, Mapping):
        schema = value.get("schema_version")
        for raw_key, child in value.items():
            key = str(raw_key).casefold().replace("-", "_")
            public_prediction = (
                schema == N1_SCORE_REQUEST_SCHEMA_VERSION
                and key == "predicted_answer"
            )
            _require(
                public_prediction or key not in _FORBIDDEN_PUBLIC_KEYS,
                f"private field entered route evidence at {path}.{raw_key}",
            )
            _assert_no_private_keys(child, f"{path}.{raw_key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _assert_no_private_keys(child, f"{path}[{index}]")
    else:
        _require(
            not isinstance(value, (bytes, bytearray)),
            f"raw bytes entered route evidence at {path}",
        )


def _validate_shape(evidence: Mapping[str, Any]) -> None:
    is_legacy = "semantic_input_profile_verified" not in evidence
    is_episode = (
        evidence.get("schema_version")
        == SEMANTIC_ROUTE_EPISODE_EVIDENCE_SCHEMA_VERSION
    )
    _exact_fields(
        evidence,
        (_EVIDENCE_FIELDS | {"cache_episode_id"}) if is_episode
        else _LEGACY_EVIDENCE_FIELDS if is_legacy else _EVIDENCE_FIELDS,
        "semantic route evidence",
    )
    _require(
        evidence.get("schema_version") in {
            SEMANTIC_ROUTE_EVIDENCE_SCHEMA_VERSION,
            SEMANTIC_ROUTE_EPISODE_EVIDENCE_SCHEMA_VERSION,
        }
        and evidence.get("status") == "COMPLETE",
        "semantic route evidence schema or status changed",
    )
    route = _exact_fields(evidence.get("route"), _ROUTE_FIELDS, "route")
    if is_episode:
        _require(
            not is_legacy
            and isinstance(evidence.get("cache_episode_id"), str)
            and _IDENTIFIER.fullmatch(evidence["cache_episode_id"]) is not None
            and route.get("route_family") == "local-cache-derived",
            "episode evidence lacks a valid cache scope",
        )

    artifacts = evidence.get("artifact_identities")
    _require(isinstance(artifacts, list), "artifact_identities must be an array")
    for index, row in enumerate(artifacts):
        _exact_fields(row, _ARTIFACT_IDENTITY_FIELDS, f"artifact identity {index}")

    provisioning = evidence.get("provisioning_references")
    _require(
        isinstance(provisioning, list),
        "provisioning_references must be an array",
    )
    for index, row in enumerate(provisioning):
        _exact_fields(
            row,
            _PROVISIONING_REFERENCE_FIELDS,
            f"provisioning reference {index}",
        )

    stages = evidence.get("stage_results")
    _require(isinstance(stages, list), "stage_results must be an array")
    for index, row in enumerate(stages):
        stage = _exact_fields(row, _STAGE_RESULT_FIELDS, f"stage result {index}")
        condition = stage.get("condition")
        if condition is not None:
            _exact_fields(
                condition,
                _CACHE_CONDITION_FIELDS,
                f"stage result {index} condition",
            )

    cache = evidence.get("cache_branches")
    _require(isinstance(cache, list), "cache_branches must be an array")
    for index, row in enumerate(cache):
        _exact_fields(row, _CACHE_BRANCH_FIELDS, f"cache branch {index}")

    model_input_value = evidence.get("model_input")
    _require(isinstance(model_input_value, Mapping), "model_input must be an object")
    if is_legacy:
        _model_input_fields = _LEGACY_MODEL_INPUT_FIELDS
    elif set(model_input_value) == _PRE_DIRECT_VIDEO_MODEL_INPUT_FIELDS:
        _model_input_fields = _PRE_DIRECT_VIDEO_MODEL_INPUT_FIELDS
    else:
        _model_input_fields = _MODEL_INPUT_FIELDS
    model_input = _exact_fields(
        model_input_value,
        _model_input_fields,
        "model_input",
    )
    if _model_input_fields is _PRE_DIRECT_VIDEO_MODEL_INPUT_FIELDS:
        _require(
            model_input.get("direct_video_input") is False,
            "direct video was claimed without its content-bound evidence",
        )
    if not is_legacy:
        _digest(
            model_input.get("semantic_content_sha256"),
            "semantic_content_sha256",
        )
        frame_count = model_input.get("frame_count")
        frame_timestamps = model_input.get("frame_timestamps_seconds")
        frame_dimensions = model_input.get("frame_dimensions")
        _require(
            type(frame_count) is int
            and frame_count >= 0
            and isinstance(frame_timestamps, list)
            and len(frame_timestamps) == frame_count
            and isinstance(frame_dimensions, list)
            and len(frame_dimensions) == frame_count
            and all(
                not isinstance(value, bool)
                and isinstance(value, (int, float))
                and math.isfinite(float(value))
                and float(value) >= 0.0
                for value in frame_timestamps
            )
            and all(
                isinstance(value, Mapping)
                and set(value) == {"width", "height"}
                and type(value.get("width")) is int
                and value["width"] > 0
                and type(value.get("height")) is int
                and value["height"] > 0
                for value in frame_dimensions
            )
            and type(model_input.get("frame_payload_bytes")) is int
            and model_input["frame_payload_bytes"] >= 0,
            "semantic frame metadata is invalid",
        )
        for name in (
            "frame_sequence_sha256",
            "digest_input_sha256",
            "semantic_input_profile_sha256",
        ):
            value = model_input.get(name)
            if value is not None:
                _digest(value, name)
        _require(
            (
                frame_count == 0
                and model_input.get("frame_sequence_sha256") is None
                and model_input.get("frame_payload_bytes") == 0
            )
            or (
                frame_count > 0
                and model_input.get("frame_sequence_sha256") is not None
                and model_input.get("frame_payload_bytes") > 0
            ),
            "semantic frame sequence evidence is inconsistent",
        )
        window = model_input.get("temporal_window_fraction")
        _require(
            window is None
            or (
                isinstance(window, list)
                and len(window) == 2
                and all(
                    not isinstance(value, bool)
                    and isinstance(value, (int, float))
                    and math.isfinite(float(value))
                    for value in window
                )
                and 0.0 <= float(window[0]) < float(window[1]) <= 1.0
            ),
            "semantic temporal window is invalid",
        )
        profile_id = model_input.get("semantic_input_profile_id")
        profile_verified = model_input.get("semantic_input_profile_verified")
        _require(
            type(profile_verified) is bool
            and evidence.get("semantic_input_profile_verified")
            is profile_verified
            and (
                (
                    profile_verified is True
                    and isinstance(profile_id, str)
                    and _IDENTIFIER.fullmatch(profile_id) is not None
                    and model_input.get("semantic_input_profile_sha256")
                    is not None
                )
                or (
                    profile_verified is False
                    and profile_id is None
                    and model_input.get("semantic_input_profile_sha256")
                    is None
                )
            ),
            "semantic input profile evidence is invalid",
        )
        _verify_direct_video_claim(model_input, profile_id, profile_verified)
    _exact_fields(evidence.get("semantic"), _SEMANTIC_FIELDS, "semantic")
    _exact_fields(evidence.get("scoring"), _SCORING_FIELDS, "scoring")

    request = _exact_fields(
        evidence.get("n1_score_request"),
        _N1_SCORE_REQUEST_FIELDS,
        "n1_score_request",
    )
    _require(
        request.get("schema_version") == N1_SCORE_REQUEST_SCHEMA_VERSION
        and isinstance(request.get("predicted_answer"), str)
        and request.get("credentials_recorded") is False,
        "n1_score_request schema changed",
    )
    result = _exact_fields(
        evidence.get("n1_score_result"),
        _N1_SCORE_RESULT_FIELDS,
        "n1_score_result",
    )
    _require(
        result.get("schema_version") == N1_SCORE_RESULT_SCHEMA_VERSION
        and result.get("status") == "SCORED"
        and result.get("node_id") == "N1"
        and type(result.get("correct")) is bool
        and result.get("score") == (1.0 if result["correct"] else 0.0)
        and type(result.get("idempotent_replay")) is bool
        and result.get("hidden_answer_returned") is False
        and result.get("credentials_recorded") is False
        and result.get("eligible_for_scientific_claims") is False,
        "n1_score_result schema changed",
    )
    request_sha256 = _sha256(_canonical(request))
    prediction_sha256 = _sha256(
        str(request["predicted_answer"]).encode("utf-8")
    )
    _require(
        result.get("request_sha256") == request_sha256
        and result.get("prediction_sha256") == prediction_sha256,
        "n1_score_result is not bound to its retained public request",
    )
    for name in (
        "oracle_instance_hmac_sha256",
        "score_evidence_hmac_sha256",
    ):
        _digest(result.get(name), f"n1_score_result {name}")
    result_core = dict(result)
    result_content_sha256 = result_core.pop("result_content_sha256", None)
    _require(
        result_content_sha256 == _sha256(_canonical(result_core)),
        "n1_score_result content digest changed",
    )
    _require(
        all(
            request.get(name) == result.get(name)
            for name in (
                "score_request_id",
                "evaluation_unit_id",
                "oracle_id",
                "run_id",
                "trial_id",
                "object_id",
                "task_binding_sha256",
            )
        ),
        "retained N1 request and result identities differ",
    )

    scoring = evidence["scoring"]
    _require(
        scoring.get("oracle_id") == result.get("oracle_id")
        and scoring.get("score_request_id") == result.get("score_request_id")
        and scoring.get("evaluation_unit_id") == result.get("evaluation_unit_id")
        and scoring.get("task_binding_sha256")
        == result.get("task_binding_sha256")
        and scoring.get("task_success") is result.get("correct")
        and scoring.get("score") == result.get("score")
        and scoring.get("score_evidence_hmac_sha256")
        == result.get("score_evidence_hmac_sha256")
        and scoring.get("result_content_sha256")
        == result.get("result_content_sha256")
        and scoring.get("authenticated_n1_v1alpha2") is True,
        "scoring summary differs from retained N1 result",
    )
    expected_authentication = _sha256(_canonical({
        "domain": "pathfinder.authenticated-n1-score-verification/v1",
        "request_sha256": result["request_sha256"],
        "result_content_sha256": result["result_content_sha256"],
        "score_evidence_hmac_sha256": result["score_evidence_hmac_sha256"],
    }))
    _require(
        scoring.get("authentication_verification_sha256")
        == expected_authentication,
        "N1 authentication verification commitment changed",
    )
    _require(
        request.get("run_id") == evidence.get("run_id")
        and request.get("trial_id") == evidence.get("trial_id")
        and request.get("object_id") == evidence.get("artifact_object_id")
        and request.get("task_binding_sha256")
        == evidence.get("public_task_binding_sha256")
        and evidence["semantic"].get("final_answer_sha256")
        == prediction_sha256,
        "retained N1 exchange differs from semantic route identity",
    )

    observation = _exact_fields(
        evidence.get("neutral_observation_candidate"),
        _NEUTRAL_OBSERVATION_FIELDS,
        "neutral_observation_candidate",
    )
    components = observation.get("component_service_time_ms")
    _require(
        isinstance(components, Mapping),
        "component_service_time_ms must be an object",
    )
    for key, value in components.items():
        _require(
            isinstance(key, str) and _IDENTIFIER.fullmatch(key) is not None,
            "component_service_time_ms has an invalid component id",
        )
        _number(value, f"component_service_time_ms.{key}")
    bytes_by_kind = _exact_fields(
        observation.get("byte_measurements"),
        _BYTE_MEASUREMENT_FIELDS,
        "byte_measurements",
    )
    for key, value in bytes_by_kind.items():
        _require(
            type(value) is int and value >= 0,
            f"byte_measurements.{key} must be a non-negative integer",
        )
    _require(
        observation.get("task_success") is result.get("correct")
        and observation.get("score") == result.get("score")
        and observation.get("score_authenticity_verified") is True
        and observation.get("monetary_measurement_available") is False
        and observation.get("monetary_values_included") is False
        and observation.get("synthetic_monetary_inputs_consumed") is False
        and observation.get("credentials_recorded") is False
        and observation.get("eligible_for_scientific_claims") is False,
        "neutral observation differs from retained N1 result or safety boundary",
    )
    _require(
        evidence.get("endpoint_values_included") is False
        and evidence.get("credential_values_included") is False
        and evidence.get("credentials_recorded") is False
        and evidence.get("eligible_for_scientific_claims") is False,
        "semantic route evidence safety flags changed",
    )


def verify_public_semantic_route_evidence(
    value: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate exact public shape and the replay-stable evidence commitment.

    A coordinator replay intentionally flips only ``idempotent_replay`` while
    retaining the original document commitment.  Both the original evidence
    and that one sanctioned replay representation therefore verify against the
    same ``evidence_sha256``; no other mutation receives normalization.
    """

    _require(isinstance(value, Mapping), "semantic route evidence must be an object")
    evidence = _copy_json(value)
    _assert_no_private_keys(evidence)
    _validate_shape(evidence)
    recorded = _digest(evidence.get("evidence_sha256"), "evidence_sha256")
    core = dict(evidence)
    del core["evidence_sha256"]
    current = _sha256(_canonical(core))
    if current != recorded and core.get("idempotent_replay") is True:
        core["idempotent_replay"] = False
        current = _sha256(_canonical(core))
    _require(current == recorded, "semantic route evidence digest mismatch")
    return evidence


__all__ = [
    "SEMANTIC_ROUTE_EPISODE_EVIDENCE_SCHEMA_VERSION",
    "SEMANTIC_ROUTE_EVIDENCE_SCHEMA_VERSION",
    "SemanticRouteEvidenceValidationError",
    "verify_public_semantic_route_evidence",
]
