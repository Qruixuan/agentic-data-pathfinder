"""One-task, endpoint-free integration harness for the logical full flow.

The harness composes the portable service implementations for all eight
logical Pathfinder nodes without binding them to Docker, FlowMesh, network
addresses, or credentials:

* N2 performs a real deterministic index query;
* N3 resolves and reads the exact raw object through its Data Agent package;
* N5 executes an already-frozen frame-bundle materialization plan;
* N4 atomically publishes that derived artifact and resolves it through its
  Data Agent package;
* N7 and N8 persist, reopen, and verify the exact artifact bytes;
* an injected, offline N6 adapter returns one prediction; and
* N1 alone loads the hidden label and emits opaque scoring evidence.

Only a public task binding and content identities enter the portable output.
The prediction is passed directly from N6 to N1 in memory and is not written
to the harness evidence.  This proves local component composition for one
public task.  It does not exercise HTTP/FlowMesh boundaries, real semantic
quality, route-policy selection, 4x8 coverage, or performance/cost behavior.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Protocol

from ..data_agent_manifest import load_data_agent_manifest
from .full_flow_cache import FullFlowArtifactCache
from .hidden_oracle import (
    N1HiddenOracleService,
    assert_hidden_oracle_fields_absent,
    build_n1_public_task_binding,
    build_n1_score_request,
    verify_n1_oracle_package,
    verify_n1_score_result,
)
from .index_service import (
    N2IndexService,
    build_n2_index_query_request,
    verify_n2_index_package,
    verify_n2_index_query_result,
)
from .n4_derived_data_plane import (
    DATA_AGENT_MANIFEST_PATH as N4_DATA_AGENT_MANIFEST_PATH,
    FRAME_BUNDLE_REPRESENTATION_ID,
    N4ArtifactProvenance,
    N4DerivedArtifactInput,
    N4DerivedRepresentationStore,
    N4_LOGICAL_LOCATION,
    verify_n4_derived_data_package,
    verify_n4_publication_receipt,
)
from .n5_materialization import (
    FrameSampler,
    N5MaterializationRuntime,
    verify_n5_materialization_plan,
)
from .raw_cold_data_plane import (
    DATA_AGENT_MANIFEST_PATH as N3_DATA_AGENT_MANIFEST_PATH,
    REPRESENTATION_ID as RAW_VIDEO_REPRESENTATION_ID,
    SOURCE_LOCATION as N3_SOURCE_LOCATION,
)
from .n3_indexed_data_plane import verify_n3_semantic_data_plane_package


LOCAL_FULL_FLOW_HARNESS_SCHEMA_VERSION = (
    "pathfinder.local-full-flow-harness-evidence/v1alpha2"
)
LOCAL_FULL_FLOW_HARNESS_STATUS = "COMPLETE"
EVIDENCE_NAME = "local-full-flow-harness-evidence.json"
PUBLIC_TASK_NAME = "public-task-binding.json"
CHECKSUMS_NAME = "SHA256SUMS"

_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._|:+-]{0,191}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_WINDOWS_ABSOLUTE = re.compile(r"^[A-Za-z]:[\\/]")
_SENSITIVE_KEY = re.compile(
    r"(?:^|_)(?:api_?key|bearer|password|secret|token)(?:$|_)",
    re.IGNORECASE,
)

_STAGE_ORDER = ["N2", "N3", "N5", "N4", "N7", "N8", "N6", "N1"]
_LOGICAL_NODES = [f"N{index}" for index in range(1, 9)]


class LocalFullFlowHarnessError(RuntimeError):
    """Raised when a component or cross-stage binding is not trustworthy."""


@dataclass(frozen=True)
class N6SemanticInput:
    """Public, content-bound N6 input; it deliberately contains no label."""

    object_id: str
    representation_id: str
    public_task_binding: Mapping[str, Any]
    public_task_binding_sha256: str
    index_ranking_sha256: str
    artifact_sha256: str
    artifact_size_bytes: int
    artifact_bytes: bytes = field(repr=False)


@dataclass(frozen=True)
class N6SemanticResult:
    """One offline prediction returned by an injected N6 adapter."""

    adapter_id: str
    predicted_answer: str = field(repr=False)
    llm_called: bool = False
    external_services_called: bool = False


class N6SemanticAdapter(Protocol):
    """Runtime-only semantic seam used by the local integration harness."""

    def predict(self, value: N6SemanticInput) -> N6SemanticResult:
        """Return one prediction without consulting the N1 hidden oracle."""


class AttestedFrameSampler(FrameSampler, Protocol):
    """Frame sampler carrying explicit, evidence-safe decode provenance."""

    sampler_id: str
    media_decode_exercised: bool


def _require(condition: object, message: str) -> None:
    if not condition:
        raise LocalFullFlowHarnessError(message)


def _identifier(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _IDENTIFIER.fullmatch(value) is not None,
        f"{name} is invalid",
    )
    return value


def _digest(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _SHA256.fullmatch(value) is not None,
        f"{name} is not a lowercase SHA-256 digest",
    )
    return value


def _positive_integer(value: Any, name: str) -> int:
    _require(
        isinstance(value, int) and not isinstance(value, bool) and value > 0,
        f"{name} must be a positive integer",
    )
    return value


def _canonical_bytes(value: Any) -> bytes:
    try:
        return (
            json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise LocalFullFlowHarnessError("value is not canonical JSON") from exc


def _document_bytes(value: Any) -> bytes:
    try:
        return (
            json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise LocalFullFlowHarnessError("value is not JSON serializable") from exc


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _value_sha256(value: Any) -> str:
    return _sha256(_canonical_bytes(value))


def _read_json(path: Path, name: str) -> tuple[bytes, dict[str, Any]]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise LocalFullFlowHarnessError(f"{name} is not readable JSON") from exc
    _require(isinstance(value, dict), f"{name} must be a JSON object")
    return raw, value


def _package_sha256(root: Path, name: str) -> str:
    """Bind a complete portable package without exposing its host path."""

    _require(root.is_dir(), f"{name} package does not exist")
    rows: list[dict[str, Any]] = []
    for candidate in sorted(root.rglob("*")):
        _require(not candidate.is_symlink(), f"{name} contains a symbolic link")
        if not candidate.is_file():
            continue
        relative = candidate.relative_to(root).as_posix()
        payload = candidate.read_bytes()
        rows.append({
            "package_path": relative,
            "size_bytes": len(payload),
            "sha256": _sha256(payload),
        })
    _require(bool(rows), f"{name} package is empty")
    return _value_sha256(rows)


def _assert_portable_public_value(value: Any, name: str = "evidence") -> None:
    """Reject deployment data, secrets, host paths, and hidden-label fields."""

    if isinstance(value, Mapping):
        for key, child in value.items():
            _require(isinstance(key, str), f"{name} has a non-string key")
            if key != "credentials_recorded":
                _require(
                    _SENSITIVE_KEY.search(key) is None,
                    f"{name} contains a sensitive field",
                )
            _assert_portable_public_value(child, f"{name}.{key}")
        return
    if isinstance(value, list):
        for index, child in enumerate(value):
            _assert_portable_public_value(child, f"{name}[{index}]")
        return
    if isinstance(value, str):
        _require("://" not in value, f"{name} contains a network address")
        _require(
            not value.startswith(("/", "\\\\"))
            and _WINDOWS_ABSOLUTE.match(value) is None,
            f"{name} contains an absolute host path",
        )


def _validate_public_task(value: Mapping[str, Any]) -> dict[str, Any]:
    _require(isinstance(value, Mapping), "public task binding must be an object")
    try:
        rebuilt = build_n1_public_task_binding(
            workload_id=value["workload_id"],
            object_id=value["object_id"],
            task_class_id=value["task_class_id"],
            question=value["question"],
            answer_options=value["answer_options"],
            success_scoring_rule=value["success_scoring_rule"],
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise LocalFullFlowHarnessError("public task binding is invalid") from exc
    _require(rebuilt == dict(value), "public task binding is not canonical")
    assert_hidden_oracle_fields_absent(rebuilt)
    return rebuilt


def _write_result(
    output_dir: Path,
    public_task: Mapping[str, Any],
    evidence: Mapping[str, Any],
) -> None:
    _require(not output_dir.exists(), "harness output directory already exists")
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".full-flow-local-", dir=output_dir.parent))
    try:
        documents = {
            PUBLIC_TASK_NAME: _document_bytes(public_task),
            EVIDENCE_NAME: _document_bytes(evidence),
        }
        for filename, payload in documents.items():
            (stage / filename).write_bytes(payload)
        checksums = b"".join(
            f"{_sha256(documents[name])}  {name}\n".encode("utf-8")
            for name in sorted(documents)
        )
        (stage / CHECKSUMS_NAME).write_bytes(checksums)
        verify_local_full_flow_harness(stage)
        os.replace(stage, output_dir)
    except BaseException:
        shutil.rmtree(stage, ignore_errors=True)
        raise


def run_local_full_flow_harness(
    *,
    harness_id: str,
    public_task_binding: Mapping[str, Any],
    n1_oracle_package_dir: str | Path,
    n1_state_db: str | Path,
    n1_evidence_secret: bytes,
    n2_index_package_dir: str | Path,
    n3_raw_package_dir: str | Path,
    n5_materialization_plan: Mapping[str, Any],
    n5_sampler: AttestedFrameSampler,
    n4_store_dir: str | Path,
    n7_cache_dir: str | Path,
    n8_cache_dir: str | Path,
    n6_semantic_adapter: N6SemanticAdapter,
    output_dir: str | Path,
    cache_capacity_bytes: int = 128 * 1024 * 1024,
) -> dict[str, Any]:
    """Run one public task through all logical nodes using direct interfaces.

    Deployment paths and the N1 evidence secret are runtime-only arguments.
    State locations must be new so this bounded harness cannot silently adopt
    evidence from an earlier invocation.
    """

    harness_id = _identifier(harness_id, "harness_id")
    public_task = _validate_public_task(public_task_binding)
    output = Path(output_dir).resolve()
    n1_db = Path(n1_state_db).resolve()
    n4_root = Path(n4_store_dir).resolve()
    n7_root = Path(n7_cache_dir).resolve()
    n8_root = Path(n8_cache_dir).resolve()
    for state, name in (
        (n1_db, "N1 state database"),
        (n4_root, "N4 publication store"),
        (n7_root, "N7 cache"),
        (n8_root, "N8 cache"),
        (output, "harness output"),
    ):
        _require(not state.exists(), f"{name} already exists")
    _positive_integer(cache_capacity_bytes, "cache_capacity_bytes")
    _require(
        isinstance(n1_evidence_secret, bytes)
        and len(n1_evidence_secret) >= 32,
        "N1 evidence secret must contain at least 32 bytes",
    )
    sampler_id = _identifier(getattr(n5_sampler, "sampler_id", None), "sampler_id")
    media_decode_exercised = getattr(
        n5_sampler,
        "media_decode_exercised",
        None,
    )
    _require(
        type(media_decode_exercised) is bool,
        "N5 sampler must explicitly attest media_decode_exercised",
    )

    n1_root = Path(n1_oracle_package_dir).resolve()
    n2_root = Path(n2_index_package_dir).resolve()
    n3_root = Path(n3_raw_package_dir).resolve()
    n1_package = verify_n1_oracle_package(n1_root)
    n2_package = verify_n2_index_package(n2_root)
    n3_package = verify_n3_semantic_data_plane_package(n3_root)
    n1_package_sha = _package_sha256(n1_root, "N1")
    n2_package_sha = _package_sha256(n2_root, "N2")
    n3_package_sha = _package_sha256(n3_root, "N3")

    n2_service = N2IndexService(n2_root)
    query_request = build_n2_index_query_request(
        request_id=f"{harness_id}-query",
        query_id=f"{harness_id}-public-task",
        index_id=n2_package["index_id"],
        query_text=public_task["question"],
        top_k=1,
    )
    query_result = n2_service.query(query_request)
    verify_n2_index_query_result(
        package_dir=n2_root,
        request=query_request,
        result=query_result,
    )
    selected_object_id = query_result["ranked_candidates"][0]["object_id"]
    _require(
        selected_object_id == public_task["object_id"],
        "N2 did not select the public task object",
    )

    n5_plan = verify_n5_materialization_plan(n5_materialization_plan)
    _require(
        n5_plan["input"]["object_id"] == selected_object_id,
        "N5 plan object differs from the N2 selection",
    )
    n3_manifest = load_data_agent_manifest(
        n3_root / N3_DATA_AGENT_MANIFEST_PATH
    )
    n3_access = n3_manifest.resolve(
        plan_id=n5_plan["plan_id"],
        object_id=selected_object_id,
        representation_id=RAW_VIDEO_REPRESENTATION_ID,
        requested_location=N3_SOURCE_LOCATION,
    )
    _require(
        n3_access.path.is_relative_to(n3_root),
        "N3 Data Agent path escapes its package",
    )
    raw_video = n3_access.path.read_bytes()
    _require(
        len(raw_video) == n5_plan["input"]["size_bytes"]
        and _sha256(raw_video) == n5_plan["input"]["sha256"],
        "N3 raw object differs from the frozen N5 input",
    )

    n5_runtime = N5MaterializationRuntime(
        sampler=n5_sampler,
        software_versions=n5_plan["transformation"]["software_versions"],
    )
    n5_execution = n5_runtime.execute(n5_plan, raw_video)
    n5_evidence = n5_execution.evidence
    _require(
        n5_evidence["input_content_binding_verified"] is True
        and n5_evidence["canonical_frame_bundle_verified"] is True
        and n5_evidence["output_binding_verified"] is True
        and n5_evidence["llm_called"] is False,
        "N5 materialization evidence is incomplete",
    )
    derived = n5_execution.artifact_bytes
    derived_sha = _sha256(derived)
    _require(
        derived_sha == n5_plan["expected_output"]["artifact_sha256"]
        and len(derived) == n5_plan["expected_output"]["artifact_size_bytes"],
        "N5 artifact differs from the frozen output binding",
    )

    n4_store = N4DerivedRepresentationStore(n4_root)
    publication = n4_store.publish(
        publication_id=f"{harness_id}-publication",
        package_id=f"{harness_id}-n4-package",
        catalog_version=f"{harness_id}-n4-catalog",
        expected_current_catalog_version=None,
        artifacts=[N4DerivedArtifactInput(
            object_id=selected_object_id,
            representation_id=FRAME_BUNDLE_REPRESENTATION_ID,
            artifact_bytes=derived,
            plan_ids=(n5_plan["plan_id"],),
            provenance=N4ArtifactProvenance(
                producer_node_id="N5",
                publication_source_id=n5_plan["idempotency_key"],
                source_representation_id=RAW_VIDEO_REPRESENTATION_ID,
                source_content_sha256=n5_plan["input"]["sha256"],
                # N5's transformation identifier is a schema-like value with
                # a slash; N4 provenance identifiers deliberately exclude
                # slashes.  The stable alias is human-readable while the
                # adjacent derivation SHA binds the exact frozen contract.
                derivation_id="n5-uniform-midpoint-frame-bundle-v1",
                derivation_sha256=n5_plan["transformation_contract_sha256"],
            ),
            expected_sha256=derived_sha,
            expected_size_bytes=len(derived),
        )],
    )
    _require(not publication.idempotent_replay, "N4 publication was a replay")
    receipt = verify_n4_publication_receipt(publication.receipt)
    n4_package = verify_n4_derived_data_package(publication.snapshot.package_dir)
    n4_manifest = load_data_agent_manifest(
        publication.snapshot.package_dir / N4_DATA_AGENT_MANIFEST_PATH
    )
    n4_access = n4_manifest.resolve(
        plan_id=n5_plan["plan_id"],
        object_id=selected_object_id,
        representation_id=FRAME_BUNDLE_REPRESENTATION_ID,
        requested_location=N4_LOGICAL_LOCATION,
    )
    published = n4_access.path.read_bytes()
    _require(
        published == derived and _sha256(published) == derived_sha,
        "N4 Data Agent resolved different derived bytes",
    )

    cache_observations: dict[str, dict[str, Any]] = {}
    upstream = published
    for node_id, state_root in (("N7", n7_root), ("N8", n8_root)):
        cache = FullFlowArtifactCache(
            state_root,
            node_id=node_id,
            cache_id=f"{harness_id}-{node_id.lower()}-cache",
            capacity_bytes=cache_capacity_bytes,
        )
        initial = cache.lookup(
            object_id=selected_object_id,
            representation_id=FRAME_BUNDLE_REPRESENTATION_ID,
            expected_sha256=derived_sha,
        )
        _require(initial is None, f"{node_id} cache was not initially empty")
        stored = cache.put(
            request_id=f"{harness_id}-{node_id.lower()}-store",
            object_id=selected_object_id,
            representation_id=FRAME_BUNDLE_REPRESENTATION_ID,
            payload=upstream,
            expected_sha256=derived_sha,
        )
        reopened = FullFlowArtifactCache(
            state_root,
            node_id=node_id,
            cache_id=cache.cache_id,
            capacity_bytes=cache_capacity_bytes,
        )
        hit = reopened.lookup(
            object_id=selected_object_id,
            representation_id=FRAME_BUNDLE_REPRESENTATION_ID,
            expected_sha256=derived_sha,
        )
        _require(
            hit is not None and hit.payload == upstream,
            f"{node_id} cache did not survive reopen",
        )
        verified_cache = reopened.verify()
        cache_observations[node_id] = {
            "node_id": node_id,
            "cache_id": reopened.cache_id,
            "representation_id": FRAME_BUNDLE_REPRESENTATION_ID,
            "artifact_size_bytes": len(hit.payload),
            "artifact_sha256": hit.content_sha256,
            "initial_miss": True,
            "stored": stored["status"] == "STORED",
            "persistent_reopen_hit": True,
            "persistent_state_verified": (
                verified_cache["persistent_state"] is True
            ),
        }
        upstream = hit.payload

    semantic_input_core = {
        "object_id": selected_object_id,
        "representation_id": FRAME_BUNDLE_REPRESENTATION_ID,
        "public_task_binding_sha256": public_task["task_binding_sha256"],
        "index_ranking_sha256": query_result["ranking_sha256"],
        "artifact_sha256": derived_sha,
        "artifact_size_bytes": len(upstream),
    }
    semantic_input_sha = _value_sha256(semantic_input_core)
    semantic_input = N6SemanticInput(
        object_id=selected_object_id,
        representation_id=FRAME_BUNDLE_REPRESENTATION_ID,
        public_task_binding=dict(public_task),
        public_task_binding_sha256=public_task["task_binding_sha256"],
        index_ranking_sha256=query_result["ranking_sha256"],
        artifact_sha256=derived_sha,
        artifact_size_bytes=len(upstream),
        artifact_bytes=upstream,
    )
    semantic_result = n6_semantic_adapter.predict(semantic_input)
    _require(
        isinstance(semantic_result, N6SemanticResult),
        "N6 adapter returned an invalid result type",
    )
    adapter_id = _identifier(semantic_result.adapter_id, "N6 adapter_id")
    _require(
        isinstance(semantic_result.predicted_answer, str)
        and 0 < len(semantic_result.predicted_answer.encode("utf-8")) <= 16 * 1024,
        "N6 prediction is invalid",
    )
    _require(
        semantic_result.llm_called is False
        and semantic_result.external_services_called is False,
        "local N6 adapter must attest no LLM or external service call",
    )
    prediction_sha = _sha256(semantic_result.predicted_answer.encode("utf-8"))

    n1_service = N1HiddenOracleService(
        n1_root,
        state_db=n1_db,
        evidence_secret=n1_evidence_secret,
    )
    score_request = build_n1_score_request(
        score_request_id=f"{harness_id}-score",
        oracle_id=n1_package["oracle_id"],
        run_id=harness_id,
        trial_id=f"{harness_id}-trial",
        object_id=selected_object_id,
        task_binding_sha256=public_task["task_binding_sha256"],
        predicted_answer=semantic_result.predicted_answer,
    )
    score_result = n1_service.score(score_request)
    score_verified = verify_n1_score_result(
        package_dir=n1_root,
        request=score_request,
        result=score_result,
        evidence_secret=n1_evidence_secret,
    )
    _require(
        score_verified["prediction_sha256"] == prediction_sha,
        "N1 scored a different N6 prediction",
    )

    components = {
        "N1": {
            "node_id": "N1",
            "oracle_package_sha256": n1_package_sha,
            "public_task_set_sha256": n1_package["public_task_set_sha256"],
            "score_request_sha256": score_verified["request_sha256"],
            "prediction_sha256": prediction_sha,
            "score_result_sha256": score_result["result_content_sha256"],
            "score_evidence_hmac_sha256": score_result[
                "score_evidence_hmac_sha256"
            ],
            "hidden_answer_returned": False,
        },
        "N2": {
            "node_id": "N2",
            "index_package_sha256": n2_package_sha,
            "index_sha256": n2_package["index_sha256"],
            "query_request_sha256": query_result["request_sha256"],
            "query_result_sha256": query_result["result_content_sha256"],
            "ranking_sha256": query_result["ranking_sha256"],
            "selected_object_id": selected_object_id,
            "lexical_retrieval_executed": True,
        },
        "N3": {
            "node_id": "N3",
            "raw_package_sha256": n3_package_sha,
            "catalog_version": n3_package["catalog_version"],
            "representation_id": RAW_VIDEO_REPRESENTATION_ID,
            "artifact_size_bytes": len(raw_video),
            "artifact_sha256": _sha256(raw_video),
            "data_agent_contract_used": True,
        },
        "N4": {
            "node_id": "N4",
            "package_sha256": n4_package["package_sha256"],
            "catalog_version": n4_package["catalog_version"],
            "publication_receipt_sha256": receipt["receipt_sha256"],
            "representation_id": FRAME_BUNDLE_REPRESENTATION_ID,
            "artifact_size_bytes": len(published),
            "artifact_sha256": derived_sha,
            "data_agent_contract_used": True,
            "atomic_publication_verified": True,
        },
        "N5": {
            "node_id": "N5",
            "plan_sha256": n5_plan["plan_sha256"],
            "transformation_contract_sha256": n5_plan[
                "transformation_contract_sha256"
            ],
            "representation_id": FRAME_BUNDLE_REPRESENTATION_ID,
            "artifact_size_bytes": len(derived),
            "artifact_sha256": derived_sha,
            "sampler_id": sampler_id,
            "media_decode_exercised": media_decode_exercised,
            "llm_called": False,
            "canonical_frame_bundle_verified": True,
        },
        "N6": {
            "node_id": "N6",
            "adapter_id": adapter_id,
            "semantic_input_sha256": semantic_input_sha,
            "prediction_sha256": prediction_sha,
            "public_task_binding_sha256": public_task[
                "task_binding_sha256"
            ],
            "representation_id": FRAME_BUNDLE_REPRESENTATION_ID,
            "llm_called": False,
            "external_services_called": False,
        },
        "N7": cache_observations["N7"],
        "N8": cache_observations["N8"],
    }
    evidence: dict[str, Any] = {
        "schema_version": LOCAL_FULL_FLOW_HARNESS_SCHEMA_VERSION,
        "status": LOCAL_FULL_FLOW_HARNESS_STATUS,
        "evidence_class": "local-component-composition-single-public-task",
        "harness_id": harness_id,
        "public_task_binding_sha256": public_task["task_binding_sha256"],
        "object_id": selected_object_id,
        "task_class_id": public_task["task_class_id"],
        "logical_stage_order": list(_STAGE_ORDER),
        "logical_nodes_exercised": list(_LOGICAL_NODES),
        "component_bindings": components,
        "task_success": score_verified["correct"],
        "score": score_verified["score"],
        "hidden_answer_returned": False,
        "content_binding_verified": True,
        "component_composition_verified": True,
        "semantic_adapter_invoked": True,
        "semantic_task_executed": False,
        "semantic_quality_evaluated": False,
        "semantic_adapter_mode": "injected-offline-test-adapter",
        "n1_trial_control_exercised": False,
        "http_service_boundaries_exercised": False,
        "route_policy_exercised": False,
        "external_attestation_verified": False,
        "self_authenticating": False,
        "media_decode_exercised": media_decode_exercised,
        "single_public_task_only": True,
        "four_by_eight_semantic_coverage_verified": False,
        "real_performance_measured": False,
        "real_cost_measured": False,
        "deployment_binding_included": False,
        "docker_used": False,
        "flowmesh_used": False,
        "external_network_called": False,
        "llm_called": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    assert_hidden_oracle_fields_absent(evidence)
    _assert_portable_public_value(evidence)
    evidence["evidence_sha256"] = _value_sha256(evidence)
    _write_result(output, public_task, evidence)
    verified = verify_local_full_flow_harness(output)
    return {
        **verified,
        "status": "AUTHENTICATED_LOCAL_COMPONENT_RUN",
        "task_success": score_verified["correct"],
        "score": score_verified["score"],
        "reported_task_success": evidence["task_success"],
        "reported_score": evidence["score"],
        "oracle_hmac_verified": True,
        "output_dir": str(output),
    }


_TOP_LEVEL_FIELDS = {
    "component_bindings",
    "component_composition_verified",
    "content_binding_verified",
    "credentials_recorded",
    "deployment_binding_included",
    "docker_used",
    "eligible_for_scientific_claims",
    "evidence_class",
    "evidence_sha256",
    "external_network_called",
    "external_attestation_verified",
    "flowmesh_used",
    "four_by_eight_semantic_coverage_verified",
    "harness_id",
    "hidden_answer_returned",
    "http_service_boundaries_exercised",
    "llm_called",
    "logical_nodes_exercised",
    "logical_stage_order",
    "media_decode_exercised",
    "n1_trial_control_exercised",
    "object_id",
    "public_task_binding_sha256",
    "real_cost_measured",
    "real_performance_measured",
    "route_policy_exercised",
    "schema_version",
    "score",
    "self_authenticating",
    "semantic_adapter_mode",
    "semantic_adapter_invoked",
    "semantic_quality_evaluated",
    "semantic_task_executed",
    "single_public_task_only",
    "status",
    "task_class_id",
    "task_success",
}

_COMPONENT_FIELDS = {
    "N1": {
        "hidden_answer_returned",
        "node_id",
        "oracle_package_sha256",
        "prediction_sha256",
        "public_task_set_sha256",
        "score_evidence_hmac_sha256",
        "score_request_sha256",
        "score_result_sha256",
    },
    "N2": {
        "index_package_sha256",
        "index_sha256",
        "lexical_retrieval_executed",
        "node_id",
        "query_request_sha256",
        "query_result_sha256",
        "ranking_sha256",
        "selected_object_id",
    },
    "N3": {
        "artifact_sha256",
        "artifact_size_bytes",
        "catalog_version",
        "data_agent_contract_used",
        "node_id",
        "raw_package_sha256",
        "representation_id",
    },
    "N4": {
        "artifact_sha256",
        "artifact_size_bytes",
        "atomic_publication_verified",
        "catalog_version",
        "data_agent_contract_used",
        "node_id",
        "package_sha256",
        "publication_receipt_sha256",
        "representation_id",
    },
    "N5": {
        "artifact_sha256",
        "artifact_size_bytes",
        "canonical_frame_bundle_verified",
        "llm_called",
        "media_decode_exercised",
        "node_id",
        "plan_sha256",
        "representation_id",
        "sampler_id",
        "transformation_contract_sha256",
    },
    "N6": {
        "adapter_id",
        "external_services_called",
        "llm_called",
        "node_id",
        "prediction_sha256",
        "public_task_binding_sha256",
        "representation_id",
        "semantic_input_sha256",
    },
    "N7": {
        "artifact_sha256",
        "artifact_size_bytes",
        "cache_id",
        "initial_miss",
        "node_id",
        "persistent_reopen_hit",
        "persistent_state_verified",
        "representation_id",
        "stored",
    },
    "N8": {
        "artifact_sha256",
        "artifact_size_bytes",
        "cache_id",
        "initial_miss",
        "node_id",
        "persistent_reopen_hit",
        "persistent_state_verified",
        "representation_id",
        "stored",
    },
}


def _verify_checksums(root: Path) -> None:
    expected = {EVIDENCE_NAME, PUBLIC_TASK_NAME}
    actual_files = {
        path.name for path in root.iterdir() if path.is_file()
    }
    _require(
        actual_files == expected | {CHECKSUMS_NAME},
        "harness output file set changed",
    )
    try:
        raw = (root / CHECKSUMS_NAME).read_text(encoding="utf-8")
    except OSError as exc:
        raise LocalFullFlowHarnessError("cannot read harness checksums") from exc
    rows: dict[str, str] = {}
    for line in raw.splitlines():
        digest, separator, name = line.partition("  ")
        _require(separator == "  " and name in expected, "malformed checksums")
        _digest(digest, "checksum")
        _require(name not in rows, "duplicate checksum entry")
        rows[name] = digest
    _require(set(rows) == expected, "harness checksums are incomplete")
    canonical = "".join(
        f"{rows[name]}  {name}\n" for name in sorted(rows)
    )
    _require(raw == canonical, "harness checksums are not canonical")
    for name in expected:
        _require(
            _sha256((root / name).read_bytes()) == rows[name],
            f"harness checksum mismatch: {name}",
        )


def verify_local_full_flow_harness(
    output_dir: str | Path,
) -> dict[str, Any]:
    """Verify portable evidence and every recorded cross-node content chain."""

    root = Path(output_dir).resolve()
    _require(root.is_dir(), "harness output directory does not exist")
    _require(
        all(not path.is_symlink() for path in root.iterdir()),
        "harness output contains a symbolic link",
    )
    _verify_checksums(root)
    task_raw, task_value = _read_json(root / PUBLIC_TASK_NAME, PUBLIC_TASK_NAME)
    evidence_raw, evidence = _read_json(root / EVIDENCE_NAME, EVIDENCE_NAME)
    _require(
        task_raw == _document_bytes(task_value),
        "public task document is not canonical",
    )
    _require(
        evidence_raw == _document_bytes(evidence),
        "harness evidence is not canonical",
    )
    public_task = _validate_public_task(task_value)
    _require(set(evidence) == _TOP_LEVEL_FIELDS, "evidence field set changed")
    _require(
        evidence["schema_version"] == LOCAL_FULL_FLOW_HARNESS_SCHEMA_VERSION
        and evidence["status"] == LOCAL_FULL_FLOW_HARNESS_STATUS
        and evidence["evidence_class"]
        == "local-component-composition-single-public-task",
        "harness evidence schema, status, or class changed",
    )
    _identifier(evidence["harness_id"], "harness_id")
    _identifier(evidence["object_id"], "object_id")
    _identifier(evidence["task_class_id"], "task_class_id")
    _require(
        evidence["object_id"] == public_task["object_id"]
        and evidence["task_class_id"] == public_task["task_class_id"]
        and evidence["public_task_binding_sha256"]
        == public_task["task_binding_sha256"],
        "public task binding changed",
    )
    recorded_sha = _digest(evidence["evidence_sha256"], "evidence_sha256")
    unsigned = dict(evidence)
    del unsigned["evidence_sha256"]
    _require(recorded_sha == _value_sha256(unsigned), "evidence digest mismatch")
    _require(
        evidence["logical_stage_order"] == _STAGE_ORDER
        and evidence["logical_nodes_exercised"] == _LOGICAL_NODES,
        "logical node coverage or stage order changed",
    )
    components = evidence["component_bindings"]
    _require(
        isinstance(components, dict) and set(components) == set(_LOGICAL_NODES),
        "component binding set changed",
    )
    for node_id in _LOGICAL_NODES:
        value = components[node_id]
        _require(
            isinstance(value, dict)
            and set(value) == _COMPONENT_FIELDS[node_id]
            and value.get("node_id") == node_id,
            f"{node_id} component identity changed",
        )

    n1 = components["N1"]
    n2 = components["N2"]
    n3 = components["N3"]
    n4 = components["N4"]
    n5 = components["N5"]
    n6 = components["N6"]
    n7 = components["N7"]
    n8 = components["N8"]
    digest_fields = (
        (n1, "oracle_package_sha256"),
        (n1, "public_task_set_sha256"),
        (n1, "score_request_sha256"),
        (n1, "prediction_sha256"),
        (n1, "score_result_sha256"),
        (n1, "score_evidence_hmac_sha256"),
        (n2, "index_package_sha256"),
        (n2, "index_sha256"),
        (n2, "query_request_sha256"),
        (n2, "query_result_sha256"),
        (n2, "ranking_sha256"),
        (n3, "raw_package_sha256"),
        (n3, "artifact_sha256"),
        (n4, "package_sha256"),
        (n4, "publication_receipt_sha256"),
        (n4, "artifact_sha256"),
        (n5, "plan_sha256"),
        (n5, "transformation_contract_sha256"),
        (n5, "artifact_sha256"),
        (n6, "semantic_input_sha256"),
        (n6, "prediction_sha256"),
        (n7, "artifact_sha256"),
        (n8, "artifact_sha256"),
    )
    for owner, field_name in digest_fields:
        _digest(owner.get(field_name), field_name)

    artifact_sha = n3["artifact_sha256"]
    derived_sha = n5["artifact_sha256"]
    _require(
        n4["artifact_sha256"] == derived_sha
        and n7["artifact_sha256"] == derived_sha
        and n8["artifact_sha256"] == derived_sha,
        "N5-N4-N7-N8 derived content chain changed",
    )
    _require(
        n5["artifact_size_bytes"] == n4["artifact_size_bytes"]
        == n7["artifact_size_bytes"] == n8["artifact_size_bytes"],
        "N5-N4-N7-N8 derived size chain changed",
    )
    _positive_integer(n3["artifact_size_bytes"], "N3 artifact size")
    _positive_integer(n5["artifact_size_bytes"], "derived artifact size")
    for owner, field_name in (
        (n1, "oracle_package_sha256"),
        (n2, "selected_object_id"),
        (n2, "index_sha256"),
        (n3, "catalog_version"),
        (n4, "catalog_version"),
        (n5, "sampler_id"),
        (n6, "adapter_id"),
        (n7, "cache_id"),
        (n8, "cache_id"),
    ):
        if field_name.endswith("sha256"):
            _digest(owner[field_name], field_name)
        else:
            _identifier(owner[field_name], field_name)
    _require(
        n2["selected_object_id"] == evidence["object_id"],
        "N2 selection differs from the public task",
    )
    _require(
        n6["public_task_binding_sha256"]
        == evidence["public_task_binding_sha256"]
        and n6["prediction_sha256"] == n1["prediction_sha256"],
        "N6-to-N1 scoring binding changed",
    )
    _require(
        n3["representation_id"] == RAW_VIDEO_REPRESENTATION_ID
        and n4["representation_id"] == FRAME_BUNDLE_REPRESENTATION_ID
        and n5["representation_id"] == FRAME_BUNDLE_REPRESENTATION_ID
        and n6["representation_id"] == FRAME_BUNDLE_REPRESENTATION_ID
        and n7["representation_id"] == FRAME_BUNDLE_REPRESENTATION_ID
        and n8["representation_id"] == FRAME_BUNDLE_REPRESENTATION_ID,
        "representation chain changed",
    )
    for key, expected in (
        ("hidden_answer_returned", False),
        ("content_binding_verified", True),
        ("component_composition_verified", True),
        ("semantic_adapter_invoked", True),
        ("semantic_task_executed", False),
        ("semantic_quality_evaluated", False),
        ("n1_trial_control_exercised", False),
        ("http_service_boundaries_exercised", False),
        ("route_policy_exercised", False),
        ("external_attestation_verified", False),
        ("self_authenticating", False),
        ("single_public_task_only", True),
        ("four_by_eight_semantic_coverage_verified", False),
        ("real_performance_measured", False),
        ("real_cost_measured", False),
        ("deployment_binding_included", False),
        ("docker_used", False),
        ("flowmesh_used", False),
        ("external_network_called", False),
        ("llm_called", False),
        ("credentials_recorded", False),
        ("eligible_for_scientific_claims", False),
    ):
        _require(evidence[key] is expected, f"evidence safety flag changed: {key}")
    _require(
        evidence["semantic_adapter_mode"] == "injected-offline-test-adapter",
        "semantic adapter mode changed",
    )
    _require(
        type(evidence["media_decode_exercised"]) is bool
        and evidence["media_decode_exercised"]
        is n5["media_decode_exercised"],
        "media decode attestation changed",
    )
    _require(
        type(evidence["task_success"]) is bool
        and evidence["score"]
        == (1.0 if evidence["task_success"] else 0.0),
        "task success and score disagree",
    )
    _require(
        n1["hidden_answer_returned"] is False
        and n2["lexical_retrieval_executed"] is True
        and n3["data_agent_contract_used"] is True
        and n4["data_agent_contract_used"] is True
        and n4["atomic_publication_verified"] is True
        and n5["llm_called"] is False
        and n5["canonical_frame_bundle_verified"] is True
        and n6["llm_called"] is False
        and n6["external_services_called"] is False,
        "component execution attestations changed",
    )
    for cache in (n7, n8):
        _require(
            cache["initial_miss"] is True
            and cache["stored"] is True
            and cache["persistent_reopen_hit"] is True
            and cache["persistent_state_verified"] is True,
            "cache persistence evidence changed",
        )
    _require(artifact_sha != derived_sha, "raw and derived identities collapsed")
    assert_hidden_oracle_fields_absent(public_task)
    assert_hidden_oracle_fields_absent(evidence)
    _assert_portable_public_value(public_task, "public task")
    _assert_portable_public_value(evidence)
    return {
        "status": "STRUCTURALLY_VERIFIED",
        "harness_id": evidence["harness_id"],
        "object_id": evidence["object_id"],
        "logical_node_count": len(_LOGICAL_NODES),
        "task_success": None,
        "score": None,
        "reported_task_success": evidence["task_success"],
        "reported_score": evidence["score"],
        "oracle_hmac_verified": False,
        "media_decode_exercised": evidence["media_decode_exercised"],
        "component_composition_verified": True,
        "semantic_quality_evaluated": False,
        "http_service_boundaries_exercised": False,
        "external_attestation_verified": False,
        "self_authenticating": False,
        "single_public_task_only": True,
        "four_by_eight_semantic_coverage_verified": False,
        "real_performance_measured": False,
        "real_cost_measured": False,
        "external_network_called": False,
        "llm_called": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


__all__ = [
    "AttestedFrameSampler",
    "CHECKSUMS_NAME",
    "EVIDENCE_NAME",
    "LOCAL_FULL_FLOW_HARNESS_SCHEMA_VERSION",
    "LOCAL_FULL_FLOW_HARNESS_STATUS",
    "LocalFullFlowHarnessError",
    "N6SemanticAdapter",
    "N6SemanticInput",
    "N6SemanticResult",
    "PUBLIC_TASK_NAME",
    "run_local_full_flow_harness",
    "verify_local_full_flow_harness",
]
