"""Small, auditable semantic executions on an already-running local cluster.

This layer deliberately sits beside the infrastructure-only container runner.
It sends a real, bounded text representation to an explicitly enabled
container-side LLM executor, scores the returned answer with Pathfinder's
frozen multiple-choice rule, and writes only hashes of the prompt and source
representation.  It does *not* claim that the host-to-container prompt upload
reproduces the frozen data-plane route; that coupling requires a later,
artifact-backed container adapter.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from hashlib import sha256
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Mapping
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from pathfinder.distributed.scoring import (
    MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
    MULTIPLE_CHOICE_EXACT_SCORING_RULE,
    WorkloadScoringContract,
    WorkloadScoringError,
    evaluate_workload_answer,
    load_workload_scoring_contract,
    render_workload_question,
)

from .container_node import (
    CONTAINER_NODE_SEMANTIC_REQUEST_SCHEMA_VERSION,
    CONTAINER_NODE_SEMANTIC_RESULT_SCHEMA_VERSION,
    ContainerNodeRuntime,
)
from .local_container import verify_local_container_compose


SEMANTIC_WORKLOAD_MANIFEST_SCHEMA_VERSION = (
    "pathfinder.local-container-semantic-workloads/v1alpha1"
)
SEMANTIC_RECORD_SCHEMA_VERSION = (
    "pathfinder.local-container-semantic-record/v1alpha1"
)
SEMANTIC_RUN_MANIFEST_SCHEMA_VERSION = (
    "pathfinder.local-container-semantic-run/v1alpha1"
)
SEMANTIC_SCORE_ALIGNMENT_SCHEMA_VERSION = (
    "pathfinder.local-container-semantic-score-alignment/v1alpha1"
)

_OUTPUT_FILES = {"semantic_records.jsonl", "semantic_run_manifest.json"}
_ALIGNMENT_OUTPUT_FILES = {"semantic_score_alignment.json"}
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAX_REPRESENTATION_BYTES = 1024 * 1024
_SEMANTIC_SCORING_RULES = frozenset({
    MULTIPLE_CHOICE_EXACT_SCORING_RULE,
    MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
})


class SemanticExecutionError(RuntimeError):
    """Raised when a local semantic execution is malformed or unsafe."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise SemanticExecutionError(message)


def _text(value: Any, name: str) -> str:
    _require(isinstance(value, str) and bool(value.strip()), f"{name} must be text")
    return value.strip()


def _sha256_bytes(value: bytes) -> str:
    return sha256(value).hexdigest()


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _json_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False)
        + "\n"
    ).encode("utf-8")


def _jsonl_bytes(values: Iterable[Mapping[str, Any]]) -> bytes:
    return b"".join(_canonical_bytes(value) + b"\n" for value in values)


def _read_json(path: Path, name: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SemanticExecutionError(f"cannot read valid {name}: {path}") from exc


def _safe_relative_path(value: Any, name: str) -> PurePosixPath:
    text = _text(value, name).replace("\\", "/")
    path = PurePosixPath(text)
    _require(not path.is_absolute(), f"{name} must be relative")
    _require(".." not in path.parts and "." not in path.parts, f"{name} escapes root")
    _require(bool(path.parts), f"{name} is empty")
    return path


def _safe_file(root: Path, relative: PurePosixPath) -> Path:
    candidate = (root / Path(*relative.parts)).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise SemanticExecutionError("representation path escapes root") from exc
    _require(candidate.is_file(), f"representation file is missing: {relative.as_posix()}")
    return candidate


def _load_semantic_workloads(path: str | Path) -> tuple[bytes, dict[str, Any], list[dict[str, Any]]]:
    source = Path(path).resolve()
    try:
        raw = source.read_bytes()
        document = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SemanticExecutionError("semantic workload manifest is invalid") from exc
    _require(isinstance(document, Mapping), "semantic workload manifest must be an object")
    document = dict(document)
    _require(
        document.get("schema_version") == SEMANTIC_WORKLOAD_MANIFEST_SCHEMA_VERSION,
        "unsupported semantic workload manifest schema_version",
    )
    _text(document.get("semantic_run_id"), "semantic_run_id")
    scoring_rule = document.get("success_scoring_rule")
    _require(
        scoring_rule in _SEMANTIC_SCORING_RULES,
        "semantic workloads require a supported single-option scoring rule",
    )
    _text(document.get("semantic_executor_node_id"), "semantic_executor_node_id")
    workloads = document.get("workloads")
    _require(isinstance(workloads, list) and bool(workloads), "workloads must be a non-empty array")
    normalized: list[dict[str, Any]] = []
    seen_trial_keys: set[str] = set()
    for index, raw_workload in enumerate(workloads):
        _require(isinstance(raw_workload, Mapping), f"workloads[{index}] must be an object")
        workload = dict(raw_workload)
        trial_key = _text(workload.get("semantic_trial_key"), f"workloads[{index}].semantic_trial_key")
        _require(trial_key not in seen_trial_keys, f"duplicate semantic_trial_key: {trial_key}")
        seen_trial_keys.add(trial_key)
        _text(workload.get("workload_id"), f"workloads[{index}].workload_id")
        _text(workload.get("object_id"), f"workloads[{index}].object_id")
        _text(workload.get("design_id"), f"workloads[{index}].design_id")
        _text(workload.get("representation_id"), f"workloads[{index}].representation_id")
        _safe_relative_path(workload.get("representation_path"), f"workloads[{index}].representation_path")
        try:
            load_workload_scoring_contract(
                workload,
                scoring_rule,
                name=f"workloads[{index}]",
            )
        except WorkloadScoringError as exc:
            raise SemanticExecutionError(str(exc)) from exc
        normalized.append(workload)
    return raw, document, normalized


def _request_json(url: str, payload: Mapping[str, Any], timeout_seconds: float) -> dict[str, Any]:
    body = _canonical_bytes(payload)
    request = Request(
        url,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            raw = response.read(2 * 1024 * 1024 + 1)
    except HTTPError as exc:
        raise SemanticExecutionError(
            f"semantic container endpoint returned HTTP {exc.code}"
        ) from exc
    except (URLError, TimeoutError, OSError) as exc:
        raise SemanticExecutionError(
            f"semantic container endpoint failed: {type(exc).__name__}"
        ) from exc
    _require(len(raw) <= 2 * 1024 * 1024, "semantic container response is too large")
    try:
        response = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise SemanticExecutionError("semantic container response is invalid") from exc
    _require(isinstance(response, dict), "semantic container response must be an object")
    return response


def _get_json(url: str, timeout_seconds: float) -> dict[str, Any]:
    try:
        with urlopen(url, timeout=timeout_seconds) as response:
            raw = response.read(64 * 1024)
    except (HTTPError, URLError, TimeoutError, OSError) as exc:
        raise SemanticExecutionError(
            f"semantic executor health check failed: {type(exc).__name__}"
        ) from exc
    try:
        response = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise SemanticExecutionError("semantic executor health is invalid") from exc
    _require(isinstance(response, dict), "semantic executor health must be an object")
    return response


def _render_prompt(representation_id: str, representation_text: str, question: str) -> str:
    return ContainerNodeRuntime.build_semantic_prompt(
        representation_id,
        representation_text,
        question,
    )


def _checksum_bytes(documents: Mapping[str, bytes]) -> bytes:
    return b"".join(
        f"{_sha256_bytes(content)}  {name}\n".encode("utf-8")
        for name, content in sorted(documents.items())
    )


def execute_local_container_semantic_run(
    compose_package_dir: str | Path,
    semantic_workload_manifest: str | Path,
    representation_root: str | Path,
    *,
    output_dir: str | Path,
    request_timeout_seconds: float = 240.0,
    max_representation_bytes: int = _MAX_REPRESENTATION_BYTES,
) -> dict[str, Any]:
    """Run bounded, scored text tasks through one enabled container executor.

    The function intentionally creates a fresh output directory only.  API
    requests are non-deterministic and potentially billable, so automatic
    resume/replay is not safe without a separately frozen response cache.
    """

    _require(request_timeout_seconds > 0.0, "request timeout must be positive")
    _require(
        type(max_representation_bytes) is int and max_representation_bytes > 0,
        "max_representation_bytes must be a positive integer",
    )
    compose_root = Path(compose_package_dir).resolve()
    compose = verify_local_container_compose(compose_root)
    _require(compose.get("semantic_quality_enabled") is True, "Compose package has no semantic executor")
    raw_manifest, manifest, workloads = _load_semantic_workloads(semantic_workload_manifest)
    scoring_rule = str(manifest["success_scoring_rule"])
    executor_node_id = str(manifest["semantic_executor_node_id"])
    _require(
        compose.get("semantic_executor_node_id") == executor_node_id,
        "semantic workload and Compose executor nodes differ",
    )
    root = Path(representation_root).resolve()
    _require(root.is_dir(), "representation root does not exist")
    endpoint_document = _read_json(compose_root / "container_endpoints.json", "container endpoints")
    _require(isinstance(endpoint_document, Mapping), "container endpoints must be an object")
    endpoints = endpoint_document.get("endpoints")
    _require(isinstance(endpoints, Mapping), "container endpoints are invalid")
    executor_endpoint = endpoints.get(executor_node_id)
    _require(isinstance(executor_endpoint, Mapping), "semantic executor endpoint is missing")
    health_url = _text(executor_endpoint.get("host_health_url"), "semantic executor health URL")
    semantic_url = _text(executor_endpoint.get("host_semantic_url"), "semantic executor URL")
    health = _get_json(health_url, min(request_timeout_seconds, 10.0))
    _require(health.get("status") == "ok", "semantic executor is unhealthy")
    _require(health.get("node_id") == executor_node_id, "semantic executor node changed")
    _require(health.get("semantic_quality_enabled") is True, "semantic executor is disabled")
    _require(health.get("semantic_llm_configured") is True, "semantic LLM runtime configuration is incomplete")
    raw_source_node_ids = compose.get("semantic_artifact_source_node_ids", [])
    _require(isinstance(raw_source_node_ids, list), "semantic artifact source nodes are invalid")
    route_coupled = bool(raw_source_node_ids)
    source_node_ids = set(raw_source_node_ids)
    if route_coupled:
        for source_node_id in source_node_ids:
            source_endpoint = endpoints.get(source_node_id)
            _require(
                isinstance(source_endpoint, Mapping),
                f"semantic source endpoint is missing: {source_node_id}",
            )
            source_health = _get_json(
                _text(source_endpoint.get("host_health_url"), "semantic source health URL"),
                min(request_timeout_seconds, 10.0),
            )
            _require(
                source_health.get("status") == "ok"
                and source_health.get("node_id") == source_node_id
                and source_health.get("semantic_artifact_serving") is True,
                f"semantic source is not ready: {source_node_id}",
            )

    target = Path(output_dir).resolve()
    _require(not target.exists(), "semantic output directory already exists")
    records: list[dict[str, Any]] = []
    for index, workload in enumerate(workloads):
        relative = _safe_relative_path(workload["representation_path"], "representation_path")
        representation_path = _safe_file(root, relative)
        raw_representation = representation_path.read_bytes()
        _require(
            len(raw_representation) <= max_representation_bytes,
            f"representation exceeds byte limit: {relative.as_posix()}",
        )
        try:
            representation_text = raw_representation.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise SemanticExecutionError(
                f"representation is not UTF-8 text: {relative.as_posix()}"
            ) from exc
        contract = load_workload_scoring_contract(
            workload,
            scoring_rule,
            name=f"workloads[{index}]",
        )
        question = render_workload_question(workload, contract)
        prompt = _render_prompt(
            str(workload["representation_id"]),
            representation_text,
            question,
        )
        prompt_sha256 = _sha256_bytes(prompt.encode("utf-8"))
        representation_sha256 = _sha256_bytes(raw_representation)
        request_id = _sha256_bytes(
            _canonical_bytes(
                {
                    "semantic_run_id": manifest["semantic_run_id"],
                    "semantic_trial_key": workload["semantic_trial_key"],
                    "prompt_sha256": prompt_sha256,
                    "representation_sha256": representation_sha256,
                    "execution_node_id": executor_node_id,
                }
            )
        )
        request: dict[str, Any] = {
            "schema_version": CONTAINER_NODE_SEMANTIC_REQUEST_SCHEMA_VERSION,
            "semantic_request_id": request_id,
            "execution_node_id": executor_node_id,
            "prompt_sha256": prompt_sha256,
            "representation_sha256": representation_sha256,
        }
        source_node_id: str | None = None
        if route_coupled:
            source_node_id = _text(
                workload.get("source_node_id"),
                f"workloads[{index}].source_node_id",
            )
            _require(
                source_node_id in source_node_ids,
                f"semantic workload source node is not enabled: {source_node_id}",
            )
            source_endpoint = endpoints[source_node_id]
            request.update({
                "source_node_id": source_node_id,
                "source_container_url": _text(
                    source_endpoint.get("container_url"),
                    "semantic source container URL",
                ),
                "representation_path": relative.as_posix(),
                "representation_id": workload["representation_id"],
                "question": question,
            })
        else:
            request["prompt"] = prompt
        response = _request_json(semantic_url, request, request_timeout_seconds)
        _require(
            response.get("schema_version") == CONTAINER_NODE_SEMANTIC_RESULT_SCHEMA_VERSION,
            "semantic executor returned an unsupported result schema",
        )
        _require(response.get("status") == "completed", "semantic executor did not complete")
        _require(response.get("outcome_type") == "completed", "semantic executor outcome is not completed")
        _require(response.get("telemetry_complete") is True, "semantic executor telemetry is incomplete")
        _require(response.get("credentials_recorded") is False, "semantic executor recorded credentials")
        _require(response.get("llm_called") is True, "semantic executor did not call the LLM")
        _require(response.get("semantic_request_id") == request_id, "semantic request ID changed")
        _require(response.get("execution_node_id") == executor_node_id, "semantic executor node changed")
        _require(response.get("prompt_sha256") == prompt_sha256, "semantic prompt digest changed")
        _require(
            response.get("representation_sha256") == representation_sha256,
            "semantic representation digest changed",
        )
        _require(
            response.get("data_plane_artifact_delivery_verified") is route_coupled,
            "semantic artifact delivery mode changed",
        )
        if route_coupled:
            _require(
                response.get("source_node_id") == source_node_id,
                "semantic source node changed",
            )
            _require(
                response.get("representation_delivery_bytes") == len(raw_representation),
                "semantic representation delivery byte count changed",
            )
        answer = response.get("final_answer")
        _require(isinstance(answer, str), "semantic executor answer is not text")
        _require(
            response.get("final_answer_sha256") == _sha256_bytes(answer.encode("utf-8")),
            "semantic answer digest changed",
        )
        success = evaluate_workload_answer(answer, contract)
        _require(type(success) is bool, "semantic exact-match score is not boolean")
        records.append(
            {
                "schema_version": SEMANTIC_RECORD_SCHEMA_VERSION,
                "semantic_run_id": manifest["semantic_run_id"],
                "semantic_trial_key": workload["semantic_trial_key"],
                "workload_id": workload["workload_id"],
                "object_id": workload["object_id"],
                "design_id": workload["design_id"],
                "representation_id": workload["representation_id"],
                "representation_sha256": representation_sha256,
                "prompt_sha256": prompt_sha256,
                "success_scoring_rule": contract.rule,
                "correct_answer_id": contract.correct_answer_id,
                "final_answer": answer,
                "final_answer_sha256": response["final_answer_sha256"],
                "task_success": success,
                "execution_node_id": executor_node_id,
                "model": response.get("model"),
                "llm_service_time_ms": response.get("service_time_ms"),
                "llm_called": True,
                "telemetry_complete": True,
                "host_content_loaded": True,
                "data_plane_artifact_delivery_verified": route_coupled,
                "source_node_id": source_node_id,
                "representation_delivery_bytes": (
                    len(raw_representation) if route_coupled else None
                ),
                "credentials_recorded": False,
                "eligible_for_scientific_claims": False,
            }
        )

    documents: dict[str, bytes] = {"semantic_records.jsonl": _jsonl_bytes(records)}
    semantic_manifest = {
        "schema_version": SEMANTIC_RUN_MANIFEST_SCHEMA_VERSION,
        "status": "COMPLETE_SEMANTIC_LOCAL",
        "semantic_run_id": manifest["semantic_run_id"],
        "semantic_workload_manifest_sha256": _sha256_bytes(raw_manifest),
        "semantic_executor_node_id": executor_node_id,
        "semantic_workload_count": len(records),
        "success_scoring_rule": scoring_rule,
        "task_success_count": sum(record["task_success"] for record in records),
        "task_accuracy": sum(record["task_success"] for record in records) / len(records),
        "models": sorted({str(record["model"]) for record in records}),
        "representation_content_loaded_by": (
            "source-container-over-docker-network"
            if route_coupled
            else "host-runner"
        ),
        "semantic_inference_executed_by": "container-node",
        "data_plane_artifact_delivery_verified": route_coupled,
        "configured_cost_evaluated": False,
        "llm_called": True,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
        "limitations": [
            (
                "The selected source container delivered the bounded frozen text representation to the executor over the Docker network."
                if route_coupled
                else "The host runner reads the frozen text representation and sends a bounded prompt to the selected container executor."
            ),
            (
                "The semantic source route is independently bound by node IDs, representation digest, and exact delivery bytes; it is not yet proven equivalent to every infrastructure-plan operation route."
                if route_coupled
                else "This validates real representation-to-LLM-to-score execution, but does not yet bind semantic artifact delivery to the simulated storage and network operation route."
            ),
            "Remote-model nondeterminism and provider-side queueing are not controlled by this local container run.",
            "Configured rate-card costs are excluded; this run records no physical monetary cost.",
        ],
        "output_sha256": {
            name: _sha256_bytes(content) for name, content in sorted(documents.items())
        },
    }
    documents["semantic_run_manifest.json"] = _json_bytes(semantic_manifest)
    documents["SHA256SUMS"] = _checksum_bytes(documents)

    target.parent.mkdir(parents=True, exist_ok=True)
    staging_parent = Path(tempfile.mkdtemp(prefix=".semantic-", dir=target.parent))
    staging = staging_parent / "output"
    try:
        staging.mkdir()
        for name, content in documents.items():
            (staging / name).write_bytes(content)
        verify_local_container_semantic_run(staging)
        os.replace(staging, target)
    finally:
        for child in staging_parent.glob("**/*"):
            if child.is_file():
                child.unlink()
        for child in sorted(staging_parent.glob("**/*"), reverse=True):
            if child.is_dir():
                child.rmdir()
        staging_parent.rmdir()
    return {**semantic_manifest, "output_dir": str(target)}


def verify_local_container_semantic_run(output_dir: str | Path) -> dict[str, Any]:
    """Verify a semantic-run output without API or container access."""

    root = Path(output_dir).resolve()
    _require(root.is_dir(), "semantic output directory is missing")
    actual = {path.name for path in root.iterdir() if path.is_file()}
    _require(actual == _OUTPUT_FILES | {"SHA256SUMS"}, "semantic output file set changed")
    checksums: dict[str, str] = {}
    for line in (root / "SHA256SUMS").read_text(encoding="utf-8").splitlines():
        digest, separator, name = line.partition("  ")
        _require(separator == "  " and name in _OUTPUT_FILES, "malformed semantic checksum")
        _require(_SHA256.fullmatch(digest) is not None, "semantic checksum is invalid")
        _require(name not in checksums, "duplicate semantic checksum")
        _require(_sha256_bytes((root / name).read_bytes()) == digest, "semantic checksum mismatch")
        checksums[name] = digest
    _require(set(checksums) == _OUTPUT_FILES, "semantic checksums are incomplete")
    manifest = _read_json(root / "semantic_run_manifest.json", "semantic run manifest")
    _require(isinstance(manifest, Mapping), "semantic run manifest must be an object")
    _require(
        manifest.get("schema_version") == SEMANTIC_RUN_MANIFEST_SCHEMA_VERSION,
        "unsupported semantic run manifest schema_version",
    )
    _require(manifest.get("status") == "COMPLETE_SEMANTIC_LOCAL", "semantic run is incomplete")
    _require(manifest.get("credentials_recorded") is False, "semantic run recorded credentials")
    _require(manifest.get("llm_called") is True, "semantic run did not call the LLM")
    scoring_rule = manifest.get("success_scoring_rule")
    _require(
        scoring_rule is None or scoring_rule in _SEMANTIC_SCORING_RULES,
        "semantic run scoring rule is unsupported",
    )
    route_coupled = manifest.get("data_plane_artifact_delivery_verified")
    _require(type(route_coupled) is bool, "semantic delivery mode is invalid")
    _require(manifest.get("output_sha256") == {"semantic_records.jsonl": checksums["semantic_records.jsonl"]}, "semantic output digests disagree")
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate((root / "semantic_records.jsonl").read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise SemanticExecutionError(f"semantic record {line_number} is invalid") from exc
        _require(isinstance(record, dict), "semantic record must be an object")
        _require(record.get("schema_version") == SEMANTIC_RECORD_SCHEMA_VERSION, "semantic record schema changed")
        _require(
            record.get("semantic_run_id") == manifest.get("semantic_run_id"),
            "semantic record run ID changed",
        )
        for field in (
            "semantic_trial_key",
            "workload_id",
            "object_id",
            "design_id",
            "representation_id",
            "correct_answer_id",
            "final_answer",
            "execution_node_id",
            "model",
        ):
            _text(record.get(field), f"semantic record {field}")
        record_scoring_rule = record.get("success_scoring_rule")
        _require(
            record_scoring_rule in _SEMANTIC_SCORING_RULES,
            "semantic record scoring rule is unsupported",
        )
        if scoring_rule is None:
            # v1alpha1 semantic outputs predate a run-level rule field.  The
            # rule is still present in every record, so infer it only when all
            # records agree instead of reinterpreting an old result.
            scoring_rule = record_scoring_rule
        _require(record_scoring_rule == scoring_rule, "semantic record scoring rule changed")
        _require(type(record.get("task_success")) is bool, "semantic task score is invalid")
        _require(record.get("telemetry_complete") is True, "semantic record telemetry is incomplete")
        _require(record.get("llm_called") is True, "semantic record did not call the LLM")
        _require(record.get("credentials_recorded") is False, "semantic record recorded credentials")
        _require(
            record.get("data_plane_artifact_delivery_verified") is route_coupled,
            "semantic record delivery mode changed",
        )
        if route_coupled:
            _text(record.get("source_node_id"), "semantic record source_node_id")
            _require(
                type(record.get("representation_delivery_bytes")) is int
                and record["representation_delivery_bytes"] > 0,
                "semantic representation delivery bytes are invalid",
            )
        else:
            _require(record.get("source_node_id") is None, "direct semantic record has a source node")
            _require(
                record.get("representation_delivery_bytes") is None,
                "direct semantic record has delivery bytes",
            )
        service_time = record.get("llm_service_time_ms")
        _require(
            type(service_time) in (int, float) and type(service_time) is not bool and service_time >= 0.0,
            "semantic LLM service time is invalid",
        )
        for field in ("representation_sha256", "prompt_sha256", "final_answer_sha256"):
            _require(_SHA256.fullmatch(str(record.get(field))) is not None, f"semantic {field} is invalid")
        _require(
            record["final_answer_sha256"] == _sha256_bytes(str(record.get("final_answer")).encode("utf-8")),
            "semantic answer digest changed",
        )
        expected_success = evaluate_workload_answer(
            str(record["final_answer"]),
            WorkloadScoringContract(
                rule=str(scoring_rule),
                correct_answer_id=str(record["correct_answer_id"]),
            ),
        )
        _require(
            record["task_success"] is expected_success,
            "semantic record task score disagrees with its frozen rule",
        )
        forbidden = {
            key
            for key in record
            if key in {"prompt", "representation_text"}
            or any(token in key.casefold() for token in ("api_key", "authorization", "secret"))
        }
        _require(not forbidden, "semantic record leaks protected input or credential fields")
        records.append(record)
    _require(len(records) == manifest.get("semantic_workload_count"), "semantic record count changed")
    _require(bool(records), "semantic run has no records")
    _require(scoring_rule in _SEMANTIC_SCORING_RULES, "semantic run lacks a scoring rule")
    _require(len({record["semantic_trial_key"] for record in records}) == len(records), "semantic trial keys are not unique")
    _require(
        manifest.get("task_success_count") == sum(record["task_success"] for record in records),
        "semantic success count changed",
    )
    _require(
        manifest.get("task_accuracy") == sum(record["task_success"] for record in records) / len(records),
        "semantic accuracy changed",
    )
    return {
        "status": "VERIFIED_OFFLINE",
        "semantic_run_id": manifest["semantic_run_id"],
        "semantic_workload_count": len(records),
        "task_accuracy": manifest["task_accuracy"],
        "semantic_inference_executed_by": manifest["semantic_inference_executed_by"],
        "success_scoring_rule": scoring_rule,
        "data_plane_artifact_delivery_verified": route_coupled,
        "eligible_for_scientific_claims": False,
    }


def _load_semantic_records(root: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    try:
        lines = (root / "semantic_records.jsonl").read_text(
            encoding="utf-8"
        ).splitlines()
    except (OSError, UnicodeError) as exc:
        raise SemanticExecutionError("semantic records are unreadable") from exc
    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise SemanticExecutionError(
                f"semantic record {line_number} is invalid"
            ) from exc
        _require(isinstance(record, dict), "semantic record must be an object")
        records.append(record)
    _require(bool(records), "semantic run has no records")
    return records


def _alignment_checksum_bytes(documents: Mapping[str, bytes]) -> bytes:
    return _checksum_bytes(documents)


def align_local_container_semantic_scores(
    semantic_output_dir: str | Path,
    semantic_workload_manifest: str | Path,
    *,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Derive a new, audit-safe canonical-marker score from a legacy smoke.

    This is deliberately score-only: it never resubmits an LLM request, never
    changes the original execution output, and accepts only an exact-match
    source run plus a separately supplied canonical-marker workload manifest.
    """

    source = Path(semantic_output_dir).resolve()
    source_report = verify_local_container_semantic_run(source)
    raw_workload_manifest, workload_document, workloads = _load_semantic_workloads(
        semantic_workload_manifest
    )
    _require(
        workload_document.get("success_scoring_rule")
        == MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
        "score alignment requires the canonical option-marker rule",
    )
    source_records = _load_semantic_records(source)
    source_rules = {record.get("success_scoring_rule") for record in source_records}
    _require(
        source_rules == {MULTIPLE_CHOICE_EXACT_SCORING_RULE},
        "score alignment requires an exact-match source run",
    )
    by_trial_key = {
        _text(record.get("semantic_trial_key"), "source semantic_trial_key"): record
        for record in source_records
    }
    _require(
        len(by_trial_key) == len(source_records),
        "source semantic trial keys are not unique",
    )
    workload_by_trial_key = {
        _text(workload.get("semantic_trial_key"), "alignment semantic_trial_key"): workload
        for workload in workloads
    }
    _require(
        set(by_trial_key) == set(workload_by_trial_key),
        "alignment workload trials differ from the source run",
    )

    rows: list[dict[str, Any]] = []
    for trial_key in sorted(by_trial_key):
        record = by_trial_key[trial_key]
        workload = workload_by_trial_key[trial_key]
        for field in ("workload_id", "object_id", "design_id", "representation_id"):
            _require(
                record.get(field) == workload.get(field),
                f"alignment {field} differs for {trial_key}",
            )
        try:
            contract = load_workload_scoring_contract(
                workload,
                MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
                name=f"alignment workload {trial_key}",
            )
        except WorkloadScoringError as exc:
            raise SemanticExecutionError(str(exc)) from exc
        answer = _text(record.get("final_answer"), "source final_answer")
        aligned_success = evaluate_workload_answer(answer, contract)
        _require(type(aligned_success) is bool, "canonical option score is invalid")
        rows.append({
            "semantic_trial_key": trial_key,
            "source_record_sha256": _sha256_bytes(_canonical_bytes(record)),
            "final_answer_sha256": _sha256_bytes(answer.encode("utf-8")),
            "correct_answer_id": contract.correct_answer_id,
            "previous_task_success": record["task_success"],
            "aligned_task_success": aligned_success,
            "matched_option_id": contract.correct_answer_id if aligned_success else None,
        })

    source_records_bytes = (source / "semantic_records.jsonl").read_bytes()
    source_manifest_bytes = (source / "semantic_run_manifest.json").read_bytes()
    document = {
        "schema_version": SEMANTIC_SCORE_ALIGNMENT_SCHEMA_VERSION,
        "status": "COMPLETE_SCORING_ALIGNMENT",
        "source_semantic_run_id": source_report["semantic_run_id"],
        "source_semantic_records_sha256": _sha256_bytes(source_records_bytes),
        "source_semantic_manifest_sha256": _sha256_bytes(source_manifest_bytes),
        "canonical_workload_manifest_sha256": _sha256_bytes(raw_workload_manifest),
        "source_scoring_rule": MULTIPLE_CHOICE_EXACT_SCORING_RULE,
        "applied_scoring_rule": MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
        "semantic_trial_count": len(rows),
        "previous_task_success_count": sum(row["previous_task_success"] for row in rows),
        "aligned_task_success_count": sum(row["aligned_task_success"] for row in rows),
        "aligned_task_accuracy": sum(row["aligned_task_success"] for row in rows) / len(rows),
        "rows": rows,
        "external_api_called": False,
        "source_result_modified": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    documents: dict[str, bytes] = {
        "semantic_score_alignment.json": _json_bytes(document),
    }
    documents["SHA256SUMS"] = _alignment_checksum_bytes(documents)
    target = Path(output_dir).resolve()
    _require(not target.exists(), "semantic score alignment output already exists")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging_parent = Path(tempfile.mkdtemp(prefix=".semantic-alignment-", dir=target.parent))
    staging = staging_parent / "output"
    try:
        staging.mkdir()
        for name, content in documents.items():
            (staging / name).write_bytes(content)
        verify_local_container_semantic_score_alignment(staging)
        os.replace(staging, target)
    finally:
        for child in staging_parent.glob("**/*"):
            if child.is_file():
                child.unlink()
        for child in sorted(staging_parent.glob("**/*"), reverse=True):
            if child.is_dir():
                child.rmdir()
        staging_parent.rmdir()
    return {**document, "output_dir": str(target)}


def verify_local_container_semantic_score_alignment(
    output_dir: str | Path,
) -> dict[str, Any]:
    """Verify a score-alignment artifact without API or container access."""

    root = Path(output_dir).resolve()
    _require(root.is_dir(), "semantic score alignment output is missing")
    actual = {path.name for path in root.iterdir() if path.is_file()}
    _require(
        actual == _ALIGNMENT_OUTPUT_FILES | {"SHA256SUMS"},
        "semantic score alignment file set changed",
    )
    digest, separator, name = (root / "SHA256SUMS").read_text(
        encoding="utf-8"
    ).strip().partition("  ")
    _require(
        separator == "  " and name == "semantic_score_alignment.json",
        "semantic score alignment checksum is malformed",
    )
    _require(_SHA256.fullmatch(digest) is not None, "semantic score alignment checksum is invalid")
    content = (root / name).read_bytes()
    _require(_sha256_bytes(content) == digest, "semantic score alignment checksum mismatch")
    document = _read_json(root / name, "semantic score alignment")
    _require(isinstance(document, Mapping), "semantic score alignment must be an object")
    _require(
        document.get("schema_version") == SEMANTIC_SCORE_ALIGNMENT_SCHEMA_VERSION,
        "semantic score alignment schema changed",
    )
    _require(document.get("status") == "COMPLETE_SCORING_ALIGNMENT", "semantic score alignment is incomplete")
    _require(document.get("source_scoring_rule") == MULTIPLE_CHOICE_EXACT_SCORING_RULE, "unexpected source scoring rule")
    _require(document.get("applied_scoring_rule") == MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE, "unexpected aligned scoring rule")
    _require(document.get("external_api_called") is False, "score alignment called an external API")
    _require(document.get("source_result_modified") is False, "score alignment modified source output")
    _require(document.get("credentials_recorded") is False, "score alignment recorded credentials")
    rows = document.get("rows")
    _require(isinstance(rows, list) and bool(rows), "semantic score alignment rows are invalid")
    for row in rows:
        _require(isinstance(row, Mapping), "semantic score alignment row is invalid")
        _text(row.get("semantic_trial_key"), "semantic score alignment trial key")
        for field in ("source_record_sha256", "final_answer_sha256"):
            _require(_SHA256.fullmatch(str(row.get(field))) is not None, f"semantic score alignment {field} is invalid")
        _text(row.get("correct_answer_id"), "semantic score alignment correct answer")
        _require(type(row.get("previous_task_success")) is bool, "previous semantic score is invalid")
        _require(type(row.get("aligned_task_success")) is bool, "aligned semantic score is invalid")
        if row["aligned_task_success"]:
            _require(row.get("matched_option_id") == row.get("correct_answer_id"), "aligned option ID changed")
        else:
            _require(row.get("matched_option_id") is None, "failed alignment has a matched option")
    _require(len(rows) == document.get("semantic_trial_count"), "semantic score alignment row count changed")
    _require(document.get("previous_task_success_count") == sum(row["previous_task_success"] for row in rows), "previous score count changed")
    _require(document.get("aligned_task_success_count") == sum(row["aligned_task_success"] for row in rows), "aligned score count changed")
    _require(document.get("aligned_task_accuracy") == sum(row["aligned_task_success"] for row in rows) / len(rows), "aligned accuracy changed")
    return {
        "status": "VERIFIED_OFFLINE",
        "source_semantic_run_id": document["source_semantic_run_id"],
        "semantic_trial_count": len(rows),
        "aligned_task_accuracy": document["aligned_task_accuracy"],
        "external_api_called": False,
        "eligible_for_scientific_claims": False,
    }
