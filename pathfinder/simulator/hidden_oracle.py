"""Hidden-label N1 scoring service for migration-equivalent simulation.

The frozen package is endpoint-free and contains the labels only on the N1
side.  Agent-facing and FlowMesh-facing payloads carry a public task binding,
never a correct answer.  N1 returns correctness, a numeric score, and opaque
hash/HMAC provenance without returning or echoing either answer.

Completed scoring requests are persisted in SQLite.  Reusing a request ID
with identical bytes is a durable replay.  Each frozen run/trial pair defines
one evaluation unit, and only one request may ever consume that unit even if a
caller changes the request ID.  Deployment addresses, bearer credentials, and
the HMAC secret are constructor-only inputs and are never written to the
package or database.
"""

from __future__ import annotations

import hashlib
import hmac
import ipaddress
import json
import os
import re
import shutil
import sqlite3
import tempfile
from contextlib import closing
from dataclasses import dataclass, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import (
    HTTPRedirectHandler,
    ProxyHandler,
    Request,
    build_opener,
)

from pathfinder.distributed.scoring import (
    MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
    MULTIPLE_CHOICE_EXACT_SCORING_RULE,
)


N1_PUBLIC_TASK_SCHEMA_VERSION = (
    "pathfinder.n1-public-task-binding/v1alpha1"
)
N1_LABEL_SOURCE_SCHEMA_VERSION = "pathfinder.n1-hidden-label-source/v1alpha1"
N1_ORACLE_PACKAGE_SCHEMA_VERSION = "pathfinder.n1-oracle-package/v1alpha1"
N1_SCORE_REQUEST_SCHEMA_VERSION = "pathfinder.n1-score-request/v1alpha2"
N1_SCORE_RESULT_SCHEMA_VERSION = "pathfinder.n1-score-result/v1alpha2"
N1_ORACLE_SERVICE_API_VERSION = "pathfinder.n1-oracle-service/v1alpha2"
N1_SCORE_STORE_SCHEMA_VERSION = "pathfinder.n1-oracle-score-store/v1alpha2"
N1_NODE_ID = "N1"

_LEGACY_SCORE_REQUEST_SCHEMA_VERSION = "pathfinder.n1-score-request/v1alpha1"
_LEGACY_SCORE_RESULT_SCHEMA_VERSION = "pathfinder.n1-score-result/v1alpha1"
_LEGACY_SCORE_STORE_SCHEMA_VERSION = "pathfinder.n1-score-result/v1alpha1"

_SCORING_RULES = frozenset(
    {
        MULTIPLE_CHOICE_EXACT_SCORING_RULE,
        MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
    }
)
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._|:+-]{0,255}")
_OPTION_ID = re.compile(r"[A-Z][A-Z0-9_-]{0,15}")
_CANONICAL_OPTION = re.compile(
    r"(?:([A-Z][A-Z0-9_-]{0,15})|"
    r"[\[\(［（]\s*([A-Z][A-Z0-9_-]{0,15})\s*[\]\)］）])\Z"
)
_SHA256 = re.compile(r"[0-9a-f]{64}")
_SIMULATOR_HOST = re.compile(
    r"(?:pathfinder-sim|pathfinder-full-flow)-[a-z0-9-]+"
)
_MAX_REQUEST_BYTES = 128 * 1024
_MAX_RESPONSE_BYTES = 512 * 1024
_MAX_QUESTION_BYTES = 64 * 1024
_MAX_PREDICTION_BYTES = 16 * 1024

_PUBLIC_TASK_FIELDS = frozenset(
    {
        "schema_version",
        "workload_id",
        "object_id",
        "task_class_id",
        "question",
        "answer_options",
        "success_scoring_rule",
        "credentials_recorded",
        "task_binding_sha256",
    }
)
_LABEL_SOURCE_FIELDS = frozenset(
    {
        "schema_version",
        "oracle_id",
        "logical_node_id",
        "labels",
        "credentials_recorded",
    }
)
_LABEL_FIELDS = frozenset(
    {
        "object_id",
        "task_binding_sha256",
        "success_scoring_rule",
        "answer_option_ids",
        "correct_answer_id",
    }
)
_SCORE_REQUEST_FIELDS = frozenset(
    {
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
)
_SCORE_RESULT_FIELDS = frozenset(
    {
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
)
_HIDDEN_FIELD_NAMES = frozenset(
    {
        "correct_answer_id",
        "accepted_answer_substrings",
        "hidden_labels",
        "labels",
        "label_package",
        "label_package_path",
    }
)


class N1OracleError(ValueError):
    """Raised when hidden-oracle data or public evidence is invalid."""


class N1OracleIdempotencyConflict(N1OracleError):
    """Raised when a durable score ID is reused for another request."""


class N1OracleHTTPError(RuntimeError):
    """Raised when the N1 HTTP boundary fails or violates its contract."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise N1OracleError(message)


def _unique_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        _require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _invalid_number(value: str) -> None:
    raise N1OracleError(f"non-finite JSON number: {value}")


def _json_value(raw: bytes, name: str) -> Any:
    try:
        return json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_unique_keys,
            parse_constant=_invalid_number,
        )
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise N1OracleError(f"{name} must be valid UTF-8 JSON") from exc


def _read_json(path: Path, name: str) -> tuple[bytes, Any]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise N1OracleError(f"cannot read {name}: {path}") from exc
    return raw, _json_value(raw, name)


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    _require(isinstance(value, Mapping), f"{name} must be an object")
    return value


def _array(value: Any, name: str) -> list[Any]:
    _require(isinstance(value, list), f"{name} must be an array")
    return value


def _exact_fields(
    value: Mapping[str, Any],
    expected: frozenset[str],
    name: str,
) -> None:
    _require(set(value) == expected, f"{name} fields changed")


def _text(value: Any, name: str, *, max_bytes: int = 16 * 1024) -> str:
    _require(isinstance(value, str), f"{name} must be a string")
    _require(value == value.strip() and bool(value), f"{name} is not canonical")
    _require(len(value.encode("utf-8")) <= max_bytes, f"{name} is too large")
    return value


def _identifier(value: Any, name: str) -> str:
    text = _text(value, name, max_bytes=256)
    _require(_IDENTIFIER.fullmatch(text) is not None, f"{name} is invalid")
    return text


def _option_id(value: Any, name: str) -> str:
    text = _text(value, name, max_bytes=16)
    _require(_OPTION_ID.fullmatch(text) is not None, f"{name} is invalid")
    return text


def _digest(value: Any, name: str) -> str:
    text = _text(value, name, max_bytes=64)
    _require(_SHA256.fullmatch(text) is not None, f"{name} is not SHA-256")
    return text


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _file_json_bytes(value: Any) -> bytes:
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


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _value_sha256(value: Any) -> str:
    return _sha256(_canonical_json_bytes(value))


def _hmac_sha256(secret: bytes, value: Any) -> str:
    return hmac.new(secret, _canonical_json_bytes(value), hashlib.sha256).hexdigest()


def _validate_options(value: Any, name: str) -> list[dict[str, str]]:
    raw = _array(value, name)
    _require(2 <= len(raw) <= 32, f"{name} must contain 2 to 32 options")
    options: list[dict[str, str]] = []
    seen: set[str] = set()
    for position, item in enumerate(raw):
        option = _mapping(item, f"{name}[{position}]")
        _require(
            set(option) == {"option_id", "text"},
            f"{name}[{position}] fields changed",
        )
        option_id = _option_id(option.get("option_id"), "option_id")
        _require(option_id not in seen, f"{name} repeats an option_id")
        seen.add(option_id)
        options.append(
            {
                "option_id": option_id,
                "text": _text(option.get("text"), "option text"),
            }
        )
    return options


def build_n1_public_task_binding(
    *,
    workload_id: str,
    object_id: str,
    task_class_id: str,
    question: str,
    answer_options: Sequence[Mapping[str, Any]],
    success_scoring_rule: str,
) -> dict[str, Any]:
    """Build the complete task data that may safely enter a FlowMesh plan."""

    task = {
        "schema_version": N1_PUBLIC_TASK_SCHEMA_VERSION,
        "workload_id": workload_id,
        "object_id": object_id,
        "task_class_id": task_class_id,
        "question": question,
        "answer_options": [dict(option) for option in answer_options],
        "success_scoring_rule": success_scoring_rule,
        "credentials_recorded": False,
    }
    validated = _validate_public_task({**task, "task_binding_sha256": "0" * 64})
    binding_core = dict(validated)
    del binding_core["task_binding_sha256"]
    validated["task_binding_sha256"] = _value_sha256(binding_core)
    return validated


def build_n1_hidden_label_record(
    public_task_binding: Mapping[str, Any],
    *,
    correct_answer_id: str,
) -> dict[str, Any]:
    """Derive one private label from its already-frozen public task."""

    task = _validate_public_task(public_task_binding)
    correct = _option_id(correct_answer_id, "correct_answer_id")
    option_ids = [
        option["option_id"] for option in task["answer_options"]
    ]
    _require(correct in option_ids, "correct answer is not a declared option")
    return {
        "object_id": task["object_id"],
        "task_binding_sha256": task["task_binding_sha256"],
        "success_scoring_rule": task["success_scoring_rule"],
        "answer_option_ids": option_ids,
        "correct_answer_id": correct,
    }


def _validate_public_task(value: Any) -> dict[str, Any]:
    task = dict(_mapping(value, "public task binding"))
    _exact_fields(task, _PUBLIC_TASK_FIELDS, "public task binding")
    _require(
        task.get("schema_version") == N1_PUBLIC_TASK_SCHEMA_VERSION,
        "unsupported public task schema_version",
    )
    _identifier(task.get("workload_id"), "workload_id")
    _identifier(task.get("object_id"), "object_id")
    _identifier(task.get("task_class_id"), "task_class_id")
    _text(task.get("question"), "question", max_bytes=_MAX_QUESTION_BYTES)
    task["answer_options"] = _validate_options(
        task.get("answer_options"),
        "answer_options",
    )
    _require(
        task.get("success_scoring_rule") in _SCORING_RULES,
        "unsupported public scoring rule",
    )
    _require(
        task.get("credentials_recorded") is False,
        "public task records credentials",
    )
    supplied = _digest(task.get("task_binding_sha256"), "task_binding_sha256")
    core = dict(task)
    del core["task_binding_sha256"]
    if supplied != "0" * 64:
        _require(supplied == _value_sha256(core), "public task binding mismatch")
    return task


def assert_hidden_oracle_fields_absent(value: Any) -> None:
    """Fail if a FlowMesh-facing value contains a hidden-label field."""

    def walk(item: Any, path: str) -> None:
        if isinstance(item, Mapping):
            for key, child in item.items():
                _require(
                    key not in _HIDDEN_FIELD_NAMES,
                    f"hidden oracle field entered public payload at {path}.{key}",
                )
                walk(child, f"{path}.{key}")
        elif isinstance(item, list):
            for position, child in enumerate(item):
                walk(child, f"{path}[{position}]")

    walk(value, "$")


def _validate_label_source(value: Any) -> tuple[str, list[dict[str, Any]]]:
    root = _mapping(value, "hidden label source")
    _exact_fields(root, _LABEL_SOURCE_FIELDS, "hidden label source")
    _require(
        root.get("schema_version") == N1_LABEL_SOURCE_SCHEMA_VERSION,
        "unsupported hidden label schema_version",
    )
    _require(root.get("logical_node_id") == N1_NODE_ID, "oracle node must be N1")
    _require(
        root.get("credentials_recorded") is False,
        "hidden label source records credentials",
    )
    oracle_id = _identifier(root.get("oracle_id"), "oracle_id")
    labels: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for position, item in enumerate(_array(root.get("labels"), "labels")):
        label = dict(_mapping(item, f"labels[{position}]"))
        _exact_fields(label, _LABEL_FIELDS, f"labels[{position}]")
        object_id = _identifier(label.get("object_id"), "label.object_id")
        task_sha = _digest(
            label.get("task_binding_sha256"),
            "label.task_binding_sha256",
        )
        key = (object_id, task_sha)
        _require(key not in seen, "duplicate object/task label")
        seen.add(key)
        rule = _text(label.get("success_scoring_rule"), "success_scoring_rule")
        _require(rule in _SCORING_RULES, "unsupported hidden scoring rule")
        option_ids = [
            _option_id(option, "answer_option_id")
            for option in _array(label.get("answer_option_ids"), "answer_option_ids")
        ]
        _require(2 <= len(option_ids) <= 32, "answer_option_ids count is invalid")
        _require(len(option_ids) == len(set(option_ids)), "answer_option_ids repeat")
        correct = _option_id(label.get("correct_answer_id"), "correct_answer_id")
        _require(correct in option_ids, "correct answer is not a declared option")
        labels.append(
            {
                "object_id": object_id,
                "task_binding_sha256": task_sha,
                "success_scoring_rule": rule,
                "answer_option_ids": option_ids,
                "correct_answer_id": correct,
            }
        )
    _require(bool(labels), "hidden label source contains no labels")
    ordered = sorted(
        labels,
        key=lambda label: (label["object_id"], label["task_binding_sha256"]),
    )
    _require(labels == ordered, "labels must be sorted by object and task binding")
    return oracle_id, labels


def build_n1_oracle_package(
    label_source_path: str | Path,
    *,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Freeze hidden N1 labels without introducing deployment bindings."""

    source = Path(label_source_path).resolve()
    target = Path(output_dir).resolve()
    _require(not target.exists(), f"oracle package already exists: {target}")
    _, source_value = _read_json(source, "hidden label source")
    oracle_id, labels = _validate_label_source(source_value)
    hidden_bytes = _file_json_bytes(source_value)
    hidden_sha = _sha256(hidden_bytes)
    public_tasks = [
        {
            "object_id": label["object_id"],
            "task_binding_sha256": label["task_binding_sha256"],
            "success_scoring_rule": label["success_scoring_rule"],
            "answer_option_ids": label["answer_option_ids"],
        }
        for label in labels
    ]
    public_task_set_sha = _value_sha256(public_tasks)
    manifest = {
        "schema_version": N1_ORACLE_PACKAGE_SCHEMA_VERSION,
        "status": "FROZEN_HIDDEN_ORACLE",
        "logical_node_id": N1_NODE_ID,
        "oracle_id": oracle_id,
        "label_count": len(labels),
        "hidden_labels_sha256": hidden_sha,
        "public_task_set_sha256": public_task_set_sha,
        "label_values_hidden": True,
        "not_for_flowmesh_plan": True,
        "endpoint_binding_present": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    manifest_bytes = _file_json_bytes(manifest)
    checksums = (
        f"{hidden_sha}  hidden-labels.json\n"
        f"{_sha256(manifest_bytes)}  n1-oracle-package.json\n"
    ).encode("utf-8")
    target.parent.mkdir(parents=True, exist_ok=True)
    stage_parent = Path(tempfile.mkdtemp(prefix=".n1-oracle-", dir=target.parent))
    stage = stage_parent / "package"
    try:
        stage.mkdir()
        (stage / "hidden-labels.json").write_bytes(hidden_bytes)
        (stage / "n1-oracle-package.json").write_bytes(manifest_bytes)
        (stage / "SHA256SUMS").write_bytes(checksums)
        verify_n1_oracle_package(stage)
        _require(not target.exists(), f"oracle package already exists: {target}")
        os.replace(stage, target)
    finally:
        shutil.rmtree(stage_parent, ignore_errors=True)
    return {
        "status": "FROZEN_HIDDEN_ORACLE",
        "logical_node_id": N1_NODE_ID,
        "oracle_id": oracle_id,
        "label_count": len(labels),
        "public_task_set_sha256": public_task_set_sha,
        "output_dir": str(target),
        "label_values_returned": False,
        "endpoint_binding_present": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def _checksum_entries(path: Path) -> dict[str, str]:
    expected = {"hidden-labels.json", "n1-oracle-package.json"}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise N1OracleError("cannot read oracle SHA256SUMS") from exc
    found: dict[str, str] = {}
    for line in lines:
        digest, separator, name = line.partition("  ")
        _require(separator == "  " and name in expected, "malformed SHA256SUMS")
        _digest(digest, "checksum")
        _require(name not in found, f"duplicate checksum: {name}")
        found[name] = digest
    _require(set(found) == expected, "oracle checksums are incomplete")
    return found


def _load_verified_package(root: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    _require(root.is_dir(), f"oracle package does not exist: {root}")
    expected = {"hidden-labels.json", "n1-oracle-package.json", "SHA256SUMS"}
    _require(
        {path.name for path in root.iterdir()} == expected,
        "oracle package file set changed",
    )
    checksums = _checksum_entries(root / "SHA256SUMS")
    hidden_raw, hidden_value = _read_json(root / "hidden-labels.json", "hidden labels")
    manifest_raw, manifest_value = _read_json(
        root / "n1-oracle-package.json",
        "oracle package manifest",
    )
    _require(
        hidden_raw == _file_json_bytes(hidden_value),
        "hidden labels are not canonical",
    )
    _require(
        manifest_raw == _file_json_bytes(manifest_value),
        "oracle manifest is not canonical",
    )
    _require(
        checksums["hidden-labels.json"] == _sha256(hidden_raw),
        "hidden label checksum mismatch",
    )
    _require(
        checksums["n1-oracle-package.json"] == _sha256(manifest_raw),
        "oracle manifest checksum mismatch",
    )
    oracle_id, labels = _validate_label_source(hidden_value)
    manifest = dict(_mapping(manifest_value, "oracle package manifest"))
    expected_fields = {
        "schema_version",
        "status",
        "logical_node_id",
        "oracle_id",
        "label_count",
        "hidden_labels_sha256",
        "public_task_set_sha256",
        "label_values_hidden",
        "not_for_flowmesh_plan",
        "endpoint_binding_present",
        "credentials_recorded",
        "eligible_for_scientific_claims",
    }
    _require(set(manifest) == expected_fields, "oracle package fields changed")
    _require(
        manifest.get("schema_version") == N1_ORACLE_PACKAGE_SCHEMA_VERSION,
        "unsupported oracle package schema_version",
    )
    _require(
        manifest.get("status") == "FROZEN_HIDDEN_ORACLE",
        "oracle package is not frozen",
    )
    _require(
        manifest.get("logical_node_id") == N1_NODE_ID,
        "oracle package node changed",
    )
    _require(manifest.get("oracle_id") == oracle_id, "oracle_id binding mismatch")
    _require(manifest.get("label_count") == len(labels), "label_count mismatch")
    _require(
        manifest.get("hidden_labels_sha256") == _sha256(hidden_raw),
        "hidden label binding mismatch",
    )
    public_tasks = [
        {
            "object_id": label["object_id"],
            "task_binding_sha256": label["task_binding_sha256"],
            "success_scoring_rule": label["success_scoring_rule"],
            "answer_option_ids": label["answer_option_ids"],
        }
        for label in labels
    ]
    _require(
        manifest.get("public_task_set_sha256") == _value_sha256(public_tasks),
        "public task set binding mismatch",
    )
    for field_name in (
        "label_values_hidden",
        "not_for_flowmesh_plan",
    ):
        _require(manifest.get(field_name) is True, f"{field_name} must be true")
    for field_name in (
        "endpoint_binding_present",
        "credentials_recorded",
        "eligible_for_scientific_claims",
    ):
        _require(manifest.get(field_name) is False, f"{field_name} must be false")
    return manifest, labels


def verify_n1_oracle_package(output_dir: str | Path) -> dict[str, Any]:
    """Verify hidden labels while returning no label value."""

    manifest, labels = _load_verified_package(Path(output_dir).resolve())
    return {
        "status": "VERIFIED",
        "logical_node_id": N1_NODE_ID,
        "oracle_id": manifest["oracle_id"],
        "label_count": len(labels),
        "public_task_set_sha256": manifest["public_task_set_sha256"],
        "label_values_returned": False,
        "checked_files": 3,
        "eligible_for_scientific_claims": False,
    }


def build_n1_evaluation_unit_id(
    *,
    oracle_id: str,
    run_id: str,
    trial_id: str,
) -> str:
    """Derive the immutable one-shot scoring identity for a frozen trial."""

    return _value_sha256(
        {
            "domain": "pathfinder.n1-evaluation-unit/v1",
            "oracle_id": _identifier(oracle_id, "oracle_id"),
            "run_id": _text(run_id, "run_id", max_bytes=256),
            "trial_id": _text(trial_id, "trial_id", max_bytes=256),
        }
    )


def build_n1_score_request(
    *,
    score_request_id: str,
    oracle_id: str,
    run_id: str,
    trial_id: str,
    object_id: str,
    task_binding_sha256: str,
    predicted_answer: str,
) -> dict[str, Any]:
    evaluation_unit_id = build_n1_evaluation_unit_id(
        oracle_id=oracle_id,
        run_id=run_id,
        trial_id=trial_id,
    )
    request = {
        "schema_version": N1_SCORE_REQUEST_SCHEMA_VERSION,
        "score_request_id": score_request_id,
        "evaluation_unit_id": evaluation_unit_id,
        "oracle_id": oracle_id,
        "run_id": run_id,
        "trial_id": trial_id,
        "object_id": object_id,
        "task_binding_sha256": task_binding_sha256,
        "predicted_answer": predicted_answer,
        "credentials_recorded": False,
    }
    return _validate_score_request(request)


def _validate_score_request(value: Any) -> dict[str, Any]:
    request = dict(_mapping(value, "score request"))
    schema_version = request.get("schema_version")
    _require(
        schema_version != _LEGACY_SCORE_REQUEST_SCHEMA_VERSION,
        "legacy score request schema is unsupported; rebuild the request "
        "with a frozen run_id and trial_id",
    )
    _require(
        schema_version == N1_SCORE_REQUEST_SCHEMA_VERSION,
        "unsupported score request schema_version",
    )
    _exact_fields(request, _SCORE_REQUEST_FIELDS, "score request")
    _identifier(request.get("score_request_id"), "score_request_id")
    oracle_id = _identifier(request.get("oracle_id"), "oracle_id")
    run_id = _text(request.get("run_id"), "run_id", max_bytes=256)
    trial_id = _text(request.get("trial_id"), "trial_id", max_bytes=256)
    _require(
        request.get("evaluation_unit_id")
        == build_n1_evaluation_unit_id(
            oracle_id=oracle_id,
            run_id=run_id,
            trial_id=trial_id,
        ),
        "evaluation_unit_id does not match oracle_id, run_id, and trial_id",
    )
    _identifier(request.get("object_id"), "object_id")
    _digest(request.get("task_binding_sha256"), "task_binding_sha256")
    predicted = request.get("predicted_answer")
    _require(isinstance(predicted, str), "predicted_answer must be a string")
    _require(
        len(predicted.encode("utf-8")) <= _MAX_PREDICTION_BYTES,
        "predicted_answer is too large",
    )
    _require(
        request.get("credentials_recorded") is False,
        "score request records credentials",
    )
    return request


def _score_answer(predicted: str, correct: str, rule: str) -> bool:
    if rule == MULTIPLE_CHOICE_EXACT_SCORING_RULE:
        return predicted.strip() == correct
    match = _CANONICAL_OPTION.fullmatch(predicted.strip())
    return match is not None and (match.group(1) or match.group(2)) == correct


class _SQLiteScoreStore:
    def __init__(
        self,
        path: Path,
        *,
        oracle_id: str,
        hidden_sha256: str,
        evidence_key_id: str,
    ):
        self.path = path.resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.oracle_id = oracle_id
        self.hidden_sha256 = hidden_sha256
        self.evidence_key_id = _digest(evidence_key_id, "evidence_key_id")
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=30.0,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def _initialize(self) -> None:
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS oracle_state (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    schema_version TEXT NOT NULL,
                    oracle_id TEXT NOT NULL,
                    hidden_labels_sha256 TEXT NOT NULL,
                    evidence_key_id TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS oracle_scores (
                    score_request_id TEXT PRIMARY KEY,
                    evaluation_unit_id TEXT NOT NULL UNIQUE,
                    run_id TEXT NOT NULL,
                    trial_id TEXT NOT NULL,
                    request_sha256 TEXT NOT NULL,
                    request_json TEXT NOT NULL,
                    response_json TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
            state = connection.execute(
                "SELECT * FROM oracle_state WHERE singleton = 1"
            ).fetchone()
            if state is None:
                connection.execute(
                    """
                    INSERT INTO oracle_state (
                        singleton, schema_version, oracle_id,
                        hidden_labels_sha256, evidence_key_id
                    ) VALUES (1, ?, ?, ?, ?)
                    """,
                    (
                        N1_SCORE_STORE_SCHEMA_VERSION,
                        self.oracle_id,
                        self.hidden_sha256,
                        self.evidence_key_id,
                    ),
                )
            else:
                _require(
                    state["schema_version"]
                    not in {
                        _LEGACY_SCORE_STORE_SCHEMA_VERSION,
                        _LEGACY_SCORE_RESULT_SCHEMA_VERSION,
                    },
                    "legacy oracle score database is unsupported; start with "
                    "a new state database",
                )
                _require(
                    state["schema_version"] == N1_SCORE_STORE_SCHEMA_VERSION
                    and state["oracle_id"] == self.oracle_id
                    and state["hidden_labels_sha256"] == self.hidden_sha256
                    and state["evidence_key_id"] == self.evidence_key_id,
                    "oracle database binding differs from mounted labels",
                )
            columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(oracle_scores)")
            }
            _require(
                columns
                == {
                    "score_request_id",
                    "evaluation_unit_id",
                    "run_id",
                    "trial_id",
                    "request_sha256",
                    "request_json",
                    "response_json",
                    "created_at",
                },
                "oracle score database schema changed",
            )

    def execute_once(
        self,
        *,
        request: Mapping[str, Any],
        response: Mapping[str, Any],
    ) -> tuple[dict[str, Any], bool]:
        request_bytes = _canonical_json_bytes(request)
        request_sha = _sha256(request_bytes)
        response_text = _canonical_json_bytes(response).decode("utf-8")
        request_text = request_bytes.decode("utf-8")
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                existing = connection.execute(
                    "SELECT * FROM oracle_scores WHERE score_request_id = ?",
                    (request["score_request_id"],),
                ).fetchone()
                if existing is not None:
                    if existing["request_sha256"] != request_sha:
                        raise N1OracleIdempotencyConflict(
                            "score_request_id was reused for a different request"
                        )
                    stored = _json_value(
                        str(existing["response_json"]).encode("utf-8"),
                        "stored score response",
                    )
                    connection.execute("COMMIT")
                    return dict(_mapping(stored, "stored score response")), True
                consumed = connection.execute(
                    "SELECT score_request_id FROM oracle_scores "
                    "WHERE evaluation_unit_id = ?",
                    (request["evaluation_unit_id"],),
                ).fetchone()
                if consumed is not None:
                    raise N1OracleIdempotencyConflict(
                        "evaluation unit already consumed by another score request"
                    )
                connection.execute(
                    """
                    INSERT INTO oracle_scores (
                        score_request_id, evaluation_unit_id, run_id, trial_id,
                        request_sha256,
                        request_json, response_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        request["score_request_id"],
                        request["evaluation_unit_id"],
                        request["run_id"],
                        request["trial_id"],
                        request_sha,
                        request_text,
                        response_text,
                    ),
                )
                connection.execute("COMMIT")
                return dict(response), False
            except BaseException:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                raise

    def replay_if_present(
        self,
        request: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        """Return an exact durable replay or reject a conflicting ID."""

        request_sha = _value_sha256(request)
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT * FROM oracle_scores WHERE score_request_id = ?",
                (request["score_request_id"],),
            ).fetchone()
            consumed = connection.execute(
                "SELECT score_request_id FROM oracle_scores "
                "WHERE evaluation_unit_id = ?",
                (request["evaluation_unit_id"],),
            ).fetchone()
        if row is None:
            if consumed is not None:
                raise N1OracleIdempotencyConflict(
                    "evaluation unit already consumed by another score request"
                )
            return None
        if row["request_sha256"] != request_sha:
            raise N1OracleIdempotencyConflict(
                "score_request_id was reused for a different request"
            )
        stored = _json_value(
            str(row["response_json"]).encode("utf-8"),
            "stored score response",
        )
        return dict(_mapping(stored, "stored score response"))

    def count(self) -> int:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM oracle_scores"
            ).fetchone()
        _require(row is not None, "oracle score count is unavailable")
        return int(row["count"])


class N1HiddenOracleService:
    """N1 scorer with hidden labels and durable exactly-once evidence."""

    def __init__(
        self,
        package_dir: str | Path,
        *,
        state_db: str | Path,
        evidence_secret: bytes,
    ) -> None:
        _require(
            isinstance(evidence_secret, bytes) and len(evidence_secret) >= 32,
            "evidence_secret must contain at least 32 bytes",
        )
        self.package_dir = Path(package_dir).resolve()
        self.manifest, labels = _load_verified_package(self.package_dir)
        self._labels = {
            (label["object_id"], label["task_binding_sha256"]): label
            for label in labels
        }
        self._secret = evidence_secret
        self._store = _SQLiteScoreStore(
            Path(state_db),
            oracle_id=self.manifest["oracle_id"],
            hidden_sha256=self.manifest["hidden_labels_sha256"],
            evidence_key_id=_hmac_sha256(
                evidence_secret,
                {
                    "domain": "pathfinder.n1-oracle-evidence-key/v1",
                    "oracle_id": self.manifest["oracle_id"],
                },
            ),
        )

    def health(self) -> dict[str, Any]:
        return {
            "schema_version": N1_ORACLE_SERVICE_API_VERSION,
            "status": "ok",
            "node_id": N1_NODE_ID,
            "oracle_id": self.manifest["oracle_id"],
            "public_task_set_sha256": self.manifest["public_task_set_sha256"],
            "label_count": self.manifest["label_count"],
            "hidden_labels_loaded": True,
            "persistent_state": True,
            "hidden_answer_returned": False,
            "credentials_recorded": False,
        }

    def score(self, value: Mapping[str, Any]) -> dict[str, Any]:
        request = _validate_score_request(value)
        _require(
            request["oracle_id"] == self.manifest["oracle_id"],
            "score request oracle_id does not match mounted labels",
        )
        durable = self._store.replay_if_present(request)
        if durable is not None:
            durable["idempotent_replay"] = True
            core = dict(durable)
            core.pop("result_content_sha256", None)
            durable["result_content_sha256"] = _value_sha256(core)
            return _validate_public_score_result(
                request,
                durable,
                expected_oracle_id=self.manifest["oracle_id"],
                expected_public_task_set_sha256=(
                    self.manifest["public_task_set_sha256"]
                ),
            )
        key = (request["object_id"], request["task_binding_sha256"])
        label = self._labels.get(key)
        _require(label is not None, "no hidden label matches object and task binding")
        correct = _score_answer(
            request["predicted_answer"],
            label["correct_answer_id"],
            label["success_scoring_rule"],
        )
        request_sha = _value_sha256(request)
        prediction_sha = _sha256(request["predicted_answer"].encode("utf-8"))
        oracle_instance = _hmac_sha256(
            self._secret,
            {
                "oracle_id": self.manifest["oracle_id"],
                "hidden_labels_sha256": self.manifest["hidden_labels_sha256"],
            },
        )
        score_evidence = _hmac_sha256(
            self._secret,
            {
                "request_sha256": request_sha,
                "evaluation_unit_id": request["evaluation_unit_id"],
                "run_id": request["run_id"],
                "trial_id": request["trial_id"],
                "object_id": label["object_id"],
                "task_binding_sha256": label["task_binding_sha256"],
                "success_scoring_rule": label["success_scoring_rule"],
                "answer_option_ids": label["answer_option_ids"],
                "correct_answer_id": label["correct_answer_id"],
                "correct": correct,
                "score": 1.0 if correct else 0.0,
            },
        )
        response = {
            "schema_version": N1_SCORE_RESULT_SCHEMA_VERSION,
            "status": "SCORED",
            "score_request_id": request["score_request_id"],
            "evaluation_unit_id": request["evaluation_unit_id"],
            "oracle_id": self.manifest["oracle_id"],
            "node_id": N1_NODE_ID,
            "run_id": request["run_id"],
            "trial_id": request["trial_id"],
            "object_id": request["object_id"],
            "task_binding_sha256": request["task_binding_sha256"],
            "request_sha256": request_sha,
            "prediction_sha256": prediction_sha,
            "success_scoring_rule": label["success_scoring_rule"],
            "correct": correct,
            "score": 1.0 if correct else 0.0,
            "public_task_set_sha256": self.manifest["public_task_set_sha256"],
            "oracle_instance_hmac_sha256": oracle_instance,
            "score_evidence_hmac_sha256": score_evidence,
            "idempotent_replay": False,
            "hidden_answer_returned": False,
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        }
        response["result_content_sha256"] = _value_sha256(response)
        stored, replayed = self._store.execute_once(
            request=request,
            response=response,
        )
        if replayed:
            stored["idempotent_replay"] = True
            core = dict(stored)
            core.pop("result_content_sha256", None)
            stored["result_content_sha256"] = _value_sha256(core)
        _validate_public_score_result(
            request,
            stored,
            expected_oracle_id=self.manifest["oracle_id"],
            expected_public_task_set_sha256=(
                self.manifest["public_task_set_sha256"]
            ),
        )
        return stored

    def score_count(self) -> int:
        return self._store.count()


def _validate_public_score_result(
    request_value: Mapping[str, Any],
    result_value: Mapping[str, Any],
    *,
    expected_oracle_id: str,
    expected_public_task_set_sha256: str,
) -> dict[str, Any]:
    request = _validate_score_request(request_value)
    result = dict(_mapping(result_value, "score result"))
    schema_version = result.get("schema_version")
    _require(
        schema_version != _LEGACY_SCORE_RESULT_SCHEMA_VERSION,
        "legacy score result schema is unsupported; rescore with an "
        "evaluation-unit-bound oracle",
    )
    _require(
        schema_version == N1_SCORE_RESULT_SCHEMA_VERSION,
        "unsupported score result schema_version",
    )
    _exact_fields(result, _SCORE_RESULT_FIELDS, "score result")
    _require(result.get("status") == "SCORED", "score did not complete")
    _require(
        result.get("score_request_id") == request["score_request_id"],
        "score_request_id mismatch",
    )
    _require(
        result.get("evaluation_unit_id") == request["evaluation_unit_id"],
        "evaluation_unit_id mismatch",
    )
    _require(result.get("oracle_id") == expected_oracle_id, "oracle_id mismatch")
    _require(result.get("node_id") == N1_NODE_ID, "score result did not come from N1")
    _require(result.get("run_id") == request["run_id"], "run_id mismatch")
    _require(result.get("trial_id") == request["trial_id"], "trial_id mismatch")
    _require(result.get("object_id") == request["object_id"], "object_id mismatch")
    _require(
        result.get("task_binding_sha256") == request["task_binding_sha256"],
        "task binding mismatch",
    )
    _require(
        result.get("request_sha256") == _value_sha256(request),
        "request digest mismatch",
    )
    _require(
        result.get("prediction_sha256")
        == _sha256(request["predicted_answer"].encode("utf-8")),
        "prediction digest mismatch",
    )
    _require(
        result.get("success_scoring_rule") in _SCORING_RULES,
        "scoring rule changed",
    )
    _require(type(result.get("correct")) is bool, "correct must be a boolean")
    _require(
        result.get("score") == (1.0 if result["correct"] else 0.0),
        "score and correctness disagree",
    )
    _require(
        result.get("public_task_set_sha256")
        == _digest(expected_public_task_set_sha256, "public_task_set_sha256"),
        "public task set digest mismatch",
    )
    _digest(result.get("oracle_instance_hmac_sha256"), "oracle instance HMAC")
    _digest(result.get("score_evidence_hmac_sha256"), "score evidence HMAC")
    _require(
        type(result.get("idempotent_replay")) is bool,
        "idempotent_replay is invalid",
    )
    _require(
        result.get("hidden_answer_returned") is False,
        "hidden answer was returned",
    )
    _require(
        result.get("credentials_recorded") is False,
        "score result records credentials",
    )
    _require(
        result.get("eligible_for_scientific_claims") is False,
        "score result changes scientific eligibility",
    )
    content_sha = _digest(result.get("result_content_sha256"), "result content digest")
    core = dict(result)
    del core["result_content_sha256"]
    _require(content_sha == _value_sha256(core), "result content digest mismatch")
    assert_hidden_oracle_fields_absent(result)
    return result


def verify_n1_score_result(
    *,
    package_dir: str | Path,
    request: Mapping[str, Any],
    result: Mapping[str, Any],
    evidence_secret: bytes,
) -> dict[str, Any]:
    """Privileged offline verification without returning the hidden answer."""

    _require(
        isinstance(evidence_secret, bytes) and len(evidence_secret) >= 32,
        "evidence_secret must contain at least 32 bytes",
    )
    manifest, labels = _load_verified_package(Path(package_dir).resolve())
    checked = _validate_public_score_result(
        request,
        result,
        expected_oracle_id=manifest["oracle_id"],
        expected_public_task_set_sha256=manifest["public_task_set_sha256"],
    )
    request_checked = _validate_score_request(request)
    label = next(
        (
            item
            for item in labels
            if item["object_id"] == request_checked["object_id"]
            and item["task_binding_sha256"]
            == request_checked["task_binding_sha256"]
        ),
        None,
    )
    _require(label is not None, "no hidden label matches object and task binding")
    expected_correct = _score_answer(
        request_checked["predicted_answer"],
        label["correct_answer_id"],
        label["success_scoring_rule"],
    )
    expected_instance = _hmac_sha256(
        evidence_secret,
        {
            "oracle_id": manifest["oracle_id"],
            "hidden_labels_sha256": manifest["hidden_labels_sha256"],
        },
    )
    expected_evidence = _hmac_sha256(
        evidence_secret,
        {
            "request_sha256": _value_sha256(request_checked),
            "evaluation_unit_id": request_checked["evaluation_unit_id"],
            "run_id": request_checked["run_id"],
            "trial_id": request_checked["trial_id"],
            "object_id": label["object_id"],
            "task_binding_sha256": label["task_binding_sha256"],
            "success_scoring_rule": label["success_scoring_rule"],
            "answer_option_ids": label["answer_option_ids"],
            "correct_answer_id": label["correct_answer_id"],
            "correct": expected_correct,
            "score": 1.0 if expected_correct else 0.0,
        },
    )
    _require(
        checked["correct"] is expected_correct,
        "correctness differs from hidden label",
    )
    _require(
        hmac.compare_digest(
            checked["oracle_instance_hmac_sha256"],
            expected_instance,
        ),
        "oracle instance HMAC mismatch",
    )
    _require(
        hmac.compare_digest(
            checked["score_evidence_hmac_sha256"],
            expected_evidence,
        ),
        "score evidence HMAC mismatch",
    )
    return {
        "status": "VERIFIED",
        "node_id": N1_NODE_ID,
        "oracle_id": manifest["oracle_id"],
        "score_request_id": checked["score_request_id"],
        "evaluation_unit_id": checked["evaluation_unit_id"],
        "run_id": checked["run_id"],
        "trial_id": checked["trial_id"],
        "request_sha256": checked["request_sha256"],
        "prediction_sha256": checked["prediction_sha256"],
        "correct": checked["correct"],
        "score": checked["score"],
        "hidden_answer_returned": False,
        "eligible_for_scientific_claims": False,
    }


class N1OracleHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    service: N1HiddenOracleService
    bearer_token: str


class _N1OracleRequestHandler(BaseHTTPRequestHandler):
    server: N1OracleHTTPServer
    server_version = "PathfinderN1Oracle/0.1"

    def log_message(self, format: str, *args: object) -> None:
        del format, args

    def _write_json(self, status: HTTPStatus, value: Mapping[str, Any]) -> None:
        body = _canonical_json_bytes(value) + b"\n"
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _error(self, status: HTTPStatus, code: str, message: str) -> None:
        self._write_json(
            status,
            {
                "schema_version": N1_ORACLE_SERVICE_API_VERSION,
                "status": "error",
                "error": {"code": code, "message": message},
                "hidden_answer_returned": False,
                "credentials_recorded": False,
            },
        )

    def do_GET(self) -> None:
        parsed = urlsplit(self.path)
        if parsed.path == "/healthz" and not parsed.query:
            self._write_json(HTTPStatus.OK, self.server.service.health())
            return
        self._error(HTTPStatus.NOT_FOUND, "not_found", "unknown endpoint")

    def do_POST(self) -> None:
        try:
            if self.path != "/v1/oracle/score":
                self._error(HTTPStatus.NOT_FOUND, "not_found", "unknown endpoint")
                return
            supplied = self.headers.get("Authorization")
            if not (
                isinstance(supplied, str)
                and hmac.compare_digest(
                    supplied,
                    f"Bearer {self.server.bearer_token}",
                )
            ):
                self._error(
                    HTTPStatus.UNAUTHORIZED,
                    "unauthorized",
                    "invalid bearer token",
                )
                return
            _require(
                self.headers.get("X-Pathfinder-Oracle-Protocol-Version")
                == N1_ORACLE_SERVICE_API_VERSION,
                "missing or unsupported oracle protocol header",
            )
            media_type = (
                self.headers.get("Content-Type", "")
                .partition(";")[0]
                .strip()
                .casefold()
            )
            _require(
                media_type == "application/json",
                "Content-Type must be application/json",
            )
            raw_length = self.headers.get("Content-Length")
            _require(
                isinstance(raw_length, str) and raw_length.isdecimal(),
                "Content-Length is required",
            )
            length = int(raw_length)
            _require(length <= _MAX_REQUEST_BYTES, "score request is too large")
            raw = self.rfile.read(length)
            _require(len(raw) == length, "score request is truncated")
            request = _validate_score_request(_json_value(raw, "score request"))
            _require(
                self.headers.get("Idempotency-Key")
                == request["score_request_id"],
                "Idempotency-Key must equal score_request_id",
            )
            self._write_json(HTTPStatus.OK, self.server.service.score(request))
        except N1OracleIdempotencyConflict as exc:
            self._error(HTTPStatus.CONFLICT, "idempotency_conflict", str(exc))
        except N1OracleError as exc:
            self._error(HTTPStatus.BAD_REQUEST, "invalid_request", str(exc))
        except Exception:
            self._error(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                "internal_error",
                "oracle scoring failed",
            )


def create_n1_oracle_http_server(
    package_dir: str | Path,
    *,
    state_db: str | Path,
    bearer_token: str,
    evidence_secret: bytes,
    host: str = "127.0.0.1",
    port: int = 0,
) -> N1OracleHTTPServer:
    """Create, but do not start, the authenticated N1 scoring service."""

    token = _text(bearer_token, "bearer_token", max_bytes=4096)
    service = N1HiddenOracleService(
        package_dir,
        state_db=state_db,
        evidence_secret=evidence_secret,
    )
    server = N1OracleHTTPServer((host, port), _N1OracleRequestHandler)
    server.service = service
    server.bearer_token = token
    return server


class _RejectRedirects(HTTPRedirectHandler):
    def redirect_request(
        self,
        req: Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        del req, fp, code, msg, headers, newurl
        return None


def _loopback(hostname: str | None) -> bool:
    if hostname is None:
        return False
    if hostname.casefold() == "localhost":
        return True
    try:
        return ipaddress.ip_address(hostname).is_loopback
    except ValueError:
        return False


@dataclass(frozen=True)
class N1OracleHTTPClient:
    """Deployment-bound N7 client for the hidden N1 scoring boundary."""

    base_url: str
    expected_oracle_id: str
    expected_public_task_set_sha256: str
    bearer_token: str = field(repr=False)
    timeout_seconds: float = 10.0
    simulator_private_http_hosts: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        parsed = urlsplit(self.base_url)
        _require(
            parsed.scheme in {"http", "https"},
            "oracle base_url scheme is invalid",
        )
        _require(
            bool(parsed.hostname) and not parsed.query and not parsed.fragment,
            "oracle base_url is invalid",
        )
        _require(parsed.path in {"", "/"}, "oracle base_url must not contain a path")
        allowed = tuple(sorted(set(self.simulator_private_http_hosts)))
        for host in allowed:
            _require(
                _SIMULATOR_HOST.fullmatch(host) is not None,
                "simulator host is invalid",
            )
        _require(
            parsed.scheme == "https"
            or _loopback(parsed.hostname)
            or parsed.hostname in allowed,
            "plain HTTP is allowed only for loopback or an explicit simulator host",
        )
        _identifier(self.expected_oracle_id, "expected_oracle_id")
        _digest(
            self.expected_public_task_set_sha256,
            "expected_public_task_set_sha256",
        )
        _text(self.bearer_token, "bearer_token", max_bytes=4096)
        _require(
            type(self.timeout_seconds) in {int, float}
            and float(self.timeout_seconds) > 0.0,
            "timeout_seconds must be positive",
        )
        object.__setattr__(self, "base_url", self.base_url.rstrip("/"))
        object.__setattr__(self, "simulator_private_http_hosts", allowed)

    def _opener(self) -> Any:
        parsed = urlsplit(self.base_url)
        handlers: list[Any] = [_RejectRedirects()]
        if (
            _loopback(parsed.hostname)
            or parsed.hostname in self.simulator_private_http_hosts
        ):
            handlers.insert(0, ProxyHandler({}))
        return build_opener(*handlers)

    def _request(self, request: Request) -> dict[str, Any]:
        try:
            with self._opener().open(
                request,
                timeout=float(self.timeout_seconds),
            ) as response:
                _require(
                    response.headers.get_content_type() == "application/json",
                    "oracle response Content-Type is invalid",
                )
                raw = response.read(_MAX_RESPONSE_BYTES + 1)
                _require(
                    len(raw) <= _MAX_RESPONSE_BYTES,
                    "oracle response is too large",
                )
        except HTTPError as exc:
            raise N1OracleHTTPError(f"N1 oracle returned HTTP {exc.code}") from exc
        except URLError as exc:
            raise N1OracleHTTPError("N1 oracle request failed") from exc
        value = _json_value(raw, "oracle HTTP response")
        return dict(_mapping(value, "oracle HTTP response"))

    def health(self) -> dict[str, Any]:
        health = self._request(Request(f"{self.base_url}/healthz", method="GET"))
        _require(health.get("status") == "ok", "N1 oracle health is not ok")
        _require(health.get("node_id") == N1_NODE_ID, "oracle endpoint is not N1")
        _require(
            health.get("oracle_id") == self.expected_oracle_id,
            "health oracle_id mismatch",
        )
        _require(
            health.get("public_task_set_sha256")
            == self.expected_public_task_set_sha256,
            "health public task set mismatch",
        )
        _require(
            health.get("hidden_answer_returned") is False,
            "health leaks hidden answer",
        )
        _require(health.get("credentials_recorded") is False, "unsafe health response")
        return health

    def score(self, value: Mapping[str, Any]) -> dict[str, Any]:
        request_value = _validate_score_request(value)
        _require(
            request_value["oracle_id"] == self.expected_oracle_id,
            "score oracle_id differs from deployment binding",
        )
        result = self._request(
            Request(
                f"{self.base_url}/v1/oracle/score",
                data=_canonical_json_bytes(request_value),
                headers={
                    "Authorization": f"Bearer {self.bearer_token}",
                    "Content-Type": "application/json",
                    "X-Pathfinder-Oracle-Protocol-Version": (
                        N1_ORACLE_SERVICE_API_VERSION
                    ),
                    "Idempotency-Key": request_value["score_request_id"],
                },
                method="POST",
            )
        )
        return _validate_public_score_result(
            request_value,
            result,
            expected_oracle_id=self.expected_oracle_id,
            expected_public_task_set_sha256=(
                self.expected_public_task_set_sha256
            ),
        )
