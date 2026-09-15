"""Portable deterministic lexical-index package and service contract.

This module turns frozen *visible* metadata into a real, queryable lexical
index.  The package contains no endpoint or credential binding, so the same
bytes can be mounted by logical N2, N7, or N8 locally and by their future VMs.
The historical package identity remains N2; a serving node identity is an
explicit runtime binding and never changes the frozen content.  Network
addresses and bearer tokens belong only to :class:`N2IndexHTTPClient` and the
HTTP server construction call.

The ranking algorithm intentionally uses integer arithmetic.  This makes the
ranked candidates and their hashes stable across hosts while still executing
real tokenisation, document-frequency weighting, term-frequency scoring, and
candidate filtering.  It is a development index, not a claim that lexical
retrieval is the final multimodal retrieval method.
"""

from __future__ import annotations

import hashlib
import hmac
import ipaddress
import json
import os
import re
import shutil
import tempfile
import unicodedata
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


INDEX_SOURCE_SCHEMA_VERSION = "pathfinder.n2-index-source/v1alpha1"
INDEX_ARTIFACT_SCHEMA_VERSION = "pathfinder.n2-lexical-index/v1alpha1"
INDEX_PACKAGE_SCHEMA_VERSION = "pathfinder.n2-index-package/v1alpha1"
INDEX_QUERY_REQUEST_SCHEMA_VERSION = (
    "pathfinder.n2-index-query-request/v1alpha1"
)
INDEX_QUERY_RESULT_SCHEMA_VERSION = (
    "pathfinder.n2-index-query-result/v1alpha1"
)
INDEX_PUBLIC_QUERY_RESULT_SCHEMA_VERSION = (
    "pathfinder.n2-public-index-query-result/v1alpha1"
)
INDEX_SERVICE_API_VERSION = "pathfinder.n2-index-service/v1alpha1"

N2_NODE_ID = "N2"
INDEX_SERVING_NODE_IDS = frozenset({"N2", "N7", "N8"})
INDEX_ALGORITHM = "deterministic-lexical-overlap-v1"
INDEX_TOKENIZER = "unicode-nfkc-alphanumeric-casefold-v1"
_SCORE_SCALE = 1_000_000
_MAX_QUERY_BYTES = 64 * 1024
_MAX_REQUEST_BYTES = 128 * 1024
_MAX_RESPONSE_BYTES = 4 * 1024 * 1024
_MAX_TOP_K = 1_000
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:+-]{0,255}")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_SIMULATOR_HOST = re.compile(
    r"(?:pathfinder-sim|pathfinder-full-flow)-[a-z0-9-]+"
)
_TOKEN = re.compile(r"[^\W_]+", flags=re.UNICODE)

_SOURCE_FIELDS = frozenset(
    {
        "schema_version",
        "index_id",
        "logical_node_id",
        "documents",
        "credentials_recorded",
    }
)
_DOCUMENT_FIELDS = frozenset(
    {"object_id", "source_object_group", "visible_fields"}
)
_VISIBLE_FIELDS = frozenset(
    {
        "title",
        "summary",
        "description",
        "digest",
        "tags",
        "media_type",
        "modalities",
        "source_collection",
    }
)
_FIELD_WEIGHTS = {
    "title": 4,
    "tags": 3,
    "summary": 2,
    "description": 1,
    "digest": 1,
    "media_type": 1,
    "modalities": 1,
    "source_collection": 1,
}
_REQUEST_FIELDS = frozenset(
    {
        "schema_version",
        "request_id",
        "query_id",
        "requested_node_id",
        "index_id",
        "query_text",
        "top_k",
        "candidate_object_ids",
        "credentials_recorded",
    }
)
_RESULT_FIELDS = frozenset(
    {
        "schema_version",
        "status",
        "request_id",
        "query_id",
        "node_id",
        "index_id",
        "index_sha256",
        "source_manifest_sha256",
        "request_sha256",
        "query_text_sha256",
        "candidate_set_sha256",
        "ranking_sha256",
        "result_content_sha256",
        "algorithm",
        "tokenizer",
        "query_token_count",
        "candidate_count",
        "top_k",
        "ranked_candidates",
        "lexical_retrieval_executed",
        "llm_called",
        "credentials_recorded",
        "eligible_for_scientific_claims",
    }
)
_RANKED_FIELDS = frozenset(
    {
        "rank",
        "object_id",
        "source_object_group",
        "lexical_score_units",
        "matched_terms",
        "visible_fields_sha256",
    }
)
_PUBLIC_RANKED_FIELDS = frozenset(
    {
        "rank",
        "object_id",
        "lexical_score_units",
        "matched_terms",
        "visible_fields_sha256",
    }
)


class N2IndexError(ValueError):
    """Raised when an index package, request, or result is not trustworthy."""


class N2IndexHTTPError(RuntimeError):
    """Raised when a remote N2 endpoint violates the service contract."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise N2IndexError(message)


def _serving_node_id(value: Any, name: str = "node_id") -> str:
    node_id = _identifier(value, name)
    _require(
        node_id in INDEX_SERVING_NODE_IDS,
        f"{name} must target N2, N7, or N8",
    )
    return node_id


def _unique_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        _require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _invalid_number(value: str) -> None:
    raise N2IndexError(f"non-finite JSON number: {value}")


def _json_value(raw: bytes, name: str) -> Any:
    try:
        return json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_unique_keys,
            parse_constant=_invalid_number,
        )
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise N2IndexError(f"{name} must be valid UTF-8 JSON") from exc


def _read_json(path: Path, name: str) -> tuple[bytes, Any]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise N2IndexError(f"cannot read {name}: {path}") from exc
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
    actual = set(value)
    _require(actual == expected, f"{name} fields changed")


def _text(value: Any, name: str, *, max_bytes: int = 16 * 1024) -> str:
    _require(isinstance(value, str), f"{name} must be a string")
    _require(value == value.strip() and bool(value), f"{name} is not canonical")
    _require(len(value.encode("utf-8")) <= max_bytes, f"{name} is too large")
    return value


def _identifier(value: Any, name: str) -> str:
    text = _text(value, name, max_bytes=256)
    _require(_IDENTIFIER.fullmatch(text) is not None, f"{name} is invalid")
    return text


def _digest(value: Any, name: str) -> str:
    text = _text(value, name, max_bytes=64)
    _require(_SHA256.fullmatch(text) is not None, f"{name} is not SHA-256")
    return text


def _positive_integer(value: Any, name: str) -> int:
    _require(type(value) is int and value > 0, f"{name} must be positive")
    return value


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


def _tokens(value: str) -> tuple[str, ...]:
    normalized = unicodedata.normalize("NFKC", value).casefold()
    return tuple(_TOKEN.findall(normalized))


def _visible_value(value: Any, name: str) -> tuple[str, ...]:
    if isinstance(value, str):
        return (_text(value, name),)
    items = _array(value, name)
    _require(bool(items), f"{name} must not be empty")
    texts = tuple(_text(item, f"{name} item") for item in items)
    _require(len(texts) == len(set(texts)), f"{name} repeats a value")
    _require(list(texts) == sorted(texts), f"{name} must be sorted")
    return texts


def _validate_source(value: Any) -> tuple[str, list[dict[str, Any]]]:
    root = _mapping(value, "index source")
    _exact_fields(root, _SOURCE_FIELDS, "index source")
    _require(
        root.get("schema_version") == INDEX_SOURCE_SCHEMA_VERSION,
        "unsupported index source schema_version",
    )
    _require(root.get("logical_node_id") == N2_NODE_ID, "index node must be N2")
    _require(
        root.get("credentials_recorded") is False,
        "index source must record credentials_recorded=false",
    )
    index_id = _identifier(root.get("index_id"), "index_id")
    documents: list[dict[str, Any]] = []
    seen: set[str] = set()
    for position, item in enumerate(_array(root.get("documents"), "documents")):
        document = _mapping(item, f"documents[{position}]")
        _exact_fields(document, _DOCUMENT_FIELDS, f"documents[{position}]")
        object_id = _identifier(document.get("object_id"), "object_id")
        _require(object_id not in seen, f"duplicate object_id: {object_id}")
        seen.add(object_id)
        group = _identifier(
            document.get("source_object_group"),
            "source_object_group",
        )
        fields = _mapping(document.get("visible_fields"), "visible_fields")
        _require(bool(fields), f"visible_fields is empty: {object_id}")
        _require(
            set(fields).issubset(_VISIBLE_FIELDS),
            f"visible_fields contains an unsupported or hidden field: {object_id}",
        )
        normalized_fields: dict[str, Any] = {}
        weighted_tokens: list[str] = []
        for field_name in sorted(fields):
            values = _visible_value(
                fields[field_name],
                f"{object_id}.visible_fields.{field_name}",
            )
            normalized_fields[field_name] = (
                values[0] if isinstance(fields[field_name], str) else list(values)
            )
            weight = _FIELD_WEIGHTS[field_name]
            for text in values:
                field_tokens = _tokens(text)
                weighted_tokens.extend(field_tokens * weight)
        _require(bool(weighted_tokens), f"document has no indexable text: {object_id}")
        documents.append(
            {
                "object_id": object_id,
                "source_object_group": group,
                "visible_fields": normalized_fields,
                "weighted_tokens": tuple(weighted_tokens),
            }
        )
    _require(bool(documents), "index source must contain at least one document")
    ordered = sorted(documents, key=lambda item: item["object_id"])
    _require(
        [item["object_id"] for item in documents]
        == [item["object_id"] for item in ordered],
        "documents must be sorted by object_id",
    )
    return index_id, ordered


def _build_artifact(
    index_id: str,
    documents: list[dict[str, Any]],
    source_sha256: str,
) -> dict[str, Any]:
    indexed: list[dict[str, Any]] = []
    document_frequency: dict[str, int] = {}
    for document in documents:
        counts: dict[str, int] = {}
        for token in document["weighted_tokens"]:
            counts[token] = counts.get(token, 0) + 1
        for token in counts:
            document_frequency[token] = document_frequency.get(token, 0) + 1
        indexed.append(
            {
                "object_id": document["object_id"],
                "source_object_group": document["source_object_group"],
                "document_length": sum(counts.values()),
                "term_frequencies": dict(sorted(counts.items())),
                "visible_fields_sha256": _value_sha256(
                    document["visible_fields"]
                ),
            }
        )
    document_count = len(indexed)
    weights = {
        term: ((document_count + 1) * _SCORE_SCALE) // (frequency + 1)
        for term, frequency in sorted(document_frequency.items())
    }
    candidate_ids = [document["object_id"] for document in indexed]
    return {
        "schema_version": INDEX_ARTIFACT_SCHEMA_VERSION,
        "status": "FROZEN",
        "logical_node_id": N2_NODE_ID,
        "index_id": index_id,
        "algorithm": INDEX_ALGORITHM,
        "algorithm_parameters": {
            "idf_scale": _SCORE_SCALE,
            "max_term_frequency_contribution": 8,
            "field_weights": dict(sorted(_FIELD_WEIGHTS.items())),
        },
        "tokenizer": INDEX_TOKENIZER,
        "source_manifest_sha256": source_sha256,
        "document_count": document_count,
        "candidate_object_ids": candidate_ids,
        "candidate_set_sha256": _value_sha256(candidate_ids),
        "document_frequency": dict(sorted(document_frequency.items())),
        "term_weight_units": weights,
        "documents": indexed,
        "endpoint_binding_present": False,
        "llm_called": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def _validate_artifact(value: Any) -> dict[str, Any]:
    artifact = dict(_mapping(value, "index artifact"))
    expected_fields = {
        "schema_version",
        "status",
        "logical_node_id",
        "index_id",
        "algorithm",
        "algorithm_parameters",
        "tokenizer",
        "source_manifest_sha256",
        "document_count",
        "candidate_object_ids",
        "candidate_set_sha256",
        "document_frequency",
        "term_weight_units",
        "documents",
        "endpoint_binding_present",
        "llm_called",
        "credentials_recorded",
        "eligible_for_scientific_claims",
    }
    _require(set(artifact) == expected_fields, "index artifact fields changed")
    _require(
        artifact.get("schema_version") == INDEX_ARTIFACT_SCHEMA_VERSION,
        "unsupported index artifact schema_version",
    )
    _require(artifact.get("status") == "FROZEN", "index is not frozen")
    _require(artifact.get("logical_node_id") == N2_NODE_ID, "index node changed")
    index_id = _identifier(artifact.get("index_id"), "index_id")
    del index_id
    _digest(artifact.get("source_manifest_sha256"), "source_manifest_sha256")
    _require(artifact.get("algorithm") == INDEX_ALGORITHM, "algorithm changed")
    _require(artifact.get("tokenizer") == INDEX_TOKENIZER, "tokenizer changed")
    expected_parameters = {
        "idf_scale": _SCORE_SCALE,
        "max_term_frequency_contribution": 8,
        "field_weights": dict(sorted(_FIELD_WEIGHTS.items())),
    }
    _require(
        artifact.get("algorithm_parameters") == expected_parameters,
        "algorithm parameters changed",
    )
    _require(
        artifact.get("endpoint_binding_present") is False,
        "portable index contains endpoint binding",
    )
    for field in (
        "llm_called",
        "credentials_recorded",
        "eligible_for_scientific_claims",
    ):
        _require(artifact.get(field) is False, f"index {field} must be false")

    documents = _array(artifact.get("documents"), "index documents")
    count = _positive_integer(artifact.get("document_count"), "document_count")
    _require(count == len(documents), "document_count mismatch")
    expected_doc_fields = {
        "object_id",
        "source_object_group",
        "document_length",
        "term_frequencies",
        "visible_fields_sha256",
    }
    ids: list[str] = []
    recomputed_df: dict[str, int] = {}
    for position, item in enumerate(documents):
        document = _mapping(item, f"index documents[{position}]")
        _require(
            set(document) == expected_doc_fields,
            f"index documents[{position}] fields changed",
        )
        object_id = _identifier(document.get("object_id"), "object_id")
        ids.append(object_id)
        _identifier(document.get("source_object_group"), "source_object_group")
        _digest(document.get("visible_fields_sha256"), "visible_fields_sha256")
        counts = _mapping(document.get("term_frequencies"), "term_frequencies")
        _require(bool(counts), f"term frequencies are empty: {object_id}")
        total = 0
        for term, frequency in counts.items():
            _require(term in _tokens(term), f"noncanonical index term: {term}")
            amount = _positive_integer(frequency, f"term frequency {term}")
            total += amount
            recomputed_df[term] = recomputed_df.get(term, 0) + 1
        _require(
            document.get("document_length") == total,
            f"document_length mismatch: {object_id}",
        )
    _require(ids == sorted(set(ids)), "index documents are not sorted and unique")
    _require(
        artifact.get("candidate_object_ids") == ids,
        "candidate_object_ids mismatch",
    )
    _require(
        artifact.get("candidate_set_sha256") == _value_sha256(ids),
        "candidate set digest mismatch",
    )
    expected_df = dict(sorted(recomputed_df.items()))
    _require(
        artifact.get("document_frequency") == expected_df,
        "document frequency mismatch",
    )
    expected_weights = {
        term: ((count + 1) * _SCORE_SCALE) // (frequency + 1)
        for term, frequency in expected_df.items()
    }
    _require(
        artifact.get("term_weight_units") == expected_weights,
        "term weights mismatch",
    )
    return artifact


def build_n2_index_package(
    source_manifest_path: str | Path,
    *,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Freeze a portable N2 index package from visible metadata."""

    source = Path(source_manifest_path).resolve()
    target = Path(output_dir).resolve()
    _require(not target.exists(), f"index package already exists: {target}")
    _, source_value = _read_json(source, "index source")
    index_id, documents = _validate_source(source_value)
    source_bytes = _file_json_bytes(source_value)
    source_sha256 = _sha256(source_bytes)
    artifact = _build_artifact(index_id, documents, source_sha256)
    artifact_bytes = _file_json_bytes(artifact)
    package = {
        "schema_version": INDEX_PACKAGE_SCHEMA_VERSION,
        "status": "FROZEN_PORTABLE_INDEX",
        "logical_node_id": N2_NODE_ID,
        "index_id": index_id,
        "source_manifest_sha256": source_sha256,
        "index_sha256": _sha256(artifact_bytes),
        "document_count": len(documents),
        "candidate_set_sha256": artifact["candidate_set_sha256"],
        "algorithm": INDEX_ALGORITHM,
        "tokenizer": INDEX_TOKENIZER,
        "endpoint_binding_present": False,
        "external_services_called": False,
        "llm_called": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    package_bytes = _file_json_bytes(package)
    checksums = (
        f"{_sha256(source_bytes)}  index-source.json\n"
        f"{_sha256(artifact_bytes)}  lexical-index.json\n"
        f"{_sha256(package_bytes)}  n2-index-package.json\n"
    ).encode("utf-8")

    target.parent.mkdir(parents=True, exist_ok=True)
    stage_parent = Path(tempfile.mkdtemp(prefix=".n2-index-", dir=target.parent))
    stage = stage_parent / "package"
    try:
        stage.mkdir()
        (stage / "index-source.json").write_bytes(source_bytes)
        (stage / "lexical-index.json").write_bytes(artifact_bytes)
        (stage / "n2-index-package.json").write_bytes(package_bytes)
        (stage / "SHA256SUMS").write_bytes(checksums)
        verify_n2_index_package(stage)
        _require(not target.exists(), f"index package already exists: {target}")
        os.replace(stage, target)
    finally:
        shutil.rmtree(stage_parent, ignore_errors=True)
    return {
        "status": "FROZEN_PORTABLE_INDEX",
        "logical_node_id": N2_NODE_ID,
        "index_id": index_id,
        "document_count": len(documents),
        "index_sha256": package["index_sha256"],
        "source_manifest_sha256": source_sha256,
        "output_dir": str(target),
        "endpoint_binding_present": False,
        "llm_called": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def _checksum_entries(path: Path) -> dict[str, str]:
    expected = {
        "index-source.json",
        "lexical-index.json",
        "n2-index-package.json",
    }
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise N2IndexError("cannot read SHA256SUMS") from exc
    found: dict[str, str] = {}
    for line in lines:
        digest, separator, name = line.partition("  ")
        _require(separator == "  " and name in expected, "malformed SHA256SUMS")
        _digest(digest, "checksum")
        _require(name not in found, f"duplicate checksum: {name}")
        found[name] = digest
    _require(set(found) == expected, "index checksums are incomplete")
    return found


def _load_verified_package(root: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    _require(root.is_dir(), f"index package does not exist: {root}")
    expected_files = {
        "index-source.json",
        "lexical-index.json",
        "n2-index-package.json",
        "SHA256SUMS",
    }
    _require(
        {path.name for path in root.iterdir()} == expected_files,
        "index package file set changed",
    )
    checksums = _checksum_entries(root / "SHA256SUMS")
    source_raw, source_value = _read_json(root / "index-source.json", "index source")
    artifact_raw, artifact_value = _read_json(
        root / "lexical-index.json", "index artifact"
    )
    package_raw, package_value = _read_json(
        root / "n2-index-package.json", "index package manifest"
    )
    _require(
        source_raw == _file_json_bytes(source_value),
        "index source is not canonically serialized",
    )
    _require(
        artifact_raw == _file_json_bytes(artifact_value),
        "index artifact is not canonically serialized",
    )
    _require(
        package_raw == _file_json_bytes(package_value),
        "index package manifest is not canonically serialized",
    )
    _require(
        checksums["index-source.json"] == _sha256(source_raw),
        "index source checksum mismatch",
    )
    _require(
        checksums["lexical-index.json"] == _sha256(artifact_raw),
        "index artifact checksum mismatch",
    )
    _require(
        checksums["n2-index-package.json"] == _sha256(package_raw),
        "index package checksum mismatch",
    )
    source_index_id, source_documents = _validate_source(source_value)
    artifact = _validate_artifact(artifact_value)
    expected_artifact = _build_artifact(
        source_index_id,
        source_documents,
        _sha256(source_raw),
    )
    _require(
        artifact == expected_artifact,
        "index artifact differs from rebuilding its frozen visible source",
    )
    package = dict(_mapping(package_value, "index package manifest"))
    expected_manifest_fields = {
        "schema_version",
        "status",
        "logical_node_id",
        "index_id",
        "source_manifest_sha256",
        "index_sha256",
        "document_count",
        "candidate_set_sha256",
        "algorithm",
        "tokenizer",
        "endpoint_binding_present",
        "external_services_called",
        "llm_called",
        "credentials_recorded",
        "eligible_for_scientific_claims",
    }
    _require(set(package) == expected_manifest_fields, "package fields changed")
    _require(
        package.get("schema_version") == INDEX_PACKAGE_SCHEMA_VERSION,
        "unsupported package schema_version",
    )
    _require(
        package.get("status") == "FROZEN_PORTABLE_INDEX",
        "index package is not frozen",
    )
    for name in (
        "logical_node_id",
        "index_id",
        "source_manifest_sha256",
        "document_count",
        "candidate_set_sha256",
        "algorithm",
        "tokenizer",
        "endpoint_binding_present",
        "llm_called",
        "credentials_recorded",
        "eligible_for_scientific_claims",
    ):
        _require(package.get(name) == artifact.get(name), f"{name} binding mismatch")
    _require(
        package.get("external_services_called") is False,
        "external_services_called must be false",
    )
    _require(
        package.get("index_sha256") == _sha256(artifact_raw),
        "index binding mismatch",
    )
    return artifact, package


def verify_n2_index_package(output_dir: str | Path) -> dict[str, Any]:
    """Verify package bytes, internal index arithmetic, and portability."""

    root = Path(output_dir).resolve()
    artifact, package = _load_verified_package(root)
    return {
        "status": "VERIFIED",
        "logical_node_id": N2_NODE_ID,
        "index_id": artifact["index_id"],
        "document_count": artifact["document_count"],
        "index_sha256": package["index_sha256"],
        "source_manifest_sha256": artifact["source_manifest_sha256"],
        "checked_files": 4,
        "endpoint_binding_present": False,
        "eligible_for_scientific_claims": False,
    }


def build_n2_index_query_request(
    *,
    request_id: str,
    query_id: str,
    index_id: str,
    query_text: str,
    top_k: int,
    candidate_object_ids: Sequence[str] | None = None,
    requested_node_id: str = N2_NODE_ID,
) -> dict[str, Any]:
    """Build an endpoint-free query for an N2/N7/N8 logical index service.

    The default remains N2 so existing global-index callers retain their exact
    request bytes.  N7 and N8 are explicit deployment identities only; they do
    not alter the mounted frozen index package or its content digest.
    """

    candidates = None
    if candidate_object_ids is not None:
        candidates = sorted(
            _identifier(value, "candidate_object_id")
            for value in candidate_object_ids
        )
    request = {
        "schema_version": INDEX_QUERY_REQUEST_SCHEMA_VERSION,
        "request_id": request_id,
        "query_id": query_id,
        "requested_node_id": _serving_node_id(
            requested_node_id,
            "requested_node_id",
        ),
        "index_id": index_id,
        "query_text": query_text,
        "top_k": top_k,
        "candidate_object_ids": candidates,
        "credentials_recorded": False,
    }
    _validate_query_request(request)
    return request


def _validate_query_request(value: Any) -> dict[str, Any]:
    request = dict(_mapping(value, "index query request"))
    _exact_fields(request, _REQUEST_FIELDS, "index query request")
    _require(
        request.get("schema_version") == INDEX_QUERY_REQUEST_SCHEMA_VERSION,
        "unsupported query request schema_version",
    )
    _identifier(request.get("request_id"), "request_id")
    _identifier(request.get("query_id"), "query_id")
    _serving_node_id(request.get("requested_node_id"), "requested_node_id")
    _identifier(request.get("index_id"), "index_id")
    query_text = _text(
        request.get("query_text"),
        "query_text",
        max_bytes=_MAX_QUERY_BYTES,
    )
    _require(bool(_tokens(query_text)), "query has no indexable tokens")
    top_k = _positive_integer(request.get("top_k"), "top_k")
    _require(top_k <= _MAX_TOP_K, "top_k exceeds the safety limit")
    raw_candidates = request.get("candidate_object_ids")
    if raw_candidates is not None:
        candidates = [
            _identifier(item, "candidate_object_id")
            for item in _array(raw_candidates, "candidate_object_ids")
        ]
        _require(bool(candidates), "candidate_object_ids must not be empty")
        _require(
            candidates == sorted(set(candidates)),
            "candidate_object_ids must be sorted and unique",
        )
    _require(
        request.get("credentials_recorded") is False,
        "query request must record credentials_recorded=false",
    )
    return request


class N2IndexService:
    """In-process implementation used unchanged behind local or cloud HTTP.

    ``node_id`` identifies the logical service instance.  The package remains
    the endpoint-free N2-authored frozen content artifact, allowing identical
    bytes to back the N2 global index and N7/N8 local-index roles.
    """

    def __init__(
        self,
        package_dir: str | Path,
        *,
        node_id: str = N2_NODE_ID,
    ):
        self.package_dir = Path(package_dir).resolve()
        self.artifact, self.package = _load_verified_package(self.package_dir)
        self.node_id = _serving_node_id(node_id)
        self._documents = {
            item["object_id"]: item for item in self.artifact["documents"]
        }

    def health(self) -> dict[str, Any]:
        return {
            "schema_version": INDEX_SERVICE_API_VERSION,
            "status": "ok",
            "node_id": self.node_id,
            "index_id": self.artifact["index_id"],
            "index_sha256": self.package["index_sha256"],
            "document_count": self.artifact["document_count"],
            "algorithm": INDEX_ALGORITHM,
            "credentials_recorded": False,
        }

    def query(self, value: Mapping[str, Any]) -> dict[str, Any]:
        request = _validate_query_request(value)
        _require(
            request["requested_node_id"] == self.node_id,
            f"query must target this logical index node ({self.node_id})",
        )
        _require(
            request["index_id"] == self.artifact["index_id"],
            "query index_id does not match the mounted index",
        )
        all_ids = list(self.artifact["candidate_object_ids"])
        requested_ids = request["candidate_object_ids"]
        candidate_ids = all_ids if requested_ids is None else list(requested_ids)
        unknown = sorted(set(candidate_ids) - set(all_ids))
        _require(not unknown, "query names an unknown candidate object")
        _require(
            request["top_k"] <= len(candidate_ids),
            "top_k exceeds candidate count",
        )

        query_tokens = _tokens(request["query_text"])
        query_counts: dict[str, int] = {}
        for token in query_tokens:
            query_counts[token] = query_counts.get(token, 0) + 1
        weights = self.artifact["term_weight_units"]
        scored: list[tuple[int, int, str, list[str]]] = []
        for object_id in candidate_ids:
            document = self._documents[object_id]
            frequencies = document["term_frequencies"]
            matched = sorted(set(query_counts).intersection(frequencies))
            score = sum(
                query_counts[term]
                * min(int(frequencies[term]), 8)
                * int(weights[term])
                for term in matched
            )
            scored.append((len(matched), score, object_id, matched))
        scored.sort(key=lambda item: (-item[0], -item[1], item[2]))
        ranked = []
        for rank, (_, score, object_id, matched) in enumerate(
            scored[: request["top_k"]], start=1
        ):
            document = self._documents[object_id]
            ranked.append(
                {
                    "rank": rank,
                    "object_id": object_id,
                    "source_object_group": document["source_object_group"],
                    "lexical_score_units": score,
                    "matched_terms": matched,
                    "visible_fields_sha256": document["visible_fields_sha256"],
                }
            )
        result = {
            "schema_version": INDEX_QUERY_RESULT_SCHEMA_VERSION,
            "status": "COMPLETED",
            "request_id": request["request_id"],
            "query_id": request["query_id"],
            "node_id": self.node_id,
            "index_id": self.artifact["index_id"],
            "index_sha256": self.package["index_sha256"],
            "source_manifest_sha256": self.artifact["source_manifest_sha256"],
            "request_sha256": _value_sha256(request),
            "query_text_sha256": _sha256(request["query_text"].encode("utf-8")),
            "candidate_set_sha256": _value_sha256(candidate_ids),
            "ranking_sha256": _value_sha256(ranked),
            "algorithm": INDEX_ALGORITHM,
            "tokenizer": INDEX_TOKENIZER,
            "query_token_count": len(query_tokens),
            "candidate_count": len(candidate_ids),
            "top_k": request["top_k"],
            "ranked_candidates": ranked,
            "lexical_retrieval_executed": True,
            "llm_called": False,
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        }
        result["result_content_sha256"] = _value_sha256(result)
        return result

    def query_public(self, value: Mapping[str, Any]) -> dict[str, Any]:
        """Return the deterministic ranking without private source grouping.

        Source grouping remains available to trusted index maintenance code,
        but it never crosses this public query boundary.  The public result is
        independently content-addressed after redaction.
        """

        internal = self.query(value)
        ranked = [
            {
                "rank": row["rank"],
                "object_id": row["object_id"],
                "lexical_score_units": row["lexical_score_units"],
                "matched_terms": list(row["matched_terms"]),
                "visible_fields_sha256": row["visible_fields_sha256"],
            }
            for row in internal["ranked_candidates"]
        ]
        result = {
            key: value
            for key, value in internal.items()
            if key not in {"schema_version", "ranked_candidates", "ranking_sha256",
                           "result_content_sha256"}
        }
        result["schema_version"] = INDEX_PUBLIC_QUERY_RESULT_SCHEMA_VERSION
        result["ranked_candidates"] = ranked
        result["ranking_sha256"] = _value_sha256(ranked)
        result["result_content_sha256"] = _value_sha256(result)
        return result


def _validate_result_envelope(
    request_value: Mapping[str, Any],
    result_value: Mapping[str, Any],
    *,
    expected_index_sha256: str,
    expected_node_id: str = N2_NODE_ID,
) -> dict[str, Any]:
    request = _validate_query_request(request_value)
    expected_node_id = _serving_node_id(expected_node_id, "expected_node_id")
    _require(
        request["requested_node_id"] == expected_node_id,
        "query requested_node_id differs from the expected logical index node",
    )
    result = dict(_mapping(result_value, "index query result"))
    _exact_fields(result, _RESULT_FIELDS, "index query result")
    _require(
        result.get("schema_version") == INDEX_QUERY_RESULT_SCHEMA_VERSION,
        "unsupported query result schema_version",
    )
    _require(result.get("status") == "COMPLETED", "query did not complete")
    _require(result.get("request_id") == request["request_id"], "request_id mismatch")
    _require(result.get("query_id") == request["query_id"], "query_id mismatch")
    _require(
        result.get("node_id") == expected_node_id,
        "query result did not come from the expected logical index node",
    )
    _require(result.get("index_id") == request["index_id"], "index_id mismatch")
    _require(
        _digest(result.get("index_sha256"), "index_sha256")
        == _digest(expected_index_sha256, "expected_index_sha256"),
        "index digest mismatch",
    )
    _digest(result.get("source_manifest_sha256"), "source_manifest_sha256")
    _require(
        result.get("request_sha256") == _value_sha256(request),
        "request digest mismatch",
    )
    _require(
        result.get("query_text_sha256")
        == _sha256(request["query_text"].encode("utf-8")),
        "query text digest mismatch",
    )
    candidate_ids = request["candidate_object_ids"]
    if candidate_ids is not None:
        _require(
            result.get("candidate_set_sha256") == _value_sha256(candidate_ids),
            "candidate set digest mismatch",
        )
        _require(
            result.get("candidate_count") == len(candidate_ids),
            "candidate count mismatch",
        )
    else:
        _digest(result.get("candidate_set_sha256"), "candidate_set_sha256")
        _positive_integer(result.get("candidate_count"), "candidate_count")
    ranked = _array(result.get("ranked_candidates"), "ranked_candidates")
    _require(len(ranked) == request["top_k"], "ranked candidate count mismatch")
    seen: set[str] = set()
    for position, item in enumerate(ranked, start=1):
        row = _mapping(item, f"ranked_candidates[{position - 1}]")
        _exact_fields(row, _RANKED_FIELDS, "ranked candidate")
        _require(row.get("rank") == position, "ranking is not contiguous")
        object_id = _identifier(row.get("object_id"), "ranked object_id")
        _require(object_id not in seen, "ranking repeats an object")
        seen.add(object_id)
        if candidate_ids is not None:
            _require(object_id in candidate_ids, "ranking contains a non-candidate")
        _identifier(row.get("source_object_group"), "source_object_group")
        _require(
            type(row.get("lexical_score_units")) is int
            and row["lexical_score_units"] >= 0,
            "lexical_score_units is invalid",
        )
        matched = _array(row.get("matched_terms"), "matched_terms")
        _require(
            matched == sorted(set(matched))
            and all(isinstance(term, str) and bool(term) for term in matched),
            "matched_terms are not canonical",
        )
        _digest(row.get("visible_fields_sha256"), "visible_fields_sha256")
    _require(
        result.get("ranking_sha256") == _value_sha256(ranked),
        "ranking digest mismatch",
    )
    _require(result.get("algorithm") == INDEX_ALGORITHM, "result algorithm changed")
    _require(result.get("tokenizer") == INDEX_TOKENIZER, "result tokenizer changed")
    _require(
        result.get("query_token_count") == len(_tokens(request["query_text"])),
        "query token count mismatch",
    )
    _require(result.get("top_k") == request["top_k"], "top_k mismatch")
    _require(
        result.get("lexical_retrieval_executed") is True,
        "retrieval was not executed",
    )
    for field in (
        "llm_called",
        "credentials_recorded",
        "eligible_for_scientific_claims",
    ):
        _require(result.get(field) is False, f"query result {field} must be false")
    content_digest = _digest(
        result.get("result_content_sha256"),
        "result_content_sha256",
    )
    core = dict(result)
    del core["result_content_sha256"]
    _require(content_digest == _value_sha256(core), "result content digest mismatch")
    return result


def _validate_public_result_envelope(
    request_value: Mapping[str, Any],
    result_value: Mapping[str, Any],
    *,
    expected_index_sha256: str,
    expected_node_id: str = N2_NODE_ID,
) -> dict[str, Any]:
    """Validate the label-safe projection returned to a public W4 ranker."""

    request = _validate_query_request(request_value)
    expected_node_id = _serving_node_id(expected_node_id, "expected_node_id")
    result = dict(_mapping(result_value, "public index query result"))
    _exact_fields(result, _RESULT_FIELDS, "public index query result")
    _require(
        result.get("schema_version") == INDEX_PUBLIC_QUERY_RESULT_SCHEMA_VERSION,
        "unsupported public query result schema_version",
    )
    _require(result.get("status") == "COMPLETED", "public query did not complete")
    _require(result.get("request_id") == request["request_id"], "request_id mismatch")
    _require(result.get("query_id") == request["query_id"], "query_id mismatch")
    _require(
        result.get("node_id") == expected_node_id,
        "public query result came from the wrong logical index node",
    )
    _require(result.get("index_id") == request["index_id"], "index_id mismatch")
    _require(
        _digest(result.get("index_sha256"), "index_sha256")
        == _digest(expected_index_sha256, "expected_index_sha256"),
        "index digest mismatch",
    )
    _digest(result.get("source_manifest_sha256"), "source_manifest_sha256")
    _require(
        result.get("request_sha256") == _value_sha256(request),
        "request digest mismatch",
    )
    _require(
        result.get("query_text_sha256")
        == _sha256(request["query_text"].encode("utf-8")),
        "query text digest mismatch",
    )
    candidate_ids = request["candidate_object_ids"]
    if candidate_ids is not None:
        _require(
            result.get("candidate_set_sha256") == _value_sha256(candidate_ids)
            and result.get("candidate_count") == len(candidate_ids),
            "candidate binding mismatch",
        )
    else:
        _digest(result.get("candidate_set_sha256"), "candidate_set_sha256")
        _positive_integer(result.get("candidate_count"), "candidate_count")
    ranked = _array(result.get("ranked_candidates"), "ranked_candidates")
    _require(len(ranked) == request["top_k"], "ranked candidate count mismatch")
    seen: set[str] = set()
    ranking_keys: list[tuple[int, int, str]] = []
    for position, item in enumerate(ranked, start=1):
        row = _mapping(item, f"ranked_candidates[{position - 1}]")
        _exact_fields(row, _PUBLIC_RANKED_FIELDS, "public ranked candidate")
        _require(row.get("rank") == position, "ranking is not contiguous")
        object_id = _identifier(row.get("object_id"), "ranked object_id")
        _require(object_id not in seen, "ranking repeats an object")
        seen.add(object_id)
        if candidate_ids is not None:
            _require(object_id in candidate_ids, "ranking contains a non-candidate")
        score = row.get("lexical_score_units")
        _require(
            type(score) is int and score >= 0,
            "lexical_score_units is invalid",
        )
        matched = _array(row.get("matched_terms"), "matched_terms")
        _require(
            matched == sorted(set(matched))
            and all(isinstance(term, str) and bool(term) for term in matched),
            "matched_terms are not canonical",
        )
        _digest(row.get("visible_fields_sha256"), "visible_fields_sha256")
        ranking_keys.append((-len(matched), -score, object_id))
    _require(
        ranking_keys == sorted(ranking_keys),
        "public lexical ranking order changed",
    )
    _require(
        result.get("ranking_sha256") == _value_sha256(ranked),
        "ranking digest mismatch",
    )
    _require(result.get("algorithm") == INDEX_ALGORITHM, "result algorithm changed")
    _require(result.get("tokenizer") == INDEX_TOKENIZER, "result tokenizer changed")
    _require(
        result.get("query_token_count") == len(_tokens(request["query_text"])),
        "query token count mismatch",
    )
    _require(result.get("top_k") == request["top_k"], "top_k mismatch")
    _require(
        result.get("lexical_retrieval_executed") is True
        and result.get("llm_called") is False
        and result.get("credentials_recorded") is False
        and result.get("eligible_for_scientific_claims") is False,
        "public query result claim boundary changed",
    )
    supplied = _digest(
        result.get("result_content_sha256"),
        "result_content_sha256",
    )
    core = dict(result)
    del core["result_content_sha256"]
    _require(supplied == _value_sha256(core), "result content digest mismatch")
    return result


def verify_n2_index_query_result(
    *,
    package_dir: str | Path,
    request: Mapping[str, Any],
    result: Mapping[str, Any],
    expected_node_id: str = N2_NODE_ID,
) -> dict[str, Any]:
    """Re-execute a query and require byte-equivalent logical evidence."""

    expected_node_id = _serving_node_id(expected_node_id, "expected_node_id")
    service = N2IndexService(package_dir, node_id=expected_node_id)
    checked = _validate_result_envelope(
        request,
        result,
        expected_index_sha256=service.package["index_sha256"],
        expected_node_id=expected_node_id,
    )
    expected = service.query(request)
    _require(checked == expected, "query result differs from deterministic replay")
    return {
        "status": "VERIFIED",
        "node_id": expected_node_id,
        "index_id": checked["index_id"],
        "request_sha256": checked["request_sha256"],
        "ranking_sha256": checked["ranking_sha256"],
        "result_content_sha256": checked["result_content_sha256"],
        "eligible_for_scientific_claims": False,
    }


def verify_n2_public_index_query_result(
    *,
    package_dir: str | Path,
    request: Mapping[str, Any],
    result: Mapping[str, Any],
    expected_node_id: str = N2_NODE_ID,
) -> dict[str, Any]:
    """Re-execute and verify the public, source-group-free projection."""

    expected_node_id = _serving_node_id(expected_node_id, "expected_node_id")
    service = N2IndexService(package_dir, node_id=expected_node_id)
    checked = _validate_public_result_envelope(
        request,
        result,
        expected_index_sha256=service.package["index_sha256"],
        expected_node_id=expected_node_id,
    )
    _require(
        checked == service.query_public(request),
        "public query result differs from deterministic replay",
    )
    return {
        "status": "VERIFIED_PUBLIC_PROJECTION",
        "node_id": expected_node_id,
        "index_id": checked["index_id"],
        "request_sha256": checked["request_sha256"],
        "ranking_sha256": checked["ranking_sha256"],
        "result_content_sha256": checked["result_content_sha256"],
        "source_object_group_included": False,
        "eligible_for_scientific_claims": False,
    }


class N2IndexHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    service: N2IndexService
    bearer_token: str | None


class _N2IndexRequestHandler(BaseHTTPRequestHandler):
    server: N2IndexHTTPServer
    server_version = "PathfinderN2Index/0.1"

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
                "schema_version": INDEX_SERVICE_API_VERSION,
                "status": "error",
                "error": {"code": code, "message": message},
                "credentials_recorded": False,
            },
        )

    def _authorized(self) -> bool:
        token = self.server.bearer_token
        supplied = self.headers.get("Authorization")
        return token is None or (
            isinstance(supplied, str)
            and hmac.compare_digest(supplied, f"Bearer {token}")
        )

    def do_GET(self) -> None:
        parsed = urlsplit(self.path)
        if parsed.path == "/healthz" and not parsed.query:
            self._write_json(HTTPStatus.OK, self.server.service.health())
            return
        self._error(HTTPStatus.NOT_FOUND, "not_found", "unknown endpoint")

    def do_POST(self) -> None:
        try:
            if self.path not in {"/v1/index/query", "/v1/index/query-public"}:
                self._error(HTTPStatus.NOT_FOUND, "not_found", "unknown endpoint")
                return
            if not self._authorized():
                self._error(
                    HTTPStatus.UNAUTHORIZED,
                    "unauthorized",
                    "invalid bearer token",
                )
                return
            _require(
                self.headers.get("X-Pathfinder-Index-Protocol-Version")
                == INDEX_SERVICE_API_VERSION,
                "missing or unsupported index protocol header",
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
            _require(length <= _MAX_REQUEST_BYTES, "query request is too large")
            raw = self.rfile.read(length)
            _require(len(raw) == length, "query request is truncated")
            request = _validate_query_request(_json_value(raw, "index query request"))
            _require(
                self.headers.get("Idempotency-Key") == request["request_id"],
                "Idempotency-Key must equal request_id",
            )
            result = (
                self.server.service.query_public(request)
                if self.path == "/v1/index/query-public"
                else self.server.service.query(request)
            )
            self._write_json(HTTPStatus.OK, result)
        except N2IndexError as exc:
            self._error(HTTPStatus.BAD_REQUEST, "invalid_request", str(exc))
        except Exception:
            self._error(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                "internal_error",
                "index query failed",
            )


def create_n2_index_http_server(
    package_dir: str | Path,
    *,
    host: str = "127.0.0.1",
    port: int = 0,
    bearer_token: str | None = None,
    node_id: str = N2_NODE_ID,
) -> N2IndexHTTPServer:
    """Create, but do not start, a logical N2/N7/N8 HTTP index service."""

    if bearer_token is not None:
        _text(bearer_token, "bearer_token", max_bytes=4096)
    server = N2IndexHTTPServer((host, port), _N2IndexRequestHandler)
    server.service = N2IndexService(package_dir, node_id=node_id)
    server.bearer_token = bearer_token
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
class N2IndexHTTPClient:
    """Deployment-bound client for the endpoint-free logical index contract."""

    base_url: str
    expected_index_id: str
    expected_index_sha256: str
    bearer_token: str | None = field(default=None, repr=False)
    timeout_seconds: float = 10.0
    simulator_private_http_hosts: tuple[str, ...] = ()
    expected_node_id: str = N2_NODE_ID

    def __post_init__(self) -> None:
        parsed = urlsplit(self.base_url)
        _require(parsed.scheme in {"http", "https"}, "index base_url scheme is invalid")
        _require(
            bool(parsed.hostname) and not parsed.query and not parsed.fragment,
            "index base_url is invalid",
        )
        _require(parsed.path in {"", "/"}, "index base_url must not contain a path")
        allowed = tuple(sorted(set(self.simulator_private_http_hosts)))
        for host in allowed:
            _require(
                _SIMULATOR_HOST.fullmatch(host) is not None,
                "simulator private host is invalid",
            )
        _require(
            parsed.scheme == "https"
            or _loopback(parsed.hostname)
            or parsed.hostname in allowed,
            "plain HTTP is allowed only for loopback or an explicit simulator host",
        )
        _identifier(self.expected_index_id, "expected_index_id")
        _digest(self.expected_index_sha256, "expected_index_sha256")
        _serving_node_id(self.expected_node_id, "expected_node_id")
        _require(
            type(self.timeout_seconds) in {int, float}
            and float(self.timeout_seconds) > 0.0,
            "timeout_seconds must be positive",
        )
        if self.bearer_token is not None:
            _text(self.bearer_token, "bearer_token", max_bytes=4096)
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
                media_type = response.headers.get_content_type()
                _require(
                    media_type == "application/json",
                    "index response Content-Type is invalid",
                )
                raw = response.read(_MAX_RESPONSE_BYTES + 1)
                _require(
                    len(raw) <= _MAX_RESPONSE_BYTES,
                    "index response is too large",
                )
                value = _json_value(raw, "index HTTP response")
        except HTTPError as exc:
            raise N2IndexHTTPError(f"N2 index returned HTTP {exc.code}") from exc
        except URLError as exc:
            raise N2IndexHTTPError("N2 index request failed") from exc
        return dict(_mapping(value, "index HTTP response"))

    def health(self) -> dict[str, Any]:
        health = self._request(Request(f"{self.base_url}/healthz", method="GET"))
        _require(health.get("status") == "ok", "index health is not ok")
        _require(
            health.get("node_id") == self.expected_node_id,
            "index endpoint has the wrong logical node identity",
        )
        _require(
            health.get("index_id") == self.expected_index_id,
            "health index_id mismatch",
        )
        _require(
            health.get("index_sha256") == self.expected_index_sha256,
            "health index digest mismatch",
        )
        _require(health.get("credentials_recorded") is False, "unsafe health response")
        return health

    def query(self, value: Mapping[str, Any]) -> dict[str, Any]:
        request_value = _validate_query_request(value)
        _require(
            request_value["requested_node_id"] == self.expected_node_id,
            "query requested_node_id differs from the client deployment binding",
        )
        _require(
            request_value["index_id"] == self.expected_index_id,
            "query index_id differs from the deployment binding",
        )
        body = _canonical_json_bytes(request_value)
        headers = {
            "Content-Type": "application/json",
            "X-Pathfinder-Index-Protocol-Version": INDEX_SERVICE_API_VERSION,
            "Idempotency-Key": request_value["request_id"],
        }
        if self.bearer_token is not None:
            headers["Authorization"] = f"Bearer {self.bearer_token}"
        result = self._request(
            Request(
                f"{self.base_url}/v1/index/query",
                data=body,
                headers=headers,
                method="POST",
            )
        )
        return _validate_result_envelope(
            request_value,
            result,
            expected_index_sha256=self.expected_index_sha256,
            expected_node_id=self.expected_node_id,
        )

    def query_public(self, value: Mapping[str, Any]) -> dict[str, Any]:
        """Query the public projection that excludes source-object grouping."""

        request_value = _validate_query_request(value)
        _require(
            request_value["requested_node_id"] == self.expected_node_id,
            "query requested_node_id differs from the client deployment binding",
        )
        _require(
            request_value["index_id"] == self.expected_index_id,
            "query index_id differs from the deployment binding",
        )
        body = _canonical_json_bytes(request_value)
        headers = {
            "Content-Type": "application/json",
            "X-Pathfinder-Index-Protocol-Version": INDEX_SERVICE_API_VERSION,
            "Idempotency-Key": request_value["request_id"],
        }
        if self.bearer_token is not None:
            headers["Authorization"] = f"Bearer {self.bearer_token}"
        result = self._request(
            Request(
                f"{self.base_url}/v1/index/query-public",
                data=body,
                headers=headers,
                method="POST",
            )
        )
        return _validate_public_result_envelope(
            request_value,
            result,
            expected_index_sha256=self.expected_index_sha256,
            expected_node_id=self.expected_node_id,
        )
