"""Fail-closed assembly for the N7/N8 full-flow semantic route service.

This module is deliberately a construction boundary.  Frozen packages are
verified and reduced to immutable identities here, while addresses and
credentials exist only in short-lived client objects.  The safe descriptor
returned to service bootstrap code contains neither endpoint values nor
credential values.

The factory consumes only the promoted, self-contained public runtime
admission.  Neither the legacy admission's private sources nor the N1 oracle
package may be mounted on N7/N8.  N1 identity is checked against a public
preselection commitment, while score evidence is verified through the remote
N1 boundary.  The handler independently rejects any request not
byte-equivalent to the verified bound trial and stage rows.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import sqlite3
import tempfile
import threading
import urllib.error
import urllib.parse
import urllib.request
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, Mapping, Sequence

from ..data_agent_client import DataAgentClientSettings, HttpDataAgentClient
from ..frame_bundle_ingest import FRAME_BUNDLE_MEDIA_TYPE
from ..integrations.flowmesh.semantic_matrix_trial import (
    GenericSemanticRouteRequestHandler,
    validate_semantic_route_request,
)
from .full_flow_cache import HttpFullFlowArtifactCacheClient
from .full_flow_exact_range_catalog import ExactFullObjectRangeCatalog
from .n3_indexed_data_plane import INDEXED_REPRESENTATION_ID
from .full_flow_index_query_plan_catalog import (
    CHECKSUMS_NAME,
    INDEX_QUERY_PLAN_CATALOG_NAME,
    INDEX_QUERY_PLAN_CATALOG_SCHEMA_VERSION,
    verify_full_flow_index_query_plan_catalog,
)
from .full_flow_local_semantic_admission import (
    load_full_flow_local_semantic_execution_inputs,
    verify_full_flow_local_semantic_runtime_package,
)
from .full_flow_n1_remote_verification import N1RemoteScoreEvidenceVerifier
from .full_flow_n6_adapters import (
    N6ModelInputAdapter,
    N6SampledFrame,
    RawVideoFrameSampler,
)
from .full_flow_provisioning_catalog import FrozenProvisioningCatalog
from .full_flow_route_adapters import (
    ApplicationShapedByteTransferAdapter,
    BoundDataAgentAccessRequestFactory,
    BoundIndexQueryAdapter,
    DataAgentArtifactSourceAdapter,
    FrozenDataAgentPlanIdCatalog,
    FrozenIndexQueryPlan,
    FrozenIndexQueryPlanCatalog,
    FrozenProvisioningReferenceAdapter,
    FullFlowRouteAdapterError,
    HttpArtifactCacheRouteAdapter,
    HttpContainerNodeSemanticClient,
    SQLiteCacheLineageStore,
    VerifiedN1HTTPScoringAdapter,
    build_http_semantic_route_adapters,
)
from .full_flow_semantic_route_runtime import (
    GenericSemanticRouteCoordinator,
    RouteExecutionStore,
)
from .hidden_oracle import N1OracleHTTPClient
from .hidden_oracle_commitment import (
    COMMITMENT_NAME as N1_COMMITMENT_NAME,
    verify_n1_oracle_preselection_commitment,
)
from .index_service import N2IndexHTTPClient, verify_n2_index_package
from .n4_derived_data_plane import (
    PACKAGE_MANIFEST_NAME as N4_MANIFEST_NAME,
)
from .raw_cold_data_plane import (
    PACKAGE_MANIFEST_NAME as N3_MANIFEST_NAME,
)


SERVICE_FACTORY_SCHEMA_VERSION = (
    "pathfinder.full-flow-semantic-route-service-factory/v1alpha1"
)
SERVICE_FACTORY_GAP_SCHEMA_VERSION = (
    "pathfinder.full-flow-semantic-route-service-factory-gap/v1alpha1"
)
SQLITE_ROUTE_STORE_SCHEMA_VERSION = (
    "pathfinder.full-flow-semantic-route-execution-store/v1alpha1"
)
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:+-]{0,255}\Z")
_RUNTIME_EPOCH = re.compile(r"[0-9a-f]{32}\Z")
_PRIVATE_SIMULATOR_HOST = re.compile(
    r"(?:pathfinder-sim|pathfinder-full-flow)-[a-z0-9-]+\Z"
)
_MAX_HEALTH_RESPONSE_BYTES = 64 * 1024
_DEFAULT_MAX_ARTIFACT_BYTES = 2 * 1024 * 1024 * 1024


class FullFlowSemanticRouteServiceFactoryError(RuntimeError):
    """Raised when service construction cannot preserve frozen semantics."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FullFlowSemanticRouteServiceFactoryError(message)


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise FullFlowSemanticRouteServiceFactoryError(
            "service-factory value is not canonical JSON"
        ) from exc


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _digest(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _SHA256.fullmatch(value) is not None,
        f"{name} is not lowercase SHA-256",
    )
    return str(value)


def _identifier(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _IDENTIFIER.fullmatch(value) is not None,
        f"{name} is invalid",
    )
    return str(value)


def _strict_json(path: Path, name: str) -> dict[str, Any]:
    _require(path.is_file() and not path.is_symlink(), f"{name} is missing")

    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            _require(key not in result, f"{name} repeats key {key}")
            result[key] = value
        return result

    try:
        value = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=unique,
            parse_constant=lambda token: (_ for _ in ()).throw(
                FullFlowSemanticRouteServiceFactoryError(
                    f"{name} contains invalid number {token}"
                )
            ),
        )
    except FullFlowSemanticRouteServiceFactoryError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowSemanticRouteServiceFactoryError(
            f"cannot read {name}"
        ) from exc
    _require(isinstance(value, dict), f"{name} must be an object")
    return value


def _strict_jsonl(path: Path, name: str) -> tuple[dict[str, Any], ...]:
    _require(path.is_file() and not path.is_symlink(), f"{name} is missing")
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise FullFlowSemanticRouteServiceFactoryError(
            f"cannot read {name}"
        ) from exc
    rows: list[dict[str, Any]] = []
    for position, line in enumerate(lines, start=1):
        _require(bool(line.strip()), f"{name} contains a blank line")
        try:
            value = json.loads(
                line,
                object_pairs_hook=lambda pairs: _unique_object(
                    pairs, f"{name} line {position}"
                ),
                parse_constant=lambda token: (_ for _ in ()).throw(
                    FullFlowSemanticRouteServiceFactoryError(
                        f"{name} line {position} contains invalid number {token}"
                    )
                ),
            )
        except FullFlowSemanticRouteServiceFactoryError:
            raise
        except json.JSONDecodeError as exc:
            raise FullFlowSemanticRouteServiceFactoryError(
                f"{name} line {position} is invalid JSON"
            ) from exc
        _require(isinstance(value, dict), f"{name} row is not an object")
        rows.append(value)
    _require(bool(rows), f"{name} is empty")
    return tuple(rows)


def _unique_object(
    pairs: list[tuple[str, Any]],
    name: str,
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        _require(key not in result, f"{name} repeats key {key}")
        result[key] = value
    return result


@dataclass(frozen=True)
class FrozenSemanticRouteServiceSources:
    """Public, endpoint-free packages safe to mount on N7 or N8.

    The legacy admission sources and the N1 private oracle package are
    intentionally absent.  Their source-bound result is represented by the
    promoted local admission and the label-free N1 commitment.
    """

    local_admission_dir: Path
    n1_public_commitment_dir: Path
    artifact_binding_dir: Path
    n2_index_package_dir: Path
    n3_package_dir: Path
    n4_package_dir: Path
    exact_range_catalog_dir: Path
    provisioning_catalog_dir: Path
    index_query_plan_catalog_dir: Path | None = None

    def __post_init__(self) -> None:
        for name in (
            "local_admission_dir",
            "n1_public_commitment_dir",
            "artifact_binding_dir",
            "n2_index_package_dir",
            "n3_package_dir",
            "n4_package_dir",
            "exact_range_catalog_dir",
            "provisioning_catalog_dir",
        ):
            object.__setattr__(self, name, Path(getattr(self, name)).resolve())
        if self.index_query_plan_catalog_dir is not None:
            object.__setattr__(
                self,
                "index_query_plan_catalog_dir",
                Path(self.index_query_plan_catalog_dir).resolve(),
            )


@dataclass(frozen=True, repr=False)
class RuntimeSemanticServiceInputs:
    """Runtime-only origins, tokens, and public service identities.

    ``repr`` is intentionally redacted.  Instances must never be serialized;
    the service assembly retains only constructed clients.
    """

    logical_node_id: str
    index_base_urls: Mapping[str, str]
    index_bearer_tokens: Mapping[str, str | None]
    data_agent_base_urls: Mapping[str, str]
    data_agent_bearer_tokens: Mapping[str, str]
    cache_base_urls: Mapping[str, str]
    cache_bearer_tokens: Mapping[str, str]
    cache_ids: Mapping[str, str]
    node_health_base_urls: Mapping[str, str]
    n6_base_url: str = field(repr=False)
    n6_bearer_token: str = field(repr=False)
    n1_base_url: str = field(repr=False)
    n1_bearer_token: str = field(repr=False)
    n1_verification_base_url: str = field(repr=False)
    n1_verification_bearer_token: str = field(repr=False)
    semantic_model: str
    application_transfer_profile_id: str | None = None
    application_transfer_bandwidth_bytes_per_second: float | None = None
    application_transfer_round_trip_time_ms: float | None = None
    timeout_seconds: float = 300.0
    max_artifact_bytes: int = _DEFAULT_MAX_ARTIFACT_BYTES
    simulator_private_http_hosts: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require(
            self.logical_node_id in {"N7", "N8"},
            "semantic route service must be N7 or N8",
        )
        shaping = (
            self.application_transfer_profile_id,
            self.application_transfer_bandwidth_bytes_per_second,
            self.application_transfer_round_trip_time_ms,
        )
        _require(
            all(value is None for value in shaping)
            or all(value is not None for value in shaping),
            "application transfer shaping must be fully specified",
        )
        if self.application_transfer_profile_id is not None:
            _identifier(
                self.application_transfer_profile_id,
                "application transfer profile ID",
            )
            bandwidth = self.application_transfer_bandwidth_bytes_per_second
            rtt = self.application_transfer_round_trip_time_ms
            _require(
                not isinstance(bandwidth, bool)
                and isinstance(bandwidth, (int, float))
                and math.isfinite(float(bandwidth))
                and float(bandwidth) > 0.0,
                "application transfer bandwidth must be positive",
            )
            _require(
                not isinstance(rtt, bool)
                and isinstance(rtt, (int, float))
                and math.isfinite(float(rtt))
                and float(rtt) >= 0.0,
                "application transfer RTT must be non-negative",
            )
        mappings = {
            "index_base_urls": (self.index_base_urls, {"N2", "N7", "N8"}),
            "index_bearer_tokens": (
                self.index_bearer_tokens,
                {"N2", "N7", "N8"},
            ),
            "data_agent_base_urls": (
                self.data_agent_base_urls,
                {"N3", "N4"},
            ),
            "data_agent_bearer_tokens": (
                self.data_agent_bearer_tokens,
                {"N3", "N4"},
            ),
            "cache_base_urls": (self.cache_base_urls, {"N7", "N8"}),
            "cache_bearer_tokens": (
                self.cache_bearer_tokens,
                {"N7", "N8"},
            ),
            "cache_ids": (self.cache_ids, {"N7", "N8"}),
            "node_health_base_urls": (
                self.node_health_base_urls,
                {"N7", "N8"},
            ),
        }
        for name, (value, expected) in mappings.items():
            _require(
                isinstance(value, Mapping) and set(value) == expected,
                f"{name} must bind exactly {sorted(expected)}",
            )
            object.__setattr__(self, name, MappingProxyType(dict(value)))
        for name, token in {
            **dict(self.data_agent_bearer_tokens),
            **dict(self.cache_bearer_tokens),
            "N6": self.n6_bearer_token,
            "N1-score": self.n1_bearer_token,
            "N1-verify": self.n1_verification_bearer_token,
        }.items():
            _require(
                isinstance(token, str)
                and 16 <= len(token.encode("utf-8")) <= 8192,
                f"{name} bearer token length is invalid",
            )
        for node, token in self.index_bearer_tokens.items():
            _require(
                token is None
                or (
                    isinstance(token, str)
                    and 16 <= len(token.encode("utf-8")) <= 8192
                ),
                f"{node} index bearer token length is invalid",
            )
        _identifier(self.semantic_model, "semantic_model")
        _require(
            type(self.timeout_seconds) in {int, float}
            and float(self.timeout_seconds) > 0.0,
            "timeout_seconds must be positive",
        )
        _require(
            type(self.max_artifact_bytes) is int
            and self.max_artifact_bytes > 0,
            "max_artifact_bytes must be a positive integer",
        )
        hosts = tuple(sorted(set(self.simulator_private_http_hosts)))
        _require(
            all(_PRIVATE_SIMULATOR_HOST.fullmatch(host) for host in hosts),
            "simulator private host allowlist is invalid",
        )
        object.__setattr__(self, "simulator_private_http_hosts", hosts)

    def __repr__(self) -> str:
        return (
            "RuntimeSemanticServiceInputs("
            f"logical_node_id={self.logical_node_id!r}, runtime_values=<redacted>)"
        )


class SQLiteRouteExecutionStore(RouteExecutionStore):
    """Durable exactly-once route evidence keyed by execution identity."""

    def __init__(self, database: str | Path, *, logical_node_id: str) -> None:
        _require(logical_node_id in {"N7", "N8"}, "route-store node is invalid")
        self._path = Path(database).resolve()
        self._path.parent.mkdir(parents=True, exist_ok=True)
        _require(
            self._path.parent.is_dir() and not self._path.parent.is_symlink(),
            "route-store parent must be a regular directory",
        )
        self._node = logical_node_id
        self._lock = threading.RLock()
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS route_store_metadata (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    schema_version TEXT NOT NULL,
                    logical_node_id TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS route_executions (
                    execution_id TEXT PRIMARY KEY,
                    request_sha256 TEXT NOT NULL,
                    state TEXT NOT NULL,
                    evidence_json TEXT,
                    failure_sha256 TEXT
                )
                """
            )
            row = connection.execute(
                "SELECT * FROM route_store_metadata WHERE singleton = 1"
            ).fetchone()
            if row is None:
                connection.execute(
                    "INSERT INTO route_store_metadata VALUES (1, ?, ?)",
                    (SQLITE_ROUTE_STORE_SCHEMA_VERSION, logical_node_id),
                )
            else:
                _require(
                    row["schema_version"] == SQLITE_ROUTE_STORE_SCHEMA_VERSION
                    and row["logical_node_id"] == logical_node_id,
                    "route-store identity changed",
                )
            connection.commit()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._path, timeout=30.0)
        connection.row_factory = sqlite3.Row
        return connection

    @staticmethod
    def _key(execution_id: str, request_sha256: str) -> tuple[str, str]:
        return (
            _digest(execution_id, "execution_id"),
            _digest(request_sha256, "request_sha256"),
        )

    def begin(
        self,
        execution_id: str,
        request_sha256: str,
    ) -> Mapping[str, Any] | None:
        key = self._key(execution_id, request_sha256)
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM route_executions WHERE execution_id = ?",
                (key[0],),
            ).fetchone()
            if row is None:
                connection.execute(
                    "INSERT INTO route_executions VALUES (?, ?, 'RUNNING', NULL, NULL)",
                    key,
                )
                connection.commit()
                return None
            _require(
                row["request_sha256"] == key[1],
                "execution_id was reused for different frozen input",
            )
            _require(
                row["state"] != "RUNNING",
                "route execution is already in progress",
            )
            _require(
                row["state"] == "COMPLETE"
                and isinstance(row["evidence_json"], str)
                and row["failure_sha256"] is None,
                "failed route execution cannot be replayed ambiguously",
            )
            try:
                evidence = json.loads(row["evidence_json"])
            except json.JSONDecodeError as exc:
                raise FullFlowSemanticRouteServiceFactoryError(
                    "stored route evidence is invalid"
                ) from exc
            _require(
                isinstance(evidence, dict)
                and _canonical(evidence).decode("utf-8") == row["evidence_json"],
                "stored route evidence is not canonical",
            )
            connection.commit()
            return evidence

    def complete(
        self,
        execution_id: str,
        request_sha256: str,
        evidence: Mapping[str, Any],
    ) -> None:
        key = self._key(execution_id, request_sha256)
        encoded = _canonical(evidence).decode("utf-8")
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            changed = connection.execute(
                """
                UPDATE route_executions
                SET state = 'COMPLETE', evidence_json = ?, failure_sha256 = NULL
                WHERE execution_id = ? AND request_sha256 = ? AND state = 'RUNNING'
                """,
                (encoded, *key),
            ).rowcount
            _require(changed == 1, "route-store completion state changed")
            connection.commit()

    def fail(
        self,
        execution_id: str,
        request_sha256: str,
        reason: str,
    ) -> None:
        key = self._key(execution_id, request_sha256)
        _require(isinstance(reason, str), "route failure reason must be text")
        failure = _sha256(reason.encode("utf-8"))
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                UPDATE route_executions
                SET state = 'FAILED', evidence_json = NULL, failure_sha256 = ?
                WHERE execution_id = ? AND request_sha256 = ? AND state = 'RUNNING'
                """,
                (failure, *key),
            )
            connection.commit()


class PyAVRawVideoFrameSampler:
    """Decode routed bytes with the repository's real PyAV/Pillow sampler."""

    def __init__(self, scratch_dir: str | Path) -> None:
        self._root = Path(scratch_dir).resolve()
        self._root.mkdir(parents=True, exist_ok=True)
        _require(
            self._root.is_dir() and not self._root.is_symlink(),
            "raw sampler scratch directory is invalid",
        )

    def __call__(
        self,
        payload: bytes,
        *,
        object_id: str,
        source_payload_sha256: str,
        frame_count: int,
        jpeg_max_dimension: int,
        temporal_start_fraction: float,
        temporal_end_fraction: float,
    ) -> Sequence[N6SampledFrame]:
        _identifier(object_id, "raw sampler object_id")
        _require(isinstance(payload, bytes) and bool(payload), "raw payload is empty")
        _require(
            _sha256(payload) == _digest(
                source_payload_sha256, "source_payload_sha256"
            ),
            "raw sampler payload digest changed",
        )
        from ..video_prep import sample_video

        descriptor, raw_path = tempfile.mkstemp(
            prefix="pathfinder-raw-route-",
            suffix=".mp4",
            dir=self._root,
        )
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            sampled, _duration = sample_video(
                Path(raw_path),
                frame_count=frame_count,
                jpeg_max_dimension=jpeg_max_dimension,
                temporal_start_fraction=temporal_start_fraction,
                temporal_end_fraction=temporal_end_fraction,
            )
            return tuple(
                N6SampledFrame(
                    frame_index=value.frame_index,
                    timestamp_seconds=value.timestamp_seconds,
                    width=value.width,
                    height=value.height,
                    jpeg_bytes=value.jpeg_bytes,
                )
                for value in sampled
            )
        finally:
            Path(raw_path).unlink(missing_ok=True)


class _RejectingIndexQueryPlans:
    def resolve(self, **_kwargs: Any) -> FrozenIndexQueryPlan:
        raise FullFlowRouteAdapterError(
            "no verified frozen index-query-plan catalog is bound"
        )


class _NodeHealthProbe:
    """Proxy-free, bounded health probe used only for cache epoch lineage."""

    def __init__(
        self,
        *,
        base_url: str,
        expected_node_id: str,
        timeout_seconds: float,
        simulator_private_http_hosts: Sequence[str],
    ) -> None:
        parsed = urllib.parse.urlsplit(base_url)
        private = tuple(sorted(set(simulator_private_http_hosts)))
        loopback = parsed.hostname in {"127.0.0.1", "localhost", "::1"}
        _require(
            parsed.scheme in {"http", "https"}
            and parsed.hostname is not None
            and parsed.username is None
            and parsed.password is None
            and parsed.path in {"", "/"}
            and not parsed.query
            and not parsed.fragment,
            "node health base_url is invalid",
        )
        _require(
            parsed.scheme == "https" or loopback or parsed.hostname in private,
            "plain node-health HTTP requires loopback or simulator host",
        )
        _require(expected_node_id in {"N7", "N8"}, "health node is invalid")
        self._base = base_url.rstrip("/")
        self._node = expected_node_id
        self._timeout = float(timeout_seconds)
        handlers: list[Any] = [_NoRedirects()]
        if loopback or parsed.hostname in private:
            handlers.insert(0, urllib.request.ProxyHandler({}))
        self._open = urllib.request.build_opener(*handlers).open

    def __call__(self) -> Mapping[str, Any]:
        request = urllib.request.Request(
            self._base + "/healthz",
            headers={"Accept": "application/json"},
            method="GET",
        )
        try:
            with self._open(request, timeout=self._timeout) as response:
                _require(
                    response.status == 200
                    and response.headers.get_content_type() == "application/json",
                    "node health HTTP response is invalid",
                )
                raw = response.read(_MAX_HEALTH_RESPONSE_BYTES + 1)
        except urllib.error.HTTPError as exc:
            raise FullFlowSemanticRouteServiceFactoryError(
                f"node health returned HTTP {exc.code}"
            ) from exc
        except (urllib.error.URLError, OSError) as exc:
            raise FullFlowSemanticRouteServiceFactoryError(
                "node health request failed"
            ) from exc
        _require(
            len(raw) <= _MAX_HEALTH_RESPONSE_BYTES,
            "node health response is too large",
        )
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise FullFlowSemanticRouteServiceFactoryError(
                "node health response is not JSON"
            ) from exc
        _require(
            isinstance(value, dict)
            and value.get("status") == "ok"
            and value.get("node_id") == self._node
            and _RUNTIME_EPOCH.fullmatch(str(value.get("runtime_epoch")))
            and value.get("credentials_recorded") is False,
            "node health identity is invalid",
        )
        return value


class _NoRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self,
        req: Any,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        del req, fp, code, msg, headers, newurl
        return None


class FrozenCatalogBoundSemanticRouteRequestHandler:
    """Reject requests outside one verified admission catalog and node."""

    def __init__(
        self,
        *,
        logical_node_id: str,
        delegate: GenericSemanticRouteRequestHandler,
        bound_trials: Sequence[Mapping[str, Any]],
        bound_stages: Sequence[Mapping[str, Any]],
        bound_cache_episodes: Mapping[tuple[str, str], str] | None = None,
    ) -> None:
        _require(logical_node_id in {"N7", "N8"}, "handler node is invalid")
        self._node = logical_node_id
        self._delegate = delegate
        self._trials: dict[str, dict[str, Any]] = {}
        self._stages: dict[str, dict[str, Any]] = {}
        for raw in bound_trials:
            trial = json.loads(_canonical(raw))
            key = trial.get("trial_key")
            _require(
                isinstance(key, str) and key and key not in self._trials,
                "bound trial catalog repeats an identity",
            )
            self._trials[key] = trial
        for raw in bound_stages:
            stage = json.loads(_canonical(raw))
            key = stage.get("stage_key")
            _require(
                isinstance(key, str) and key and key not in self._stages,
                "bound stage catalog repeats an identity",
            )
            self._stages[key] = stage
        self._cache_episodes: dict[tuple[str, str], str] = {}
        for key, episode_id in (bound_cache_episodes or {}).items():
            _require(
                isinstance(key, tuple) and len(key) == 2
                and all(isinstance(part, str) and part for part in key)
                and isinstance(episode_id, str) and episode_id,
                "bound cache episode identity is invalid",
            )
            run_id, trial_key = key
            _identifier(run_id, "bound cache run_id")
            _identifier(episode_id, "bound cache episode_id")
            trial = self._trials.get(trial_key)
            _require(
                trial is not None
                and trial.get("route_family") == "local-cache-derived"
                and trial.get("executor_node_id") == self._node,
                "cache episode is not bound to a local-cache trial",
            )
            _require(
                all(existing_run != run_id for existing_run, _ in self._cache_episodes),
                "bound cache run ID is reused",
            )
            self._cache_episodes[(run_id, trial_key)] = episode_id

    def execute(self, value: Mapping[str, Any]) -> dict[str, Any]:
        request = validate_semantic_route_request(value)
        trial = request["bound_trial"]
        trial_key = trial["trial_key"]
        _require(
            trial.get("executor_node_id") == self._node,
            "semantic trial was delivered to the wrong route coordinator",
        )
        _require(
            self._trials.get(trial_key) == trial,
            "semantic trial is absent from the verified admission catalog",
        )
        episode_id = request.get("cache_episode_id")
        _require(
            episode_id is None
            or self._cache_episodes.get((request["run_id"], trial_key))
            == episode_id,
            "cache episode is absent from the verified run binding",
        )
        expected_keys = trial["semantic_stage_keys"]
        expected = [self._stages.get(key) for key in expected_keys]
        _require(
            all(stage is not None for stage in expected)
            and expected == request["bound_stages"],
            "semantic stages differ from the verified admission catalog",
        )
        return self._delegate.execute(request)


@dataclass(frozen=True, repr=False)
class SemanticRouteServiceAssembly:
    """A handler plus a credential- and endpoint-free construction report."""

    handler: FrozenCatalogBoundSemanticRouteRequestHandler = field(repr=False)
    _descriptor: Mapping[str, Any] = field(repr=False)

    @property
    def ready(self) -> bool:
        return bool(self._descriptor.get("service_start_allowed"))

    @property
    def health_descriptor(self) -> dict[str, Any]:
        return json.loads(_canonical(dict(self._descriptor)))

    def require_ready(self) -> None:
        _require(
            self.ready,
            "semantic route service assembly has unresolved runtime gaps",
        )

    def __repr__(self) -> str:
        return (
            "SemanticRouteServiceAssembly("
            f"ready={self.ready}, runtime_values=<redacted>)"
        )


def _verify_sources(
    sources: FrozenSemanticRouteServiceSources,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    tuple[dict[str, Any], ...],
    tuple[dict[str, Any], ...],
]:
    try:
        admission = verify_full_flow_local_semantic_runtime_package(
            sources.local_admission_dir
        )
        inputs = load_full_flow_local_semantic_execution_inputs(
            sources.local_admission_dir
        )
        commitment = verify_n1_oracle_preselection_commitment(
            sources.n1_public_commitment_dir
        )
        index = verify_n2_index_package(sources.n2_index_package_dir)
        exact_ranges = ExactFullObjectRangeCatalog(
            sources.exact_range_catalog_dir,
            sources.n3_package_dir,
        )
        provisioning = FrozenProvisioningCatalog(
            sources.provisioning_catalog_dir,
            artifact_binding_dir=sources.artifact_binding_dir,
            n4_package_dir=sources.n4_package_dir,
        )
    except Exception as exc:
        if isinstance(exc, FullFlowSemanticRouteServiceFactoryError):
            raise
        raise FullFlowSemanticRouteServiceFactoryError(
            f"frozen semantic service source verification failed: "
            f"{type(exc).__name__}"
        ) from exc
    admission_document = dict(inputs.admission)
    trials = tuple(dict(value) for value in inputs.bound_trials)
    stages = tuple(dict(value) for value in inputs.bound_stages)
    commitment_document = _strict_json(
        sources.n1_public_commitment_dir / N1_COMMITMENT_NAME,
        "public N1 preselection commitment",
    )
    _require(
        admission_document.get("admission_sha256")
        == admission.get("admission_sha256"),
        "admission verifier returned a different commitment",
    )
    public_oracle = admission_document.get("public_oracle_binding")
    _require(
        isinstance(public_oracle, Mapping)
        and commitment.get("private_package_binding_verified") is False
        and commitment_document.get("label_values_included") is False
        and public_oracle.get("hidden_label_content_included") is False
        and public_oracle.get("n1_private_package_required_by_n7_n8_runtime")
        is False,
        "public N1 boundary reports unsafe state",
    )
    _require(
        admission.get("oracle_id") == public_oracle.get("oracle_id")
        == commitment_document.get("oracle_id")
        and admission.get("public_task_set_sha256")
        == public_oracle.get("public_task_set_sha256")
        == commitment_document.get("public_task_set_sha256"),
        "public N1 identity differs across frozen commitments",
    )
    source_commitments = admission_document.get("source_commitments")
    _require(
        isinstance(source_commitments, Mapping)
        and source_commitments.get("exact_range_catalog_sha256")
        == exact_ranges.catalog_sha256
        and source_commitments.get("preprovisioned_catalog_sha256")
        == provisioning.catalog_sha256,
        "runtime catalogs differ from the promoted admission commitments",
    )
    return (
        {**admission, "document": admission_document},
        {
            "index": index,
            "oracle": dict(public_oracle),
            "commitment": commitment,
            "exact_ranges": exact_ranges,
            "provisioning": provisioning,
        },
        trials,
        stages,
    )


def _manifest_rows(path: Path, name: str) -> dict[tuple[str, str], dict[str, Any]]:
    document = _strict_json(path, name)
    rows = document.get("objects")
    _require(isinstance(rows, list) and bool(rows), f"{name} has no objects")
    result: dict[tuple[str, str], dict[str, Any]] = {}
    for raw in rows:
        _require(isinstance(raw, Mapping), f"{name} object row is invalid")
        key = (
            _identifier(raw.get("object_id"), f"{name} object_id"),
            _identifier(
                raw.get("representation_id"),
                f"{name} representation_id",
            ),
        )
        _require(key not in result, f"{name} repeats an artifact")
        plans = raw.get("plan_ids")
        _require(
            isinstance(plans, list)
            and plans == sorted(set(plans))
            and bool(plans),
            f"{name} artifact plan IDs are invalid",
        )
        result[key] = dict(raw)
    return result


def _data_agent_plan_catalog(
    sources: FrozenSemanticRouteServiceSources,
    trials: Sequence[Mapping[str, Any]],
) -> FrozenDataAgentPlanIdCatalog:
    n3 = _manifest_rows(
        sources.n3_package_dir / N3_MANIFEST_NAME,
        "N3 package manifest",
    )
    n4 = _manifest_rows(
        sources.n4_package_dir / N4_MANIFEST_NAME,
        "N4 package manifest",
    )
    bindings: dict[tuple[str, str, str, str], str] = {}
    for trial in trials:
        trial_key = str(trial.get("trial_key"))
        design_id = _identifier(trial.get("design_id"), "trial design_id")
        identities = trial.get("representation_identities")
        _require(isinstance(identities, list), "trial identities are missing")
        for identity in identities:
            _require(isinstance(identity, Mapping), "trial identity is invalid")
            object_id = _identifier(
                identity.get("artifact_object_id"), "artifact_object_id"
            )
            representation = _identifier(
                identity.get("representation_id"), "representation_id"
            )
            node = "N3" if representation == "raw_video" else "N4"
            manifest = n3 if node == "N3" else n4
            row = manifest.get((object_id, representation))
            _require(
                row is not None,
                f"{node} manifest lacks a trial artifact",
            )
            _require(
                design_id in row["plan_ids"],
                f"{node} artifact has no exact {design_id} plan binding",
            )
            key = (trial_key, node, object_id, representation)
            bindings[key] = design_id
        if trial.get("route_family") in {"indexed-raw", "indexed-derived"}:
            raw_identities = [
                value
                for value in identities
                if isinstance(value, Mapping)
                and value.get("representation_id") == "raw_video"
            ]
            _require(
                len(raw_identities) == 1,
                "indexed trial does not bind one raw object",
            )
            object_id = _identifier(
                raw_identities[0].get("artifact_object_id"),
                "indexed artifact_object_id",
            )
            selected = n3.get((object_id, INDEXED_REPRESENTATION_ID))
            if selected is not None:
                _require(
                    design_id in selected["plan_ids"],
                    "N3 projection has no exact design plan binding",
                )
                bindings[
                    (
                        trial_key,
                        "N3",
                        object_id,
                        INDEXED_REPRESENTATION_ID,
                    )
                ] = design_id
    return FrozenDataAgentPlanIdCatalog(bindings)


def _index_plan_catalog(
    sources: FrozenSemanticRouteServiceSources,
) -> FrozenIndexQueryPlanCatalog | _RejectingIndexQueryPlans:
    root = sources.index_query_plan_catalog_dir
    if root is None:
        return _RejectingIndexQueryPlans()
    try:
        report = verify_full_flow_index_query_plan_catalog(
            root,
            local_semantic_admission_dir=sources.local_admission_dir,
            n2_index_package_dir=sources.n2_index_package_dir,
        )
    except Exception as exc:
        raise FullFlowSemanticRouteServiceFactoryError(
            "canonical index-query-plan verification failed"
        ) from exc
    _require(
        report.get("status") == "VERIFIED"
        and report.get("source_binding_checked") is True
        and report.get("w4_retrieval_quality_evaluated") is False
        and report.get("credentials_recorded") is False,
        "canonical index-query-plan verifier returned an unsafe report",
    )
    document = _strict_json(
        root / INDEX_QUERY_PLAN_CATALOG_NAME,
        "index-query-plan catalog",
    )
    _require(
        document.get("schema_version") == INDEX_QUERY_PLAN_CATALOG_SCHEMA_VERSION
        and document.get("catalog_sha256") == report.get("catalog_sha256"),
        "canonical index-query-plan identity changed after verification",
    )
    entries = document.get("entries")
    _require(isinstance(entries, list), "index-query-plan entries are missing")
    plans = tuple(
        FrozenIndexQueryPlan(
            trial_key=row.get("trial_key"),
            task_binding_sha256=row.get("task_binding_sha256"),
            index_id=row.get("index_id"),
            query_id=row.get("query_id"),
            query_text=row.get("query_text"),
            top_k=row.get("top_k"),
            candidate_object_ids=(
                None
                if row.get("candidate_object_ids") is None
                else tuple(row.get("candidate_object_ids"))
            ),
        )
        for row in entries
    )
    return FrozenIndexQueryPlanCatalog(plans)


def _gap(gap_id: str, reason: str) -> dict[str, Any]:
    return {
        "schema_version": SERVICE_FACTORY_GAP_SCHEMA_VERSION,
        "gap_id": gap_id,
        "reason": reason,
        "blocks_service_start": True,
        "requires_upcloud": False,
    }


def _safe_constructor_graph() -> list[dict[str, Any]]:
    return [
        {
            "component_id": "trial-control",
            "logical_node_ids": ["N1"],
            "adapter": "in-process-content-bound-admission",
        },
        {
            "component_id": "index-query",
            "logical_node_ids": ["N2", "N7", "N8"],
            "adapter": "node-parametric-index-http-client",
        },
        {
            "component_id": "artifact-access",
            "logical_node_ids": ["N3", "N4"],
            "adapter": "authenticated-data-agent-http-client",
        },
        {
            "component_id": "derived-provisioning",
            "logical_node_ids": ["N5", "N4"],
            "adapter": "verified-preprovisioned-reference",
        },
        {
            "component_id": "artifact-cache",
            "logical_node_ids": ["N7", "N8"],
            "adapter": "authenticated-cache-http-client-with-sqlite-lineage",
        },
        {
            "component_id": "model-input-and-inference",
            "logical_node_ids": ["N6"],
            "adapter": "semantic-container-node-http-client",
        },
        {
            "component_id": "hidden-score-request",
            "logical_node_ids": ["N1"],
            "adapter": "authenticated-oracle-score-http-client",
        },
        {
            "component_id": "hidden-score-verification",
            "logical_node_ids": ["N1"],
            "adapter": "authenticated-remote-n1-evidence-verifier",
        },
        {
            "component_id": "route-idempotency",
            "logical_node_ids": ["N7", "N8"],
            "adapter": "sqlite-exactly-once-store",
        },
    ]


def assemble_full_flow_semantic_route_service(
    sources: FrozenSemanticRouteServiceSources,
    runtime: RuntimeSemanticServiceInputs,
    *,
    state_dir: str | Path,
    n1_score_evidence_verifier: N1RemoteScoreEvidenceVerifier | None = None,
    raw_video_frame_sampler: RawVideoFrameSampler | None = None,
) -> SemanticRouteServiceAssembly:
    """Verify sources and construct one catalog-bound N7/N8 route handler.

    No health request, workflow submission, LLM call, or service lifecycle
    action occurs here.  Callers must check ``assembly.ready`` before exposing
    the handler on a listening socket.
    """

    (
        admission_report,
        core_reports,
        trials,
        stages,
    ) = _verify_sources(sources)
    admission = admission_report["document"]
    index_report = core_reports["index"]
    oracle_report = core_reports["oracle"]
    _require(
        not any(
            trial.get("route_family") == "indexed-derived"
            for trial in trials
        ),
        "indexed-derived admission lacks a source-bound multi-question "
        "N3 plan and selection catalog",
    )
    state = Path(state_dir).resolve()
    state.mkdir(parents=True, exist_ok=True)
    _require(
        state.is_dir() and not state.is_symlink(),
        "semantic route state directory is invalid",
    )

    index_plans = _index_plan_catalog(sources)
    exact_ranges = core_reports["exact_ranges"]
    plan_ids = _data_agent_plan_catalog(sources, trials)
    provisioning_catalog = core_reports["provisioning"]

    private_hosts = runtime.simulator_private_http_hosts
    index_clients = {
        node: N2IndexHTTPClient(
            base_url=runtime.index_base_urls[node],
            expected_index_id=str(index_report["index_id"]),
            expected_index_sha256=str(index_report["index_sha256"]),
            bearer_token=runtime.index_bearer_tokens[node],
            timeout_seconds=runtime.timeout_seconds,
            simulator_private_http_hosts=private_hosts,
            expected_node_id=node,
        )
        for node in ("N2", "N7", "N8")
    }
    index_adapter = BoundIndexQueryAdapter(
        clients=index_clients,
        query_plans=index_plans,
        exact_ranges=exact_ranges,
    )
    data_agent_clients = {
        node: HttpDataAgentClient(DataAgentClientSettings(
            base_url=runtime.data_agent_base_urls[node],
            token=runtime.data_agent_bearer_tokens[node],
            timeout_seconds=runtime.timeout_seconds,
            max_retries=1,
            max_artifact_bytes=runtime.max_artifact_bytes,
            simulator_private_http_hosts=private_hosts,
        ))
        for node in ("N3", "N4")
    }
    request_factory = BoundDataAgentAccessRequestFactory(
        source_locations={"N3": "origin-cold", "N4": "origin-warm"},
        plan_ids=plan_ids,
    )
    artifacts = DataAgentArtifactSourceAdapter(
        clients=data_agent_clients,
        request_factory=request_factory,
        allowed_media_types={
            "raw_video": ("video/mp4",),
            "sampled_frame_bundle": (FRAME_BUNDLE_MEDIA_TYPE,),
            INDEXED_REPRESENTATION_ID: (FRAME_BUNDLE_MEDIA_TYPE,),
        },
    )
    cache_clients = {
        node: HttpFullFlowArtifactCacheClient(
            base_url=runtime.cache_base_urls[node],
            token=runtime.cache_bearer_tokens[node],
            expected_node_id=node,
            expected_cache_id=runtime.cache_ids[node],
            timeout_seconds=runtime.timeout_seconds,
            max_artifact_bytes=runtime.max_artifact_bytes,
            simulator_private_http_hosts=private_hosts,
        )
        for node in ("N7", "N8")
    }
    epoch_probes = {
        node: _NodeHealthProbe(
            base_url=runtime.node_health_base_urls[node],
            expected_node_id=node,
            timeout_seconds=runtime.timeout_seconds,
            simulator_private_http_hosts=private_hosts,
        )
        for node in ("N7", "N8")
    }
    cache = HttpArtifactCacheRouteAdapter(
        clients=cache_clients,
        runtime_epoch_probes=epoch_probes,
        lineage=SQLiteCacheLineageStore(state / "cache-lineage.sqlite3"),
    )
    semantic_client = HttpContainerNodeSemanticClient(
        base_url=runtime.n6_base_url,
        bearer_token=runtime.n6_bearer_token,
        timeout_seconds=runtime.timeout_seconds,
        simulator_private_http_hosts=private_hosts,
    )
    n1_client = N1OracleHTTPClient(
        base_url=runtime.n1_base_url,
        expected_oracle_id=str(oracle_report["oracle_id"]),
        expected_public_task_set_sha256=str(
            oracle_report["public_task_set_sha256"]
        ),
        bearer_token=runtime.n1_bearer_token,
        timeout_seconds=runtime.timeout_seconds,
        simulator_private_http_hosts=private_hosts,
    )
    if n1_score_evidence_verifier is None:
        verifier = N1RemoteScoreEvidenceVerifier(
            base_url=runtime.n1_verification_base_url,
            expected_oracle_id=str(oracle_report["oracle_id"]),
            expected_public_task_set_sha256=str(
                oracle_report["public_task_set_sha256"]
            ),
            bearer_token=runtime.n1_verification_bearer_token,
            timeout_seconds=runtime.timeout_seconds,
            simulator_private_http_hosts=private_hosts,
        )
    else:
        _require(
            type(n1_score_evidence_verifier)
            is N1RemoteScoreEvidenceVerifier,
            "N7/N8 accepts only the remote N1 score-evidence verifier",
        )
        _require(
            n1_score_evidence_verifier.expected_oracle_id
            == oracle_report["oracle_id"]
            and n1_score_evidence_verifier.expected_public_task_set_sha256
            == oracle_report["public_task_set_sha256"],
            "remote N1 verifier identity differs from frozen commitments",
        )
        verifier = n1_score_evidence_verifier
    scorer = VerifiedN1HTTPScoringAdapter(client=n1_client, verifier=verifier)
    sampler = raw_video_frame_sampler or PyAVRawVideoFrameSampler(
        state / "raw-sampling"
    )
    adapters = build_http_semantic_route_adapters(
        index=index_adapter,
        artifacts=artifacts,
        request_factory=request_factory,
        cache=cache,
        model_input=N6ModelInputAdapter(raw_sampler=sampler),
        semantic_client=semantic_client,
        semantic_model=runtime.semantic_model,
        scorer=scorer,
        provisioning=FrozenProvisioningReferenceAdapter(
            provisioning_catalog.references
        ),
        transport=(
            None
            if runtime.application_transfer_profile_id is None
            else ApplicationShapedByteTransferAdapter(
                profile_id=runtime.application_transfer_profile_id,
                executor_node_id=runtime.logical_node_id,
                bandwidth_bytes_per_second=float(
                    runtime.application_transfer_bandwidth_bytes_per_second
                ),
                round_trip_time_ms=float(
                    runtime.application_transfer_round_trip_time_ms
                ),
            )
        ),
    )
    coordinator = GenericSemanticRouteCoordinator(
        adapters=adapters,
        store=SQLiteRouteExecutionStore(
            state / "route-executions.sqlite3",
            logical_node_id=runtime.logical_node_id,
        ),
        oracle_id=str(oracle_report["oracle_id"]),
        oracle_public_task_set_sha256=str(
            oracle_report["public_task_set_sha256"]
        ),
    )
    generic_handler = GenericSemanticRouteRequestHandler(coordinator)
    handler = FrozenCatalogBoundSemanticRouteRequestHandler(
        logical_node_id=runtime.logical_node_id,
        delegate=generic_handler,
        bound_trials=trials,
        bound_stages=stages,
    )

    gaps: list[dict[str, Any]] = []
    authorized = all(
        trial.get("flowmesh_submission_authorized") is True
        and trial.get("required_runtime_adapter_ids") == []
        for trial in trials
    )
    _require(
        admission_report.get("status")
        == "VERIFIED_PUBLIC_LOCAL_RUNTIME_INPUTS"
        and admission.get("trial_template_flowmesh_submission_authorized")
        is True
        and authorized,
        "promoted public semantic admission is not executable",
    )
    if sources.index_query_plan_catalog_dir is None:
        gaps.append(_gap(
            "frozen-index-query-plan-catalog-missing",
            "Indexed trials have no source-bound visible query-plan catalog.",
        ))
    descriptor: dict[str, Any] = {
        "schema_version": SERVICE_FACTORY_SCHEMA_VERSION,
        "status": (
            "READY_NOT_PROBED" if not gaps else "ASSEMBLED_BLOCKED"
        ),
        "logical_node_id": runtime.logical_node_id,
        "service_start_allowed": not gaps,
        "runtime_health_probed": False,
        "workflow_submitted": False,
        "llm_called": False,
        "constructor_graph": _safe_constructor_graph(),
        "frozen_source_bindings": {
            "promotion_id": admission.get("promotion_id"),
            "admission_sha256": admission.get("admission_sha256"),
            "index_id": index_report.get("index_id"),
            "index_sha256": index_report.get("index_sha256"),
            "oracle_id": oracle_report.get("oracle_id"),
            "oracle_public_task_set_sha256": oracle_report.get(
                "public_task_set_sha256"
            ),
            "exact_range_catalog_sha256": exact_ranges.catalog_sha256,
            "provisioning_catalog_sha256": (
                provisioning_catalog.catalog_sha256
            ),
            "n1_preselection_commitment_sha256": core_reports[
                "commitment"
            ].get("commitment_sha256"),
        },
        "trial_count": len(trials),
        "stage_count": len(stages),
        "runtime_gap_count": len(gaps),
        "runtime_gaps": gaps,
        "limitations": [
            "Runtime service health is checked on first use, not by assembly.",
            "N5 evidence describes a verified preprovisioned snapshot; live "
            "materialization time and cost are not measured.",
            "In-coordinator byte handoff is not a physical-network measurement.",
            "Container evidence is not UpCloud performance evidence.",
        ],
        "endpoint_values_included": False,
        "credential_values_included": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    serialized = _canonical(descriptor).decode("utf-8")
    for value in (
        *runtime.index_base_urls.values(),
        *runtime.data_agent_base_urls.values(),
        *runtime.cache_base_urls.values(),
        *runtime.node_health_base_urls.values(),
        runtime.n6_base_url,
        runtime.n1_base_url,
        runtime.n1_verification_base_url,
        verifier.base_url,
        *[
            token
            for token in runtime.index_bearer_tokens.values()
            if token is not None
        ],
        *runtime.data_agent_bearer_tokens.values(),
        *runtime.cache_bearer_tokens.values(),
        runtime.n6_bearer_token,
        runtime.n1_bearer_token,
        runtime.n1_verification_bearer_token,
        verifier.bearer_token,
    ):
        _require(value not in serialized, "runtime value entered safe descriptor")
    return SemanticRouteServiceAssembly(
        handler=handler,
        _descriptor=MappingProxyType(json.loads(_canonical(descriptor))),
    )


__all__ = [
    "CHECKSUMS_NAME",
    "FrozenCatalogBoundSemanticRouteRequestHandler",
    "FrozenSemanticRouteServiceSources",
    "FullFlowSemanticRouteServiceFactoryError",
    "INDEX_QUERY_PLAN_CATALOG_NAME",
    "INDEX_QUERY_PLAN_CATALOG_SCHEMA_VERSION",
    "PyAVRawVideoFrameSampler",
    "RuntimeSemanticServiceInputs",
    "SERVICE_FACTORY_GAP_SCHEMA_VERSION",
    "SERVICE_FACTORY_SCHEMA_VERSION",
    "SQLITE_ROUTE_STORE_SCHEMA_VERSION",
    "SQLiteRouteExecutionStore",
    "SemanticRouteServiceAssembly",
    "assemble_full_flow_semantic_route_service",
]
