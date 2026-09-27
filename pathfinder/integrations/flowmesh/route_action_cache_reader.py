"""Read-only, content-bound cache observations for route offers.

The caller must verify the artifact catalog and runtime cache identity before
constructing this reader. An observation is re-read at choice commit; execution
still performs its own authoritative lookup and insertion-lineage checks.
"""

from __future__ import annotations

from collections.abc import Mapping
from hashlib import sha256
import json
import re
from typing import Any

from .route_action_bridge import CacheObservation, RouteActionBridgeError
from ...simulator.full_flow_route_adapters import (
    semantic_cache_episode_namespace,
)


_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")


def _require(condition: object, message: str) -> None:
    if not condition:
        raise RouteActionBridgeError(message)


def _sha(value: Any) -> str:
    return sha256(json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")).hexdigest()


class LiveCacheStatusReader:
    """Probe every required component without downloading cached payloads.

    ``expected_artifacts`` must come from a separately verified, frozen public
    artifact catalog. The catalog commitment is retained in every observation.
    Neither a hit nor a miss here replaces the runtime cache lineage check.
    """

    def __init__(
        self, *, clients: Mapping[str, Any],
        expected_artifacts: Mapping[str, Mapping[str, str]],
        artifact_catalog_sha256: str,
    ) -> None:
        _require(set(clients) == {"N7", "N8"},
                 "cache status reader requires both execution nodes")
        _require(isinstance(artifact_catalog_sha256, str)
                 and _DIGEST.fullmatch(artifact_catalog_sha256),
                 "verified artifact catalog digest is required")
        _require(bool(expected_artifacts),
                 "cache status reader lacks artifact identities")
        normalized: dict[str, dict[str, str]] = {}
        for object_id, components in expected_artifacts.items():
            _require(isinstance(object_id, str)
                     and _IDENTIFIER.fullmatch(object_id),
                     "cache object identity is invalid")
            _require(isinstance(components, Mapping) and bool(components),
                     "cache artifact components are absent")
            normalized[object_id] = {}
            for representation_id, digest in components.items():
                _require(isinstance(representation_id, str)
                         and _IDENTIFIER.fullmatch(representation_id)
                         and isinstance(digest, str)
                         and _DIGEST.fullmatch(digest),
                         "cache component identity is invalid")
                normalized[object_id][representation_id] = digest
        self._clients = dict(clients)
        self._artifacts = normalized
        self._catalog_sha = artifact_catalog_sha256

    def observe(
        self, *, question_id: str, object_id: str,
        executor_node_id: str, cache_episode_id: str,
    ) -> CacheObservation:
        _require(isinstance(question_id, str)
                 and _IDENTIFIER.fullmatch(question_id),
                 "cache observation question is invalid")
        _require(object_id in self._artifacts,
                 "cache observation object is not in artifact catalog")
        _require(executor_node_id in self._clients,
                 "cache observation node is not configured")
        _require(isinstance(cache_episode_id, str)
                 and _IDENTIFIER.fullmatch(cache_episode_id),
                 "cache observation episode is invalid")
        namespace = semantic_cache_episode_namespace(cache_episode_id)
        statuses = []
        for representation_id, digest in sorted(
            self._artifacts[object_id].items()
        ):
            status = self._clients[executor_node_id].peek(
                cache_namespace=namespace,
                object_id=object_id,
                representation_id=representation_id,
                expected_sha256=digest,
            )
            _require(status.get("status") in {"HIT", "MISS"}
                     and status.get("node_id") == executor_node_id
                     and status.get("cache_namespace") == namespace
                     and status.get("object_id") == object_id
                     and status.get("representation_id")
                     == representation_id
                     and status.get("expected_sha256") == digest
                     and status.get("payload_included") is False
                     and status.get("state_mutated") is False,
                     "cache status response is not identity-bound")
            statuses.append(status)
        state = "hit" if all(
            item["status"] == "HIT" for item in statuses
        ) else "miss"
        evidence = _sha({
            "domain": "pathfinder.route-action-cache-observation/v1",
            "question_id": question_id,
            "object_id": object_id,
            "node_id": executor_node_id,
            "cache_episode_id": cache_episode_id,
            "artifact_catalog_sha256": self._catalog_sha,
            "statuses": statuses,
        })
        return CacheObservation(
            cache_episode_id=cache_episode_id,
            state=state,
            evidence_sha256=evidence,
        )
