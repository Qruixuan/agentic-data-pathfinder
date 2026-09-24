"""Metadata-only LRU state for capacity-aware, offline cache replay.

The durable cache remains authoritative. This model shares its cache-key
function and is checked against the real cache in focused tests; it never
claims an unobserved route outcome or stores artifact payloads.
"""

from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations
import re
from typing import Mapping

from pathfinder.simulator.full_flow_cache import cache_key


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


def tight_cache_capacity(pair_bytes_by_object: Mapping[str, int]) -> int:
    """Freeze the predeclared one-video-equivalent pressure treatment.

    Every video's two entries must fit, but no two whole videos may coexist.
    A size distribution that cannot satisfy this rule is rejected before any
    quality outcome or policy comparison is inspected.
    """

    if (len(pair_bytes_by_object) != 3
            or any(type(size) is not int or size <= 0
                   for size in pair_bytes_by_object.values())):
        raise ValueError("three positive public cache payload sizes are required")
    largest = max(pair_bytes_by_object.values())
    capacity = (11 * largest + 9) // 10
    if any(a + b <= capacity for a, b in combinations(
        pair_bytes_by_object.values(), 2,
    )):
        raise ValueError("the frozen cohort cannot create two-video pressure")
    return capacity


@dataclass(frozen=True)
class ReplayCacheEntry:
    cache_namespace: str
    object_id: str
    representation_id: str
    content_sha256: str
    size_bytes: int
    last_access_sequence: int

    @property
    def key(self) -> str:
        return cache_key(
            self.object_id,
            self.representation_id,
            cache_namespace=self.cache_namespace,
        )


class ReplayCacheState:
    """One node's byte-bounded, per-representation cache state.

    ``lookup`` and ``put`` mirror the durable cache's LRU metadata rules.
    The caller must apply them in the *observed* cache-event order and use a
    measured outcome matching the resulting hit/miss vector.
    """

    def __init__(self, *, node_id: str, capacity_bytes: int) -> None:
        if node_id not in {"N7", "N8"}:
            raise ValueError("cache node must be N7 or N8")
        if type(capacity_bytes) is not int or capacity_bytes <= 0:
            raise ValueError("cache capacity must be a positive byte count")
        self.node_id = node_id
        self.capacity_bytes = capacity_bytes
        self._sequence = 0
        self._entries: dict[str, ReplayCacheEntry] = {}

    def _identity(self, namespace: str, object_id: str,
                  representation_id: str) -> str:
        return cache_key(
            object_id, representation_id, cache_namespace=namespace,
        )

    def _digest(self, digest: str) -> str:
        if not isinstance(digest, str) or _SHA256.fullmatch(digest) is None:
            raise ValueError("artifact content digest is invalid")
        return digest

    def lookup(self, *, namespace: str, object_id: str,
               representation_id: str, expected_sha256: str) -> bool:
        key = self._identity(namespace, object_id, representation_id)
        digest = self._digest(expected_sha256)
        entry = self._entries.get(key)
        if entry is None or entry.content_sha256 != digest:
            return False
        self._sequence += 1
        self._entries[key] = ReplayCacheEntry(
            namespace, object_id, representation_id, digest,
            entry.size_bytes, self._sequence,
        )
        return True

    def put(self, *, namespace: str, object_id: str,
            representation_id: str, content_sha256: str,
            size_bytes: int) -> tuple[ReplayCacheEntry, ...]:
        key = self._identity(namespace, object_id, representation_id)
        digest = self._digest(content_sha256)
        if type(size_bytes) is not int or not 0 < size_bytes <= self.capacity_bytes:
            raise ValueError("artifact size exceeds cache capacity")
        existing = self._entries.get(key)
        used = sum(entry.size_bytes for entry in self._entries.values())
        if existing is not None:
            used -= existing.size_bytes
        evicted: list[ReplayCacheEntry] = []
        while used + size_bytes > self.capacity_bytes:
            victims = (entry for candidate, entry in self._entries.items()
                       if candidate != key)
            victim = min(victims, key=lambda row: (
                row.last_access_sequence, row.key,
            ), default=None)
            if victim is None:
                raise ValueError("cache cannot select an eviction")
            del self._entries[victim.key]
            used -= victim.size_bytes
            evicted.append(victim)
        self._sequence += 1
        self._entries[key] = ReplayCacheEntry(
            namespace, object_id, representation_id, digest,
            size_bytes, self._sequence,
        )
        return tuple(evicted)

    def snapshot(self) -> dict:
        entries = sorted(self._entries.values(), key=lambda row: row.key)
        return {
            "node_id": self.node_id,
            "capacity_bytes": self.capacity_bytes,
            "used_bytes": sum(row.size_bytes for row in entries),
            "entries": [{
                "cache_key": row.key,
                "cache_namespace": row.cache_namespace,
                "object_id": row.object_id,
                "representation_id": row.representation_id,
                "content_sha256": row.content_sha256,
                "size_bytes": row.size_bytes,
                "last_access_sequence": row.last_access_sequence,
            } for row in entries],
        }

    def fork(self) -> ReplayCacheState:
        """Make an isolated branch for counterfactual evidence matching."""

        branch = ReplayCacheState(
            node_id=self.node_id, capacity_bytes=self.capacity_bytes,
        )
        branch._sequence = self._sequence
        branch._entries = dict(self._entries)
        return branch
