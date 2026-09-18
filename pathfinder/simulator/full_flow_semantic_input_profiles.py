"""Frozen semantic-input policies for the full-flow route families.

The physical route determines which artifacts reach an execution node.  This
module separately freezes how those artifacts are presented to N6.  Keeping
that policy explicit prevents a raw video and a derived frame bundle from
silently collapsing to the same model input.

Indexed raw uses an N3-side, source-decoded temporal frame bundle.  N2 binds
that exact projection to the authoritative MP4, and N6 consumes the returned
bundle directly.  No arbitrary MP4 byte range or N6-only crop is claimed.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any


SEMANTIC_INPUT_PROFILE_SCHEMA_VERSION = (
    "pathfinder.full-flow-semantic-input-profile/v1alpha1"
)

RAW_DENSE_PROFILE_ID = "raw-dense-uniform-24-v1"
INDEXED_WINDOW_PROFILE_ID = "indexed-middle-window-8-v1"
DERIVED_SPARSE_FRAMES_PROFILE_ID = "derived-sparse-frames-4-v1"
DERIVED_SPARSE_FUSION_PROFILE_ID = "derived-sparse-fusion-4-v1"
DIGEST_ONLY_PROFILE_ID = "derived-digest-only-v1"

_ROUTE_FAMILIES = {
    "raw",
    "indexed-raw",
    "remote-derived",
    "local-cache-derived",
}
_DERIVED_REPRESENTATIONS = {
    "multimodal_digest",
    "sampled_frame_bundle",
}


class SemanticInputProfileError(ValueError):
    """Raised when a frozen profile disagrees with its trial or stage DAG."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise SemanticInputProfileError(message)


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def profile_sha256(profile: Mapping[str, Any]) -> str:
    """Return the commitment used in runtime evidence."""

    return hashlib.sha256(_canonical(profile)).hexdigest()


def _profile(
    *,
    profile_id: str,
    input_mode: str,
    representation_ids: Sequence[str],
    frame_count: int | None,
    temporal_window_fraction: tuple[float, float] | None,
    digest_included: bool,
    source_byte_range_kind: str,
) -> dict[str, Any]:
    return {
        "schema_version": SEMANTIC_INPUT_PROFILE_SCHEMA_VERSION,
        "profile_id": profile_id,
        "input_mode": input_mode,
        "representation_ids": sorted(representation_ids),
        "frame_selection": (
            None
            if frame_count is None
            else {
                "method": "uniform-midpoint",
                "frame_count": frame_count,
                "temporal_window_fraction": list(temporal_window_fraction),
            }
        ),
        "digest_included": digest_included,
        "source_byte_range_kind": source_byte_range_kind,
        "source_byte_selectivity_claimed": (
            source_byte_range_kind
            == "source-decoded-temporal-frame-bundle"
        ),
        "direct_video_input": False,
    }


def build_semantic_input_profile(
    *,
    route_family: str,
    model_input_representation_ids: Sequence[str],
) -> dict[str, Any]:
    """Build the one allowed profile for a frozen model-input frontier."""

    _require(route_family in _ROUTE_FAMILIES, "semantic route family is invalid")
    representations = tuple(sorted(model_input_representation_ids))
    _require(
        representations and len(representations) == len(set(representations)),
        "semantic model-input representations are empty or repeated",
    )
    values = set(representations)
    if route_family == "raw":
        _require(values == {"raw_video"}, "raw profile requires raw_video")
        return _profile(
            profile_id=RAW_DENSE_PROFILE_ID,
            input_mode="raw-prepared-frames",
            representation_ids=representations,
            frame_count=24,
            temporal_window_fraction=(0.0, 1.0),
            digest_included=False,
            source_byte_range_kind="complete-artifact",
        )
    if route_family == "indexed-raw":
        _require(
            values == {"raw_video"},
            "indexed profile requires raw_video",
        )
        return _profile(
            profile_id=INDEXED_WINDOW_PROFILE_ID,
            input_mode="raw-prepared-frames",
            representation_ids=representations,
            frame_count=8,
            temporal_window_fraction=(0.25, 0.75),
            digest_included=False,
            source_byte_range_kind="source-decoded-temporal-frame-bundle",
        )
    _require(
        values <= _DERIVED_REPRESENTATIONS,
        "derived profile has an unsupported representation",
    )
    if values == {"multimodal_digest"}:
        return _profile(
            profile_id=DIGEST_ONLY_PROFILE_ID,
            input_mode="digest",
            representation_ids=representations,
            frame_count=None,
            temporal_window_fraction=None,
            digest_included=True,
            source_byte_range_kind="complete-artifact",
        )
    if values == {"sampled_frame_bundle"}:
        return _profile(
            profile_id=DERIVED_SPARSE_FRAMES_PROFILE_ID,
            input_mode="frame-bundle",
            representation_ids=representations,
            frame_count=4,
            temporal_window_fraction=(0.0, 1.0),
            digest_included=False,
            source_byte_range_kind="complete-artifact",
        )
    _require(
        values == _DERIVED_REPRESENTATIONS,
        "derived fusion profile requires digest and frame bundle",
    )
    return _profile(
        profile_id=DERIVED_SPARSE_FUSION_PROFILE_ID,
        input_mode="digest+frames-fusion",
        representation_ids=representations,
        frame_count=4,
        temporal_window_fraction=(0.0, 1.0),
        digest_included=True,
        source_byte_range_kind="complete-artifact",
    )


def model_input_frontier_representation_ids(
    bound_stages: Sequence[Mapping[str, Any]],
) -> tuple[str, ...]:
    """Derive representation IDs on the frozen frontier immediately before N6."""

    by_key: dict[str, Mapping[str, Any]] = {}
    for stage in bound_stages:
        _require(isinstance(stage, Mapping), "bound semantic stage is invalid")
        key = stage.get("stage_key")
        _require(isinstance(key, str) and key, "bound stage key is invalid")
        _require(key not in by_key, "bound semantic stage key is repeated")
        by_key[key] = stage
    infer = [stage for stage in bound_stages if stage.get("action") == "infer"]
    _require(len(infer) == 1, "bound trial must contain exactly one infer stage")
    dependencies = infer[0].get("dependency_stage_keys")
    _require(
        isinstance(dependencies, list) and dependencies,
        "bound infer stage has no dependencies",
    )
    representations: set[str] = set()
    seen: set[str] = set()

    def walk(key: str, path: tuple[str, ...]) -> None:
        _require(key not in path, "bound semantic stage DAG contains a cycle")
        stage = by_key.get(key)
        _require(stage is not None, "bound semantic stage dependency is missing")
        identity = stage.get("object_representation_identity")
        if isinstance(identity, Mapping):
            representation_id = identity.get("representation_id")
            if isinstance(representation_id, str) and representation_id:
                representations.add(representation_id)
                return
        if key in seen:
            return
        seen.add(key)
        parents = stage.get("dependency_stage_keys")
        _require(isinstance(parents, list), "bound stage dependencies are invalid")
        for parent in parents:
            _require(isinstance(parent, str), "bound dependency key is invalid")
            walk(parent, (*path, key))

    for dependency in dependencies:
        _require(isinstance(dependency, str), "infer dependency key is invalid")
        walk(dependency, (str(infer[0]["stage_key"]),))
    _require(representations, "model-input frontier has no representations")
    return tuple(sorted(representations))


def validate_semantic_input_profile(
    profile: Mapping[str, Any],
    *,
    route_family: str,
    model_input_representation_ids: Sequence[str],
) -> dict[str, Any]:
    """Rebuild a profile rather than trusting its recorded parameters."""

    _require(isinstance(profile, Mapping), "semantic input profile is missing")
    expected = build_semantic_input_profile(
        route_family=route_family,
        model_input_representation_ids=model_input_representation_ids,
    )
    supplied = json.loads(_canonical(profile))
    _require(
        supplied == expected,
        "semantic input profile differs from its frozen route frontier",
    )
    return expected


__all__ = [
    "DERIVED_SPARSE_FRAMES_PROFILE_ID",
    "DERIVED_SPARSE_FUSION_PROFILE_ID",
    "DIGEST_ONLY_PROFILE_ID",
    "INDEXED_WINDOW_PROFILE_ID",
    "RAW_DENSE_PROFILE_ID",
    "SEMANTIC_INPUT_PROFILE_SCHEMA_VERSION",
    "SemanticInputProfileError",
    "build_semantic_input_profile",
    "model_input_frontier_representation_ids",
    "profile_sha256",
    "validate_semantic_input_profile",
]
