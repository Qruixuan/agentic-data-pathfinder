"""Materialization-aware, credential-free RSI-Exam replay.

This is an additive v2 package. It never rewrites a v1 package or pretends
that frozen warm-path outcomes measured cold materialization latency.
"""

from __future__ import annotations

import json
import os
import random
import re
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from ..simulator.n4_derived_data_plane import verify_n4_derived_data_package
from .offline_replay import (
    ACTIONS_NAME,
    BASELINE_POLICY_NAMES,
    CASES_NAME,
    CHECKSUMS_NAME,
    MANIFEST_NAME,
    OUTCOMES_NAME,
    README_NAME,
    OFFLINE_REPLAY_SCHEMA_VERSION,
    REPLAY_MODES,
    _atomic_write,
    _checksum_bytes,
    _json_bytes,
    _jsonl_bytes,
    _load_json_bytes,
    _load_jsonl,
    _policy,
    _require,
    _sha256,
    _validate_package_documents,
    _verify_checksum_directory,
    _verify_no_leakage,
    load_offline_replay_package,
)
from .temporal_index_collection import (
    verify_formal_temporal_caption_package,
    verify_formal_temporal_index_preparation,
)


V2_SCHEMA_VERSION = "pathfinder.rsi-exam-offline-replay/v1alpha2"
COMPONENTS = (
    "sampling",
    "frame_bundle",
    "captions",
    "digest",
    "index_embedding",
    "index_projection",
)
PROFILE_COMPONENTS = {
    "raw-direct-video-v1": (),
    "indexed-query-aware-temporal-selection-v1": (
        "sampling", "captions", "index_embedding", "index_projection"
    ),
    "derived-sparse-frames-4-v1": ("sampling", "frame_bundle"),
    "derived-digest-only-v1": ("sampling", "captions", "digest"),
    "derived-sparse-fusion-4-v1": (
        "sampling", "frame_bundle", "captions", "digest"
    ),
}
PROFILE_REPRESENTATION = {
    "derived-sparse-frames-4-v1": "sampled_frame_bundle",
    "derived-digest-only-v1": "multimodal_digest",
    "derived-sparse-fusion-4-v1": "sampled_frame_bundle+multimodal_digest",
}
PACKAGE_FILES = frozenset({
    MANIFEST_NAME, CASES_NAME, ACTIONS_NAME, OUTCOMES_NAME,
    README_NAME, CHECKSUMS_NAME,
})


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _object(path: Path, label: str) -> dict[str, Any]:
    value = _load_json_bytes(path.read_bytes(), label)
    _require(isinstance(value, dict), f"{label} is not an object")
    return value


def _component(
    source_bytes: int,
    output_bytes: int | None,
    *,
    source_basis: str,
    usage_unknown: bool = False,
) -> dict[str, Any]:
    _require(type(source_bytes) is int and source_bytes >= 0,
             "component source bytes are invalid")
    _require(output_bytes is None or
             (type(output_bytes) is int and output_bytes >= 0),
             "component output bytes are invalid")
    return {
        "source_bytes_charged": source_bytes,
        "source_bytes_basis": source_basis,
        "artifact_output_bytes": output_bytes,
        "latency_ms": None,
        "provider_usage": {
            "unit": "tokens",
            "input_units": None if usage_unknown else 0,
            "output_units": None if usage_unknown else 0,
            "status": (
                "historical-usage-unavailable" if usage_unknown
                else "not-applicable"
            ),
        },
        "monetary_cost_usd": None,
    }


def _costs_from_sources(
    case: Mapping[str, Any],
    *,
    preparation: Mapping[str, Any] | None,
    frame_rows: Sequence[Mapping[str, Any]] | None,
    caption_rows: Sequence[Mapping[str, Any]] | None,
    n4_rows: Mapping[str, Mapping[str, Any]],
    preparation_sha256: str | None,
    caption_sha256: str | None,
) -> dict[str, dict[str, Any]]:
    object_id = case["object_id"]
    source_size = case["index_build"]["source_bytes"]
    _require(type(source_size) is int and source_size > 0,
             "source size is invalid")
    source_sha = n4_rows["sampled_frame_bundle"]["provenance"][
        "source_content_sha256"
    ]
    _require(n4_rows["multimodal_digest"]["provenance"][
        "source_content_sha256"
    ] == source_sha, "N4 representations have different raw sources")
    if preparation is not None:
        _require(preparation["source_video_sha256"] == source_sha
                 and preparation["source_video_size_bytes"] == source_size,
                 "N4 source differs from verified preparation")
        _require(bool(frame_rows) and bool(caption_rows),
                 "materialization inputs are incomplete")
        _require(preparation_sha256 is not None and caption_sha256 is not None,
                 "materialization package digests are missing")
    for representation, derivation_id in (
        ("sampled_frame_bundle", "rsi-exam-question-independent-frame-bundle-v1"),
        ("multimodal_digest", "rsi-exam-question-independent-temporal-digest-v1"),
    ):
        row = n4_rows[representation]
        provenance = row["provenance"]
        _require(
            provenance["source_content_sha256"] == source_sha
            and provenance["derivation_id"] == derivation_id,
            "N4 artifact has unexpected derivation identity",
        )
        if preparation is not None:
            derivation = {
                "derivation_id": derivation_id,
                "object_id": object_id,
                "representation_id": representation,
                "source_video_sha256": source_sha,
                "preparation_sha256": preparation_sha256,
                "caption_package_sha256": caption_sha256,
            }
            _require(
                provenance["derivation_sha256"]
                == _sha256(_canonical(derivation)),
                "N4 artifact does not bind preparation and captions",
            )
    sampling_bytes = (
        sum(row["jpeg_size_bytes"] for row in frame_rows)
        if frame_rows is not None else None
    )
    caption_bytes = (
        sum(len(_canonical(row)) + 1 for row in caption_rows)
        if caption_rows is not None else None
    )
    return {
        "sampling": _component(
            source_size, sampling_bytes,
            source_basis="modeled-complete-object-size-proxy",
        ),
        "frame_bundle": _component(
            0, n4_rows["sampled_frame_bundle"]["artifact_size_bytes"],
            source_basis="shared-sampling-output",
        ),
        "captions": _component(
            0, caption_bytes, source_basis="shared-sampling-output",
            usage_unknown=True,
        ),
        "digest": _component(
            0, n4_rows["multimodal_digest"]["artifact_size_bytes"],
            source_basis="shared-caption-output",
        ),
        "index_embedding": _component(
            0, None, source_basis="shared-caption-output",
            usage_unknown=True,
        ),
        "index_projection": _component(
            case["index_build"]["source_bytes"],
            case["index_build"]["output_bytes"],
            source_basis="frozen-index-build-accounting",
        ),
    }


def build_offline_replay_v2(
    v1_package_dir: str | Path,
    *,
    n4_package_dir: str | Path,
    preparation_dir: str | Path | None = None,
    caption_dir: str | Path | None = None,
    output_dir: str | Path,
    package_id: str,
    builder_commit: str,
) -> dict[str, Any]:
    """Freeze v2 using only verified public, question-independent sources."""

    _require(re.fullmatch(r"[0-9a-f]{40}", builder_commit) is not None,
             "builder_commit must be a full Git SHA-1")
    _require(bool(package_id) and "/" not in package_id
             and "\\" not in package_id, "package_id is invalid")
    target = Path(output_dir).resolve()
    _require(not target.exists(), "v2 output directory already exists")
    v1 = load_offline_replay_package(v1_package_dir)
    n4_root = Path(n4_package_dir).resolve()
    n4_verified = verify_n4_derived_data_package(n4_root)
    n4 = _object(n4_root / "n4-derived-data-package.json", "N4 manifest")
    _require((preparation_dir is None) == (caption_dir is None),
             "preparation and captions must be supplied together")
    prep_verified: dict[str, Any] | None = None
    caption_verified: dict[str, Any] | None = None
    prep_by_object: dict[str, dict[str, Any]] = {}
    frame_rows: list[dict[str, Any]] = []
    caption_rows: list[dict[str, Any]] = []
    caption_cost_sha256: str | None = None
    if preparation_dir is not None and caption_dir is not None:
        prep_root = Path(preparation_dir).resolve()
        caption_root = Path(caption_dir).resolve()
        prep_verified = verify_formal_temporal_index_preparation(prep_root)
        caption_verified = verify_formal_temporal_caption_package(
            caption_root, prep_root,
        )
        prep = _object(
            prep_root / "temporal-index-preparation.json", "preparation",
        )
        cost_path = caption_root / "temporal-index-build-cost.json"
        cost = _object(cost_path, "caption cost")
        frame_rows = _load_jsonl(
            (prep_root / "caption-frames.jsonl").read_bytes(),
            "caption frames",
        )
        caption_rows = _load_jsonl(
            (caption_root / "fine-captions.jsonl").read_bytes(),
            "fine captions",
        )
        _require(cost.get("caption_window_count") == len(caption_rows),
                 "caption cost receipt does not cover captions")
        caption_cost_sha256 = _sha256(cost_path.read_bytes())
        prep_by_object = {row["object_id"]: row for row in prep["objects"]}
    n4_by_object: dict[str, dict[str, dict[str, Any]]] = {}
    for row in n4["objects"]:
        n4_by_object.setdefault(row["object_id"], {})[
            row["representation_id"]
        ] = row
    case_objects = {case["object_id"] for case in v1["cases"]}
    _require(case_objects == set(n4_by_object),
             "v1 and N4 object sets differ")
    if prep_verified is not None:
        _require(case_objects == set(prep_by_object),
                 "v1 and preparation object sets differ")
    cases: list[dict[str, Any]] = []
    for old_case in v1["cases"]:
        case = json.loads(_canonical(old_case))
        object_id = case["object_id"]
        _require(case["initial_state"]["index_available"] is False
                 and not any(case["initial_state"][
                     "cache_warm_by_node"
                 ].values()), "v1 case is not cold at the replay boundary")
        case["initial_state"]["built_components"] = []
        case["materialization_components"] = _costs_from_sources(
            case,
            preparation=prep_by_object.get(object_id),
            frame_rows=(
                [row for row in frame_rows if row["object_id"] == object_id]
                if prep_verified is not None else None
            ),
            caption_rows=(
                [row for row in caption_rows if row["object_id"] == object_id]
                if prep_verified is not None else None
            ),
            n4_rows=n4_by_object[object_id],
            preparation_sha256=(
                prep_verified["preparation_sha256"] if prep_verified else None
            ),
            caption_sha256=(
                caption_verified["package_sha256"] if caption_verified else None
            ),
        )
        case["materialization_source_video_sha256"] = n4_by_object[
            object_id
        ]["sampled_frame_bundle"]["provenance"]["source_content_sha256"]
        cases.append(case)
    actions: list[dict[str, Any]] = []
    for old_action in v1["actions"]:
        action = dict(old_action)
        profile = action["semantic_input_profile_id"]
        _require(profile in PROFILE_COMPONENTS,
                 f"unsupported materialization profile {profile!r}")
        action["required_build_components"] = list(PROFILE_COMPONENTS[profile])
        if profile in PROFILE_REPRESENTATION:
            action["representation_id"] = PROFILE_REPRESENTATION[profile]
        actions.append(action)
    manifest = {
        **v1["manifest"],
        "schema_version": V2_SCHEMA_VERSION,
        "package_id": package_id,
        "builder_source_commit": builder_commit,
        "source_v1_package_id": v1["manifest"]["package_id"],
        "source_v1_package_sha256": v1["package_sha256"],
        "materialization_source_bindings": {
            "n4_package_sha256": n4_verified["package_sha256"],
            "preparation_sha256": (
                prep_verified["preparation_sha256"] if prep_verified else None
            ),
            "caption_package_sha256": (
                caption_verified["package_sha256"]
                if caption_verified else None
            ),
            "caption_cost_receipt_sha256": caption_cost_sha256,
        },
        "materialization_cost_model": {
            "initial_state": "cold-per-object",
            "shared_components_charged_once": True,
            "source_byte_basis": "logical-full-object-size-proxy",
            "historical_caption_usage_complete": False,
            "preparation_source_binding_checked": prep_verified is not None,
            "historical_build_latency_complete": False,
            "monetary_cost_measured": False,
            "warm_path_outcomes_reused": True,
            "cold_path_latency_measured": False,
        },
        "objective": {
            "kind": "quality-then-logical-byte-proxy-v2",
            "quality_constraint": "per-case minimum_success_rate",
            "cost_order": ["total_source_bytes_proxy", "total_model_input_bytes"],
            "monetary_cost_ranked": False,
        },
    }
    readme = (
        f"# Pathfinder RSI-Exam offline replay v2: `{package_id}`\n\n"
        "This immutable, offline package adds cold materialization state to "
        "the verified v1 warm-path outcomes. Sampling, captions, frame bundle, "
        "digest, embedding and index projection become one-time obligations "
        "per object; only known resource amounts receive numeric charges. "
        "Independent queries reset that state.\n\n"
        "The sampling source-byte charge is a logical complete-object-size "
        "proxy, not measured storage I/O. Historical caption/embedding usage, "
        "build latency and monetary cost remain unknown, never zero. Frozen "
        "warm-path outcomes do not measure cold-start execution latency. "
        "The ranking is a quality/known-byte proxy, not a dollar ranking.\n"
    ).encode("utf-8")
    documents = {
        MANIFEST_NAME: _json_bytes(manifest),
        CASES_NAME: _jsonl_bytes(cases),
        ACTIONS_NAME: _jsonl_bytes(actions),
        OUTCOMES_NAME: _jsonl_bytes(v1["outcomes"]),
        README_NAME: readme,
    }
    documents[CHECKSUMS_NAME] = _checksum_bytes(documents)
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.", dir=target.parent))
    try:
        for name, payload in documents.items():
            _atomic_write(staging / name, payload)
        verify_offline_replay_v2(
            staging, source_v1_dir=v1_package_dir,
            source_n4_dir=n4_package_dir,
        )
        os.replace(staging, target)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return {
        "status": "FROZEN_MATERIALIZATION_AWARE_REPLAY",
        "package_id": package_id,
        "package_dir": str(target),
        "package_sha256": _sha256((target / CHECKSUMS_NAME).read_bytes()),
        "case_count": len(cases),
        "action_count": len(actions),
        "monetary_cost_measured": False,
        "external_calls_made": False,
    }


def _validate_v2(
    manifest: Mapping[str, Any],
    cases: Sequence[Mapping[str, Any]],
    actions: Sequence[Mapping[str, Any]],
    outcomes: Sequence[Mapping[str, Any]],
) -> None:
    _require(manifest.get("schema_version") == V2_SCHEMA_VERSION,
             "unsupported v2 replay schema")
    _require(manifest.get("materialization_cost_model", {}).get(
        "monetary_cost_measured") is False, "v2 asserts measured monetary cost")
    _require(manifest.get("objective", {}).get("kind")
             == "quality-then-logical-byte-proxy-v2"
             and manifest["objective"].get("monetary_cost_ranked") is False,
             "v2 objective incorrectly claims a priced ranking")
    _validate_package_documents(
        {**manifest, "schema_version": OFFLINE_REPLAY_SCHEMA_VERSION},
        cases, actions, outcomes,
    )
    by_action = {action["action_id"]: action for action in actions}
    for action in actions:
        profile = action["semantic_input_profile_id"]
        _require(profile in PROFILE_COMPONENTS,
                 "v2 action has unsupported semantic input profile")
        _require(action.get("required_build_components")
                 == list(PROFILE_COMPONENTS[profile]),
                 "v2 action build dependencies differ from profile")
        if profile in PROFILE_REPRESENTATION:
            _require(action.get("representation_id")
                     == PROFILE_REPRESENTATION[profile],
                     "v2 derived representation differs from profile")
    for case in cases:
        components = case.get("materialization_components")
        _require(re.fullmatch(r"[0-9a-f]{64}", str(case.get(
            "materialization_source_video_sha256", ""
        ))) is not None, "v2 source video identity is invalid")
        _require(isinstance(components, dict)
                 and set(components) == set(COMPONENTS),
                 "v2 case materialization components are incomplete")
        _require(case["initial_state"].get("built_components") == [],
                 "v2 initial materialization state must be cold")
        _require(case["initial_state"].get("index_available") is False
                 and not any(case["initial_state"].get(
                     "cache_warm_by_node", {}
                 ).values()), "v2 initial index/cache state must be cold")
        _require(components["sampling"]["source_bytes_charged"]
                 == case["index_build"]["source_bytes"],
                 "sampling proxy differs from source object size")
        _require(components["index_projection"]["source_bytes_charged"]
                 == case["index_build"]["source_bytes"],
                 "index projection charge differs from frozen accounting")
        _require(components["index_projection"]["artifact_output_bytes"]
                 == case["index_build"]["output_bytes"],
                 "index projection output differs from frozen accounting")
        _require(components["sampling"]["source_bytes_basis"]
                 == "modeled-complete-object-size-proxy"
                 and components["index_projection"]["source_bytes_basis"]
                 == "frozen-index-build-accounting",
                 "v2 source-byte basis changed")
        for name, cost in components.items():
            _require(isinstance(cost, dict), f"{name} cost is invalid")
            _require(type(cost.get("source_bytes_charged")) is int
                     and cost["source_bytes_charged"] >= 0,
                     f"{name} source-byte charge is invalid")
            if name not in {"sampling", "index_projection"}:
                _require(cost["source_bytes_charged"] == 0,
                         f"{name} duplicates a source read charge")
            output = cost.get("artifact_output_bytes")
            _require(output is None or
                     (type(output) is int and output >= 0),
                     f"{name} output bytes are invalid")
            _require(cost.get("latency_ms") is None,
                     "unmeasured build latency must be null")
            _require(cost.get("monetary_cost_usd") is None,
                     "unmeasured money cost must be null")
            usage = cost.get("provider_usage")
            _require(isinstance(usage, dict) and usage.get("unit") == "tokens",
                     f"{name} provider usage is invalid")
            if name in {"captions", "index_embedding"}:
                _require(usage.get("input_units") is None
                         and usage.get("output_units") is None
                         and usage.get("status") == "historical-usage-unavailable",
                         f"{name} historical provider usage must be unknown")
            else:
                _require(usage.get("input_units") == 0
                         and usage.get("output_units") == 0
                         and usage.get("status") == "not-applicable",
                         f"{name} provider usage must be not applicable")
        for action_id in case["allowed_action_ids"]:
            _require(action_id in by_action, "case names unknown action")
    _verify_no_leakage([manifest, *cases, *actions, *outcomes])


def verify_offline_replay_v2(
    package_dir: str | Path,
    *,
    source_v1_dir: str | Path | None = None,
    source_n4_dir: str | Path | None = None,
) -> dict[str, Any]:
    root = Path(package_dir).resolve()
    _require(root.is_dir(), "v2 replay directory is missing")
    _require({path.name for path in root.iterdir() if path.is_file()}
             == PACKAGE_FILES, "v2 replay file set changed")
    entries = _verify_checksum_directory(root)
    _require(set(entries) == PACKAGE_FILES - {CHECKSUMS_NAME},
             "v2 checksum file set changed")
    payloads = {name: (root / name).read_bytes() for name in PACKAGE_FILES}
    for name, payload in payloads.items():
        _require(b"\r" not in payload, f"{name} contains CR bytes")
    manifest = _load_json_bytes(payloads[MANIFEST_NAME], MANIFEST_NAME)
    cases = _load_jsonl(payloads[CASES_NAME], CASES_NAME)
    actions = _load_jsonl(payloads[ACTIONS_NAME], ACTIONS_NAME)
    outcomes = _load_jsonl(payloads[OUTCOMES_NAME], OUTCOMES_NAME)
    _require(payloads[MANIFEST_NAME] == _json_bytes(manifest),
             "v2 manifest is not canonical")
    for name, rows in ((CASES_NAME, cases), (ACTIONS_NAME, actions),
                       (OUTCOMES_NAME, outcomes)):
        _require(payloads[name] == _jsonl_bytes(rows),
                 f"{name} is not canonical")
    _validate_v2(manifest, cases, actions, outcomes)
    if source_v1_dir is not None:
        source = load_offline_replay_package(source_v1_dir)
        _require(manifest["source_v1_package_sha256"]
                 == source["package_sha256"],
                 "source v1 package digest differs")
        _require(manifest["source_v1_package_id"]
                 == source["manifest"]["package_id"],
                 "source v1 package ID differs")
        _require(outcomes == source["outcomes"],
                 "v2 changed frozen measured outcomes")
        _require({case["case_id"] for case in cases}
                 == {case["case_id"] for case in source["cases"]},
                 "v2 changed frozen case set")
    if source_n4_dir is not None:
        n4_root = Path(source_n4_dir).resolve()
        n4_verified = verify_n4_derived_data_package(n4_root)
        _require(manifest["materialization_source_bindings"][
            "n4_package_sha256"
        ] == n4_verified["package_sha256"],
                 "source N4 package digest differs")
        n4 = _object(n4_root / "n4-derived-data-package.json", "N4 manifest")
        by_object: dict[str, dict[str, dict[str, Any]]] = {}
        for row in n4["objects"]:
            by_object.setdefault(row["object_id"], {})[
                row["representation_id"]
            ] = row
        _require({case["object_id"] for case in cases} == set(by_object),
                 "source N4 object set differs")
        for case in cases:
            rows = by_object[case["object_id"]]
            _require(case["materialization_source_video_sha256"]
                     == rows["sampled_frame_bundle"]["provenance"][
                         "source_content_sha256"
                     ] == rows["multimodal_digest"]["provenance"][
                         "source_content_sha256"
                     ], "source N4 video identity differs")
            for component, representation in (
                ("frame_bundle", "sampled_frame_bundle"),
                ("digest", "multimodal_digest"),
            ):
                _require(case["materialization_components"][component][
                    "artifact_output_bytes"
                ] == rows[representation]["artifact_size_bytes"],
                         f"source N4 {component} size differs")
    return {
        "status": "VERIFIED_MATERIALIZATION_AWARE_REPLAY",
        "package_id": manifest["package_id"],
        "package_sha256": _sha256(payloads[CHECKSUMS_NAME]),
        "case_count": len(cases),
        "action_count": len(actions),
        "source_v1_checked": source_v1_dir is not None,
        "source_n4_checked": source_n4_dir is not None,
        "monetary_cost_measured": False,
        "credentials_recorded": False,
    }


def load_offline_replay_v2(package_dir: str | Path) -> dict[str, Any]:
    receipt = verify_offline_replay_v2(package_dir)
    root = Path(package_dir).resolve()
    return {
        "manifest": _object(root / MANIFEST_NAME, MANIFEST_NAME),
        "cases": _load_jsonl((root / CASES_NAME).read_bytes(), CASES_NAME),
        "actions": _load_jsonl((root / ACTIONS_NAME).read_bytes(), ACTIONS_NAME),
        "outcomes": _load_jsonl((root / OUTCOMES_NAME).read_bytes(), OUTCOMES_NAME),
        "package_sha256": receipt["package_sha256"],
    }


@dataclass(frozen=True)
class MaterializationObservation:
    case_id: str
    object_id: str
    workload_id: str
    stratum: str
    split: str
    remaining_queries: int
    available_actions: tuple[dict[str, Any], ...]
    index_available: bool
    cache_warm_by_node: Mapping[str, bool]
    built_components: tuple[str, ...]
    component_costs: Mapping[str, Mapping[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "case_id": self.case_id,
            "object_id": self.object_id,
            "workload_id": self.workload_id,
            "stratum": self.stratum,
            "split": self.split,
            "remaining_queries": self.remaining_queries,
            "available_actions": [dict(row) for row in self.available_actions],
            "component_costs": dict(self.component_costs),
            "state": {
                "index_available": self.index_available,
                "cache_warm_by_node": dict(self.cache_warm_by_node),
                "built_components": list(self.built_components),
            },
        }


class MaterializationReplayEvaluator:
    """Stateful exact-outcome replay with one-time component charging."""

    def __init__(self, package: Mapping[str, Any], *, mode: str,
                 seed: int = 0) -> None:
        _require(mode in REPLAY_MODES, "unknown replay mode")
        self.mode = mode
        self._random = random.Random(seed)
        self._cases = {row["case_id"]: row for row in package["cases"]}
        self._actions = {row["action_id"]: row for row in package["actions"]}
        self._outcomes: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
        for row in package["outcomes"]:
            key = (row["case_id"], row["action_id"], row["state_variant"])
            self._outcomes.setdefault(key, []).append(row)
        self._state_by_case: dict[str, dict[str, Any]] = {}

    def reset_case(self, case_id: str) -> None:
        case = self._cases.get(case_id)
        _require(case is not None, "unknown replay case")
        self._state_by_case[case_id] = {
            "built_components": set(),
            "cache_warm_by_node": dict(case["initial_state"]["cache_warm_by_node"]),
        }

    def observation(self, case_id: str, *,
                    remaining_queries: int) -> MaterializationObservation:
        _require(type(remaining_queries) is int and remaining_queries >= 1,
                 "remaining_queries must be positive")
        case = self._cases.get(case_id)
        _require(case is not None, "unknown replay case")
        if case_id not in self._state_by_case:
            self.reset_case(case_id)
        state = self._state_by_case[case_id]
        built = state["built_components"]
        return MaterializationObservation(
            case_id=case_id, object_id=case["object_id"],
            workload_id=case["workload_id"], stratum=case["stratum"],
            split=case["split"], remaining_queries=remaining_queries,
            available_actions=tuple(
                json.loads(_canonical(self._actions[action_id]))
                for action_id in case["allowed_action_ids"]
            ),
            index_available="index_projection" in built,
            cache_warm_by_node=dict(state["cache_warm_by_node"]),
            built_components=tuple(name for name in COMPONENTS if name in built),
            component_costs=json.loads(_canonical(
                case["materialization_components"]
            )),
        )

    def step(self, case_id: str, action_id: str, *,
             remaining_queries: int) -> dict[str, Any]:
        observation = self.observation(
            case_id, remaining_queries=remaining_queries,
        )
        case = self._cases[case_id]
        if action_id not in case["allowed_action_ids"]:
            return {
                "status": "unsupported_action", "case_id": case_id,
                "action_id": action_id, "state_changed": False,
                "reason": "action is not present in the frozen case",
            }
        action = self._actions[action_id]
        state = self._state_by_case[case_id]
        node = action["executor_node_id"]
        variant = "default"
        if action["uses_cache"]:
            variant = (
                "cache-hit" if state["cache_warm_by_node"].get(node, False)
                else "cache-miss"
            )
        candidates = self._outcomes.get((case_id, action_id, variant), [])
        if not candidates:
            return {
                "status": "unsupported_action", "case_id": case_id,
                "action_id": action_id, "state_changed": False,
                "reason": f"no measured outcome for state variant {variant}",
            }
        outcome = candidates[
            0 if len(candidates) == 1 else self._random.randrange(len(candidates))
        ]
        required = set(action["required_build_components"])
        newly_built = [
            name for name in COMPONENTS
            if name in required and name not in state["built_components"]
        ]
        costs = case["materialization_components"]
        build_source_bytes = sum(
            costs[name]["source_bytes_charged"] for name in newly_built
        )
        known_output_bytes = sum(
            costs[name]["artifact_output_bytes"] or 0 for name in newly_built
        )
        output_complete = all(
            costs[name]["artifact_output_bytes"] is not None
            for name in newly_built
        )
        provider_complete = all(
            costs[name]["provider_usage"]["input_units"] is not None
            and costs[name]["provider_usage"]["output_units"] is not None
            for name in newly_built
        )
        latency_complete = all(
            costs[name]["latency_ms"] is not None for name in newly_built
        )
        state["built_components"].update(newly_built)
        if action["uses_cache"]:
            state["cache_warm_by_node"][node] = True
        metrics = dict(outcome["metrics"])
        return {
            "status": "replayed", "case_id": case_id,
            "action_id": action_id, "action_kind": action["action_kind"],
            "state_variant": variant, "outcome_id": outcome["outcome_id"],
            "task_success": outcome["task_success"],
            "infrastructure_complete": outcome["infrastructure_complete"],
            "newly_built_components": newly_built,
            "metrics": {
                **metrics,
                "build_source_bytes_proxy": build_source_bytes,
                "known_build_artifact_output_bytes": known_output_bytes,
                "build_artifact_output_bytes_complete": output_complete,
                "build_latency_ms": (
                    sum(costs[name]["latency_ms"] for name in newly_built)
                    if latency_complete else None
                ),
                "provider_input_units": (
                    sum(costs[name]["provider_usage"]["input_units"]
                        for name in newly_built)
                    if provider_complete else None
                ),
                "provider_output_units": (
                    sum(costs[name]["provider_usage"]["output_units"]
                        for name in newly_built)
                    if provider_complete else None
                ),
                "provider_usage_complete": provider_complete,
                "monetary_cost_usd": None,
                "total_source_bytes_proxy": (
                    metrics["query_origin_bytes"] + build_source_bytes
                ),
            },
            "state_before": observation.to_dict()["state"],
            "state_after": {
                "index_available": "index_projection" in state["built_components"],
                "cache_warm_by_node": dict(state["cache_warm_by_node"]),
                "built_components": [
                    name for name in COMPONENTS
                    if name in state["built_components"]
                ],
            },
        }


def run_offline_replay_v2(
    package_dir: str | Path, *, policy_name: str, mode: str,
    query_count: int, seed: int = 0, case_id: str | None = None,
) -> dict[str, Any]:
    _require(type(query_count) is int and query_count >= 1,
             "query_count must be positive")
    package = load_offline_replay_v2(package_dir)
    cases = package["cases"]
    _require(bool(cases), "v2 package has no cases")
    selected = case_id or cases[0]["case_id"]
    case = next((row for row in cases if row["case_id"] == selected), None)
    _require(case is not None, "requested v2 case is absent")
    policy = _policy(policy_name, seed=seed)
    evaluator = MaterializationReplayEvaluator(package, mode=mode, seed=seed + 1)
    steps: list[dict[str, Any]] = []
    for index in range(query_count):
        if mode == "independent-query":
            evaluator.reset_case(selected)
        remaining = query_count - index
        action_id = policy.select_action(
            evaluator.observation(selected, remaining_queries=remaining)
        )
        step = evaluator.step(selected, action_id, remaining_queries=remaining)
        steps.append(step)
        if step["status"] != "replayed":
            break
    completed = [step for step in steps if step["status"] == "replayed"]
    successes = sum(step["task_success"] for step in completed)
    success_rate = successes / len(completed) if completed else 0.0
    feasible = (
        len(completed) == query_count
        and success_rate >= case["minimum_success_rate"]
    )
    total_source = sum(
        step["metrics"]["total_source_bytes_proxy"] for step in completed
    )
    total_model = sum(
        step["metrics"]["model_input_bytes"] for step in completed
    )
    total_known_output = sum(
        step["metrics"]["known_build_artifact_output_bytes"]
        for step in completed
    )
    all_output_complete = all(
        step["metrics"]["build_artifact_output_bytes_complete"]
        for step in completed
    )
    return {
        "schema_version": V2_SCHEMA_VERSION,
        "status": "COMPLETE" if len(completed) == query_count
                  else "UNSUPPORTED_ACTION",
        "package_id": package["manifest"]["package_id"],
        "package_sha256": package["package_sha256"],
        "policy_name": policy_name, "mode": mode, "seed": seed,
        "case_id": selected, "query_count": query_count, "steps": steps,
        "metrics": {
            "queries": len(completed), "task_successes": successes,
            "success_rate": success_rate,
            "build_components_charged": sum(
                len(step["newly_built_components"]) for step in completed
            ),
            "total_build_source_bytes_proxy": sum(
                step["metrics"]["build_source_bytes_proxy"]
                for step in completed
            ),
            "total_source_bytes_proxy": total_source,
            "total_model_input_bytes": total_model,
            "total_known_build_artifact_output_bytes": total_known_output,
            "build_artifact_output_bytes_complete": all_output_complete,
            "all_provider_usage_complete": all(
                step["metrics"]["provider_usage_complete"]
                for step in completed
            ),
            "monetary_cost_usd": None,
            "cold_build_latency_measured": False,
        },
        "objective": {
            "feasible": feasible,
            "kind": "quality-then-logical-byte-proxy-v2",
            "lexicographic_key": [
                0 if feasible else 1, total_source, total_model,
            ],
            "complete_monetary_ranking": False,
        },
        "external_calls_made": False,
        "credentials_recorded": False,
        "hidden_outcomes_exposed_to_policy": False,
    }


def compare_offline_replay_v2_baselines(
    package_dir: str | Path, *, mode: str, query_count: int,
    seed: int = 0, case_id: str | None = None,
) -> dict[str, Any]:
    results = [
        run_offline_replay_v2(
            package_dir, policy_name=name, mode=mode,
            query_count=query_count, seed=seed, case_id=case_id,
        )
        for name in BASELINE_POLICY_NAMES
    ]
    ranked = sorted(results, key=lambda row: tuple(
        row["objective"]["lexicographic_key"]
    ))
    return {
        "schema_version": V2_SCHEMA_VERSION, "status": "COMPLETE",
        "mode": mode, "query_count": query_count, "seed": seed,
        "ranking": [row["policy_name"] for row in ranked],
        "ranking_scope": "quality-and-known-byte-proxy-only",
        "complete_monetary_ranking": False,
        "policies": results,
        "external_calls_made": False, "credentials_recorded": False,
    }
