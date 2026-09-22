"""Credential-free offline replay for the Pathfinder RSI-Exam task.

The replay package contains only public observations, physical action
descriptors, and measured outcomes copied from checksum-verified accounting
evidence.  A policy chooses an action before the evaluator reveals its
outcome.  Missing action/state cells fail closed instead of being estimated.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import random
import re
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence


OFFLINE_REPLAY_SCHEMA_VERSION = "pathfinder.rsi-exam-offline-replay/v1alpha1"
ACCOUNTING_SCHEMA_VERSION = "pathfinder.temporal-index-v2-accounting/v1"
MANIFEST_NAME = "replay-manifest.json"
CASES_NAME = "cases.jsonl"
ACTIONS_NAME = "actions.jsonl"
OUTCOMES_NAME = "outcomes.jsonl"
README_NAME = "README.md"
CHECKSUMS_NAME = "SHA256SUMS"
PACKAGE_FILES = frozenset({
    MANIFEST_NAME,
    CASES_NAME,
    ACTIONS_NAME,
    OUTCOMES_NAME,
    README_NAME,
    CHECKSUMS_NAME,
})
REPLAY_MODES = frozenset({"independent-query", "shared-dataset-sequence"})
BASELINE_POLICY_NAMES = (
    "always-direct-video",
    "always-indexed",
    "always-derived",
    "myopic-cost-first",
    "amortization-aware",
    "random-seeded",
)


class OfflineReplayError(ValueError):
    """A replay package or requested replay operation is invalid."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise OfflineReplayError(message)


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise OfflineReplayError(f"JSON contains duplicate key {key!r}")
        value[key] = item
    return value


def _load_json_bytes(payload: bytes, label: str) -> Any:
    try:
        return json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_strict_object,
            parse_constant=lambda value: (_ for _ in ()).throw(
                OfflineReplayError(
                    f"{label} contains non-finite JSON value {value}"
                )
            ),
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OfflineReplayError(f"cannot parse {label}") from exc


def _json_bytes(value: Any) -> bytes:
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


def _compact_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _jsonl_bytes(rows: Sequence[Mapping[str, Any]]) -> bytes:
    return b"".join(_compact_json_bytes(dict(row)) + b"\n" for row in rows)


def _load_jsonl(payload: bytes, label: str) -> list[dict[str, Any]]:
    _require(b"\r" not in payload, f"{label} contains CR bytes")
    _require(payload.endswith(b"\n"), f"{label} is not LF terminated")
    rows: list[dict[str, Any]] = []
    for index, line in enumerate(payload.splitlines(), start=1):
        value = _load_json_bytes(line, f"{label} line {index}")
        _require(isinstance(value, dict), f"{label} line {index} is not an object")
        rows.append(value)
    return rows


def _safe_relative_name(name: str) -> bool:
    path = Path(name)
    return (
        bool(name)
        and not path.is_absolute()
        and len(path.parts) == 1
        and name not in {".", ".."}
    )


def _verify_checksum_directory(root: Path) -> dict[str, str]:
    checksum_path = root / CHECKSUMS_NAME
    _require(checksum_path.is_file(), f"{root.name} SHA256SUMS is missing")
    raw = checksum_path.read_bytes()
    _require(b"\r" not in raw, f"{root.name} SHA256SUMS contains CR bytes")
    try:
        lines = raw.decode("utf-8").splitlines()
    except UnicodeDecodeError as exc:
        raise OfflineReplayError("SHA256SUMS is not UTF-8") from exc
    _require(bool(lines), f"{root.name} SHA256SUMS is empty")
    entries: dict[str, str] = {}
    for line in lines:
        match = re.fullmatch(r"([0-9a-f]{64})[ \t]+(.+)", line)
        _require(match is not None, f"{root.name} SHA256SUMS is malformed")
        digest, name = match.groups()
        _require(_safe_relative_name(name), "SHA256SUMS contains an unsafe path")
        _require(name not in entries, f"SHA256SUMS repeats {name}")
        path = root / name
        _require(path.is_file(), f"SHA256SUMS references missing {name}")
        _require(
            _sha256(path.read_bytes()) == digest,
            f"SHA256SUMS mismatch for {name}",
        )
        entries[name] = digest
    return entries


def _checksum_bytes(documents: Mapping[str, bytes]) -> bytes:
    return "".join(
        f"{_sha256(documents[name])}  {name}\n" for name in sorted(documents)
    ).encode("utf-8")


def _atomic_write(path: Path, payload: bytes) -> None:
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    try:
        with temporary.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _as_nonnegative_int(value: Any, label: str) -> int:
    _require(
        isinstance(value, int) and not isinstance(value, bool) and value >= 0,
        f"{label} must be a non-negative integer",
    )
    return value


def _as_nonnegative_number(value: Any, label: str) -> float:
    _require(
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
        and float(value) >= 0.0,
        f"{label} must be a finite non-negative number",
    )
    return float(value)


def _as_bool(value: Any, label: str) -> bool:
    _require(isinstance(value, bool), f"{label} must be boolean")
    return value


def _as_string(value: Any, label: str) -> str:
    _require(isinstance(value, str) and bool(value), f"{label} must be a string")
    return value


def _action_kind(row: Mapping[str, Any]) -> str:
    family = _as_string(row.get("route_family"), "route_family")
    if family == "raw":
        return "direct-video"
    if family == "indexed-raw":
        return "query-aware-temporal-index"
    if family == "remote-derived":
        return "remote-derived"
    if family == "local-cache-derived":
        return "read-through-derived-cache"
    raise OfflineReplayError(f"unsupported route family {family!r}")


def _representation_id(kind: str) -> str:
    return {
        "direct-video": "raw-video",
        "query-aware-temporal-index": (
            "query-aware-temporal-index-selected-frames"
        ),
        "remote-derived": "derived-frame-bundle",
        "read-through-derived-cache": "derived-frame-bundle",
    }[kind]


def _public_action(row: Mapping[str, Any]) -> dict[str, Any]:
    kind = _action_kind(row)
    action_id = _as_string(row.get("design_id"), "design_id")
    node = _as_string(row.get("executor_node_id"), "executor_node_id")
    return {
        "action_id": action_id,
        "action_kind": kind,
        "executor_node_id": node,
        "representation_id": _representation_id(kind),
        "semantic_input_profile_id": _as_string(
            row.get("semantic_input_profile_id"),
            "semantic_input_profile_id",
        ),
        "uses_cache": kind == "read-through-derived-cache",
        "uses_temporal_index": kind == "query-aware-temporal-index",
        "state_effects": {
            "warms_executor_cache": kind == "read-through-derived-cache",
            "materializes_index_if_missing": (
                kind == "query-aware-temporal-index"
            ),
        },
    }


def _measured_outcome(
    row: Mapping[str, Any],
    *,
    case_id: str,
    source_run_id: str,
) -> dict[str, Any]:
    action_id = _as_string(row.get("design_id"), "design_id")
    source_case_id = _as_string(row.get("case_id"), "case_id")
    branch = row.get("cache_branch")
    _require(branch in {None, "miss", "hit"}, "cache_branch is invalid")
    status = _as_string(row.get("status"), "status")
    _require(status == "COMPLETE", "offline replay accepts only COMPLETE rows")
    source_row = dict(row)
    return {
        "outcome_id": (
            f"{case_id}:{_sha256(source_run_id.encode('utf-8'))[:12]}:"
            f"{source_case_id}"
        ),
        "case_id": case_id,
        "action_id": action_id,
        "state_variant": "default" if branch is None else f"cache-{branch}",
        "source_row_sha256": _sha256(_compact_json_bytes(source_row)),
        "infrastructure_complete": True,
        "task_success": _as_bool(row.get("task_success"), "task_success"),
        "score_authenticity_verified": _as_bool(
            row.get("n1_exactly_once_authenticated"),
            "n1_exactly_once_authenticated",
        ),
        "metrics": {
            "query_origin_bytes": _as_nonnegative_int(
                row.get("origin_bytes_read"), "origin_bytes_read"
            ),
            "cache_bytes": _as_nonnegative_int(
                row.get("cache_bytes_read"), "cache_bytes_read"
            ),
            "model_input_bytes": _as_nonnegative_int(
                row.get("model_input_bytes_sent_to_n6"),
                "model_input_bytes_sent_to_n6",
            ),
            "index_query_bytes_read": _as_nonnegative_int(
                row.get("index_query_bytes_read"),
                "index_query_bytes_read",
            ),
            "index_query_bytes_sent": _as_nonnegative_int(
                row.get("index_query_bytes_sent"),
                "index_query_bytes_sent",
            ),
            "index_query_ms": _as_nonnegative_number(
                row.get("index_query_ms"), "index_query_ms"
            ),
            "source_read_ms": _as_nonnegative_number(
                row.get("source_read_ms"), "source_read_ms"
            ),
            "prepare_model_input_ms": _as_nonnegative_number(
                row.get("prepare_model_input_ms"),
                "prepare_model_input_ms",
            ),
            "n6_infer_ms": _as_nonnegative_number(
                row.get("n6_infer_ms"), "n6_infer_ms"
            ),
            "n1_score_ms": _as_nonnegative_number(
                row.get("n1_score_ms"), "n1_score_ms"
            ),
            "route_wall_ms_excluding_inference": _as_nonnegative_number(
                row.get("route_wall_ms_excluding_inference"),
                "route_wall_ms_excluding_inference",
            ),
        },
    }


def _case_from_accounting(
    accounting: Mapping[str, Any],
    *,
    split: str,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    object_id = _as_string(accounting.get("object_id"), "object_id")
    source_run_id = _as_string(accounting.get("run_id"), "run_id")
    public_case_id = accounting.get("public_case_id", object_id)
    case_id = _as_string(public_case_id, "public_case_id")
    rows = accounting.get("rows")
    _require(isinstance(rows, list) and bool(rows), "accounting rows are missing")
    trial_keys = {
        _as_string(row.get("trial_key"), "trial_key") for row in rows
        if isinstance(row, dict)
    }
    _require(len(trial_keys) == len(rows), "accounting trial keys are invalid")
    workload_ids = {value.split("|", 1)[0] for value in trial_keys}
    strata = {
        value.split("|")[1].removeprefix("smoke-")
        for value in trial_keys
        if len(value.split("|")) >= 2
    }
    _require(len(workload_ids) == 1, "rows bind multiple workloads")
    _require(len(strata) == 1, "rows bind multiple workload strata")
    build = accounting.get("one_time_build")
    _require(isinstance(build, dict), "one_time_build is missing")
    actions_by_id: dict[str, dict[str, Any]] = {}
    outcomes: list[dict[str, Any]] = []
    for raw_row in rows:
        _require(isinstance(raw_row, dict), "accounting row is not an object")
        action = _public_action(raw_row)
        action_id = action["action_id"]
        existing = actions_by_id.get(action_id)
        if existing is None:
            actions_by_id[action_id] = action
        else:
            _require(existing == action, f"action {action_id} changes across rows")
        outcomes.append(
            _measured_outcome(
                raw_row,
                case_id=case_id,
                source_run_id=source_run_id,
            )
        )
    action_ids = sorted(actions_by_id)
    _require(len(action_ids) >= 2, "case has too few physical actions")
    index_actions = [
        action for action in actions_by_id.values()
        if action["uses_temporal_index"]
    ]
    _require(bool(index_actions), "case contains no temporal-index action")
    case = {
        "case_id": case_id,
        "object_id": object_id,
        "workload_id": next(iter(workload_ids)),
        "question_public_text": None,
        "stratum": next(iter(strata)),
        "split": split,
        "allowed_action_ids": action_ids,
        "default_query_count": 1,
        "minimum_success_rate": 1.0,
        "initial_state": {
            "index_available": False,
            "cache_warm_by_node": {
                action["executor_node_id"]: False
                for action in actions_by_id.values()
                if action["uses_cache"]
            },
        },
        "index_build": {
            "source_bytes": _as_nonnegative_int(
                build.get("this_object_source_bytes_read"),
                "this_object_source_bytes_read",
            ),
            "output_bytes": _as_nonnegative_int(
                build.get("this_object_projection_bytes"),
                "this_object_projection_bytes",
            ),
            "latency_ms": None,
            "read_mode": _as_string(build.get("read_mode"), "read_mode"),
            "partial_mp4_byte_range_claimed": _as_bool(
                build.get("partial_mp4_byte_range_claimed"),
                "partial_mp4_byte_range_claimed",
            ),
            "reduced_source_storage_io_claimed": _as_bool(
                build.get("reduced_source_storage_io_claimed"),
                "reduced_source_storage_io_claimed",
            ),
        },
    }
    return case, list(actions_by_id.values()), outcomes


def _read_split_manifest(path: str | Path | None) -> dict[str, str]:
    if path is None:
        return {}
    source = Path(path).resolve()
    value = _load_json_bytes(source.read_bytes(), source.name)
    _require(isinstance(value, dict), "split manifest must be an object")
    result: dict[str, str] = {}
    for object_id, split in value.items():
        _require(isinstance(object_id, str) and object_id, "invalid split object")
        _require(split in {"train", "development", "test", "fixture"}, "invalid split")
        result[object_id] = split
    return result


def _readme(package_id: str) -> bytes:
    return (
        f"# Pathfinder RSI-Exam offline replay: `{package_id}`\n\n"
        "This immutable package replays measured Pathfinder physical-path "
        "outcomes without UpCloud, FlowMesh, Docker, an LLM, source videos, "
        "credentials, or hidden labels.\n\n"
        "A policy sees only `cases.jsonl`, `actions.jsonl`, and current replay "
        "state before choosing. The evaluator reveals the matching row from "
        "`outcomes.jsonl` afterward. Missing action/state cells fail closed; "
        "the evaluator never interpolates a counterfactual.\n\n"
        "`independent-query` charges index construction whenever the initial "
        "state lacks an index. `shared-dataset-sequence` preserves index and "
        "cache state so one-time work can amortize over later queries. Null "
        "means unmeasured; it never means zero.\n\n"
        "This one-case package is a conformance fixture. It is not eligible "
        "for scientific accuracy, latency, GPU-performance, or causal claims.\n"
    ).encode("utf-8")


def build_offline_replay_package(
    accounting_dirs: Sequence[str | Path],
    *,
    output_dir: str | Path,
    source_commit: str,
    builder_commit: str,
    package_id: str,
    split_manifest: str | Path | None = None,
) -> dict[str, Any]:
    """Freeze verified public accounting evidence into an offline package."""

    _require(
        bool(accounting_dirs),
        "at least one accounting directory is required",
    )
    _require(
        re.fullmatch(r"[0-9a-f]{40}", source_commit) is not None,
        "source_commit must be a full Git SHA-1",
    )
    _require(
        re.fullmatch(r"[0-9a-f]{40}", builder_commit) is not None,
        "builder_commit must be a full Git SHA-1",
    )
    _require(
        bool(package_id)
        and "/" not in package_id
        and "\\" not in package_id,
        "package_id is invalid",
    )
    target = Path(output_dir).resolve()
    _require(not target.exists(), "offline replay output directory already exists")
    split_by_object = _read_split_manifest(split_manifest)
    source_bindings: list[dict[str, Any]] = []
    cases_by_id: dict[str, dict[str, Any]] = {}
    outcomes: list[dict[str, Any]] = []
    actions_by_id: dict[str, dict[str, Any]] = {}
    seen_objects: set[str] = set()
    seen_sources: set[tuple[str, str]] = set()

    for value in accounting_dirs:
        source = Path(value).resolve()
        _require(source.is_dir(), "accounting directory is missing")
        entries = _verify_checksum_directory(source)
        _require(
            "accounting.json" in entries,
            "accounting SHA256SUMS does not bind accounting.json",
        )
        raw = (source / "accounting.json").read_bytes()
        accounting = _load_json_bytes(raw, "accounting.json")
        _require(isinstance(accounting, dict), "accounting.json is not an object")
        _verify_no_leakage([accounting])
        _require(
            accounting.get("schema_version") == ACCOUNTING_SCHEMA_VERSION,
            "unsupported accounting schema",
        )
        _require(
            accounting.get("credentials_recorded") is False,
            "accounting records credentials",
        )
        _require(
            accounting.get("hidden_label_values_included") is False,
            "accounting includes hidden labels",
        )
        object_id = _as_string(accounting.get("object_id"), "object_id")
        seen_objects.add(object_id)
        run_id = _as_string(accounting.get("run_id"), "run_id")
        source_key = (object_id, run_id)
        _require(source_key not in seen_sources, "duplicate accounting source")
        seen_sources.add(source_key)
        split = split_by_object.get(object_id, "fixture")
        case, case_actions, case_outcomes = _case_from_accounting(
            accounting,
            split=split,
        )
        existing_case = cases_by_id.get(case["case_id"])
        if existing_case is None:
            cases_by_id[case["case_id"]] = case
        else:
            _require(
                existing_case == case,
                f"repeated case {case['case_id']} changes its public state",
            )
        outcomes.extend(case_outcomes)
        for action in case_actions:
            action_id = action["action_id"]
            existing = actions_by_id.get(action_id)
            if existing is None:
                actions_by_id[action_id] = action
            else:
                _require(existing == action, f"action {action_id} differs across cases")
        source_bindings.append({
            "run_id": run_id,
            "case_id": case["case_id"],
            "object_id": object_id,
            "accounting_schema_version": accounting["schema_version"],
            "accounting_sha256": entries["accounting.json"],
            "source_sha256s_sha256": _sha256((source / CHECKSUMS_NAME).read_bytes()),
        })

    unknown_split_objects = set(split_by_object) - seen_objects
    _require(not unknown_split_objects, "split manifest contains unknown objects")
    cases = sorted(cases_by_id.values(), key=lambda row: row["case_id"])
    actions = sorted(actions_by_id.values(), key=lambda row: row["action_id"])
    outcomes.sort(key=lambda row: row["outcome_id"])
    splits: dict[str, list[str]] = {
        name: sorted(case["object_id"] for case in cases if case["split"] == name)
        for name in ("train", "development", "test", "fixture")
    }
    manifest = {
        "schema_version": OFFLINE_REPLAY_SCHEMA_VERSION,
        "package_id": package_id,
        "experiment_source_commit": source_commit,
        "builder_source_commit": builder_commit,
        "source_bindings": sorted(
            source_bindings,
            key=lambda row: (row["object_id"], row["run_id"]),
        ),
        "case_count": len(cases),
        "action_count": len(actions),
        "outcome_count": len(outcomes),
        "splits": splits,
        "supported_modes": sorted(REPLAY_MODES),
        "included_baselines": list(BASELINE_POLICY_NAMES),
        "outcome_sampling": {
            "current_fixture": "single-observation-exact-replay",
            "future_repetitions": "seeded-empirical-sampling",
            "interpolation_allowed": False,
        },
        "objective": {
            "kind": "lexicographic-quality-then-bytes-v1",
            "quality_constraint": "per-case minimum_success_rate",
            "cost_order": ["total_source_bytes", "total_model_input_bytes"],
            "weights": None,
        },
        "runtime_requirements": {
            "network": False,
            "upcloud": False,
            "flowmesh": False,
            "docker": False,
            "llm": False,
            "source_video": False,
        },
        "claim_boundaries": {
            "conformance_fixture": len(cases) == 1,
            "eligible_for_scientific_claims": False,
            "general_accuracy_claimed": False,
            "general_latency_claimed": False,
            "gpu_performance_claimed": False,
            "causal_quality_claimed": False,
            "reduced_source_storage_io_claimed": False,
        },
        "credentials_recorded": False,
        "hidden_label_values_included": False,
        "hidden_outcomes_exposed_to_policy": False,
    }
    documents = {
        MANIFEST_NAME: _json_bytes(manifest),
        CASES_NAME: _jsonl_bytes(cases),
        ACTIONS_NAME: _jsonl_bytes(actions),
        OUTCOMES_NAME: _jsonl_bytes(outcomes),
        README_NAME: _readme(package_id),
    }
    documents[CHECKSUMS_NAME] = _checksum_bytes(documents)

    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.", dir=target.parent))
    try:
        for name, payload in documents.items():
            _atomic_write(staging / name, payload)
        verify_offline_replay_package(
            staging,
            source_accounting_dirs=accounting_dirs,
        )
        os.replace(staging, target)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return {
        "status": "FROZEN_OFFLINE_REPLAY",
        "package_id": package_id,
        "package_dir": str(target),
        "package_sha256": _sha256((target / CHECKSUMS_NAME).read_bytes()),
        "case_count": len(cases),
        "action_count": len(actions),
        "outcome_count": len(outcomes),
        "credentials_recorded": False,
        "hidden_label_values_included": False,
    }


def _walk_json(value: Any, *, path: str = "$") -> list[tuple[str, Any]]:
    rows = [(path, value)]
    if isinstance(value, dict):
        for key, item in value.items():
            rows.extend(_walk_json(item, path=f"{path}.{key}"))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            rows.extend(_walk_json(item, path=f"{path}[{index}]"))
    return rows


_FORBIDDEN_KEYS = frozenset({
    "answer",
    "answer_text",
    "correct_answer",
    "hidden_label",
    "hidden_labels",
    "oracle_answer",
    "oracle_evidence",
    "prediction",
    "prompt",
    "model_response",
    "api_key",
    "password",
    "secret",
    "signed_url",
    "token",
})
_SAFE_AUDIT_KEYS = frozenset({
    "credentials_recorded",
    "hidden_label_values_included",
})
_FORBIDDEN_KEY_FRAGMENTS = (
    "api_key",
    "authorization",
    "bearer",
    "credential",
    "hidden_label",
    "password",
    "secret",
    "signed_url",
    "token",
)


def _verify_no_leakage(documents: Sequence[Any]) -> None:
    for document in documents:
        for path, value in _walk_json(document):
            key = path.rsplit(".", 1)[-1].split("[", 1)[0]
            unsafe_key = (
                key in _FORBIDDEN_KEYS
                or any(
                    fragment in key.lower()
                    for fragment in _FORBIDDEN_KEY_FRAGMENTS
                )
            )
            if unsafe_key and key not in _SAFE_AUDIT_KEYS:
                raise OfflineReplayError(
                    f"replay package exposes forbidden field {path}"
                )
            if isinstance(value, str):
                _require(
                    re.match(r"^[A-Za-z]:[\\/]", value) is None,
                    f"replay package exposes an absolute Windows path at {path}",
                )
                _require(
                    re.match(
                        r"^/(?:home|root|Users|etc|var|opt)/",
                        value,
                    )
                    is None,
                    f"replay package exposes an absolute host path at {path}",
                )


def _validate_package_documents(
    manifest: Mapping[str, Any],
    cases: Sequence[Mapping[str, Any]],
    actions: Sequence[Mapping[str, Any]],
    outcomes: Sequence[Mapping[str, Any]],
) -> None:
    _require(
        manifest.get("schema_version") == OFFLINE_REPLAY_SCHEMA_VERSION,
        "unsupported replay schema",
    )
    _require(
        manifest.get("credentials_recorded") is False,
        "replay records credentials",
    )
    _require(
        manifest.get("hidden_label_values_included") is False,
        "replay includes hidden labels",
    )
    _require(
        manifest.get("hidden_outcomes_exposed_to_policy") is False,
        "replay exposes outcomes to policy",
    )
    _require(manifest.get("case_count") == len(cases), "case_count differs")
    _require(manifest.get("action_count") == len(actions), "action_count differs")
    _require(manifest.get("outcome_count") == len(outcomes), "outcome_count differs")
    case_ids = [row.get("case_id") for row in cases]
    action_ids = [row.get("action_id") for row in actions]
    outcome_ids = [row.get("outcome_id") for row in outcomes]
    _require(len(case_ids) == len(set(case_ids)), "duplicate case ID")
    _require(len(action_ids) == len(set(action_ids)), "duplicate action ID")
    _require(len(outcome_ids) == len(set(outcome_ids)), "duplicate outcome ID")
    _require(
        all(isinstance(value, str) and value for value in case_ids),
        "invalid case ID",
    )
    _require(
        all(isinstance(value, str) and value for value in action_ids),
        "invalid action ID",
    )
    actions_by_id = {row["action_id"]: row for row in actions}
    cases_by_id = {row["case_id"]: row for row in cases}
    split_objects: dict[str, set[str]] = {}
    for case in cases:
        split = case.get("split")
        _require(
            split in {"train", "development", "test", "fixture"},
            "invalid case split",
        )
        object_id = _as_string(case.get("object_id"), "object_id")
        split_objects.setdefault(split, set()).add(object_id)
        allowed = case.get("allowed_action_ids")
        _require(isinstance(allowed, list) and bool(allowed), "case has no actions")
        _require(set(allowed) <= set(actions_by_id), "case names unknown action")
        build = case.get("index_build")
        _require(isinstance(build, dict), "case index_build is missing")
        _as_nonnegative_int(build.get("source_bytes"), "index build source_bytes")
        _as_nonnegative_int(build.get("output_bytes"), "index build output_bytes")
        _require(
            build.get("latency_ms") is None,
            "unmeasured build latency must be null",
        )
        _require(
            build.get("partial_mp4_byte_range_claimed") is False,
            "unexpected partial MP4 claim",
        )
        _require(
            build.get("reduced_source_storage_io_claimed") is False,
            "unexpected storage-I/O claim",
        )
    named_splits = [
        split_objects.get(name, set())
        for name in ("train", "development", "test")
    ]
    for index, left in enumerate(named_splits):
        for right in named_splits[index + 1:]:
            _require(
                not left.intersection(right),
                "video/object appears in multiple evaluation splits",
            )
    cache_variants: dict[tuple[str, str], set[str]] = {}
    for outcome in outcomes:
        case_id = outcome.get("case_id")
        action_id = outcome.get("action_id")
        variant = outcome.get("state_variant")
        _require(case_id in cases_by_id, "outcome names unknown case")
        _require(action_id in actions_by_id, "outcome names unknown action")
        _require(
            action_id in cases_by_id[case_id]["allowed_action_ids"],
            "outcome action is not allowed for case",
        )
        _require(
            variant in {"default", "cache-miss", "cache-hit"},
            "outcome state variant is invalid",
        )
        metrics = outcome.get("metrics")
        _require(isinstance(metrics, dict), "outcome metrics are missing")
        for name in ("query_origin_bytes", "cache_bytes", "model_input_bytes"):
            _as_nonnegative_int(metrics.get(name), name)
        _as_bool(outcome.get("task_success"), "task_success")
        action = actions_by_id[action_id]
        cache_variants.setdefault((case_id, action_id), set()).add(variant)
        if not action.get("uses_cache"):
            _require(
                variant == "default",
                "non-cache action has cache state variant",
            )
    for (case_id, action_id), variants in cache_variants.items():
        if actions_by_id[action_id].get("uses_cache"):
            _require(
                variants == {"cache-miss", "cache-hit"},
                f"cache action {case_id}/{action_id} lacks an exact "
                "miss/hit pair",
            )
    _verify_no_leakage([manifest, *cases, *actions, *outcomes])


def verify_offline_replay_package(
    package_dir: str | Path,
    *,
    source_accounting_dirs: Sequence[str | Path] = (),
) -> dict[str, Any]:
    """Verify package bytes, schema, replay cells, and optional provenance."""

    root = Path(package_dir).resolve()
    _require(root.is_dir(), "offline replay package directory is missing")
    actual_files = {path.name for path in root.iterdir() if path.is_file()}
    _require(
        actual_files == PACKAGE_FILES,
        "offline replay package file set changed",
    )
    entries = _verify_checksum_directory(root)
    _require(
        set(entries) == PACKAGE_FILES - {CHECKSUMS_NAME},
        "offline replay checksum file set changed",
    )
    manifest_payload = (root / MANIFEST_NAME).read_bytes()
    cases_payload = (root / CASES_NAME).read_bytes()
    actions_payload = (root / ACTIONS_NAME).read_bytes()
    outcomes_payload = (root / OUTCOMES_NAME).read_bytes()
    for name, payload in {
        MANIFEST_NAME: manifest_payload,
        CASES_NAME: cases_payload,
        ACTIONS_NAME: actions_payload,
        OUTCOMES_NAME: outcomes_payload,
        README_NAME: (root / README_NAME).read_bytes(),
        CHECKSUMS_NAME: (root / CHECKSUMS_NAME).read_bytes(),
    }.items():
        _require(b"\r" not in payload, f"{name} contains CR bytes")
    manifest = _load_json_bytes(manifest_payload, MANIFEST_NAME)
    _require(isinstance(manifest, dict), "replay manifest is not an object")
    cases = _load_jsonl(cases_payload, CASES_NAME)
    actions = _load_jsonl(actions_payload, ACTIONS_NAME)
    outcomes = _load_jsonl(outcomes_payload, OUTCOMES_NAME)
    _require(
        manifest_payload == _json_bytes(manifest),
        "replay manifest is not canonical",
    )
    _require(
        cases_payload == _jsonl_bytes(cases),
        "cases JSONL is not canonical",
    )
    _require(
        actions_payload == _jsonl_bytes(actions),
        "actions JSONL is not canonical",
    )
    _require(
        outcomes_payload == _jsonl_bytes(outcomes),
        "outcomes JSONL is not canonical",
    )
    _validate_package_documents(manifest, cases, actions, outcomes)

    if source_accounting_dirs:
        expected = {
            (binding["object_id"], binding["run_id"]): binding
            for binding in manifest.get("source_bindings", [])
        }
        _require(
            len(expected) == len(source_accounting_dirs),
            "source accounting directory count differs",
        )
        verified_objects: set[str] = set()
        for value in source_accounting_dirs:
            source = Path(value).resolve()
            source_entries = _verify_checksum_directory(source)
            raw = (source / "accounting.json").read_bytes()
            accounting = _load_json_bytes(raw, "accounting.json")
            _require(isinstance(accounting, dict), "accounting is not an object")
            object_id = _as_string(accounting.get("object_id"), "object_id")
            run_id = _as_string(accounting.get("run_id"), "run_id")
            binding = expected.get((object_id, run_id))
            _require(
                binding is not None,
                f"unbound source accounting object {object_id}",
            )
            _require(
                binding["accounting_sha256"]
                == source_entries.get("accounting.json"),
                "source accounting digest differs",
            )
            _require(
                binding["source_sha256s_sha256"]
                == _sha256((source / CHECKSUMS_NAME).read_bytes()),
                "source SHA256SUMS digest differs",
            )
            verified_objects.add(f"{object_id}\0{run_id}")
        _require(
            verified_objects
            == {f"{object_id}\0{run_id}" for object_id, run_id in expected},
            "not all replay sources were verified",
        )

    return {
        "status": "VERIFIED_OFFLINE_REPLAY",
        "package_id": manifest["package_id"],
        "package_sha256": _sha256((root / CHECKSUMS_NAME).read_bytes()),
        "case_count": len(cases),
        "action_count": len(actions),
        "outcome_count": len(outcomes),
        "source_binding_checked": bool(source_accounting_dirs),
        "credentials_recorded": False,
        "hidden_label_values_included": False,
    }


def load_offline_replay_package(package_dir: str | Path) -> dict[str, Any]:
    """Load an internally verified replay package."""

    verify_offline_replay_package(package_dir)
    root = Path(package_dir).resolve()
    return {
        "manifest": _load_json_bytes(
            (root / MANIFEST_NAME).read_bytes(),
            MANIFEST_NAME,
        ),
        "cases": _load_jsonl((root / CASES_NAME).read_bytes(), CASES_NAME),
        "actions": _load_jsonl((root / ACTIONS_NAME).read_bytes(), ACTIONS_NAME),
        "outcomes": _load_jsonl((root / OUTCOMES_NAME).read_bytes(), OUTCOMES_NAME),
        "package_sha256": _sha256((root / CHECKSUMS_NAME).read_bytes()),
    }


@dataclass(frozen=True)
class ReplayObservation:
    """The complete public view available before policy selection."""

    case_id: str
    object_id: str
    workload_id: str
    stratum: str
    split: str
    remaining_queries: int
    available_actions: tuple[dict[str, Any], ...]
    index_available: bool
    cache_warm_by_node: Mapping[str, bool]

    def to_dict(self) -> dict[str, Any]:
        return {
            "case_id": self.case_id,
            "object_id": self.object_id,
            "workload_id": self.workload_id,
            "stratum": self.stratum,
            "split": self.split,
            "remaining_queries": self.remaining_queries,
            "available_actions": [dict(value) for value in self.available_actions],
            "state": {
                "index_available": self.index_available,
                "cache_warm_by_node": dict(self.cache_warm_by_node),
            },
        }


class ReplayPolicy(Protocol):
    """Policy boundary: observations in, one allowed physical action out."""

    policy_name: str

    def select_action(self, observation: ReplayObservation) -> str: ...


class _KindPolicy:
    def __init__(self, policy_name: str, preferred_kinds: Sequence[str]) -> None:
        self.policy_name = policy_name
        self._preferred_kinds = tuple(preferred_kinds)

    def select_action(self, observation: ReplayObservation) -> str:
        for kind in self._preferred_kinds:
            candidates = sorted(
                action["action_id"]
                for action in observation.available_actions
                if action["action_kind"] == kind
            )
            if candidates:
                return candidates[0]
        raise OfflineReplayError(f"{self.policy_name} has no supported action")


class _MyopicCostPolicy:
    policy_name = "myopic-cost-first"

    def select_action(self, observation: ReplayObservation) -> str:
        warm_nodes = {
            node for node, warm in observation.cache_warm_by_node.items() if warm
        }
        if warm_nodes:
            cached = sorted(
                action["action_id"]
                for action in observation.available_actions
                if action["action_kind"] == "read-through-derived-cache"
                and action["executor_node_id"] in warm_nodes
            )
            if cached:
                return cached[0]
        return _KindPolicy(
            self.policy_name,
            ("remote-derived", "query-aware-temporal-index", "direct-video"),
        ).select_action(observation)


class _AmortizationAwarePolicy:
    policy_name = "amortization-aware"

    def select_action(self, observation: ReplayObservation) -> str:
        preferred = (
            "query-aware-temporal-index"
            if observation.index_available or observation.remaining_queries >= 2
            else "direct-video"
        )
        return _KindPolicy(
            self.policy_name,
            (preferred, "direct-video", "remote-derived"),
        ).select_action(observation)


class _RandomPolicy:
    policy_name = "random-seeded"

    def __init__(self, seed: int) -> None:
        self._random = random.Random(seed)

    def select_action(self, observation: ReplayObservation) -> str:
        choices = sorted(
            action["action_id"] for action in observation.available_actions
        )
        _require(bool(choices), "random policy has no available action")
        return self._random.choice(choices)


def _policy(name: str, *, seed: int) -> ReplayPolicy:
    if name == "always-direct-video":
        return _KindPolicy(name, ("direct-video",))
    if name == "always-indexed":
        return _KindPolicy(name, ("query-aware-temporal-index",))
    if name == "always-derived":
        return _KindPolicy(name, ("remote-derived",))
    if name == "myopic-cost-first":
        return _MyopicCostPolicy()
    if name == "amortization-aware":
        return _AmortizationAwarePolicy()
    if name == "random-seeded":
        return _RandomPolicy(seed)
    raise OfflineReplayError(f"unknown replay policy {name!r}")


class ReplayEvaluator:
    """Exact, stateful lookup over a verified offline replay package."""

    def __init__(
        self,
        package: Mapping[str, Any],
        *,
        mode: str,
        seed: int = 0,
    ) -> None:
        _require(mode in REPLAY_MODES, f"unknown replay mode {mode!r}")
        self.mode = mode
        self._random = random.Random(seed)
        self._cases = {row["case_id"]: row for row in package["cases"]}
        self._actions = {row["action_id"]: row for row in package["actions"]}
        self._outcomes: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
        for outcome in package["outcomes"]:
            key = (
                outcome["case_id"],
                outcome["action_id"],
                outcome["state_variant"],
            )
            self._outcomes.setdefault(key, []).append(outcome)
        self._state_by_case: dict[str, dict[str, Any]] = {}

    def reset_case(self, case_id: str) -> None:
        case = self._cases.get(case_id)
        _require(case is not None, f"unknown replay case {case_id!r}")
        self._state_by_case[case_id] = {
            "index_available": bool(case["initial_state"]["index_available"]),
            "cache_warm_by_node": dict(
                case["initial_state"]["cache_warm_by_node"]
            ),
        }

    def observation(self, case_id: str, *, remaining_queries: int) -> ReplayObservation:
        _require(remaining_queries >= 1, "remaining_queries must be positive")
        case = self._cases.get(case_id)
        _require(case is not None, f"unknown replay case {case_id!r}")
        if case_id not in self._state_by_case:
            self.reset_case(case_id)
        state = self._state_by_case[case_id]
        actions = tuple(
            dict(self._actions[action_id])
            for action_id in case["allowed_action_ids"]
        )
        return ReplayObservation(
            case_id=case_id,
            object_id=case["object_id"],
            workload_id=case["workload_id"],
            stratum=case["stratum"],
            split=case["split"],
            remaining_queries=remaining_queries,
            available_actions=actions,
            index_available=bool(state["index_available"]),
            cache_warm_by_node=dict(state["cache_warm_by_node"]),
        )

    def step(
        self,
        case_id: str,
        action_id: str,
        *,
        remaining_queries: int,
    ) -> dict[str, Any]:
        observation = self.observation(case_id, remaining_queries=remaining_queries)
        case = self._cases[case_id]
        if action_id not in case["allowed_action_ids"]:
            return {
                "status": "unsupported_action",
                "case_id": case_id,
                "action_id": action_id,
                "reason": "action is not present in the frozen case",
                "state_changed": False,
            }
        action = self._actions[action_id]
        state = self._state_by_case[case_id]
        variant = "default"
        if action["uses_cache"]:
            node = action["executor_node_id"]
            variant = (
                "cache-hit" if state["cache_warm_by_node"].get(node, False)
                else "cache-miss"
            )
        candidates = self._outcomes.get((case_id, action_id, variant), [])
        if not candidates:
            return {
                "status": "unsupported_action",
                "case_id": case_id,
                "action_id": action_id,
                "reason": f"no measured outcome for state variant {variant}",
                "state_changed": False,
            }
        outcome = candidates[
            0 if len(candidates) == 1 else self._random.randrange(len(candidates))
        ]
        index_build_source_bytes = 0
        index_build_output_bytes = 0
        index_build_latency_ms: float | None = None
        if action["uses_temporal_index"] and not state["index_available"]:
            build = case["index_build"]
            index_build_source_bytes = build["source_bytes"]
            index_build_output_bytes = build["output_bytes"]
            index_build_latency_ms = build["latency_ms"]
            state["index_available"] = True
        if action["uses_cache"]:
            state["cache_warm_by_node"][action["executor_node_id"]] = True
        metrics = dict(outcome["metrics"])
        result = {
            "status": "replayed",
            "case_id": case_id,
            "action_id": action_id,
            "action_kind": action["action_kind"],
            "state_variant": variant,
            "outcome_id": outcome["outcome_id"],
            "task_success": outcome["task_success"],
            "infrastructure_complete": outcome["infrastructure_complete"],
            "metrics": {
                **metrics,
                "index_build_source_bytes": index_build_source_bytes,
                "index_build_output_bytes": index_build_output_bytes,
                "index_build_latency_ms": index_build_latency_ms,
                "total_source_bytes_charged": (
                    metrics["query_origin_bytes"] + index_build_source_bytes
                ),
            },
            "state_before": observation.to_dict()["state"],
            "state_after": {
                "index_available": bool(state["index_available"]),
                "cache_warm_by_node": dict(state["cache_warm_by_node"]),
            },
        }
        return result


def _empty_totals() -> dict[str, Any]:
    return {
        "queries": 0,
        "task_successes": 0,
        "task_failures": 0,
        "unsupported_actions": 0,
        "index_builds": 0,
        "cache_hits": 0,
        "cache_misses": 0,
        "total_query_origin_bytes": 0,
        "total_index_build_source_bytes": 0,
        "total_cache_bytes": 0,
        "total_source_bytes": 0,
        "total_model_input_bytes": 0,
        "total_index_query_ms": 0.0,
        "total_source_read_ms": 0.0,
        "total_prepare_model_input_ms": 0.0,
        "total_n6_infer_ms": 0.0,
        "total_n1_score_ms": 0.0,
        "index_build_latency_ms": None,
    }


def _add_step(totals: dict[str, Any], step: Mapping[str, Any]) -> None:
    if step["status"] != "replayed":
        totals["unsupported_actions"] += 1
        return
    metrics = step["metrics"]
    totals["queries"] += 1
    totals["task_successes"] += int(step["task_success"])
    totals["task_failures"] += int(not step["task_success"])
    totals["index_builds"] += int(metrics["index_build_source_bytes"] > 0)
    totals["cache_hits"] += int(step["state_variant"] == "cache-hit")
    totals["cache_misses"] += int(step["state_variant"] == "cache-miss")
    totals["total_query_origin_bytes"] += metrics["query_origin_bytes"]
    totals["total_index_build_source_bytes"] += metrics[
        "index_build_source_bytes"
    ]
    totals["total_cache_bytes"] += metrics["cache_bytes"]
    totals["total_source_bytes"] += metrics["total_source_bytes_charged"]
    totals["total_model_input_bytes"] += metrics["model_input_bytes"]
    for source, target in (
        ("index_query_ms", "total_index_query_ms"),
        ("source_read_ms", "total_source_read_ms"),
        ("prepare_model_input_ms", "total_prepare_model_input_ms"),
        ("n6_infer_ms", "total_n6_infer_ms"),
        ("n1_score_ms", "total_n1_score_ms"),
    ):
        totals[target] += metrics[source]


def run_offline_replay_policy(
    package_dir: str | Path,
    *,
    policy_name: str,
    mode: str,
    query_count: int,
    seed: int = 0,
    case_id: str | None = None,
) -> dict[str, Any]:
    """Run one built-in policy against exact frozen outcomes."""

    _require(query_count >= 1, "query_count must be positive")
    package = load_offline_replay_package(package_dir)
    cases = package["cases"]
    _require(bool(cases), "replay package has no cases")
    selected_case = case_id or cases[0]["case_id"]
    _require(
        any(row["case_id"] == selected_case for row in cases),
        "requested replay case is absent",
    )
    policy = _policy(policy_name, seed=seed)
    evaluator = ReplayEvaluator(package, mode=mode, seed=seed + 1)
    totals = _empty_totals()
    steps: list[dict[str, Any]] = []
    for query_index in range(query_count):
        if mode == "independent-query":
            evaluator.reset_case(selected_case)
        remaining = query_count - query_index
        observation = evaluator.observation(
            selected_case,
            remaining_queries=remaining,
        )
        action_id = policy.select_action(observation)
        step = evaluator.step(
            selected_case,
            action_id,
            remaining_queries=remaining,
        )
        steps.append(step)
        _add_step(totals, step)
        if step["status"] != "replayed":
            break
    success_rate = (
        totals["task_successes"] / totals["queries"]
        if totals["queries"] else 0.0
    )
    case = next(row for row in cases if row["case_id"] == selected_case)
    feasible = (
        totals["unsupported_actions"] == 0
        and success_rate >= float(case["minimum_success_rate"])
    )
    return {
        "schema_version": OFFLINE_REPLAY_SCHEMA_VERSION,
        "status": (
            "COMPLETE"
            if totals["unsupported_actions"] == 0
            else "UNSUPPORTED_ACTION"
        ),
        "package_id": package["manifest"]["package_id"],
        "package_sha256": package["package_sha256"],
        "policy_name": policy.policy_name,
        "mode": mode,
        "seed": seed,
        "case_id": selected_case,
        "query_count": query_count,
        "steps": steps,
        "metrics": {
            **totals,
            "success_rate": success_rate,
            "minimum_success_rate": case["minimum_success_rate"],
            "quality_constraint_satisfied": feasible,
            "latency_claim_eligible": False,
        },
        "objective": {
            "feasible": feasible,
            "lexicographic_key": [
                0 if feasible else 1,
                totals["total_source_bytes"],
                totals["total_model_input_bytes"],
            ],
        },
        "external_calls_made": False,
        "credentials_recorded": False,
        "hidden_outcomes_exposed_to_policy": False,
    }


def compare_offline_replay_baselines(
    package_dir: str | Path,
    *,
    mode: str,
    query_count: int,
    seed: int = 0,
    case_id: str | None = None,
) -> dict[str, Any]:
    """Run and rank every included baseline using the frozen objective."""

    results = [
        run_offline_replay_policy(
            package_dir,
            policy_name=name,
            mode=mode,
            query_count=query_count,
            seed=seed,
            case_id=case_id,
        )
        for name in BASELINE_POLICY_NAMES
    ]
    ranked = sorted(
        results,
        key=lambda row: tuple(row["objective"]["lexicographic_key"]),
    )
    return {
        "schema_version": OFFLINE_REPLAY_SCHEMA_VERSION,
        "status": "COMPLETE",
        "mode": mode,
        "query_count": query_count,
        "seed": seed,
        "ranking": [row["policy_name"] for row in ranked],
        "policies": results,
        "external_calls_made": False,
        "credentials_recorded": False,
    }
