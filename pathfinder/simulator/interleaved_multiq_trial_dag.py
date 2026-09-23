"""Construct four-arm multi-question route DAGs without admitting execution.

The builder is deliberately endpoint- and credential-free.  A later runtime
admission must bind service endpoints, exact access plans, source digests, and
the N1 oracle before it may set ``flowmesh_submission_authorized`` to true.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Mapping, Sequence

from ..distributed.scoring import MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE
from .full_flow_semantic_execution_admission import (
    BOUND_STAGE_SCHEMA_VERSION,
    BOUND_TRIAL_SCHEMA_VERSION,
)
from .full_flow_semantic_input_profiles import (
    QUERY_AWARE_TEMPORAL_INDEX_SELECTION,
    build_semantic_input_profile,
)
from .full_flow_semantic_route_runtime import _validate_trial_and_stages
from .hidden_oracle import build_n1_public_task_binding
from .interleaved_multiq_route_bindings import (
    verify_interleaved_route_bindings,
)

PACKAGE_SCHEMA = "pathfinder.interleaved-multiq-trial-dags/v1alpha1"
PACKAGE_MANIFEST = "interleaved-trial-dags.json"
PACKAGE_TRIALS = "interleaved-trials.jsonl"
PACKAGE_STAGES = "interleaved-stages.jsonl"
PACKAGE_CHECKSUMS = "SHA256SUMS"


class InterleavedTrialDagError(ValueError):
    """One of the four arms cannot form an exact, verified DAG."""


def _require(value: object, message: str) -> None:
    if not value:
        raise InterleavedTrialDagError(message)


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _pretty(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, indent=2,
                       ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8")


def _jsonl(values: Sequence[Mapping[str, Any]]) -> bytes:
    return b"".join(_canonical(value) + b"\n" for value in values)


def _identity(
    object_id: str, representation: str | None,
    artifact: Mapping[str, Any] | None,
    catalog_version: str | None,
) -> dict[str, Any]:
    return {
        "logical_object_id": object_id,
        "artifact_object_id": object_id,
        "representation_id": representation,
        "representation_binding": (
            None if representation is None else {
                "representation_id": representation,
                "artifact_sha256": artifact["artifact_sha256"],
                "artifact_size_bytes": artifact["artifact_size_bytes"],
                "object_catalog_version": catalog_version,
            }
        ),
    }


def _stage_specs(arm: str) -> list[tuple[str, str, str | None, list[str],
                                        list[str], dict[str, Any] | None]]:
    """Return named topological edges, with cache branches explicit."""

    specs = []

    def add(name: str, action: str, rep: str | None,
            nodes: list[str], deps: list[str],
            condition: dict[str, Any] | None = None) -> None:
        specs.append((name, action, rep, nodes, deps, condition))

    add("schedule", "admit-trial", None, ["N1"], [])
    if arm == "R":
        add("read-raw", "access-raw-artifact", "raw_video", ["N3"],
            ["schedule"])
        add("transfer-raw", "transfer-bytes", "raw_video", ["N3", "N7"],
            ["read-raw"])
        add("decode", "prepare-model-input", None, ["N7"],
            ["transfer-raw"])
        add("send-model-input", "transfer-bytes", None, ["N7", "N6"],
            ["decode"])
        infer_deps = ["send-model-input"]
    elif arm == "I":
        add("query-index", "query-index", None, ["N2"], ["schedule"])
        add("return-index", "transfer-bytes", None, ["N2", "N7"],
            ["query-index"])
        add("read-selected", "access-raw-artifact", "raw_video", ["N3"],
            ["return-index"])
        add("transfer-selected", "transfer-bytes", "raw_video",
            ["N3", "N7"], ["read-selected"])
        add("read-digest", "access-derived-artifact", "multimodal_digest",
            ["N4"], ["schedule"])
        add("transfer-digest", "transfer-bytes", "multimodal_digest",
            ["N4", "N7"], ["read-digest"])
        infer_deps = ["transfer-selected", "transfer-digest"]
    elif arm == "D":
        for rep, suffix in (("multimodal_digest", "digest"),
                            ("sampled_frame_bundle", "frames")):
            add(f"read-{suffix}", "access-derived-artifact", rep,
                ["N4"], ["schedule"])
            add(f"transfer-{suffix}", "transfer-bytes", rep,
                ["N4", "N7"], [f"read-{suffix}"])
            add(f"send-{suffix}", "transfer-bytes", rep,
                ["N7", "N6"], [f"transfer-{suffix}"])
        infer_deps = ["send-digest", "send-frames"]
    elif arm == "DC":
        for rep, suffix in (("multimodal_digest", "digest"),
                            ("sampled_frame_bundle", "frames")):
            lookup = f"lookup-{suffix}"
            hit = {"cache_operation_id": lookup,
                   "cache_operation_key": lookup, "equals": "hit"}
            miss = {**hit, "equals": "miss"}
            add(lookup, "lookup", rep, ["N7"], ["schedule"])
            add(f"read-local-{suffix}", "read", rep, ["N7"], [lookup], hit)
            add(f"read-remote-{suffix}", "access-derived-artifact", rep,
                ["N4"], [lookup], miss)
            add(f"transfer-{suffix}", "transfer-bytes", rep,
                ["N4", "N7"], [lookup, f"read-remote-{suffix}"], miss)
            add(f"insert-{suffix}", "insert", rep, ["N7"],
                [lookup, f"transfer-{suffix}"], miss)
            add(f"{suffix}-ready", "join-hit-or-miss-branch", None,
                ["N7"], [f"read-local-{suffix}", f"insert-{suffix}"])
            add(f"send-{suffix}", "transfer-bytes", rep,
                ["N7", "N6"], [f"{suffix}-ready"])
        infer_deps = ["send-digest", "send-frames"]
    else:
        raise InterleavedTrialDagError("route arm is unsupported")
    add("infer", "infer", None, ["N6"], infer_deps)
    add("return-answer", "transfer-bytes", None, ["N6", "N1"], ["infer"])
    add("hidden-score", "score-hidden-answer", None, ["N1"],
        ["return-answer"])
    return specs


def build_interleaved_trial_dag(
    route: Mapping[str, Any], question: Mapping[str, Any],
    *, raw_artifact: Mapping[str, Any],
    derived_artifacts: Mapping[str, Mapping[str, Any]],
    n3_catalog_version: str, n4_catalog_version: str,
    selected_policy: Mapping[str, Any] | None = None,
    worker_alias: str = "pathfinder_costaware_20260815a",
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Build one N7 DAG and validate it against the actual route executor."""

    arm = route["arm_id"]
    object_id = route["object_id"]
    _require(object_id == question["object_id"]
             and route["public_task_sha256"] == question["public_task_sha256"],
             "route and public question disagree")
    public_task = build_n1_public_task_binding(
        workload_id=question["question_id"], object_id=object_id,
        task_class_id=question["stratum"], question=question["question"],
        answer_options=question["answer_options"],
        success_scoring_rule=MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
    )
    _require(public_task["task_binding_sha256"]
             == question["public_task_sha256"],
             "public question digest changed")
    identities = {
        "raw_video": _identity(object_id, "raw_video", raw_artifact,
                               n3_catalog_version),
        **{
            rep: _identity(object_id, rep, artifact, n4_catalog_version)
            for rep, artifact in derived_artifacts.items()
        },
    }
    if arm == "R":
        route_family = "raw"
        representations = ["raw_video"]
    elif arm == "I":
        route_family = "indexed-derived"
        representations = ["raw_video", "multimodal_digest"]
        _require(isinstance(selected_policy, Mapping),
                 "indexed arm lacks a question-bound N3 policy")
    else:
        route_family = ("local-cache-derived" if arm == "DC"
                        else "remote-derived")
        representations = ["multimodal_digest", "sampled_frame_bundle"]
    trial_key = route["trial_key"]
    specs = _stage_specs(arm)
    stages = []
    for index, (name, action, rep, nodes, deps, condition) in enumerate(specs):
        condition = None if condition is None else {
            **condition,
            "cache_operation_key": f"{trial_key}|{condition['cache_operation_key']}",
        }
        stage = {
            "schema_version": BOUND_STAGE_SCHEMA_VERSION,
            "stage_key": f"{trial_key}|{name}",
            "trial_key": trial_key,
            "stage_index": index,
            "phase": "execution",
            "action": action,
            "condition": condition,
            "dependency_stage_keys": [f"{trial_key}|{dep}" for dep in deps],
            "logical_node_ids": nodes,
            "object_representation_identity": (
                identities[rep] if rep else _identity(object_id, None,
                                                       None, None)
            ),
            "public_task_binding_sha256": public_task["task_binding_sha256"],
            "credential_values_included": False,
        }
        stages.append(stage)
    if arm == "I":
        window = selected_policy["temporal_window_fraction"]
        profile = build_semantic_input_profile(
            route_family=route_family,
            model_input_representation_ids=representations,
            indexed_selection_kind=QUERY_AWARE_TEMPORAL_INDEX_SELECTION,
            indexed_frame_count=selected_policy["frame_count"],
            indexed_temporal_window_fraction=(float(window[0]),
                                              float(window[1])),
        )
    else:
        profile = build_semantic_input_profile(
            route_family=route_family,
            model_input_representation_ids=representations,
        )
    trial = {
        "schema_version": BOUND_TRIAL_SCHEMA_VERSION,
        "trial_key": trial_key,
        "order_index": route["ordinal"] * 4
        + ("R", "D", "DC", "I").index(arm),
        "workload_id": question["question_id"],
        "workload_class": {"descriptive": "W1", "temporal": "W2",
                           "causal": "W3"}[question["stratum"]],
        "design_id": arm,
        "repetition": 0,
        "route_family": route_family,
        "executor_node_id": "N7",
        "public_task_binding_sha256": public_task["task_binding_sha256"],
        "public_task_binding": public_task,
        "artifact_object_id": object_id,
        "representation_identities": [identities[rep]
                                      for rep in representations],
        "semantic_stage_keys": [stage["stage_key"] for stage in stages],
        "bound_stage_sha256": [_sha(_canonical(stage)) for stage in stages],
        "required_provisioning_chain_ids": [],
        "source_semantic_trial_sha256": _sha(_canonical(route)),
        "worker_alias": worker_alias,
        "flowmesh_execution_shape": "one-api-task-to-route-coordinator",
        "required_runtime_adapter_ids": ["multiq-runtime-admission-pending"],
        "flowmesh_submission_authorized": False,
        "semantic_execution_performed": False,
        "semantic_input_profile": profile,
        "credentials_recorded": False,
    }
    try:
        _validate_trial_and_stages(trial, stages)
    except Exception as exc:
        raise InterleavedTrialDagError(
            f"{arm} route DAG is invalid: {type(exc).__name__}: {exc}"
        ) from exc
    return trial, stages


def build_all_interleaved_trial_dags(
    *, binding_dir: str | Path, plan_dir: str | Path,
    n3_package_dir: str | Path, n4_package_dir: str | Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Build all 24 endpoint-free DAGs from one already-verified binding."""

    root = Path(binding_dir)
    manifest = json.loads((root / "route-input-bindings.json").read_bytes())
    _require(manifest["status"]
             == "FROZEN_INTERLEAVED_ROUTE_INPUTS_NOT_ADMITTED"
             and manifest["runtime_admission_created"] is False,
             "route binding is not a non-admitted frozen input")
    routes = [json.loads(line) for line in
              (root / "route-inputs.jsonl").read_text(encoding="utf-8")
              .splitlines()]
    questions = {row["question_id"]: row for row in
                 (json.loads(line) for line in
                  (Path(plan_dir) / "public-questions.jsonl")
                  .read_text(encoding="utf-8").splitlines())}
    n3 = json.loads((Path(n3_package_dir) /
                     "raw-cold-data-plane.json").read_bytes())
    n4 = json.loads((Path(n4_package_dir) /
                     "n4-derived-data-package.json").read_bytes())
    raw = {row["object_id"]: row for row in n3["raw_objects"]}
    selected = {(row["object_id"], row["task_binding_sha256"]): row
                for row in n3["question_selections"]}
    derived = {(row["object_id"], row["representation_id"]): row
               for row in n4["objects"]}
    trials = []
    stages = []
    for route in routes:
        object_id = route["object_id"]
        question = questions[route["question_id"]]
        trial, trial_stages = build_interleaved_trial_dag(
            route, question,
            raw_artifact=raw[object_id],
            derived_artifacts={rep: derived[(object_id, rep)]
                               for rep in ("multimodal_digest",
                                           "sampled_frame_bundle")},
            n3_catalog_version=n3["catalog_version"],
            n4_catalog_version=n4["catalog_version"],
            selected_policy=(selected[(object_id,
                                       question["public_task_sha256"])]
                             ["selection_policy"]
                             if route["arm_id"] == "I" else None),
        )
        trials.append(trial)
        stages.extend(trial_stages)
    _require(len(trials) == 24
             and len({row["trial_key"] for row in trials}) == 24,
             "24-route trial DAG coverage changed")
    return trials, stages


def _package_contents(
    *, binding_dir: str | Path, plan_dir: str | Path,
    n3_package_dir: str | Path, n4_package_dir: str | Path,
    **binding_sources: str | Path,
) -> dict[str, bytes]:
    bound = verify_interleaved_route_bindings(
        binding_dir, plan_dir=plan_dir,
        n3_package_dir=n3_package_dir, n4_package_dir=n4_package_dir,
        **binding_sources,
    )
    trials, stages = build_all_interleaved_trial_dags(
        binding_dir=binding_dir, plan_dir=plan_dir,
        n3_package_dir=n3_package_dir, n4_package_dir=n4_package_dir,
    )
    trial_bytes = _jsonl(trials)
    stage_bytes = _jsonl(stages)
    manifest = {
        "schema_version": PACKAGE_SCHEMA,
        "status": "FROZEN_INTERLEAVED_TRIAL_DAGS_NOT_ADMITTED",
        "route_binding_manifest_sha256": bound["manifest_sha256"],
        "compiler_source_sha256": _sha(Path(__file__).read_bytes()
                                        .replace(b"\r\n", b"\n")),
        "trial_count": len(trials),
        "stage_count": len(stages),
        "trials_sha256": _sha(trial_bytes),
        "stages_sha256": _sha(stage_bytes),
        "runtime_admission_created": False,
        "workflow_submitted": False,
        "llm_called": False,
        "hidden_label_values_read": False,
        "credentials_recorded": False,
    }
    manifest["package_sha256"] = _sha(_canonical(manifest))
    return {
        PACKAGE_MANIFEST: _pretty(manifest),
        PACKAGE_TRIALS: trial_bytes,
        PACKAGE_STAGES: stage_bytes,
    }


def _checksums(documents: Mapping[str, bytes]) -> bytes:
    return b"".join(
        f"{_sha(documents[name])}  {name}\n".encode("ascii")
        for name in sorted(documents)
    )


def freeze_interleaved_trial_dags(
    *, output_dir: str | Path, **sources: str | Path,
) -> dict[str, Any]:
    """Freeze 24 validated DAGs, with submission authority still withheld."""

    documents = _package_contents(**sources)
    target = Path(output_dir).resolve()
    _require(not target.exists(), "trial DAG output already exists")
    target.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".multiq-trial-dags-",
                                  dir=target.parent))
    try:
        for name, payload in documents.items():
            (stage / name).write_bytes(payload)
        (stage / PACKAGE_CHECKSUMS).write_bytes(_checksums(documents))
        verify_interleaved_trial_dags(stage, **sources)
        os.replace(stage, target)
    finally:
        if stage.exists():
            shutil.rmtree(stage)
    return verify_interleaved_trial_dags(target, **sources)


def verify_interleaved_trial_dags(
    package_dir: str | Path, **sources: str | Path,
) -> dict[str, Any]:
    root = Path(package_dir).resolve()
    names = {PACKAGE_MANIFEST, PACKAGE_TRIALS,
             PACKAGE_STAGES, PACKAGE_CHECKSUMS}
    _require(root.is_dir() and {path.name for path in root.iterdir()} == names,
             "trial DAG file set changed")
    documents = _package_contents(**sources)
    _require(
        all((root / name).read_bytes() == payload
            for name, payload in documents.items())
        and (root / PACKAGE_CHECKSUMS).read_bytes() == _checksums(documents),
        "trial DAGs differ from their exact source bindings",
    )
    manifest = json.loads(documents[PACKAGE_MANIFEST])
    return {
        "status": "VERIFIED_INTERLEAVED_TRIAL_DAGS_NOT_ADMITTED",
        "trial_count": manifest["trial_count"],
        "stage_count": manifest["stage_count"],
        "package_sha256": manifest["package_sha256"],
        "runtime_admission_created": False,
        "workflow_submitted": False,
        "credentials_recorded": False,
    }


__all__ = ["InterleavedTrialDagError", "build_interleaved_trial_dag",
           "build_all_interleaved_trial_dags", "freeze_interleaved_trial_dags",
           "verify_interleaved_trial_dags"]
