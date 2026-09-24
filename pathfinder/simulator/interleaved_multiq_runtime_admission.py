"""Source-bound 24-route admission for the interleaved two-video plumbing gate.

This freezes runnable public requests, but never submits them.  The runtime
must independently verify every source, mount and endpoint before exposing a
handler; the runbook's service/worker/auth checks still gate actual execution.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Mapping

from ..integrations.flowmesh.semantic_matrix_trial import (
    build_semantic_route_request,
)
from ..rsi_exam.interleaved_multiq_plan import (
    QUESTIONS,
    interleaved_cache_episode_bindings,
    verify_interleaved_plan,
)
from .full_flow_route_adapters import FrozenIndexQueryPlan
from .hidden_oracle_commitment import verify_n1_oracle_preselection_commitment
from .interleaved_multiq_trial_dag import verify_interleaved_trial_dags
from .index_service import verify_n2_index_package

SCHEMA = "pathfinder.interleaved-multiq-runtime-admission/v1alpha1"
MANIFEST = "interleaved-runtime-admission.json"
TRIALS = "admitted-trials.jsonl"
STAGES = "admitted-stages.jsonl"
INDEX_PLANS = "index-query-plans.jsonl"
ACCESS_PLANS = "data-agent-plan-bindings.jsonl"
CACHE_EPISODES = "cache-episode-bindings.jsonl"
CHECKSUMS = "SHA256SUMS"
_CONTENT = (MANIFEST, TRIALS, STAGES, INDEX_PLANS,
            ACCESS_PLANS, CACHE_EPISODES)
_PILOT_COORDINATOR_ORIGINS = frozenset({
    "http://10.70.0.17:8780",
    "http://10.70.0.17:18780",
})


class InterleavedRuntimeAdmissionError(ValueError):
    """A 24-route admission differs from exact public inputs."""


def _require(value: object, message: str) -> None:
    if not value:
        raise InterleavedRuntimeAdmissionError(message)


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _pretty(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, indent=2,
                       ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8")


def _jsonl(values: list[dict[str, Any]]) -> bytes:
    return b"".join(_canonical(value) + b"\n" for value in values)


def _rows(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8")
            .splitlines() if line]


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _expected(
    *, trial_dag_dir: Path, binding_dir: Path, plan_dir: Path,
    n1_public_commitment_dir: Path, n2_index_package_dir: Path,
    n3_package_dir: Path, raw_package_dir: Path, n4_package_dir: Path,
    query_dir: Path, video_index_dir: Path, preparation_dir: Path,
    caption_dir: Path, coordinator_base_url: str,
) -> dict[str, bytes]:
    _require(coordinator_base_url in _PILOT_COORDINATOR_ORIGINS,
             "interleaved pilot must bind an approved N7 private origin")
    dag_report = verify_interleaved_trial_dags(
        trial_dag_dir, binding_dir=binding_dir, plan_dir=plan_dir,
        n1_public_commitment_dir=n1_public_commitment_dir,
        n3_package_dir=n3_package_dir, raw_package_dir=raw_package_dir,
        n4_package_dir=n4_package_dir, query_dir=query_dir,
        video_index_dir=video_index_dir,
        preparation_dir=preparation_dir, caption_dir=caption_dir,
    )
    public_questions = _rows(plan_dir / QUESTIONS)
    plan_doc = json.loads((plan_dir / "interleaved-plan.json").read_bytes())
    plan = verify_interleaved_plan(
        plan_dir, public_questions,
        public_source_sha256=plan_doc["public_source_sha256"],
    )
    n1 = verify_n1_oracle_preselection_commitment(n1_public_commitment_dir)
    n1_doc = json.loads((n1_public_commitment_dir /
                         "n1-oracle-preselection-commitment.json").read_bytes())
    n2 = verify_n2_index_package(n2_index_package_dir)
    lexical = json.loads((n2_index_package_dir /
                          "lexical-index.json").read_bytes())
    _require(lexical["index_id"] == n2["index_id"],
             "N2 index verifier returned a different index")
    candidates = set(lexical["candidate_object_ids"])
    _require({row["object_id"] for row in public_questions} <= candidates,
             "N2 index lacks a frozen interleaved video")
    routes = _rows(binding_dir / "route-inputs.jsonl")
    dag_trials = _rows(trial_dag_dir / "interleaved-trials.jsonl")
    stages = _rows(trial_dag_dir / "interleaved-stages.jsonl")
    _require(len(routes) == len(dag_trials) == plan["route_count"],
             "route or DAG coverage differs from the plan")
    by_route = {row["trial_key"]: row for row in routes}
    by_stage = {row["stage_key"]: row for row in stages}
    _require(len(by_route) == len(routes) and len(by_stage) == len(stages),
             "route or stage identities repeat")
    cache = interleaved_cache_episode_bindings(
        plan_dir, public_questions,
        public_source_sha256=plan["public_source_sha256"],
    )
    admitted: list[dict[str, Any]] = []
    index_plans: list[dict[str, Any]] = []
    access_plans: list[dict[str, Any]] = []
    for trial in dag_trials:
        route = by_route.get(trial["trial_key"])
        _require(route is not None, "DAG is absent from route bindings")
        _require(
            trial["public_task_binding_sha256"] == route["public_task_sha256"]
            and trial["artifact_object_id"] == route["object_id"]
            and trial["design_id"] == route["arm_id"],
            "admission trial differs from its source-bound route",
        )
        promoted = {
            **trial,
            "required_runtime_adapter_ids": [],
            "flowmesh_submission_authorized": True,
            "route_coordinator_binding": {
                "service_contract_id": "N7.execution-compute",
                "base_url": coordinator_base_url,
                "adapter_id": "contract-http-adapter-v1",
                "credential_env_names": [
                    "PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET"
                ],
            },
        }
        bound_stages = [by_stage[key]
                        for key in promoted["semantic_stage_keys"]]
        episode = route["cache_episode_id"]
        _require(
            (episode is None and promoted["design_id"] != "DC")
            or (episode is not None and promoted["design_id"] == "DC"
                and cache[(route["run_id"], route["trial_key"])] == episode),
            "cache episode differs from the frozen DC schedule",
        )
        build_semantic_route_request(
            run_id=route["run_id"],
            idempotency_key=_sha(_canonical({
                "domain": "interleaved-admission-idempotency/v1",
                "run_id": route["run_id"],
                "trial_key": route["trial_key"],
            })),
            bound_trial=promoted, bound_stages=bound_stages,
            cache_episode_id=episode,
        )
        admitted.append(promoted)
        if promoted["route_family"] == "indexed-derived":
            query_id = "query-" + _sha(_canonical({
                "domain": "pathfinder.visible-index-query-plan/v1",
                "trial_key": trial["trial_key"],
                "task_binding_sha256": trial["public_task_binding_sha256"],
            }))[:32]
            index_plan = FrozenIndexQueryPlan(
                trial_key=trial["trial_key"],
                task_binding_sha256=trial["public_task_binding_sha256"],
                index_id=n2["index_id"], query_id=query_id,
                query_text=trial["public_task_binding"]["question"],
                top_k=1, candidate_object_ids=(route["object_id"],),
            )
            index_plans.append({
                "trial_key": index_plan.trial_key,
                "task_binding_sha256": index_plan.task_binding_sha256,
                "index_id": index_plan.index_id,
                "query_id": index_plan.query_id,
                "query_text": index_plan.query_text,
                "top_k": index_plan.top_k,
                "candidate_object_ids": list(
                    index_plan.candidate_object_ids or ()),
            })
        for source in route["inputs"]:
            access_plans.append({
                "trial_key": trial["trial_key"],
                "node_id": source["node_id"],
                "object_id": route["object_id"],
                "representation_id": source["representation_id"],
                "plan_id": source["plan_id"],
                "artifact_sha256": source["artifact_sha256"],
                "artifact_size_bytes": source["artifact_size_bytes"],
            })
    _require(len(admitted) == plan["route_count"]
             and len(index_plans) == plan["question_count"]
             and len(access_plans) == 7 * plan["question_count"]
             and len(cache) == plan["question_count"],
             "runtime plan or cache coverage changed")
    admitted.sort(key=lambda row: row["order_index"])
    index_plans.sort(key=lambda row: row["trial_key"])
    access_plans.sort(key=lambda row: (
        row["trial_key"], row["node_id"], row["representation_id"]),
    )
    cache_rows = sorted(({
        "run_id": run_id, "trial_key": trial_key,
        "cache_episode_id": episode,
    } for (run_id, trial_key), episode in cache.items()),
        key=lambda row: (row["run_id"], row["trial_key"]))
    documents = {
        TRIALS: _jsonl(admitted),
        STAGES: _jsonl(stages),
        INDEX_PLANS: _jsonl(index_plans),
        ACCESS_PLANS: _jsonl(access_plans),
        CACHE_EPISODES: _jsonl(cache_rows),
    }
    manifest = {
        "schema_version": SCHEMA,
        "status": "FROZEN_INTERLEAVED_RUNTIME_ADMISSION_NOT_DEPLOYED",
        "compiler_source_sha256": _sha(Path(__file__).read_bytes()
                                        .replace(b"\r\n", b"\n")),
        "plan_sha256": plan["plan_sha256"],
        "trial_dag_package_sha256": dag_report["package_sha256"],
        "n1_public_commitment_sha256": n1["commitment_sha256"],
        "oracle_id": n1["oracle_id"],
        "public_task_set_sha256": n1_doc["public_task_set_sha256"],
        "n2_index_id": n2["index_id"],
        "n2_index_sha256": n2["index_sha256"],
        "coordinator_node_id": "N7",
        "coordinator_base_url": coordinator_base_url,
        "trial_count": len(admitted),
        "stage_count": len(stages),
        "index_query_plan_count": len(index_plans),
        "data_agent_plan_binding_count": len(access_plans),
        "cache_episode_binding_count": len(cache_rows),
        "source_commitments": {
            name: _sha(payload) for name, payload in sorted(documents.items())
        },
        "workflow_submitted": False,
        "llm_called": False,
        "hidden_label_values_read": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    manifest["admission_sha256"] = _sha(_canonical(manifest))
    return {MANIFEST: _pretty(manifest), **documents}


def _checksums(documents: Mapping[str, bytes]) -> bytes:
    return b"".join(f"{_sha(documents[name])}  {name}\n".encode("ascii")
                    for name in sorted(documents))


def freeze_interleaved_runtime_admission(
    *, output_dir: str | Path, **sources: str | Path,
) -> dict[str, Any]:
    paths = {key: (str(value) if key == "coordinator_base_url"
                   else Path(value).resolve()) for key, value in sources.items()}
    documents = _expected(**paths)
    target = Path(output_dir).resolve()
    _require(not target.exists(), "runtime admission output already exists")
    target.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".interleaved-admission-",
                                  dir=target.parent))
    try:
        for name, payload in documents.items():
            (stage / name).write_bytes(payload)
        (stage / CHECKSUMS).write_bytes(_checksums(documents))
        verify_interleaved_runtime_admission(stage, **sources)
        os.replace(stage, target)
    finally:
        if stage.exists():
            shutil.rmtree(stage)
    return verify_interleaved_runtime_admission(target, **sources)


def verify_interleaved_runtime_admission(
    admission_dir: str | Path, **sources: str | Path,
) -> dict[str, Any]:
    root = Path(admission_dir).resolve()
    _require(root.is_dir() and {path.name for path in root.iterdir()}
             == set(_CONTENT) | {CHECKSUMS},
             "runtime admission file set changed")
    paths = {key: (str(value) if key == "coordinator_base_url"
                   else Path(value).resolve()) for key, value in sources.items()}
    documents = _expected(**paths)
    _require(all((root / name).read_bytes() == payload
                 for name, payload in documents.items())
             and (root / CHECKSUMS).read_bytes() == _checksums(documents),
             "runtime admission differs from its verified source inputs")
    manifest = json.loads(documents[MANIFEST])
    return {
        "status": "VERIFIED_INTERLEAVED_RUNTIME_ADMISSION_NOT_DEPLOYED",
        "admission_sha256": manifest["admission_sha256"],
        "trial_count": manifest["trial_count"],
        "stage_count": manifest["stage_count"],
        "index_query_plan_count": manifest["index_query_plan_count"],
        "data_agent_plan_binding_count": manifest[
            "data_agent_plan_binding_count"
        ],
        "cache_episode_binding_count": manifest[
            "cache_episode_binding_count"
        ],
        "workflow_submitted": False,
        "credentials_recorded": False,
    }


__all__ = ["InterleavedRuntimeAdmissionError",
           "freeze_interleaved_runtime_admission",
           "verify_interleaved_runtime_admission"]
