"""Resolve an N2 temporal selection by video *and* public task binding."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from ..rsi_exam.interleaved_multiq_plan import (
    ARMS,
    QUESTIONS,
    SCHEDULE,
    interleaved_trial_key,
)
from ..rsi_exam.ten_route_multiq_plan import (
    SCHEMA as TEN_ROUTE_SCHEMA,
    LIGHT_D_SCHEMA,
    SCHEDULE as TEN_ROUTE_SCHEDULE,
    load_verified_multiq_plan,
    ten_route_trial_key,
)
from ..video_prep import sample_video
from .full_flow_semantic_route_runtime import (
    ArtifactIdentity,
    ExactTemporalFrameSelection,
)
from .n3_indexed_data_plane import INDEXED_REPRESENTATION_ID
from .n3_multiq_indexed_data_plane import verify_n3_multiq_indexed_package
from .n4_derived_data_plane import (
    PACKAGE_MANIFEST_NAME as N4_PACKAGE_MANIFEST_NAME,
    verify_n4_derived_data_package,
)
from .raw_cold_data_plane import PACKAGE_MANIFEST_NAME


class MultiQuestionSelectionError(ValueError):
    """No source-bound selection exists for this video/question pair."""


class MultiQuestionExactSelectionCatalog:
    def __init__(
        self,
        n3_package_dir: str | Path,
        *, raw_package_dir: str | Path,
        question_policies: Sequence[Mapping[str, Any]],
        sampler: Any = sample_video,
    ) -> None:
        root = Path(n3_package_dir).resolve()
        verify_n3_multiq_indexed_package(
            root, raw_package_dir=raw_package_dir,
            question_policies=question_policies, sampler=sampler,
        )
        report = json.loads((root / PACKAGE_MANIFEST_NAME).read_bytes())
        self.catalog_sha256 = hashlib.sha256(
            (root / PACKAGE_MANIFEST_NAME).read_bytes()
        ).hexdigest()
        raw_by_object = {
            row["object_id"]: row for row in report["raw_objects"]
        }
        self._selections = {}
        self._plan_ids = {}
        for row in report["question_selections"]:
            object_id = row["object_id"]
            task_sha = row["task_binding_sha256"]
            raw = raw_by_object[object_id]
            window = row["selection_policy"]["temporal_window_fraction"]
            key = (object_id, task_sha)
            if key in self._selections:
                raise MultiQuestionSelectionError(
                    "question selection identity repeats"
                )
            self._selections[key] = ExactTemporalFrameSelection(
                object_id=object_id,
                representation_id="raw_video",
                object_catalog_version=raw["catalog_version"],
                full_artifact_size_bytes=raw["artifact_size_bytes"],
                full_artifact_sha256=raw["artifact_sha256"],
                selected_representation_id=INDEXED_REPRESENTATION_ID,
                selected_artifact_size_bytes=row["artifact_size_bytes"],
                selected_artifact_sha256=row["artifact_sha256"],
                frame_count=row["selection_policy"]["frame_count"],
                temporal_start_fraction=window[0],
                temporal_end_fraction=window[1],
                selection_policy_sha256=row["selection_policy_sha256"],
            )
            self._plan_ids[key] = row["plan_id"]

    def resolve_for_task(
        self, identity: ArtifactIdentity, *, task_binding_sha256: str,
    ) -> ExactTemporalFrameSelection:
        key = (identity.object_id, task_binding_sha256)
        selection = self._selections.get(key)
        if selection is None or not selection.matches(identity):
            raise MultiQuestionSelectionError(
                "no exact temporal selection binds this video and task"
            )
        return selection

    def plan_id_for_task(
        self, object_id: str, task_binding_sha256: str,
    ) -> str:
        plan_id = self._plan_ids.get((object_id, task_binding_sha256))
        if plan_id is None:
            raise MultiQuestionSelectionError(
                "no N3 plan ID binds this video and task"
            )
        return plan_id


_N4_PLAN_BY_ARM = {"D": "D2", "DC": "D3", "I": "D2"}


def interleaved_data_agent_plan_bindings(
    *,
    plan_dir: str | Path,
    public_questions: Sequence[Mapping[str, Any]],
    public_source_sha256: str,
    n3_package_dir: str | Path,
    raw_package_dir: str | Path,
    question_policies: Sequence[Mapping[str, Any]],
    n4_package_dir: str | Path,
    sampler: Any = sample_video,
) -> dict[tuple[str, str, str, str], str]:
    """Rebuild exact per-trial Data Agent plans from a verified multiq plan.

    N3's selected bundle is addressed by both video and public task digest;
    an arm ID or video ID alone is never sufficient. N4's existing D2/D3
    plans address question-independent artifacts of the same video.
    """

    plan_root = Path(plan_dir).resolve()
    manifest, _, plan = load_verified_multiq_plan(
        plan_root, public_questions,
    )
    if manifest["public_source_sha256"] != public_source_sha256:
        raise MultiQuestionSelectionError("public source digest differs")
    ten_route = manifest["schema_version"] in {TEN_ROUTE_SCHEMA, LIGHT_D_SCHEMA}
    light_d = manifest["schema_version"] == LIGHT_D_SCHEMA
    frame_only = light_d and manifest["derived_representation_ids"] == [
        "sampled_frame_bundle"
    ]
    n3_root = Path(n3_package_dir).resolve()
    n3 = MultiQuestionExactSelectionCatalog(
        n3_root, raw_package_dir=raw_package_dir,
        question_policies=question_policies, sampler=sampler,
    )
    n4_root = Path(n4_package_dir).resolve()
    verify_n4_derived_data_package(n4_root)
    n3_manifest = json.loads((n3_root / PACKAGE_MANIFEST_NAME).read_bytes())
    n4_manifest = json.loads(
        (n4_root / N4_PACKAGE_MANIFEST_NAME).read_bytes()
    )
    n3_keys = {
        (row["object_id"], row["task_binding_sha256"])
        for row in n3_manifest["question_selections"]
    }
    questions = {
        row["question_id"]: row
        for row in (json.loads(line) for line in
                    (plan_root / QUESTIONS).read_text(encoding="utf-8")
                    .splitlines())
    }
    expected_keys = {
        (row["object_id"], row["public_task_sha256"])
        for row in questions.values()
    }
    if n3_keys != expected_keys:
        raise MultiQuestionSelectionError(
            "N3 question selections differ from the frozen public tasks"
        )
    for selection in n3_manifest["question_selections"]:
        question = questions.get(selection["question_id"])
        if (
            question is None
            or selection["object_id"] != question["object_id"]
            or selection["task_binding_sha256"]
            != question["public_task_sha256"]
            or selection["public_question_sha256"]
            != hashlib.sha256(question["question"].encode("utf-8")).hexdigest()
        ):
            raise MultiQuestionSelectionError(
                "N3 temporal selection does not bind the frozen question text"
            )
    n4_rows = {
        (row["object_id"], row["representation_id"]): row
        for row in n4_manifest["objects"]
    }
    if len(n4_rows) != len(n4_manifest["objects"]):
        raise MultiQuestionSelectionError("N4 artifact identities repeat")
    schedule = [
        json.loads(line) for line in
        (plan_root / (TEN_ROUTE_SCHEDULE if ten_route else SCHEDULE))
        .read_text(encoding="utf-8").splitlines()
    ]
    bindings: dict[tuple[str, str, str, str], str] = {}
    for row in schedule:
        question = questions[row["question_id"]]
        object_id = row["object_id"]
        task_sha = question["public_task_sha256"]
        n3_plan_id = n3.plan_id_for_task(object_id, task_sha)
        for slot in row["route_slots"]:
            arm = slot["arm_id"]
            if arm not in ARMS:
                raise MultiQuestionSelectionError("route arm is invalid")
            trial_key = (
                ten_route_trial_key(
                    manifest["experiment_id"], question["question_id"],
                    slot["design_id"], slot["repetition"],
                ) if ten_route else interleaved_trial_key(
                    manifest["experiment_id"], question["question_id"], arm,
                )
            )
            if arm == "R":
                bindings[(trial_key, "N3", object_id, "raw_video")] = (
                    n3_plan_id
                )
                continue
            if arm == "I" and ten_route:
                bindings[(trial_key, "N3", object_id,
                          INDEXED_REPRESENTATION_ID)] = n3_plan_id
                continue
            n4_plan_id = _N4_PLAN_BY_ARM[arm]
            representations = (
                ("multimodal_digest",)
                if arm == "I"
                else (("sampled_frame_bundle",) if frame_only else
                      ("multimodal_digest", "sampled_frame_bundle"))
            )
            for representation in representations:
                n4_row = n4_rows.get((object_id, representation))
                if n4_row is None or n4_plan_id not in n4_row["plan_ids"]:
                    raise MultiQuestionSelectionError(
                        "N4 lacks the frozen arm artifact plan binding"
                    )
                bindings[(trial_key, "N4", object_id, representation)] = (
                    n4_plan_id
                )
            if arm == "I":
                bindings[(
                    trial_key, "N3", object_id,
                    INDEXED_REPRESENTATION_ID,
                )] = n3_plan_id
    expected = (6 if frame_only else 12 if light_d
                else 16 if ten_route else 7) * len(questions)
    if len(bindings) != expected:
        # New ten-slot profile: both nodes have R=1, I=1, D=2,
        # and two DC repetitions of two entries (eight per node).
        raise MultiQuestionSelectionError(
            "multi-question Data Agent plan binding count differs"
        )
    return bindings


__all__ = [
    "MultiQuestionExactSelectionCatalog", "MultiQuestionSelectionError",
    "interleaved_data_agent_plan_bindings",
]
