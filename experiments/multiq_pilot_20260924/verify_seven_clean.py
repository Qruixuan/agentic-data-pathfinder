"""Read-only cross-check of seven-question freezes from a clean archive.

Run with ``python -P`` and PYTHONPATH set to an exact Git archive. The script
itself need not be part of that archive; all verifiers must be imported from it.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from pathfinder.rsi_exam.interleaved_multiq_plan import (
    verify_interleaved_plan,
)
from pathfinder.rsi_exam.temporal_index_layers import (
    verify_temporal_query_batch,
)
from pathfinder.simulator.hidden_oracle_commitment import (
    verify_n1_oracle_preselection_commitment,
)
from pathfinder.simulator.interleaved_multiq_route_bindings import (
    verify_interleaved_route_bindings,
)
from pathfinder.simulator.interleaved_multiq_trial_dag import (
    verify_interleaved_trial_dags,
)
from pathfinder.simulator.interleaved_multiq_runtime_admission import (
    verify_interleaved_runtime_admission,
)
from pathfinder.simulator.n3_multiq_indexed_data_plane import (
    derive_n3_multiq_question_policies,
    verify_n3_multiq_indexed_package,
)


ROOT = Path(__file__).resolve().parents[2] / "artifacts"
PLAN = ROOT / "multiq-sealed-test-plan-20260924-v2"
QUERY = ROOT / "multiq-sealed-query-20260924-v2"
VIDEO = ROOT / "interleaved-multiq-index-2561a1f-v1/video-index-v1"
PREP = ROOT / "rsi-exam-formal-temporal-preparation-ab60687-v2"
CAPTIONS = ROOT / "rsi-exam-formal-temporal-captions-7a5a8dd-v2"
RAW = ROOT / "rsi-exam-formal-n3-raw-12video-ab60687-v2"
N3 = ROOT / "multiq-sealed-n3-20260924-v2"
N4 = ROOT / "rsi-exam-formal-runtime-foundation-321f33a-v2-public/n4-package"
N2 = ROOT / "rsi-exam-formal-runtime-foundation-321f33a-v2-public/n2-package"
N1_PUBLIC = ROOT / "multiq-sealed-test-oracle-commitment-20260924-v2"
BINDINGS = ROOT / "multiq-sealed-route-bindings-20260924-v2"
DAGS = ROOT / "multiq-sealed-trial-dags-20260924-v2"
ADMISSION = ROOT / "multiq-sealed-runtime-admission-20260924-v2"
ORIGIN = "http://10.70.0.17:18780"


def main() -> None:
    import pathfinder

    source = str(Path(pathfinder.__file__).resolve())
    archive = Path(os.environ["PF_CLEAN_SOURCE_ROOT"]).resolve()
    if not Path(source).is_relative_to(archive):
        raise ValueError("verifier did not import the clean Git archive")
    questions = [json.loads(line) for line in (
        PLAN / "public-questions.jsonl"
    ).read_text(encoding="utf-8").splitlines()]
    manifest = json.loads((PLAN / "interleaved-plan.json").read_text(
        encoding="utf-8"
    ))
    plan = verify_interleaved_plan(
        PLAN, questions,
        public_source_sha256=manifest["public_source_sha256"],
    )
    query_questions = [{key: row[key] for key in (
        "question_id", "object_id", "question"
    )} for row in questions]
    query = verify_temporal_query_batch(
        QUERY, VIDEO, PREP, CAPTIONS, query_questions,
    )
    n1 = verify_n1_oracle_preselection_commitment(N1_PUBLIC)
    policies = derive_n3_multiq_question_policies(
        plan_dir=PLAN, public_questions=questions,
        public_source_sha256=manifest["public_source_sha256"],
        query_dir=QUERY, video_index_dir=VIDEO,
        preparation_dir=PREP, caption_dir=CAPTIONS,
        raw_package_dir=RAW,
    )
    n3 = verify_n3_multiq_indexed_package(
        N3, raw_package_dir=RAW, question_policies=policies,
    )
    sources = {
        "plan_dir": PLAN, "n1_public_commitment_dir": N1_PUBLIC,
        "n3_package_dir": N3, "raw_package_dir": RAW,
        "n4_package_dir": N4, "query_dir": QUERY,
        "video_index_dir": VIDEO, "preparation_dir": PREP,
        "caption_dir": CAPTIONS,
    }
    routes = verify_interleaved_route_bindings(BINDINGS, **sources)
    dags = verify_interleaved_trial_dags(
        DAGS, binding_dir=BINDINGS, **sources,
    )
    admission = verify_interleaved_runtime_admission(
        ADMISSION, trial_dag_dir=DAGS, binding_dir=BINDINGS,
        n2_index_package_dir=N2, coordinator_base_url=ORIGIN,
        **sources,
    )
    if not (
        plan["route_count"] == routes["route_count"]
        == dags["trial_count"] == admission["trial_count"] == 28
        and query["question_count"] == n1["label_count"]
        == n3["question_count"] == 7
    ):
        raise ValueError("seven-question clean-source counts differ")
    print(json.dumps({
        "status": "CLEAN_SOURCE_SEVEN_QUESTION_INPUTS_VERIFIED",
        "archive_source": source,
        "plan_sha256": plan["plan_sha256"],
        "query_sha256": query["package_sha256"],
        "n1_commitment_sha256": n1["commitment_sha256"],
        "n3_package_id": n3["package_id"],
        "route_binding_sha256": routes["manifest_sha256"],
        "trial_dag_sha256": dags["package_sha256"],
        "admission_sha256": admission["admission_sha256"],
        "route_count": 28,
        "credentials_recorded": False,
    }, sort_keys=True))


if __name__ == "__main__":
    main()
