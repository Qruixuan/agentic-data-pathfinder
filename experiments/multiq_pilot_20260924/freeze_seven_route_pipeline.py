"""Freeze source-bound seven-question route inputs, DAGs and admission.

Each stage is separate and immutable. No stage submits a workflow or calls a
model. Re-running a completed stage fails instead of overwriting evidence.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pathfinder.simulator.interleaved_multiq_route_bindings import (
    freeze_interleaved_route_bindings,
    verify_interleaved_route_bindings,
)
from pathfinder.simulator.interleaved_multiq_trial_dag import (
    freeze_interleaved_trial_dags,
    verify_interleaved_trial_dags,
)
from pathfinder.simulator.interleaved_multiq_runtime_admission import (
    freeze_interleaved_runtime_admission,
    verify_interleaved_runtime_admission,
)


ROOT = Path(__file__).resolve().parents[2] / "artifacts"
BINDINGS = ROOT / "multiq-sealed-route-bindings-20260924-v2"
DAGS = ROOT / "multiq-sealed-trial-dags-20260924-v2"
ADMISSION = ROOT / "multiq-sealed-runtime-admission-20260924-v2"
SOURCES = {
    "plan_dir": ROOT / "multiq-sealed-test-plan-20260924-v2",
    "n1_public_commitment_dir": (
        ROOT / "multiq-sealed-test-oracle-commitment-20260924-v2"
    ),
    "n3_package_dir": ROOT / "multiq-sealed-n3-20260924-v2",
    "raw_package_dir": ROOT / "rsi-exam-formal-n3-raw-12video-ab60687-v2",
    "n4_package_dir": (
        ROOT / "rsi-exam-formal-runtime-foundation-321f33a-v2-public/n4-package"
    ),
    "query_dir": ROOT / "multiq-sealed-query-20260924-v2",
    "video_index_dir": (
        ROOT / "interleaved-multiq-index-2561a1f-v1/video-index-v1"
    ),
    "preparation_dir": (
        ROOT / "rsi-exam-formal-temporal-preparation-ab60687-v2"
    ),
    "caption_dir": ROOT / "rsi-exam-formal-temporal-captions-7a5a8dd-v2",
}
N2 = ROOT / "rsi-exam-formal-runtime-foundation-321f33a-v2-public/n2-package"
ORIGIN = "http://10.70.0.17:18780"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("stage", choices=("bindings", "dags", "admission"))
    args = parser.parse_args()
    if args.stage == "bindings":
        report = freeze_interleaved_route_bindings(
            output_dir=BINDINGS, **SOURCES,
        )
        verified = verify_interleaved_route_bindings(BINDINGS, **SOURCES)
        expected = (28, 7, 49)
        observed = (report["route_count"], report["question_count"],
                    report["data_agent_binding_count"])
    elif args.stage == "dags":
        report = freeze_interleaved_trial_dags(
            output_dir=DAGS, binding_dir=BINDINGS, **SOURCES,
        )
        verified = verify_interleaved_trial_dags(
            DAGS, binding_dir=BINDINGS, **SOURCES,
        )
        expected = (28,)
        observed = (report["trial_count"],)
    else:
        report = freeze_interleaved_runtime_admission(
            output_dir=ADMISSION, trial_dag_dir=DAGS,
            binding_dir=BINDINGS, n2_index_package_dir=N2,
            coordinator_base_url=ORIGIN, **SOURCES,
        )
        verified = verify_interleaved_runtime_admission(
            ADMISSION, trial_dag_dir=DAGS,
            binding_dir=BINDINGS, n2_index_package_dir=N2,
            coordinator_base_url=ORIGIN, **SOURCES,
        )
        expected = (28, 7, 49, 7)
        observed = (report["trial_count"],
                    report["index_query_plan_count"],
                    report["data_agent_plan_binding_count"],
                    report["cache_episode_binding_count"])
    if report != verified or observed != expected:
        raise ValueError("seven-question source binding or counts differ")
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
