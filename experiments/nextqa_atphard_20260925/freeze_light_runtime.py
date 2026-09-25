"""Reuse canonical binding/DAG/admission freezers for the light-D plan."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pathfinder.rsi_exam.ten_route_multiq_plan import (
    LIGHT_D_SCHEMA, load_verified_multiq_plan,
)
from pathfinder.simulator.interleaved_multiq_route_bindings import (
    freeze_interleaved_route_bindings,
)
from pathfinder.simulator.interleaved_multiq_trial_dag import (
    freeze_interleaved_trial_dags,
)
from pathfinder.simulator.interleaved_multiq_runtime_admission import (
    freeze_interleaved_runtime_admission,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    for name in (
        "plan", "n1-public-commitment", "n2-index-package",
        "n3-package", "raw-package", "n4-package", "query",
        "video-index", "preparation", "caption", "output",
    ):
        parser.add_argument(f"--{name}-dir", type=Path, required=True)
    parser.add_argument("--coordinator-n7", required=True)
    parser.add_argument("--coordinator-n8", required=True)
    args = parser.parse_args()
    plan, _, _ = load_verified_multiq_plan(args.plan_dir)
    if plan["schema_version"] != LIGHT_D_SCHEMA:
        raise ValueError("runtime inputs need a verified light-D plan")
    output = args.output_dir.resolve()
    if output.exists():
        raise ValueError("runtime output root already exists")
    output.mkdir(parents=True, exist_ok=False)
    binding_sources = {
        "plan_dir": args.plan_dir,
        "n1_public_commitment_dir": args.n1_public_commitment_dir,
        "n3_package_dir": args.n3_package_dir,
        "raw_package_dir": args.raw_package_dir,
        "n4_package_dir": args.n4_package_dir,
        "query_dir": args.query_dir,
        "video_index_dir": args.video_index_dir,
        "preparation_dir": args.preparation_dir,
        "caption_dir": args.caption_dir,
    }
    binding = freeze_interleaved_route_bindings(
        output_dir=output / "bindings", **binding_sources,
    )
    dag = freeze_interleaved_trial_dags(
        output_dir=output / "dags", binding_dir=output / "bindings",
        **binding_sources,
    )
    admission = freeze_interleaved_runtime_admission(
        output_dir=output / "admission",
        trial_dag_dir=output / "dags", binding_dir=output / "bindings",
        n2_index_package_dir=args.n2_index_package_dir,
        coordinator_base_urls={
            "N7": args.coordinator_n7, "N8": args.coordinator_n8,
        },
        **binding_sources,
    )
    if (binding["route_count"] != dag["trial_count"]
            or dag["trial_count"] != admission["trial_count"]
            or admission["trial_count"] != 6 * plan["question_count"]
            or admission["index_query_plan_count"] != 0
            or admission["data_agent_plan_binding_count"]
            != admission["trial_count"]):
        raise ValueError("light-D canonical coverage differs")
    print(json.dumps({
        "status": "VERIFIED_LIGHT_D_RUNTIME_INPUTS",
        "plan_sha256": plan["plan_sha256"],
        "admission_sha256": admission["admission_sha256"],
        "trial_count": admission["trial_count"],
        "index_query_plan_count": admission["index_query_plan_count"],
        "data_agent_plan_binding_count": admission[
            "data_agent_plan_binding_count"],
        "credentials_recorded": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
