"""Thin adapters for historical pilot entry points; no duplicate run loop."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from experiments import interleaved_batch as batch


_PRESETS = {24: "dev-24.draft.json", 28: "sealed-28.draft.json"}
_COUNTS = {
    24: {"stage_count": 276, "data_agent_plan_binding_count": 42,
         "cache_episode_binding_count": 6},
    28: {"stage_count": 322, "data_agent_plan_binding_count": 49,
         "cache_episode_binding_count": 7},
}


def historical_inputs(
    count: int, artifact_root: Path, baseline_spec_dir: Path | None = None,
) -> tuple[dict, dict]:
    """Retain the original cohort gates for offline consumers."""
    config = batch._read(Path(__file__).parent / "configs" / _PRESETS[count])
    # Legacy callers may keep the baseline outside the main artifact root.
    # It is still verified against both the admission and plan below.
    config["baseline_spec_dir"] = None
    batch._validate_config(config)
    context = batch.load_inputs(config, artifact_root)
    if any(context["report"].get(key) != value
           for key, value in _COUNTS[count].items()):
        raise ValueError("historical admission pre-submit counts differ")
    if count == 28:
        if baseline_spec_dir is None:
            raise ValueError("sealed cohort requires the frozen baseline")
        baseline = baseline_spec_dir.resolve()
        context["baseline_sha256"] = batch._baseline(
            baseline.parent, baseline.name,
            context["report"]["admission_sha256"],
            context["plan"]["plan_sha256"],
        )
    return config, context


def verify_legacy(
    count: int, artifact_root: Path, output_dir: Path, *,
    baseline_spec_dir: Path | None = None, seal: bool = False,
) -> dict:
    config, context = historical_inputs(count, artifact_root, baseline_spec_dir)
    if batch._read(output_dir / "summary.json").get("status") != (
        f"VERIFIED_{count}_ROUTE_EXECUTION"
    ):
        raise ValueError("use experiments.batch verify for non-legacy output")
    result = batch.verify_output(config, "", context, output_dir, seal=seal)
    # Keep cost-audit/replay callers and their historical status contract intact.
    result.pop("config_sha256")
    if count == 24:
        result.pop("question_count")
        result.pop("baseline_spec_sha256")
    return {**result, "status": f"VERIFIED_{count}_ROUTE_OUTPUT"}


def verifier_main(count: int, argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--seal", action="store_true")
    if count == 28:
        parser.add_argument("--baseline-spec-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    print(json.dumps(verify_legacy(
        count, args.artifact_root, args.output_dir,
        baseline_spec_dir=getattr(args, "baseline_spec_dir", None),
        seal=args.seal,
    ), sort_keys=True))


def runner_main(count: int, argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Legacy entry point; prefer python -m experiments.batch",
    )
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--config-dir", type=Path)
    parser.add_argument("--output-dir", type=Path)
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument("--preflight", action="store_true")
    actions.add_argument("--execute", action="store_true")
    if count == 28:
        parser.add_argument("--baseline-spec-dir", type=Path)
    args = parser.parse_args(argv)
    if args.execute and args.config_dir is None:
        parser.error("--execute requires --config-dir; do not reuse pilot run IDs")
    if args.config_dir is not None:
        if getattr(args, "baseline_spec_dir", None) is not None:
            parser.error("with --config-dir, baseline must be bound in the config")
        config, digest = batch.load_config(args.config_dir)
        if config["schema_version"] != batch.SCHEMA or (
            config["expected_route_count"] != count
        ):
            parser.error("configuration belongs to another route family/count")
        context = batch.load_inputs(config, args.artifact_root)
    else:
        config, context = historical_inputs(
            count, args.artifact_root,
            getattr(args, "baseline_spec_dir", None),
        )
        digest = ""
    if args.preflight or args.execute:
        result = batch.run(
            config, digest, context, args.output_dir, execute=args.execute,
        )
    else:
        result = {
            "status": "SOURCE_BOUND_INPUTS_VERIFIED",
            "admission_sha256": context["report"]["admission_sha256"],
            "route_count": len(context["trials"]),
        }
    print(json.dumps(result, sort_keys=True))
    return 2 if result["status"] == "STOPPED_AT_FIRST_FAILURE" else 0
