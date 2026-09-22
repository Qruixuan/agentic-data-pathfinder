"""CLI wiring for the self-contained RSI-Exam offline replay task."""

from __future__ import annotations

import argparse
from pathlib import Path

from ._common import PayloadPrinter, add_compact


COMMAND_NAMES = frozenset({
    "audit-rsi-exam-trace-collection-candidates",
    "freeze-rsi-exam-trace-collection-plan",
    "verify-rsi-exam-trace-collection-plan",
    "prepare-rsi-exam-formal-temporal-index",
    "materialize-rsi-exam-formal-temporal-captions",
    "finalize-rsi-exam-formal-temporal-index",
    "verify-rsi-exam-formal-temporal-index-preparation",
    "verify-rsi-exam-formal-temporal-caption-package",
    "build-rsi-exam-offline-replay",
    "verify-rsi-exam-offline-replay",
    "run-rsi-exam-offline-replay",
    "compare-rsi-exam-offline-replay-baselines",
})


def _positive_integer(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return parsed


def register_rsi_exam_commands(
    subcommands: argparse._SubParsersAction,
) -> None:
    """Register package, verification, and exact-replay commands."""

    audit = subcommands.add_parser(
        "audit-rsi-exam-trace-collection-candidates",
        help="audit public candidates without reading task outcomes",
    )
    audit.add_argument("--public-task-set", type=Path, required=True)
    audit.add_argument("--cohort-spec", type=Path, required=True)
    add_compact(audit)

    freeze = subcommands.add_parser(
        "freeze-rsi-exam-trace-collection-plan",
        help="freeze an outcome-blind, video-disjoint trace collection plan",
    )
    freeze.add_argument("--public-task-set", type=Path, required=True)
    freeze.add_argument("--cohort-spec", type=Path, required=True)
    freeze.add_argument("--builder-commit", required=True)
    freeze.add_argument("--output-dir", type=Path, required=True)
    add_compact(freeze)

    verify_plan = subcommands.add_parser(
        "verify-rsi-exam-trace-collection-plan",
        help="verify a frozen trace collection plan",
    )
    verify_plan.add_argument("--plan-dir", type=Path, required=True)
    verify_plan.add_argument("--public-task-set", type=Path)
    verify_plan.add_argument("--cohort-spec", type=Path)
    verify_plan.add_argument("--builder-commit")
    add_compact(verify_plan)

    prepare_index = subcommands.add_parser(
        "prepare-rsi-exam-formal-temporal-index",
        help="freeze question-independent caption frames and temporal windows",
    )
    prepare_index.add_argument("--collection-plan-dir", type=Path, required=True)
    prepare_index.add_argument("--n3-raw-package-dir", type=Path, required=True)
    prepare_index.add_argument("--package-id", required=True)
    prepare_index.add_argument("--caption-frame-count", type=_positive_integer,
                               default=24)
    prepare_index.add_argument("--jpeg-max-dimension", type=_positive_integer,
                               default=768)
    prepare_index.add_argument("--output-dir", type=Path, required=True)
    add_compact(prepare_index)

    verify_preparation = subcommands.add_parser(
        "verify-rsi-exam-formal-temporal-index-preparation",
        help="verify frozen formal temporal-index frames and windows",
    )
    verify_preparation.add_argument("--preparation-dir", type=Path, required=True)
    add_compact(verify_preparation)

    caption = subcommands.add_parser(
        "materialize-rsi-exam-formal-temporal-captions",
        help="materialize durable question-independent captions",
    )
    caption.add_argument("--preparation-dir", type=Path, required=True)
    caption.add_argument("--cache-dir", type=Path, required=True)
    caption.add_argument("--package-id", required=True)
    caption.add_argument("--model-id", required=True)
    caption.add_argument("--max-attempts-per-window", type=_positive_integer,
                         default=2)
    caption.add_argument("--parallelism", type=_positive_integer, default=1)
    caption.add_argument("--timeout-seconds", type=float, default=180.0)
    caption.add_argument("--output-dir", type=Path, required=True)
    add_compact(caption)

    verify_caption = subcommands.add_parser(
        "verify-rsi-exam-formal-temporal-caption-package",
        help="verify formal question-independent captions",
    )
    verify_caption.add_argument("--caption-dir", type=Path, required=True)
    verify_caption.add_argument("--preparation-dir", type=Path, required=True)
    add_compact(verify_caption)

    finalize_index = subcommands.add_parser(
        "finalize-rsi-exam-formal-temporal-index",
        help="embed captions and public anchors, then freeze per-object N3 policies",
    )
    finalize_index.add_argument("--preparation-dir", type=Path, required=True)
    finalize_index.add_argument("--caption-dir", type=Path, required=True)
    finalize_index.add_argument("--collection-plan-dir", type=Path, required=True)
    finalize_index.add_argument("--public-task-set", type=Path, required=True)
    finalize_index.add_argument("--n3-raw-package-dir", type=Path, required=True)
    finalize_index.add_argument("--package-id", required=True)
    finalize_index.add_argument("--n3-package-id", required=True)
    finalize_index.add_argument("--embedding-model-id", required=True)
    finalize_index.add_argument("--embedding-dimension", type=_positive_integer,
                                default=1024)
    finalize_index.add_argument("--embedding-batch-size", type=_positive_integer,
                                default=64)
    finalize_index.add_argument("--runtime-frame-count", type=_positive_integer,
                                default=10)
    finalize_index.add_argument("--jpeg-max-dimension", type=_positive_integer,
                                default=768)
    finalize_index.add_argument("--timeout-seconds", type=float, default=180.0)
    finalize_index.add_argument("--n3-output-dir", type=Path, required=True)
    finalize_index.add_argument("--runtime-frame-manifest-dir", type=Path,
                                required=True)
    finalize_index.add_argument("--output-dir", type=Path, required=True)
    add_compact(finalize_index)

    build = subcommands.add_parser(
        "build-rsi-exam-offline-replay",
        help="freeze verified public accounting into an offline replay package",
    )
    build.add_argument(
        "--accounting-dir",
        type=Path,
        action="append",
        required=True,
        help="repeat once per checksum-verified case accounting directory",
    )
    build.add_argument("--source-commit", required=True)
    build.add_argument("--builder-commit", required=True)
    build.add_argument("--package-id", required=True)
    build.add_argument("--split-manifest", type=Path)
    build.add_argument("--output-dir", type=Path, required=True)
    add_compact(build)

    verify = subcommands.add_parser(
        "verify-rsi-exam-offline-replay",
        help="verify an offline replay package and optional source accounting",
    )
    verify.add_argument("--package-dir", type=Path, required=True)
    verify.add_argument(
        "--accounting-dir",
        type=Path,
        action="append",
        default=[],
    )
    add_compact(verify)

    run = subcommands.add_parser(
        "run-rsi-exam-offline-replay",
        help="run one built-in policy without external services",
    )
    run.add_argument("--package-dir", type=Path, required=True)
    run.add_argument(
        "--policy",
        choices=(
            "always-direct-video",
            "always-indexed",
            "always-derived",
            "myopic-cost-first",
            "amortization-aware",
            "random-seeded",
        ),
        required=True,
    )
    run.add_argument(
        "--mode",
        choices=("independent-query", "shared-dataset-sequence"),
        required=True,
    )
    run.add_argument("--queries", type=_positive_integer, default=1)
    run.add_argument("--seed", type=int, default=0)
    run.add_argument("--case-id")
    add_compact(run)

    compare = subcommands.add_parser(
        "compare-rsi-exam-offline-replay-baselines",
        help="compare all frozen offline replay baselines",
    )
    compare.add_argument("--package-dir", type=Path, required=True)
    compare.add_argument(
        "--mode",
        choices=("independent-query", "shared-dataset-sequence"),
        required=True,
    )
    compare.add_argument("--queries", type=_positive_integer, default=1)
    compare.add_argument("--seed", type=int, default=0)
    compare.add_argument("--case-id")
    add_compact(compare)


def dispatch_rsi_exam_command(
    args: argparse.Namespace,
    *,
    print_payload: PayloadPrinter,
) -> int | None:
    """Dispatch an RSI-Exam command, returning None when not handled."""

    if args.command not in COMMAND_NAMES:
        return None
    if args.command == "audit-rsi-exam-trace-collection-candidates":
        from ..rsi_exam.collection_plan import audit_collection_candidates

        payload = audit_collection_candidates(
            args.public_task_set,
            args.cohort_spec,
        )
        printed = print_payload(payload, compact=args.compact)
        if payload["status"] != "READY_FOR_OUTCOME_BLIND_SELECTION":
            return 2
        return printed
    if args.command == "freeze-rsi-exam-trace-collection-plan":
        from ..rsi_exam.collection_plan import freeze_collection_plan

        payload = freeze_collection_plan(
            args.public_task_set,
            args.cohort_spec,
            builder_commit=args.builder_commit,
            output_dir=args.output_dir,
        )
        return print_payload(payload, compact=args.compact)
    if args.command == "verify-rsi-exam-trace-collection-plan":
        from ..rsi_exam.collection_plan import verify_collection_plan

        payload = verify_collection_plan(
            args.plan_dir,
            public_task_set=args.public_task_set,
            cohort_spec=args.cohort_spec,
            builder_commit=args.builder_commit,
        )
        return print_payload(payload, compact=args.compact)
    if args.command == "prepare-rsi-exam-formal-temporal-index":
        from ..rsi_exam.temporal_index_collection import (
            prepare_formal_temporal_index,
        )

        payload = prepare_formal_temporal_index(
            args.collection_plan_dir,
            args.n3_raw_package_dir,
            output_dir=args.output_dir,
            package_id=args.package_id,
            caption_frame_count=args.caption_frame_count,
            jpeg_max_dimension=args.jpeg_max_dimension,
        )
        return print_payload(payload, compact=args.compact)
    if args.command == "verify-rsi-exam-formal-temporal-index-preparation":
        from ..rsi_exam.temporal_index_collection import (
            verify_formal_temporal_index_preparation,
        )

        payload = verify_formal_temporal_index_preparation(args.preparation_dir)
        return print_payload(payload, compact=args.compact)
    if args.command == "materialize-rsi-exam-formal-temporal-captions":
        import os

        from ..rsi_exam.temporal_index_collection import (
            materialize_formal_temporal_captions,
        )

        payload = materialize_formal_temporal_captions(
            args.preparation_dir,
            output_dir=args.output_dir,
            cache_dir=args.cache_dir,
            package_id=args.package_id,
            model_id=args.model_id,
            base_url=os.environ.get("PATHFINDER_PREP_LLM_BASE_URL", ""),
            api_key=os.environ.get("PATHFINDER_PREP_LLM_API_KEY", ""),
            max_attempts_per_window=args.max_attempts_per_window,
            parallelism=args.parallelism,
            timeout_seconds=args.timeout_seconds,
        )
        return print_payload(payload, compact=args.compact)
    if args.command == "verify-rsi-exam-formal-temporal-caption-package":
        from ..rsi_exam.temporal_index_collection import (
            verify_formal_temporal_caption_package,
        )

        payload = verify_formal_temporal_caption_package(
            args.caption_dir, args.preparation_dir
        )
        return print_payload(payload, compact=args.compact)
    if args.command == "finalize-rsi-exam-formal-temporal-index":
        import os

        from ..rsi_exam.temporal_index_collection import (
            finalize_formal_temporal_index,
        )

        payload = finalize_formal_temporal_index(
            args.preparation_dir,
            args.caption_dir,
            args.collection_plan_dir,
            args.public_task_set,
            args.n3_raw_package_dir,
            output_dir=args.output_dir,
            n3_output_dir=args.n3_output_dir,
            runtime_frame_manifest_dir=args.runtime_frame_manifest_dir,
            package_id=args.package_id,
            n3_package_id=args.n3_package_id,
            embedding_model_id=args.embedding_model_id,
            base_url=os.environ.get("PATHFINDER_PREP_LLM_BASE_URL", ""),
            api_key=os.environ.get("PATHFINDER_PREP_LLM_API_KEY", ""),
            dimension=args.embedding_dimension,
            batch_size=args.embedding_batch_size,
            runtime_frame_count=args.runtime_frame_count,
            jpeg_max_dimension=args.jpeg_max_dimension,
            timeout_seconds=args.timeout_seconds,
        )
        return print_payload(payload, compact=args.compact)
    if args.command == "build-rsi-exam-offline-replay":
        from ..rsi_exam.offline_replay import build_offline_replay_package

        payload = build_offline_replay_package(
            args.accounting_dir,
            output_dir=args.output_dir,
            source_commit=args.source_commit,
            builder_commit=args.builder_commit,
            package_id=args.package_id,
            split_manifest=args.split_manifest,
        )
        return print_payload(payload, compact=args.compact)
    if args.command == "verify-rsi-exam-offline-replay":
        from ..rsi_exam.offline_replay import verify_offline_replay_package

        payload = verify_offline_replay_package(
            args.package_dir,
            source_accounting_dirs=args.accounting_dir,
        )
        return print_payload(payload, compact=args.compact)
    if args.command == "run-rsi-exam-offline-replay":
        from ..rsi_exam.offline_replay import run_offline_replay_policy

        payload = run_offline_replay_policy(
            args.package_dir,
            policy_name=args.policy,
            mode=args.mode,
            query_count=args.queries,
            seed=args.seed,
            case_id=args.case_id,
        )
        return print_payload(payload, compact=args.compact)

    from ..rsi_exam.offline_replay import compare_offline_replay_baselines

    payload = compare_offline_replay_baselines(
        args.package_dir,
        mode=args.mode,
        query_count=args.queries,
        seed=args.seed,
        case_id=args.case_id,
    )
    return print_payload(payload, compact=args.compact)
