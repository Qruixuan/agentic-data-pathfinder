"""CLI wiring for the self-contained RSI-Exam offline replay task."""

from __future__ import annotations

import argparse
from pathlib import Path

from ._common import PayloadPrinter, add_compact


COMMAND_NAMES = frozenset({
    "audit-rsi-exam-trace-collection-candidates",
    "freeze-rsi-exam-trace-collection-plan",
    "verify-rsi-exam-trace-collection-plan",
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
