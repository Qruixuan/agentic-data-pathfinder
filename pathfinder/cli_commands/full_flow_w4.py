"""CLI wiring for the full-flow W4 retrieval command family.

Only parser and dispatch concerns live here. W4 planning, execution, scoring,
and FlowMesh implementations remain lazily imported from their owning modules.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Callable

from ..config import ConfigError
from ._common import (
    PayloadPrinter,
    add_compact as _add_compact,
    add_required_path as _add_required_path,
)


COMMAND_NAMES = frozenset({
    "freeze-simulator-full-flow-w4-retrieval-contract",
    "verify-simulator-full-flow-w4-retrieval-contract",
    "evaluate-simulator-full-flow-w4-retrieval",
    "verify-simulator-full-flow-w4-retrieval-evaluation",
    "freeze-simulator-full-flow-w4-retrieval-runtime",
    "verify-simulator-full-flow-w4-retrieval-runtime",
    "run-simulator-full-flow-w4-lexical-ranker",
    "verify-simulator-full-flow-w4-ranker-run",
    "freeze-simulator-full-flow-w4-candidate-routes",
    "verify-simulator-full-flow-w4-candidate-routes",
    "freeze-simulator-full-flow-w4-index-artifact-crosswalk",
    "verify-simulator-full-flow-w4-index-artifact-crosswalk",
    "run-simulator-full-flow-w4-candidate-conformance",
    "verify-simulator-full-flow-w4-candidate-conformance",
    "freeze-simulator-full-flow-w4-component-execution-receipt",
    "verify-simulator-full-flow-w4-component-execution-receipt",
    "run-simulator-full-flow-w4-local-component-execution",
    "freeze-simulator-full-flow-w4-flowmesh-plan",
    "verify-simulator-full-flow-w4-flowmesh-plan",
    "serve-simulator-full-flow-w4-flowmesh-coordinator",
    "run-simulator-full-flow-w4-flowmesh-matrix",
    "verify-simulator-full-flow-w4-flowmesh-matrix",
})


def register_full_flow_w4_commands(
    subcommands: argparse._SubParsersAction,
    *,
    positive_finite_float: Callable[[str], float],
) -> None:
    """Register the complete W4 retrieval and FlowMesh command family."""

    w4_contract_freeze = subcommands.add_parser(
        "freeze-simulator-full-flow-w4-retrieval-contract",
        help=(
            "freeze the public W4 ranking task and a separate N1-private "
            "relevance oracle"
        ),
    )
    _add_required_path(w4_contract_freeze, "--semantic-matrix-dir")
    _add_required_path(w4_contract_freeze, "--retrieval-config")
    _add_required_path(w4_contract_freeze, "--representation-manifest")
    w4_contract_freeze.add_argument("--selected-query-id", required=True)
    w4_contract_freeze.add_argument("--contract-id", required=True)
    _add_required_path(w4_contract_freeze, "--output-dir")
    _add_compact(w4_contract_freeze)

    w4_contract_verify = subcommands.add_parser(
        "verify-simulator-full-flow-w4-retrieval-contract",
        help="verify the split public/private W4 retrieval contract",
    )
    _add_required_path(w4_contract_verify, "--contract-dir")
    _add_compact(w4_contract_verify)

    w4_evaluate = subcommands.add_parser(
        "evaluate-simulator-full-flow-w4-retrieval",
        help="score complete W4 rankings without returning hidden relevance IDs",
    )
    _add_required_path(w4_evaluate, "--contract-dir")
    _add_required_path(w4_evaluate, "--observations")
    _add_required_path(w4_evaluate, "--output-dir")
    _add_compact(w4_evaluate)

    w4_evaluation_verify = subcommands.add_parser(
        "verify-simulator-full-flow-w4-retrieval-evaluation",
        help="verify W4 ranking metrics, optionally by source-bound replay",
    )
    _add_required_path(w4_evaluation_verify, "--output-dir")
    w4_evaluation_verify.add_argument("--contract-dir", type=Path)
    w4_evaluation_verify.add_argument("--observations", type=Path)
    _add_compact(w4_evaluation_verify)

    w4_runtime_freeze = subcommands.add_parser(
        "freeze-simulator-full-flow-w4-retrieval-runtime",
        help=(
            "bind the public W4 retrieval contract to all sixteen local "
            "design/repetition coordinates"
        ),
    )
    _add_required_path(w4_runtime_freeze, "--contract-dir")
    _add_required_path(w4_runtime_freeze, "--local-semantic-admission-dir")
    w4_runtime_freeze.add_argument("--runtime-overlay-id", required=True)
    _add_required_path(w4_runtime_freeze, "--output-dir")
    _add_compact(w4_runtime_freeze)

    w4_runtime_verify = subcommands.add_parser(
        "verify-simulator-full-flow-w4-retrieval-runtime",
        help="verify the public-only W4 ranker runtime package",
    )
    _add_required_path(w4_runtime_verify, "--output-dir")
    _add_compact(w4_runtime_verify)

    w4_ranker_run = subcommands.add_parser(
        "run-simulator-full-flow-w4-lexical-ranker",
        help=(
            "run the public W4 lexical ranker against bound N2/N7/N8 "
            "index services without reading N1 relevance labels"
        ),
    )
    _add_required_path(w4_ranker_run, "--runtime-overlay-dir")
    w4_ranker_run.add_argument("--n2-index-base-url", required=True)
    w4_ranker_run.add_argument("--n7-index-base-url", required=True)
    w4_ranker_run.add_argument("--n8-index-base-url", required=True)
    w4_ranker_run.add_argument(
        "--index-package-dir",
        type=Path,
        required=True,
        help=(
            "verified endpoint-free N2 index package used to pin the "
            "index ID, index digest, and source-manifest digest"
        ),
    )
    w4_ranker_run.add_argument("--run-id", required=True)
    w4_ranker_run.add_argument(
        "--allow-http-simulator-host", action="append", default=[]
    )
    w4_ranker_run.add_argument(
        "--timeout-seconds", type=positive_finite_float, default=10.0
    )
    _add_required_path(w4_ranker_run, "--output-dir")
    _add_compact(w4_ranker_run)

    w4_ranker_run_verify = subcommands.add_parser(
        "verify-simulator-full-flow-w4-ranker-run",
        help=(
            "verify public W4 ranking observations, optionally against "
            "their frozen runtime package"
        ),
    )
    _add_required_path(w4_ranker_run_verify, "--output-dir")
    w4_ranker_run_verify.add_argument(
        "--runtime-overlay-dir", type=Path
    )
    _add_compact(w4_ranker_run_verify)

    w4_candidate_routes_freeze = subcommands.add_parser(
        "freeze-simulator-full-flow-w4-candidate-routes",
        help="freeze candidate-wide D0-D7 W4 physical-route blueprints",
    )
    _add_required_path(w4_candidate_routes_freeze, "--runtime-overlay-dir")
    _add_required_path(w4_candidate_routes_freeze, "--n3-package-dir")
    _add_required_path(w4_candidate_routes_freeze, "--n4-package-dir")
    _add_required_path(w4_candidate_routes_freeze, "--index-package-dir")
    _add_required_path(w4_candidate_routes_freeze, "--exact-range-catalog-dir")
    w4_candidate_routes_freeze.add_argument(
        "--physical-plan-id", required=True
    )
    _add_required_path(w4_candidate_routes_freeze, "--output-dir")
    _add_compact(w4_candidate_routes_freeze)

    w4_candidate_routes_verify = subcommands.add_parser(
        "verify-simulator-full-flow-w4-candidate-routes",
        help="verify frozen candidate-wide W4 physical-route blueprints",
    )
    _add_required_path(w4_candidate_routes_verify, "--output-dir")
    _add_compact(w4_candidate_routes_verify)

    w4_crosswalk_freeze = subcommands.add_parser(
        "freeze-simulator-full-flow-w4-index-artifact-crosswalk",
        help=(
            "freeze the public N2 index-to-representation identity "
            "crosswalk"
        ),
    )
    _add_required_path(w4_crosswalk_freeze, "--route-package-dir")
    _add_required_path(w4_crosswalk_freeze, "--index-package-dir")
    _add_required_path(w4_crosswalk_freeze, "--output-dir")
    _add_compact(w4_crosswalk_freeze)

    w4_crosswalk_verify = subcommands.add_parser(
        "verify-simulator-full-flow-w4-index-artifact-crosswalk",
        help="verify the crosswalk by replaying its route and N2 sources",
    )
    _add_required_path(w4_crosswalk_verify, "--output-dir")
    _add_required_path(w4_crosswalk_verify, "--route-package-dir")
    _add_required_path(w4_crosswalk_verify, "--index-package-dir")
    _add_compact(w4_crosswalk_verify)

    w4_candidate_run = subcommands.add_parser(
        "run-simulator-full-flow-w4-candidate-conformance",
        help=(
            "execute all sixteen W4 candidate-wide route plans with the "
            "deterministic public conformance adapter; no LLM or FlowMesh"
        ),
    )
    _add_required_path(w4_candidate_run, "--route-package-dir")
    w4_candidate_run.add_argument("--run-id", required=True)
    _add_required_path(w4_candidate_run, "--output-dir")
    _add_compact(w4_candidate_run)

    w4_candidate_run_verify = subcommands.add_parser(
        "verify-simulator-full-flow-w4-candidate-conformance",
        help="verify the source-bound W4 candidate coordinator output",
    )
    _add_required_path(w4_candidate_run_verify, "--run-dir")
    _add_required_path(w4_candidate_run_verify, "--route-package-dir")
    _add_compact(w4_candidate_run_verify)

    w4_component_receipt_freeze = subcommands.add_parser(
        "freeze-simulator-full-flow-w4-component-execution-receipt",
        help=(
            "freeze strict persisted component events against a completed "
            "W4 coordinator run"
        ),
    )
    _add_required_path(w4_component_receipt_freeze, "--coordinator-run-dir")
    _add_required_path(w4_component_receipt_freeze, "--route-package-dir")
    _add_required_path(w4_component_receipt_freeze, "--crosswalk-dir")
    _add_required_path(w4_component_receipt_freeze, "--index-package-dir")
    _add_required_path(w4_component_receipt_freeze, "--component-events")
    w4_component_receipt_freeze.add_argument(
        "--evidence-class",
        choices=(
            "strict-fake-component-conformance",
            "live-local-component-execution",
        ),
        required=True,
    )
    _add_required_path(w4_component_receipt_freeze, "--output-dir")
    _add_compact(w4_component_receipt_freeze)

    w4_component_receipt_verify = subcommands.add_parser(
        "verify-simulator-full-flow-w4-component-execution-receipt",
        help="verify a component receipt against all of its frozen sources",
    )
    _add_required_path(w4_component_receipt_verify, "--output-dir")
    _add_required_path(w4_component_receipt_verify, "--coordinator-run-dir")
    _add_required_path(w4_component_receipt_verify, "--route-package-dir")
    _add_required_path(w4_component_receipt_verify, "--crosswalk-dir")
    _add_required_path(w4_component_receipt_verify, "--index-package-dir")
    _add_compact(w4_component_receipt_verify)

    w4_local_components_run = subcommands.add_parser(
        "run-simulator-full-flow-w4-local-component-execution",
        help=(
            "run all sixteen W4 trials through local index, Data Agent, "
            "cache, and N6 semantic components, then freeze their receipt"
        ),
    )
    _add_required_path(w4_local_components_run, "--route-package-dir")
    _add_required_path(w4_local_components_run, "--crosswalk-dir")
    for node in ("n2", "n7", "n8"):
        _add_required_path(w4_local_components_run, f"--{node}-index-package-dir")
        w4_local_components_run.add_argument(
            f"--{node}-index-base-url", required=True
        )
    w4_local_components_run.add_argument(
        "--n3-data-agent-base-url", required=True
    )
    w4_local_components_run.add_argument(
        "--n4-data-agent-base-url", required=True
    )
    w4_local_components_run.add_argument(
        "--n3-data-agent-location", default="origin-cold"
    )
    w4_local_components_run.add_argument(
        "--n4-data-agent-location", default="origin-warm"
    )
    for node in ("n7", "n8"):
        w4_local_components_run.add_argument(
            f"--{node}-cache-base-url", required=True
        )
        w4_local_components_run.add_argument(
            f"--{node}-cache-id", required=True
        )
    w4_local_components_run.add_argument(
        "--n6-base-url", required=True
    )
    w4_local_components_run.add_argument(
        "--semantic-model", required=True
    )
    _add_required_path(w4_local_components_run, "--raw-sampler-scratch-dir")
    w4_local_components_run.add_argument("--run-id", required=True)
    _add_required_path(w4_local_components_run, "--output-dir")
    w4_local_components_run.add_argument(
        "--timeout-seconds", type=positive_finite_float, default=300.0
    )
    w4_local_components_run.add_argument(
        "--simulator-private-http-hosts",
        default="",
        help="comma-separated simulator-private service names",
    )
    _add_compact(w4_local_components_run)

    w4_flowmesh_plan = subcommands.add_parser(
        "freeze-simulator-full-flow-w4-flowmesh-plan",
        help="freeze sixteen serial worker-pinned W4 coordinator API tasks",
    )
    _add_required_path(w4_flowmesh_plan, "--route-package-dir")
    w4_flowmesh_plan.add_argument("--run-id", required=True)
    w4_flowmesh_plan.add_argument("--worker-alias", required=True)
    w4_flowmesh_plan.add_argument("--owner", default="pathfinder")
    w4_flowmesh_plan.add_argument(
        "--api-task-timeout-seconds", type=int, default=900
    )
    _add_required_path(w4_flowmesh_plan, "--output-dir")
    _add_compact(w4_flowmesh_plan)

    w4_flowmesh_plan_verify = subcommands.add_parser(
        "verify-simulator-full-flow-w4-flowmesh-plan",
        help="recompile and verify a frozen W4 FlowMesh plan",
    )
    _add_required_path(w4_flowmesh_plan_verify, "--plan-dir")
    _add_required_path(w4_flowmesh_plan_verify, "--route-package-dir")
    _add_compact(w4_flowmesh_plan_verify)

    w4_flowmesh_serve = subcommands.add_parser(
        "serve-simulator-full-flow-w4-flowmesh-coordinator",
        help="serve one source-bound N7 or N8 W4 trial coordinator",
    )
    w4_flowmesh_serve.add_argument(
        "--coordinator-node-id", choices=("N7", "N8"), required=True
    )
    _add_required_path(w4_flowmesh_serve, "--route-package-dir")
    _add_required_path(w4_flowmesh_serve, "--crosswalk-dir")
    for node in ("n2", "n7", "n8"):
        _add_required_path(w4_flowmesh_serve, f"--{node}-index-package-dir")
        w4_flowmesh_serve.add_argument(
            f"--{node}-index-base-url", required=True
        )
    w4_flowmesh_serve.add_argument(
        "--n3-data-agent-base-url", required=True
    )
    w4_flowmesh_serve.add_argument(
        "--n4-data-agent-base-url", required=True
    )
    w4_flowmesh_serve.add_argument(
        "--n3-data-agent-location", default="origin-cold"
    )
    w4_flowmesh_serve.add_argument(
        "--n4-data-agent-location", default="origin-warm"
    )
    for node in ("n7", "n8"):
        w4_flowmesh_serve.add_argument(
            f"--{node}-cache-base-url", required=True
        )
        w4_flowmesh_serve.add_argument(f"--{node}-cache-id", required=True)
    w4_flowmesh_serve.add_argument("--n6-base-url", required=True)
    w4_flowmesh_serve.add_argument("--semantic-model", required=True)
    _add_required_path(w4_flowmesh_serve, "--raw-sampler-scratch-dir")
    _add_required_path(w4_flowmesh_serve, "--state-db")
    w4_flowmesh_serve.add_argument("--host", default="127.0.0.1")
    w4_flowmesh_serve.add_argument("--port", type=int, required=True)
    w4_flowmesh_serve.add_argument(
        "--timeout-seconds", type=positive_finite_float, default=300.0
    )
    w4_flowmesh_serve.add_argument(
        "--max-artifact-bytes", type=int, default=2 * 1024 * 1024 * 1024
    )
    w4_flowmesh_serve.add_argument(
        "--simulator-private-http-hosts", default=""
    )
    _add_compact(w4_flowmesh_serve)

    w4_flowmesh_run = subcommands.add_parser(
        "run-simulator-full-flow-w4-flowmesh-matrix",
        help="submit and freeze the sixteen-task W4 FlowMesh wrapper",
    )
    _add_required_path(w4_flowmesh_run, "--plan-dir")
    _add_required_path(w4_flowmesh_run, "--route-package-dir")
    _add_required_path(w4_flowmesh_run, "--crosswalk-dir")
    _add_required_path(w4_flowmesh_run, "--index-package-dir")
    w4_flowmesh_run.add_argument(
        "--n7-coordinator-base-url", required=True
    )
    w4_flowmesh_run.add_argument(
        "--n8-coordinator-base-url", required=True
    )
    w4_flowmesh_run.add_argument("--worker-alias", required=True)
    w4_flowmesh_run.add_argument("--flowmesh-base-url")
    w4_flowmesh_run.add_argument(
        "--poll-interval", type=positive_finite_float, default=2.0
    )
    w4_flowmesh_run.add_argument(
        "--simulator-private-http-hosts", default=""
    )
    _add_required_path(w4_flowmesh_run, "--output-dir")
    _add_compact(w4_flowmesh_run)

    w4_flowmesh_run_verify = subcommands.add_parser(
        "verify-simulator-full-flow-w4-flowmesh-matrix",
        help="verify W4 FlowMesh evidence against every frozen source",
    )
    _add_required_path(w4_flowmesh_run_verify, "--run-dir")
    _add_required_path(w4_flowmesh_run_verify, "--plan-dir")
    _add_required_path(w4_flowmesh_run_verify, "--route-package-dir")
    _add_required_path(w4_flowmesh_run_verify, "--crosswalk-dir")
    _add_required_path(w4_flowmesh_run_verify, "--index-package-dir")
    _add_compact(w4_flowmesh_run_verify)


def dispatch_full_flow_w4_command(
    args: argparse.Namespace,
    *,
    print_payload: PayloadPrinter,
) -> int | None:
    """Dispatch the W4 family, returning None when not handled."""

    if args.command not in COMMAND_NAMES:
        return None

    if (
        args.command
        == "freeze-simulator-full-flow-w4-retrieval-contract"
    ):
        from ..simulator.full_flow_w4_retrieval_contract import (
            freeze_full_flow_w4_retrieval_contract,
        )

        payload = freeze_full_flow_w4_retrieval_contract(
            args.semantic_matrix_dir,
            args.retrieval_config,
            args.representation_manifest,
            selected_query_id=args.selected_query_id,
            contract_id=args.contract_id,
            output_dir=args.output_dir,
        )
        return print_payload(payload, compact=args.compact)
    if (
        args.command
        == "verify-simulator-full-flow-w4-retrieval-contract"
    ):
        from ..simulator.full_flow_w4_retrieval_contract import (
            verify_full_flow_w4_retrieval_contract,
        )

        payload = verify_full_flow_w4_retrieval_contract(
            args.contract_dir
        )
        return print_payload(payload, compact=args.compact)
    if args.command == "evaluate-simulator-full-flow-w4-retrieval":
        from ..simulator.full_flow_w4_retrieval_contract import (
            evaluate_full_flow_w4_retrieval,
        )

        payload = evaluate_full_flow_w4_retrieval(
            args.contract_dir,
            args.observations,
            output_dir=args.output_dir,
        )
        return print_payload(payload, compact=args.compact)
    if (
        args.command
        == "verify-simulator-full-flow-w4-retrieval-evaluation"
    ):
        from ..simulator.full_flow_w4_retrieval_contract import (
            verify_full_flow_w4_retrieval_evaluation,
        )

        payload = verify_full_flow_w4_retrieval_evaluation(
            args.output_dir,
            contract_dir=args.contract_dir,
            observations_path=args.observations,
        )
        return print_payload(payload, compact=args.compact)
    if (
        args.command
        == "freeze-simulator-full-flow-w4-retrieval-runtime"
    ):
        from ..simulator.full_flow_w4_retrieval_runtime import (
            freeze_full_flow_w4_retrieval_runtime_overlay,
        )

        payload = freeze_full_flow_w4_retrieval_runtime_overlay(
            args.contract_dir,
            args.local_semantic_admission_dir,
            runtime_overlay_id=args.runtime_overlay_id,
            output_dir=args.output_dir,
        )
        return print_payload(payload, compact=args.compact)
    if (
        args.command
        == "verify-simulator-full-flow-w4-retrieval-runtime"
    ):
        from ..simulator.full_flow_w4_retrieval_runtime import (
            verify_full_flow_w4_retrieval_runtime_overlay,
        )

        payload = verify_full_flow_w4_retrieval_runtime_overlay(
            args.output_dir
        )
        return print_payload(payload, compact=args.compact)
    if args.command == "run-simulator-full-flow-w4-lexical-ranker":
        from ..simulator.full_flow_w4_retrieval_runtime import (
            W4LexicalIndexRankingExecutor,
            run_full_flow_w4_retrieval_ranker,
        )
        from ..simulator.index_service import (
            N2IndexHTTPClient,
            verify_n2_index_package,
        )

        token = os.environ.get("PATHFINDER_N2_INDEX_TOKEN")
        index = verify_n2_index_package(args.index_package_dir)
        private_hosts = tuple(args.allow_http_simulator_host)
        urls = {
            "N2": args.n2_index_base_url,
            "N7": args.n7_index_base_url,
            "N8": args.n8_index_base_url,
        }
        clients = {
            node_id: N2IndexHTTPClient(
                base_url=base_url,
                expected_index_id=index["index_id"],
                expected_index_sha256=index["index_sha256"],
                bearer_token=token,
                timeout_seconds=args.timeout_seconds,
                simulator_private_http_hosts=private_hosts,
                expected_node_id=node_id,
            )
            for node_id, base_url in urls.items()
        }
        executor = W4LexicalIndexRankingExecutor(
            clients=clients,
            index_package_dir=args.index_package_dir,
            index_id=index["index_id"],
            index_sha256=index["index_sha256"],
            source_manifest_sha256=index["source_manifest_sha256"],
        )
        payload = run_full_flow_w4_retrieval_ranker(
            args.runtime_overlay_dir,
            run_id=args.run_id,
            executor=executor,
            output_dir=args.output_dir,
        )
        return print_payload(payload, compact=args.compact)
    if args.command == "verify-simulator-full-flow-w4-ranker-run":
        from ..simulator.full_flow_w4_retrieval_runtime import (
            verify_full_flow_w4_retrieval_ranker_run,
        )

        payload = verify_full_flow_w4_retrieval_ranker_run(
            args.output_dir,
            runtime_overlay_dir=args.runtime_overlay_dir,
        )
        return print_payload(payload, compact=args.compact)
    if (
        args.command
        == "freeze-simulator-full-flow-w4-candidate-routes"
    ):
        from ..simulator.full_flow_w4_candidate_routes import (
            freeze_full_flow_w4_candidate_routes,
        )

        payload = freeze_full_flow_w4_candidate_routes(
            args.runtime_overlay_dir,
            args.n3_package_dir,
            args.n4_package_dir,
            args.index_package_dir,
            args.exact_range_catalog_dir,
            physical_plan_id=args.physical_plan_id,
            output_dir=args.output_dir,
        )
        return print_payload(payload, compact=args.compact)
    if (
        args.command
        == "verify-simulator-full-flow-w4-candidate-routes"
    ):
        from ..simulator.full_flow_w4_candidate_routes import (
            verify_full_flow_w4_candidate_routes,
        )

        payload = verify_full_flow_w4_candidate_routes(args.output_dir)
        return print_payload(payload, compact=args.compact)
    if (
        args.command
        == "freeze-simulator-full-flow-w4-index-artifact-crosswalk"
    ):
        from ..simulator.full_flow_w4_live_executor import (
            freeze_full_flow_w4_index_artifact_crosswalk,
        )

        payload = freeze_full_flow_w4_index_artifact_crosswalk(
            args.route_package_dir,
            args.index_package_dir,
            output_dir=args.output_dir,
        )
        return print_payload(payload, compact=args.compact)
    if (
        args.command
        == "verify-simulator-full-flow-w4-index-artifact-crosswalk"
    ):
        from ..simulator.full_flow_w4_live_executor import (
            verify_full_flow_w4_index_artifact_crosswalk,
        )

        payload = verify_full_flow_w4_index_artifact_crosswalk(
            args.output_dir,
            route_package_dir=args.route_package_dir,
            index_package_dir=args.index_package_dir,
        )
        return print_payload(payload, compact=args.compact)
    if (
        args.command
        == "run-simulator-full-flow-w4-candidate-conformance"
    ):
        from ..simulator.full_flow_w4_candidate_coordinator import (
            DeterministicW4CandidateOperationExecutor,
            run_full_flow_w4_candidate_coordinator,
        )

        payload = run_full_flow_w4_candidate_coordinator(
            args.route_package_dir,
            run_id=args.run_id,
            executor=DeterministicW4CandidateOperationExecutor(),
            output_dir=args.output_dir,
        )
        return print_payload(payload, compact=args.compact)
    if (
        args.command
        == "verify-simulator-full-flow-w4-candidate-conformance"
    ):
        from ..simulator.full_flow_w4_candidate_coordinator import (
            verify_full_flow_w4_candidate_coordinator_run,
        )

        payload = verify_full_flow_w4_candidate_coordinator_run(
            args.run_dir,
            route_package_dir=args.route_package_dir,
        )
        return print_payload(payload, compact=args.compact)
    if (
        args.command
        == "freeze-simulator-full-flow-w4-component-execution-receipt"
    ):
        from ..simulator.full_flow_w4_live_executor import (
            freeze_full_flow_w4_component_execution_receipt,
        )

        payload = freeze_full_flow_w4_component_execution_receipt(
            args.coordinator_run_dir,
            route_package_dir=args.route_package_dir,
            crosswalk_dir=args.crosswalk_dir,
            index_package_dir=args.index_package_dir,
            component_events_path=args.component_events,
            evidence_class=args.evidence_class,
            output_dir=args.output_dir,
        )
        return print_payload(payload, compact=args.compact)
    if (
        args.command
        == "verify-simulator-full-flow-w4-component-execution-receipt"
    ):
        from ..simulator.full_flow_w4_live_executor import (
            verify_full_flow_w4_component_execution_receipt,
        )

        payload = verify_full_flow_w4_component_execution_receipt(
            args.output_dir,
            coordinator_run_dir=args.coordinator_run_dir,
            route_package_dir=args.route_package_dir,
            crosswalk_dir=args.crosswalk_dir,
            index_package_dir=args.index_package_dir,
        )
        return print_payload(payload, compact=args.compact)
    if (
        args.command
        == "run-simulator-full-flow-w4-local-component-execution"
    ):
        from ..simulator.full_flow_w4_local_factory import (
            W4LocalRuntimeInputs,
        )
        from ..simulator.full_flow_w4_local_run import (
            run_full_flow_w4_local_component_execution,
        )

        credential_environment = {
            "N2 index": ("PATHFINDER_N2_INDEX_TOKEN",),
            "N7 index": (
                "PATHFINDER_N7_INDEX_TOKEN",
                "PATHFINDER_N2_INDEX_TOKEN",
            ),
            "N8 index": (
                "PATHFINDER_N8_INDEX_TOKEN",
                "PATHFINDER_N2_INDEX_TOKEN",
            ),
            "N3 Data Agent": (
                "PATHFINDER_N3_DATA_AGENT_TOKEN",
                "PATHFINDER_DATA_AGENT_TOKEN",
            ),
            "N4 Data Agent": (
                "PATHFINDER_N4_DATA_AGENT_TOKEN",
                "PATHFINDER_DATA_AGENT_TOKEN",
            ),
            "N7 cache": (
                "PATHFINDER_N7_FULL_FLOW_CACHE_TOKEN",
                "PATHFINDER_FULL_FLOW_CACHE_TOKEN",
            ),
            "N8 cache": (
                "PATHFINDER_N8_FULL_FLOW_CACHE_TOKEN",
                "PATHFINDER_FULL_FLOW_CACHE_TOKEN",
            ),
            "N6 semantic": ("PATHFINDER_CONTAINER_NODE_TOKEN",),
        }
        credentials = {
            name: next(
                (
                    os.environ[environment_name]
                    for environment_name in environment_names
                    if os.environ.get(environment_name)
                ),
                None,
            )
            for name, environment_names in credential_environment.items()
        }
        missing = [
            "/".join(credential_environment[name])
            for name, value in credentials.items()
            if not value
        ]
        if missing:
            raise ConfigError(
                "local W4 component execution requires runtime "
                "credential environment variables: "
                + ", ".join(sorted(missing))
            )
        private_hosts = tuple(
            value.strip()
            for value in args.simulator_private_http_hosts.split(",")
            if value.strip()
        )
        runtime = W4LocalRuntimeInputs(
            index_base_urls={
                "N2": args.n2_index_base_url,
                "N7": args.n7_index_base_url,
                "N8": args.n8_index_base_url,
            },
            index_bearer_tokens={
                "N2": credentials["N2 index"],
                "N7": credentials["N7 index"],
                "N8": credentials["N8 index"],
            },
            index_package_dirs={
                "N2": args.n2_index_package_dir,
                "N7": args.n7_index_package_dir,
                "N8": args.n8_index_package_dir,
            },
            data_agent_base_urls={
                "N3": args.n3_data_agent_base_url,
                "N4": args.n4_data_agent_base_url,
            },
            data_agent_bearer_tokens={
                "N3": credentials["N3 Data Agent"],
                "N4": credentials["N4 Data Agent"],
            },
            data_agent_locations={
                "N3": args.n3_data_agent_location,
                "N4": args.n4_data_agent_location,
            },
            cache_base_urls={
                "N7": args.n7_cache_base_url,
                "N8": args.n8_cache_base_url,
            },
            cache_bearer_tokens={
                "N7": credentials["N7 cache"],
                "N8": credentials["N8 cache"],
            },
            cache_ids={
                "N7": args.n7_cache_id,
                "N8": args.n8_cache_id,
            },
            n6_base_url=args.n6_base_url,
            n6_bearer_token=credentials["N6 semantic"],
            semantic_model=args.semantic_model,
            raw_sampler_scratch_dir=args.raw_sampler_scratch_dir,
            timeout_seconds=args.timeout_seconds,
            simulator_private_http_hosts=private_hosts,
        )
        payload = run_full_flow_w4_local_component_execution(
            route_package_dir=args.route_package_dir,
            crosswalk_dir=args.crosswalk_dir,
            runtime=runtime,
            run_id=args.run_id,
            output_dir=args.output_dir,
        )
        return print_payload(payload, compact=args.compact)
    if (
        args.command
        == "freeze-simulator-full-flow-w4-flowmesh-plan"
    ):
        from ..integrations.flowmesh.w4_candidate_matrix import (
            plan_flowmesh_w4_candidate_matrix,
        )

        payload = plan_flowmesh_w4_candidate_matrix(
            route_package_dir=args.route_package_dir,
            run_id=args.run_id,
            worker_alias=args.worker_alias,
            owner=args.owner,
            api_task_timeout_seconds=args.api_task_timeout_seconds,
            output_dir=args.output_dir,
        )
        return print_payload(payload, compact=args.compact)
    if (
        args.command
        == "verify-simulator-full-flow-w4-flowmesh-plan"
    ):
        from ..integrations.flowmesh.w4_candidate_matrix import (
            verify_flowmesh_w4_candidate_matrix_plan,
        )

        payload = verify_flowmesh_w4_candidate_matrix_plan(
            args.plan_dir,
            route_package_dir=args.route_package_dir,
        )
        return print_payload(payload, compact=args.compact)
    if (
        args.command
        == "serve-simulator-full-flow-w4-flowmesh-coordinator"
    ):
        from ..simulator.full_flow_w4_flowmesh_service import (
            build_local_full_flow_w4_flowmesh_coordinator,
            create_full_flow_w4_flowmesh_http_server,
        )
        from ..simulator.full_flow_w4_local_factory import (
            W4LocalRuntimeInputs,
        )

        credential_environment = {
            "N2 index": ("PATHFINDER_N2_INDEX_TOKEN",),
            "N7 index": (
                "PATHFINDER_N7_INDEX_TOKEN",
                "PATHFINDER_N2_INDEX_TOKEN",
            ),
            "N8 index": (
                "PATHFINDER_N8_INDEX_TOKEN",
                "PATHFINDER_N2_INDEX_TOKEN",
            ),
            "N3 Data Agent": (
                "PATHFINDER_N3_DATA_AGENT_TOKEN",
                "PATHFINDER_DATA_AGENT_TOKEN",
            ),
            "N4 Data Agent": (
                "PATHFINDER_N4_DATA_AGENT_TOKEN",
                "PATHFINDER_DATA_AGENT_TOKEN",
            ),
            "N7 cache": (
                "PATHFINDER_N7_W4_CACHE_TOKEN",
                "PATHFINDER_N7_FULL_FLOW_CACHE_TOKEN",
                "PATHFINDER_FULL_FLOW_CACHE_TOKEN",
            ),
            "N8 cache": (
                "PATHFINDER_N8_W4_CACHE_TOKEN",
                "PATHFINDER_N8_FULL_FLOW_CACHE_TOKEN",
                "PATHFINDER_FULL_FLOW_CACHE_TOKEN",
            ),
            "N6 semantic": ("PATHFINDER_CONTAINER_NODE_TOKEN",),
            "W4 ingress": (
                "PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET",
            ),
        }
        credentials = {
            name: next(
                (
                    os.environ[environment_name]
                    for environment_name in environment_names
                    if os.environ.get(environment_name)
                ),
                None,
            )
            for name, environment_names in credential_environment.items()
        }
        missing = [
            "/".join(credential_environment[name])
            for name, value in credentials.items()
            if not value
        ]
        if missing:
            raise ConfigError(
                "W4 FlowMesh coordinator requires runtime credential "
                "environment variables: " + ", ".join(sorted(missing))
            )
        private_hosts = tuple(
            value.strip()
            for value in args.simulator_private_http_hosts.split(",")
            if value.strip()
        )
        runtime = W4LocalRuntimeInputs(
            index_base_urls={
                "N2": args.n2_index_base_url,
                "N7": args.n7_index_base_url,
                "N8": args.n8_index_base_url,
            },
            index_bearer_tokens={
                "N2": credentials["N2 index"],
                "N7": credentials["N7 index"],
                "N8": credentials["N8 index"],
            },
            index_package_dirs={
                "N2": args.n2_index_package_dir,
                "N7": args.n7_index_package_dir,
                "N8": args.n8_index_package_dir,
            },
            data_agent_base_urls={
                "N3": args.n3_data_agent_base_url,
                "N4": args.n4_data_agent_base_url,
            },
            data_agent_bearer_tokens={
                "N3": credentials["N3 Data Agent"],
                "N4": credentials["N4 Data Agent"],
            },
            data_agent_locations={
                "N3": args.n3_data_agent_location,
                "N4": args.n4_data_agent_location,
            },
            cache_base_urls={
                "N7": args.n7_cache_base_url,
                "N8": args.n8_cache_base_url,
            },
            cache_bearer_tokens={
                "N7": credentials["N7 cache"],
                "N8": credentials["N8 cache"],
            },
            cache_ids={
                "N7": args.n7_cache_id,
                "N8": args.n8_cache_id,
            },
            n6_base_url=args.n6_base_url,
            n6_bearer_token=credentials["N6 semantic"],
            semantic_model=args.semantic_model,
            raw_sampler_scratch_dir=args.raw_sampler_scratch_dir,
            timeout_seconds=args.timeout_seconds,
            max_artifact_bytes=args.max_artifact_bytes,
            simulator_private_http_hosts=private_hosts,
        )
        coordinator = build_local_full_flow_w4_flowmesh_coordinator(
            coordinator_node_id=args.coordinator_node_id,
            route_package_dir=args.route_package_dir,
            crosswalk_dir=args.crosswalk_dir,
            runtime=runtime,
            state_db=args.state_db,
        )
        server = create_full_flow_w4_flowmesh_http_server(
            coordinator,
            host=args.host,
            port=args.port,
            hmac_secret=credentials["W4 ingress"],
        )
        print_payload(coordinator.health(), compact=args.compact)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            server.server_close()
        return 0
    if args.command == "run-simulator-full-flow-w4-flowmesh-matrix":
        from ..integrations.flowmesh import (
            FlowMeshSettings,
            SdkFlowMeshClient,
        )
        from ..integrations.flowmesh.w4_candidate_matrix import (
            full_flow_w4_hmac_header_provider,
            run_flowmesh_w4_candidate_matrix,
        )

        ingress_secret = os.environ.get(
            "PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET"
        )
        if not ingress_secret:
            raise ConfigError(
                "W4 FlowMesh run requires "
                "PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET"
            )
        private_hosts = tuple(
            value.strip()
            for value in args.simulator_private_http_hosts.split(",")
            if value.strip()
        )
        settings = FlowMeshSettings.from_environment(
            base_url=args.flowmesh_base_url,
            poll_interval_seconds=args.poll_interval,
            worker_alias=args.worker_alias,
            validate_before_submit=True,
        )
        client = SdkFlowMeshClient(settings)
        try:
            payload = run_flowmesh_w4_candidate_matrix(
                plan_dir=args.plan_dir,
                route_package_dir=args.route_package_dir,
                crosswalk_dir=args.crosswalk_dir,
                index_package_dir=args.index_package_dir,
                output_dir=args.output_dir,
                coordinator_base_urls={
                    "N7": args.n7_coordinator_base_url,
                    "N8": args.n8_coordinator_base_url,
                },
                runtime_header_provider=(
                    full_flow_w4_hmac_header_provider(ingress_secret)
                ),
                client=client,
                settings=settings,
                simulator_private_http_hosts=private_hosts,
            )
        finally:
            client.close()
        return print_payload(payload, compact=args.compact)
    if (
        args.command
        == "verify-simulator-full-flow-w4-flowmesh-matrix"
    ):
        from ..integrations.flowmesh.w4_candidate_matrix import (
            verify_flowmesh_w4_candidate_matrix_run,
        )

        payload = verify_flowmesh_w4_candidate_matrix_run(
            args.run_dir,
            plan_dir=args.plan_dir,
            route_package_dir=args.route_package_dir,
            crosswalk_dir=args.crosswalk_dir,
            index_package_dir=args.index_package_dir,
        )
        return print_payload(payload, compact=args.compact)
    return None
