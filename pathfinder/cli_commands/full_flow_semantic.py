"""CLI wiring for full-flow semantic execution and local validation.

The semantic implementations remain in their owning simulator and FlowMesh
modules. Imports stay lazy, and runtime-only credentials are never captured by
the parser layer.
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
    resolve_full_flow_credentials as _resolve_full_flow_credentials,
)


COMMAND_NAMES = frozenset({
    "freeze-simulator-full-flow-semantic-execution-admission",
    "verify-simulator-full-flow-semantic-execution-admission",
    "preflight-simulator-full-flow-semantic-artifacts",
    "verify-simulator-full-flow-semantic-artifact-preflight",
    "promote-simulator-full-flow-local-semantic-execution-admission",
    "verify-simulator-full-flow-local-semantic-execution-admission",
    "verify-simulator-full-flow-local-semantic-runtime-package",
    "build-simulator-full-flow-index-query-plan-catalog",
    "verify-simulator-full-flow-index-query-plan-catalog",
    "freeze-simulator-full-flow-n4-preprovisioned-serve-gate",
    "verify-simulator-full-flow-n4-preprovisioned-serve-gate",
    "freeze-simulator-full-flow-n4-live-serve-gate",
    "verify-simulator-full-flow-n4-live-serve-gate",
    "run-simulator-full-flow-local-semantic-smokes",
    "verify-simulator-full-flow-local-semantic-smokes",
    "run-simulator-full-flow-local-semantic-matrix",
    "verify-simulator-full-flow-local-semantic-matrix",
    "serve-simulator-full-flow-semantic-route",
})


def register_full_flow_semantic_commands(
    subcommands: argparse._SubParsersAction,
    *,
    positive_finite_float: Callable[[str], float],
) -> None:
    """Register semantic admission, execution, and route-service commands."""

    semantic_admission_freeze = subcommands.add_parser(
        "freeze-simulator-full-flow-semantic-execution-admission",
        help=(
            "bind semantic routes to a deployment and report exact runtime "
            "admission gaps"
        ),
    )
    _add_required_path(semantic_admission_freeze, "--semantic-matrix-dir")
    _add_required_path(semantic_admission_freeze, "--deployment-binding-dir")
    _add_required_path(semantic_admission_freeze, "--logical-plan-dir")
    _add_required_path(semantic_admission_freeze, "--scenario")
    _add_required_path(semantic_admission_freeze, "--container-plan-dir")
    _add_required_path(semantic_admission_freeze, "--public-task-set")
    _add_required_path(semantic_admission_freeze, "--artifact-bindings")
    _add_required_path(semantic_admission_freeze, "--n1-oracle-package-dir")
    semantic_admission_freeze.add_argument("--worker-alias", required=True)
    semantic_admission_freeze.add_argument(
        "--admission-id",
        default="full-flow-semantic-execution-admission-v1",
    )
    _add_required_path(semantic_admission_freeze, "--output-dir")
    _add_compact(semantic_admission_freeze)

    semantic_admission_verify = subcommands.add_parser(
        "verify-simulator-full-flow-semantic-execution-admission",
        help="verify the source-bound semantic execution admission package",
    )
    _add_required_path(semantic_admission_verify, "--admission-dir")
    _add_required_path(semantic_admission_verify, "--semantic-matrix-dir")
    _add_required_path(semantic_admission_verify, "--deployment-binding-dir")
    _add_required_path(semantic_admission_verify, "--logical-plan-dir")
    _add_required_path(semantic_admission_verify, "--scenario")
    _add_required_path(semantic_admission_verify, "--container-plan-dir")
    _add_required_path(semantic_admission_verify, "--public-task-set")
    _add_required_path(semantic_admission_verify, "--artifact-bindings")
    _add_required_path(semantic_admission_verify, "--n1-oracle-package-dir")
    _add_compact(semantic_admission_verify)

    artifact_preflight = subcommands.add_parser(
        "preflight-simulator-full-flow-semantic-artifacts",
        help=(
            "authenticate to N3/N4 and fully fetch every frozen semantic "
            "artifact exactly once"
        ),
    )
    _add_required_path(artifact_preflight, "--semantic-execution-admission-dir")
    _add_required_path(artifact_preflight, "--n3-package-dir")
    _add_required_path(artifact_preflight, "--n4-package-dir")
    artifact_preflight.add_argument("--n3-data-agent-url", required=True)
    artifact_preflight.add_argument("--n4-data-agent-url", required=True)
    artifact_preflight.add_argument("--preflight-id", required=True)
    artifact_preflight.add_argument(
        "--timeout-seconds", type=float, default=30.0
    )
    artifact_preflight.add_argument("--max-retries", type=int, default=1)
    artifact_preflight.add_argument(
        "--max-artifact-bytes",
        type=int,
        default=64 * 1024 * 1024 * 1024,
    )
    artifact_preflight.add_argument(
        "--telemetry-quiescence-timeout-seconds",
        type=float,
        default=5.0,
    )
    artifact_preflight.add_argument(
        "--simulator-private-http-host",
        action="append",
        default=[],
    )
    _add_required_path(artifact_preflight, "--output-dir")
    _add_compact(artifact_preflight)

    artifact_preflight_verify = subcommands.add_parser(
        "verify-simulator-full-flow-semantic-artifact-preflight",
        help=(
            "verify semantic artifact evidence against admission and N3/N4 "
            "packages"
        ),
    )
    _add_required_path(artifact_preflight_verify, "--preflight-dir")
    _add_required_path(artifact_preflight_verify, "--semantic-execution-admission-dir")
    _add_required_path(artifact_preflight_verify, "--n3-package-dir")
    _add_required_path(artifact_preflight_verify, "--n4-package-dir")
    _add_compact(artifact_preflight_verify)

    local_semantic_promote = subcommands.add_parser(
        "promote-simulator-full-flow-local-semantic-execution-admission",
        help=(
            "promote the immutable blocked admission into public-only local "
            "semantic execution inputs"
        ),
    )
    _add_required_path(local_semantic_promote, "--legacy-admission-dir")
    _add_required_path(local_semantic_promote, "--semantic-matrix-dir")
    _add_required_path(local_semantic_promote, "--deployment-binding-dir")
    _add_required_path(local_semantic_promote, "--logical-plan-dir")
    _add_required_path(local_semantic_promote, "--scenario")
    _add_required_path(local_semantic_promote, "--container-plan-dir")
    _add_required_path(local_semantic_promote, "--public-task-set")
    _add_required_path(local_semantic_promote, "--artifact-bindings")
    _add_required_path(local_semantic_promote, "--n1-oracle-package-dir")
    _add_required_path(local_semantic_promote, "--artifact-preflight-dir")
    _add_required_path(local_semantic_promote, "--exact-range-catalog-dir")
    _add_required_path(local_semantic_promote, "--n3-package-dir")
    _add_required_path(local_semantic_promote, "--provisioning-catalog-dir")
    _add_required_path(local_semantic_promote, "--n4-package-dir")
    local_semantic_promote.add_argument("--semantics-mode", required=True)
    local_semantic_promote.add_argument("--promotion-id", required=True)
    _add_required_path(local_semantic_promote, "--output-dir")
    _add_compact(local_semantic_promote)

    local_semantic_verify = subcommands.add_parser(
        "verify-simulator-full-flow-local-semantic-execution-admission",
        help="verify promoted local semantic inputs against every source",
    )
    _add_required_path(local_semantic_verify, "--admission-dir")
    _add_required_path(local_semantic_verify, "--legacy-admission-dir")
    _add_required_path(local_semantic_verify, "--semantic-matrix-dir")
    _add_required_path(local_semantic_verify, "--deployment-binding-dir")
    _add_required_path(local_semantic_verify, "--logical-plan-dir")
    _add_required_path(local_semantic_verify, "--scenario")
    _add_required_path(local_semantic_verify, "--container-plan-dir")
    _add_required_path(local_semantic_verify, "--public-task-set")
    _add_required_path(local_semantic_verify, "--artifact-bindings")
    _add_required_path(local_semantic_verify, "--n1-oracle-package-dir")
    _add_required_path(local_semantic_verify, "--artifact-preflight-dir")
    _add_required_path(local_semantic_verify, "--exact-range-catalog-dir")
    _add_required_path(local_semantic_verify, "--n3-package-dir")
    _add_required_path(local_semantic_verify, "--provisioning-catalog-dir")
    _add_required_path(local_semantic_verify, "--n4-package-dir")
    _add_compact(local_semantic_verify)

    local_semantic_runtime_verify = subcommands.add_parser(
        "verify-simulator-full-flow-local-semantic-runtime-package",
        help="verify the self-contained public local runtime package",
    )
    _add_required_path(local_semantic_runtime_verify, "--admission-dir")
    _add_compact(local_semantic_runtime_verify)

    index_query_plan_build = subcommands.add_parser(
        "build-simulator-full-flow-index-query-plan-catalog",
        help="freeze visible N2 query plans for indexed semantic trials",
    )
    _add_required_path(index_query_plan_build, "--local-semantic-admission-dir")
    _add_required_path(index_query_plan_build, "--n2-index-package-dir")
    _add_required_path(index_query_plan_build, "--output-dir")
    _add_compact(index_query_plan_build)

    index_query_plan_verify = subcommands.add_parser(
        "verify-simulator-full-flow-index-query-plan-catalog",
        help="verify visible N2 query plans against their frozen sources",
    )
    _add_required_path(index_query_plan_verify, "--catalog-dir")
    _add_required_path(index_query_plan_verify, "--local-semantic-admission-dir")
    _add_required_path(index_query_plan_verify, "--n2-index-package-dir")
    _add_compact(index_query_plan_verify)

    n4_serve_gate_freeze = subcommands.add_parser(
        "freeze-simulator-full-flow-n4-preprovisioned-serve-gate",
        help=(
            "freeze the source-bound authorization for serving an immutable "
            "preprovisioned N4 snapshot"
        ),
    )
    for flag in (
        "compose-overlay-dir",
        "service-bootstrap-dir",
        "deployment-binding-dir",
        "logical-plan-dir",
        "scenario",
        "container-plan-dir",
        "provisioning-catalog-dir",
        "artifact-binding-dir",
        "n4-package-dir",
    ):
        n4_serve_gate_freeze.add_argument(
            "--" + flag, type=Path, required=True
        )
    n4_serve_gate_freeze.add_argument("--gate-id", required=True)
    _add_required_path(n4_serve_gate_freeze, "--output-dir")
    _add_compact(n4_serve_gate_freeze)

    n4_serve_gate_verify = subcommands.add_parser(
        "verify-simulator-full-flow-n4-preprovisioned-serve-gate",
        help="verify the N4 serve authorization against every frozen source",
    )
    _add_required_path(n4_serve_gate_verify, "--gate-dir")
    for flag in (
        "compose-overlay-dir",
        "service-bootstrap-dir",
        "deployment-binding-dir",
        "logical-plan-dir",
        "scenario",
        "container-plan-dir",
        "provisioning-catalog-dir",
        "artifact-binding-dir",
        "n4-package-dir",
    ):
        n4_serve_gate_verify.add_argument(
            "--" + flag, type=Path, required=True
        )
    _add_compact(n4_serve_gate_verify)

    def add_n4_live_gate_sources(command: argparse.ArgumentParser) -> None:
        command.add_argument(
            "--live-receipt-bindings",
            type=Path,
            required=True,
            help=(
                "operator-local JSON array of frame/digest receipt source "
                "bindings; paths are used for verification and not frozen"
            ),
        )
        _add_required_path(command, "--n4-publication-store-root")
        _add_required_path(command, "--rebound-artifact-binding-dir")
        _add_required_path(command, "--rebound-semantic-matrix-dir")
        _add_required_path(command, "--rebound-admission-dir")

    n4_live_gate_freeze = subcommands.add_parser(
        "freeze-simulator-full-flow-n4-live-serve-gate",
        help=(
            "freeze serve-frozen authorization after live N5-to-N4 "
            "publication and downstream input rebinding"
        ),
    )
    add_n4_live_gate_sources(n4_live_gate_freeze)
    n4_live_gate_freeze.add_argument("--gate-id", required=True)
    _add_required_path(n4_live_gate_freeze, "--output-dir")
    _add_compact(n4_live_gate_freeze)

    n4_live_gate_verify = subcommands.add_parser(
        "verify-simulator-full-flow-n4-live-serve-gate",
        help="verify live publication, immutable N4 state, and rebound inputs",
    )
    _add_required_path(n4_live_gate_verify, "--gate-dir")
    add_n4_live_gate_sources(n4_live_gate_verify)
    _add_compact(n4_live_gate_verify)

    def add_n4_serve_gate_sources(
        command: argparse.ArgumentParser,
        *,
        include_shared_sources: bool,
    ) -> None:
        _add_required_path(command, "--n4-serve-gate-dir")
        _add_required_path(command, "--compose-overlay-dir")
        _add_required_path(command, "--service-bootstrap-dir")
        _add_required_path(command, "--provisioning-catalog-dir")
        _add_required_path(command, "--artifact-binding-dir")
        _add_required_path(command, "--n4-package-dir")
        command.add_argument(
            "--n4-live-gate-sources",
            type=Path,
            help=(
                "operator-local JSON object selecting the live N5-to-N4 "
                "serve gate; relative paths resolve from this file"
            ),
        )
        if include_shared_sources:
            _add_required_path(command, "--deployment-binding-dir")
            _add_required_path(command, "--logical-plan-dir")
            _add_required_path(command, "--scenario")
            _add_required_path(command, "--container-plan-dir")

    local_semantic_smoke_run = subcommands.add_parser(
        "run-simulator-full-flow-local-semantic-smokes",
        help=(
            "run the ten semantic interoperability smokes after verifying "
            "the source-bound N4 serve-frozen authorization"
        ),
    )
    _add_required_path(local_semantic_smoke_run, "--local-semantic-admission-dir")
    add_n4_serve_gate_sources(
        local_semantic_smoke_run,
        include_shared_sources=True,
    )
    local_semantic_smoke_run.add_argument("--run-id", required=True)
    _add_required_path(local_semantic_smoke_run, "--output-dir")
    local_semantic_smoke_run.add_argument("--flowmesh-base-url")
    local_semantic_smoke_run.add_argument(
        "--task-timeout", type=int, default=900
    )
    local_semantic_smoke_run.add_argument(
        "--poll-interval", type=positive_finite_float, default=2.0
    )
    _add_compact(local_semantic_smoke_run)

    local_semantic_smoke_verify = subcommands.add_parser(
        "verify-simulator-full-flow-local-semantic-smokes",
        help="verify the ten-smoke receipt, N4 gate, and frozen sources",
    )
    _add_required_path(local_semantic_smoke_verify, "--smoke-dir")
    _add_required_path(local_semantic_smoke_verify, "--local-semantic-admission-dir")
    add_n4_serve_gate_sources(
        local_semantic_smoke_verify,
        include_shared_sources=True,
    )
    _add_compact(local_semantic_smoke_verify)

    def add_local_semantic_matrix_gate_sources(
        command: argparse.ArgumentParser,
    ) -> None:
        _add_required_path(command, "--local-semantic-admission-dir")
        _add_required_path(command, "--smoke-dir")
        add_n4_serve_gate_sources(
            command,
            include_shared_sources=False,
        )
        _add_required_path(command, "--semantic-matrix-dir")
        _add_required_path(command, "--deployment-binding-dir")
        _add_required_path(command, "--logical-plan-dir")
        _add_required_path(command, "--scenario")
        _add_required_path(command, "--container-plan-dir")
        _add_required_path(command, "--public-task-set")
        _add_required_path(command, "--artifact-bindings")

    local_semantic_matrix_run = subcommands.add_parser(
        "run-simulator-full-flow-local-semantic-matrix",
        help=(
            "run or resume the semantic 64-trial matrix only after a "
            "verified source-bound ten-smoke receipt"
        ),
    )
    add_local_semantic_matrix_gate_sources(local_semantic_matrix_run)
    local_semantic_matrix_run.add_argument("--run-id", required=True)
    _add_required_path(local_semantic_matrix_run, "--output-dir")
    local_semantic_matrix_run.add_argument("--flowmesh-base-url")
    local_semantic_matrix_run.add_argument(
        "--task-timeout", type=int, default=900
    )
    local_semantic_matrix_run.add_argument(
        "--poll-interval", type=positive_finite_float, default=2.0
    )
    local_semantic_matrix_run.add_argument(
        "--acknowledge-failed-entry-sha256"
    )
    _add_compact(local_semantic_matrix_run)

    local_semantic_matrix_verify = subcommands.add_parser(
        "verify-simulator-full-flow-local-semantic-matrix",
        help="verify the source-bound smoke gate and completed semantic run",
    )
    add_local_semantic_matrix_gate_sources(local_semantic_matrix_verify)
    _add_required_path(local_semantic_matrix_verify, "--output-dir")
    _add_compact(local_semantic_matrix_verify)

    semantic_route_serve = subcommands.add_parser(
        "serve-simulator-full-flow-semantic-route",
        help=(
            "serve one N7/N8 catalog-bound semantic route coordinator using "
            "public frozen inputs and runtime-only credentials"
        ),
    )
    semantic_route_serve.add_argument(
        "--node-id", choices=("N7", "N8"), required=True
    )
    for flag in (
        "local-semantic-admission-dir",
        "n1-public-commitment-dir",
        "artifact-binding-dir",
        "n2-index-package-dir",
        "n3-package-dir",
        "n4-package-dir",
        "exact-range-catalog-dir",
        "provisioning-catalog-dir",
        "index-query-plan-catalog-dir",
        "state-dir",
    ):
        semantic_route_serve.add_argument(
            "--" + flag, type=Path, required=True
        )
    for flag in (
        "n2-index-base-url",
        "n7-index-base-url",
        "n8-index-base-url",
        "n3-data-agent-base-url",
        "n4-data-agent-base-url",
        "n7-cache-base-url",
        "n8-cache-base-url",
        "n7-cache-id",
        "n8-cache-id",
        "n7-node-health-base-url",
        "n8-node-health-base-url",
        "n6-base-url",
        "n1-base-url",
        "n1-verification-base-url",
        "semantic-model",
    ):
        semantic_route_serve.add_argument("--" + flag, required=True)
    semantic_route_serve.add_argument(
        "--simulator-private-http-hosts",
        default="",
        help=(
            "comma-separated pathfinder-sim-* or pathfinder-full-flow-* "
            "container hostnames permitted for private HTTP"
        ),
    )
    semantic_route_serve.add_argument(
        "--timeout-seconds", type=positive_finite_float, default=300.0
    )
    semantic_route_serve.add_argument(
        "--max-artifact-bytes", type=int, default=2 * 1024 * 1024 * 1024
    )
    semantic_route_serve.add_argument("--host", default="0.0.0.0")
    semantic_route_serve.add_argument("--port", type=int, required=True)


def dispatch_full_flow_semantic_command(
    args: argparse.Namespace,
    *,
    print_payload: PayloadPrinter,
    load_strict_json_file: Callable[..., object],
    load_n4_live_gate_sources: Callable[[Path | None], dict | None],
    local_semantic_flowmesh_executor: Callable[..., object],
) -> int | None:
    """Dispatch the semantic family, returning None when not handled."""

    if args.command not in COMMAND_NAMES:
        return None

    if (
        args.command
        == "freeze-simulator-full-flow-semantic-execution-admission"
    ):
        from ..simulator.full_flow_semantic_execution_admission import (
            freeze_full_flow_semantic_execution_admission,
        )

        payload = freeze_full_flow_semantic_execution_admission(
            args.semantic_matrix_dir,
            args.deployment_binding_dir,
            args.logical_plan_dir,
            args.scenario,
            args.container_plan_dir,
            args.public_task_set,
            args.artifact_bindings,
            args.n1_oracle_package_dir,
            worker_alias=args.worker_alias,
            admission_id=args.admission_id,
            output_dir=args.output_dir,
        )
        return print_payload(payload, compact=args.compact)
    if (
        args.command
        == "verify-simulator-full-flow-semantic-execution-admission"
    ):
        from ..simulator.full_flow_semantic_execution_admission import (
            verify_full_flow_semantic_execution_admission,
        )

        payload = verify_full_flow_semantic_execution_admission(
            args.admission_dir,
            args.semantic_matrix_dir,
            args.deployment_binding_dir,
            args.logical_plan_dir,
            args.scenario,
            args.container_plan_dir,
            args.public_task_set,
            args.artifact_bindings,
            args.n1_oracle_package_dir,
        )
        return print_payload(payload, compact=args.compact)
    if (
        args.command
        == "preflight-simulator-full-flow-semantic-artifacts"
    ):
        from ..simulator.full_flow_artifact_preflight import (
            preflight_full_flow_semantic_artifacts_over_http,
        )

        shared_token = os.environ.get("PATHFINDER_DATA_AGENT_TOKEN")
        n3_token = (
            os.environ.get("PATHFINDER_N3_DATA_AGENT_TOKEN")
            or shared_token
        )
        n4_token = (
            os.environ.get("PATHFINDER_N4_DATA_AGENT_TOKEN")
            or shared_token
        )
        if not n3_token or not n4_token:
            raise ConfigError(
                "node-specific PATHFINDER_N3_DATA_AGENT_TOKEN and "
                "PATHFINDER_N4_DATA_AGENT_TOKEN, or the shared "
                "PATHFINDER_DATA_AGENT_TOKEN, are required"
            )
        payload = preflight_full_flow_semantic_artifacts_over_http(
            args.semantic_execution_admission_dir,
            args.n3_package_dir,
            args.n4_package_dir,
            n3_base_url=args.n3_data_agent_url,
            n4_base_url=args.n4_data_agent_url,
            n3_token=n3_token,
            n4_token=n4_token,
            preflight_id=args.preflight_id,
            output_dir=args.output_dir,
            timeout_seconds=args.timeout_seconds,
            max_retries=args.max_retries,
            max_artifact_bytes=args.max_artifact_bytes,
            telemetry_quiescence_timeout_seconds=(
                args.telemetry_quiescence_timeout_seconds
            ),
            simulator_private_http_hosts=tuple(
                args.simulator_private_http_host
            ),
        )
        return print_payload(payload, compact=args.compact)
    if (
        args.command
        == "verify-simulator-full-flow-semantic-artifact-preflight"
    ):
        from ..simulator.full_flow_artifact_preflight import (
            verify_full_flow_semantic_artifact_preflight,
        )

        payload = verify_full_flow_semantic_artifact_preflight(
            args.preflight_dir,
            semantic_execution_admission_dir=(
                args.semantic_execution_admission_dir
            ),
            n3_package_dir=args.n3_package_dir,
            n4_package_dir=args.n4_package_dir,
        )
        return print_payload(payload, compact=args.compact)
    if (
        args.command
        == "promote-simulator-full-flow-local-semantic-execution-admission"
    ):
        from ..simulator.full_flow_local_semantic_admission import (
            promote_full_flow_local_semantic_execution_admission,
        )

        payload = promote_full_flow_local_semantic_execution_admission(
            args.legacy_admission_dir,
            args.semantic_matrix_dir,
            args.deployment_binding_dir,
            args.logical_plan_dir,
            args.scenario,
            args.container_plan_dir,
            args.public_task_set,
            args.artifact_bindings,
            args.n1_oracle_package_dir,
            args.artifact_preflight_dir,
            args.exact_range_catalog_dir,
            args.n3_package_dir,
            args.provisioning_catalog_dir,
            args.n4_package_dir,
            semantics_mode=args.semantics_mode,
            promotion_id=args.promotion_id,
            output_dir=args.output_dir,
        )
        return print_payload(payload, compact=args.compact)
    if (
        args.command
        == "verify-simulator-full-flow-local-semantic-execution-admission"
    ):
        from ..simulator.full_flow_local_semantic_admission import (
            verify_full_flow_local_semantic_execution_admission,
        )

        payload = verify_full_flow_local_semantic_execution_admission(
            args.admission_dir,
            args.legacy_admission_dir,
            args.semantic_matrix_dir,
            args.deployment_binding_dir,
            args.logical_plan_dir,
            args.scenario,
            args.container_plan_dir,
            args.public_task_set,
            args.artifact_bindings,
            args.n1_oracle_package_dir,
            args.artifact_preflight_dir,
            args.exact_range_catalog_dir,
            args.n3_package_dir,
            args.provisioning_catalog_dir,
            args.n4_package_dir,
        )
        return print_payload(payload, compact=args.compact)
    if (
        args.command
        == "verify-simulator-full-flow-local-semantic-runtime-package"
    ):
        from ..simulator.full_flow_local_semantic_admission import (
            verify_full_flow_local_semantic_runtime_package,
        )

        payload = verify_full_flow_local_semantic_runtime_package(
            args.admission_dir
        )
        return print_payload(payload, compact=args.compact)
    if (
        args.command
        == "build-simulator-full-flow-index-query-plan-catalog"
    ):
        from ..simulator.full_flow_index_query_plan_catalog import (
            build_full_flow_index_query_plan_catalog,
        )

        payload = build_full_flow_index_query_plan_catalog(
            args.local_semantic_admission_dir,
            args.n2_index_package_dir,
            output_dir=args.output_dir,
        )
        return print_payload(payload, compact=args.compact)
    if (
        args.command
        == "verify-simulator-full-flow-index-query-plan-catalog"
    ):
        from ..simulator.full_flow_index_query_plan_catalog import (
            verify_full_flow_index_query_plan_catalog,
        )

        payload = verify_full_flow_index_query_plan_catalog(
            args.catalog_dir,
            local_semantic_admission_dir=(
                args.local_semantic_admission_dir
            ),
            n2_index_package_dir=args.n2_index_package_dir,
        )
        return print_payload(payload, compact=args.compact)
    if (
        args.command
        == "freeze-simulator-full-flow-n4-preprovisioned-serve-gate"
    ):
        from ..simulator.full_flow_n4_serve_gate import (
            freeze_full_flow_n4_preprovisioned_serve_gate,
        )

        payload = freeze_full_flow_n4_preprovisioned_serve_gate(
            args.compose_overlay_dir,
            args.service_bootstrap_dir,
            args.deployment_binding_dir,
            args.logical_plan_dir,
            args.scenario,
            args.container_plan_dir,
            args.provisioning_catalog_dir,
            args.artifact_binding_dir,
            args.n4_package_dir,
            gate_id=args.gate_id,
            output_dir=args.output_dir,
        )
        return print_payload(payload, compact=args.compact)
    if (
        args.command
        == "verify-simulator-full-flow-n4-preprovisioned-serve-gate"
    ):
        from ..simulator.full_flow_n4_serve_gate import (
            verify_full_flow_n4_preprovisioned_serve_gate,
        )

        payload = verify_full_flow_n4_preprovisioned_serve_gate(
            args.gate_dir,
            args.compose_overlay_dir,
            args.service_bootstrap_dir,
            args.deployment_binding_dir,
            args.logical_plan_dir,
            args.scenario,
            args.container_plan_dir,
            args.provisioning_catalog_dir,
            args.artifact_binding_dir,
            args.n4_package_dir,
        )
        return print_payload(payload, compact=args.compact)
    if args.command in {
        "freeze-simulator-full-flow-n4-live-serve-gate",
        "verify-simulator-full-flow-n4-live-serve-gate",
    }:
        live_receipt_bindings = load_strict_json_file(
            args.live_receipt_bindings,
            label="live receipt bindings",
            expected_type=list,
        )
        if args.command.startswith("freeze-"):
            from ..simulator.full_flow_n4_live_serve_gate import (
                freeze_full_flow_n4_live_serve_gate,
            )

            payload = freeze_full_flow_n4_live_serve_gate(
                live_receipt_bindings,
                args.n4_publication_store_root,
                args.rebound_artifact_binding_dir,
                args.rebound_semantic_matrix_dir,
                args.rebound_admission_dir,
                gate_id=args.gate_id,
                output_dir=args.output_dir,
            )
        else:
            from ..simulator.full_flow_n4_live_serve_gate import (
                verify_full_flow_n4_live_serve_gate,
            )

            payload = verify_full_flow_n4_live_serve_gate(
                args.gate_dir,
                live_receipt_bindings,
                args.n4_publication_store_root,
                args.rebound_artifact_binding_dir,
                args.rebound_semantic_matrix_dir,
                args.rebound_admission_dir,
            )
        return print_payload(payload, compact=args.compact)
    if args.command == "run-simulator-full-flow-local-semantic-smokes":
        from ..simulator.full_flow_local_semantic_smoke import (
            run_full_flow_local_semantic_smokes,
        )

        n4_live_gate_sources = load_n4_live_gate_sources(
            args.n4_live_gate_sources
        )
        with local_semantic_flowmesh_executor(
            args.local_semantic_admission_dir,
            run_id=args.run_id,
            flowmesh_base_url=args.flowmesh_base_url,
            task_timeout_seconds=args.task_timeout,
            poll_interval_seconds=args.poll_interval,
        ) as executor:
            payload = run_full_flow_local_semantic_smokes(
                args.local_semantic_admission_dir,
                args.n4_serve_gate_dir,
                args.compose_overlay_dir,
                args.service_bootstrap_dir,
                args.deployment_binding_dir,
                args.logical_plan_dir,
                args.scenario,
                args.container_plan_dir,
                args.provisioning_catalog_dir,
                args.artifact_binding_dir,
                args.n4_package_dir,
                run_id=args.run_id,
                executor=executor,
                output_dir=args.output_dir,
                n4_live_gate_sources=n4_live_gate_sources,
            )
        return print_payload(payload, compact=args.compact)
    if args.command == "verify-simulator-full-flow-local-semantic-smokes":
        from ..simulator.full_flow_local_semantic_smoke import (
            verify_full_flow_local_semantic_smokes,
        )

        n4_live_gate_sources = load_n4_live_gate_sources(
            args.n4_live_gate_sources
        )
        payload = verify_full_flow_local_semantic_smokes(
            args.smoke_dir,
            local_semantic_admission_dir=(
                args.local_semantic_admission_dir
            ),
            n4_serve_gate_dir=args.n4_serve_gate_dir,
            compose_overlay_dir=args.compose_overlay_dir,
            service_bootstrap_dir=args.service_bootstrap_dir,
            deployment_binding_dir=args.deployment_binding_dir,
            logical_route_dir=args.logical_plan_dir,
            scenario_path=args.scenario,
            container_plan_dir=args.container_plan_dir,
            provisioning_catalog_dir=args.provisioning_catalog_dir,
            artifact_binding_dir=args.artifact_binding_dir,
            n4_package_dir=args.n4_package_dir,
            n4_live_gate_sources=n4_live_gate_sources,
        )
        return print_payload(payload, compact=args.compact)
    if args.command == "run-simulator-full-flow-local-semantic-matrix":
        from ..simulator.full_flow_local_semantic_matrix_gate import (
            run_smoke_gated_full_flow_local_semantic_matrix,
        )

        n4_live_gate_sources = load_n4_live_gate_sources(
            args.n4_live_gate_sources
        )
        with local_semantic_flowmesh_executor(
            args.local_semantic_admission_dir,
            run_id=args.run_id,
            flowmesh_base_url=args.flowmesh_base_url,
            task_timeout_seconds=args.task_timeout,
            poll_interval_seconds=args.poll_interval,
        ) as executor:
            payload = run_smoke_gated_full_flow_local_semantic_matrix(
                args.local_semantic_admission_dir,
                args.smoke_dir,
                args.n4_serve_gate_dir,
                args.compose_overlay_dir,
                args.service_bootstrap_dir,
                args.provisioning_catalog_dir,
                args.artifact_binding_dir,
                args.n4_package_dir,
                args.semantic_matrix_dir,
                args.deployment_binding_dir,
                args.logical_plan_dir,
                args.scenario,
                args.container_plan_dir,
                args.public_task_set,
                args.artifact_bindings,
                run_id=args.run_id,
                output_dir=args.output_dir,
                executor=executor,
                acknowledge_failed_entry_sha256=(
                    args.acknowledge_failed_entry_sha256
                ),
                n4_live_gate_sources=n4_live_gate_sources,
            )
        return print_payload(payload, compact=args.compact)
    if args.command == "verify-simulator-full-flow-local-semantic-matrix":
        from ..simulator.full_flow_local_semantic_matrix_gate import (
            verify_smoke_gated_full_flow_local_semantic_matrix_run,
        )

        n4_live_gate_sources = load_n4_live_gate_sources(
            args.n4_live_gate_sources
        )
        payload = verify_smoke_gated_full_flow_local_semantic_matrix_run(
            args.local_semantic_admission_dir,
            args.smoke_dir,
            args.n4_serve_gate_dir,
            args.compose_overlay_dir,
            args.service_bootstrap_dir,
            args.provisioning_catalog_dir,
            args.artifact_binding_dir,
            args.n4_package_dir,
            args.semantic_matrix_dir,
            args.deployment_binding_dir,
            args.logical_plan_dir,
            args.scenario,
            args.container_plan_dir,
            args.public_task_set,
            args.artifact_bindings,
            output_dir=args.output_dir,
            n4_live_gate_sources=n4_live_gate_sources,
        )
        return print_payload(payload, compact=args.compact)
    if args.command == "serve-simulator-full-flow-semantic-route":
        from ..simulator.container_node import serve_container_node
        from ..simulator.full_flow_semantic_route_service_factory import (
            FrozenSemanticRouteServiceSources,
            RuntimeSemanticServiceInputs,
            assemble_full_flow_semantic_route_service,
        )

        credential_names = (
            "N2 index",
            "N7 index",
            "N8 index",
            "N3 Data Agent",
            "N4 Data Agent",
            "N7 cache",
            "N8 cache",
            "N6 semantic",
            "N1 score",
            "N1 verifier",
            "route ingress",
        )
        credentials, missing = _resolve_full_flow_credentials(
            os.environ,
            credential_names,
        )
        if missing:
            raise ConfigError(
                "semantic route service requires runtime-only "
                "credential environment variables: "
                + ", ".join(missing)
            )
        private_hosts = tuple(
            value.strip()
            for value in args.simulator_private_http_hosts.split(",")
            if value.strip()
        )
        sources = FrozenSemanticRouteServiceSources(
            local_admission_dir=args.local_semantic_admission_dir,
            n1_public_commitment_dir=args.n1_public_commitment_dir,
            artifact_binding_dir=args.artifact_binding_dir,
            n2_index_package_dir=args.n2_index_package_dir,
            n3_package_dir=args.n3_package_dir,
            n4_package_dir=args.n4_package_dir,
            exact_range_catalog_dir=args.exact_range_catalog_dir,
            provisioning_catalog_dir=args.provisioning_catalog_dir,
            index_query_plan_catalog_dir=(
                args.index_query_plan_catalog_dir
            ),
        )
        runtime = RuntimeSemanticServiceInputs(
            logical_node_id=args.node_id,
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
            data_agent_base_urls={
                "N3": args.n3_data_agent_base_url,
                "N4": args.n4_data_agent_base_url,
            },
            data_agent_bearer_tokens={
                "N3": credentials["N3 Data Agent"],
                "N4": credentials["N4 Data Agent"],
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
            node_health_base_urls={
                "N7": args.n7_node_health_base_url,
                "N8": args.n8_node_health_base_url,
            },
            n6_base_url=args.n6_base_url,
            n6_bearer_token=credentials["N6 semantic"],
            n1_base_url=args.n1_base_url,
            n1_bearer_token=credentials["N1 score"],
            n1_verification_base_url=args.n1_verification_base_url,
            n1_verification_bearer_token=credentials["N1 verifier"],
            semantic_model=args.semantic_model,
            timeout_seconds=args.timeout_seconds,
            max_artifact_bytes=args.max_artifact_bytes,
            simulator_private_http_hosts=private_hosts,
        )
        assembly = assemble_full_flow_semantic_route_service(
            sources,
            runtime,
            state_dir=args.state_dir,
        )
        assembly.require_ready()
        serve_container_node(
            args.node_id,
            args.state_dir,
            host=args.host,
            port=args.port,
            semantic_route_handler=assembly.handler,
        )
        return 0
    return None
