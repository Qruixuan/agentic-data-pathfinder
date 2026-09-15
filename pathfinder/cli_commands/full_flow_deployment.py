"""CLI wiring for full-flow deployment and local Compose contracts.

This module deliberately contains only argument-parser and dispatch wiring.
The implementation remains in :mod:`pathfinder.simulator`, and imports stay
lazy so building ``pathfinder --help`` does not load simulator services.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Callable

from ._common import PayloadPrinter, add_compact, add_output_dir


COMMAND_NAMES = frozenset({
    "freeze-simulator-full-flow-service-bootstrap",
    "verify-simulator-full-flow-service-bootstrap",
    "build-simulator-full-flow-deployment",
    "verify-simulator-full-flow-deployment",
    "preflight-simulator-full-flow-deployment",
    "render-simulator-full-flow-compose-overlay",
    "verify-simulator-full-flow-compose-overlay",
    "generate-simulator-full-flow-deployment-template",
    "verify-simulator-full-flow-deployment-template",
    "validate-simulator-full-flow-deployment-source",
})


def _add_logical_inputs(command: argparse.ArgumentParser) -> None:
    command.add_argument("--logical-plan-dir", type=Path, required=True)
    command.add_argument("--scenario", type=Path, required=True)
    command.add_argument("--container-plan-dir", type=Path, required=True)


def register_full_flow_deployment_commands(
    subcommands: argparse._SubParsersAction,
    *,
    positive_finite_float: Callable[[str], float],
) -> None:
    """Register endpoint-free bootstrap, deployment, and Compose commands."""

    service_bootstrap_freeze = subcommands.add_parser(
        "freeze-simulator-full-flow-service-bootstrap",
        help="freeze endpoint-free N1-N8 process startup contracts",
    )
    _add_logical_inputs(service_bootstrap_freeze)
    service_bootstrap_freeze.add_argument("--bootstrap-id", required=True)
    add_output_dir(service_bootstrap_freeze)
    add_compact(service_bootstrap_freeze)

    service_bootstrap_verify = subcommands.add_parser(
        "verify-simulator-full-flow-service-bootstrap",
        help="re-derive and verify N1-N8 process startup contracts",
    )
    service_bootstrap_verify.add_argument(
        "--bootstrap-dir", type=Path, required=True
    )
    _add_logical_inputs(service_bootstrap_verify)
    add_compact(service_bootstrap_verify)

    deployment_build = subcommands.add_parser(
        "build-simulator-full-flow-deployment",
        help="bind every logical full-flow service to a concrete deployment",
    )
    _add_logical_inputs(deployment_build)
    deployment_build.add_argument(
        "--deployment-source", type=Path, required=True
    )
    add_output_dir(deployment_build)
    add_compact(deployment_build)

    deployment_verify = subcommands.add_parser(
        "verify-simulator-full-flow-deployment",
        help="verify all service, representation, and state bindings offline",
    )
    deployment_verify.add_argument("--binding-dir", type=Path, required=True)
    _add_logical_inputs(deployment_verify)
    add_compact(deployment_verify)

    deployment_preflight = subcommands.add_parser(
        "preflight-simulator-full-flow-deployment",
        help="read-only health probe every distinct full-flow service origin",
    )
    deployment_preflight.add_argument(
        "--binding-dir", type=Path, required=True
    )
    _add_logical_inputs(deployment_preflight)
    deployment_preflight.add_argument(
        "--timeout-seconds", type=positive_finite_float, default=5.0
    )
    add_compact(deployment_preflight)

    compose_overlay_render = subcommands.add_parser(
        "render-simulator-full-flow-compose-overlay",
        help="freeze a unified N1-N8 Compose overlay without launching it",
    )
    compose_overlay_render.add_argument(
        "--service-bootstrap-dir", type=Path, required=True
    )
    compose_overlay_render.add_argument(
        "--deployment-binding-dir", type=Path, required=True
    )
    _add_logical_inputs(compose_overlay_render)
    compose_overlay_render.add_argument("--overlay-id", required=True)
    add_output_dir(compose_overlay_render)
    add_compact(compose_overlay_render)

    compose_overlay_verify = subcommands.add_parser(
        "verify-simulator-full-flow-compose-overlay",
        help="verify a unified N1-N8 Compose overlay from source contracts",
    )
    compose_overlay_verify.add_argument(
        "--overlay-dir", type=Path, required=True
    )
    compose_overlay_verify.add_argument(
        "--service-bootstrap-dir", type=Path, required=True
    )
    compose_overlay_verify.add_argument(
        "--deployment-binding-dir", type=Path, required=True
    )
    _add_logical_inputs(compose_overlay_verify)
    add_compact(compose_overlay_verify)

    deployment_template_generate = subcommands.add_parser(
        "generate-simulator-full-flow-deployment-template",
        help=(
            "generate an endpoint-free deployment source template for all "
            "logical services"
        ),
    )
    _add_logical_inputs(deployment_template_generate)
    deployment_template_generate.add_argument("--template-id", required=True)
    add_output_dir(deployment_template_generate)
    add_compact(deployment_template_generate)

    deployment_template_verify = subcommands.add_parser(
        "verify-simulator-full-flow-deployment-template",
        help="verify a full-flow deployment source template offline",
    )
    deployment_template_verify.add_argument(
        "--template-dir", type=Path, required=True
    )
    _add_logical_inputs(deployment_template_verify)
    add_compact(deployment_template_verify)

    deployment_source_validate = subcommands.add_parser(
        "validate-simulator-full-flow-deployment-source",
        help=(
            "validate an operator-completed deployment source without "
            "publishing a binding"
        ),
    )
    deployment_source_validate.add_argument(
        "--deployment-source", type=Path, required=True
    )
    deployment_source_validate.add_argument(
        "--template-dir", type=Path, required=True
    )
    _add_logical_inputs(deployment_source_validate)
    add_compact(deployment_source_validate)


def dispatch_full_flow_deployment_command(
    args: argparse.Namespace,
    *,
    print_payload: PayloadPrinter,
) -> int | None:
    """Dispatch this command family, returning ``None`` when not handled."""

    if args.command not in COMMAND_NAMES:
        return None

    if args.command == "freeze-simulator-full-flow-service-bootstrap":
        from ..simulator.full_flow_service_bootstrap import (
            freeze_full_flow_local_service_bootstrap,
        )

        payload = freeze_full_flow_local_service_bootstrap(
            args.logical_plan_dir,
            args.scenario,
            args.container_plan_dir,
            bootstrap_id=args.bootstrap_id,
            output_dir=args.output_dir,
        )
    elif args.command == "verify-simulator-full-flow-service-bootstrap":
        from ..simulator.full_flow_service_bootstrap import (
            verify_full_flow_local_service_bootstrap,
        )

        payload = verify_full_flow_local_service_bootstrap(
            args.bootstrap_dir,
            logical_plan_dir=args.logical_plan_dir,
            scenario_path=args.scenario,
            container_plan_dir=args.container_plan_dir,
        )
    elif args.command == "build-simulator-full-flow-deployment":
        from ..simulator.full_flow_deployment import (
            build_full_flow_deployment_binding,
        )

        payload = build_full_flow_deployment_binding(
            args.logical_plan_dir,
            args.scenario,
            args.container_plan_dir,
            args.deployment_source,
            output_dir=args.output_dir,
        )
    elif args.command == "verify-simulator-full-flow-deployment":
        from ..simulator.full_flow_deployment import (
            verify_full_flow_deployment_binding,
        )

        payload = verify_full_flow_deployment_binding(
            args.binding_dir,
            logical_plan_dir=args.logical_plan_dir,
            scenario_path=args.scenario,
            container_plan_dir=args.container_plan_dir,
        )
    elif args.command == "preflight-simulator-full-flow-deployment":
        from ..simulator.full_flow_deployment import (
            preflight_full_flow_deployment,
        )

        payload = preflight_full_flow_deployment(
            args.binding_dir,
            logical_plan_dir=args.logical_plan_dir,
            scenario_path=args.scenario,
            container_plan_dir=args.container_plan_dir,
            timeout_seconds=args.timeout_seconds,
        )
    elif args.command == "render-simulator-full-flow-compose-overlay":
        from ..simulator.full_flow_compose_overlay import (
            render_full_flow_local_compose_overlay,
        )

        payload = render_full_flow_local_compose_overlay(
            args.service_bootstrap_dir,
            args.deployment_binding_dir,
            logical_plan_dir=args.logical_plan_dir,
            scenario_path=args.scenario,
            container_plan_dir=args.container_plan_dir,
            overlay_id=args.overlay_id,
            output_dir=args.output_dir,
        )
    elif args.command == "verify-simulator-full-flow-compose-overlay":
        from ..simulator.full_flow_compose_overlay import (
            verify_full_flow_local_compose_overlay,
        )

        payload = verify_full_flow_local_compose_overlay(
            args.overlay_dir,
            service_bootstrap_dir=args.service_bootstrap_dir,
            deployment_binding_dir=args.deployment_binding_dir,
            logical_plan_dir=args.logical_plan_dir,
            scenario_path=args.scenario,
            container_plan_dir=args.container_plan_dir,
        )
    elif args.command == "generate-simulator-full-flow-deployment-template":
        from ..simulator.full_flow_deployment_template import (
            generate_full_flow_deployment_source_template,
        )

        payload = generate_full_flow_deployment_source_template(
            args.logical_plan_dir,
            args.scenario,
            args.container_plan_dir,
            template_id=args.template_id,
            output_dir=args.output_dir,
        )
    elif args.command == "verify-simulator-full-flow-deployment-template":
        from ..simulator.full_flow_deployment_template import (
            verify_full_flow_deployment_source_template,
        )

        payload = verify_full_flow_deployment_source_template(
            args.template_dir,
            logical_plan_dir=args.logical_plan_dir,
            scenario_path=args.scenario,
            container_plan_dir=args.container_plan_dir,
        )
    else:
        from ..simulator.full_flow_deployment_template import (
            validate_completed_full_flow_deployment_source,
        )

        payload = validate_completed_full_flow_deployment_source(
            args.deployment_source,
            template_dir=args.template_dir,
            logical_plan_dir=args.logical_plan_dir,
            scenario_path=args.scenario,
            container_plan_dir=args.container_plan_dir,
        )

    return print_payload(payload, compact=args.compact)
