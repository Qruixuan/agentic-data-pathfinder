"""Generate public, isolated Compose clone specs for the ten-route cohort.

This reuses the frozen service fragments and the existing interleaved route
overlay; only the experiment-specific paths, origins, ports and cache sizes
are supplied here.  No credential value is part of the generated files.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


PUBLIC = "/home/pathfinder/t60-public-20260925-v1"
DEPLOY = "/home/pathfinder/t60-deploy-20260925-v1/t60-deployment-specs-v5"
ORACLE = (
    "/opt/pathfinder/formal/private/t60-oracle-20260925-v1/"
    "oracle/n1-oracle-package"
)
PREFIX = "pathfinder-full-flow-"
PATHS = {
    "PATHFINDER_LOCAL_SEMANTIC_ADMISSION_DIR": "t60-runtime-v1/admission",
    "PATHFINDER_N1_PUBLIC_COMMITMENT_DIR": "t60-n1-public-v1/commitment",
    "PATHFINDER_FULL_FLOW_ARTIFACT_BINDING_DIR": "t60-runtime-v1/bindings",
    "PATHFINDER_N2_PACKAGE_DIR": "t60-final-v1/n2",
    "PATHFINDER_N3_PACKAGE_DIR": "t60-final-v1/n3",
    "PATHFINDER_N4_PACKAGE_DIR": "t60-final-v1/n4",
    "PATHFINDER_INTERLEAVED_TRIAL_DAG_DIR": "t60-runtime-v1/dags",
    "PATHFINDER_INTERLEAVED_PLAN_DIR": (
        "ten-route-multiq-sealed-20260925-v1-plan-20260924t171621z"
    ),
    "PATHFINDER_INTERLEAVED_RAW_PACKAGE_DIR": "t60-final-input-v1/build/raw",
    "PATHFINDER_INTERLEAVED_QUERY_DIR": "t60-paid-public-20260925-v1/query",
    "PATHFINDER_INTERLEAVED_VIDEO_INDEX_DIR": (
        "t60-paid-public-20260925-v1/video-index"
    ),
    "PATHFINDER_INTERLEAVED_PREPARATION_DIR": (
        "t60-final-input-v1/build/preparation"
    ),
    "PATHFINDER_INTERLEAVED_CAPTION_DIR": (
        "t60-paid-public-20260925-v1/captions"
    ),
}
ORIGINS = {
    "PATHFINDER_N1_ORACLE_BASE_URL": (
        "http://pathfinder-full-flow-n1-hidden-score:19091"
    ),
    "PATHFINDER_N1_VERIFICATION_BASE_URL": (
        "http://pathfinder-full-flow-n1-hidden-score-n1-remote-verification:19191"
    ),
    "PATHFINDER_N2_INDEX_BASE_URL": (
        "http://pathfinder-full-flow-n2-global-index:19092"
    ),
    "PATHFINDER_N3_DATA_AGENT_BASE_URL": (
        "http://pathfinder-full-flow-n3-raw-data-agent:19093"
    ),
    "PATHFINDER_N4_DATA_AGENT_BASE_URL": (
        "http://pathfinder-full-flow-n4-derived-data-agent:19094"
    ),
    "PATHFINDER_N7_CACHE_BASE_URL": (
        "http://pathfinder-full-flow-n7-persistent-cache:19287"
    ),
    "PATHFINDER_N8_CACHE_BASE_URL": (
        "http://pathfinder-full-flow-n8-persistent-cache:19288"
    ),
    "PATHFINDER_N7_NODE_HEALTH_BASE_URL": (
        "http://pathfinder-full-flow-n7-execution-compute:18781"
    ),
    "PATHFINDER_N8_NODE_HEALTH_BASE_URL": (
        "http://pathfinder-full-flow-n8-execution-compute:18881"
    ),
    "PATHFINDER_N7_CACHE_ID": "pathfinder-t60-n7-cache-v1",
    "PATHFINDER_N8_CACHE_ID": "pathfinder-t60-n8-cache-v1",
}


def _spec(node: str, suffix: str, old_project: str,
          project: str, port: int, overrides: dict[str, str],
          *, overlay: bool = False) -> dict:
    service = PREFIX + suffix
    fragment = (
        f"/opt/pathfinder/deploy/node-bundles/{node}/"
        f"compose.service.{service}.yaml"
    )
    overrides = {
        **overrides,
        service.upper().replace("-", "_") + "_HOST_PORT": str(port),
    }
    result = {
        "service": service,
        "predecessor": f"{old_project}-{service}-1",
        "project": project,
        "bind_address": f"10.70.0.{10 + int(node[1])}",
        "host_port": port,
        "fragments": [fragment],
        "overrides": overrides,
    }
    if node in {"N1", "N2", "N3", "N4"}:
        result["env_files"] = [
            f"/opt/pathfinder/env/node-formal-bd2da19-{node.lower()}.env",
            "/home/pathfinder/multiq-config-d328726/"
            + ("pilot-v2.env" if node == "N3" else "pilot.env"),
        ]
    if overlay:
        result["fragments"].append(f"{DEPLOY}/route-overlay-{node.lower()}.yaml")
    return result


def _route_overlay(reference: str, node: str) -> str:
    if node == "N7":
        return reference.replace(
            "pathfinder-multiq-n7-route-state-d328726",
            "pathfinder-t60-n7-route-state-v1",
        )
    replacements = {
        "pathfinder-full-flow-n7-execution-compute:\n": (
            "pathfinder-full-flow-n8-execution-compute:\n"
        ),
        "      - N7\n": "      - N8\n",
        "PATHFINDER_N7_ROUTE_STATE_DIR": "PATHFINDER_N8_ROUTE_STATE_DIR",
        "PATHFINDER_N7_ROUTE_LISTEN_PORT": "PATHFINDER_N8_ROUTE_LISTEN_PORT",
        "pathfinder-multiq-n7-route-state-d328726": (
            "pathfinder-t60-n8-route-state-v1"
        ),
        "multiq-n7-route-state": "multiq-n8-route-state",
    }
    text = reference
    for before, after in replacements.items():
        if before not in text:
            raise ValueError("route overlay source shape changed")
        text = text.replace(before, after)
    if "- N7\n" in text or "PATHFINDER_N7_ROUTE_STATE_DIR" in text:
        raise ValueError("N8 route overlay still contains N7 own-node identity")
    return text


def build(reference: str, capacity: int,
          route_image: str) -> tuple[dict[str, dict], dict[str, str]]:
    if capacity <= 0:
        raise ValueError("cache capacity must be positive")
    if not route_image.startswith("sha256:"):
        raise ValueError("route image must be digest-pinned")
    specs = {
        "n1-score": _spec(
            "N1", "n1-hidden-score", "r24n1", "t60n1", 19091,
            {"PATHFINDER_N1_PACKAGE_DIR": ORACLE,
             "PATHFINDER_COMPOSE_N1_HIDDEN_SCORE_STATE_VOLUME":
                 "pathfinder-t60-n1-state-v1"},
        ),
        "n1-verify": _spec(
            "N1", "n1-hidden-score-n1-remote-verification",
            "r24n1", "t60n1", 19191,
            {"PATHFINDER_N1_PACKAGE_DIR": ORACLE,
             "PATHFINDER_COMPOSE_N1_HIDDEN_SCORE_STATE_VOLUME":
                 "pathfinder-t60-n1-state-v1"},
        ),
        "n2": _spec(
            "N2", "n2-global-index", "r24n2", "t60n2", 19092,
            {"PATHFINDER_N2_PACKAGE_DIR": f"{PUBLIC}/t60-final-v1/n2"},
        ),
        "n3": _spec(
            "N3", "n3-raw-data-agent", "r24n3", "t60n3", 19093,
            {"PATHFINDER_N3_PACKAGE_DIR": f"{PUBLIC}/t60-final-v1/n3",
             "PATHFINDER_N3_PUBLIC_BASE_URL":
                 ORIGINS["PATHFINDER_N3_DATA_AGENT_BASE_URL"],
             "PATHFINDER_COMPOSE_N3_RAW_DATA_AGENT_STATE_VOLUME":
                 "pathfinder-t60-n3-state-v1"},
        ),
        "n4": _spec(
            "N4", "n4-derived-data-agent", "r24n4", "t60n4", 19094,
            {"PATHFINDER_N4_PACKAGE_DIR": f"{PUBLIC}/t60-final-v1/n4",
             "PATHFINDER_N4_PUBLIC_BASE_URL":
                 ORIGINS["PATHFINDER_N4_DATA_AGENT_BASE_URL"],
             "PATHFINDER_COMPOSE_N4_DERIVED_DATA_AGENT_STATE_VOLUME":
                 "pathfinder-t60-n4-state-v1"},
        ),
    }
    specs["n1-verify"]["reuse_state_volumes"] = [
        "pathfinder-t60-n1-state-v1"
    ]
    for node, old_cache, old_route, route_port, cache_port in (
        ("N7", "pathfinder-multiq-d328726-n7", "r24n7", 18781, 19287),
        ("N8", "pathfinder-full-flow-n8", "pathfinder-full-flow-n8",
         18881, 19288),
    ):
        lower = node.lower()
        cache_overrides = {
            f"PATHFINDER_{node}_CACHE_ID": ORIGINS[f"PATHFINDER_{node}_CACHE_ID"],
            f"PATHFINDER_{node}_CACHE_CAPACITY_BYTES": str(capacity),
            f"PATHFINDER_COMPOSE_{node}_PERSISTENT_CACHE_STATE_VOLUME": (
                f"pathfinder-t60-{lower}-cache-state-v1"
            ),
        }
        specs[f"{lower}-cache"] = _spec(
            node, f"{lower}-persistent-cache", old_cache,
            f"t60{lower}cache", cache_port, cache_overrides,
        )
        route_overrides = {
            **{key: f"{PUBLIC}/{value}" for key, value in PATHS.items()},
            **ORIGINS,
        }
        specs[f"{lower}-route"] = _spec(
            node, f"{lower}-execution-compute", old_route,
            f"t60{lower}route", route_port, route_overrides,
            overlay=True,
        )
        specs[f"{lower}-route"]["image"] = route_image
    overlays = {node: _route_overlay(reference, node) for node in ("N7", "N8")}
    return specs, overlays


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--capacity", type=int, required=True)
    parser.add_argument("--route-image", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.output_dir.exists():
        raise ValueError("deployment spec directory already exists")
    specs, overlays = build(args.reference.read_text(encoding="utf-8"),
                            args.capacity, args.route_image)
    args.output_dir.mkdir(parents=True)
    for name, spec in specs.items():
        (args.output_dir / f"{name}.json").write_text(
            json.dumps(spec, sort_keys=True, indent=2) + "\n", encoding="utf-8",
            newline="\n",
        )
    for node, overlay in overlays.items():
        (args.output_dir / f"route-overlay-{node.lower()}.yaml").write_text(
            overlay, encoding="utf-8", newline="\n",
        )


if __name__ == "__main__":
    main()
