"""Bounded, public Data Agent access probe without FlowMesh or an LLM.

Run only after immutable binding and credential preflights. This creates one
durable Data Agent operation at each of N3, N4 remote, and N7 replica; it
never fetches an artifact or prints inline content, URLs, or credentials.
"""

from __future__ import annotations

import argparse
import json
import uuid
from pathlib import Path

from pathfinder.config import load_config
from pathfinder.distributed.routing import (
    build_routed_gateway_backend,
    close_routed_backend,
)
from pathfinder.integrations.flowmesh.gateway import GatewaySession


PROBES = (
    ("PPD_REMOTE_DIGEST", "raw_video", "n3_raw"),
    ("PPD_REMOTE_DIGEST", "sampled_frame_bundle", "n4_remote"),
    ("PPD_LOCAL_DIGEST", "multimodal_digest", "n4_n7_replica"),
)


def probe(config_path: Path, registry_path: Path, object_id: str) -> list[dict]:
    config = load_config(config_path)
    backend, registry = build_routed_gateway_backend(registry_path)
    observations = []
    try:
        for design_id, representation_id, expected_endpoint in PROBES:
            route = registry.route(
                design_id=design_id,
                representation_id=representation_id,
            )
            if route.endpoint_id != expected_endpoint:
                raise RuntimeError("frozen PPD placement differs")
            session_id = "ppd-binding-check-" + uuid.uuid4().hex
            session = GatewaySession(
                session_id=session_id,
                trial_id=session_id,
                question="public data-plane binding check",
                design_id=design_id,
                task_class_id="video_qa",
                quote_profile_id="as_designed",
                latency_multiplier=1.0,
                seed=0,
                price_universe_version=config.price_universe_version,
                status="RUNNING",
                object_id=object_id,
            )
            result = backend.access(
                config=config,
                session=session,
                representation_id=representation_id,
                event_index=0,
            )
            if result.endpoint_id != expected_endpoint:
                raise RuntimeError("Data Agent endpoint differs")
            if result.content_sha256 is None:
                raise RuntimeError("Data Agent content identity is missing")
            observations.append({
                "endpoint_id": expected_endpoint,
                "representation_id": representation_id,
                "status": "ACCESS_OK",
                "content_identity_present": True,
            })
    finally:
        close_routed_backend(backend)
    return observations


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--endpoint-registry", type=Path, required=True)
    parser.add_argument("--object-id", required=True)
    args = parser.parse_args()
    try:
        print(json.dumps({
            "status": "VERIFIED",
            "observations": probe(
                args.config, args.endpoint_registry, args.object_id,
            ),
            "workflow_submitted": False,
            "llm_called": False,
            "credentials_recorded": False,
        }, sort_keys=True))
    except Exception as exc:
        frames = []
        frame = exc.__traceback__
        while frame is not None:
            frames.append(
                f"{Path(frame.tb_frame.f_code.co_filename).name}:"
                f"{frame.tb_frame.f_code.co_name}:{frame.tb_lineno}"
            )
            frame = frame.tb_next
        print(json.dumps({
            "status": "FAILED", "error_class": type(exc).__name__,
            "http_status": getattr(exc, "status_code", None),
            "failure_frames": frames[-8:],
            "workflow_submitted": False, "llm_called": False,
            "credentials_recorded": False,
        }, sort_keys=True))
        raise SystemExit(1) from None
