"""Bound binary Data Agent artifacts to N6 vision, never to MCP text.

The deployed FlowMesh MCP adapter serializes tool results to strings.  A
base64 image in such a result is *not* a model image.  This bridge keeps the
verified media inside Pathfinder and sends it to N6's existing multimodal
endpoint; the agent receives only the inference result and accounting data.
"""

from __future__ import annotations

import base64
import hashlib
import json
import math
import uuid
from dataclasses import dataclass
from typing import Any, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from ...data_agent_client import (
    DataAgentBinaryArtifact,
    DataAgentClientSettings,
)
from ...frame_bundle_ingest import (
    FRAME_BUNDLE_MEDIA_TYPE,
    validate_frame_bundle_bytes,
)


class VisualArtifactError(RuntimeError):
    """A visual artifact or N6 result failed a required binding."""


_MEDIA_TYPES = {
    "raw_video": "video/mp4",
    "sampled_frame_bundle": FRAME_BUNDLE_MEDIA_TYPE,
}
_MAX_RESPONSE_BYTES = 1024 * 1024
_MAX_N6_VIDEO_BYTES = 7_000_000


def _digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _canonical(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"),
        ensure_ascii=False, allow_nan=False,
    ).encode("utf-8")


def visual_request(
    *,
    session_id: str,
    question: str,
    object_id: str,
    representation_id: str,
    artifact: DataAgentBinaryArtifact,
    video_frames_per_second: float = 2.0,
) -> dict[str, Any]:
    """Construct the exact N6 vision schema from verified Data Agent bytes."""
    # Import lazily: importing the simulator package while the FlowMesh
    # gateway module is initializing would create a package import cycle.
    from ...simulator.container_node import (
        CONTAINER_NODE_SEMANTIC_VIDEO_REQUEST_SCHEMA_VERSION,
        CONTAINER_NODE_SEMANTIC_VISION_REQUEST_SCHEMA_VERSION,
        ContainerNodeRuntime,
        semantic_frame_sequence_sha256,
    )
    if not session_id or not question or not object_id:
        raise VisualArtifactError("session, question and object are required")
    expected_media = _MEDIA_TYPES.get(representation_id)
    if expected_media is None or artifact.media_type != expected_media:
        raise VisualArtifactError("representation/media type is not visual")
    if artifact.object_id != object_id:
        raise VisualArtifactError("visual artifact belongs to another object")
    if (
        artifact.size_bytes != len(artifact.data)
        or artifact.sha256 != _digest(artifact.data)
    ):
        raise VisualArtifactError("visual artifact identity changed")

    if representation_id == "raw_video":
        if not 0 < artifact.size_bytes <= _MAX_N6_VIDEO_BYTES:
            raise VisualArtifactError("video exceeds the N6 direct-video bound")
        if not isinstance(video_frames_per_second, float) or not (
            0.1 <= video_frames_per_second <= 10.0
        ):
            raise VisualArtifactError("video frame rate is invalid")
        prompt = ContainerNodeRuntime.build_semantic_video_prompt(
            representation_id, question,
        )
        fields: dict[str, Any] = {
            "schema_version": CONTAINER_NODE_SEMANTIC_VIDEO_REQUEST_SCHEMA_VERSION,
            "execution_node_id": "N6",
            "representation_id": representation_id,
            "representation_sha256": artifact.sha256,
            "question": question,
            "prompt_sha256": _digest(prompt.encode("utf-8")),
            "video_media_type": artifact.media_type,
            "video_size_bytes": artifact.size_bytes,
            "video_sha256": artifact.sha256,
            "video_frames_per_second": video_frames_per_second,
            "video_base64": base64.b64encode(artifact.data).decode("ascii"),
        }
    else:
        bundle = validate_frame_bundle_bytes(
            artifact.data,
            expected_object_id=object_id,
            expected_sha256=artifact.sha256,
            expected_size_bytes=artifact.size_bytes,
            artifact_media_type=artifact.media_type,
        )
        frames = [
            {
                "frame_index": frame.frame_index,
                "timestamp_seconds": frame.timestamp_seconds,
                "width": frame.width,
                "height": frame.height,
                "jpeg_size_bytes": len(frame.jpeg_bytes),
                "jpeg_sha256": _digest(frame.jpeg_bytes),
                "jpeg_base64": base64.b64encode(frame.jpeg_bytes).decode("ascii"),
            }
            for frame in bundle.vision_frames()
        ]
        prompt = ContainerNodeRuntime.build_semantic_vision_prompt(
            representation_id, len(frames), question,
        )
        fields = {
            "schema_version": CONTAINER_NODE_SEMANTIC_VISION_REQUEST_SCHEMA_VERSION,
            "execution_node_id": "N6",
            "representation_id": representation_id,
            "representation_sha256": artifact.sha256,
            "question": question,
            "prompt_sha256": _digest(prompt.encode("utf-8")),
            "frame_sequence_sha256": semantic_frame_sequence_sha256(frames),
            "frames": frames,
        }
    request_id = uuid.uuid5(
        uuid.NAMESPACE_URL,
        "pathfinder-ppd-vision/v1:"
        + session_id + ":" + artifact.access_id + ":" + _digest(_canonical(fields)),
    )
    return {**fields, "semantic_request_id": str(request_id)}


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request: Any, fp: Any, code: int,
                         msg: str, headers: Any, newurl: str) -> None:
        return None


@dataclass(frozen=True)
class N6VisualInferenceClient:
    """A fixed-origin N6 client; no URL, token or media bytes reach the agent."""

    base_url: str
    bearer_token: str
    timeout_seconds: float = 180.0
    private_http_service_name: str | None = None

    def __post_init__(self) -> None:
        parsed = urlsplit(self.base_url)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname or parsed.username or parsed.password
            or parsed.path not in {"", "/"} or parsed.query or parsed.fragment
        ):
            raise ValueError("N6 origin must be a bare HTTP(S) origin")
        if (
            not self.bearer_token
            or not self.bearer_token.isascii()
            or any(not 33 <= ord(char) <= 126 for char in self.bearer_token)
            or not math.isfinite(self.timeout_seconds)
            or self.timeout_seconds <= 0
        ):
            raise ValueError("N6 credential and positive timeout are required")
        # Use the same explicit service-name HTTP policy as Data Agents.
        # A bare 10.x origin is not an authorization to send model inputs.
        DataAgentClientSettings(
            base_url=self.base_url,
            simulator_private_http_hosts=(
                () if self.private_http_service_name is None
                else (self.private_http_service_name,)
            ),
        )

    def infer(self, request: Mapping[str, Any]) -> dict[str, Any]:
        body = _canonical(request)
        opener = build_opener(ProxyHandler({}), _NoRedirect())
        wire = Request(
            self.base_url.rstrip("/") + "/v1/semantic/chat-completions",
            data=body,
            method="POST",
            headers={
                "Authorization": "Bearer " + self.bearer_token,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        try:
            with opener.open(wire, timeout=self.timeout_seconds) as response:
                if response.status != 200:
                    raise VisualArtifactError("N6 returned a non-200 status")
                raw = response.read(_MAX_RESPONSE_BYTES + 1)
        except (HTTPError, URLError, TimeoutError) as exc:
            # Do not include a response body, URL or credential in the error.
            raise VisualArtifactError("N6 visual inference did not complete") from exc
        if len(raw) > _MAX_RESPONSE_BYTES:
            raise VisualArtifactError("N6 result exceeded the response bound")
        try:
            result = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise VisualArtifactError("N6 result is not valid JSON") from exc
        if not isinstance(result, dict) or (
            result.get("status") != "completed"
            or result.get("semantic_request_id") != request["semantic_request_id"]
            or result.get("representation_sha256")
            != request["representation_sha256"]
            or result.get("request_sha256") != _digest(body)
            or result.get("llm_called") is not True
        ):
            raise VisualArtifactError("N6 result does not bind the visual request")
        answer = result.get("final_answer")
        if not isinstance(answer, str) or (
            result.get("final_answer_sha256") != _digest(answer.encode("utf-8"))
        ):
            raise VisualArtifactError("N6 answer digest does not match")
        return {
            "final_answer": answer,
            "final_answer_sha256": result["final_answer_sha256"],
            "semantic_request_id": result["semantic_request_id"],
            "semantic_input_kind": result.get("semantic_input_kind"),
            "representation_sha256": result["representation_sha256"],
            "representation_delivery_bytes": result.get(
                "representation_delivery_bytes"
            ),
            "service_time_ms": result.get("service_time_ms"),
            "provider_usage": result.get("provider_usage"),
            "model": result.get("model"),
            "credentials_recorded": False,
        }
