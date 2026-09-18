"""Transport-size coverage for the real direct-encoded-video N6 request.

The direct-video request is roughly an order of magnitude larger than any
previous N6 request because it carries a base64 video.  These tests drive the
real N7 client against a real N6 HTTP server using a genuine NExT-QA MP4, so
a byte bound that would only fail during a cloud run is caught locally.

The outbound provider call is stubbed: this exercises Pathfinder transport,
not the external model.
"""

from __future__ import annotations

import base64
import hashlib
import json
import shutil
import tempfile
import threading
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from pathfinder.simulator.container_node import (
    CONTAINER_NODE_SEMANTIC_VIDEO_RESULT_SCHEMA_VERSION,
    create_container_node_server,
)
from pathfinder.simulator.full_flow_n6_adapters import (
    DIRECT_VIDEO_MEDIA_TYPE,
    N6ModelInputAdapter,
    decode_prepared_semantic_request,
)
from pathfinder.simulator.full_flow_route_adapters import (
    HttpContainerNodeSemanticClient,
)
from pathfinder.simulator.full_flow_semantic_input_profiles import (
    build_semantic_input_profile,
)
from pathfinder.simulator.full_flow_semantic_route_runtime import (
    ArtifactAccess,
    ArtifactIdentity,
)
from pathfinder.simulator.hidden_oracle import build_n1_public_task_binding

OBJECT_ID = "nextqa-val-4010069381"
CATALOG_VERSION = "catalog-v1"
TOKEN = "unit-test-node-token"
# The real routed object used by the representative experiment.  If it is not
# present in this checkout the test synthesizes a payload of the same size so
# the byte bounds are still exercised.
_REAL_VIDEO = Path(
    ".qruix_smoke_stage/artifacts/minimum-real-indexed-e47f3f0"
    "/n3-indexed-package/artifacts/nextqa-val-4010069381/raw_video.mp4"
)
_REAL_VIDEO_SIZE = 1_271_056


def _video_bytes() -> bytes:
    if _REAL_VIDEO.is_file():
        return _REAL_VIDEO.read_bytes()
    filler = b"\x00\x00\x00\x18ftypisom"
    return (filler * (_REAL_VIDEO_SIZE // len(filler) + 1))[:_REAL_VIDEO_SIZE]


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _public_task() -> dict[str, Any]:
    return build_n1_public_task_binding(
        workload_id="smoke-retrieval",
        object_id=OBJECT_ID,
        task_class_id="W3",
        question="Why did the person move the object?",
        answer_options=[
            {"option_id": "A", "text": "to clean underneath"},
            {"option_id": "B", "text": "to reach the switch"},
        ],
        success_scoring_rule="multiple-choice-option-id-canonical-match-v1",
    )


def _forbidden_sampler(*args: Any, **kwargs: Any):
    raise AssertionError("the direct-video path must never decode frames")


class DirectVideoTransportTest(unittest.TestCase):
    def setUp(self) -> None:
        self.video = _video_bytes()
        self.state = tempfile.mkdtemp(prefix="pf-direct-video-transport-")
        self.addCleanup(shutil.rmtree, self.state, True)

    def _prepared_request(self) -> dict[str, Any]:
        adapter = N6ModelInputAdapter(raw_sampler=_forbidden_sampler)
        profile = build_semantic_input_profile(
            route_family="raw",
            model_input_representation_ids=["raw_video"],
        )
        identity = ArtifactIdentity(
            object_id=OBJECT_ID,
            representation_id="raw_video",
            object_catalog_version=CATALOG_VERSION,
            artifact_size_bytes=len(self.video),
            artifact_sha256=_sha(self.video),
        )
        prepared = adapter.prepare(
            run_id="direct-video-transport-v1",
            trial={
                "trial_key": "scenario|W3|D0|r0000",
                "route_family": "raw",
                "artifact_object_id": OBJECT_ID,
                "semantic_input_profile": profile,
            },
            stage={"stage_key": "scenario|W3|D0|prepare"},
            public_task=_public_task(),
            mode="direct-video",
            artifacts=[ArtifactAccess(identity, self.video)],
        )
        return decode_prepared_semantic_request(prepared)

    def test_real_video_request_fits_every_declared_byte_bound(self) -> None:
        request = self._prepared_request()
        encoded = json.dumps(
            request, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
        self.assertEqual(len(self.video), request["video_size_bytes"])
        # Base64 inflates by ~4/3; the envelope must stay inside the N6
        # request cap and well inside the provider's documented guidance.
        self.assertLess(len(encoded), 10 * 1024 * 1024)
        self.assertLess(len(self.video), 6 * 1024 * 1024)
        self.assertGreater(len(encoded), len(self.video))

    def test_real_video_crosses_the_n7_to_n6_http_boundary(self) -> None:
        request = self._prepared_request()
        captured: dict[str, Any] = {}

        class _Response:
            def __enter__(self_inner):
                return self_inner

            def __exit__(self_inner, *args: Any) -> None:
                return None

            def read(self_inner, _limit: int) -> bytes:
                return json.dumps({
                    "model": "qwen3.8-27b",
                    "choices": [{"message": {"content": "B"}}],
                }).encode("utf-8")

        def _fake_opener(_base_url: str):
            class _Opener:
                def open(self_inner, outbound: Any, timeout: float) -> Any:
                    captured["body"] = json.loads(outbound.data.decode("utf-8"))
                    return _Response()

            return _Opener()

        server = create_container_node_server(
            "N6",
            self.state,
            enable_semantic_llm=True,
            semantic_bearer_token=TOKEN,
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        host, port = server.server_address[0], server.server_address[1]

        env = {
            "PATHFINDER_SEMANTIC_LLM_BASE_URL": "https://example.invalid/v1",
            "PATHFINDER_SEMANTIC_LLM_MODEL": "qwen3.8-27b",
            "PATHFINDER_SEMANTIC_LLM_API_KEY": "unit-test-placeholder",
        }
        client = HttpContainerNodeSemanticClient(
            base_url=f"http://{host}:{port}",
            bearer_token=TOKEN,
        )
        with mock.patch.dict("os.environ", env, clear=False), mock.patch(
            "pathfinder.simulator.container_node._semantic_llm_opener",
            _fake_opener,
        ):
            health = client.health()
            self.assertTrue(health["semantic_video_request_adapter_supported"])
            result = client.execute(request)

        self.assertEqual(
            CONTAINER_NODE_SEMANTIC_VIDEO_RESULT_SCHEMA_VERSION,
            result["schema_version"],
        )
        self.assertTrue(result["direct_video_input"])
        self.assertEqual("direct-encoded-video", result["semantic_input_kind"])
        self.assertEqual(_sha(self.video), result["video_sha256"])
        self.assertEqual(len(self.video), result["representation_delivery_bytes"])
        # The exact routed bytes reached the provider boundary intact.
        block = captured["body"]["messages"][0]["content"][0]
        self.assertEqual("video_url", block["type"])
        prefix = f"data:{DIRECT_VIDEO_MEDIA_TYPE};base64,"
        self.assertEqual(
            self.video,
            base64.b64decode(block["video_url"]["url"][len(prefix):], validate=True),
        )


if __name__ == "__main__":
    unittest.main()
