"""No-network check of the derived worker's numeric capture overlay."""

from __future__ import annotations

import ast
import asyncio
from hashlib import sha256
import json
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace


EXECUTOR = Path("/app/worker/executors/agent_executor.py")
TASK_ID = "tsk-00000000-0000-4000-8000-000000000001"


def main() -> None:
    tree = ast.parse(EXECUTOR.read_text(encoding="utf-8"))
    calls = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "capture_agent_usage"
    ]
    imports = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        and node.module == "pathfinder_agent_usage_capture"
    ]
    starts = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "begin_httpx_attempt_capture"
    ]
    if len(calls) != 3 or len(imports) != 1 or len(starts) != 1:
        raise RuntimeError("Agent executor capture hooks differ")
    sys.path.insert(0, str(EXECUTOR.parents[2]))
    from worker.executors.pathfinder_agent_usage_capture import (
        capture_agent_usage,
    )
    from worker.executors.pathfinder_agent_provider_attempts import (
        begin_httpx_attempt_capture,
        take_httpx_attempts,
    )

    usage = SimpleNamespace(
        requests=1, input_tokens=11, output_tokens=2, total_tokens=13,
        input_tokens_details=SimpleNamespace(cached_tokens=0),
        request_usage_entries=[SimpleNamespace(
            input_tokens=11, output_tokens=2, total_tokens=13,
            input_tokens_details=SimpleNamespace(cached_tokens=0),
        )],
    )
    with tempfile.TemporaryDirectory() as root:
        directory = Path(root) / TASK_ID
        result = SimpleNamespace(
            context_wrapper=SimpleNamespace(usage=usage), raw_responses=[None],
            final_output="do-not-record-this-answer",
        )
        capture_agent_usage(result, directory, "complete")
        files = list((directory / "artifacts").glob(
            "pathfinder-agent-usage-*.json"
        ))
        if len(files) != 1:
            raise RuntimeError("numeric capture receipt was not written")
        receipt = json.loads(files[0].read_text(encoding="utf-8"))
        if (receipt["aggregate"]["input_tokens"] != 11
                or receipt["aggregate"]["output_tokens"] != 2
                or not receipt["request_entries_reconciled"]
                or "do-not-record-this-answer" in files[0].read_text()):
            raise RuntimeError("numeric capture receipt is invalid")
        import httpx

        async def probe_transport() -> list[dict[str, object]]:
            if not begin_httpx_attempt_capture(directory):
                raise RuntimeError("task-scoped HTTP attempt capture did not start")
            transport = httpx.MockTransport(lambda request: httpx.Response(
                200, headers={"x-request-id": "synthetic-request-id"},
            ))
            async with httpx.AsyncClient(transport=transport) as client:
                response = await client.post(
                    "https://dashscope-intl.aliyuncs.com/"
                    "compatible-mode/v1/chat/completions", json={},
                )
            if response.status_code != 200:
                raise RuntimeError("synthetic provider response differed")
            return take_httpx_attempts()

        attempts = asyncio.run(probe_transport())
        if (len(attempts) != 1
                or attempts[0]["http_status"] != 200
                or attempts[0]["request_id_sha256"] != sha256(
                    b"synthetic-request-id"
                ).hexdigest()
                or "synthetic-request-id" in json.dumps(attempts)):
            raise RuntimeError("HTTP attempt capture is not metadata-only")
    print("OFFLINE_AGENT_USAGE_CAPTURE_OK")


if __name__ == "__main__":
    main()
