"""Fail-closed build-time overlay for the pinned FlowMesh Agent executor.

This patches only a derived Pathfinder worker image. The FlowMesh repository
and its published base image remain unchanged.
"""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path


EXPECTED_SHA256 = (
    "e2e5435b4329f4daea5b71d958cc237291bdf426a6e063c585f8579b73c4b6d3"
)
TARGET = Path("/app/worker/executors/agent_executor.py")


def patched_text(source: str) -> str:
    changes = (
        (
            "from .base_executor import ExecutionError, Executor, ExecutorTask\n",
            "from .base_executor import ExecutionError, Executor, ExecutorTask\n"
            "from .pathfinder_agent_usage_capture import capture_agent_usage\n"
            "from .pathfinder_agent_provider_attempts import "
            "begin_httpx_attempt_capture\n",
        ),
        (
            "            result_streaming = agent.run_streamed(input=task_input)\n",
            "            begin_httpx_attempt_capture(out_dir)\n"
            "            result_streaming = agent.run_streamed(input=task_input)\n",
        ),
        (
            "            result = result_streaming\n"
            "            logger.info(\"✅ Execution completed\")",
            "            result = result_streaming\n"
            "            capture_agent_usage(result_streaming, out_dir, \"complete\")\n"
            "            logger.info(\"✅ Execution completed\")",
        ),
        (
            "        except TimeoutError:\n"
            "            logger.error(f\"⏰ Task timeout ({task_timeout} seconds)\")",
            "        except TimeoutError:\n"
            "            capture_agent_usage(locals().get(\"result_streaming\"), "
            "out_dir, \"timeout\")\n"
            "            logger.error(f\"⏰ Task timeout ({task_timeout} seconds)\")",
        ),
        (
            "        except Exception as e:\n"
            "            logger.error(f\"❌ Error during execution: {str(e)}\")",
            "        except Exception as e:\n"
            "            capture_agent_usage(locals().get(\"result_streaming\"), "
            "out_dir, \"failed\")\n"
            "            logger.error(f\"❌ Error during execution: {str(e)}\")",
        ),
    )
    result = source
    for before, after in changes:
        expected = 2 if "result_streaming = agent.run_streamed" in before else 1
        if result.count(before) != expected:
            raise ValueError("pinned Agent executor patch anchor differs")
        result = result.replace(before, after, 1)
    return result


def main() -> int:
    if len(sys.argv) != 1:
        raise ValueError("patcher accepts no runtime arguments")
    payload = TARGET.read_bytes()
    if hashlib.sha256(payload).hexdigest() != EXPECTED_SHA256:
        raise ValueError("installed Agent executor digest differs")
    source = payload.decode("utf-8")
    if "\r" in source:
        raise ValueError("installed Agent executor line endings differ")
    target = patched_text(source).encode("utf-8")
    TARGET.write_bytes(target)
    print("PATHFINDER_AGENT_USAGE_OVERLAY_INSTALLED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
