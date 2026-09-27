"""Offline tests for Pathfinder's derived-image Agent usage capture."""

import json
import hashlib
import asyncio
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from experiments.upcloud_ppd_20260925 import run_qwen_usage_probe
from integrations.flowmesh.patch_agent_executor_usage import patched_text
from integrations.flowmesh.pathfinder_agent_usage_capture import (
    capture_agent_usage,
    summarize_sdk_usage,
)
from integrations.flowmesh import pathfinder_agent_provider_attempts as attempts


TASK_ID = "tsk-5c4eaabc-adf8-4f26-91b5-2fa644721596"


def _usage() -> SimpleNamespace:
    entry = SimpleNamespace(
        input_tokens=120, output_tokens=15, total_tokens=135,
        input_tokens_details=SimpleNamespace(cached_tokens=20),
        prompt="private prompt", api_key="private credential",
    )
    return SimpleNamespace(
        requests=1, input_tokens=120, output_tokens=15,
        total_tokens=135,
        input_tokens_details=SimpleNamespace(cached_tokens=20),
        request_usage_entries=[entry],
    )


class AgentUsageCaptureTests(unittest.TestCase):
    def test_http_transport_attempts_are_task_bound_and_content_free(self):
        class FakeClient:
            async def send(self, request, *args, **kwargs):
                if request.url.host == "dashscope-intl.aliyuncs.com":
                    return SimpleNamespace(
                        status_code=200,
                        headers={"x-request-id": "provider-request-123"},
                        body="private answer",
                    )
                return SimpleNamespace(status_code=200, headers={})

        module = SimpleNamespace(AsyncClient=FakeClient)
        model_request = SimpleNamespace(
            method="POST",
            url=SimpleNamespace(
                host="dashscope-intl.aliyuncs.com",
                path="/compatible-mode/v1/chat/completions",
            ),
            body="private prompt",
            headers={"Authorization": "private credential"},
        )
        unrelated = SimpleNamespace(
            method="GET",
            url=SimpleNamespace(host="flowmesh.local", path="/healthz"),
        )
        with (patch.object(attempts, "_installed", False),
              patch.dict(sys.modules, {"httpx": module})):
            self.assertTrue(attempts.begin_httpx_attempt_capture(
                Path("/tmp") / TASK_ID
            ))
            async def run():
                client = FakeClient()
                await client.send(unrelated)
                await client.send(model_request)
            asyncio.run(run())
            rows = attempts.take_httpx_attempts()
            self.assertEqual(1, len(rows))
            self.assertEqual(200, rows[0]["http_status"])
            self.assertEqual(
                hashlib.sha256(b"provider-request-123").hexdigest(),
                rows[0]["request_id_sha256"],
            )
            self.assertNotIn("private", json.dumps(rows))
            self.assertNotIn("provider-request-123", json.dumps(rows))
            self.assertEqual([], attempts.take_httpx_attempts())

    def test_usage_wrapper_mounts_proven_v7_answer_parser(self):
        with tempfile.TemporaryDirectory() as root:
            base = Path(root) / "supervisor.py"
            base.write_text(
                "def main():\n"
                "    return int(not (WORKER == 'pathfinder_ppd_visual_20260927h'\n"
                "        and str(RUNNER).endswith('runner_v7.py')\n"
                "        and len(RUNNER_SHA256) == 64))\n",
                encoding="utf-8",
            )
            runner = Path(root) / "runner_v7.py"
            runner.write_text("# tested v7 runner\n", encoding="utf-8")
            base_hash = hashlib.sha256(base.read_bytes()).hexdigest()
            runner_hash = hashlib.sha256(runner.read_bytes()).hexdigest()
            argv = ["probe", "--worker-image", "sha256:" + "a" * 64,
                    "--worker-alias", "pathfinder_ppd_visual_20260927h"]
            with (patch.object(run_qwen_usage_probe, "BASE", base),
                  patch.object(run_qwen_usage_probe, "BASE_SHA256", base_hash),
                  patch.object(run_qwen_usage_probe, "RUNNER_V7", runner),
                  patch.object(run_qwen_usage_probe, "RUNNER_V7_SHA256",
                               runner_hash),
                  patch.object(run_qwen_usage_probe,
                               "_require_node_registry_healthy"),
                  patch.object(sys, "argv", argv)):
                self.assertEqual(0, run_qwen_usage_probe.main())

    def test_node_registry_readiness_is_bounded(self):
        response = MagicMock()
        response.__enter__.return_value.status = 200
        opener = MagicMock()
        opener.open.return_value = response
        with patch.object(run_qwen_usage_probe, "build_opener",
                          return_value=opener):
            run_qwen_usage_probe._require_node_registry_healthy()
        self.assertEqual(5, opener.open.call_args.kwargs["timeout"])

    def test_node_registry_timeout_blocks_submission(self):
        opener = MagicMock()
        opener.open.side_effect = TimeoutError("registry stalled")
        with patch.object(run_qwen_usage_probe, "build_opener",
                          return_value=opener):
            with self.assertRaisesRegex(RuntimeError, "not dispatch-ready"):
                run_qwen_usage_probe._require_node_registry_healthy()

    def test_numeric_only_and_per_request_reconciliation(self):
        result = SimpleNamespace(
            context_wrapper=SimpleNamespace(usage=_usage()),
            raw_responses=[SimpleNamespace(output="private answer")],
            final_output="private answer",
        )
        report = summarize_sdk_usage(result, "complete")
        self.assertEqual(120, report["aggregate"]["input_tokens"])
        self.assertEqual(20, report["aggregate"]["cached_input_tokens_sdk"])
        self.assertTrue(report["request_entries_reconciled"])
        self.assertFalse(report["provider_attempts_reconciled"])
        self.assertFalse(report["cached_input_provider_verified"])
        self.assertNotIn("private", json.dumps(report))

    def test_missing_and_incoherent_usage_fail_closed(self):
        missing = summarize_sdk_usage(None, "failed")
        self.assertFalse(missing["usage_present"])
        usage = _usage()
        usage.total_tokens = 999
        bad = summarize_sdk_usage(
            SimpleNamespace(context_wrapper=SimpleNamespace(usage=usage)),
            "complete",
        )
        self.assertFalse(bad["aggregate_units_coherent"])
        self.assertFalse(bad["request_entries_reconciled"])

    def test_exact_task_directory_receipt_is_private_and_never_overwritten(self):
        result = SimpleNamespace(
            context_wrapper=SimpleNamespace(usage=_usage()),
            raw_responses=[], final_output="private answer",
        )
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / TASK_ID
            capture_agent_usage(result, directory, "complete")
            capture_agent_usage(result, directory, "complete")
            paths = sorted((directory / "artifacts").glob(
                "pathfinder-agent-usage-*.json"
            ))
            self.assertEqual(2, len(paths))
            self.assertNotEqual(paths[0].name, paths[1].name)
            for path in paths:
                payload = path.read_text(encoding="utf-8")
                self.assertNotIn("private", payload)
                self.assertEqual(TASK_ID, json.loads(payload)["task_id"])

    def test_unbound_directory_is_not_written(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / "not-a-task"
            capture_agent_usage(None, directory, "failed")
            self.assertFalse(directory.exists())


class AgentExecutorPatchTests(unittest.TestCase):
    def test_patch_has_success_and_both_failure_capture_points(self):
        source = (
            "from .base_executor import ExecutionError, Executor, ExecutorTask\n"
            "            result_streaming = agent.run_streamed(input=task_input)\n"
            "            result = result_streaming\n"
            "            logger.info(\"✅ Execution completed\")\n"
            "        except TimeoutError:\n"
            "            logger.error(f\"⏰ Task timeout ({task_timeout} seconds)\")\n"
            "        except Exception as e:\n"
            "            logger.error(f\"❌ Error during execution: {str(e)}\")\n"
            "                    result_streaming = agent.run_streamed(input=task_input)\n"
        )
        patched = patched_text(source)
        self.assertEqual(4, patched.count("capture_agent_usage"))
        self.assertEqual(1, patched.count("begin_httpx_attempt_capture(out_dir)"))
        self.assertIn('out_dir, "complete"', patched)
        self.assertIn('out_dir, "timeout"', patched)
        self.assertIn('out_dir, "failed"', patched)

    def test_unknown_base_source_rejected(self):
        with self.assertRaisesRegex(ValueError, "anchor differs"):
            patched_text("unrelated FlowMesh revision")


if __name__ == "__main__":
    unittest.main()
