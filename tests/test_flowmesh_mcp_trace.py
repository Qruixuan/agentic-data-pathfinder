"""No-network regression tests for opt-in Gateway tool boundary tracing."""

from __future__ import annotations

import hashlib
import io
import json
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from pathfinder.integrations.flowmesh.mcp_server import _invoke_traced_tool


class GatewayToolTraceTests(unittest.TestCase):
    def test_disabled_trace_preserves_return_without_output(self) -> None:
        output = io.StringIO()
        with patch.dict("os.environ", {"PATHFINDER_PPD_TOOL_TRACE": "0"}):
            with redirect_stdout(output):
                result = _invoke_traced_tool(
                    "list_offers", "session-1", lambda: {"private": "value"},
                )
        self.assertEqual({"private": "value"}, result)
        self.assertEqual("", output.getvalue())

    def test_enabled_trace_records_only_boundary_metadata(self) -> None:
        output = io.StringIO()
        with patch.dict("os.environ", {"PATHFINDER_PPD_TOOL_TRACE": "1"}):
            with redirect_stdout(output):
                result = _invoke_traced_tool(
                    "list_offers", "session-1", lambda: {"private": "value"},
                )
        self.assertEqual({"private": "value"}, result)
        rows = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(["started", "complete"], [row["status"] for row in rows])
        self.assertEqual(
            hashlib.sha256(b"session-1").hexdigest(),
            rows[0]["session_sha256"],
        )
        self.assertEqual({"list_offers"}, {row["tool"] for row in rows})
        self.assertNotIn("session-1", output.getvalue())
        self.assertNotIn("private", output.getvalue())
        self.assertNotIn("value", output.getvalue())

    def test_enabled_trace_preserves_exception_without_message(self) -> None:
        output = io.StringIO()

        def fail() -> dict[str, object]:
            raise ValueError("secret value must not be logged")

        with patch.dict("os.environ", {"PATHFINDER_PPD_TOOL_TRACE": "1"}):
            with redirect_stdout(output):
                with self.assertRaisesRegex(ValueError, "secret value"):
                    _invoke_traced_tool("list_offers", "session-1", fail)
        rows = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(["started", "error"], [row["status"] for row in rows])
        self.assertEqual("ValueError", rows[1]["error_class"])
        self.assertNotIn("secret", output.getvalue())


if __name__ == "__main__":
    unittest.main()
