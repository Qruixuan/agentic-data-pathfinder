"""No-network preview wiring for the existing verified public cohort."""

import json
from pathlib import Path
import tempfile
import unittest

import yaml

from pathfinder.cli import _parser
from pathfinder.integrations.flowmesh.route_action_bootstrap import (
    PREVIEW_SCHEMA, load_route_action_preview, require_preview_loopback,
)
from pathfinder.integrations.flowmesh.route_action_bridge import (
    RouteActionBridgeError,
)


ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "artifacts/nextqa-atphard-8x5-prep-input-20260925-v1/plan"
QUOTE = ROOT / "artifacts/route-action-quotes-t60-dev-20260927-v2-draft"


class RouteActionBootstrapTests(unittest.TestCase):
    def test_agent_requests_streamed_provider_usage(self):
        config = yaml.safe_load((
            ROOT / "integrations/flowmesh/agent_configs/"
            "pathfinder_route_action_qwen_first_offer.yaml"
        ).read_text(encoding="utf-8"))
        settings = config["model"]["model_settings"]
        self.assertIs(settings["include_usage"], True)
        self.assertEqual("list_route_offers", settings["tool_choice"])

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config_file = self.root / "preview.json"

    def _config(self):
        manifest = json.loads((QUOTE / "route-quotes.json").read_bytes())
        return {
            "schema_version": PREVIEW_SCHEMA,
            "preview_only": True,
            "credentials_recorded": False,
            "plan_dir": str(PLAN),
            "quote_package_dir": str(QUOTE),
            "plan_sha256": manifest["plan_sha256"],
            "source_trace_package_sha256": manifest[
                "source_trace_package_sha256"],
            "rate_card_sha256": manifest["rate_card_sha256"],
            "execution_namespace": "unit-preview-one",
            "choice_db": str(self.root / "choices.sqlite3"),
            "session_db": str(self.root / "sessions.sqlite3"),
        }

    def _load(self, config):
        self.config_file.write_text(
            json.dumps(config), encoding="utf-8",
        )
        return load_route_action_preview(self.config_file)

    @unittest.skipUnless(PLAN.is_dir() and QUOTE.is_dir(),
                         "local frozen development fixtures are absent")
    def test_real_quote_and_plan_bind_to_no_model_gateway(self):
        gateway = self._load(self._config())
        first = json.loads((PLAN / "public-questions.jsonl")
                           .read_text(encoding="utf-8").splitlines()[0])
        public = gateway.bridge.public_question(first["question_id"])
        gateway.register_session(
            session_id="preview-session", question_id=public["question_id"],
            physical_design_id="D_base", object_id=public["object_id"],
            public_task_sha256=public["public_task_sha256"],
        )
        listed = gateway.list_route_offers("preview-session")
        self.assertEqual(4, len(listed["offers"]))
        self.assertEqual(
            {"D0", "D2", "D4", "D6"},
            {offer["action_id"] for offer in listed["offers"]},
        )
        self.assertNotIn("run_id", str(listed))
        committed = gateway.commit_route_choice(
            "preview-session", "D0", listed["offer_set_sha256"],
        )
        self.assertEqual("COMMITTED", committed["status"])

    @unittest.skipUnless(PLAN.is_dir() and QUOTE.is_dir(),
                         "local frozen development fixtures are absent")
    def test_wrong_binding_and_frozen_package_write_are_rejected(self):
        config = self._config()
        config["plan_sha256"] = "a" * 64
        with self.assertRaises(RouteActionBridgeError):
            self._load(config)
        config = self._config()
        config["session_db"] = str(PLAN / "sessions.sqlite3")
        with self.assertRaisesRegex(RouteActionBridgeError, "frozen package"):
            self._load(config)

    def test_preview_is_opt_in_loopback_only(self):
        args = _parser().parse_args([
            "serve-flowmesh-tools", "--host", "127.0.0.1",
            "--route-action-preview-config", str(self.config_file),
        ])
        self.assertEqual(self.config_file, args.route_action_preview_config)
        require_preview_loopback(args.host)
        with self.assertRaisesRegex(RouteActionBridgeError, "loopback"):
            require_preview_loopback("0.0.0.0")


if __name__ == "__main__":
    unittest.main()
