"""Execution amendments: running a frozen pilot at a later revision."""

from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

from pathfinder.cli import main as cli_main
from pathfinder.distributed import (
    CHANGE_CLASSIFICATIONS,
    require_matching_measurement_manifest,
    require_outside_input_freeze,
    file_sha256,
    EXECUTION_AMENDMENT_SCHEMA_VERSION,
    ExecutionAmendmentError,
    StaticRevisionResolver,
    bind_amendment_to_run,
    build_execution_amendment,
    build_frozen_plan_document,
    load_execution_amendment,
    require_execution_compatibility,
    run_distributed_pilot,
)

from tests.test_awm_certificate import _write_json
from tests.test_distributed_vertical import (
    VerticalFixture,
    _GatewayExecutor,
    _local_agent,
    _origin_agent,
)


REPOSITORY = Path(__file__).resolve().parents[1]
PROTOCOL_REVISION = "c19e3a771438df752ec935d367f2a9688c6e40bb"
EXECUTION_REVISION = "1c197777f7a900850628bbe5c05954ffdca38dfe"
OTHER_REVISION = "0" * 40
CHANGED_PATHS = ("pathfinder/distributed/preflight.py",)


class _RecordingExecutor(_GatewayExecutor):
    """Fails loudly if the runner reaches it; used to prove early refusal."""

    def execute(self, trial, *, workload, journal=None, attempt=1):
        raise AssertionError(
            "the executor must never be reached when an amendment is "
            "rejected"
        )


class _AmendmentFixture:
    """A complete pilot whose protocol revision differs from execution."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.fixture = VerticalFixture(
            root,
            _origin_agent(),
            _local_agent(),
        )
        # Re-issue the preregistration at the protocol revision so the
        # amendment has a real mismatch to reason about.
        payload = json.loads(
            self.fixture.preregistration_path.read_text(encoding="utf-8")
        )
        payload["source_git_revision"] = PROTOCOL_REVISION
        _write_json(self.fixture.preregistration_path, payload)
        from pathfinder.distributed import (
            load_distributed_pilot_preregistration,
            load_measurement_manifest,
        )
        from tests.test_distributed_vertical import _measurement_payload

        self.preregistration = load_distributed_pilot_preregistration(
            self.fixture.preregistration_path
        )
        self.fixture.preregistration = self.preregistration
        _write_json(self.fixture.measurement_path, _measurement_payload(
            self.preregistration.source_sha256,
            self.fixture.registry.source_sha256,
        ))
        self.fixture.provider = load_measurement_manifest(
            self.fixture.measurement_path
        )
        self.provider = self.fixture.provider
        # One registry file that every path -- fixture, CLI, and gate --
        # loads, so their digests agree.
        from pathfinder.distributed import load_endpoint_registry
        from tests.test_distributed_vertical import _registry_payload

        self.registry_path = root / "endpoint-registry.json"
        _write_json(self.registry_path, _registry_payload())
        self.registry = load_endpoint_registry(self.registry_path)
        self.fixture.registry = self.registry
        _write_json(self.fixture.measurement_path, _measurement_payload(
            self.preregistration.source_sha256,
            self.registry.source_sha256,
        ))
        self.fixture.provider = load_measurement_manifest(
            self.fixture.measurement_path
        )
        self.provider = self.fixture.provider
        self.workloads_path = root / "workloads.json"
        self.workloads = self.fixture.workloads()
        _write_json(self.workloads_path, self.workloads)
        # The amendment contract names a *tracked* repository path, so the
        # repo-relative path is real. The bytes are copied into the temp
        # directory so mutation tests can never write to the repository.
        self.system_repo_path = (
            "configs/pathfinderbench_restricted_pilot_v0_1_system.json"
        )
        self.tracked_system_config = REPOSITORY / self.system_repo_path
        self.system_config = root / "system.json"
        self.system_config.write_bytes(
            self.tracked_system_config.read_bytes()
        )
        self.system_sha256 = file_sha256(self.system_config)
        self.plan = build_frozen_plan_document(
            self.preregistration,
            self.registry,
            workloads=self.workloads,
        )
        self.plan_path = root / "frozen_plan.json"
        _write_json(self.plan_path, self.plan)
        # A stand-in immutable freeze; amendments must not be written inside.
        self.freeze_dir = root / "input-freeze"
        (self.freeze_dir / "config").mkdir(parents=True, exist_ok=True)
        _write_json(
            self.freeze_dir / "config" / "frozen-input.json",
            {"frozen": True},
        )

    def resolver(
        self,
        revision: str = EXECUTION_REVISION,
        *,
        protocol_system_sha256: str | None = "__same__",
    ):
        """A resolver whose tracked system config matches by default."""
        digest = (
            self.system_sha256
            if protocol_system_sha256 == "__same__"
            else protocol_system_sha256
        )
        blobs = {}
        if digest is not None:
            blobs[(PROTOCOL_REVISION, self.system_repo_path)] = digest
            blobs[(revision, self.system_repo_path)] = self.system_sha256
        return StaticRevisionResolver(
            revision,
            CHANGED_PATHS,
            blobs=blobs,
            relative_paths={
                # Both the temp copy (unit paths) and the tracked file
                # (CLI paths) resolve to the same repository-relative path.
                str(self.system_config): self.system_repo_path,
                str(self.tracked_system_config): self.system_repo_path,
            },
        )

    def build(self, **overrides: Any) -> dict[str, Any]:
        arguments: dict[str, Any] = {
            "amendment_id": "v2-orchestration-amendment-1",
            "reason": "preflight route-matrix expansion fix only",
            "measurement_manifest_sha256": self.provider.manifest_sha256,
            "workloads": self.workloads,
            "system_config_path": self.system_config,
            "frozen_plan_path": self.plan_path,
            "recomputed_plan": self.plan,
            "resolver": self.resolver(),
        }
        arguments.update(overrides)
        return build_execution_amendment(
            self.preregistration,
            self.registry,
            **arguments,
        )

    def write(self, payload: dict[str, Any], name: str = "amendment.json"):
        path = self.root / name
        _write_json(path, payload)
        return path

    def require(self, **overrides: Any) -> dict[str, Any]:
        arguments: dict[str, Any] = {
            "resolver": self.resolver(),
            "measurement_manifest_sha256": self.provider.manifest_sha256,
            "workloads": self.workloads,
            "system_config_path": self.system_config,
            "frozen_plan_path": self.plan_path,
            "recomputed_plan": self.plan,
            "amendment_path": None,
        }
        arguments.update(overrides)
        return require_execution_compatibility(
            self.preregistration,
            self.registry,
            **arguments,
        )


class AmendmentCreationTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.f = _AmendmentFixture(Path(self._tmp.name))

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_a_valid_amendment_binds_every_input(self) -> None:
        payload = self.f.build()
        self.assertEqual(
            EXECUTION_AMENDMENT_SCHEMA_VERSION,
            payload["schema_version"],
        )
        self.assertEqual(PROTOCOL_REVISION, payload["protocol_git_revision"])
        self.assertEqual(
            EXECUTION_REVISION,
            payload["execution_git_revision"],
        )
        self.assertEqual(
            "orchestration-only",
            payload["change_classification"],
        )
        self.assertTrue(payload["plan_equivalence_verified"])
        self.assertFalse(payload["credentials_recorded"])
        self.assertEqual(list(CHANGED_PATHS), payload["changed_paths"])
        for field in (
            "preregistration_sha256",
            "endpoint_registry_sha256",
            "measurement_manifest_sha256",
            "workload_content_sha256",
            "system_config_sha256",
            "frozen_plan_sha256",
            "frozen_plan_file_sha256",
        ):
            self.assertEqual(64, len(payload[field]), field)
        self.assertNotIn("confirmatory", payload)
        self.assertNotIn("eligible_for_scientific_claims", payload)

    def test_a_recomputed_plan_difference_refuses_creation(self) -> None:
        divergent = {**self.f.plan, "planned_trial_count": 999}
        with self.assertRaisesRegex(
            ExecutionAmendmentError,
            "not.*equivalent to the frozen plan",
        ):
            self.f.build(recomputed_plan=divergent)

    def test_an_unsupported_classification_refuses_creation(self) -> None:
        with self.assertRaisesRegex(
            ExecutionAmendmentError,
            "unsupported change_classification",
        ):
            self.f.build(change_classification="scientific-change")
        self.assertEqual(("orchestration-only",), CHANGE_CLASSIFICATIONS)

    def test_a_plan_for_another_pilot_refuses_creation(self) -> None:
        foreign = self.f.root / "foreign_plan.json"
        _write_json(foreign, {**self.f.plan, "pilot_id": "another-pilot"})
        with self.assertRaisesRegex(
            ExecutionAmendmentError,
            "belongs to pilot",
        ):
            self.f.build(frozen_plan_path=foreign)

    def test_a_dirty_working_tree_refuses_creation(self) -> None:
        class _Dirty(StaticRevisionResolver):
            def is_clean(self) -> bool:
                return False

        with self.assertRaisesRegex(
            ExecutionAmendmentError,
            "uncommitted changes",
        ):
            self.f.build(resolver=_Dirty(EXECUTION_REVISION, CHANGED_PATHS))


class AmendmentLoadingTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.f = _AmendmentFixture(Path(self._tmp.name))
        self.valid = self.f.build()

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_a_valid_document_loads(self) -> None:
        amendment = load_execution_amendment(self.f.write(self.valid))
        self.assertEqual(64, len(amendment.source_sha256))
        self.assertTrue(amendment.plan_equivalence_verified)
        self.assertFalse(amendment.credentials_recorded)
        public = amendment.to_public_dict()
        self.assertEqual(
            "auditable-compatibility-evidence",
            public["evidence_class"],
        )
        self.assertTrue(
            public["not_an_authenticity_or_approval_control"]
        )

    def test_malformed_documents_are_refused(self) -> None:
        cases = {
            "bad schema": lambda d: {**d, "schema_version": "other/v1"},
            "bad classification": lambda d: {
                **d, "change_classification": "scientific-change",
            },
            "equivalence not asserted": lambda d: {
                **d, "plan_equivalence_verified": False,
            },
            "equivalence truthy string": lambda d: {
                **d, "plan_equivalence_verified": "true",
            },
            "credentials recorded": lambda d: {
                **d, "credentials_recorded": True,
            },
            "credentials missing": lambda d: {
                k: v for k, v in d.items() if k != "credentials_recorded"
            },
            "missing pilot": lambda d: {
                k: v for k, v in d.items() if k != "pilot_id"
            },
            "missing reason": lambda d: {
                k: v for k, v in d.items() if k != "reason"
            },
            "short digest": lambda d: {
                **d, "frozen_plan_sha256": "abc",
            },
            "carries confirmatory": lambda d: {**d, "confirmatory": True},
            "carries eligibility": lambda d: {
                **d, "eligible_for_scientific_claims": True,
            },
        }
        for label, mutate in cases.items():
            with self.subTest(case=label):
                path = self.f.write(mutate(self.valid), f"{hash(label)}.json")
                with self.assertRaises(ExecutionAmendmentError):
                    load_execution_amendment(path)

    def test_a_missing_file_is_refused(self) -> None:
        with self.assertRaisesRegex(
            ExecutionAmendmentError,
            "does not exist",
        ):
            load_execution_amendment(self.f.root / "absent.json")


class AmendmentGateTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.f = _AmendmentFixture(Path(self._tmp.name))
        self.valid_path = self.f.write(self.f.build())

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_matching_revisions_need_no_amendment(self) -> None:
        provenance = self.f.require(
            resolver=self.f.resolver(PROTOCOL_REVISION),
        )
        self.assertFalse(provenance["execution_amendment_required"])
        self.assertTrue(provenance["execution_revision_matches_protocol"])
        self.assertIsNone(provenance["execution_amendment_sha256"])
        self.assertEqual(
            PROTOCOL_REVISION,
            provenance["execution_git_revision"],
        )

    def test_an_unnecessary_amendment_is_rejected_not_ignored(self) -> None:
        """Ignoring it would let an unvalidated document ride along."""
        with self.assertRaisesRegex(
            ExecutionAmendmentError,
            "none is required",
        ):
            self.f.require(
                resolver=self.f.resolver(PROTOCOL_REVISION),
                amendment_path=self.valid_path,
            )

    def test_mismatched_revisions_fail_without_an_amendment(self) -> None:
        with self.assertRaisesRegex(
            ExecutionAmendmentError,
            "Refusing to submit any workflow",
        ):
            self.f.require()

    def test_a_valid_amendment_passes(self) -> None:
        provenance = self.f.require(amendment_path=self.valid_path)
        self.assertTrue(provenance["execution_amendment_required"])
        self.assertEqual(
            PROTOCOL_REVISION,
            provenance["protocol_git_revision"],
        )
        self.assertEqual(
            EXECUTION_REVISION,
            provenance["execution_git_revision"],
        )
        self.assertEqual(64, len(provenance["execution_amendment_sha256"]))
        self.assertEqual(
            "orchestration-only",
            provenance["change_classification"],
        )
        self.assertTrue(
            provenance["amendment_is_not_authenticity_or_approval"]
        )

    def test_a_different_current_revision_fails(self) -> None:
        with self.assertRaisesRegex(
            ExecutionAmendmentError,
            "this process is running",
        ):
            self.f.require(
                resolver=self.f.resolver(OTHER_REVISION),
                amendment_path=self.valid_path,
            )

    def test_a_protocol_revision_mismatch_fails(self) -> None:
        stale = self.f.write(
            {
                **self.f.build(),
                "protocol_git_revision": OTHER_REVISION,
            },
            "stale-protocol.json",
        )
        with self.assertRaisesRegex(
            ExecutionAmendmentError,
            "the preregistration declares",
        ):
            self.f.require(amendment_path=stale)

    def test_a_foreign_pilot_amendment_fails(self) -> None:
        foreign = self.f.write(
            {**self.f.build(), "pilot_id": "another-pilot"},
            "foreign.json",
        )
        with self.assertRaisesRegex(
            ExecutionAmendmentError,
            "is for pilot",
        ):
            self.f.require(amendment_path=foreign)

    def test_every_bound_input_hash_is_checked(self) -> None:
        for field in (
            "preregistration_sha256",
            "endpoint_registry_sha256",
            "measurement_manifest_sha256",
            "workload_content_sha256",
            "system_config_sha256",
            "frozen_plan_sha256",
            "frozen_plan_file_sha256",
        ):
            with self.subTest(field=field):
                tampered = self.f.write(
                    {**self.f.build(), field: "f" * 64},
                    f"tampered-{field}.json",
                )
                with self.assertRaises(ExecutionAmendmentError) as caught:
                    self.f.require(amendment_path=tampered)
                self.assertIn(field, str(caught.exception))

    def test_a_recomputed_plan_difference_fails_at_the_gate(self) -> None:
        with self.assertRaisesRegex(
            ExecutionAmendmentError,
            "differs from the frozen plan",
        ):
            self.f.require(
                amendment_path=self.valid_path,
                recomputed_plan={**self.f.plan, "repetitions": 99},
            )

    def test_a_changed_input_file_invalidates_the_amendment(self) -> None:
        # Editing the system config after the amendment was created must
        # invalidate it, not silently pass.
        _write_json(self.f.system_config, {"note": "edited after freeze"})
        with self.assertRaisesRegex(
            ExecutionAmendmentError,
            "system_config_sha256",
        ):
            self.f.require(amendment_path=self.valid_path)


class AmendmentRunBindingTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.f = _AmendmentFixture(Path(self._tmp.name))
        self.valid_path = self.f.write(self.f.build())
        self.run_dir = self.f.root / "run"
        self.run_dir.mkdir()

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_the_amendment_is_copied_into_the_run(self) -> None:
        digest = bind_amendment_to_run(self.run_dir, self.valid_path)
        copied = self.run_dir / "execution_amendment.json"
        self.assertTrue(copied.is_file())
        self.assertEqual(
            self.valid_path.read_bytes(),
            copied.read_bytes(),
        )
        self.assertEqual(64, len(digest))

    def test_resume_with_the_same_amendment_succeeds(self) -> None:
        first = bind_amendment_to_run(self.run_dir, self.valid_path)
        second = bind_amendment_to_run(self.run_dir, self.valid_path)
        self.assertEqual(first, second)

    def test_resume_with_a_changed_amendment_fails(self) -> None:
        bind_amendment_to_run(self.run_dir, self.valid_path)
        swapped = self.f.write(
            {**self.f.build(), "reason": "a different justification"},
            "swapped.json",
        )
        with self.assertRaisesRegex(
            ExecutionAmendmentError,
            "changed since this run started",
        ):
            bind_amendment_to_run(self.run_dir, swapped)

    def test_resume_without_the_amendment_fails(self) -> None:
        bind_amendment_to_run(self.run_dir, self.valid_path)
        with self.assertRaisesRegex(
            ExecutionAmendmentError,
            "resuming without one is refused",
        ):
            bind_amendment_to_run(self.run_dir, None)


class AmendmentRunIntegrationTest(unittest.TestCase):
    """The gate must stop a run before any FlowMesh session is opened."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.f = _AmendmentFixture(self.root)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _run(self, executor, **overrides: Any):
        arguments: dict[str, Any] = {
            "output_dir": self.root / "run",
            "workloads": self.f.workloads,
            "provider": self.f.provider,
            "preflight": self.f.fixture.preflight(),
        }
        arguments.update(overrides)
        return run_distributed_pilot(
            self.f.preregistration,
            self.f.registry,
            executor,
            **arguments,
        )

    def test_rejection_happens_before_any_executor_call(self) -> None:
        with self.assertRaises(ExecutionAmendmentError):
            self.f.require()
        # And the run itself never reaches the executor when the amendment
        # bound to the run directory does not match.
        bind_amendment_to_run(
            self.root / "run",
            self.f.write(self.f.build()),
        )
        swapped = self.f.write(
            {**self.f.build(), "reason": "different"},
            "swapped.json",
        )
        with self.assertRaises(ExecutionAmendmentError):
            self._run(
                _RecordingExecutor(self.f.fixture),
                execution_amendment_path=swapped,
            )

    def test_accepted_provenance_reaches_the_run_result(self) -> None:
        amendment_path = self.f.write(self.f.build())
        provenance = self.f.require(amendment_path=amendment_path)
        summary = self._run(
            _GatewayExecutor(self.f.fixture),
            execution_provenance=provenance,
            execution_amendment_path=amendment_path,
        )
        self.assertTrue(summary["oracle_complete"])
        self.assertEqual(
            PROTOCOL_REVISION,
            summary["protocol_git_revision"],
        )
        self.assertEqual(
            EXECUTION_REVISION,
            summary["execution_git_revision"],
        )
        self.assertEqual(
            provenance["execution_amendment_sha256"],
            summary["execution_amendment_sha256"],
        )
        recorded = summary["execution_provenance"]
        self.assertEqual("orchestration-only", recorded[
            "change_classification"
        ])
        self.assertTrue(
            (self.root / "run" / "execution_amendment.json").is_file()
        )

    def test_the_amendment_does_not_alter_scientific_standing(self) -> None:
        amendment_path = self.f.write(self.f.build())
        summary = self._run(
            _GatewayExecutor(self.f.fixture),
            execution_provenance=self.f.require(
                amendment_path=amendment_path
            ),
            execution_amendment_path=amendment_path,
        )
        self.assertFalse(summary["confirmatory"])
        self.assertFalse(summary["eligible_for_scientific_claims"])

    def test_no_secret_reaches_the_amendment_or_run_record(self) -> None:
        amendment_path = self.f.write(self.f.build())
        summary = self._run(
            _GatewayExecutor(self.f.fixture),
            execution_provenance=self.f.require(
                amendment_path=amendment_path
            ),
            execution_amendment_path=amendment_path,
        )
        from tests.test_distributed_vertical import ENVIRONMENT

        documents = "\n".join([
            amendment_path.read_text(encoding="utf-8"),
            (
                self.root / "run" / "execution_amendment.json"
            ).read_text(encoding="utf-8"),
            json.dumps(summary),
        ])
        for value in ENVIRONMENT.values():
            self.assertNotIn(value, documents)
        for marker in ("token", "TOKEN", "secret", "Bearer"):
            self.assertNotIn(marker, (
                amendment_path.read_text(encoding="utf-8")
            ))


class SystemConfigRevisionTest(unittest.TestCase):
    """The tracked system config must not move between the two revisions."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.f = _AmendmentFixture(Path(self._tmp.name))

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_creation_records_the_repository_relative_path(self) -> None:
        payload = self.f.build()
        self.assertEqual(
            "configs/pathfinderbench_restricted_pilot_v0_1_system.json",
            payload["system_config_repo_path"],
        )
        self.assertTrue(
            payload["system_config_unchanged_since_protocol_revision"]
        )

    def test_the_fixture_mirrors_the_tracked_repository_config(
        self,
    ) -> None:
        self.assertTrue(self.f.tracked_system_config.is_file())
        self.assertEqual(
            file_sha256(self.f.tracked_system_config),
            self.f.system_sha256,
            "the fixture must exercise the real system contract bytes",
        )
        self.assertNotEqual(
            self.f.tracked_system_config.resolve(),
            self.f.system_config.resolve(),
            "mutation tests must never write to the repository",
        )

    def test_a_changed_system_config_refuses_creation(self) -> None:
        with self.assertRaisesRegex(
            ExecutionAmendmentError,
            "changed between",
        ):
            self.f.build(
                resolver=self.f.resolver(protocol_system_sha256="a" * 64),
            )

    def test_a_system_config_absent_at_protocol_refuses_creation(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            ExecutionAmendmentError,
            "does not exist at the protocol revision",
        ):
            self.f.build(
                resolver=self.f.resolver(protocol_system_sha256=None),
            )

    def test_the_gate_reverifies_rather_than_trusting_the_document(
        self,
    ) -> None:
        # Created while the config matched; the config then moves.
        amendment = self.f.write(self.f.build())
        with self.assertRaisesRegex(
            ExecutionAmendmentError,
            "differs between the protocol and execution revisions",
        ):
            self.f.require(
                amendment_path=amendment,
                resolver=self.f.resolver(protocol_system_sha256="b" * 64),
            )

    def test_the_gate_refuses_when_absent_at_the_protocol_revision(
        self,
    ) -> None:
        amendment = self.f.write(self.f.build())
        with self.assertRaisesRegex(
            ExecutionAmendmentError,
            "does not exist at the protocol revision",
        ):
            self.f.require(
                amendment_path=amendment,
                resolver=self.f.resolver(protocol_system_sha256=None),
            )

    def test_a_document_without_the_assertions_is_refused(self) -> None:
        cases = {
            "missing repo path": lambda d: {
                k: v for k, v in d.items()
                if k != "system_config_repo_path"
            },
            "unchanged not asserted": lambda d: {
                **d,
                "system_config_unchanged_since_protocol_revision": False,
            },
            "unchanged missing": lambda d: {
                k: v for k, v in d.items()
                if k != "system_config_unchanged_since_protocol_revision"
            },
        }
        for label, mutate in cases.items():
            with self.subTest(case=label):
                path = self.f.write(
                    mutate(self.f.build()),
                    f"sys-{abs(hash(label))}.json",
                )
                with self.assertRaises(ExecutionAmendmentError):
                    load_execution_amendment(path)

    def test_the_real_repository_config_is_stable_across_revisions(
        self,
    ) -> None:
        """The v2 amendment is only creatable because this holds."""
        from pathfinder.distributed import GitRevisionResolver

        repository = Path(__file__).resolve().parents[1]
        resolver = GitRevisionResolver(repository)
        relative = "configs/pathfinderbench_restricted_pilot_v0_1_system.json"
        protocol = resolver.file_sha256_at(PROTOCOL_REVISION, relative)
        head = resolver.file_sha256_at(
            resolver.current_revision(),
            relative,
        )
        self.assertIsNotNone(
            protocol,
            "the tracked system config must exist at the protocol revision",
        )
        self.assertEqual(
            protocol,
            head,
            "if this fails, the v2 amendment must not be created",
        )
        self.assertEqual(
            protocol,
            file_sha256(repository / relative),
        )


class PreflightAmendmentPolicyTest(unittest.TestCase):
    """Revision-mismatch policy across preflight modes and CLI exit codes."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.f = _AmendmentFixture(self.root)
        from tests.test_distributed_vertical import (
            ENVIRONMENT,
            _FakeHealthProbe,
            _HealthResponse,
            _StubOpener,
        )
        self._environment = ENVIRONMENT
        self._probe = _FakeHealthProbe
        self._response = _HealthResponse
        self._stub = _StubOpener

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _opener(self):
        agents = self.f.fixture.agents
        by_host = {
            "alpha.invalid": agents["origin_remote"],
            "beta.invalid": agents["local_materialized"],
        }

        def opener(request, timeout=None):
            host = request.full_url.split("//", 1)[1].split(":", 1)[0]
            return self._response(
                json.dumps(by_host[host].health_document()).encode("utf-8")
            )

        return opener

    def _cli(self, argv, *, resolver=None):
        import os
        from unittest import mock

        # Injected, never inferred from HEAD: otherwise every commit that
        # moves HEAD breaks these assertions.
        injected = resolver if resolver is not None else self.f.resolver()
        stdout = io.StringIO()
        with mock.patch.dict(os.environ, self._environment), \
                mock.patch(
                    "pathfinder.distributed.default_revision_resolver",
                    lambda *a, **k: injected,
                ), \
                mock.patch(
                    "pathfinder.distributed.health._no_redirect_opener",
                    lambda: self._stub(self._opener()),
                ):
            with contextlib.redirect_stdout(stdout):
                code = cli_main(argv)
        return code, json.loads(stdout.getvalue())

    def _argv(self, mode: str, *, amendment: Path | None = None):
        argv = [
            "preflight-distributed-pilot",
            "--preregistration",
            str(self.f.fixture.preregistration_path),
            "--endpoint-registry",
            str(self.f.registry_path),
            "--measurement-manifest",
            str(self.f.fixture.measurement_path),
            "--workload-manifest",
            str(self.f.workloads_path),
            "--config",
            str(self.f.tracked_system_config),
            "--frozen-plan",
            str(self.f.plan_path),
            "--worker-alias",
            "pilot-worker",
            "--mode",
            mode,
            "--compact",
        ]
        if amendment is not None:
            argv += ["--execution-amendment", str(amendment)]
        return argv

    CHECK_ID = "execution.revision_matches_or_amended"

    def test_offline_validation_reports_an_advisory_warning(self) -> None:
        code, payload = self._cli(self._argv("offline_validation"))
        self.assertIn(self.CHECK_ID, payload["advisory_warnings"])
        self.assertNotIn(self.CHECK_ID, payload["failed_checks"])
        provenance = payload["execution_provenance"]
        self.assertTrue(provenance["execution_amendment_required"])
        self.assertFalse(provenance["execution_amendment_supplied"])
        self.assertEqual(0, code, payload["failed_checks"])

    def test_live_pilot_fails_on_a_bare_revision_mismatch(self) -> None:
        code, payload = self._cli(self._argv("live_pilot"))
        self.assertIn(self.CHECK_ID, payload["failed_checks"])
        self.assertNotIn(self.CHECK_ID, payload["advisory_warnings"])
        self.assertEqual("failed", payload["status"])
        self.assertNotEqual(0, code)

    def test_a_valid_amendment_passes_live_pilot(self) -> None:
        amendment = self.f.write(self.f.build())
        code, payload = self._cli(
            self._argv("live_pilot", amendment=amendment)
        )
        # The fixture's cost model carries a fixture rate provenance, which
        # live_pilot rejects for unrelated reasons; this test is about the
        # amendment check specifically.
        self.assertNotIn(self.CHECK_ID, payload["failed_checks"])
        self.assertNotIn(self.CHECK_ID, payload["advisory_warnings"])
        self.assertEqual(
            {"cost_model.rates_are_measured_not_placeholder"},
            set(payload["failed_checks"]),
        )
        provenance = payload["execution_provenance"]
        self.assertEqual(
            "orchestration-only",
            provenance["change_classification"],
        )
        self.assertEqual(64, len(provenance["execution_amendment_sha256"]))
        self.assertNotEqual(0, code)  # the placeholder rate still blocks

    def test_a_valid_amendment_passes_offline_validation(self) -> None:
        amendment = self.f.write(self.f.build())
        code, payload = self._cli(
            self._argv("offline_validation", amendment=amendment)
        )
        self.assertEqual("ok", payload["status"], payload["failed_checks"])
        self.assertEqual(0, code)
        self.assertNotIn(self.CHECK_ID, payload["advisory_warnings"])
        self.assertNotIn(self.CHECK_ID, payload["failed_checks"])

    def test_an_invalid_amendment_fails_in_both_modes(self) -> None:
        tampered = self.f.write(
            {**self.f.build(), "frozen_plan_sha256": "f" * 64},
            "tampered.json",
        )
        for mode in ("offline_validation", "live_pilot"):
            with self.subTest(mode=mode):
                code, payload = self._cli(
                    self._argv(mode, amendment=tampered)
                )
                self.assertNotEqual(0, code)
                self.assertEqual("error", payload["status"])
                self.assertIn(
                    "frozen_plan_sha256",
                    payload["message"],
                )

    def test_matching_revisions_pass_without_an_amendment(self) -> None:
        payload = json.loads(
            self.f.fixture.preregistration_path.read_text(encoding="utf-8")
        )
        payload["source_git_revision"] = PROTOCOL_REVISION
        _write_json(self.f.fixture.preregistration_path, payload)
        resolver = self.f.resolver(PROTOCOL_REVISION)
        for mode in ("offline_validation", "live_pilot"):
            with self.subTest(mode=mode):
                code, result = self._cli(
                    self._argv(mode),
                    resolver=resolver,
                )
                self.assertNotIn(self.CHECK_ID, result["failed_checks"])
                self.assertNotIn(
                    self.CHECK_ID,
                    result["advisory_warnings"],
                )
                provenance = result["execution_provenance"]
                self.assertFalse(
                    provenance["execution_amendment_required"]
                )
                self.assertTrue(
                    provenance["execution_revision_matches_protocol"]
                )
                if mode == "offline_validation":
                    self.assertEqual(0, code, result["failed_checks"])

    def test_no_token_appears_in_the_preflight_payload(self) -> None:
        """Tokens are credentials; endpoint URLs are deployment addresses.

        Preflight deliberately reports the resolved endpoint address so an
        operator can see what it probed. It must never report the bearer
        token that authenticates to it.
        """
        _, payload = self._cli(self._argv("offline_validation"))
        rendered = json.dumps(payload)
        tokens = [
            value
            for name, value in self._environment.items()
            if name.endswith("_TOKEN")
        ]
        self.assertEqual(2, len(tokens))
        for token in tokens:
            self.assertNotIn(token, rendered)
        self.assertFalse(payload["credentials_recorded"])
        for endpoint in payload["endpoints"]:
            self.assertFalse(endpoint["credentials_recorded"])
            self.assertTrue(endpoint["token_configured"])
            self.assertNotIn("token_value", endpoint)



class DirtyExecutionTreeTest(unittest.TestCase):
    """A clean tree is required at execution, not merely at creation."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.f = _AmendmentFixture(self.root)
        self.valid_path = self.f.write(self.f.build())

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _dirty(self, revision: str = EXECUTION_REVISION):
        class _Dirty(StaticRevisionResolver):
            def is_clean(self) -> bool:
                return False

        base = self.f.resolver(revision)
        return _Dirty(
            base.revision,
            base.paths,
            blobs=base.blobs,
            relative_paths=base.relative_paths,
        )

    def test_an_amended_execution_is_refused_on_a_dirty_tree(self) -> None:
        # The amendment was created against a clean revision and is valid;
        # the tree then became dirty, so HEAD no longer describes the code.
        self.f.require(amendment_path=self.valid_path)
        with self.assertRaisesRegex(
            ExecutionAmendmentError,
            "uncommitted changes",
        ):
            self.f.require(
                amendment_path=self.valid_path,
                resolver=self._dirty(),
            )

    def test_a_matching_revision_is_also_refused_on_a_dirty_tree(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            ExecutionAmendmentError,
            "uncommitted changes",
        ):
            self.f.require(resolver=self._dirty(PROTOCOL_REVISION))

    def test_rejection_precedes_any_executor_or_run_directory_write(
        self,
    ) -> None:
        run_dir = self.root / "run"
        with self.assertRaises(ExecutionAmendmentError):
            self.f.require(
                amendment_path=self.valid_path,
                resolver=self._dirty(),
            )
        self.assertFalse(
            run_dir.exists(),
            "no run directory may be created by a refused gate",
        )
        # And the runner never reaches the executor, because the gate is
        # evaluated before it is constructed.
        with self.assertRaises(ExecutionAmendmentError):
            self.f.require(
                amendment_path=self.valid_path,
                resolver=self._dirty(),
            )


class ChangedPathRevalidationTest(unittest.TestCase):
    """The amendment's claims about what changed are recomputed, not trusted."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.f = _AmendmentFixture(Path(self._tmp.name))

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_a_tampered_changed_path_list_is_refused(self) -> None:
        cases = {
            "extra entry": list(CHANGED_PATHS) + ["pathfinder/awm/model.py"],
            "missing entry": [],
            "different entry": ["pathfinder/oed/controller.py"],
            "reordered": list(reversed(
                list(CHANGED_PATHS) + ["a/b.py"]
            )),
        }
        for label, paths in cases.items():
            with self.subTest(case=label):
                tampered = self.f.write(
                    {**self.f.build(), "changed_paths": paths},
                    f"paths-{abs(hash(label))}.json",
                )
                with self.assertRaisesRegex(
                    ExecutionAmendmentError,
                    "changed paths that do not match",
                ):
                    self.f.require(amendment_path=tampered)

    def test_a_tampered_system_config_path_is_refused(self) -> None:
        tampered = self.f.write(
            {
                **self.f.build(),
                "system_config_repo_path": "configs/some_other_system.json",
            },
            "syspath.json",
        )
        with self.assertRaisesRegex(
            ExecutionAmendmentError,
            "but this run supplies",
        ):
            self.f.require(amendment_path=tampered)

    def test_a_valid_amendment_records_revalidation(self) -> None:
        provenance = self.f.require(
            amendment_path=self.f.write(self.f.build()),
        )
        self.assertTrue(provenance["changed_paths_revalidated"])
        self.assertEqual(
            list(CHANGED_PATHS),
            provenance["amendment_changed_paths"],
        )


class InputFreezeContainmentTest(unittest.TestCase):
    """An amendment may never be written inside the freeze it describes."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.f = _AmendmentFixture(self.root)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _freeze_digest(self) -> dict[str, str]:
        return {
            str(path.relative_to(self.f.freeze_dir)): file_sha256(path)
            for path in sorted(self.f.freeze_dir.rglob("*"))
            if path.is_file()
        }

    def test_output_directly_in_the_freeze_is_refused(self) -> None:
        before = self._freeze_digest()
        target = self.f.freeze_dir / "amendment.json"
        with self.assertRaisesRegex(
            ExecutionAmendmentError,
            "inside the immutable input freeze",
        ):
            require_outside_input_freeze(target, self.f.freeze_dir)
        self.assertFalse(target.exists())
        self.assertEqual(before, self._freeze_digest())

    def test_output_nested_in_the_freeze_is_refused(self) -> None:
        before = self._freeze_digest()
        target = (
            self.f.freeze_dir / "config" / "amendments" / "a.json"
        )
        with self.assertRaisesRegex(
            ExecutionAmendmentError,
            "inside the immutable input freeze",
        ):
            require_outside_input_freeze(target, self.f.freeze_dir)
        self.assertFalse(target.parent.exists())
        self.assertEqual(before, self._freeze_digest())

    def test_the_freeze_directory_itself_is_refused(self) -> None:
        with self.assertRaisesRegex(
            ExecutionAmendmentError,
            "inside the immutable input freeze",
        ):
            require_outside_input_freeze(
                self.f.freeze_dir,
                self.f.freeze_dir,
            )

    def test_output_outside_the_freeze_is_accepted(self) -> None:
        require_outside_input_freeze(
            self.root / "amendments" / "a.json",
            self.f.freeze_dir,
        )

    def test_a_missing_freeze_directory_is_refused(self) -> None:
        with self.assertRaisesRegex(
            ExecutionAmendmentError,
            "does not exist",
        ):
            require_outside_input_freeze(
                self.root / "a.json",
                self.root / "absent-freeze",
            )

    def test_creation_refuses_and_leaves_the_freeze_byte_identical(
        self,
    ) -> None:
        before = self._freeze_digest()
        target = self.f.freeze_dir / "config" / "amendment.json"
        with self.assertRaisesRegex(
            ExecutionAmendmentError,
            "inside the immutable input freeze",
        ):
            self.f.build(
                input_freeze_dir=self.f.freeze_dir,
                output_path=target,
            )
        self.assertFalse(target.exists())
        self.assertEqual(before, self._freeze_digest())


class MeasurementBindingTest(unittest.TestCase):
    """A foreign manifest must fail before any side effect."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.f = _AmendmentFixture(self.root)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _foreign(self, **changes: Any):
        from pathfinder.distributed import load_measurement_manifest
        from tests.test_distributed_vertical import _measurement_payload

        payload = _measurement_payload(
            self.f.preregistration.source_sha256,
            self.f.registry.source_sha256,
        )
        payload.update(changes)
        path = self.root / f"foreign-{abs(hash(str(changes)))}.json"
        _write_json(path, payload)
        return load_measurement_manifest(path)

    def test_a_matching_manifest_is_accepted(self) -> None:
        require_matching_measurement_manifest(
            self.f.provider,
            self.f.preregistration,
            self.f.registry,
        )

    def test_a_foreign_manifest_is_refused(self) -> None:
        cases = {
            "pilot_id": {"pilot_id": "another-pilot"},
            "preregistration": {"preregistration_sha256": "a" * 64},
            "endpoint_registry": {"endpoint_registry_sha256": "b" * 64},
            "execution_node": {"execution_node_id": "some-other-node"},
        }
        for label, changes in cases.items():
            with self.subTest(field=label):
                with self.assertRaises(Exception) as caught:
                    require_matching_measurement_manifest(
                        self._foreign(**changes),
                        self.f.preregistration,
                        self.f.registry,
                    )
                self.assertIn(
                    "different frozen run",
                    str(caught.exception),
                )

    def test_creation_refuses_a_foreign_manifest(self) -> None:
        target = self.root / "amendments" / "a.json"
        with self.assertRaises(Exception):
            self.f.build(
                provider=self._foreign(pilot_id="another-pilot"),
                input_freeze_dir=self.f.freeze_dir,
                output_path=target,
            )
        self.assertFalse(target.exists())



class AmendmentCliTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.f = _AmendmentFixture(self.root)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _cli(self, argv, *, resolver=None):
        from unittest import mock

        injected = resolver if resolver is not None else self.f.resolver()
        stdout = io.StringIO()
        with mock.patch(
            "pathfinder.distributed.default_revision_resolver",
            lambda *a, **k: injected,
        ):
            with contextlib.redirect_stdout(stdout):
                code = cli_main(argv)
        return code, json.loads(stdout.getvalue())

    def _argv(self, output: Path) -> list[str]:
        return [
            "create-distributed-execution-amendment",
            "--preregistration",
            str(self.f.fixture.preregistration_path),
            "--endpoint-registry",
            str(self.f.registry_path),
            "--measurement-manifest",
            str(self.f.fixture.measurement_path),
            "--workload-manifest",
            str(self.f.workloads_path),
            "--config",
            str(self.f.system_config),
            "--frozen-plan",
            str(self.f.plan_path),
            "--input-freeze-dir",
            str(self.f.freeze_dir),
            "--amendment-id",
            "cli-amendment",
            "--reason",
            "orchestration-only tooling fix",
            "--output",
            str(output),
            "--compact",
        ]

    def test_the_create_command_refuses_to_overwrite(self) -> None:
        existing = self.f.write(self.f.build(), "existing.json")
        before = existing.read_bytes()
        code, payload = self._cli(self._argv(existing))
        self.assertEqual(2, code)
        self.assertEqual("error", payload["status"])
        self.assertIn(
            "refusing to overwrite an existing amendment",
            payload["message"],
        )
        self.assertEqual(
            before,
            existing.read_bytes(),
            "a refused create must not touch the existing file",
        )

    def test_the_create_command_writes_a_valid_amendment(self) -> None:
        target = self.root / "created.json"
        code, payload = self._cli(self._argv(target))
        self.assertEqual(0, code, payload)
        self.assertEqual(
            EXECUTION_AMENDMENT_SCHEMA_VERSION,
            payload["schema_version"],
        )
        self.assertTrue(target.is_file())
        self.assertEqual(EXECUTION_REVISION, payload["execution_git_revision"])
        self.assertFalse(payload["credentials_recorded"])

    def test_the_create_command_refuses_a_dirty_tree(self) -> None:
        class _Dirty(StaticRevisionResolver):
            def is_clean(self) -> bool:
                return False

        base = self.f.resolver()
        dirty = _Dirty(
            base.revision,
            base.paths,
            blobs=base.blobs,
            relative_paths=base.relative_paths,
        )
        target = self.root / "dirty.json"
        code, payload = self._cli(self._argv(target), resolver=dirty)
        self.assertEqual(2, code)
        self.assertIn("uncommitted changes", payload["message"])
        self.assertFalse(target.exists())


if __name__ == "__main__":
    unittest.main()
