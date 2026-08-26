"""Cross-tool agreement on the Reduced Oracle snapshot digest."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from pathfinder.awm import (
    audit_awm_workload_heterogeneity,
    certify_awm_restricted_policy,
)
from pathfinder.reduced_oracle import (
    ORACLE_SNAPSHOT_ALGORITHM,
    OracleSnapshotError,
    reduced_oracle_snapshot,
)

from tests.test_awm_certificate import (
    DESIGN_IDS,
    FRAMES,
    PAIR,
    SAFE,
    _Cell,
    _Fixture,
    _plan,
    _workload_ids,
    _write_json,
)


def _heterogeneity_config(root: Path, groups: dict[str, tuple[str, ...]]):
    path = root / "heterogeneity.json"
    _write_json(path, {
        "schema_version": "pathfinder.awm-heterogeneity/v1alpha1",
        "audit_id": "snapshot-cross-tool",
        "posthoc": True,
        "eligible_for_scientific_claims": False,
        "safe_design_id": SAFE,
        "design_ids": list(DESIGN_IDS),
        "training_repetitions": [0, 1],
        "evaluation_repetitions": [2, 3],
        "workload_groups": {
            group: list(workloads) for group, workloads in groups.items()
        },
        "policy": {
            "minimum_training_workloads_per_group": 1,
            "minimum_candidate_mean_gain": 0,
            "minimum_candidate_positive_fraction": 0.6,
        },
    })
    return path


class OracleSnapshotAlgorithmTest(unittest.TestCase):
    def _fixture(self, root: Path) -> _Fixture:
        return _Fixture(
            root,
            strata={"causal": _workload_ids("causal", 4)},
            plan=_plan(
                ("causal",),
                origin=_Cell(successes=1, cost=1.0),
                candidate=_Cell(successes=4, cost=0.2),
            ),
        )

    def test_scope_travels_with_the_digest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = self._fixture(Path(temporary))
            snapshot = reduced_oracle_snapshot(
                fixture.oracle_output,
                DESIGN_IDS,
                scope="full-declared-design-set",
            )
            self.assertEqual(ORACLE_SNAPSHOT_ALGORITHM, snapshot.algorithm)
            self.assertEqual(DESIGN_IDS, snapshot.design_ids)
            self.assertEqual(
                "full-declared-design-set",
                snapshot.scope,
            )
            self.assertEqual(5, len(snapshot.relative_paths))
            fields = snapshot.to_manifest_fields("oracle_snapshot")
            self.assertEqual(
                snapshot.sha256,
                fields["oracle_snapshot_sha256"],
            )
            self.assertEqual(
                ORACLE_SNAPSHOT_ALGORITHM,
                fields["oracle_snapshot_algorithm"],
            )
            self.assertEqual(5, fields["oracle_snapshot_file_count"])

    def test_a_different_scope_yields_a_different_digest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = self._fixture(Path(temporary))
            full = reduced_oracle_snapshot(
                fixture.oracle_output,
                DESIGN_IDS,
                scope="full-declared-design-set",
            )
            subset = reduced_oracle_snapshot(
                fixture.oracle_output,
                tuple(d for d in DESIGN_IDS if d != PAIR),
                scope="analysed-design-subset",
            )
            self.assertNotEqual(full.sha256, subset.sha256)
            self.assertEqual(full.algorithm, subset.algorithm)

    def test_an_absent_input_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = self._fixture(Path(temporary))
            with self.assertRaisesRegex(
                OracleSnapshotError,
                "does not exist",
            ):
                reduced_oracle_snapshot(
                    fixture.oracle_output,
                    DESIGN_IDS + ("D_absent",),
                    scope="full-declared-design-set",
                )

    def test_an_empty_scope_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = self._fixture(Path(temporary))
            with self.assertRaisesRegex(
                OracleSnapshotError,
                "cannot be empty",
            ):
                reduced_oracle_snapshot(
                    fixture.oracle_output,
                    (),
                    scope="empty",
                )

    def test_a_changed_record_changes_the_digest(self) -> None:
        digests = []
        for cost in (0.2, 0.3):
            with tempfile.TemporaryDirectory() as temporary:
                fixture = _Fixture(
                    Path(temporary),
                    strata={"causal": _workload_ids("causal", 4)},
                    plan=_plan(
                        ("causal",),
                        origin=_Cell(successes=1, cost=1.0),
                        candidate=_Cell(successes=4, cost=cost),
                    ),
                )
                digests.append(reduced_oracle_snapshot(
                    fixture.oracle_output,
                    DESIGN_IDS,
                    scope="full-declared-design-set",
                ).sha256)
        self.assertNotEqual(digests[0], digests[1])


class CrossToolSnapshotAgreementTest(unittest.TestCase):
    def test_heterogeneity_and_certificate_agree_on_the_full_digest(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = _Fixture(
                root,
                strata={"causal": _workload_ids("causal", 4)},
                plan=_plan(
                    ("causal",),
                    origin=_Cell(successes=1, cost=1.0),
                    candidate=_Cell(successes=4, cost=0.2),
                ),
            )
            audit = audit_awm_workload_heterogeneity(
                _heterogeneity_config(root, fixture.strata),
                fixture.oracle_path,
                oracle_output_dir=fixture.oracle_output,
                output_dir=root / "heterogeneity-output",
            )
            certificate = certify_awm_restricted_policy(
                fixture.certificate_config(),
                fixture.oracle_path,
                oracle_output_dir=fixture.oracle_output,
                output_dir=root / "certificate-output",
            )
            self.assertEqual(
                audit["oracle_snapshot_algorithm"],
                certificate["oracle_full_snapshot_algorithm"],
            )
            self.assertEqual(
                audit["oracle_snapshot_sha256"],
                certificate["oracle_full_snapshot_sha256"],
                "the two read-only tools must publish one identical digest "
                "for the same full Oracle scope",
            )
            self.assertEqual(
                "full-declared-design-set",
                audit["oracle_snapshot_scope"],
            )
            self.assertEqual(
                list(DESIGN_IDS),
                audit["oracle_snapshot_design_ids"],
            )

    def test_the_analysed_subset_digest_is_named_apart(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = _Fixture(
                root,
                strata={"causal": _workload_ids("causal", 4)},
                plan=_plan(
                    ("causal",),
                    origin=_Cell(successes=1, cost=1.0),
                    candidate=_Cell(successes=4, cost=0.2),
                ),
            )
            audit = audit_awm_workload_heterogeneity(
                _heterogeneity_config(root, fixture.strata),
                fixture.oracle_path,
                oracle_output_dir=fixture.oracle_output,
                output_dir=root / "heterogeneity-output",
            )
            certificate = certify_awm_restricted_policy(
                fixture.certificate_config(),
                fixture.oracle_path,
                oracle_output_dir=fixture.oracle_output,
                output_dir=root / "certificate-output",
            )
            self.assertNotIn("oracle_snapshot_sha256", certificate)
            self.assertNotEqual(
                certificate["oracle_analysed_snapshot_sha256"],
                certificate["oracle_full_snapshot_sha256"],
                "excluding D_local_pair must change the analysed digest",
            )
            self.assertEqual(
                audit["oracle_snapshot_sha256"],
                certificate["oracle_full_snapshot_sha256"],
            )
            self.assertEqual(
                [SAFE, FRAMES],
                certificate["oracle_analysed_snapshot_design_ids"],
            )
            evaluation = json.loads(
                (
                    Path(certificate["evaluation_path"]).parent
                    / "certificate_evaluation.json"
                ).read_text(encoding="utf-8")
            )
            snapshot = evaluation["oracle_snapshot"]
            self.assertEqual(
                certificate["oracle_full_snapshot_sha256"],
                snapshot["full_declared_design_set_sha256"],
            )
            self.assertEqual(
                certificate["oracle_analysed_snapshot_sha256"],
                snapshot["analysed_design_subset_sha256"],
            )


if __name__ == "__main__":
    unittest.main()
