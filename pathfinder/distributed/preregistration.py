"""Preregistration contract for the distributed pilot.

The distributed pilot is a *pilot*: it fixes its thresholds, its restricted
candidate set, and its workload manifest before any new outcome is observed,
but it is still an engineering shakedown rather than a confirmatory study.
This module encodes exactly that position.

Two things it refuses to do:

* It will not let a run claim ``eligible_for_scientific_claims``. Pilot
  thresholds are engineering choices, and the restricted candidate set was
  narrowed on an already-inspected frozen Oracle, so no amount of flag
  flipping in this document can make the result confirmatory.
* It will not let a resumed run silently change. The configuration hash and
  the workload-manifest hash are both checked, so an edited plan is a hard
  failure rather than a quietly mixed dataset.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable, Mapping

from .cost import CostModel, load_cost_model


PILOT_PREREGISTRATION_SCHEMA_VERSION = (
    "pathfinder.distributed-pilot-preregistration/v1alpha1"
)
#: Pilot thresholds are fixed in advance but are not scientific operating
#: points. Only this provenance is accepted.
PILOT_THRESHOLD_PROVENANCE = (
    "pilot-engineering-threshold-fixed-before-new-pilot-outcomes"
)
SUCCESS_SCORING_RULES = (
    "accepted-answer-substring-match",
)
COST_BASES = ("service_cost", "total_cost")
FALLBACK_DESIGN_ID = "D_origin_remote"


class PilotPreregistrationError(ValueError):
    """Raised when a distributed-pilot preregistration is invalid."""


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise PilotPreregistrationError(f"{name} must be an object")
    return value


def _string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PilotPreregistrationError(f"{name} must be a non-empty string")
    return value.strip()


def _boolean(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise PilotPreregistrationError(f"{name} must be a boolean")
    return value


def _require(payload: Mapping[str, Any], key: str, name: str) -> Any:
    if key not in payload:
        raise PilotPreregistrationError(
            f"{name} is a required configuration field"
        )
    return payload[key]


def _positive_integer(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise PilotPreregistrationError(f"{name} must be a positive integer")
    return value


def _finite_number(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PilotPreregistrationError(f"{name} must be a number")
    result = float(value)
    if not math.isfinite(result):
        raise PilotPreregistrationError(f"{name} must be finite")
    return result


def _string_array(value: Any, name: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise PilotPreregistrationError(f"{name} must be a non-empty array")
    result = tuple(
        _string(item, f"{name}[{index}]")
        for index, item in enumerate(value)
    )
    duplicates = sorted({v for v in result if result.count(v) > 1})
    if duplicates:
        raise PilotPreregistrationError(
            f"{name} contains duplicates: " + ", ".join(duplicates)
        )
    return result


def manifest_sha256(workload_ids: Iterable[str]) -> str:
    """Order-independent digest over workload IDENTIFIERS only.

    This is deliberately *not* a workload-content hash: two runs whose
    questions or accepted answers differ produce the same value. Use
    :func:`workload_content_sha256` to bind what a workload actually says.
    """
    canonical = "\n".join(sorted(workload_ids))
    return sha256(canonical.encode("utf-8")).hexdigest()


def workload_content_sha256(
    workloads: Mapping[str, Mapping[str, Any]],
    workload_ids: Iterable[str],
) -> str:
    """Digest the complete definitions of the planned workloads.

    Only planned ids contribute, so unrelated entries in a larger manifest
    cannot invalidate a run. Each definition is serialised with sorted keys,
    which makes the digest independent of key order in equivalent JSON while
    still changing if any question, object id, or accepted answer changes.
    """
    ordered = sorted(set(workload_ids))
    missing = [
        workload_id for workload_id in ordered
        if workload_id not in workloads
    ]
    if missing:
        raise PilotPreregistrationError(
            "cannot hash workload content; no definition for: "
            + ", ".join(missing)
        )
    canonical = json.dumps(
        {workload_id: workloads[workload_id] for workload_id in ordered},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class PilotStratum:
    stratum_id: str
    candidate_design_id: str
    workload_ids: tuple[str, ...]

    @property
    def manifest_sha256(self) -> str:
        return manifest_sha256(self.workload_ids)


@dataclass(frozen=True)
class DistributedPilotPreregistration:
    schema_version: str
    pilot_id: str
    source_path: Path
    source_sha256: str
    source_git_revision: str
    posthoc: bool
    confirmatory: bool
    eligible_for_scientific_claims: bool
    delta_success_margin: float
    minimum_cost_saving: float
    alpha: float
    threshold_provenance: str
    safe_design_id: str
    design_ids: tuple[str, ...]
    excluded_design_ids: tuple[str, ...]
    strata: tuple[PilotStratum, ...]
    excluded_workload_ids: tuple[str, ...]
    excluded_workload_manifest_sha256: str
    repetitions: int
    success_scoring_rule: str
    cost_basis: str
    cost_model: CostModel
    fallback_design_id: str
    immutable_after_first_observation: bool

    @property
    def workload_ids(self) -> tuple[str, ...]:
        return tuple(
            workload_id
            for stratum in self.strata
            for workload_id in stratum.workload_ids
        )

    @property
    def workload_manifest_sha256(self) -> str:
        return manifest_sha256(self.workload_ids)

    @property
    def candidate_design_ids(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(
            stratum.candidate_design_id for stratum in self.strata
        ))

    @property
    def independent_workload_count(self) -> int:
        return len(self.workload_ids)

    @property
    def planned_trial_count(self) -> int:
        """Two designs per workload, one complete repetition block each."""
        return self.independent_workload_count * 2 * self.repetitions

    def stratum_for(self, workload_id: str) -> PilotStratum:
        for stratum in self.strata:
            if workload_id in stratum.workload_ids:
                return stratum
        raise PilotPreregistrationError(
            f"workload is not in any declared stratum: {workload_id}"
        )

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "pilot_id": self.pilot_id,
            "source_sha256": self.source_sha256,
            "source_git_revision": self.source_git_revision,
            "posthoc": self.posthoc,
            "confirmatory": self.confirmatory,
            "eligible_for_scientific_claims": (
                self.eligible_for_scientific_claims
            ),
            "claim_boundary": (
                "This is an engineering pilot. Thresholds are pilot "
                "engineering values fixed before new pilot outcomes, and the "
                "restricted candidate set was narrowed post-hoc on an "
                "already inspected frozen Oracle. It is not confirmatory "
                "evidence and cannot become confirmatory by flag."
            ),
            "thresholds": {
                "delta_success_margin": self.delta_success_margin,
                "minimum_cost_saving": self.minimum_cost_saving,
                "alpha": self.alpha,
                "provenance": self.threshold_provenance,
                "scientifically_justified": False,
            },
            "safe_design_id": self.safe_design_id,
            "design_ids": list(self.design_ids),
            "excluded_design_ids": list(self.excluded_design_ids),
            "strata": [
                {
                    "stratum_id": stratum.stratum_id,
                    "candidate_design_id": stratum.candidate_design_id,
                    "workload_count": len(stratum.workload_ids),
                    "workload_ids": list(stratum.workload_ids),
                    "workload_manifest_sha256": stratum.manifest_sha256,
                }
                for stratum in self.strata
            ],
            "workload_manifest_sha256": self.workload_manifest_sha256,
            "independent_workload_count": self.independent_workload_count,
            "excluded_workload_count": len(self.excluded_workload_ids),
            "excluded_workload_manifest_sha256": (
                self.excluded_workload_manifest_sha256
            ),
            "repetitions": self.repetitions,
            "planned_trial_count": self.planned_trial_count,
            "success_scoring_rule": self.success_scoring_rule,
            "cost_basis": self.cost_basis,
            "cost_model": self.cost_model.to_public_dict(),
            "fallback_rule": (
                "every non-SAFE_TO_COMMIT result retains "
                + self.fallback_design_id
            ),
            "fallback_design_id": self.fallback_design_id,
            "immutable_after_first_observation": (
                self.immutable_after_first_observation
            ),
            "independent_unit": "workload-object-cluster",
            "repetitions_increase_independent_units": False,
        }


def load_distributed_pilot_preregistration(
    path: str | Path,
) -> DistributedPilotPreregistration:
    """Load and validate a distributed-pilot preregistration document."""
    source = Path(path).resolve()
    if not source.is_file():
        raise PilotPreregistrationError(
            f"pilot preregistration does not exist: {source}"
        )
    raw_bytes = source.read_bytes()
    try:
        root = _mapping(json.loads(raw_bytes), "pilot preregistration")
    except json.JSONDecodeError as exc:
        raise PilotPreregistrationError(
            f"invalid pilot preregistration JSON: {source}"
        ) from exc

    schema_version = _string(
        _require(root, "schema_version", "schema_version"),
        "schema_version",
    )
    if schema_version != PILOT_PREREGISTRATION_SCHEMA_VERSION:
        raise PilotPreregistrationError(
            "unsupported pilot preregistration schema_version: "
            + schema_version
        )

    posthoc = _boolean(_require(root, "posthoc", "posthoc"), "posthoc")
    confirmatory = _boolean(
        _require(root, "confirmatory", "confirmatory"),
        "confirmatory",
    )
    eligible = _boolean(
        _require(
            root,
            "eligible_for_scientific_claims",
            "eligible_for_scientific_claims",
        ),
        "eligible_for_scientific_claims",
    )
    if posthoc:
        raise PilotPreregistrationError(
            "a distributed pilot must declare posthoc=false; its thresholds "
            "and workload manifest are fixed before new outcomes"
        )
    if confirmatory or eligible:
        raise PilotPreregistrationError(
            "a distributed pilot cannot claim confirmatory status or "
            "scientific eligibility: it must declare confirmatory=false and "
            "eligible_for_scientific_claims=false. Confirmatory evaluation "
            "requires a separate preregistered study on new independent "
            "workloads."
        )

    thresholds = _mapping(
        _require(root, "thresholds", "thresholds"),
        "thresholds",
    )
    delta_success_margin = _finite_number(
        _require(
            thresholds,
            "delta_success_margin",
            "thresholds.delta_success_margin",
        ),
        "thresholds.delta_success_margin",
    )
    minimum_cost_saving = _finite_number(
        _require(
            thresholds,
            "minimum_cost_saving",
            "thresholds.minimum_cost_saving",
        ),
        "thresholds.minimum_cost_saving",
    )
    alpha = _finite_number(
        _require(thresholds, "alpha", "thresholds.alpha"),
        "thresholds.alpha",
    )
    if delta_success_margin < 0.0:
        raise PilotPreregistrationError(
            "thresholds.delta_success_margin must be non-negative"
        )
    if not 0.0 < alpha < 1.0:
        raise PilotPreregistrationError(
            "thresholds.alpha must be in (0, 1)"
        )
    threshold_provenance = _string(
        _require(thresholds, "provenance", "thresholds.provenance"),
        "thresholds.provenance",
    )
    if threshold_provenance != PILOT_THRESHOLD_PROVENANCE:
        raise PilotPreregistrationError(
            "thresholds.provenance must be "
            + PILOT_THRESHOLD_PROVENANCE
        )

    design_ids = _string_array(
        _require(root, "design_ids", "design_ids"),
        "design_ids",
    )
    safe_design_id = _string(
        _require(root, "safe_design_id", "safe_design_id"),
        "safe_design_id",
    )
    if safe_design_id not in design_ids:
        raise PilotPreregistrationError(
            "safe_design_id must appear in design_ids"
        )
    excluded_design_ids = tuple(
        _string(item, f"excluded_design_ids[{index}]")
        for index, item in enumerate(root.get("excluded_design_ids", []))
    )
    for design_id in excluded_design_ids:
        if design_id not in design_ids:
            raise PilotPreregistrationError(
                f"excluded design is not a declared design: {design_id}"
            )
    if safe_design_id in excluded_design_ids:
        raise PilotPreregistrationError(
            "the safe design cannot be excluded"
        )

    raw_strata = _mapping(_require(root, "strata", "strata"), "strata")
    if not raw_strata:
        raise PilotPreregistrationError("strata cannot be empty")
    strata: list[PilotStratum] = []
    seen_workloads: dict[str, str] = {}
    for stratum_id in sorted(raw_strata):
        payload = _mapping(raw_strata[stratum_id], f"strata.{stratum_id}")
        candidate = _string(
            _require(
                payload,
                "candidate_design_id",
                f"strata.{stratum_id}.candidate_design_id",
            ),
            f"strata.{stratum_id}.candidate_design_id",
        )
        if candidate not in design_ids:
            raise PilotPreregistrationError(
                f"strata.{stratum_id}.candidate_design_id is not a declared "
                f"design: {candidate}"
            )
        if candidate in excluded_design_ids:
            raise PilotPreregistrationError(
                f"strata.{stratum_id} names an excluded design: {candidate}"
            )
        if candidate == safe_design_id:
            raise PilotPreregistrationError(
                f"strata.{stratum_id} candidate cannot be the safe design"
            )
        workload_ids = _string_array(
            _require(
                payload,
                "workload_ids",
                f"strata.{stratum_id}.workload_ids",
            ),
            f"strata.{stratum_id}.workload_ids",
        )
        for workload_id in workload_ids:
            previous = seen_workloads.get(workload_id)
            if previous is not None:
                raise PilotPreregistrationError(
                    f"workload '{workload_id}' appears in both strata "
                    f"'{previous}' and '{stratum_id}'; a workload is one "
                    "independent unit and cannot be counted twice"
                )
            seen_workloads[workload_id] = stratum_id
        strata.append(PilotStratum(
            stratum_id=stratum_id,
            candidate_design_id=candidate,
            workload_ids=workload_ids,
        ))

    excluded_workloads = tuple(
        _string(item, f"excluded_workload_ids[{index}]")
        for index, item in enumerate(
            _require(root, "excluded_workload_ids", "excluded_workload_ids")
        )
    )
    duplicate_exclusions = sorted({
        value
        for value in excluded_workloads
        if excluded_workloads.count(value) > 1
    })
    if duplicate_exclusions:
        raise PilotPreregistrationError(
            "excluded_workload_ids contains duplicates: "
            + ", ".join(duplicate_exclusions)
        )
    overlap = sorted(set(excluded_workloads).intersection(seen_workloads))
    if overlap:
        raise PilotPreregistrationError(
            "pilot workloads overlap the frozen/excluded workload manifest, "
            "which would reuse already-observed outcomes: "
            + ", ".join(overlap)
        )

    declared_manifest = root.get("workload_manifest_sha256")
    computed_manifest = manifest_sha256(seen_workloads)
    if declared_manifest is not None:
        declared = _string(declared_manifest, "workload_manifest_sha256")
        if declared != computed_manifest:
            raise PilotPreregistrationError(
                "workload_manifest_sha256 does not match the declared "
                f"workloads: expected {computed_manifest}"
            )
    declared_excluded = root.get("excluded_workload_manifest_sha256")
    computed_excluded = manifest_sha256(excluded_workloads)
    if declared_excluded is not None:
        declared = _string(
            declared_excluded,
            "excluded_workload_manifest_sha256",
        )
        if declared != computed_excluded:
            raise PilotPreregistrationError(
                "excluded_workload_manifest_sha256 does not match the "
                f"declared exclusions: expected {computed_excluded}"
            )

    success_scoring_rule = _string(
        _require(root, "success_scoring_rule", "success_scoring_rule"),
        "success_scoring_rule",
    )
    if success_scoring_rule not in SUCCESS_SCORING_RULES:
        raise PilotPreregistrationError(
            "unsupported success_scoring_rule: " + success_scoring_rule
        )
    cost_contract = _mapping(
        _require(root, "total_cost_contract", "total_cost_contract"),
        "total_cost_contract",
    )
    cost_basis = _string(
        _require(
            cost_contract,
            "cost_basis",
            "total_cost_contract.cost_basis",
        ),
        "total_cost_contract.cost_basis",
    )
    if cost_basis not in COST_BASES:
        raise PilotPreregistrationError(
            "total_cost_contract.cost_basis must be one of "
            + ", ".join(COST_BASES)
        )
    cost_model = load_cost_model(
        _require(
            cost_contract,
            "cost_model",
            "total_cost_contract.cost_model",
        ),
        name="total_cost_contract.cost_model",
    )

    fallback = _mapping(
        _require(root, "fallback_rule", "fallback_rule"),
        "fallback_rule",
    )
    fallback_design_id = _string(
        _require(fallback, "design_id", "fallback_rule.design_id"),
        "fallback_rule.design_id",
    )
    if fallback_design_id != FALLBACK_DESIGN_ID:
        raise PilotPreregistrationError(
            f"fallback_rule.design_id must be {FALLBACK_DESIGN_ID}"
        )
    if fallback_design_id != safe_design_id:
        raise PilotPreregistrationError(
            "fallback_rule.design_id must equal safe_design_id"
        )
    if not _boolean(
        _require(
            fallback,
            "applies_to_every_non_safe_result",
            "fallback_rule.applies_to_every_non_safe_result",
        ),
        "fallback_rule.applies_to_every_non_safe_result",
    ):
        raise PilotPreregistrationError(
            "fallback_rule.applies_to_every_non_safe_result must be true"
        )

    run_declaration = _mapping(
        _require(root, "run_declaration", "run_declaration"),
        "run_declaration",
    )
    immutable = _boolean(
        _require(
            run_declaration,
            "immutable_after_first_observation",
            "run_declaration.immutable_after_first_observation",
        ),
        "run_declaration.immutable_after_first_observation",
    )
    if not immutable:
        raise PilotPreregistrationError(
            "run_declaration.immutable_after_first_observation must be true; "
            "a pilot plan that can change mid-run cannot be audited"
        )

    return DistributedPilotPreregistration(
        schema_version=schema_version,
        pilot_id=_string(_require(root, "pilot_id", "pilot_id"), "pilot_id"),
        source_path=source,
        source_sha256=sha256(raw_bytes).hexdigest(),
        source_git_revision=_string(
            _require(root, "source_git_revision", "source_git_revision"),
            "source_git_revision",
        ),
        posthoc=posthoc,
        confirmatory=confirmatory,
        eligible_for_scientific_claims=eligible,
        delta_success_margin=delta_success_margin,
        minimum_cost_saving=minimum_cost_saving,
        alpha=alpha,
        threshold_provenance=threshold_provenance,
        safe_design_id=safe_design_id,
        design_ids=design_ids,
        excluded_design_ids=excluded_design_ids,
        strata=tuple(strata),
        excluded_workload_ids=excluded_workloads,
        excluded_workload_manifest_sha256=computed_excluded,
        repetitions=_positive_integer(
            _require(root, "repetitions", "repetitions"),
            "repetitions",
        ),
        success_scoring_rule=success_scoring_rule,
        cost_basis=cost_basis,
        cost_model=cost_model,
        fallback_design_id=fallback_design_id,
        immutable_after_first_observation=immutable,
    )
