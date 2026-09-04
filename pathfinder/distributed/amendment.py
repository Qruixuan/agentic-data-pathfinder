"""Execution amendments for a frozen distributed pilot.

A pilot's scientific inputs are frozen: workloads, designs, placement,
representations, cost measurements, scoring, thresholds, seeds, repetitions,
and the trial plan. The *implementation* that executes them is a separate
thing, and it moves -- a preflight bug fixed after the freeze does not change
what the experiment measures, but it does change the Git revision executing
it.

Without a mechanism for that, the only honest options are to run at a revision
the preregistration does not name, or to re-freeze the entire input set after
every non-semantic tooling fix. The first destroys provenance; the second
creates version churn that makes provenance harder to read, not easier.

An execution amendment is the third option: a small, separately hashed
document that records exactly which revision will execute a pilot frozen at
another revision, what changed between them, and -- crucially -- that the plan
recomputed by the *new* code is byte-identical to the frozen one.

What this is NOT
----------------
This is **auditable compatibility evidence**, not authenticity and not
approval. It is not signed; anyone who can write the file can write any
content into it. It does not prove the code change is scientifically
harmless. A recomputed-equal plan shows the *plan-generating* semantics are
unchanged; it says nothing about whether execution, measurement, or scoring
behave the same. The classification is asserted by a human and only
``orchestration-only`` is accepted, deliberately, so that widening it is a
visible code change subject to review rather than a config edit.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable, Mapping, Protocol

from .preregistration import (
    DistributedPilotPreregistration,
    workload_content_sha256,
)
from .registry import EndpointRegistry


EXECUTION_AMENDMENT_SCHEMA_VERSION = (
    "pathfinder.distributed-execution-amendment/v1alpha1"
)
AMENDMENT_DOCUMENT = "execution_amendment.json"

#: The only change class an amendment may assert. Everything else -- a
#: workload, design, representation, cost, scoring, threshold, seed,
#: repetition, ordering, or plan-semantics change -- requires a new freeze.
#: Widening this tuple is a code change, on purpose.
CHANGE_CLASSIFICATIONS = ("orchestration-only",)

#: Hashes an amendment must reproduce exactly. Each names an input whose
#: change would make the amendment describe a different experiment.
BOUND_INPUT_FIELDS = (
    "preregistration_sha256",
    "endpoint_registry_sha256",
    "measurement_manifest_sha256",
    "workload_content_sha256",
    "system_config_sha256",
    "frozen_plan_sha256",
    "frozen_plan_file_sha256",
)


class ExecutionAmendmentError(ValueError):
    """Raised when an amendment is absent, invalid, or does not apply."""


class RevisionResolver(Protocol):
    """Resolves the Git revision currently executing, and what changed."""

    def current_revision(self) -> str:
        ...

    def changed_paths(self, base: str, head: str) -> tuple[str, ...]:
        ...

    def file_sha256_at(
        self,
        revision: str,
        repo_relative_path: str,
    ) -> str | None:
        """Digest a tracked file's content at a revision, or None if absent."""


@dataclass(frozen=True)
class GitRevisionResolver:
    """Reads revision facts from the working repository, read-only."""

    repository: Path

    def _git(self, *arguments: str) -> str:
        try:
            completed = subprocess.run(
                ("git", *arguments),
                cwd=str(self.repository),
                capture_output=True,
                text=True,
                check=True,
            )
        except FileNotFoundError as exc:
            raise ExecutionAmendmentError(
                "git is not available; cannot resolve the execution revision"
            ) from exc
        except subprocess.CalledProcessError as exc:
            raise ExecutionAmendmentError(
                "git "
                + " ".join(arguments)
                + " failed: "
                + (exc.stderr or "").strip()
            ) from exc
        return completed.stdout.strip()

    def current_revision(self) -> str:
        return self._git("rev-parse", "HEAD")

    def changed_paths(self, base: str, head: str) -> tuple[str, ...]:
        if base == head:
            return ()
        output = self._git("diff", "--name-only", f"{base}..{head}")
        return tuple(
            line.strip() for line in output.splitlines() if line.strip()
        )

    def is_clean(self) -> bool:
        return not self._git("status", "--porcelain")

    def repo_relative_path(self, path: str | Path) -> str:
        """The repository-relative path of a tracked file."""
        top = Path(self._git("rev-parse", "--show-toplevel")).resolve()
        resolved = Path(path).resolve()
        try:
            return resolved.relative_to(top).as_posix()
        except ValueError:
            raise ExecutionAmendmentError(
                f"{resolved} is outside the repository at {top}, so its "
                "content cannot be compared across revisions"
            ) from None

    def file_sha256_at(
        self,
        revision: str,
        repo_relative_path: str,
    ) -> str | None:
        try:
            completed = subprocess.run(
                ("git", "show", f"{revision}:{repo_relative_path}"),
                cwd=str(self.repository),
                capture_output=True,
                check=True,
            )
        except subprocess.CalledProcessError:
            return None
        return sha256(completed.stdout).hexdigest()


@dataclass(frozen=True)
class StaticRevisionResolver:
    """A resolver with fixed answers, for tests and reproducible checks."""

    revision: str
    paths: tuple[str, ...] = ()
    #: (revision, repo_relative_path) -> content digest, or absent for a
    #: file that does not exist at that revision.
    blobs: Mapping[tuple[str, str], str] | None = None
    relative_paths: Mapping[str, str] | None = None

    def current_revision(self) -> str:
        return self.revision

    def changed_paths(self, base: str, head: str) -> tuple[str, ...]:
        return () if base == head else self.paths

    def repo_relative_path(self, path: str | Path) -> str:
        mapped = (self.relative_paths or {}).get(str(path))
        return mapped if mapped is not None else Path(path).name

    def file_sha256_at(
        self,
        revision: str,
        repo_relative_path: str,
    ) -> str | None:
        return (self.blobs or {}).get((revision, repo_relative_path))


@dataclass(frozen=True)
class ExecutionAmendment:
    """One loaded, structurally valid amendment document."""

    schema_version: str
    amendment_id: str
    pilot_id: str
    protocol_git_revision: str
    execution_git_revision: str
    change_classification: str
    reason: str
    changed_paths: tuple[str, ...]
    plan_equivalence_verified: bool
    plan_equivalence_statement: str
    system_config_repo_path: str
    bound_inputs: dict[str, str]
    credentials_recorded: bool
    source_path: Path
    source_sha256: str

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "amendment_id": self.amendment_id,
            "pilot_id": self.pilot_id,
            "protocol_git_revision": self.protocol_git_revision,
            "execution_git_revision": self.execution_git_revision,
            "change_classification": self.change_classification,
            "reason": self.reason,
            "changed_paths": list(self.changed_paths),
            "plan_equivalence_verified": self.plan_equivalence_verified,
            "plan_equivalence_statement": self.plan_equivalence_statement,
            "system_config_repo_path": self.system_config_repo_path,
            **self.bound_inputs,
            "credentials_recorded": self.credentials_recorded,
            "amendment_sha256": self.source_sha256,
            "evidence_class": "auditable-compatibility-evidence",
            "not_an_authenticity_or_approval_control": True,
        }


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ExecutionAmendmentError(f"{name} must be an object")
    return value


def _string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ExecutionAmendmentError(f"{name} must be a non-empty string")
    return value.strip()


def _require(payload: Mapping[str, Any], key: str, name: str) -> Any:
    if key not in payload:
        raise ExecutionAmendmentError(f"{name} is a required field")
    return payload[key]


def _digest(value: Any, name: str) -> str:
    text = _string(value, name)
    if len(text) != 64 or any(c not in "0123456789abcdef" for c in text):
        raise ExecutionAmendmentError(
            f"{name} must be a lowercase hex SHA-256 digest"
        )
    return text


def file_sha256(path: str | Path) -> str:
    """Digest a file's exact bytes."""
    return sha256(Path(path).read_bytes()).hexdigest()


def canonical_plan_sha256(plan: Mapping[str, Any]) -> str:
    """Digest a plan document independently of key order."""
    return sha256(
        json.dumps(
            {k: v for k, v in plan.items() if k != "plan_sha256"},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def load_execution_amendment(path: str | Path) -> ExecutionAmendment:
    """Load and structurally validate an amendment document."""
    source = Path(path).resolve()
    if not source.is_file():
        raise ExecutionAmendmentError(
            f"execution amendment does not exist: {source}"
        )
    raw_bytes = source.read_bytes()
    try:
        root = _mapping(json.loads(raw_bytes), "execution amendment")
    except json.JSONDecodeError as exc:
        raise ExecutionAmendmentError(
            f"invalid execution amendment JSON: {source}"
        ) from exc

    schema_version = _string(
        _require(root, "schema_version", "schema_version"),
        "schema_version",
    )
    if schema_version != EXECUTION_AMENDMENT_SCHEMA_VERSION:
        raise ExecutionAmendmentError(
            "unsupported execution amendment schema_version: "
            + schema_version
        )
    classification = _string(
        _require(root, "change_classification", "change_classification"),
        "change_classification",
    )
    if classification not in CHANGE_CLASSIFICATIONS:
        raise ExecutionAmendmentError(
            f"unsupported change_classification: {classification}; only "
            + ", ".join(CHANGE_CLASSIFICATIONS)
            + " may be amended. Any other change requires a new freeze."
        )
    verified = _require(
        root,
        "plan_equivalence_verified",
        "plan_equivalence_verified",
    )
    if verified is not True:
        raise ExecutionAmendmentError(
            "plan_equivalence_verified must be literally true; an amendment "
            "that does not assert verified plan equivalence is not usable"
        )
    unchanged = root.get(
        "system_config_unchanged_since_protocol_revision",
        None,
    )
    if unchanged is not True:
        raise ExecutionAmendmentError(
            "system_config_unchanged_since_protocol_revision must be "
            "literally true; an amendment that does not assert the system "
            "contract is unchanged is not usable"
        )
    credentials_recorded = root.get("credentials_recorded", None)
    if credentials_recorded is not False:
        raise ExecutionAmendmentError(
            "credentials_recorded must be literally false"
        )
    for forbidden in ("confirmatory", "eligible_for_scientific_claims"):
        if forbidden in root:
            raise ExecutionAmendmentError(
                f"an execution amendment may not carry {forbidden}; it "
                "cannot change the pilot's scientific standing"
            )
    raw_paths = root.get("changed_paths", [])
    if not isinstance(raw_paths, list):
        raise ExecutionAmendmentError("changed_paths must be an array")

    return ExecutionAmendment(
        schema_version=schema_version,
        amendment_id=_string(
            _require(root, "amendment_id", "amendment_id"),
            "amendment_id",
        ),
        pilot_id=_string(
            _require(root, "pilot_id", "pilot_id"),
            "pilot_id",
        ),
        protocol_git_revision=_string(
            _require(
                root,
                "protocol_git_revision",
                "protocol_git_revision",
            ),
            "protocol_git_revision",
        ),
        execution_git_revision=_string(
            _require(
                root,
                "execution_git_revision",
                "execution_git_revision",
            ),
            "execution_git_revision",
        ),
        change_classification=classification,
        reason=_string(_require(root, "reason", "reason"), "reason"),
        changed_paths=tuple(
            _string(item, f"changed_paths[{index}]")
            for index, item in enumerate(raw_paths)
        ),
        plan_equivalence_verified=True,
        system_config_repo_path=_string(
            _require(
                root,
                "system_config_repo_path",
                "system_config_repo_path",
            ),
            "system_config_repo_path",
        ),
        plan_equivalence_statement=_string(
            _require(
                root,
                "plan_equivalence_statement",
                "plan_equivalence_statement",
            ),
            "plan_equivalence_statement",
        ),
        bound_inputs={
            field: _digest(_require(root, field, field), field)
            for field in BOUND_INPUT_FIELDS
        },
        credentials_recorded=False,
        source_path=source,
        source_sha256=sha256(raw_bytes).hexdigest(),
    )


def _expected_bound_inputs(
    preregistration: DistributedPilotPreregistration,
    registry: EndpointRegistry,
    *,
    measurement_manifest_sha256: str,
    workloads: Mapping[str, Mapping[str, Any]],
    system_config_path: str | Path,
    frozen_plan: Mapping[str, Any],
    frozen_plan_path: str | Path,
) -> dict[str, str]:
    return {
        "preregistration_sha256": preregistration.source_sha256,
        "endpoint_registry_sha256": registry.source_sha256,
        "measurement_manifest_sha256": measurement_manifest_sha256,
        "workload_content_sha256": workload_content_sha256(
            workloads,
            preregistration.workload_ids,
        ),
        "system_config_sha256": file_sha256(system_config_path),
        "frozen_plan_sha256": str(frozen_plan["plan_sha256"]),
        "frozen_plan_file_sha256": file_sha256(frozen_plan_path),
    }


def verify_plan_equivalence(
    recomputed_plan: Mapping[str, Any],
    frozen_plan: Mapping[str, Any],
) -> tuple[bool, tuple[str, ...]]:
    """Compare a freshly computed plan against the frozen one, field by field.

    Equality of ``plan_sha256`` alone would be circular if the digest were
    ever computed over a subset, so every key is compared and the digest is
    recomputed independently.
    """
    differences: list[str] = []
    for key in sorted(set(recomputed_plan) | set(frozen_plan)):
        if key not in recomputed_plan:
            differences.append(f"{key}: absent from the recomputed plan")
        elif key not in frozen_plan:
            differences.append(f"{key}: absent from the frozen plan")
        elif recomputed_plan[key] != frozen_plan[key]:
            differences.append(f"{key}: differs")
    if canonical_plan_sha256(recomputed_plan) != canonical_plan_sha256(
        frozen_plan
    ):
        differences.append("canonical plan digest differs")
    return not differences, tuple(differences)


def build_execution_amendment(
    preregistration: DistributedPilotPreregistration,
    registry: EndpointRegistry,
    *,
    amendment_id: str,
    reason: str,
    measurement_manifest_sha256: str,
    workloads: Mapping[str, Mapping[str, Any]],
    system_config_path: str | Path,
    frozen_plan_path: str | Path,
    recomputed_plan: Mapping[str, Any],
    resolver: RevisionResolver,
    change_classification: str = "orchestration-only",
) -> dict[str, Any]:
    """Create an amendment, refusing unless the plan recomputes identically."""
    if change_classification not in CHANGE_CLASSIFICATIONS:
        raise ExecutionAmendmentError(
            f"unsupported change_classification: {change_classification}"
        )
    frozen_plan = json.loads(
        Path(frozen_plan_path).read_text(encoding="utf-8")
    )
    if frozen_plan.get("pilot_id") != preregistration.pilot_id:
        raise ExecutionAmendmentError(
            "the frozen plan belongs to pilot "
            f"{frozen_plan.get('pilot_id')!r}, not "
            f"{preregistration.pilot_id!r}"
        )
    equivalent, differences = verify_plan_equivalence(
        recomputed_plan,
        frozen_plan,
    )
    if not equivalent:
        raise ExecutionAmendmentError(
            "the plan recomputed by the current implementation is not "
            "equivalent to the frozen plan, so this change is not "
            "orchestration-only and requires a new freeze. Differences: "
            + "; ".join(differences)
        )

    # A dirty tree would make the recorded revision a lie: HEAD names one
    # thing, the interpreter runs another. Refuse rather than certify a
    # revision that is not what will execute.
    is_clean = getattr(resolver, "is_clean", None)
    if callable(is_clean) and not is_clean():
        raise ExecutionAmendmentError(
            "the working tree has uncommitted changes, so the resolved "
            "revision does not describe the code that would execute. "
            "Commit first, then create the amendment."
        )
    protocol_revision = preregistration.source_git_revision
    execution_revision = resolver.current_revision()

    # The system configuration is a tracked repository file, not a freeze
    # artifact, so it can move between revisions independently of the frozen
    # inputs. An orchestration-only claim is false if it did: the pilot would
    # be executing under a different system contract than it was frozen with.
    relativise = getattr(resolver, "repo_relative_path", None)
    system_repo_path = (
        relativise(system_config_path)
        if callable(relativise)
        else Path(system_config_path).name
    )
    protocol_system_sha256 = resolver.file_sha256_at(
        protocol_revision,
        system_repo_path,
    )
    if protocol_system_sha256 is None:
        raise ExecutionAmendmentError(
            f"the system configuration {system_repo_path!r} does not exist "
            f"at the protocol revision {protocol_revision}; an "
            "orchestration-only amendment cannot be created for a system "
            "contract that was not part of the frozen protocol"
        )
    execution_system_sha256 = file_sha256(system_config_path)
    if protocol_system_sha256 != execution_system_sha256:
        raise ExecutionAmendmentError(
            f"the system configuration {system_repo_path!r} changed between "
            f"the protocol revision ({protocol_system_sha256}) and the "
            f"execution revision ({execution_system_sha256}); this is a "
            "change to the system contract, not orchestration, and requires "
            "a new freeze"
        )
    return {
        "schema_version": EXECUTION_AMENDMENT_SCHEMA_VERSION,
        "amendment_id": amendment_id,
        "pilot_id": preregistration.pilot_id,
        "protocol_git_revision": protocol_revision,
        "execution_git_revision": execution_revision,
        "change_classification": change_classification,
        "reason": reason,
        "changed_paths": list(
            resolver.changed_paths(protocol_revision, execution_revision)
        ),
        "system_config_repo_path": system_repo_path,
        "system_config_unchanged_since_protocol_revision": True,
        "plan_equivalence_verified": True,
        "plan_equivalence_statement": (
            "The trial plan recomputed by the execution revision using the "
            "frozen preregistration, workload manifest, endpoint registry, "
            "and system configuration is byte-identical to the frozen plan "
            "document, and the tracked system configuration is unchanged "
            "between the protocol and execution revisions. This evidences "
            "unchanged plan-generating semantics only; it does not "
            "establish that execution, measurement, or scoring behave "
            "identically."
        ),
        **_expected_bound_inputs(
            preregistration,
            registry,
            measurement_manifest_sha256=measurement_manifest_sha256,
            workloads=workloads,
            system_config_path=system_config_path,
            frozen_plan=frozen_plan,
            frozen_plan_path=frozen_plan_path,
        ),
        "credentials_recorded": False,
        "evidence_class": "auditable-compatibility-evidence",
        "not_an_authenticity_or_approval_control": True,
    }


def require_execution_compatibility(
    preregistration: DistributedPilotPreregistration,
    registry: EndpointRegistry,
    *,
    resolver: RevisionResolver,
    measurement_manifest_sha256: str,
    workloads: Mapping[str, Mapping[str, Any]],
    system_config_path: str | Path,
    frozen_plan_path: str | Path | None,
    recomputed_plan: Mapping[str, Any] | None = None,
    amendment_path: str | Path | None = None,
) -> dict[str, Any]:
    """Decide whether this revision may execute this frozen pilot.

    Returns provenance for the run record. Raises before any FlowMesh session
    is opened when the executing revision differs from the preregistered
    protocol revision without valid amendment evidence.
    """
    protocol_revision = preregistration.source_git_revision
    execution_revision = resolver.current_revision()
    provenance: dict[str, Any] = {
        "protocol_git_revision": protocol_revision,
        "execution_git_revision": execution_revision,
        "execution_revision_matches_protocol": (
            execution_revision == protocol_revision
        ),
        "execution_amendment_required": (
            execution_revision != protocol_revision
        ),
        "execution_amendment_sha256": None,
        "execution_amendment_id": None,
        "execution_amendment_path": None,
        "change_classification": None,
        "amendment_evidence_class": None,
    }

    if execution_revision == protocol_revision:
        if amendment_path is not None:
            # Not an error, but say so: an amendment that changes nothing
            # should not look like it authorised something.
            provenance["execution_amendment_note"] = (
                "an amendment was supplied but is not required; the "
                "execution revision equals the protocol revision"
            )
        return provenance

    if amendment_path is None:
        raise ExecutionAmendmentError(
            "this pilot was preregistered at protocol revision "
            f"{protocol_revision} but is executing at {execution_revision}. "
            "Supply --execution-amendment with evidence that the difference "
            "is orchestration-only, or run at the preregistered revision. "
            "Refusing to submit any workflow."
        )

    amendment = load_execution_amendment(amendment_path)
    if amendment.pilot_id != preregistration.pilot_id:
        raise ExecutionAmendmentError(
            f"the amendment is for pilot {amendment.pilot_id!r}, not "
            f"{preregistration.pilot_id!r}"
        )
    if amendment.protocol_git_revision != protocol_revision:
        raise ExecutionAmendmentError(
            "the amendment names protocol revision "
            f"{amendment.protocol_git_revision}, but the preregistration "
            f"declares {protocol_revision}"
        )
    if amendment.execution_git_revision != execution_revision:
        raise ExecutionAmendmentError(
            "the amendment authorises execution revision "
            f"{amendment.execution_git_revision}, but this process is "
            f"running {execution_revision}"
        )
    if frozen_plan_path is None:
        raise ExecutionAmendmentError(
            "a frozen plan document is required to validate an execution "
            "amendment"
        )
    frozen_plan = json.loads(
        Path(frozen_plan_path).read_text(encoding="utf-8")
    )
    expected = _expected_bound_inputs(
        preregistration,
        registry,
        measurement_manifest_sha256=measurement_manifest_sha256,
        workloads=workloads,
        system_config_path=system_config_path,
        frozen_plan=frozen_plan,
        frozen_plan_path=frozen_plan_path,
    )
    mismatched = sorted(
        field
        for field in BOUND_INPUT_FIELDS
        if amendment.bound_inputs[field] != expected[field]
    )
    if mismatched:
        raise ExecutionAmendmentError(
            "the amendment does not describe these inputs; mismatched: "
            + ", ".join(mismatched)
            + ". An amendment is valid only for the exact frozen inputs it "
            "was created from."
        )
    # Re-verified here, not merely trusted from the document: the tracked
    # system configuration could have been edited after the amendment was
    # created, and the amendment is only evidence about what was true then.
    protocol_system_sha256 = resolver.file_sha256_at(
        protocol_revision,
        amendment.system_config_repo_path,
    )
    execution_system_sha256 = file_sha256(system_config_path)
    if protocol_system_sha256 is None:
        raise ExecutionAmendmentError(
            "the system configuration "
            f"{amendment.system_config_repo_path!r} does not exist at the "
            f"protocol revision {protocol_revision}"
        )
    if protocol_system_sha256 != execution_system_sha256:
        raise ExecutionAmendmentError(
            "the system configuration "
            f"{amendment.system_config_repo_path!r} differs between the "
            "protocol and execution revisions; the amendment's "
            "orchestration-only claim does not hold"
        )
    if recomputed_plan is not None:
        equivalent, differences = verify_plan_equivalence(
            recomputed_plan,
            frozen_plan,
        )
        if not equivalent:
            raise ExecutionAmendmentError(
                "the plan recomputed by this revision differs from the "
                "frozen plan, so the amendment's equivalence claim does not "
                "hold here. Differences: " + "; ".join(differences)
            )
    provenance.update({
        "execution_amendment_sha256": amendment.source_sha256,
        "execution_amendment_id": amendment.amendment_id,
        "execution_amendment_path": str(amendment.source_path),
        "change_classification": amendment.change_classification,
        "amendment_evidence_class": "auditable-compatibility-evidence",
        "amendment_reason": amendment.reason,
        "amendment_changed_paths": list(amendment.changed_paths),
        "amendment_is_not_authenticity_or_approval": True,
        "system_config_repo_path": amendment.system_config_repo_path,
        "system_config_unchanged_since_protocol_revision": True,
    })
    return provenance


def bind_amendment_to_run(
    output_dir: str | Path,
    amendment_path: str | Path | None,
) -> str | None:
    """Copy the accepted amendment into the run, or require the same one.

    Written once, then required to match byte for byte. Swapping in a
    different amendment on resume would let a run that started under one
    compatibility claim finish under another.
    """
    target = Path(output_dir) / AMENDMENT_DOCUMENT
    if amendment_path is None:
        if target.is_file():
            raise ExecutionAmendmentError(
                "this run was started with an execution amendment "
                f"({target}); resuming without one is refused"
            )
        return None
    payload = Path(amendment_path).read_bytes()
    digest = sha256(payload).hexdigest()
    if target.is_file():
        existing = target.read_bytes()
        if sha256(existing).hexdigest() != digest:
            raise ExecutionAmendmentError(
                "the execution amendment changed since this run started; "
                "refusing to resume under different compatibility evidence"
            )
        return digest
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(payload)
    return digest
