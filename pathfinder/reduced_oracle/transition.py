from __future__ import annotations

import json
import os
import shutil
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any

from .contracts import OracleDesignSpec, ReducedOracleConfig


TRANSITION_STATE_SCHEMA_VERSION = (
    "pathfinder.reduced-oracle-transition-state/v1alpha1"
)
TRANSITION_OBSERVATION_SCHEMA_VERSION = (
    "pathfinder.reduced-oracle-transition/v1alpha1"
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(payload: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


@dataclass(frozen=True)
class TransitionObservation:
    schema_version: str
    transition_id: str
    transition_type: str
    from_design_id: str | None
    to_design_id: str
    started_at: str
    finished_at: str
    elapsed_seconds: float
    copied_bytes: int
    removed_bytes: int
    active_materialized_bytes: int
    files_created: int
    files_reused: int
    files_removed: int
    foreground_loss: float
    realized_transition_cost: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class FilesystemTransitionExecutor:
    """Materialize reduced designs without deleting unowned files.

    Every writable target must remain below the configured materialization
    root. Existing matching files are reused but never claimed or removed.
    Files created by this executor are fingerprinted in its state; restoration
    removes them only while their content still matches that fingerprint.
    """

    def __init__(
        self,
        config: ReducedOracleConfig,
        *,
        object_ids: tuple[str, ...],
        runtime_dir: str | Path,
    ) -> None:
        self.config = config
        self.object_ids = object_ids
        self.runtime_dir = Path(runtime_dir).resolve()
        self.state_path = self.runtime_dir / "transition_state.json"
        self.active_plan_path = self.runtime_dir / "active_plan.json"
        self.config.materialization_root.mkdir(parents=True, exist_ok=True)

    def _default_state(self) -> dict[str, Any]:
        return {
            "schema_version": TRANSITION_STATE_SCHEMA_VERSION,
            "active_design_id": None,
            "owned_files": {},
        }

    def _load_state(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return self._default_state()
        try:
            state = json.loads(self.state_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"transition state is invalid JSON: {self.state_path}"
            ) from exc
        if state.get("schema_version") != TRANSITION_STATE_SCHEMA_VERSION:
            raise RuntimeError("transition state has an unsupported schema")
        if not isinstance(state.get("owned_files"), dict):
            raise RuntimeError("transition state owned_files must be an object")
        return state

    def _persist_state(self, state: dict[str, Any]) -> None:
        _write_json(state, self.state_path)

    def active_design_id(self) -> str | None:
        value = self._load_state().get("active_design_id")
        return str(value) if value is not None else None

    def _render_path(self, template: str, object_id: str) -> Path:
        try:
            rendered = template.format(object_id=object_id)
        except (KeyError, ValueError) as exc:
            raise RuntimeError(
                f"invalid materialization path template: {template}"
            ) from exc
        path = Path(rendered)
        if not path.is_absolute():
            path = self.config.source_path.parent / path
        return path.resolve()

    def _require_safe_target(self, target: Path) -> None:
        try:
            target.relative_to(self.config.materialization_root)
        except ValueError as exc:
            raise RuntimeError(
                "materialization target escapes materialization_root: "
                f"{target}"
            ) from exc
        if target == self.config.materialization_root:
            raise RuntimeError("materialization target cannot be the root")

    def _desired_files(
        self,
        design: OracleDesignSpec,
    ) -> dict[Path, tuple[Path, str, str]]:
        desired: dict[Path, tuple[Path, str, str]] = {}
        for materialization in design.materializations:
            for object_id in self.object_ids:
                source = self._render_path(
                    materialization.source_template,
                    object_id,
                )
                target = self._render_path(
                    materialization.target_template,
                    object_id,
                )
                self._require_safe_target(target)
                if source == target:
                    raise RuntimeError(
                        "materialization source and target cannot be identical"
                    )
                if target in desired:
                    raise RuntimeError(f"duplicate materialization target: {target}")
                if not source.is_file():
                    raise RuntimeError(
                        f"materialization source is not a file: {source}"
                    )
                desired[target] = (
                    source,
                    object_id,
                    materialization.representation_id,
                )
        return desired

    def transition(
        self,
        to_design_id: str,
        *,
        transition_type: str,
    ) -> TransitionObservation:
        if transition_type not in {"activate", "forward", "restore"}:
            raise ValueError(f"unsupported transition_type: {transition_type}")
        design = self.config.designs[to_design_id]
        state = self._load_state()
        from_design_id = state.get("active_design_id")
        desired = self._desired_files(design)
        owned: dict[str, dict[str, Any]] = state["owned_files"]
        started_at = _utc_now()
        started = time.monotonic()
        copied_bytes = 0
        removed_bytes = 0
        files_created = 0
        files_reused = 0
        files_removed = 0

        desired_targets = {str(path) for path in desired}
        for target, (source, object_id, representation_id) in desired.items():
            expected_sha256 = _sha256_file(source)
            expected_size = source.stat().st_size
            existing_owned = owned.get(str(target))
            if target.exists():
                if not target.is_file():
                    raise RuntimeError(
                        f"materialization target is not a file: {target}"
                    )
                if _sha256_file(target) != expected_sha256:
                    raise RuntimeError(
                        "refusing to overwrite a mismatched materialization: "
                        f"{target}"
                    )
                if existing_owned is not None:
                    existing_owned["status"] = "ready"
                    self._persist_state(state)
                else:
                    files_reused += 1
                continue

            target.parent.mkdir(parents=True, exist_ok=True)
            owned[str(target)] = {
                "sha256": expected_sha256,
                "size_bytes": expected_size,
                "source": str(source),
                "object_id": object_id,
                "representation_id": representation_id,
                "status": "preparing",
            }
            self._persist_state(state)
            temporary = target.with_name(
                f".{target.name}.{uuid.uuid4().hex}.tmp"
            )
            try:
                shutil.copy2(source, temporary)
                # Windows rejects fsync on a read-only descriptor. Opening
                # read/write preserves the bytes while keeping durability
                # behaviour consistent across supported platforms.
                with temporary.open("rb+") as handle:
                    os.fsync(handle.fileno())
                if _sha256_file(temporary) != expected_sha256:
                    raise RuntimeError(
                        f"copied materialization failed digest check: {target}"
                    )
                os.replace(temporary, target)
            finally:
                temporary.unlink(missing_ok=True)
            owned[str(target)]["status"] = "ready"
            self._persist_state(state)
            copied_bytes += expected_size
            files_created += 1

        for raw_target in sorted(set(owned) - desired_targets):
            target = Path(raw_target).resolve()
            self._require_safe_target(target)
            metadata = owned[raw_target]
            if target.exists():
                if not target.is_file():
                    raise RuntimeError(
                        f"owned materialization is not a file: {target}"
                    )
                if _sha256_file(target) != metadata.get("sha256"):
                    raise RuntimeError(
                        "refusing to remove a modified materialization: "
                        f"{target}"
                    )
                removed_bytes += target.stat().st_size
                target.unlink()
                files_removed += 1
            del owned[raw_target]
            self._persist_state(state)

        state["active_design_id"] = to_design_id
        self._persist_state(state)
        active_bytes = sum(
            target.stat().st_size
            for target in desired
            if target.exists()
        )
        _write_json(
            {
                "schema_version": TRANSITION_STATE_SCHEMA_VERSION,
                "active_design_id": to_design_id,
                "activated_at": _utc_now(),
                "materialization_decision": design.materialization_decision,
                "placement_decision": design.placement_decision,
                "execution_decision": design.execution_decision,
                "materialized_paths": sorted(str(path) for path in desired),
            },
            self.active_plan_path,
        )
        elapsed = time.monotonic() - started
        gib = 1024**3
        transition_cost = (
            copied_bytes / gib * self.config.transition_cost.copy_cost_per_gib
            + elapsed
            * self.config.transition_cost.elapsed_time_cost_per_second
            + self.config.transition_cost.foreground_loss_per_transition
        )
        return TransitionObservation(
            schema_version=TRANSITION_OBSERVATION_SCHEMA_VERSION,
            transition_id=str(uuid.uuid4()),
            transition_type=transition_type,
            from_design_id=(
                str(from_design_id) if from_design_id is not None else None
            ),
            to_design_id=to_design_id,
            started_at=started_at,
            finished_at=_utc_now(),
            elapsed_seconds=round(elapsed, 9),
            copied_bytes=copied_bytes,
            removed_bytes=removed_bytes,
            active_materialized_bytes=active_bytes,
            files_created=files_created,
            files_reused=files_reused,
            files_removed=files_removed,
            foreground_loss=(
                self.config.transition_cost.foreground_loss_per_transition
            ),
            realized_transition_cost=round(transition_cost, 9),
        )
