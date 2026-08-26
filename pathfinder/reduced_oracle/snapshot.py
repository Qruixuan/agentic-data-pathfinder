"""One shared content digest over a frozen Reduced Oracle output directory.

Several read-only analysis tools need to record *which* frozen Oracle they
read. They must agree on the algorithm, and they must not be able to publish
two different digests under the same field name when they actually covered
different files. This module owns the algorithm; callers declare their scope
and receive it back alongside the digest so the two can never be separated.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Iterable


ORACLE_SNAPSHOT_ALGORITHM = "pathfinder.reduced-oracle-snapshot-sha256/v1"


class OracleSnapshotError(ValueError):
    """Raised when a declared snapshot input is absent."""


@dataclass(frozen=True)
class OracleSnapshot:
    """A digest plus the exact scope it was computed over."""

    algorithm: str
    sha256: str
    scope: str
    design_ids: tuple[str, ...]
    relative_paths: tuple[str, ...]

    def to_manifest_fields(self, prefix: str) -> dict[str, object]:
        return {
            f"{prefix}_sha256": self.sha256,
            f"{prefix}_algorithm": self.algorithm,
            f"{prefix}_scope": self.scope,
            f"{prefix}_design_ids": list(self.design_ids),
            f"{prefix}_file_count": len(self.relative_paths),
        }


def reduced_oracle_snapshot(
    oracle_output_dir: str | Path,
    design_ids: Iterable[str],
    *,
    scope: str,
) -> OracleSnapshot:
    """Digest the Oracle table, manifest, and named design record files.

    The digest binds each file's repository-relative path to its content, so
    renaming a design directory changes the result. ``design_ids`` is the
    scope: two tools that read different design subsets legitimately produce
    different digests, which is why ``scope`` travels with the value.
    """
    root = Path(oracle_output_dir).resolve()
    ordered_designs = tuple(design_ids)
    if not ordered_designs:
        raise OracleSnapshotError("snapshot scope cannot be empty")
    paths = [root / "oracle_table.csv"]
    manifest = root / "oracle_manifest.json"
    if manifest.is_file():
        paths.append(manifest)
    paths.extend(
        root / "designs" / design_id / "runs.jsonl"
        for design_id in ordered_designs
    )
    digest = sha256()
    relative_paths: list[str] = []
    for path in sorted(paths, key=lambda value: value.as_posix()):
        if not path.is_file():
            raise OracleSnapshotError(
                f"Reduced Oracle snapshot input does not exist: {path}"
            )
        relative = path.relative_to(root).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(sha256(path.read_bytes()).digest())
        relative_paths.append(relative)
    return OracleSnapshot(
        algorithm=ORACLE_SNAPSHOT_ALGORITHM,
        sha256=digest.hexdigest(),
        scope=scope,
        design_ids=ordered_designs,
        relative_paths=tuple(relative_paths),
    )
