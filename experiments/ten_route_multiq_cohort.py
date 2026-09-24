"""Freeze and check a video-disjoint public cohort for ten-route multiq.

Selection itself remains in :mod:`experiments.fresh_multiq_cohort` and runs
on N1. This adapter binds its existing public-only selector to all prior
publicly exposed video IDs; it never opens the official answer column.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys

from experiments.fresh_multiq_cohort import STRATA, canonical
from experiments.complexity_dev_inputs import freeze_media
from pathfinder.distributed.scoring import (
    MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
)
from pathfinder.rsi_exam.ten_route_multiq_plan import (
    freeze_ten_route_multiq_plan,
)
from pathfinder.simulator.hidden_oracle import build_n1_public_task_binding


PROTOCOL = "selection-protocol.json"
EXPOSURE = "exposure-inventory.json"
SELECTION = "public-selection.json"
CHECKSUMS = "SHA256SUMS"
SEED = "pathfinder-ten-route-multiq-sealed-20260925-v1"


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _checked(directory: Path, name: str) -> bytes:
    expected = b"".join(
        f"{_sha((directory / item).read_bytes())}  {item}\n".encode("ascii")
        for item in sorted(path.name for path in directory.iterdir()
                           if path.name != CHECKSUMS)
    )
    if (directory / CHECKSUMS).read_bytes() != expected:
        raise ValueError("source package checksum or file set differs")
    return (directory / name).read_bytes()


def freeze_protocol(
    *, prior_protocol_dir: Path, public_selection_dirs: list[Path],
    output_dir: Path,
) -> dict:
    """Use immutable public exposures, never outcomes, for a fresh cohort."""

    if output_dir.exists():
        raise ValueError("immutable ten-route protocol already exists")
    prior_protocol = json.loads(_checked(prior_protocol_dir, PROTOCOL))
    exposure = json.loads(_checked(prior_protocol_dir, EXPOSURE))
    if (prior_protocol.get("schema_version")
            != "pathfinder.fresh-multiq-selection/v2"
            or prior_protocol.get("excluded_object_ids")
            != exposure.get("object_ids")
            or prior_protocol.get("max_direct_video_bytes") != 7_000_000):
        raise ValueError("prior public exposure protocol differs")
    excluded = set(exposure["object_ids"])
    sources = [{
        "path": str(prior_protocol_dir),
        "sha256": _sha((prior_protocol_dir / EXPOSURE).read_bytes()),
    }]
    for root in public_selection_dirs:
        raw = _checked(root, SELECTION)
        selection = json.loads(raw)
        if (selection.get("label_values_included") is not False
                or selection.get("selection_uses_answers") is not False
                or not isinstance(selection.get("selected_object_ids"), list)):
            raise ValueError("prior selection is not public-only")
        excluded.update(selection["selected_object_ids"])
        sources.append({"path": str(root), "sha256": _sha(raw)})
    if len(public_selection_dirs) < 2 or len(excluded) <= len(
        exposure["object_ids"]
    ):
        raise ValueError("later public exposure sources are incomplete")
    public_exposure = {
        "schema_version": "pathfinder.ten-route-multiq-exposure/v1",
        "object_ids": sorted(excluded),
        "sources": sources,
        "label_values_included": False,
        "credentials_recorded": False,
    }
    protocol = {
        **prior_protocol,
        "schema_version": "pathfinder.fresh-multiq-selection/v3",
        "seed": SEED,
        "object_count": 3,
        "questions_per_video": 2,
        "excluded_object_ids": sorted(excluded),
        "strata": list(STRATA),
    }
    output_dir.mkdir(parents=True, exist_ok=False)
    for name, document in ((EXPOSURE, public_exposure),
                           (PROTOCOL, protocol)):
        (output_dir / name).write_bytes(canonical(document) + b"\n")
    checksums = b"".join(
        f"{_sha((output_dir / name).read_bytes())}  {name}\n".encode("ascii")
        for name in sorted((EXPOSURE, PROTOCOL))
    )
    (output_dir / CHECKSUMS).write_bytes(checksums)
    return {
        "status": "PUBLIC_TEN_ROUTE_PROTOCOL_FROZEN",
        "excluded_video_count": len(excluded),
        "protocol_sha256": _sha(canonical(protocol)),
        "exposure_inventory_sha256": _sha(
            (output_dir / EXPOSURE).read_bytes()
        ),
        "credentials_recorded": False,
    }


def freeze_selection(*, protocol_dir: Path, selection_bytes: bytes,
                     output_dir: Path) -> dict:
    """Accept only the existing N1 selector's public, source-bound output."""

    if output_dir.exists():
        raise ValueError("immutable public selection already exists")
    protocol = json.loads(_checked(protocol_dir, PROTOCOL))
    report = json.loads(selection_bytes)
    rows = report.get("tasks")
    selected = report.get("selected_object_ids")
    if (report.get("protocol_sha256") != _sha(canonical(protocol))
            or report.get("official_csv_sha256")
            != protocol["official_csv_sha256"]
            or report.get("selection_uses_answers") is not False
            or report.get("label_values_included") is not False
            or report.get("credentials_recorded") is not False
            or not isinstance(selected, list) or len(selected) != 3
            or len(set(selected)) != 3
            or set(selected) & set(protocol["excluded_object_ids"])
            or not isinstance(rows, list) or len(rows) != 6):
        raise ValueError("public N1 selection differs from frozen protocol")
    by_video: dict[str, set[str]] = {object_id: set()
                                      for object_id in selected}
    for row in rows:
        if (set(row) != {"question_id", "object_id", "stratum",
                         "question", "answer_options"}
                or row["object_id"] not in by_video
                or row["stratum"] not in STRATA):
            raise ValueError("selection has a non-public or invalid task")
        by_video[row["object_id"]].add(row["stratum"])
    if (any(len([row for row in rows if row["object_id"] == object_id]) != 2
            for object_id in selected)
            or {stratum: sum(row["stratum"] == stratum for row in rows)
                for stratum in STRATA} != {stratum: 2 for stratum in STRATA}):
        raise ValueError("selection is not balanced three-video/two-question")
    output_dir.mkdir(parents=True, exist_ok=False)
    payload = canonical(report) + b"\n"
    (output_dir / SELECTION).write_bytes(payload)
    (output_dir / CHECKSUMS).write_bytes(
        f"{_sha(payload)}  {SELECTION}\n".encode("ascii")
    )
    return {"status": "PUBLIC_TEN_ROUTE_COHORT_FROZEN",
            "object_count": 3, "question_count": 6,
            "selection_sha256": _sha(payload),
            "label_values_included": False, "credentials_recorded": False}


def freeze_plan(*, protocol_dir: Path, selection_dir: Path,
                experiment_id: str, output_dir: Path) -> dict:
    """Add only existing public task commitments to the frozen selection."""

    protocol = json.loads(_checked(protocol_dir, PROTOCOL))
    exposure_raw = _checked(protocol_dir, EXPOSURE)
    selected = json.loads(_checked(selection_dir, SELECTION))
    if selected.get("protocol_sha256") != _sha(canonical(protocol)):
        raise ValueError("public selection and protocol differ")
    questions = []
    for row in selected["tasks"]:
        public = build_n1_public_task_binding(
            workload_id=row["question_id"], object_id=row["object_id"],
            task_class_id=row["stratum"], question=row["question"],
            answer_options=row["answer_options"],
            success_scoring_rule=(
                MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE
            ),
        )
        questions.append({**row,
                          "public_task_sha256": public["task_binding_sha256"]})
    return freeze_ten_route_multiq_plan(
        questions, seed=protocol["seed"], experiment_id=experiment_id,
        public_source_sha256=protocol["official_csv_sha256"],
        exposure_inventory_sha256=_sha(exposure_raw), output_dir=output_dir,
    )


def freeze_selected_media(*, protocol_dir: Path, selection_dir: Path,
                          archive_reader: Path, output_dir: Path) -> dict:
    """Reuse the existing selective archive reader under this cohort's bound."""

    protocol = json.loads(_checked(protocol_dir, PROTOCOL))
    report = json.loads(_checked(selection_dir, SELECTION))
    if (report.get("protocol_sha256") != _sha(canonical(protocol))
            or protocol.get("max_direct_video_bytes") != 7_000_000):
        raise ValueError("media and public selection binding differ")
    freeze_media(
        selection_dir, output_dir, archive_reader,
        expected_objects=3, expected_questions=6, min_video_bytes=1,
        max_video_bytes=protocol["max_direct_video_bytes"],
    )
    media_dir = output_dir / "media"
    document = json.loads(_checked(media_dir, "media.json"))
    if (document.get("selection_sha256")
            != _sha((selection_dir / SELECTION).read_bytes())
            or len(document.get("objects", [])) != 3):
        raise ValueError("selectively downloaded media differs from cohort")
    return {"status": "PUBLIC_TEN_ROUTE_MEDIA_FROZEN",
            "object_count": 3, "total_video_bytes": sum(
                row["bytes"] for row in document["objects"]),
            "credentials_recorded": False}


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="action", required=True)
    protocol = sub.add_parser("freeze-protocol")
    protocol.add_argument("--prior-protocol-dir", type=Path, required=True)
    protocol.add_argument("--public-selection-dir", type=Path,
                          action="append", required=True)
    protocol.add_argument("--output-dir", type=Path, required=True)
    selection = sub.add_parser("freeze-selection")
    selection.add_argument("--protocol-dir", type=Path, required=True)
    selection.add_argument("--output-dir", type=Path, required=True)
    plan = sub.add_parser("freeze-plan")
    plan.add_argument("--protocol-dir", type=Path, required=True)
    plan.add_argument("--selection-dir", type=Path, required=True)
    plan.add_argument("--experiment-id", required=True)
    plan.add_argument("--output-dir", type=Path, required=True)
    media = sub.add_parser("freeze-media")
    media.add_argument("--protocol-dir", type=Path, required=True)
    media.add_argument("--selection-dir", type=Path, required=True)
    media.add_argument("--archive-reader", type=Path, required=True)
    media.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.action == "freeze-protocol":
            result = freeze_protocol(
                prior_protocol_dir=args.prior_protocol_dir,
                public_selection_dirs=args.public_selection_dir,
                output_dir=args.output_dir,
            )
        elif args.action == "freeze-selection":
            result = freeze_selection(
                protocol_dir=args.protocol_dir,
                selection_bytes=sys.stdin.buffer.read(),
                output_dir=args.output_dir,
            )
        elif args.action == "freeze-plan":
            result = freeze_plan(
                protocol_dir=args.protocol_dir,
                selection_dir=args.selection_dir,
                experiment_id=args.experiment_id,
                output_dir=args.output_dir,
            )
        else:
            result = freeze_selected_media(
                protocol_dir=args.protocol_dir,
                selection_dir=args.selection_dir,
                archive_reader=args.archive_reader,
                output_dir=args.output_dir,
            )
    except Exception as exc:
        # Archive redirects can contain signed query strings; fail closed
        # without rendering exception text to an operator console or log.
        print(json.dumps({"status": "FAILED",
                          "error_class": type(exc).__name__}))
        raise SystemExit(2) from None
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
