"""Freeze public media and a four-arm plan for a selected development cohort.

This stages inputs only: no oracle labels, inference, or workflow submission.
The selected public cohort is immutable and independent of observed outcomes.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import sys

from experiments.fresh_multiq_cohort import canonical


ARCHIVE_URL = (
    "https://huggingface.co/datasets/rhymes-ai/NeXTVideo/resolve/"
    "7e8ea8e056742292b95688d92a0773e05df00393/NExTVideo.zip"
)


def _selection(root: Path, *, expected_objects: int = 2,
               expected_questions: int = 6) -> tuple[dict, str]:
    raw = (root / "public-selection.json").read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    if (root / "SHA256SUMS").read_bytes() != (
        f"{digest}  public-selection.json\n".encode()
    ):
        raise ValueError("public selection checksum differs")
    doc = json.loads(raw)
    if (len(doc["selected_object_ids"]) != expected_objects
            or len(doc["tasks"]) != expected_questions
            or doc["selection_uses_answers"] is not False
            or doc["label_values_included"] is not False):
        raise ValueError("public selection contract differs")
    return doc, digest


def _write_new(path: Path, value: object) -> None:
    with path.open("xb") as handle:
        handle.write(canonical(value) + b"\n")


def _zip_module(path: Path):
    spec = importlib.util.spec_from_file_location("selective_zip", path)
    if spec is None or spec.loader is None:
        raise ValueError("pinned archive reader is unavailable")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def freeze_media(selection_root: Path, output_root: Path,
                 archive_reader: Path, *, expected_objects: int = 2,
                 expected_questions: int = 6,
                 min_video_bytes: int = 1_500_000,
                 max_video_bytes: int = 7_000_000) -> None:
    if (type(min_video_bytes) is not int
            or type(max_video_bytes) is not int
            or not 0 < min_video_bytes <= max_video_bytes):
        raise ValueError("frozen media byte bounds are invalid")
    doc, selection_sha = _selection(
        selection_root, expected_objects=expected_objects,
        expected_questions=expected_questions,
    )
    target = output_root / "media"
    if target.exists():
        raise ValueError("media directory already exists")
    target.mkdir(parents=True)
    module = _zip_module(archive_reader)
    reader = module.RangeReader(ARCHIVE_URL)
    try:
        entries = module.central_directory(reader)
        objects = []
        for object_id in doc["selected_object_ids"]:
            video_id = object_id.removeprefix("nextqa-val-")
            matches = [entry for entry in entries.values()
                       if Path(entry.name).name == video_id + ".mp4"]
            if len(matches) != 1:
                raise ValueError("selected object has no unique archive entry")
            entry = matches[0]
            if not min_video_bytes <= entry.uncompressed_size <= max_video_bytes:
                raise ValueError("selected media size differs from frozen rule")
            artifact = target / (video_id + ".mp4")
            module.extract(reader, entry, artifact)
            payload = artifact.read_bytes()
            if (len(payload) != entry.uncompressed_size
                    or module.binascii.crc32(payload) & 0xFFFFFFFF != entry.crc32):
                raise ValueError("downloaded media differs from archive entry")
            objects.append({"object_id": object_id, "filename": artifact.name,
                            "archive_entry": entry.name,
                            "bytes": len(payload),
                            "sha256": hashlib.sha256(payload).hexdigest()})
        _write_new(target / "media.json", {
            "selection_sha256": selection_sha,
            "archive_url": ARCHIVE_URL,
            "objects": objects,
            "llm_called": False,
            "credentials_recorded": False,
        })
        with (target / "SHA256SUMS").open("xb") as handle:
            for artifact in sorted(target.iterdir()):
                if artifact.name != "SHA256SUMS":
                    handle.write(
                        f"{hashlib.sha256(artifact.read_bytes()).hexdigest()}  "
                        f"{artifact.name}\n".encode()
                    )
    finally:
        reader.client.close()


def freeze_plan(selection_root: Path, output_root: Path) -> None:
    from pathfinder.distributed.scoring import (
        MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
    )
    from pathfinder.rsi_exam.interleaved_multiq_plan import freeze_interleaved_plan
    from pathfinder.simulator.hidden_oracle import build_n1_public_task_binding

    doc, selection_sha = _selection(selection_root)
    rows = []
    for row in doc["tasks"]:
        binding = build_n1_public_task_binding(
            workload_id=row["question_id"], object_id=row["object_id"],
            task_class_id=row["stratum"], question=row["question"],
            answer_options=row["answer_options"],
            success_scoring_rule=MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
        )
        rows.append({**row, "public_task_sha256":
                     binding["task_binding_sha256"]})
    report = freeze_interleaved_plan(
        rows, seed="pathfinder-relational-dev-order-20260924-v1",
        experiment_id="relational-multiq-development-20260924-v1",
        public_source_sha256=selection_sha,
        output_dir=output_root / "plan",
    )
    print(json.dumps(report, sort_keys=True))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("media", "plan"))
    parser.add_argument("--selection-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--archive-reader", type=Path)
    args = parser.parse_args()
    if args.phase == "media":
        if args.archive_reader is None:
            parser.error("media requires --archive-reader")
        freeze_media(args.selection_dir, args.output_dir, args.archive_reader)
    else:
        freeze_plan(args.selection_dir, args.output_dir)


if __name__ == "__main__":
    main()
