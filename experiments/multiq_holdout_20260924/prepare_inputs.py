"""Stage fresh public inputs; no workflows, provider inference or labels."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
from time import monotonic

from experiments.fresh_multiq_cohort import canonical


ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = ROOT / "artifacts"
SELECTION = ARTIFACTS / "multiq-fresh-holdout-20260924-v3-public-selection"
OUTPUT = ARTIFACTS / "h48-inputs-v2"
ARCHIVE_URL = (
    "https://huggingface.co/datasets/rhymes-ai/NeXTVideo/resolve/"
    "7e8ea8e056742292b95688d92a0773e05df00393/NExTVideo.zip"
)


def read_selection() -> tuple[dict, str]:
    raw = (SELECTION / "public-selection.json").read_bytes()
    sha = hashlib.sha256(raw).hexdigest()
    if (SELECTION / "SHA256SUMS").read_bytes() != (
        f"{sha}  public-selection.json\n".encode()
    ):
        raise ValueError("public selection checksum differs")
    doc = json.loads(raw)
    if len(doc["tasks"]) != 12 or len(doc["selected_object_ids"]) != 4:
        raise ValueError("selection shape differs")
    return doc, sha


def write(path: Path, value: object) -> None:
    with path.open("xb") as handle:
        handle.write(canonical(value) + b"\n")


def media() -> None:
    doc, source_sha = read_selection()
    output = OUTPUT / "media"
    output.mkdir(parents=True, exist_ok=True)
    if (output / "media.json").exists():
        raise ValueError("media receipt is already frozen")
    helper = ROOT / ".codex_build/rsi-formal-source/selective_remote_zip.py"
    spec = importlib.util.spec_from_file_location("selective_zip", helper)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    started = datetime.now(timezone.utc).isoformat()
    clock = monotonic()
    reader = module.RangeReader(ARCHIVE_URL)
    try:
        entries = module.central_directory(reader)
        rows = []
        for object_id in doc["selected_object_ids"]:
            video_id = object_id.removeprefix("nextqa-val-")
            candidates = [e for e in entries.values()
                          if Path(e.name).name == video_id + ".mp4"]
            if len(candidates) != 1:
                raise ValueError("selected video has no unique pinned archive entry")
            entry = candidates[0]
            if entry.uncompressed_size > 40 * 1024 * 1024:
                raise ValueError("selected video exceeds pre-inference staging bound")
            target = output / (video_id + ".mp4")
            if not target.exists():
                module.extract(reader, entry, target)
            payload = target.read_bytes()
            if (len(payload) != entry.uncompressed_size
                    or module.binascii.crc32(payload) & 0xFFFFFFFF != entry.crc32):
                raise ValueError("persisted video differs from archive entry")
            rows.append({"object_id": object_id, "filename": target.name,
                         "archive_entry": entry.name, "bytes": len(payload),
                         "sha256": hashlib.sha256(payload).hexdigest()})
            print(json.dumps({"downloaded": object_id, "bytes": len(payload)}),
                  flush=True)
        write(output / "media.json", {
            "selection_sha256": source_sha, "archive_url": ARCHIVE_URL,
            "started_utc": started, "finished_utc": datetime.now(timezone.utc).isoformat(),
            "elapsed_seconds": monotonic() - clock, "objects": rows,
            "llm_called": False, "credentials_recorded": False,
        })
        with (output / "SHA256SUMS").open("xb") as handle:
            for path in sorted(output.iterdir()):
                if path.name != "SHA256SUMS":
                    handle.write(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n".encode())
    finally:
        reader.client.close()


def inventory() -> None:
    output = ARTIFACTS / "h48-archive-inventory-v1"
    if output.exists():
        raise ValueError("archive inventory already exists")
    helper = ROOT / ".codex_build/rsi-formal-source/selective_remote_zip.py"
    spec = importlib.util.spec_from_file_location("selective_zip", helper)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    reader = module.RangeReader(ARCHIVE_URL)
    try:
        entries = module.central_directory(reader)
        rows = {}
        for entry in entries.values():
            path = Path(entry.name)
            if path.suffix != ".mp4" or not path.stem.isdigit():
                continue
            oid = "nextqa-val-" + path.stem
            if oid in rows:
                raise ValueError("archive video ID is not unique")
            rows[oid] = {"bytes": entry.uncompressed_size,
                         "crc32": entry.crc32, "entry": entry.name}
        output.mkdir()
        write(output / "inventory.json", {"archive_url": ARCHIVE_URL,
                                           "objects": rows})
        raw = (output / "inventory.json").read_bytes()
        with (output / "SHA256SUMS").open("xb") as handle:
            handle.write(f"{hashlib.sha256(raw).hexdigest()}  inventory.json\n".encode())
        print(json.dumps({"archive_video_count": len(rows),
                          "inventory_sha256": hashlib.sha256(raw).hexdigest()}))
    finally:
        reader.client.close()


def plan() -> None:
    from pathfinder.distributed.scoring import MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE
    from pathfinder.simulator.hidden_oracle import build_n1_public_task_binding
    from pathfinder.rsi_exam.interleaved_multiq_plan import freeze_interleaved_plan

    doc, source_sha = read_selection()
    tasks = []
    for row in doc["tasks"]:
        binding = build_n1_public_task_binding(
            workload_id=row["question_id"], object_id=row["object_id"],
            task_class_id=row["stratum"], question=row["question"],
            answer_options=row["answer_options"],
            success_scoring_rule=MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
        )
        tasks.append({**row, "public_task_sha256": binding["task_binding_sha256"]})
    report = freeze_interleaved_plan(
        tasks, seed="pathfinder-fresh-multiq-order-20260924-v1",
        experiment_id="fresh-multiq-holdout-20260924-v2",
        public_source_sha256=source_sha, output_dir=OUTPUT / "plan",
    )
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("media", "plan", "inventory"))
    args = parser.parse_args()
    try:
        {"media": media, "plan": plan, "inventory": inventory}[args.phase]()
    except Exception as exc:
        # HTTP redirect URLs can be signed. Never include the raw exception.
        print(json.dumps({"status": "FAILED", "phase": args.phase,
                          "error_class": type(exc).__name__}))
        raise SystemExit(2) from None
