"""Build a public-only, video-disjoint NExT-QA ATP-Hard pilot dataset.

This is cohort preparation, not an experiment admission. Official answer
columns are never copied to the output. Grounding annotations are used only
to require that each selected question has an independently annotated span;
the spans themselves are not exported to the route inputs.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
import shutil
import zlib


PUBLIC_FIELDS = ("question", "type", "a0", "a1", "a2", "a3", "a4")


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      allow_nan=False, separators=(",", ":")).encode("utf-8")


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def rank(seed: str, domain: str, value: str) -> str:
    return sha256(f"{seed}|{domain}|{value}".encode("utf-8"))


def rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def checked_sources(paths: dict[str, Path], expected: dict[str, str]) -> dict:
    if set(paths) != set(expected):
        raise ValueError("source digest declarations are incomplete")
    actual = {name: sha256(path.read_bytes()) for name, path in paths.items()}
    if actual != expected:
        raise ValueError("source digest differs from selection protocol")
    return actual


def public_hard_rows(official: list[dict[str, str]],
                     hard: list[dict[str, str]]) -> dict[str, dict]:
    lookup: dict[str, dict[str, str]] = {}
    for row in official:
        key = f"{row['video']}|{row['qid']}"
        if key in lookup:
            raise ValueError("duplicate official video/question identity")
        lookup[key] = row
    result = {}
    for row in hard:
        key = f"{row['video']}|{row['qid']}"
        if key in result or key not in lookup:
            raise ValueError("hard subset has duplicate or unknown identity")
        if any(row[field] != lookup[key][field] for field in PUBLIC_FIELDS):
            raise ValueError("hard subset differs from official public fields")
        if not row["type"].startswith(("C", "T")):
            raise ValueError("hard subset contains unsupported question type")
        result[key] = {
            "question_id": f"nextqa-val-{row['video']}-q{row['qid']}",
            "object_id": f"nextqa-val-{row['video']}",
            "stratum": ("causal" if row["type"].startswith("C")
                        else "temporal"),
            "question_type": row["type"],
            "question": row["question"],
            "answer_options": [
                {"option_id": chr(65 + i), "text": row[f"a{i}"]}
                for i in range(5)
            ],
        }
    return result


def select(official: list[dict[str, str]], hard: list[dict[str, str]],
           grounding: dict, inventory: dict, exposed: set[str],
           config: dict) -> dict:
    tasks = public_hard_rows(official, hard)
    candidates: dict[str, list[dict]] = {}
    for key, task in tasks.items():
        video, qid = key.split("|", 1)
        media = inventory["objects"].get(task["object_id"])
        spans = grounding.get(video, {}).get("location", {}).get(qid)
        if (media is None or not spans
                or not config["min_video_bytes"] <= media["bytes"]
                <= config["max_video_bytes"]):
            continue
        candidates.setdefault(video, []).append(task)

    seed = config["seed"]
    questions_per_video = config.get("questions_per_video", 3)
    development = []
    dev_videos = set()
    for spec in config["development"]:
        video = spec["video"]
        if video in dev_videos:
            raise ValueError("duplicate development video")
        dev_videos.add(video)
        pool = candidates.get(video, [])
        by_qid = {task["question_id"].split("-q")[-1]: task
                  for task in pool}
        required = [by_qid[qid] for qid in spec["required_qids"]]
        if len(set(spec["required_qids"])) != len(required):
            raise ValueError("duplicate required question")
        chosen = list(required)
        if not any(row["stratum"] == "causal" for row in chosen):
            options = [row for row in pool if row["stratum"] == "causal"]
            chosen.append(min(options, key=lambda row: rank(
                seed, "question", row["question_id"])))
        if not any(row["stratum"] == "temporal" for row in chosen):
            options = [row for row in pool if row["stratum"] == "temporal"]
            chosen.append(min(options, key=lambda row: rank(
                seed, "question", row["question_id"])))
        remaining = [row for row in pool if row not in chosen]
        remaining.sort(key=lambda row: rank(seed, "question", row["question_id"]))
        if len(chosen) > spec["question_count"]:
            raise ValueError("development question count is below required set")
        chosen.extend(remaining[:spec["question_count"] - len(chosen)])
        if len(chosen) != spec["question_count"]:
            raise ValueError("development video lacks eligible hard questions")
        development.extend(chosen)

    test_pool = []
    for video, pool in candidates.items():
        if f"nextqa-val-{video}" in exposed or video in dev_videos:
            continue
        causal = [row for row in pool if row["stratum"] == "causal"]
        temporal = [row for row in pool if row["stratum"] == "temporal"]
        if (len(pool) >= questions_per_video
                and len(causal) >= 1 and len(temporal) >= 2):
            test_pool.append(video)
    test_pool.sort(key=lambda video: rank(seed, "video", video))
    test_videos = test_pool[:config["test_video_count"]]
    if len(test_videos) != config["test_video_count"]:
        raise ValueError("too few video-disjoint test candidates")
    held_out = []
    for video in test_videos:
        pool = candidates[video]
        chosen = []
        for stratum, count in (("causal", 1), ("temporal", 2)):
            options = [row for row in pool if row["stratum"] == stratum]
            options.sort(key=lambda row: rank(
                seed, "question", row["question_id"]))
            chosen.extend(options[:count])
        remaining = [row for row in pool if row not in chosen]
        remaining.sort(key=lambda row: rank(seed, "question",
                                                row["question_id"]))
        chosen.extend(remaining[:questions_per_video - len(chosen)])
        if len(chosen) != questions_per_video:
            raise ValueError("test video lacks eligible hard questions")
        held_out.extend(chosen)
    return {
        "schema_version": "pathfinder.nextqa-atphard-multiq-cohort/v1",
        "status": "PUBLIC_CANDIDATE_COHORT_SELECTED",
        "selection_basis": "ATP-Hard membership plus grounding availability",
        "quality_outcomes_used_for_test_selection": False,
        "test_eligible_video_count": len(test_pool),
        "development": development,
        "test": held_out,
        "development_object_ids": [f"nextqa-val-{v}" for v in
                                   sorted(dev_videos)],
        "test_object_ids": [f"nextqa-val-{v}" for v in test_videos],
        "label_values_included": False,
        "credentials_recorded": False,
    }


def _media_digest(path: Path) -> tuple[int, int, str]:
    crc = 0
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            size += len(chunk)
            crc = zlib.crc32(chunk, crc)
            digest.update(chunk)
    return size, crc & 0xFFFFFFFF, digest.hexdigest()


def build(args: argparse.Namespace) -> dict:
    config = json.loads(args.config.read_bytes())
    if (config.get("schema_version")
            != "pathfinder.nextqa-atphard-multiq-selection-config/v1"
            or not isinstance(config.get("seed"), str)
            or not config["seed"]
            or type(config.get("test_video_count")) is not int
            or config["test_video_count"] < 1
            or type(config.get("questions_per_video", 3)) is not int
            or config.get("questions_per_video", 3) < 3
            or type(config.get("min_video_bytes")) is not int
            or type(config.get("max_video_bytes")) is not int
            or not 0 < config["min_video_bytes"]
            < config["max_video_bytes"]):
        raise ValueError("selection configuration is invalid")
    if any(type(spec.get("question_count")) is not int
           or spec["question_count"] != config.get("questions_per_video", 3)
           for spec in config["development"]):
        raise ValueError("development question count differs from cohort")
    sources = {
        "official_val": args.official_val,
        "atp_hard": args.atp_hard,
        "grounding": args.grounding,
        "inventory": args.inventory,
        "exposure": args.exposure,
        "prior_selection": args.prior_selection,
    }
    digests = checked_sources(sources, config["source_sha256"])
    inventory = json.loads(args.inventory.read_bytes())
    exposure = json.loads(args.exposure.read_bytes())
    prior = json.loads(args.prior_selection.read_bytes())
    excluded = set(exposure["object_ids"]) | set(prior["selected_object_ids"])
    cohort = select(rows(args.official_val), rows(args.atp_hard),
                    json.loads(args.grounding.read_bytes()), inventory,
                    excluded, config)
    video_ids = [oid.removeprefix("nextqa-val-") for oid in
                 cohort["development_object_ids"] + cohort["test_object_ids"]]
    media = {}
    for video in video_ids:
        source = args.media_source_dir / f"{video}.mp4"
        expected = inventory["objects"][f"nextqa-val-{video}"]
        if not source.is_file():
            raise ValueError(f"selected source video is missing: {video}")
        size, crc, digest = _media_digest(source)
        if size != expected["bytes"] or crc != expected["crc32"]:
            raise ValueError(f"source video differs from archive: {video}")
        media[video] = {"bytes": size, "crc32": crc, "sha256": digest,
                        "archive_entry": expected["entry"]}
    if args.output_dir.exists():
        raise ValueError("output directory already exists")
    args.output_dir.mkdir(parents=True)
    (args.output_dir / "media").mkdir()
    for video in video_ids:
        shutil.copyfile(args.media_source_dir / f"{video}.mp4",
                        args.output_dir / "media" / f"{video}.mp4")
        copied = args.output_dir / "media" / f"{video}.mp4"
        if _media_digest(copied) != (
                media[video]["bytes"], media[video]["crc32"],
                media[video]["sha256"]):
            raise ValueError(f"copied video differs from source: {video}")
    cohort["video_media"] = media
    cohort["source_sha256"] = digests
    cohort["selection_config_sha256"] = sha256(args.config.read_bytes())
    cohort["selector_source_sha256"] = sha256(Path(__file__).read_bytes())
    (args.output_dir / "selection.json").write_bytes(canonical(cohort) + b"\n")
    public_tasks = [
        {key: row[key] for key in (
            "question_id", "object_id", "stratum", "question",
            "answer_options")}
        for row in cohort["development"] + cohort["test"]
    ]
    (args.output_dir / "public-tasks.jsonl").write_bytes(
        b"".join(canonical(row) + b"\n" for row in public_tasks))
    checksums = []
    for path in sorted(args.output_dir.rglob("*")):
        if path.is_file():
            name = path.relative_to(args.output_dir).as_posix()
            checksums.append(f"{sha256(path.read_bytes())}  {name}\n")
    (args.output_dir / "SHA256SUMS").write_bytes(
        "".join(checksums).encode("ascii"))
    return {"status": "PUBLIC_ATPHARD_COHORT_PACKAGED",
            "development_questions": len(cohort["development"]),
            "test_questions": len(cohort["test"]),
            "videos": len(video_ids),
            "test_eligible_video_count": cohort["test_eligible_video_count"],
            "output_dir": str(args.output_dir)}


def main() -> None:
    parser = argparse.ArgumentParser()
    for name in ("config", "official-val", "atp-hard", "grounding",
                 "inventory", "exposure", "prior-selection",
                 "media-source-dir", "output-dir"):
        parser.add_argument("--" + name, type=Path, required=True)
    print(json.dumps(build(parser.parse_args()), sort_keys=True))


if __name__ == "__main__":
    main()
