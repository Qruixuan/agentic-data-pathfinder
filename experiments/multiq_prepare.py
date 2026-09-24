"""Parameter-driven materialization steps for interleaved R/D/DC/I studies.

Public inputs only. Every step records its actual host, wall and CPU time,
including failures, outside immutable packages. Offline preparation uses the
canonical builders; it does not submit workflows or call a provider.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import socket
from time import monotonic, process_time
import urllib.request
import zlib


def read(path: Path):
    return json.loads(path.read_bytes())


def write(path: Path, value: object):
    with path.open("xb") as handle:
        handle.write(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                allow_nan=False).encode("utf-8") + b"\n")


@contextmanager
def measured(root: Path, step: str, host: str):
    start, cpu = monotonic(), process_time()
    record = {"step": step, "physical_host": host,
              "container_hostname": socket.gethostname(),
              "started_utc": datetime.now(timezone.utc).isoformat(),
              "credentials_recorded": False, "status": "STARTED"}

    def append(value):
        with (root / "build-events.jsonl").open("ab") as handle:
            handle.write(json.dumps(value, sort_keys=True).encode() + b"\n")
            handle.flush()
            os.fsync(handle.fileno())

    append(record)
    try:
        yield
        record["status"] = "COMPLETE"
    except Exception as exc:
        record["status"] = "FAILED"
        record["error_class"] = type(exc).__name__
        raise
    finally:
        record.update({"finished_utc": datetime.now(timezone.utc).isoformat(),
                       "wall_seconds": monotonic() - start,
                       "process_cpu_seconds": process_time() - cpu})
        append(record)


def verify_sums(root: Path):
    for line in (root / "SHA256SUMS").read_text().splitlines():
        expected, name = line.split("  ", 1)
        path = (root / name).resolve()
        if not path.is_relative_to(root.resolve()):
            raise ValueError("checksum path escapes package")
        if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise ValueError("input checksum differs: " + name)


def verified_public_plan(plan_dir: Path) -> tuple[dict, list[dict]]:
    """Select a canonical public plan verifier by its frozen file set."""

    from pathfinder.rsi_exam.ten_route_multiq_plan import (
        load_verified_multiq_plan,
    )
    manifest, questions, _ = load_verified_multiq_plan(plan_dir)
    return manifest, questions


def freeze_cohort_media(cohort_dir: Path, output: Path) -> dict:
    """Adapt a verified public ATP-Hard cohort to the reusable prep input."""

    from experiments.complexity_dev_inputs import ARCHIVE_URL

    verify_sums(cohort_dir)
    selection_bytes = (cohort_dir / "selection.json").read_bytes()
    selection = json.loads(selection_bytes)
    if (selection.get("label_values_included") is not False
            or selection.get("credentials_recorded") is not False
            or selection.get("quality_outcomes_used_for_test_selection") is not False):
        raise ValueError("cohort is not public-only and outcome-blind")
    object_ids = set(selection["development_object_ids"])
    object_ids.update(selection["test_object_ids"])
    if len(object_ids) != (len(selection["development_object_ids"])
                           + len(selection["test_object_ids"])):
        raise ValueError("cohort development and test videos overlap")
    media = selection["video_media"]
    if set(media) != {oid.removeprefix("nextqa-val-") for oid in object_ids}:
        raise ValueError("cohort media and question objects differ")
    if output.exists():
        raise ValueError("fresh media output already exists")
    output.mkdir(parents=True)
    rows = []
    for video in sorted(media):
        source = cohort_dir / "media" / f"{video}.mp4"
        expected = media[video]
        payload = source.read_bytes()
        if (len(payload) != expected["bytes"]
                or hashlib.sha256(payload).hexdigest() != expected["sha256"]
                or zlib.crc32(payload) & 0xFFFFFFFF != expected["crc32"]):
            raise ValueError(f"cohort media differs: {video}")
        target = output / source.name
        shutil.copyfile(source, target)
        if hashlib.sha256(target.read_bytes()).hexdigest() != expected["sha256"]:
            raise ValueError(f"copied media differs: {video}")
        rows.append({"object_id": f"nextqa-val-{video}",
                     "filename": target.name,
                     "archive_entry": expected["archive_entry"],
                     "bytes": expected["bytes"],
                     "sha256": expected["sha256"]})
    write(output / "media.json", {
        "selection_sha256": hashlib.sha256(selection_bytes).hexdigest(),
        "archive_url": ARCHIVE_URL, "objects": rows,
        "llm_called": False, "credentials_recorded": False,
    })
    with (output / "SHA256SUMS").open("xb") as handle:
        for artifact in sorted(output.iterdir()):
            if artifact.name != "SHA256SUMS":
                handle.write(
                    f"{hashlib.sha256(artifact.read_bytes()).hexdigest()}  "
                    f"{artifact.name}\n".encode("ascii"))
    verify_sums(output)
    return {"status": "VERIFIED_PUBLIC_COHORT_MEDIA",
            "object_count": len(rows),
            "selection_sha256": hashlib.sha256(selection_bytes).hexdigest()}


def offline(input_dir: Path, output: Path, source_commit: str, host: str):
    # The canonical membership freezer requires a full commit identity.
    # Check before creating any output so an abbreviated SHA cannot leave
    # a partial preparation directory behind.
    if re.fullmatch(r"[0-9a-f]{40}", source_commit) is None:
        raise ValueError("source_commit must be a full Git SHA-1")
    from pathfinder.simulator.raw_cold_data_plane import (
        RawColdObjectBinding, build_raw_cold_data_plane_package,
        verify_raw_cold_data_plane_package,
    )
    from pathfinder.rsi_exam.collection_plan import freeze_collection_plan
    from pathfinder.rsi_exam.temporal_index_collection import (
        prepare_formal_temporal_index, verify_formal_temporal_index_preparation,
    )
    from pathfinder.simulator.hidden_oracle import build_n1_public_task_binding
    from pathfinder.distributed.scoring import MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE
    verify_sums(input_dir / "media")
    media = read(input_dir / "media/media.json")
    plan, questions = verified_public_plan(input_dir / "plan")
    if ({row["object_id"] for row in media["objects"]}
            != {row["object_id"] for row in questions}):
        raise ValueError("media and question objects differ")
    if output.exists():
        raise ValueError("fresh preparation output already exists")
    output.mkdir(parents=True)
    plan_ids = tuple(f"D{i}" for i in range(8))
    with measured(output, "raw-import", host):
        build_raw_cold_data_plane_package([
            RawColdObjectBinding(
                object_id=row["object_id"],
                artifact_path=input_dir / "media" / row["filename"],
                catalog_version=plan["experiment_id"] + "-raw-catalog",
                plan_ids=plan_ids, dataset_id="nextqa",
                dataset_revision="val-2432e972-video-7e8ea8e",
                source_object_id=row["object_id"].removeprefix("nextqa-val-"),
                artifact_sha256=row["sha256"], artifact_size_bytes=row["bytes"],
            ) for row in media["objects"]
        ], output_dir=output / "raw", package_id=plan["experiment_id"] + "-raw")
        verify_raw_cold_data_plane_package(output / "raw")
    # The preparation builder needs one public membership task per video.
    # Preserve the old causal choice where available; the ten-route cohort
    # may omit the causal stratum for one video, so choose its first public
    # question without inspecting an answer or outcome.
    membership = [min(
        (row for row in questions if row["object_id"] == object_id),
        key=lambda row: (row["stratum"] != "causal", row["question_id"]),
    ) for object_id in sorted({row["object_id"] for row in questions})]
    with measured(output, "membership-freeze", host):
        write(output / "public-membership.json", {
            "schema_version": "pathfinder.public-task-set/v1alpha1",
            "task_plane_id": plan["experiment_id"] + "-membership",
            "tasks": [build_n1_public_task_binding(
                workload_id=q["question_id"], object_id=q["object_id"],
                task_class_id=q["stratum"], question=q["question"],
                answer_options=q["answer_options"],
                success_scoring_rule=MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
            ) for q in membership],
            "credentials_recorded": False, "label_values_included": False,
        })
        write(output / "membership-spec.json", {
            "schema_version": "pathfinder.rsi-exam-cohort-spec/v1alpha1",
            "cohort_id": plan["experiment_id"] + "-membership",
            "collection_repetitions": 1, "selection_seed": plan["seed"],
            "stratum_by_workload": {q["question_id"]: q["stratum"]
                                    for q in membership},
            "split_stratum_targets": {"test": {
                stratum: sum(q["stratum"] == stratum for q in membership)
                for stratum in sorted({q["stratum"] for q in membership})
            }},
        })
        freeze_collection_plan(output / "public-membership.json",
                               output / "membership-spec.json",
                               builder_commit=source_commit,
                               output_dir=output / "membership")
    with measured(output, "caption-frame-decoding", host):
        report = prepare_formal_temporal_index(
            output / "membership", output / "raw",
            output_dir=output / "preparation",
            package_id=plan["experiment_id"] + "-preparation",
        )
        verify_formal_temporal_index_preparation(output / "preparation")
    print(json.dumps(report, sort_keys=True))


class BudgetedTransport:
    """Record each attempt before transmission and its usage before validation."""

    def __init__(self, root: Path, phase: str, ceiling: int):
        self.root, self.phase, self.ceiling = root, phase, ceiling
        root.mkdir(parents=True, exist_ok=True)

    def __call__(self, request, timeout):
        journal = self.root / (self.phase + "-attempts.jsonl")
        previous = [json.loads(line) for line in journal.read_bytes().splitlines()] if journal.exists() else []
        count = sum(row["event"] == "started" for row in previous)
        body = request.data
        digest = hashlib.sha256(body).hexdigest()
        if self.phase != "captions":
            for row in previous:
                if row["event"] == "persisted" and row["request_sha256"] == digest:
                    cached = self.root / f"{self.phase}-{row['attempt']:03d}.response.json"
                    raw = cached.read_bytes()
                    if hashlib.sha256(raw).hexdigest() != row["persisted_response_sha256"]:
                        raise ValueError("persisted embedding response changed")
                    return raw
        if count >= self.ceiling:
            raise ValueError("provider attempt ceiling reached")
        attempt = count + 1
        record = {"phase": self.phase, "attempt": attempt,
                  "request_sha256": digest, "request_bytes": len(body),
                  "started_utc": datetime.now(timezone.utc).isoformat(),
                  "credentials_recorded": False}

        def append(event):
            with journal.open("ab") as handle:
                handle.write(json.dumps({**record, **event}, sort_keys=True).encode() + b"\n")
                handle.flush()
                os.fsync(handle.fileno())

        append({"event": "started"})
        start = monotonic()
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                payload = response.read(8 * 1024 * 1024 + 1)
                if len(payload) > 8 * 1024 * 1024:
                    raise ValueError("provider response too large")
                document = json.loads(payload)
            append({"event": "response", "status": 200,
                    "wall_seconds": monotonic() - start,
                    "response_sha256": hashlib.sha256(payload).hexdigest(),
                    "response_bytes": len(payload),
                    "usage": document.get("usage"),
                    "model": document.get("model"),
                    "completed_utc": datetime.now(timezone.utc).isoformat()})
            # Persist only allowed public caption content/vectors and usage.
            # Provider reasoning is removed, not written into public artifacts.
            for choice in document.get("choices", []):
                message = choice.get("message", {})
                for key in list(message):
                    if key not in {"role", "content"}:
                        message.pop(key)
            for key in ("id", "request_id"):
                if isinstance(document.get(key), str):
                    document[key + "_sha256"] = hashlib.sha256(document.pop(key).encode()).hexdigest()
            safe = json.dumps(document, ensure_ascii=False).encode("utf-8")
            path = self.root / f"{self.phase}-{attempt:03d}.response.json"
            with path.open("xb") as handle:
                handle.write(safe)
                handle.flush()
                os.fsync(handle.fileno())
            append({"event": "persisted",
                    "persisted_response_sha256": hashlib.sha256(safe).hexdigest()})
            return safe
        except Exception as exc:
            append({"event": "failed", "error_class": type(exc).__name__,
                    "wall_seconds": monotonic() - start,
                    "possible_provider_charge": True})
            raise RuntimeError("provider attempt failed; inspect the sanitized journal") from None


def paid(input_dir: Path, output: Path, protocol_path: Path, phase: str, host: str):
    from pathfinder.rsi_exam.temporal_index_collection import (
        materialize_formal_temporal_captions,
        verify_formal_temporal_index_preparation,
        verify_formal_temporal_caption_package,
    )
    from pathfinder.rsi_exam.temporal_index_layers import (
        build_video_temporal_index, build_temporal_query_batch,
    )
    protocol = read(protocol_path)
    prep = input_dir / "build/preparation"
    verified = verify_formal_temporal_index_preparation(prep)
    plan, questions = verified_public_plan(input_dir / "plan")
    if (verified["window_count"] != protocol["caption_windows"]
            or plan["plan_sha256"] != protocol["plan_sha256"]):
        raise ValueError("preparation or plan differs from execution protocol")
    base = os.environ["PATHFINDER_SEMANTIC_LLM_BASE_URL"]
    key = os.environ["PATHFINDER_SEMANTIC_LLM_API_KEY"]
    if os.environ["PATHFINDER_SEMANTIC_LLM_MODEL"] != protocol["caption_model"]:
        raise ValueError("configured model differs from frozen protocol")
    output.mkdir(parents=True, exist_ok=True)
    cap = protocol[{
        "captions": "caption_provider_request_ceiling",
        "video-index": "video_embedding_provider_request_ceiling",
        "query": "query_embedding_provider_request_ceiling",
    }[phase]]
    transport = BudgetedTransport(output / "provider-journal", phase, cap)
    with measured(output, phase, host):
        if phase == "captions":
            report = materialize_formal_temporal_captions(
                prep, output_dir=output / "captions", cache_dir=output / "caption-cache",
                package_id=protocol["experiment_id"] + "-captions",
                model_id=protocol["caption_model"], base_url=base, api_key=key,
                max_attempts_per_window=protocol["caption_attempts_per_window"],
                parallelism=1, timeout_seconds=180, transport=transport,
            )
            verify_formal_temporal_caption_package(output / "captions", prep)
        elif phase == "video-index":
            report = build_video_temporal_index(
                prep, output / "captions", output_dir=output / "video-index",
                package_id=protocol["experiment_id"] + "-video-index",
                embedding_model_id=protocol["embedding_model"],
                dimension=protocol["embedding_dimension"], base_url=base,
                api_key=key, batch_size=10, transport=transport,
            )
        else:
            questions = [{k: row[k] for k in
                          ("question_id", "object_id", "question")}
                         for row in questions]
            report = build_temporal_query_batch(
                output / "video-index", prep, output / "captions", questions,
                output_dir=output / "query", package_id=protocol["experiment_id"] + "-query",
                base_url=base, api_key=key, transport=transport,
            )
    print(json.dumps(report, sort_keys=True))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("media", "offline", "captions", "video-index", "query"))
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--physical-host", required=True)
    parser.add_argument("--protocol", type=Path)
    args = parser.parse_args()
    if args.phase == "media":
        print(json.dumps(freeze_cohort_media(args.input_dir,
                                             args.output_dir), sort_keys=True))
    elif args.phase == "offline":
        offline(args.input_dir, args.output_dir, args.source_commit, args.physical_host)
    else:
        paid(args.input_dir, args.output_dir, args.protocol, args.phase, args.physical_host)


if __name__ == "__main__":
    main()
