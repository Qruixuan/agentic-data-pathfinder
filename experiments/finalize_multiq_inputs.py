"""Build public N2/N3/N4 inputs from a verified question-independent index.

No network, credentials, hidden labels or new model calls. Accounting lives
outside exact-file-set packages and distinguishes construction from rebuild
verification. Parameters are paths, not experiment-specific identities.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from experiments.multiq_prepare import measured, read, write


def rows(path):
    return [json.loads(line) for line in path.read_bytes().splitlines()]


def finalize(inputs: Path, paid: Path, output: Path, host: str):
    from pathfinder.simulator.n3_multiq_indexed_data_plane import (
        derive_n3_multiq_question_policies, build_n3_multiq_indexed_package,
    )
    from pathfinder.video_prep import sample_video
    from pathfinder.simulator.raw_cold_data_plane import PACKAGE_MANIFEST_NAME
    from pathfinder.rsi_exam.temporal_index_collection import (
        verify_formal_temporal_index_preparation,
        verify_formal_temporal_caption_package,
    )
    from pathfinder.rsi_exam.formal_foundation import (
        _frame_bundle, _digest_text, N2_SOURCE_SCHEMA_VERSION,
    )
    from pathfinder.simulator.n4_derived_data_plane import (
        N4DerivedArtifactInput, N4ArtifactProvenance,
        build_n4_derived_data_package, verify_n4_derived_data_package,
    )
    from pathfinder.simulator.index_service import (
        build_n2_index_package, verify_n2_index_package,
    )

    plan = read(inputs / "plan/interleaved-plan.json")
    questions = rows(inputs / "plan/public-questions.jsonl")
    prep_root = inputs / "build/preparation"
    prep = verify_formal_temporal_index_preparation(prep_root)
    captions = verify_formal_temporal_caption_package(paid / "captions", prep_root)
    policies = derive_n3_multiq_question_policies(
        plan_dir=inputs / "plan", public_questions=questions,
        public_source_sha256=plan["public_source_sha256"],
        query_dir=paid / "query", video_index_dir=paid / "video-index",
        preparation_dir=prep_root, caption_dir=paid / "captions",
        raw_package_dir=inputs / "build/raw",
    )
    output.mkdir(parents=True, exist_ok=False)
    policy_order = sorted(policies, key=lambda item: item["question_id"])
    calls = 0

    def timed_sampler(*args, **kwargs):
        nonlocal calls
        question = policy_order[calls % len(policy_order)]
        pass_number = calls // len(policy_order)
        calls += 1
        kind = "build" if pass_number == 0 else "verification-rebuild"
        with measured(output, f"projection:{kind}:{question['question_id']}", host):
            return sample_video(*args, **kwargs)

    with measured(output, "n3-package-and-canonical-verification", host):
        n3 = build_n3_multiq_indexed_package(
            inputs / "build/raw", output_dir=output / "n3",
            package_id=plan["experiment_id"] + "-n3",
            question_policies=policies, sampler=timed_sampler,
        )
    raw = read(inputs / "build/raw" / PACKAGE_MANIFEST_NAME)
    # Manifest name is shared with the raw package verifier, not assumed by callers.
    prepared = read(prep_root / "temporal-index-preparation.json")
    raw_by_id = {row["object_id"]: row for row in raw["objects"]}
    frames = rows(prep_root / "caption-frames.jsonl")
    caption_rows = rows(paid / "captions/fine-captions.jsonl")
    n4_inputs = []
    descriptions = {}
    for obj in prepared["objects"]:
        oid = obj["object_id"]
        selected_captions = [r for r in caption_rows if r["object_id"] == oid]
        with measured(output, "derived-assembly:" + oid, host):
            bundle = _frame_bundle(
                object_id=oid, object_row=obj,
                frame_rows=sorted([r for r in frames if r["object_id"] == oid],
                                  key=lambda row: row["frame_index"]),
                preparation_root=prep_root,
                preparation_sha256=prep["preparation_sha256"],
            )
            digest = _digest_text(oid, selected_captions)
            descriptions[oid] = digest.decode("utf-8")
            for representation, payload, suffix in (
                ("sampled_frame_bundle", bundle, "frame-bundle"),
                ("multimodal_digest", digest, "temporal-digest"),
            ):
                derivation_id = "rsi-exam-question-independent-" + suffix + "-v1"
                derivation = {
                    "derivation_id": derivation_id, "object_id": oid,
                    "representation_id": representation,
                    "source_video_sha256": raw_by_id[oid]["artifact_sha256"],
                    "preparation_sha256": prep["preparation_sha256"],
                    "caption_package_sha256": captions["package_sha256"],
                }
                canonical = json.dumps(derivation, sort_keys=True,
                                       separators=(",", ":")).encode()
                n4_inputs.append(N4DerivedArtifactInput(
                    object_id=oid, representation_id=representation,
                    artifact_bytes=payload,
                    plan_ids=tuple(f"D{i}" for i in range(8)),
                    provenance=N4ArtifactProvenance(
                        producer_node_id="N5",
                        publication_source_id=plan["experiment_id"] + "-" + oid,
                        source_representation_id="raw_video",
                        source_content_sha256=raw_by_id[oid]["artifact_sha256"],
                        derivation_id=derivation_id,
                        derivation_sha256=hashlib.sha256(canonical).hexdigest(),
                    ),
                ))
    with measured(output, "n4-package-and-verification", host):
        build_n4_derived_data_package(
            n4_inputs, output_dir=output / "n4",
            package_id=plan["experiment_id"] + "-n4",
            catalog_version=plan["experiment_id"] + "-n4-catalog",
        )
        n4 = verify_n4_derived_data_package(output / "n4")
    with measured(output, "n2-package-and-verification", host):
        write(output / "n2-source.json", {
            "schema_version": N2_SOURCE_SCHEMA_VERSION,
            "index_id": plan["experiment_id"] + "-visible-index",
            "logical_node_id": "N2",
            "documents": [{
                "object_id": oid, "source_object_group": plan["experiment_id"],
                "visible_fields": {
                    "description": "Known-target public video " + oid + ".",
                    "media_type": "video",
                    "modalities": ["text", "video"],
                    "source_collection": "nextqa-val-formal-cohort",
                    "tags": ["nextqa", "video"],
                },
            } for oid in sorted(descriptions)],
            "credentials_recorded": False,
        })
        build_n2_index_package(output / "n2-source.json", output_dir=output / "n2")
        n2 = verify_n2_index_package(output / "n2")
    report = {"status": "VERIFIED_PUBLIC_MULTIQ_INPUTS", "n2": n2, "n3": n3,
              "n4": n4, "projection_sampler_calls": calls,
              "workflow_submitted": False, "llm_called": False,
              "credentials_recorded": False}
    write(output / "finalization.json", report)
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--paid-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--physical-host", required=True)
    args = parser.parse_args()
    finalize(args.input_dir, args.paid_dir, args.output_dir, args.physical_host)
