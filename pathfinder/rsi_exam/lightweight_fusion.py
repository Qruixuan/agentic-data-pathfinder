"""One question-independent video summary plus the frozen lightweight frames.

The summary phase is intentionally separate from the no-provider frame build:
successful provider responses are persisted per object before validation, so a
later failure never discards paid work.  No task text or answer is an input.
"""

from __future__ import annotations

import argparse
import base64
from dataclasses import dataclass
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import tarfile
import tempfile
from time import monotonic_ns
from typing import Any, Callable
from urllib.request import HTTPRedirectHandler, Request, build_opener

from .lightweight_derived import PLAN_IDS, _canonical, _pretty, _sha
from ..simulator.n4_derived_data_plane import (
    N4ArtifactProvenance,
    N4DerivedArtifactInput,
    build_n4_derived_data_package,
    verify_n4_derived_data_package,
)
from ..simulator.full_flow_fine_temporal_windows import (
    extract_single_json_object,
)

PROMPT = (
    "Describe only the visible actions and their order in this video. "
    "Use the timestamps as context. Give a concise, question-independent "
    "visual summary; do not infer intent or unseen events. Return one JSON "
    'object with a nonempty string field named "summary".'
)
PROMPT_SHA256 = hashlib.sha256(PROMPT.encode("utf-8")).hexdigest()
MODEL_FRAME_INDICES = tuple(1 + 3 * index for index in range(8))
SCHEMA = "pathfinder.rsi-exam-lightweight-video-summary/v1"
FUSION_SCHEMA = "pathfinder.rsi-exam-lightweight-fusion-build/v1"
SUMMARY_DERIVATION_ID = "rsi-exam-question-independent-single-summary-v1"


@dataclass(frozen=True)
class ProviderResponse:
    body: bytes
    request_id: str | None = None
    dashscope_request_id: str | None = None


Transport = Callable[[Request, float], bytes | ProviderResponse]


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        return None


def _default_transport(request: Request, timeout: float) -> ProviderResponse:
    # urllib's default redirect handler can forward the bearer token.
    with build_opener(_NoRedirect()).open(request, timeout=timeout) as response:
        return ProviderResponse(
            response.read(), response.headers.get("X-Request-Id"),
            response.headers.get("X-DashScope-RequestId"),
        )


_REQUEST_ID = re.compile(
    r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}"
)


def _provider_id_sha256(value: str | None, api_key: str) -> str | None:
    if (not isinstance(value, str) or value == api_key
            or _REQUEST_ID.fullmatch(value) is None):
        return None
    return _sha(value.encode("ascii"))


def _bundle_frames(bundle: bytes) -> tuple[dict[str, Any], list[bytes]]:
    with tarfile.open(fileobj=io.BytesIO(bundle), mode="r:") as archive:
        manifest_member = archive.extractfile("frame_bundle_manifest.json")
        if manifest_member is None:
            raise ValueError("frame bundle manifest is missing")
        manifest = json.loads(manifest_member.read())
        rows = manifest["frames"]
        if len(rows) != 24 or [row["frame_index"] for row in rows] != list(
            range(24)
        ):
            raise ValueError("frame bundle does not contain 24 ordered frames")
        frames = []
        for row in rows:
            path = row["path"]
            if path != f"frames/{row['frame_index']:03d}.jpg":
                raise ValueError("frame path differs from the frozen policy")
            member = archive.extractfile(path)
            if member is None:
                raise ValueError("frame payload is missing")
            payload = member.read()
            if len(payload) != row["jpeg_size_bytes"] or _sha(payload) != row[
                "jpeg_sha256"
            ]:
                raise ValueError("frame payload differs from its identity")
            frames.append(payload)
    return manifest, frames


def _request_body(bundle: bytes, model_id: str) -> bytes:
    manifest, frames = _bundle_frames(bundle)
    content: list[dict[str, Any]] = [{"type": "text", "text": PROMPT}]
    for index in MODEL_FRAME_INDICES:
        row = manifest["frames"][index]
        content.extend((
            {"type": "text", "text": (
                f"timestamp_seconds={row['timestamp_seconds']:.6f}"
            )},
            {"type": "image_url", "image_url": {
                "url": "data:image/jpeg;base64,"
                + base64.b64encode(frames[index]).decode("ascii"),
                "detail": "low",
            }},
        ))
    return _canonical({
        "model": model_id,
        "messages": [{"role": "user", "content": content}],
        "temperature": 0,
        "max_tokens": 1024,
        "response_format": {"type": "json_object"},
    })


def _extract_summary(response: bytes) -> tuple[str, dict[str, int]]:
    document = json.loads(response)
    content = document["choices"][0]["message"]["content"]
    if not isinstance(content, str):
        raise ValueError("summary response content is not text")
    parsed = extract_single_json_object(content)
    if set(parsed) != {"summary"} or not isinstance(parsed["summary"], str):
        raise ValueError("summary response schema differs")
    summary = " ".join(parsed["summary"].split())
    if not summary or len(summary) > 2500:
        raise ValueError("summary is empty or unbounded")
    usage = document["usage"]
    numeric = {}
    for key in ("prompt_tokens", "completion_tokens", "total_tokens"):
        value = usage[key]
        if type(value) is not int or value < 0:
            raise ValueError("provider usage is invalid")
        numeric[key] = value
    return summary, numeric


def _bundle_rows(n4_dir: Path) -> list[dict[str, Any]]:
    verify_n4_derived_data_package(n4_dir)
    document = json.loads((n4_dir / "n4-derived-data-package.json").read_bytes())
    rows = document["objects"]
    if len(rows) != 8 or {row["representation_id"] for row in rows} != {
        "sampled_frame_bundle"
    }:
        raise ValueError("expected an eight-object frame-only source package")
    return sorted(rows, key=lambda row: row["object_id"])


def materialize_video_summaries(
    n4_dir: str | Path, *, cache_dir: str | Path, output_dir: str | Path,
    model_id: str, base_url: str, api_key: str, timeout_seconds: float = 180,
    transport: Transport = _default_transport,
) -> dict[str, Any]:
    """At most one new provider call per uncached object; never silently retry."""
    source = Path(n4_dir).resolve()
    cache = Path(cache_dir).resolve()
    target = Path(output_dir).resolve()
    if target.exists() or not model_id or not base_url or not api_key:
        raise ValueError("output exists or provider configuration is absent")
    cache.mkdir(parents=True, exist_ok=True)
    records = []
    new_requests = 0
    for row in _bundle_rows(source):
        object_id = row["object_id"]
        bundle_path = source / row["artifact_package_path"]
        bundle = bundle_path.read_bytes()
        if _sha(bundle) != row["artifact_sha256"]:
            raise ValueError("N4 frame artifact identity differs")
        body = _request_body(bundle, model_id)
        input_sha = _sha(body)
        cached = cache / f"{object_id}.json"
        if cached.exists():
            record = json.loads(cached.read_bytes())
            if (record.get("request_input_sha256") != input_sha
                    or record.get("source_bundle_sha256") != _sha(bundle)):
                raise ValueError("cached provider response has different bindings")
        else:
            request = Request(
                base_url.rstrip("/") + "/chat/completions",
                data=body, method="POST", headers={
                    "Authorization": "Bearer " + api_key,
                    "Content-Type": "application/json",
                },
            )
            start = monotonic_ns()
            delivered = transport(request, timeout_seconds)
            new_requests += 1
            response = (delivered if isinstance(delivered, bytes)
                        else delivered.body)
            request_id = (None if isinstance(delivered, bytes)
                          else delivered.request_id)
            dashscope_request_id = (None if isinstance(delivered, bytes)
                                    else delivered.dashscope_request_id)
            if api_key.encode("utf-8") in response:
                raise ValueError("provider response echoed a credential")
            record = {
                "schema_version": SCHEMA,
                "object_id": object_id,
                "model_id": model_id,
                "prompt_sha256": PROMPT_SHA256,
                "source_bundle_sha256": _sha(bundle),
                "request_input_sha256": input_sha,
                "response_sha256": _sha(response),
                "provider_request_id_sha256": _provider_id_sha256(
                    request_id, api_key),
                "provider_dashscope_request_id_sha256": _provider_id_sha256(
                    dashscope_request_id, api_key),
                "response_utf8": response.decode("utf-8"),
                "service_wall_ns": monotonic_ns() - start,
                "credentials_recorded": False,
            }
            temporary = cached.with_suffix(".partial")
            temporary.write_bytes(_pretty(record))
            temporary.replace(cached)
        response = record["response_utf8"].encode("utf-8")
        if _sha(response) != record["response_sha256"]:
            raise ValueError("cached provider response identity differs")
        summary, usage = _extract_summary(response)
        records.append({
            "object_id": object_id,
            "source_bundle_sha256": _sha(bundle),
            "request_input_sha256": input_sha,
            "response_sha256": record["response_sha256"],
            "provider_request_id_sha256": record.get(
                "provider_request_id_sha256"),
            "provider_dashscope_request_id_sha256": record.get(
                "provider_dashscope_request_id_sha256"),
            "summary": summary,
            "summary_sha256": _sha(summary.encode("utf-8")),
            "usage": usage,
            "service_wall_ns": record["service_wall_ns"],
            "model_id": model_id,
            "prompt_sha256": PROMPT_SHA256,
            "credentials_recorded": False,
        })
    target.mkdir(parents=True, exist_ok=False)
    payload = b"".join(_canonical(row) + b"\n" for row in records)
    (target / "video-summaries.jsonl").write_bytes(payload)
    (target / "SHA256SUMS").write_bytes(
        f"{_sha(payload)}  video-summaries.jsonl\n".encode("ascii")
    )
    return {
        "status": "VERIFIED_VIDEO_SUMMARIES",
        "object_count": len(records),
        "provider_request_count_total": len(records),
        "provider_requests_this_run": new_requests,
        "prompt_tokens": sum(row["usage"]["prompt_tokens"] for row in records),
        "completion_tokens": sum(row["usage"]["completion_tokens"]
                                 for row in records),
        "credentials_recorded": False,
    }


def verify_video_summaries(
    summary_dir: str | Path, frame_n4_dir: str | Path,
) -> list[dict[str, Any]]:
    root = Path(summary_dir).resolve()
    payload = (root / "video-summaries.jsonl").read_bytes()
    expected = f"{_sha(payload)}  video-summaries.jsonl\n".encode("ascii")
    if (root / "SHA256SUMS").read_bytes() != expected:
        raise ValueError("video summary checksum differs")
    rows = [json.loads(line) for line in payload.splitlines()]
    bundles = {row["object_id"]: row for row in _bundle_rows(
        Path(frame_n4_dir).resolve()
    )}
    if len(rows) != len(bundles) or len({r["object_id"] for r in rows}) != 8:
        raise ValueError("video summary object coverage differs")
    for row in rows:
        bundle = bundles[row["object_id"]]
        if (row["source_bundle_sha256"] != bundle["artifact_sha256"]
                or row["prompt_sha256"] != PROMPT_SHA256
                or row["summary_sha256"] != _sha(row["summary"].encode("utf-8"))
                or row["credentials_recorded"] is not False):
            raise ValueError("video summary source binding differs")
        if any(type(row["usage"][name]) is not int
               or row["usage"][name] < 0
               for name in ("prompt_tokens", "completion_tokens",
                            "total_tokens")):
            raise ValueError("video summary usage differs")
    return sorted(rows, key=lambda row: row["object_id"])


def freeze_lightweight_fusion(
    frame_n4_dir: str | Path, summary_dir: str | Path, *,
    output_dir: str | Path, package_id: str,
) -> dict[str, Any]:
    """Fuse the once-per-video summary with unchanged frozen frames."""
    frame_root = Path(frame_n4_dir).resolve()
    source = {row["object_id"]: row for row in _bundle_rows(frame_root)}
    summaries = verify_video_summaries(summary_dir, frame_root)
    target = Path(output_dir).resolve()
    if target.exists() or not package_id:
        raise ValueError("fusion output exists or package ID is empty")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.",
                                        dir=target.parent))
    try:
        inputs = []
        for row in summaries:
            object_id = row["object_id"]
            frame = source[object_id]
            bundle = (frame_root / frame["artifact_package_path"]).read_bytes()
            if _sha(bundle) != frame["artifact_sha256"]:
                raise ValueError("source frame bundle changed")
            digest = (
                f"Object: {object_id}\n"
                "Question-independent visual summary:\n"
                f"{row['summary']}\n"
            ).encode("utf-8")
            source_sha = frame["provenance"]["source_content_sha256"]
            for representation, payload in (
                ("sampled_frame_bundle", bundle),
                ("multimodal_digest", digest),
            ):
                if representation == "sampled_frame_bundle":
                    old = frame["provenance"]
                    source_provenance = N4ArtifactProvenance(
                        producer_node_id=old["producer_node_id"],
                        publication_source_id=old["publication_source_id"],
                        source_representation_id=old["source_representation_id"],
                        source_content_sha256=old["source_content_sha256"],
                        derivation_id=old["derivation_id"],
                        derivation_sha256=old["derivation_sha256"],
                    )
                else:
                    derivation = {
                        "derivation_id": SUMMARY_DERIVATION_ID,
                        "object_id": object_id,
                        "representation_id": representation,
                        "source_video_sha256": source_sha,
                        "frame_bundle_sha256": _sha(bundle),
                        "summary_sha256": row["summary_sha256"],
                        "summary_request_sha256": row["request_input_sha256"],
                    }
                    source_provenance = N4ArtifactProvenance(
                        producer_node_id="N5",
                        publication_source_id=f"{package_id}-{object_id}",
                        source_representation_id="raw_video",
                        source_content_sha256=source_sha,
                        derivation_id=SUMMARY_DERIVATION_ID,
                        derivation_sha256=_sha(_canonical(derivation)),
                    )
                inputs.append(N4DerivedArtifactInput(
                    object_id=object_id,
                    representation_id=representation,
                    artifact_bytes=payload,
                    plan_ids=PLAN_IDS,
                    provenance=source_provenance,
                ))
        build_n4_derived_data_package(
            inputs, output_dir=staging / "n4", package_id=package_id,
            catalog_version=f"{package_id}-catalog",
        )
        verified = verify_n4_derived_data_package(staging / "n4")
        receipt = {
            "schema_version": FUSION_SCHEMA,
            "status": "FROZEN_LIGHTWEIGHT_FUSION",
            "object_count": len(summaries),
            "frame_source_manifest_sha256": _sha(
                (frame_root / "n4-derived-data-package.json").read_bytes()
            ),
            "summary_sha256": _sha(
                (Path(summary_dir) / "video-summaries.jsonl").read_bytes()
            ),
            "summary_provider_request_count": len(summaries),
            "summary_prompt_tokens": sum(
                row["usage"]["prompt_tokens"] for row in summaries
            ),
            "summary_completion_tokens": sum(
                row["usage"]["completion_tokens"] for row in summaries
            ),
            "n4_package_sha256": verified["package_sha256"],
            "credentials_recorded": False,
        }
        receipt_bytes = _pretty(receipt)
        (staging / "build-receipt.json").write_bytes(receipt_bytes)
        (staging / "SHA256SUMS").write_bytes(
            f"{_sha(receipt_bytes)}  build-receipt.json\n".encode("ascii")
        )
        os.replace(staging, target)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return verify_lightweight_fusion(target, frame_root, summary_dir)


def verify_lightweight_fusion(
    output_dir: str | Path, frame_n4_dir: str | Path,
    summary_dir: str | Path,
) -> dict[str, Any]:
    root = Path(output_dir).resolve()
    receipt_bytes = (root / "build-receipt.json").read_bytes()
    if (root / "SHA256SUMS").read_bytes() != (
        f"{_sha(receipt_bytes)}  build-receipt.json\n".encode("ascii")
    ):
        raise ValueError("lightweight fusion receipt checksum differs")
    receipt = json.loads(receipt_bytes)
    summaries = {row["object_id"]: row for row in verify_video_summaries(
        summary_dir, frame_n4_dir,
    )}
    frames = {row["object_id"]: row for row in _bundle_rows(
        Path(frame_n4_dir).resolve(),
    )}
    n4 = verify_n4_derived_data_package(root / "n4")
    document = json.loads((root / "n4/n4-derived-data-package.json").read_bytes())
    rows = {(row["object_id"], row["representation_id"]): row
            for row in document["objects"]}
    if (receipt["schema_version"] != FUSION_SCHEMA
            or receipt["object_count"] != 8
            or receipt["n4_package_sha256"] != n4["package_sha256"]
            or receipt["frame_source_manifest_sha256"] != _sha(
                (Path(frame_n4_dir) / "n4-derived-data-package.json")
                .read_bytes())
            or receipt["summary_sha256"] != _sha(
                (Path(summary_dir) / "video-summaries.jsonl").read_bytes())
            or receipt["summary_provider_request_count"] != 8
            or receipt["summary_prompt_tokens"] != sum(
                row["usage"]["prompt_tokens"] for row in summaries.values()
            )
            or receipt["summary_completion_tokens"] != sum(
                row["usage"]["completion_tokens"]
                for row in summaries.values()
            )
            or receipt["credentials_recorded"] is not False
            or len(rows) != 16):
        raise ValueError("lightweight fusion package cardinality differs")
    for object_id, summary in summaries.items():
        frame = frames[object_id]
        for representation, expected in (
            ("sampled_frame_bundle", (Path(frame_n4_dir)
                                     / frame["artifact_package_path"]).read_bytes()),
            ("multimodal_digest", (
                f"Object: {object_id}\n"
                "Question-independent visual summary:\n"
                f"{summary['summary']}\n"
            ).encode("utf-8")),
        ):
            row = rows[(object_id, representation)]
            payload = (root / "n4" / row["artifact_package_path"]).read_bytes()
            if payload != expected or row["artifact_sha256"] != _sha(expected):
                raise ValueError("lightweight fusion artifact differs")
            provenance = row["provenance"]
            if representation == "sampled_frame_bundle":
                if provenance != frame["provenance"]:
                    raise ValueError("frozen frame provenance changed")
            else:
                derivation = {
                    "derivation_id": SUMMARY_DERIVATION_ID,
                    "object_id": object_id,
                    "representation_id": representation,
                    "source_video_sha256": frame["provenance"][
                        "source_content_sha256"],
                    "frame_bundle_sha256": frame["artifact_sha256"],
                    "summary_sha256": summary["summary_sha256"],
                    "summary_request_sha256": summary["request_input_sha256"],
                }
                if (provenance["derivation_id"] != SUMMARY_DERIVATION_ID
                        or provenance["derivation_sha256"] != _sha(
                            _canonical(derivation)
                        )):
                    raise ValueError("summary derivation binding differs")
    return {
        "status": "VERIFIED_LIGHTWEIGHT_FUSION",
        "object_count": 8,
        "n4_package_sha256": n4["package_sha256"],
        "summary_provider_request_count": 8,
        "credentials_recorded": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_subparsers(dest="action", required=True)
    summary = action.add_parser("summarize")
    summary.add_argument("--frame-n4-dir", type=Path, required=True)
    summary.add_argument("--cache-dir", type=Path, required=True)
    summary.add_argument("--output-dir", type=Path, required=True)
    fusion = action.add_parser("fuse")
    fusion.add_argument("--frame-n4-dir", type=Path, required=True)
    fusion.add_argument("--summary-dir", type=Path, required=True)
    fusion.add_argument("--output-dir", type=Path, required=True)
    fusion.add_argument("--package-id", required=True)
    check = action.add_parser("verify")
    check.add_argument("--frame-n4-dir", type=Path, required=True)
    check.add_argument("--summary-dir", type=Path, required=True)
    check.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.action == "summarize":
        report = materialize_video_summaries(
            args.frame_n4_dir, cache_dir=args.cache_dir,
            output_dir=args.output_dir,
            model_id=os.environ["PATHFINDER_SEMANTIC_LLM_MODEL"],
            base_url=os.environ["PATHFINDER_SEMANTIC_LLM_BASE_URL"],
            api_key=os.environ["PATHFINDER_SEMANTIC_LLM_API_KEY"],
        )
    elif args.action == "fuse":
        report = freeze_lightweight_fusion(
            args.frame_n4_dir, args.summary_dir,
            output_dir=args.output_dir, package_id=args.package_id,
        )
    else:
        report = verify_lightweight_fusion(
            args.output_dir, args.frame_n4_dir, args.summary_dir,
        )
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        status = getattr(exc, "code", None)
        print(json.dumps({
            "status": "LIGHTWEIGHT_FUSION_STOPPED",
            "error_class": type(exc).__name__,
            "http_status": status if type(status) is int else None,
            "credentials_recorded": False,
        }, sort_keys=True))
        raise SystemExit(2) from None
