"""List-price accounting layered over immutable RSI-Exam replay evidence.

This module prices measured provider tokens, not invoices, CPU, storage, or
end-to-end latency.  Missing N6 usage never becomes a zero-cost inference.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import Counter
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence
from xml.etree import ElementTree
from zipfile import ZipFile

from .materialization_cost_evidence import verify_materialization_cost_evidence


PRICE_SNAPSHOT = {
    "schema_version": "pathfinder.rsi-exam-provider-list-prices/v1",
    "as_of_date": "2026-09-23",
    "region": "Singapore",
    "currency": "USD",
    "unit": "per-1m-tokens",
    "qwen3.8-27b": {
        "input": "0.50",
        "implicit_cached_input": "0.10",
        "output": "3.00",
        "source": "https://www.alibabacloud.com/help/en/model-studio/qwen3-8-27b",
    },
    "text-embedding-v4": {
        "input": "0.07",
        "source": (
            "https://www.alibabacloud.com/help/en/model-studio/"
            "text-embedding-synchronous-api"
        ),
    },
}

_MILLION = Decimal(1_000_000)


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def _units(value: Any, name: str) -> int:
    _require(type(value) is int and value >= 0, f"{name} must be nonnegative")
    return value


def _usd(value: Decimal) -> str:
    return format(value.quantize(Decimal("0.000000001")), "f")


def _package_self_digest(manifest: Mapping[str, Any]) -> str:
    contents = dict(manifest)
    claimed = contents.pop("package_sha256", None)
    canonical = json.dumps(
        contents, sort_keys=True, separators=(",", ":"),
        ensure_ascii=False, allow_nan=False,
    ).encode("utf-8")
    actual = hashlib.sha256(canonical).hexdigest()
    _require(claimed == actual, "source package self digest differs")
    return actual


def _qwen_cost(input_units: int, cached_units: int,
               output_units: int) -> Decimal:
    _units(input_units, "input units")
    _units(cached_units, "cached units")
    _units(output_units, "output units")
    _require(cached_units <= input_units, "cached units exceed input units")
    rates = PRICE_SNAPSHOT["qwen3.8-27b"]
    return (
        Decimal(input_units - cached_units) * Decimal(rates["input"])
        + Decimal(cached_units) * Decimal(rates["implicit_cached_input"])
        + Decimal(output_units) * Decimal(rates["output"])
    ) / _MILLION


def _embedding_cost(input_units: int) -> Decimal:
    _units(input_units, "embedding input units")
    return (
        Decimal(input_units)
        * Decimal(PRICE_SNAPSHOT["text-embedding-v4"]["input"])
        / _MILLION
    )


def allocate_embedding_units(
    objects: Sequence[Mapping[str, Any]],
    batches: Sequence[Mapping[str, Any]],
) -> dict[str, int]:
    """Allocate measured batch tokens equally per input, preserving totals.

    The source builder submits all caption search texts in object/ordinal
    order, followed by one public-question anchor per object.  Token totals
    are measured per batch, not per input: this is an accounting allocation.
    """
    ordered = sorted(objects, key=lambda row: row["object_id"])
    ids = [row["object_id"] for row in ordered]
    _require(len(ids) == len(set(ids)), "duplicate object identity")
    owners = [
        row["object_id"]
        for row in ordered
        for _ in range(_units(row["caption_window_count"], "window count"))
    ] + ids
    allocated = {object_id: 0 for object_id in ids}
    offset = 0
    for ordinal, batch in enumerate(batches):
        _require(batch["ordinal"] == ordinal, "embedding batch order changed")
        count = _units(batch["input_count"], "embedding input count")
        units = _units(batch["input_units"], "embedding input units")
        _require(count > 0, "empty embedding batch")
        batch_owners = owners[offset:offset + count]
        _require(len(batch_owners) == count, "embedding batch exceeds inputs")
        shares = Counter(batch_owners)
        portions = {
            object_id: units * n // count
            for object_id, n in shares.items()
        }
        remainder = units - sum(portions.values())
        largest_remainders = sorted(
            shares,
            key=lambda object_id: (-(units * shares[object_id] % count),
                                   object_id),
        )
        for object_id in largest_remainders[:remainder]:
            portions[object_id] += 1
        for object_id, amount in portions.items():
            allocated[object_id] += amount
        offset += count
    _require(offset == len(owners), "embedding inputs do not match objects")
    _require(sum(allocated.values()) == sum(row["input_units"] for row in batches),
             "embedding allocation lost units")
    return allocated


def build_object_price_table(
    objects: Sequence[Mapping[str, Any]],
    batches: Sequence[Mapping[str, Any]],
    *,
    cached_caption_units: Mapping[str, int] | None = None,
) -> dict[str, Any]:
    """Price one frozen cohort without claiming unmeasured N6 or VM costs.

    ``cached_caption_units`` is optional and keyed by object ID.  If absent,
    the standard no-cache list price is used, not an inferred cache hit.
    """
    cache = dict(cached_caption_units or {})
    ids = {row["object_id"] for row in objects}
    _require(set(cache) <= ids, "cache map has an unknown object")
    embedding = allocate_embedding_units(objects, batches)
    rows = []
    for item in sorted(objects, key=lambda row: row["object_id"]):
        object_id = item["object_id"]
        input_units = _units(item["caption_input_units"], "caption input")
        output_units = _units(item["caption_output_units"], "caption output")
        cached_units = _units(cache.get(object_id, 0), "cached caption input")
        caption_usd = _qwen_cost(input_units, cached_units, output_units)
        embedding_usd = _embedding_cost(embedding[object_id])
        rows.append({
            "object_id": object_id,
            "source_video_sha256": item["source_video_sha256"],
            "caption_request_count": item["caption_request_count"],
            "caption_input_units_measured": input_units,
            "caption_cached_input_units": cached_units,
            "caption_output_units_measured": output_units,
            "embedding_input_units_allocated": embedding[object_id],
            "embedding_allocation_basis": "equal-per-input-within-measured-batch",
            "caption_list_price_usd": _usd(caption_usd),
            "embedding_list_price_usd": _usd(embedding_usd),
            "known_build_provider_list_price_usd": _usd(
                caption_usd + embedding_usd
            ),
            "cold_caption_embedding_provider_list_price_usd": _usd(
                caption_usd + embedding_usd
            ),
            "warm_reuse_build_provider_list_price_usd": "0.000000000",
            "n6_inference_list_price_usd": None,
            "complete_path_cost_usd": None,
        })
    return {
        "schema_version": "pathfinder.rsi-exam-provider-cost-table/v1",
        "price_snapshot": PRICE_SNAPSHOT,
        "cost_basis": "official-list-price-before-discounts-and-credits",
        "caption_cache_basis": (
            "caller-supplied-object-cache-unverified"
            if cached_caption_units is not None
            else "standard-no-cache-scenario"
        ),
        "embedding_allocation_is_measured_per_object": False,
        "n6_usage_complete": False,
        "vm_storage_network_cost_complete": False,
        "objects": rows,
        "cohort": {
            "caption_input_units": sum(row["caption_input_units_measured"]
                                       for row in rows),
            "caption_cached_input_units": sum(
                row["caption_cached_input_units"] for row in rows
            ),
            "caption_output_units": sum(row["caption_output_units_measured"]
                                        for row in rows),
            "embedding_input_units": sum(row["embedding_input_units_allocated"]
                                         for row in rows),
            "known_build_provider_list_price_usd": _usd(sum(
                Decimal(row["known_build_provider_list_price_usd"])
                for row in rows
            )),
            "complete_path_cost_usd": None,
        },
    }


def _workbook_usage_rows(path: str | Path) -> list[dict[str, Any]]:
    """Read only model, usage and HTTP status from an XLSX request log."""
    ns = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    with ZipFile(path) as archive:
        shared: list[str] = []
        if "xl/sharedStrings.xml" in archive.namelist():
            root = ElementTree.fromstring(archive.read("xl/sharedStrings.xml"))
            for item in root.findall("x:si", ns):
                shared.append("".join(
                    node.text or "" for node in item.findall(".//x:t", ns)
                ))
        sheets = sorted(name for name in archive.namelist() if
                        name.startswith("xl/worksheets/sheet") and
                        name.endswith(".xml"))
        _require(len(sheets) == 1, "provider workbook must contain one sheet")
        sheet = ElementTree.fromstring(archive.read(sheets[0]))
    rows: list[dict[str, Any]] = []
    for row in sheet.findall(".//x:sheetData/x:row", ns):
        cells: dict[str, str] = {}
        for cell in row.findall("x:c", ns):
            letter = "".join(char for char in cell.attrib.get("r", "")
                             if char.isalpha())
            if letter not in {"C", "D", "G"}:
                continue
            value = cell.find("x:v", ns)
            if value is None:
                inline = cell.find("x:is", ns)
                text = "" if inline is None else "".join(
                    part.text or "" for part in inline.findall(".//x:t", ns)
                )
            elif cell.attrib.get("t") == "s":
                text = shared[int(value.text or "0")]
            else:
                text = value.text or ""
            cells[letter] = text
        if cells.get("C") != "qwen3.8-27b" or cells.get("G") != "200":
            continue
        try:
            usage = json.loads(cells["D"])
        except (KeyError, json.JSONDecodeError) as exc:
            raise ValueError("successful provider row lacks usage") from exc
        _require(isinstance(usage, dict), "provider usage is not an object")
        details = usage.get("prompt_tokens_details", {})
        _require(isinstance(details, dict), "provider cache detail is invalid")
        input_units = usage.get("input_tokens")
        output_units = usage.get("output_tokens")
        cached_units = details.get("cached_tokens", 0)
        _units(input_units, "provider input units")
        _units(output_units, "provider output units")
        _units(cached_units, "provider cached units")
        _require(cached_units <= input_units, "provider cache exceeds input")
        rows.append({
            "input_units": input_units,
            "output_units": output_units,
            "cached_input_units": cached_units,
        })
    return rows


def reconcile_caption_cache(
    caption_requests: Sequence[Mapping[str, Any]],
    provider_usage_rows: Sequence[Mapping[str, Any]],
) -> dict[str, int]:
    """Require a unique token-pair join; never guess from timestamps."""
    candidates: dict[tuple[int, int], list[Mapping[str, Any]]] = {}
    for row in provider_usage_rows:
        key = (row["input_units"], row["output_units"])
        candidates.setdefault(key, []).append(row)
    matched: set[tuple[int, int]] = set()
    cached_by_object: dict[str, int] = {}
    for row in caption_requests:
        key = (row["input_units"], row["output_units"])
        matches = candidates.get(key, [])
        _require(key not in matched and len(matches) == 1,
                 "caption/provider token-pair match is not unique")
        matched.add(key)
        object_id = row["object_id"]
        cached_by_object[object_id] = (
            cached_by_object.get(object_id, 0)
            + matches[0]["cached_input_units"]
        )
    return cached_by_object


def price_replay_result(
    replay: Mapping[str, Any],
    table: Mapping[str, Any],
    *,
    object_id_by_case: Mapping[str, str],
    n6_usage_by_outcome: Mapping[str, Mapping[str, int]] | None = None,
) -> dict[str, Any]:
    """Add cold/reuse provider costs to a v2 run without changing outcomes.

    N6 usage must be bound to a specific measured outcome; no byte-to-token
    conversion or timestamp-only join is accepted.
    """
    prices = {row["object_id"]: row for row in table["objects"]}
    steps = []
    total_known = Decimal(0)
    complete = True
    usage_origins: set[str] = set()
    for step in replay["steps"]:
        if step["status"] != "replayed":
            continue
        case_id = step["case_id"]
        object_id = object_id_by_case.get(case_id)
        _require(object_id in prices, "replay object has no cost row")
        row = prices[object_id]
        built = set(step["newly_built_components"])
        build = sum((
            Decimal(row["caption_list_price_usd"])
            if name == "captions" else
            Decimal(row["embedding_list_price_usd"])
            if name == "index_embedding" else Decimal(0)
            for name in built
        ), Decimal(0))
        embedded_usage = step.get("metrics", {}).get("n6_provider_usage")
        supplied_usage = (n6_usage_by_outcome or {}).get(step["outcome_id"])
        _require(
            embedded_usage is None or supplied_usage is None
            or embedded_usage == supplied_usage,
            "supplied N6 usage differs from replay outcome",
        )
        usage = embedded_usage if embedded_usage is not None else supplied_usage
        inference = None
        if usage is not None:
            usage_origins.add(
                "replay-outcome" if embedded_usage is not None
                else "caller-supplied-unverified"
            )
            inference = _qwen_cost(
                usage["input_units"], usage.get("cached_input_units", 0),
                usage["output_units"],
            )
        else:
            complete = False
        total_known += build + (inference or Decimal(0))
        steps.append({
            "case_id": case_id,
            "action_id": step["action_id"],
            "outcome_id": step["outcome_id"],
            "newly_built_components": sorted(built),
            "known_cold_build_provider_usd": _usd(build),
            "n6_inference_provider_usd": (
                _usd(inference) if inference is not None else None
            ),
            "known_provider_usd": _usd(build + (inference or Decimal(0))),
            "complete_provider_usd": (
                _usd(build + inference) if inference is not None else None
            ),
        })
    complete = complete and len(steps) == replay["query_count"]
    return {
        "schema_version": "pathfinder.rsi-exam-priced-replay/v1",
        "mode": replay["mode"],
        "query_count": replay["query_count"],
        "cost_basis": table["cost_basis"],
        "n6_usage_binding": (
            sorted(usage_origins) if usage_origins else ["unavailable"]
        ),
        "steps": steps,
        "known_provider_list_price_usd": _usd(total_known),
        "provider_cost_complete": complete,
        "provider_cost_source_verified": False,
        "provider_list_price_usd": _usd(total_known) if complete else None,
        "complete_path_cost_usd": None,
        "external_calls_made": False,
    }


def price_verified_smoke_n6_usage(
    smoke_rows: Sequence[Mapping[str, Any]],
    journal_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Join a canonically verified smoke to N6's numeric usage journal.

    The caller must first verify the smoke's frozen source bindings and
    checksums. This join accepts no answer text, prompt, or timestamp match.
    """
    _require(len(smoke_rows) == 10, "N6 pilot requires ten smoke rows")
    by_result: dict[str, Mapping[str, Any]] = {}
    for row in journal_rows:
        digest = row.get("result_sha256")
        _require(
            isinstance(digest, str)
            and re.fullmatch(r"[0-9a-f]{64}", digest) is not None,
            "N6 journal result digest is invalid",
        )
        _require(digest not in by_result, "duplicate N6 journal result")
        by_result[digest] = row
    prices = []
    used: set[str] = set()
    for row in smoke_rows:
        _require(row.get("result", {}).get("status") == "COMPLETE",
                 "N6 pilot contains an incomplete route")
        result = row["result"]
        evidence = result.get("semantic_route_evidence")
        _require(isinstance(evidence, Mapping),
                 "N6 pilot route evidence is missing")
        _require(result.get("route_evidence_sha256") ==
                 evidence.get("evidence_sha256"),
                 "N6 pilot route evidence digest differs")
        semantic = evidence.get("semantic")
        _require(isinstance(semantic, Mapping)
                 and semantic.get("model") == "qwen3.8-27b",
                 "N6 pilot model is not the priced model")
        digest = semantic.get("result_sha256")
        _require(isinstance(digest, str) and digest in by_result,
                 "N6 usage is missing for a route result")
        _require(digest not in used, "N6 result was reused across routes")
        used.add(digest)
        journal = by_result[digest]
        _require(journal.get("request_sha256") ==
                 semantic.get("request_sha256"),
                 "N6 usage binds a different semantic request")
        input_units = _units(journal.get("input_units"), "N6 input")
        cached = _units(journal.get("cached_input_units"), "N6 cache")
        output_units = _units(journal.get("output_units"), "N6 output")
        total_units = _units(journal.get("total_units"), "N6 total")
        _require(cached <= input_units
                 and total_units == input_units + output_units,
                 "N6 usage totals are inconsistent")
        prices.append({
            "case_id": row.get("case_id"),
            "trial_key": row.get("trial_key"),
            "design_id": evidence.get("design_id"),
            "n6_result_sha256": digest,
            "input_units": input_units,
            "cached_input_units": cached,
            "output_units": output_units,
            "total_units": total_units,
            "n6_list_price_usd": _usd(_qwen_cost(
                input_units, cached, output_units,
            )),
        })
    return {
        "schema_version": "pathfinder.rsi-exam-n6-usage-pilot/v1",
        "cost_basis": "official-list-price-before-discounts-and-credits",
        "price_snapshot": PRICE_SNAPSHOT,
        "smoke_count": len(prices),
        "input_units": sum(row["input_units"] for row in prices),
        "cached_input_units": sum(row["cached_input_units"]
                                  for row in prices),
        "output_units": sum(row["output_units"] for row in prices),
        "n6_list_price_usd": _usd(sum(
            (Decimal(row["n6_list_price_usd"]) for row in prices),
            Decimal(0),
        )),
        "rows": prices,
        "historical_360_trial_usage_reconstructed": False,
        "credentials_recorded": False,
    }


def load_object_price_table(
    evidence_dir: str | Path,
    *,
    caption_package_dir: str | Path,
    index_package_dir: str | Path,
    provider_log_xlsx: str | Path | None = None,
) -> dict[str, Any]:
    """Read only the canonically verified, credential-free cost receipt."""
    verify_materialization_cost_evidence(evidence_dir)
    root = Path(evidence_dir)
    manifest = json.loads((root / "cost-evidence.json").read_bytes())
    caption_root = Path(caption_package_dir)
    index_root = Path(index_package_dir)
    caption_manifest = json.loads(
        (caption_root / "temporal-caption-package.json").read_bytes()
    )
    index_manifest = json.loads(
        (index_root / "temporal-index-package.json").read_bytes()
    )
    _require(caption_manifest.get("model_id") == "qwen3.8-27b",
             "caption model differs from price snapshot")
    _require(index_manifest.get("embedding_model_id") == "text-embedding-v4",
             "embedding model differs from price snapshot")
    _require(_package_self_digest(caption_manifest) ==
             manifest["source_caption_package_sha256"],
             "caption package is not cost-bound")
    _require(_package_self_digest(index_manifest) ==
             manifest["source_index_package_sha256"],
             "index package is not cost-bound")
    _require(hashlib.sha256(
        (index_root / "SHA256SUMS").read_bytes()
    ).hexdigest() == manifest["source_index_checksums_sha256"],
             "index checksum manifest is not cost-bound")
    objects = [json.loads(line) for line in
               (root / "objects.jsonl").read_text(encoding="utf-8").splitlines()]
    batches = [json.loads(line) for line in
               (root / "embedding-requests.jsonl").read_text(
                   encoding="utf-8"
               ).splitlines()]
    cache = None
    if provider_log_xlsx is not None:
        captions = [json.loads(line) for line in
                    (root / "caption-requests.jsonl").read_text(
                        encoding="utf-8"
                    ).splitlines()]
        cache = reconcile_caption_cache(
            captions, _workbook_usage_rows(provider_log_xlsx)
        )
    table = build_object_price_table(
        objects, batches, cached_caption_units=cache
    )
    if provider_log_xlsx is not None:
        table["caption_cache_basis"] = "unique-provider-token-pair-match"
        table["provider_log_sha256"] = hashlib.sha256(
            Path(provider_log_xlsx).read_bytes()
        ).hexdigest()
        table["caption_provider_log_match_count"] = len(captions)
    _require(table["cohort"]["caption_input_units"] ==
             manifest["caption_input_units"], "caption input total differs")
    _require(table["cohort"]["caption_output_units"] ==
             manifest["caption_output_units"], "caption output total differs")
    _require(table["cohort"]["embedding_input_units"] ==
             manifest["embedding_input_units"], "embedding total differs")
    table["source_cost_evidence_sha256"] = (
        hashlib.sha256(
            (root / "SHA256SUMS").read_bytes()
        ).hexdigest()
    )
    return table


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cost-evidence-dir", required=True)
    parser.add_argument("--caption-package-dir", required=True)
    parser.add_argument("--index-package-dir", required=True)
    parser.add_argument("--provider-log-xlsx")
    args = parser.parse_args()
    print(json.dumps(load_object_price_table(
        args.cost_evidence_dir,
        caption_package_dir=args.caption_package_dir,
        index_package_dir=args.index_package_dir,
        provider_log_xlsx=args.provider_log_xlsx,
    ),
                     ensure_ascii=False, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
