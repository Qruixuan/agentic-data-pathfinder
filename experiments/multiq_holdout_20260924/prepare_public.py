"""Freeze conservative exposure exclusions, then select public tasks on N1."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
from pathlib import Path
import re
import subprocess

from experiments.fresh_multiq_cohort import canonical


ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "artifacts/multiq-fresh-holdout-20260924-v3"
PUBLIC_NAMES = {
    "public-tasks.json", "semantic-public-tasks.json", "public-questions.jsonl",
    "selected-cases.jsonl", "raw-cold-data-plane.json",
}
BLOCKED_PARTS = {"n1-private", "private", "oracle-package", ".git"}


def write_new(path: Path, value: object) -> None:
    payload = json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False,
                         allow_nan=False).encode("utf-8") + b"\n"
    with path.open("xb") as handle:
        handle.write(payload)


def freeze_protocol() -> None:
    if OUT.exists():
        raise ValueError("immutable protocol directory already exists")
    # V3 adds only the existing runtime byte-limit eligibility. Do not treat
    # v1/v2 unexecuted selections as new historical outcome exposure.
    previous = ROOT / "artifacts/multiq-fresh-holdout-20260924-v2"
    if previous.exists():
        for line in (previous / "SHA256SUMS").read_text().splitlines():
            digest, name = line.split("  ", 1)
            if hashlib.sha256((previous / name).read_bytes()).hexdigest() != digest:
                raise ValueError("prior protocol source checksum differs")
        inventory = ROOT / "artifacts/h48-archive-inventory-v1/inventory.json"
        protocol = json.loads((previous / "selection-protocol.json").read_bytes())
        protocol.update({
            "schema_version": "pathfinder.fresh-multiq-selection/v2",
            "media_inventory_sha256": hashlib.sha256(inventory.read_bytes()).hexdigest(),
            "max_direct_video_bytes": 7_000_000,
        })
        OUT.mkdir()
        write_new(OUT / "selection-protocol.json", protocol)
        write_new(OUT / "exposure-inventory.json", json.loads(
            (previous / "exposure-inventory.json").read_bytes()))
        with (OUT / "SHA256SUMS").open("xb") as handle:
            for path in sorted(OUT.glob("*.json")):
                handle.write(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n".encode())
        print(json.dumps({"status": "BYTE_ELIGIBLE_PROTOCOL_FROZEN",
                          "max_direct_video_bytes": 7_000_000}))
        return
    sources, denied, exposed = [], [], set()
    candidates = set()
    for directory in (ROOT / "configs", ROOT / "experiments"):
        candidates.update(path for path in directory.rglob("*")
                          if path.suffix in {".md", ".json", ".jsonl"})
    # Public names only; never open hidden oracle packages or label files.
    for name in PUBLIC_NAMES:
        candidates.update((ROOT / "artifacts").rglob(name))
    for path in sorted(candidates):
        if BLOCKED_PARTS.intersection(path.parts) or not path.is_file():
            continue
        if "multiq_holdout_20260924" in path.parts:
            continue
        try:
            raw = path.read_bytes()
        except PermissionError:
            denied.append(path.relative_to(ROOT).as_posix())
            continue
        # Only object identifiers are returned; no other JSON fields printed.
        ids = set(re.findall(rb"nextqa-val-([0-9]{8,12})(?![0-9])", raw))
        ids.update(re.findall(rb'"video_id"\s*:\s*"([0-9]{8,12})"', raw))
        object_ids = sorted("nextqa-val-" + vid.decode("ascii") for vid in ids)
        if object_ids:
            exposed.update(object_ids)
            sources.append({"path": path.relative_to(ROOT).as_posix(),
                            "sha256": hashlib.sha256(raw).hexdigest(),
                            "object_ids": object_ids})
    if len(exposed) < 20:
        raise ValueError("historical exposure inventory is unexpectedly small")
    if denied:
        raise ValueError("public exposure sources unreadable; retry with read access")
    OUT.mkdir(parents=True, exist_ok=False)
    write_new(OUT / "exposure-inventory.json", {
        "schema_version": "pathfinder.conservative-exposure-inventory/v1",
        "object_ids": sorted(exposed), "sources": sources,
        "unreadable_public_sources": denied,
        "excludes_publicly_mentioned_not_only_executed": True,
        "credentials_recorded": False, "label_values_included": False,
    })
    write_new(OUT / "selection-protocol.json", {
        "schema_version": "pathfinder.fresh-multiq-selection/v1",
        "seed": "pathfinder-video-disjoint-multiq-holdout-20260924-v1",
        "object_count": 4, "strata": ["causal", "temporal", "descriptive"],
        "official_csv_sha256":
            "43198bdef8436b8d64a9b75d846b0987c10cbf94ebf4be325c4a4e54634d66b8",
        "excluded_object_ids": sorted(exposed),
        "selection_rule": "sha256-seeded-public-fields-v1",
    })
    files = sorted(OUT.glob("*.json"))
    with (OUT / "SHA256SUMS").open("xb") as handle:
        handle.write(b"".join(
            f"{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.name}\n".encode()
            for p in files
        ))
    print(json.dumps({"status": "SELECTION_PROTOCOL_FROZEN",
                      "excluded_objects": len(exposed),
                      "unreadable_public_sources": len(denied)}))


def select_remote() -> None:
    for line in (OUT / "SHA256SUMS").read_text().splitlines():
        expected, name = line.split("  ", 1)
        if hashlib.sha256((OUT / name).read_bytes()).hexdigest() != expected:
            raise ValueError("frozen selection protocol checksum differs")
    output = OUT.parent / (OUT.name + "-public-selection")
    if output.exists():
        raise ValueError("immutable public selection already exists")
    protocol = json.loads((OUT / "selection-protocol.json").read_bytes())
    encoded = base64.b64encode(canonical(protocol)).decode("ascii")
    inventory = ROOT / "artifacts/h48-archive-inventory-v1/inventory.json"
    remote_inventory = "/tmp/pathfinder-h48-inventory-99874356.json"
    copied = subprocess.run([
        "scp", "-q", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15",
        "-J", "pathfinder@94.237.65.184", str(inventory),
        "pathfinder@10.70.0.11:" + remote_inventory,
    ], capture_output=True, timeout=60, check=False)
    if copied.returncode:
        raise RuntimeError("public media inventory transfer failed")
    result = subprocess.run([
        "ssh", "-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15",
        "-J", "pathfinder@94.237.65.184", "pathfinder@10.70.0.11",
        "sudo -n python3 - --official-csv "
        "/opt/pathfinder/formal/private/multiq-24route-1c9ad84-v1/official-val.csv"
        " --protocol-base64 " + encoded + " --media-inventory " + remote_inventory,
    ], input=(ROOT / "experiments/fresh_multiq_cohort.py").read_bytes(),
        capture_output=True, timeout=60, check=False)
    if result.returncode:
        raise RuntimeError(f"public selection failed: SSH exit {result.returncode}")
    report = json.loads(result.stdout)
    if (report["protocol_sha256"] != hashlib.sha256(canonical(protocol)).hexdigest()
            or len(report["selected_object_ids"]) != 4
            or len(report["tasks"]) != 12
            or set(report["selected_object_ids"]) & set(protocol["excluded_object_ids"])
            or report["label_values_included"] is not False):
        raise ValueError("public selection binding differs")
    output.mkdir(exist_ok=False)
    write_new(output / "public-selection.json", report)
    raw = (output / "public-selection.json").read_bytes()
    with (output / "SHA256SUMS").open("xb") as handle:
        handle.write(f"{hashlib.sha256(raw).hexdigest()}  public-selection.json\n".encode())
    print(json.dumps({"status": "PUBLIC_COHORT_SELECTED",
                      "objects": report["selected_object_ids"],
                      "eligible_objects": report["eligible_object_count"],
                      "questions": len(report["tasks"])}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("protocol", "select"))
    args = parser.parse_args()
    (freeze_protocol if args.phase == "protocol" else select_remote)()
