"""Export numeric N6 usage through SSH without exposing prompts or secrets."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess


CONTAINER = (
    "pathfinder-multiq-d328726-n6-"
    "pathfinder-full-flow-n6-semantic-inference-1"
)


def collect(exporter: Path, output_dir: Path) -> dict:
    if output_dir.exists():
        raise ValueError("numeric usage output directory already exists")
    command = [
        "ssh", "-o", "BatchMode=yes", "-J",
        "pathfinder@94.237.65.184", "pathfinder@10.70.0.16",
        "sudo", "-n", "docker", "exec", "-i", CONTAINER,
        "python", "-", "--database",
        "/state/n6-provider-usage-v1.sqlite3",
    ]
    result = subprocess.run(
        command, input=exporter.read_bytes(), capture_output=True,
        check=True, timeout=60,
    )
    report = json.loads(result.stdout)
    if (
        report.get("schema_version")
        != "pathfinder.n6-numeric-usage-export/v1"
        or report.get("credentials_recorded") is not False
        or report.get("record_count") != len(report.get("rows", []))
        or report["record_count"] < 52
    ):
        raise ValueError("N6 numeric usage export is incomplete")
    output_dir.mkdir(parents=True, exist_ok=False)
    payload = json.dumps(
        report, indent=2, sort_keys=True, ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8") + b"\n"
    with (output_dir / "n6-usage-export.json").open("xb") as handle:
        handle.write(payload)
    with (output_dir / "SHA256SUMS").open(
        "x", encoding="ascii", newline="\n",
    ) as handle:
        handle.write(
            f"{hashlib.sha256(payload).hexdigest()}  n6-usage-export.json\n"
        )
    return {
        "status": "N6_NUMERIC_USAGE_EXPORTED",
        "record_count": report["record_count"],
        "output_dir": str(output_dir),
        "credentials_recorded": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--exporter", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(collect(args.exporter, args.output_dir)))


if __name__ == "__main__":
    main()
