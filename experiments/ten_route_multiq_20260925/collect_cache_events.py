"""Collect the two read-only, credential-free t60 cache event exports."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess


TARGETS = (
    ("N7", "10.70.0.17", "t60n7cache-pathfinder-full-flow-n7-persistent-cache-1"),
    ("N8", "10.70.0.18", "t60n8cache-pathfinder-full-flow-n8-persistent-cache-1"),
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.output_dir.exists():
        raise ValueError("output directory already exists")
    source = Path(__file__).with_name("export_cache_events.py").read_bytes()
    exports = []
    for node, address, container in TARGETS:
        command = [
            "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8",
            "-J", "pathfinder@94.237.65.184", f"pathfinder@{address}",
            f"sudo -n docker exec -i {container} python -",
        ]
        result = subprocess.run(
            command, input=source, capture_output=True, timeout=45,
            check=True,
        )
        export = json.loads(result.stdout)
        if (export.get("node_id") != node
                or export.get("credentials_recorded") is not False
                or export.get("schema_version")
                != "pathfinder.t60-cache-event-export/v1"):
            raise ValueError("cache export identity or safety check failed")
        exports.append((node, export))
    args.output_dir.mkdir(parents=True)
    checksums = []
    for node, export in exports:
        path = args.output_dir / f"{node.lower()}-cache-events.json"
        path.write_text(json.dumps(export, sort_keys=True, indent=2) + "\n",
                        encoding="utf-8", newline="\n")
        checksums.append(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  "
                         f"{path.name}")
    (args.output_dir / "SHA256SUMS").write_text(
        "\n".join(checksums) + "\n", encoding="utf-8", newline="\n"
    )
    print(json.dumps({"status": "CACHE_EVENTS_EXPORTED",
                      "rows": {node: len(export["events"])
                               for node, export in exports}}, sort_keys=True))


if __name__ == "__main__":
    main()
