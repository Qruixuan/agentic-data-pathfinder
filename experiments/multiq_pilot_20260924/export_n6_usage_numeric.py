"""Print only digest-bound numeric N6 usage from a read-only SQLite journal.

Run inside the N6 container with --database /state/n6-provider-usage-v1.sqlite3.
The export deliberately excludes prompts, answers, provider IDs and secrets.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sqlite3


def export(database: Path) -> dict:
    if not database.is_file():
        raise ValueError("N6 usage journal is missing")
    connection = sqlite3.connect(f"file:{database.as_posix()}?mode=ro", uri=True)
    try:
        connection.execute("PRAGMA query_only=ON")
        rows = connection.execute("""
            SELECT result_sha256, request_sha256, input_units,
                   cached_input_units, output_units, total_units
            FROM n6_provider_usage ORDER BY result_sha256
        """).fetchall()
    finally:
        connection.close()
    records = []
    for result_sha256, request_sha256, input_units, cached, output, total in rows:
        if (
            not isinstance(result_sha256, str) or len(result_sha256) != 64
            or not isinstance(request_sha256, str) or len(request_sha256) != 64
            or not all(type(value) is int and value >= 0 for value in (
                input_units, cached, output, total,
            ))
            or cached > input_units or total != input_units + output
        ):
            raise ValueError("N6 numeric usage row is invalid")
        records.append({
            "result_sha256": result_sha256,
            "request_sha256": request_sha256,
            "input_units": input_units,
            "cached_input_units": cached,
            "output_units": output,
            "total_units": total,
        })
    return {
        "schema_version": "pathfinder.n6-numeric-usage-export/v1",
        "record_count": len(records),
        "rows": records,
        "credentials_recorded": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(export(args.database), sort_keys=True))


if __name__ == "__main__":
    main()
