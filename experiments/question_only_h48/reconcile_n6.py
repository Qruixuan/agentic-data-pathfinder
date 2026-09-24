"""Read-only N6 journal join for the public question-only diagnostic."""

from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import sys


def read_rows(path: Path, sql: str, key: str) -> list[tuple]:
    connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    try:
        connection.execute("PRAGMA query_only=ON")
        return connection.execute(sql, (key,)).fetchall()
    finally:
        connection.close()


def main() -> None:
    summary = json.loads(Path(sys.argv[1]).read_bytes())
    if (summary.get("status")
            != "VERIFIED_QUESTION_ONLY_DEVELOPMENT_DIAGNOSTIC"
            or len(summary.get("observations", [])) != 12):
        raise ValueError("public diagnostic summary is incomplete")
    usage_db = Path("/state/n6-provider-usage-v1.sqlite3")
    attempt_db = Path("/state/n6-provider-trace-v1.sqlite3")
    count = 0
    for row in summary["observations"]:
        usage = read_rows(usage_db, """
            SELECT result_sha256, input_units, cached_input_units,
                   output_units FROM n6_provider_usage WHERE request_sha256=?
        """, row["n6_request_sha256"])
        if usage != [(
            row["n6_result_sha256"], row["input_units"],
            row["cached_input_units"], row["output_units"],
        )]:
            raise ValueError("N6 durable usage does not bind the result")
        attempts = read_rows(attempt_db, """
            SELECT attempt_index, outcome, result_sha256
            FROM n6_provider_attempts WHERE request_sha256=?
            ORDER BY attempt_index
        """, row["n6_request_sha256"])
        if (not 1 <= len(attempts) <= 3
                or [item[0] for item in attempts]
                != list(range(len(attempts)))
                or attempts[-1][1:] != (
                    "completed", row["n6_result_sha256"]
                )):
            raise ValueError("N6 provider attempts differ from frozen budget")
        count += len(attempts)
    if count > 36:
        raise ValueError("N6 provider attempt budget exceeded")
    print(json.dumps({
        "status": "VERIFIED_QUESTION_ONLY_N6_USAGE_AND_ATTEMPTS",
        "matched_results": 12, "provider_attempts": count,
        "max_provider_attempts": 36,
        "provider_ids_included": False, "prompts_or_answers_included": False,
        "credentials_recorded": False,
    }, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(json.dumps({"status": "N6_RECONCILIATION_FAILED",
                          "error_class": type(exc).__name__}))
        raise SystemExit(2) from None
