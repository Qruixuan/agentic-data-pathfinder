"""Read-only N6 numeric journal baseline; never prints request bodies or IDs."""

import json
import sqlite3


def _row(path, statement):
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        connection.execute("PRAGMA query_only=ON")
        return connection.execute(statement).fetchone()
    finally:
        connection.close()


usage = _row(
    "/state/n6-provider-usage-v1.sqlite3",
    "SELECT COUNT(*), COALESCE(SUM(input_units),0), "
    "COALESCE(SUM(cached_input_units),0), "
    "COALESCE(SUM(output_units),0) FROM n6_provider_usage",
)
attempts = _row(
    "/state/n6-provider-trace-v1.sqlite3",
    "SELECT COUNT(*) FROM n6_provider_attempts",
)
print(json.dumps({
    "status": "READ_ONLY_N6_JOURNAL_COUNTERS",
    "usage_rows": usage[0],
    "input_units": usage[1],
    "cached_input_units": usage[2],
    "output_units": usage[3],
    "provider_attempt_rows": attempts[0],
    "credentials_recorded": False,
}))
