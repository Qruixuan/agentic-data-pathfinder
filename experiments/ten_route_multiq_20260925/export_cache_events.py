"""Export only public, numeric cache events from an isolated cache volume.

Run with ``python -`` inside the cache container. The source database is
opened read-only and no credential or object payload is inspected.
"""

from __future__ import annotations

import json
import sqlite3


connection = sqlite3.connect("file:/state/cache.sqlite3?mode=ro", uri=True)
connection.row_factory = sqlite3.Row
rows = connection.execute(
    "SELECT event_id, event_kind, cache_key, cache_namespace, object_id, "
    "representation_id, content_sha256, size_bytes "
    "FROM cache_events ORDER BY event_id"
).fetchall()
events = [dict(row) for row in rows]
stores = {}
for row in connection.execute("SELECT result_json FROM cache_requests"):
    result = json.loads(row["result_json"])
    if result.get("status") != "STORED":
        continue
    event_id = result["event_id"]
    if event_id in stores:
        raise ValueError("duplicate cache store event")
    stores[event_id] = [
        {"object_id": item["object_id"],
         "representation_id": item["representation_id"]}
        for item in result["evicted"]
    ]
state = connection.execute(
    "SELECT node_id, cache_id, capacity_bytes FROM cache_state "
    "WHERE singleton = 1"
).fetchone()
connection.close()
print(json.dumps({
    "schema_version": "pathfinder.t60-cache-event-export/v1",
    "node_id": state["node_id"],
    "cache_id": state["cache_id"],
    "capacity_bytes": state["capacity_bytes"],
    "events": events,
    "store_evictions_by_event_id": stores,
    "credentials_recorded": False,
}, sort_keys=True))
