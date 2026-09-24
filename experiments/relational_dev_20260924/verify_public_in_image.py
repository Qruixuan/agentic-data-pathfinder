"""No-call source-bound admission check using a deployed service image."""

import hashlib
import json
from pathlib import Path

from pathfinder.simulator.interleaved_multiq_runtime_admission import (
    verify_interleaved_runtime_admission,
)


root = Path("/public")
config_root = root / "runtime/config"
config_file = config_root / "batch-config.json"
raw = config_file.read_bytes()
if (config_root / "SHA256SUMS").read_bytes() != (
    f"{hashlib.sha256(raw).hexdigest()}  batch-config.json\n".encode()
):
    raise ValueError("frozen batch configuration checksum differs")
config = json.loads(raw)
sources = {key: root / value for key, value in config["source_dirs"].items()}
result = verify_interleaved_runtime_admission(
    root / config["admission_dir"],
    **sources,
    coordinator_base_url=config["coordinator_base_url"],
)
if (result["admission_sha256"] !=
        "2dbf9859fe2b8e473e11b4a8ea86e574b8fc77787d80ec649af8ce5f62b86f8f"
        or result["trial_count"] != 24):
    raise ValueError("runtime admission identity or cardinality differs")
print(json.dumps({"status": "VERIFIED_DEPLOYED_IMAGE_SOURCE_BINDING",
                  "admission_sha256": result["admission_sha256"],
                  "trial_count": result["trial_count"],
                  "workflow_submitted": False,
                  "llm_called": False,
                  "credentials_recorded": False}))
