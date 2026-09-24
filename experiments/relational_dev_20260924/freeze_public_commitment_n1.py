"""Complete a public N1 commitment after private build succeeded."""

import json
from pathlib import Path

from pathfinder.simulator.hidden_oracle_commitment import (
    freeze_n1_oracle_preselection_commitment,
    verify_n1_oracle_preselection_commitment,
)


oracle = Path("/private/output/oracle/n1-oracle-package")
public = Path("/public/commitment")
if not oracle.is_dir() or public.exists():
    raise ValueError("private package missing or public commitment already exists")
freeze_n1_oracle_preselection_commitment(
    oracle,
    commitment_id="relational-multiq-development-20260924-v1-commitment",
    output_dir=public,
)
report = verify_n1_oracle_preselection_commitment(
    public, oracle_package_dir=oracle,
)
print(json.dumps({
    "status": "VERIFIED_N1_PUBLIC_COMMITMENT",
    "label_count": report["label_count"],
    "hidden_label_values_returned": False,
    "credentials_recorded": False,
}))
