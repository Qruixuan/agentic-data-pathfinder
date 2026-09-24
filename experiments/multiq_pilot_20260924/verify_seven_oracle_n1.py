"""Read-only uid-10001 verification of the N1 seven-label package."""

from __future__ import annotations

import json
from pathlib import Path

from pathfinder.simulator.hidden_oracle import verify_n1_oracle_package
from pathfinder.simulator.hidden_oracle_commitment import (
    verify_n1_oracle_preselection_commitment,
)


root = Path("/private/multiq-sealed-test-staging-20260924-v2/oracle")
package = verify_n1_oracle_package(root / "n1-oracle-package")
public = verify_n1_oracle_preselection_commitment(
    root / "oracle-commitment",
    oracle_package_dir=root / "n1-oracle-package",
)
if package["label_count"] != public["label_count"] or (
    package["label_count"] != 7
):
    raise ValueError("seven-label binding differs")
print(json.dumps({
    "status": "VERIFIED_N1_SEVEN_LABEL_PRIVATE_PACKAGE",
    "label_count": package["label_count"],
    "oracle_id": package["oracle_id"],
    "commitment_sha256": public["commitment_sha256"],
    "label_values_returned": False,
    "credentials_recorded": False,
}, sort_keys=True))
