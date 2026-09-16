"""Small parser helpers shared by Pathfinder command families."""

from __future__ import annotations

import argparse
from pathlib import Path
from collections.abc import Mapping, Sequence
from typing import Protocol


class PayloadPrinter(Protocol):
    def __call__(self, payload: object, *, compact: bool) -> int: ...


def add_required_path(
    command: argparse.ArgumentParser,
    option: str,
) -> None:
    command.add_argument(option, type=Path, required=True)


def add_compact(command: argparse.ArgumentParser) -> None:
    command.add_argument("--compact", action="store_true")


def add_output_dir(command: argparse.ArgumentParser) -> None:
    add_required_path(command, "--output-dir")


# Node-specific credentials take precedence over shared fallbacks.  The
# resolver returns the first defined candidate, so a shared value listed first
# silently shadows the node's own token: that is how the N4 Data Agent came to
# be addressed with the N3 token and answered 401.  A deployment that sets only
# the shared value keeps working through the fallback.
FULL_FLOW_CREDENTIAL_PRECEDENCE: dict[str, tuple[str, ...]] = {
    "N2 index": ("PATHFINDER_N2_INDEX_TOKEN",),
    "N7 index": (
        "PATHFINDER_N7_INDEX_TOKEN",
        "PATHFINDER_N2_INDEX_TOKEN",
    ),
    "N8 index": (
        "PATHFINDER_N8_INDEX_TOKEN",
        "PATHFINDER_N2_INDEX_TOKEN",
    ),
    "N3 Data Agent": (
        "PATHFINDER_N3_DATA_AGENT_TOKEN",
        "PATHFINDER_DATA_AGENT_TOKEN",
    ),
    "N4 Data Agent": (
        "PATHFINDER_N4_DATA_AGENT_TOKEN",
        "PATHFINDER_DATA_AGENT_TOKEN",
    ),
    "N7 cache": (
        "PATHFINDER_N7_FULL_FLOW_CACHE_TOKEN",
        "PATHFINDER_FULL_FLOW_CACHE_TOKEN",
    ),
    "N8 cache": (
        "PATHFINDER_N8_FULL_FLOW_CACHE_TOKEN",
        "PATHFINDER_FULL_FLOW_CACHE_TOKEN",
    ),
    "N6 semantic": ("PATHFINDER_CONTAINER_NODE_TOKEN",),
    "N1 score": ("PATHFINDER_N1_ORACLE_TOKEN",),
    "N1 verifier": ("PATHFINDER_N1_VERIFICATION_TOKEN",),
    "route ingress": ("PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET",),
}


def select_credential(
    environment: Mapping[str, str],
    candidate_names: Sequence[str],
) -> str | None:
    """Return the first non-empty candidate, node-specific names first.

    Returns ``None`` when no candidate is set so the caller fails closed; it
    never substitutes a different node's credential for a missing one.
    """

    for name in candidate_names:
        value = environment.get(name)
        if value:
            return value
    return None


def resolve_full_flow_credentials(
    environment: Mapping[str, str],
    names: Sequence[str],
) -> tuple[dict[str, str | None], list[str]]:
    """Resolve the named credentials and report which are unset.

    The second element lists the candidate names of every unresolved
    credential, so a caller can fail closed with an actionable message. No
    credential value is ever placed in that list.
    """

    resolved = {
        name: select_credential(
            environment,
            FULL_FLOW_CREDENTIAL_PRECEDENCE[name],
        )
        for name in names
    }
    missing = sorted({
        " or ".join(FULL_FLOW_CREDENTIAL_PRECEDENCE[name])
        for name, value in resolved.items()
        if not value
    })
    return resolved, missing
