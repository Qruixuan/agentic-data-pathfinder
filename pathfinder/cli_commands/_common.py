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


# Credential contracts, one per service family. These are deliberately NOT a
# single precedence table: the families genuinely disagree, and one shared
# table let the Data Agent rule be applied to the index and cache services,
# where it selected a token no deployed server accepts.
#
# Each tuple lists the credential the frozen service-bootstrap contract
# declares for that service first, then any variable kept only so an older
# deployment that sets just that one keeps working. A trailing fallback can
# never override the contracted credential.

#: N3/N4 Data Agents. Each agent authenticates with its own token; the shared
#: PATHFINDER_DATA_AGENT_TOKEN is a fallback only. Listing the shared name
#: first made every N4 access carry N3's token and answer HTTP 401.
DATA_AGENT_CREDENTIALS: dict[str, tuple[str, ...]] = {
    "N3": (
        "PATHFINDER_N3_DATA_AGENT_TOKEN",
        "PATHFINDER_DATA_AGENT_TOKEN",
    ),
    "N4": (
        "PATHFINDER_N4_DATA_AGENT_TOKEN",
        "PATHFINDER_DATA_AGENT_TOKEN",
    ),
}

#: Regular index services. N2.global-index, N7.local-index and N8.local-index
#: all declare PATHFINDER_N2_INDEX_TOKEN, so a node-specific index variable
#: must not override it: the local index servers do not accept that value.
INDEX_CREDENTIALS: dict[str, tuple[str, ...]] = {
    "N2": ("PATHFINDER_N2_INDEX_TOKEN",),
    "N7": (
        "PATHFINDER_N2_INDEX_TOKEN",
        "PATHFINDER_N7_INDEX_TOKEN",
    ),
    "N8": (
        "PATHFINDER_N2_INDEX_TOKEN",
        "PATHFINDER_N8_INDEX_TOKEN",
    ),
}

#: Regular persistent caches. N7/N8.persistent-cache declare
#: PATHFINDER_FULL_FLOW_CACHE_TOKEN.
PERSISTENT_CACHE_CREDENTIALS: dict[str, tuple[str, ...]] = {
    "N7": (
        "PATHFINDER_FULL_FLOW_CACHE_TOKEN",
        "PATHFINDER_N7_FULL_FLOW_CACHE_TOKEN",
    ),
    "N8": (
        "PATHFINDER_FULL_FLOW_CACHE_TOKEN",
        "PATHFINDER_N8_FULL_FLOW_CACHE_TOKEN",
    ),
}

#: W4 candidate caches are a separate family: each declares its own dedicated
#: PATHFINDER_<node>_W4_CACHE_TOKEN, so the regular persistent-cache rule must
#: not be applied to them.
W4_CACHE_CREDENTIALS: dict[str, tuple[str, ...]] = {
    "N7": (
        "PATHFINDER_N7_W4_CACHE_TOKEN",
        "PATHFINDER_FULL_FLOW_CACHE_TOKEN",
        "PATHFINDER_N7_FULL_FLOW_CACHE_TOKEN",
    ),
    "N8": (
        "PATHFINDER_N8_W4_CACHE_TOKEN",
        "PATHFINDER_FULL_FLOW_CACHE_TOKEN",
        "PATHFINDER_N8_FULL_FLOW_CACHE_TOKEN",
    ),
}

_SINGLE_SERVICE_CREDENTIALS: dict[str, tuple[str, ...]] = {
    "N6 semantic": ("PATHFINDER_CONTAINER_NODE_TOKEN",),
    "N1 score": ("PATHFINDER_N1_ORACLE_TOKEN",),
    "N1 verifier": ("PATHFINDER_N1_VERIFICATION_TOKEN",),
    "route ingress": ("PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET",),
    "W4 ingress": ("PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET",),
}


def semantic_route_credential_contract() -> dict[str, tuple[str, ...]]:
    """Credentials the semantic route service uses, explicit by service."""

    return {
        "N2 index": INDEX_CREDENTIALS["N2"],
        "N7 index": INDEX_CREDENTIALS["N7"],
        "N8 index": INDEX_CREDENTIALS["N8"],
        "N3 Data Agent": DATA_AGENT_CREDENTIALS["N3"],
        "N4 Data Agent": DATA_AGENT_CREDENTIALS["N4"],
        "N7 cache": PERSISTENT_CACHE_CREDENTIALS["N7"],
        "N8 cache": PERSISTENT_CACHE_CREDENTIALS["N8"],
        "N6 semantic": _SINGLE_SERVICE_CREDENTIALS["N6 semantic"],
        "N1 score": _SINGLE_SERVICE_CREDENTIALS["N1 score"],
        "N1 verifier": _SINGLE_SERVICE_CREDENTIALS["N1 verifier"],
        "route ingress": _SINGLE_SERVICE_CREDENTIALS["route ingress"],
    }


def w4_credential_contract(
    *,
    dedicated_cache: bool,
) -> dict[str, tuple[str, ...]]:
    """Credentials a W4 command uses, explicit by service.

    ``dedicated_cache`` selects the W4 candidate caches, which carry their own
    token, instead of the regular persistent caches.
    """

    cache = (
        W4_CACHE_CREDENTIALS
        if dedicated_cache
        else PERSISTENT_CACHE_CREDENTIALS
    )
    contract = {
        "N2 index": INDEX_CREDENTIALS["N2"],
        "N7 index": INDEX_CREDENTIALS["N7"],
        "N8 index": INDEX_CREDENTIALS["N8"],
        "N3 Data Agent": DATA_AGENT_CREDENTIALS["N3"],
        "N4 Data Agent": DATA_AGENT_CREDENTIALS["N4"],
        "N7 cache": cache["N7"],
        "N8 cache": cache["N8"],
        "N6 semantic": _SINGLE_SERVICE_CREDENTIALS["N6 semantic"],
    }
    if dedicated_cache:
        contract["W4 ingress"] = _SINGLE_SERVICE_CREDENTIALS["W4 ingress"]
    return contract


def select_credential(
    environment: Mapping[str, str],
    candidate_names: Sequence[str],
) -> str | None:
    """Return the first non-empty candidate in contract order.

    Returns ``None`` when no candidate is set so the caller fails closed; it
    never substitutes a different service's credential for a missing one.
    """

    for name in candidate_names:
        value = environment.get(name)
        if value:
            return value
    return None


def resolve_credentials(
    environment: Mapping[str, str],
    contract: Mapping[str, Sequence[str]],
) -> tuple[dict[str, str | None], list[str]]:
    """Resolve every credential in one contract and report the unset ones.

    The second element lists candidate variable names, never a credential
    value, so a caller can fail closed with an actionable message.
    """

    resolved = {
        name: select_credential(environment, candidates)
        for name, candidates in contract.items()
    }
    missing = sorted({
        " or ".join(contract[name])
        for name, value in resolved.items()
        if not value
    })
    return resolved, missing
