from __future__ import annotations

import os
import re
from hashlib import sha256
from urllib.parse import urlsplit


MAXIMUM_REDACTED_LENGTH = 2000

_SECRET_ENVIRONMENT_NAMES = (
    "FLOWMESH_API_KEY",
    "PATHFINDER_DATA_AGENT_TOKEN",
    "UTU_LLM_API_KEY",
)


def redact_secrets(
    message: str,
    *,
    limit: int = MAXIMUM_REDACTED_LENGTH,
) -> str:
    """Strip credentials and signed-URL query strings out of free text.

    Third-party exceptions and control-plane error strings must not turn a
    research log, a manifest, or terminal output into a credential sink.
    """
    result = str(message)
    for name in _SECRET_ENVIRONMENT_NAMES:
        value = os.getenv(name)
        if value:
            result = result.replace(value, "<redacted>")
    result = re.sub(
        r"(?i)(authorization\s*[:=]\s*bearer\s+)\S+",
        r"\1<redacted>",
        result,
    )
    result = re.sub(r"(?i)(bearer\s+)\S+", r"\1<redacted>", result)
    result = re.sub(r"(https?://[^\s?]+)\?\S+", r"\1?<redacted>", result)
    return result[:limit]


def sanitize_endpoint(base_url: str) -> str:
    """Return scheme, host, and port only.

    Any embedded user info is a credential, and a path or query string can
    carry a token, so neither is ever echoed back to the operator.
    """
    try:
        parts = urlsplit(str(base_url).strip())
    except ValueError:
        return "<unparsable-endpoint>"
    if not parts.scheme or not parts.hostname:
        return "<unparsable-endpoint>"
    try:
        port = parts.port
    except ValueError:
        return "<unparsable-endpoint>"
    host = parts.hostname
    return f"{parts.scheme}://{host}" + (f":{port}" if port is not None else "")


def endpoint_fingerprint(base_url: str) -> str:
    """Return a stable correlation key for a sanitized endpoint."""
    return sha256(sanitize_endpoint(base_url).encode("utf-8")).hexdigest()
