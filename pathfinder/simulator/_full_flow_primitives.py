"""Small, deterministic primitives shared by full-flow boundary modules.

This module deliberately contains no filesystem or publication policy.  Callers
retain their domain-specific error types, messages, and file-handling rules while
sharing the byte-level JSON, digest, identifier, and strict-decoding mechanics.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Iterable, Mapping
from typing import Any


LOWER_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
W4_IDENTIFIER_PATTERN = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9._|:+-]{0,255}\Z"
)


def canonical_json_bytes(
    value: Any,
    *,
    error_type: type[Exception] | None = None,
    error_message: str | None = None,
) -> bytes:
    """Encode canonical JSON, optionally translating encoding failures."""

    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        if error_type is None:
            raise
        if error_message is None:
            raise ValueError("error_message is required with error_type") from exc
        raise error_type(error_message) from exc


def pretty_json_bytes(
    value: Any,
    *,
    error_type: type[Exception] | None = None,
    error_message: str | None = None,
) -> bytes:
    """Encode stable, indented UTF-8 JSON with one trailing newline.

    Boundary modules may request translation into their domain exception while
    retaining the original JSON encoding failure as the exception cause.
    """

    try:
        return (
            json.dumps(
                value,
                allow_nan=False,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        if error_type is None:
            raise
        if error_message is None:
            raise ValueError("error_message is required with error_type") from exc
        raise error_type(error_message) from exc


def canonical_json_lines_bytes(
    values: Iterable[Mapping[str, Any]],
    *,
    error_type: type[Exception] | None = None,
    error_message: str | None = None,
) -> bytes:
    """Encode mappings as canonical JSON Lines without buffering text."""

    return b"".join(
        canonical_json_bytes(
            value,
            error_type=error_type,
            error_message=error_message,
        )
        + b"\n"
        for value in values
    )


def sha256_hex(value: bytes) -> str:
    """Return the lowercase SHA-256 hex digest for *value*."""

    return hashlib.sha256(value).hexdigest()


def checksum_manifest_bytes(
    documents: Mapping[str, bytes],
    *,
    encoding: str = "utf-8",
) -> bytes:
    """Encode the repository's stable two-space SHA-256 manifest format."""

    return "".join(
        f"{sha256_hex(documents[name])}  {name}\n"
        for name in sorted(documents)
    ).encode(encoding)


def checked_identifier(
    value: Any,
    label: str,
    *,
    error_type: type[Exception],
    pattern: re.Pattern[str] = W4_IDENTIFIER_PATTERN,
    message: str | None = None,
) -> str:
    """Return a string matching *pattern* or raise the caller's error type."""

    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise error_type(message if message is not None else f"{label} is invalid")
    return value


def checked_lower_sha256(
    value: Any,
    label: str,
    *,
    error_type: type[Exception],
    pattern: re.Pattern[str] = LOWER_SHA256_PATTERN,
    message: str | None = None,
) -> str:
    """Return a lowercase SHA-256 string or raise the caller's error type."""

    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise error_type(
            message
            if message is not None
            else f"{label} is not lowercase SHA-256"
        )
    return value


def strict_json_loads(
    raw: bytes | str,
    *,
    error_type: type[Exception],
    duplicate_key_message: Callable[[str], str],
    nonfinite_number_message: Callable[[str], str],
) -> Any:
    """Decode JSON while rejecting duplicate keys and non-finite numbers.

    Syntax and Unicode errors intentionally propagate so each boundary can keep
    its existing error translation and exception chaining.
    """

    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise error_type(duplicate_key_message(key))
            result[key] = value
        return result

    def invalid_number(token: str) -> None:
        raise error_type(nonfinite_number_message(token))

    return json.loads(
        raw,
        object_pairs_hook=unique,
        parse_constant=invalid_number,
    )
