"""Numeric-only provider-attempt journal for a dedicated Pathfinder worker.

This wraps only the HTTP transport boundary. It never inspects request or
response bodies, authorization headers, prompts, answers, or raw identifiers.
The resulting request-ID hashes can be joined to a separately exported
Model Studio audit log without putting provider IDs in a public receipt.
"""

from __future__ import annotations

from contextvars import ContextVar
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
import re
from threading import Lock
from typing import Any


_TASK_ID = re.compile(
    r"tsk-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12}\Z"
)
_REQUEST_ID = re.compile(r"[A-Za-z0-9_.:-]{1,256}\Z")
_PROVIDER_HOST = "dashscope-intl.aliyuncs.com"
_attempts: ContextVar[list[dict[str, Any]] | None] = ContextVar(
    "pathfinder_provider_attempts", default=None,
)
_install_lock = Lock()
_installed = False


def _request_id_hash(headers: Any) -> str | None:
    for name in ("x-request-id", "x-dashscope-request-id"):
        value = headers.get(name)
        if isinstance(value, str) and _REQUEST_ID.fullmatch(value):
            return sha256(value.encode("ascii")).hexdigest()
    return None


def _is_model_request(request: Any) -> bool:
    url = getattr(request, "url", None)
    return (
        getattr(url, "host", None) == _PROVIDER_HOST
        and getattr(url, "path", "").rstrip("/").endswith(
            "/chat/completions"
        )
        and getattr(request, "method", None) == "POST"
    )


def install_httpx_attempt_capture() -> None:
    """Install once; each task's context variable isolates its attempts."""
    global _installed
    with _install_lock:
        if _installed:
            return
        import httpx

        original_send = httpx.AsyncClient.send

        async def send(self: Any, request: Any, *args: Any,
                       **kwargs: Any) -> Any:
            collector = _attempts.get()
            if collector is None or not _is_model_request(request):
                return await original_send(self, request, *args, **kwargs)
            row: dict[str, Any] = {
                "attempt_index": len(collector) + 1,
                "started_utc": datetime.now(timezone.utc).isoformat(),
                "http_status": None,
                "request_id_sha256": None,
                "transport_error_class": None,
            }
            collector.append(row)
            try:
                response = await original_send(self, request, *args, **kwargs)
            except Exception as exc:
                row["transport_error_class"] = type(exc).__name__
                raise
            row["http_status"] = response.status_code
            row["request_id_sha256"] = _request_id_hash(response.headers)
            return response

        httpx.AsyncClient.send = send
        _installed = True


def begin_httpx_attempt_capture(out_dir: str | Path) -> bool:
    """Bind only an exact FlowMesh task directory, before Agent execution."""
    if _TASK_ID.fullmatch(Path(out_dir).name) is None:
        return False
    install_httpx_attempt_capture()
    _attempts.set([])
    return True


def take_httpx_attempts() -> list[dict[str, Any]]:
    """Consume this task's metadata, leaving no context for another task."""
    rows = _attempts.get()
    _attempts.set(None)
    return list(rows) if rows is not None else []
