"""Authenticated remote verification for N1 hidden-score evidence.

Scoring evidence is signed with an HMAC key that belongs to N1.  This module
keeps that key, and the hidden oracle package needed to recompute a score, out
of the N7/N8 execution domains: an N1 companion service verifies the exact
score request/result pair and returns a public, content-bound attestation.

The bearer credential authenticates callers and HTTPS authenticates a remote
N1 service.  Plain HTTP is accepted only on loopback or for an explicitly
allowlisted simulator hostname.  Each verification request carries a fresh
256-bit challenge; N1 records it durably and rejects its reuse.  The returned
attestation is bound to that challenge and to canonical SHA-256 commitments
of both supplied evidence objects, so a stale or substituted response is not
accepted by the client.
"""

from __future__ import annotations

import hashlib
import hmac
import ipaddress
import json
import re
import secrets
import sqlite3
from contextlib import closing
from dataclasses import dataclass, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import (
    HTTPRedirectHandler,
    ProxyHandler,
    Request,
    build_opener,
)

from .hidden_oracle import (
    N1_NODE_ID,
    N1OracleError,
    assert_hidden_oracle_fields_absent,
    verify_n1_oracle_package,
    verify_n1_score_result,
)


N1_REMOTE_VERIFICATION_API_VERSION = (
    "pathfinder.n1-score-verification-service/v1alpha1"
)
N1_REMOTE_VERIFICATION_REQUEST_SCHEMA_VERSION = (
    "pathfinder.n1-score-verification-request/v1alpha1"
)
N1_REMOTE_VERIFICATION_ATTESTATION_SCHEMA_VERSION = (
    "pathfinder.n1-score-verification-attestation/v1alpha1"
)
N1_REMOTE_VERIFICATION_STORE_SCHEMA_VERSION = (
    "pathfinder.n1-score-verification-store/v1alpha1"
)

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._|:+-]{0,255}\Z")
_SIMULATOR_HOST = re.compile(
    r"(?:pathfinder-sim|pathfinder-full-flow)-[a-z0-9-]+\Z"
)
_DEFAULT_MAX_REQUEST_BYTES = 768 * 1024
_DEFAULT_MAX_RESPONSE_BYTES = 64 * 1024
_DEFAULT_MAX_VERIFICATIONS = 1_000_000

_VERIFICATION_REQUEST_FIELDS = frozenset(
    {
        "schema_version",
        "verification_request_id",
        "oracle_id",
        "public_task_set_sha256",
        "score_request_sha256",
        "score_result_sha256",
        "score_request",
        "score_result",
        "credentials_recorded",
    }
)
_ATTESTATION_FIELDS = frozenset(
    {
        "schema_version",
        "status",
        "node_id",
        "verification_request_id",
        "oracle_id",
        "public_task_set_sha256",
        "score_request_id",
        "evaluation_unit_id",
        "run_id",
        "trial_id",
        "score_request_sha256",
        "score_result_sha256",
        "request_sha256",
        "prediction_sha256",
        "result_content_sha256",
        "score_evidence_hmac_sha256",
        "correct",
        "score",
        "replay_detected",
        "hidden_answer_returned",
        "credentials_recorded",
        "eligible_for_scientific_claims",
        "attestation_sha256",
    }
)
_HEALTH_FIELDS = frozenset(
    {
        "schema_version",
        "status",
        "node_id",
        "oracle_id",
        "public_task_set_sha256",
        "verification_mode",
        "durable_replay_rejection",
        "hidden_answer_returned",
        "credentials_recorded",
    }
)


class N1RemoteVerificationError(RuntimeError):
    """Raised when remote score verification cannot be trusted."""


class N1RemoteVerificationReplayError(N1RemoteVerificationError):
    """Raised when a verification challenge has already been consumed."""


class N1RemoteVerificationCapacityError(N1RemoteVerificationError):
    """Raised when the bounded durable replay ledger is full."""


class N1RemoteVerificationHTTPError(N1RemoteVerificationError):
    """Raised when the verification HTTP boundary fails closed."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise N1RemoteVerificationError(message)


def _canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise N1RemoteVerificationError(
            "verification value is not canonical JSON"
        ) from exc


def _sha256_value(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _digest(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _SHA256.fullmatch(value) is not None,
        f"{name} is not lowercase SHA-256",
    )
    return str(value)


def _identifier(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _IDENTIFIER.fullmatch(value) is not None,
        f"{name} is invalid",
    )
    return str(value)


def _text(value: Any, name: str, *, maximum: int) -> str:
    _require(isinstance(value, str) and bool(value), f"{name} must be text")
    _require(len(value.encode("utf-8")) <= maximum, f"{name} is too large")
    return str(value)


def _unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        _require(key not in value, f"duplicate JSON key: {key}")
        value[key] = item
    return value


def _json_object(raw: bytes, name: str) -> dict[str, Any]:
    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_unique_pairs,
            parse_constant=lambda item: (_ for _ in ()).throw(
                N1RemoteVerificationError(
                    f"{name} contains non-finite number {item}"
                )
            ),
        )
    except N1RemoteVerificationError:
        raise
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise N1RemoteVerificationError(
            f"{name} must be valid UTF-8 JSON"
        ) from exc
    _require(isinstance(value, dict), f"{name} must be an object")
    _require(raw == _canonical_bytes(value), f"{name} is not canonical JSON")
    return value


def _mapping_copy(value: Any, name: str) -> dict[str, Any]:
    _require(isinstance(value, Mapping), f"{name} must be an object")
    return json.loads(_canonical_bytes(value))


def _attestation_digest(core: Mapping[str, Any]) -> str:
    return _sha256_value(
        {
            "domain": "pathfinder.n1-remote-score-verification-attestation/v1",
            "claims": core,
        }
    )


def _new_verification_request_id() -> str:
    return secrets.token_hex(32)


def _loopback(hostname: str | None) -> bool:
    if hostname is None:
        return False
    if hostname.casefold() == "localhost":
        return True
    try:
        return ipaddress.ip_address(hostname).is_loopback
    except ValueError:
        return False


def _validate_verification_request(
    value: Any,
    *,
    expected_oracle_id: str,
    expected_public_task_set_sha256: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    envelope = _mapping_copy(value, "verification request")
    _require(
        set(envelope) == _VERIFICATION_REQUEST_FIELDS,
        "verification request fields changed",
    )
    _require(
        envelope.get("schema_version")
        == N1_REMOTE_VERIFICATION_REQUEST_SCHEMA_VERSION,
        "verification request schema changed",
    )
    _digest(envelope.get("verification_request_id"), "verification_request_id")
    _require(
        envelope.get("oracle_id") == expected_oracle_id,
        "verification oracle_id mismatch",
    )
    _require(
        envelope.get("public_task_set_sha256")
        == expected_public_task_set_sha256,
        "verification public task set mismatch",
    )
    _require(
        envelope.get("credentials_recorded") is False,
        "verification request records credentials",
    )
    request = _mapping_copy(envelope.get("score_request"), "score request")
    result = _mapping_copy(envelope.get("score_result"), "score result")
    assert_hidden_oracle_fields_absent(request)
    assert_hidden_oracle_fields_absent(result)
    _require(
        envelope.get("score_request_sha256") == _sha256_value(request),
        "score request content binding mismatch",
    )
    _require(
        envelope.get("score_result_sha256") == _sha256_value(result),
        "score result content binding mismatch",
    )
    _require(
        request.get("oracle_id") == expected_oracle_id
        and result.get("oracle_id") == expected_oracle_id,
        "score evidence oracle identity mismatch",
    )
    _require(
        result.get("public_task_set_sha256")
        == expected_public_task_set_sha256,
        "score evidence public task identity mismatch",
    )
    return envelope, request, result


def _build_attestation(
    *,
    envelope: Mapping[str, Any],
    result: Mapping[str, Any],
    verified: Mapping[str, Any],
    public_task_set_sha256: str,
) -> dict[str, Any]:
    core = {
        "schema_version": N1_REMOTE_VERIFICATION_ATTESTATION_SCHEMA_VERSION,
        "status": "VERIFIED",
        "node_id": N1_NODE_ID,
        "verification_request_id": envelope["verification_request_id"],
        "oracle_id": verified["oracle_id"],
        "public_task_set_sha256": public_task_set_sha256,
        "score_request_id": verified["score_request_id"],
        "evaluation_unit_id": verified["evaluation_unit_id"],
        "run_id": verified["run_id"],
        "trial_id": verified["trial_id"],
        "score_request_sha256": envelope["score_request_sha256"],
        "score_result_sha256": envelope["score_result_sha256"],
        "request_sha256": verified["request_sha256"],
        "prediction_sha256": verified["prediction_sha256"],
        "result_content_sha256": result["result_content_sha256"],
        "score_evidence_hmac_sha256": result[
            "score_evidence_hmac_sha256"
        ],
        "correct": verified["correct"],
        "score": verified["score"],
        "replay_detected": False,
        "hidden_answer_returned": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    return {**core, "attestation_sha256": _attestation_digest(core)}


class _VerificationReplayStore:
    def __init__(
        self,
        path: Path,
        *,
        oracle_id: str,
        public_task_set_sha256: str,
        evidence_key_id: str,
        max_verifications: int,
    ) -> None:
        _require(
            type(max_verifications) is int and max_verifications > 0,
            "max_verifications must be positive",
        )
        self.path = path.resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.oracle_id = oracle_id
        self.public_task_set_sha256 = public_task_set_sha256
        self.evidence_key_id = evidence_key_id
        self.max_verifications = max_verifications
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=30.0,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS verification_state (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    schema_version TEXT NOT NULL,
                    oracle_id TEXT NOT NULL,
                    public_task_set_sha256 TEXT NOT NULL,
                    evidence_key_id TEXT NOT NULL,
                    max_verifications INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS consumed_challenges (
                    verification_request_id TEXT PRIMARY KEY,
                    score_request_sha256 TEXT NOT NULL,
                    score_result_sha256 TEXT NOT NULL,
                    attestation_sha256 TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
            state = connection.execute(
                "SELECT * FROM verification_state WHERE singleton = 1"
            ).fetchone()
            expected = (
                N1_REMOTE_VERIFICATION_STORE_SCHEMA_VERSION,
                self.oracle_id,
                self.public_task_set_sha256,
                self.evidence_key_id,
                self.max_verifications,
            )
            if state is None:
                connection.execute(
                    """
                    INSERT INTO verification_state (
                        singleton, schema_version, oracle_id,
                        public_task_set_sha256, evidence_key_id,
                        max_verifications
                    ) VALUES (1, ?, ?, ?, ?, ?)
                    """,
                    expected,
                )
            else:
                actual = (
                    state["schema_version"],
                    state["oracle_id"],
                    state["public_task_set_sha256"],
                    state["evidence_key_id"],
                    state["max_verifications"],
                )
                _require(
                    actual == expected,
                    "verification database binding changed",
                )

    def consume(
        self,
        *,
        verification_request_id: str,
        score_request_sha256: str,
        score_result_sha256: str,
        attestation_sha256: str,
    ) -> None:
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                existing = connection.execute(
                    "SELECT 1 FROM consumed_challenges "
                    "WHERE verification_request_id = ?",
                    (verification_request_id,),
                ).fetchone()
                if existing is not None:
                    raise N1RemoteVerificationReplayError(
                        "verification challenge was already consumed"
                    )
                count = connection.execute(
                    "SELECT COUNT(*) AS count FROM consumed_challenges"
                ).fetchone()
                if count is None or int(count["count"]) >= self.max_verifications:
                    raise N1RemoteVerificationCapacityError(
                        "verification replay ledger is full"
                    )
                connection.execute(
                    """
                    INSERT INTO consumed_challenges (
                        verification_request_id, score_request_sha256,
                        score_result_sha256, attestation_sha256
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (
                        verification_request_id,
                        score_request_sha256,
                        score_result_sha256,
                        attestation_sha256,
                    ),
                )
                connection.execute("COMMIT")
            except BaseException:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                raise


class N1RemoteScoreVerificationService:
    """N1-local verifier holding both the hidden package and HMAC key."""

    def __init__(
        self,
        package_dir: str | Path,
        *,
        state_db: str | Path,
        evidence_secret: bytes,
        max_verifications: int = _DEFAULT_MAX_VERIFICATIONS,
    ) -> None:
        _require(
            isinstance(evidence_secret, bytes) and len(evidence_secret) >= 32,
            "evidence_secret must contain at least 32 bytes",
        )
        self._package_dir = Path(package_dir).resolve()
        manifest = verify_n1_oracle_package(self._package_dir)
        self.oracle_id = _identifier(manifest.get("oracle_id"), "oracle_id")
        self.public_task_set_sha256 = _digest(
            manifest.get("public_task_set_sha256"),
            "public_task_set_sha256",
        )
        self._evidence_secret = evidence_secret
        evidence_key_id = hmac.new(
            evidence_secret,
            _canonical_bytes(
                {
                    "domain": "pathfinder.n1-remote-verification-key/v1",
                    "oracle_id": self.oracle_id,
                }
            ),
            hashlib.sha256,
        ).hexdigest()
        self._store = _VerificationReplayStore(
            Path(state_db),
            oracle_id=self.oracle_id,
            public_task_set_sha256=self.public_task_set_sha256,
            evidence_key_id=evidence_key_id,
            max_verifications=max_verifications,
        )

    def health(self) -> dict[str, Any]:
        return {
            "schema_version": N1_REMOTE_VERIFICATION_API_VERSION,
            "status": "ok",
            "node_id": N1_NODE_ID,
            "oracle_id": self.oracle_id,
            "public_task_set_sha256": self.public_task_set_sha256,
            "verification_mode": "n1-local-hidden-package-and-hmac",
            "durable_replay_rejection": True,
            "hidden_answer_returned": False,
            "credentials_recorded": False,
        }

    def verify(self, value: Mapping[str, Any]) -> dict[str, Any]:
        envelope, request, result = _validate_verification_request(
            value,
            expected_oracle_id=self.oracle_id,
            expected_public_task_set_sha256=self.public_task_set_sha256,
        )
        verified = verify_n1_score_result(
            package_dir=self._package_dir,
            request=request,
            result=result,
            evidence_secret=self._evidence_secret,
        )
        attestation = _build_attestation(
            envelope=envelope,
            result=result,
            verified=verified,
            public_task_set_sha256=self.public_task_set_sha256,
        )
        assert_hidden_oracle_fields_absent(attestation)
        self._store.consume(
            verification_request_id=envelope["verification_request_id"],
            score_request_sha256=envelope["score_request_sha256"],
            score_result_sha256=envelope["score_result_sha256"],
            attestation_sha256=attestation["attestation_sha256"],
        )
        return attestation


class N1RemoteVerificationHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    service: N1RemoteScoreVerificationService
    bearer_token: str
    max_request_bytes: int


class _N1RemoteVerificationRequestHandler(BaseHTTPRequestHandler):
    server: N1RemoteVerificationHTTPServer
    server_version = "PathfinderN1Verification/0.1"

    def log_message(self, format: str, *args: object) -> None:
        del format, args

    def _write_json(self, status: HTTPStatus, value: Mapping[str, Any]) -> None:
        body = _canonical_bytes(value)
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(body)

    def _error(self, status: HTTPStatus, code: str, message: str) -> None:
        self._write_json(
            status,
            {
                "schema_version": N1_REMOTE_VERIFICATION_API_VERSION,
                "status": "error",
                "error": {"code": code, "message": message},
                "hidden_answer_returned": False,
                "credentials_recorded": False,
            },
        )

    def _authorized(self) -> bool:
        supplied = self.headers.get("Authorization")
        return isinstance(supplied, str) and hmac.compare_digest(
            supplied,
            f"Bearer {self.server.bearer_token}",
        )

    def do_GET(self) -> None:
        parsed = urlsplit(self.path)
        if parsed.path == "/healthz" and not parsed.query:
            self._write_json(HTTPStatus.OK, self.server.service.health())
            return
        self._error(HTTPStatus.NOT_FOUND, "not_found", "unknown endpoint")

    def do_POST(self) -> None:
        try:
            parsed = urlsplit(self.path)
            if (
                parsed.path != "/v1/oracle/verify-score"
                or bool(parsed.query)
            ):
                self._error(
                    HTTPStatus.NOT_FOUND,
                    "not_found",
                    "unknown endpoint",
                )
                return
            if not self._authorized():
                self._error(
                    HTTPStatus.UNAUTHORIZED,
                    "unauthorized",
                    "invalid bearer token",
                )
                return
            _require(
                self.headers.get("X-Pathfinder-Oracle-Verification-Version")
                == N1_REMOTE_VERIFICATION_API_VERSION,
                "missing or unsupported verification protocol header",
            )
            _require(
                self.headers.get("Transfer-Encoding") is None,
                "Transfer-Encoding is unsupported",
            )
            media_type = (
                self.headers.get("Content-Type", "")
                .partition(";")[0]
                .strip()
                .casefold()
            )
            _require(
                media_type == "application/json",
                "Content-Type must be application/json",
            )
            raw_length = self.headers.get("Content-Length")
            _require(
                isinstance(raw_length, str) and raw_length.isdecimal(),
                "Content-Length is required",
            )
            length = int(raw_length)
            _require(
                0 < length <= self.server.max_request_bytes,
                "verification request exceeds its byte limit",
            )
            raw = self.rfile.read(length)
            _require(len(raw) == length, "verification request is truncated")
            envelope = _json_object(raw, "verification request")
            _require(
                self.headers.get("Idempotency-Key")
                == envelope.get("verification_request_id"),
                "Idempotency-Key must equal verification_request_id",
            )
            self._write_json(
                HTTPStatus.OK,
                self.server.service.verify(envelope),
            )
        except N1RemoteVerificationReplayError:
            self._error(
                HTTPStatus.CONFLICT,
                "replay_rejected",
                "verification challenge was already consumed",
            )
        except N1RemoteVerificationCapacityError:
            self._error(
                HTTPStatus.SERVICE_UNAVAILABLE,
                "capacity_exhausted",
                "verification service is at capacity",
            )
        except (N1OracleError, N1RemoteVerificationError):
            self._error(
                HTTPStatus.BAD_REQUEST,
                "invalid_evidence",
                "score evidence verification failed",
            )
        except Exception:
            self._error(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                "internal_error",
                "score evidence verification failed",
            )


def create_n1_remote_verification_http_server(
    package_dir: str | Path,
    *,
    state_db: str | Path,
    bearer_token: str,
    evidence_secret: bytes,
    host: str = "127.0.0.1",
    port: int = 0,
    max_request_bytes: int = _DEFAULT_MAX_REQUEST_BYTES,
    max_verifications: int = _DEFAULT_MAX_VERIFICATIONS,
) -> N1RemoteVerificationHTTPServer:
    """Create, but do not start, the N1 verification companion service."""

    token = _text(bearer_token, "bearer_token", maximum=4096)
    _require("\r" not in token and "\n" not in token, "bearer_token is invalid")
    _require(
        type(port) is int and 0 <= port <= 65535,
        "verification port is invalid",
    )
    _require(
        type(max_request_bytes) is int and max_request_bytes > 0,
        "max_request_bytes must be positive",
    )
    service = N1RemoteScoreVerificationService(
        package_dir,
        state_db=state_db,
        evidence_secret=evidence_secret,
        max_verifications=max_verifications,
    )
    server = N1RemoteVerificationHTTPServer(
        (host, port),
        _N1RemoteVerificationRequestHandler,
    )
    server.service = service
    server.bearer_token = token
    server.max_request_bytes = max_request_bytes
    return server


class _RejectRedirects(HTTPRedirectHandler):
    def redirect_request(
        self,
        req: Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        del req, fp, code, msg, headers, newurl
        return None


@dataclass(frozen=True)
class N1RemoteScoreEvidenceVerifier:
    """N7/N8 verifier adapter requiring no hidden package or HMAC secret.

    Instances implement the ``N1ScoreEvidenceVerifier`` structural protocol
    consumed by ``VerifiedN1HTTPScoringAdapter``.
    """

    base_url: str
    expected_oracle_id: str
    expected_public_task_set_sha256: str
    bearer_token: str = field(repr=False)
    timeout_seconds: float = 10.0
    simulator_private_http_hosts: tuple[str, ...] = ()
    verification_request_id_factory: Callable[[], str] = field(
        default=_new_verification_request_id,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        parsed = urlsplit(self.base_url)
        _require(
            parsed.scheme in {"http", "https"},
            "verification base_url scheme is invalid",
        )
        _require(
            bool(parsed.hostname)
            and parsed.username is None
            and parsed.password is None
            and not parsed.query
            and not parsed.fragment,
            "verification base_url is invalid",
        )
        _require(
            parsed.path in {"", "/"},
            "verification base_url must not contain a path",
        )
        try:
            parsed.port
        except ValueError as exc:
            raise N1RemoteVerificationError(
                "verification base_url port is invalid"
            ) from exc
        allowed = tuple(sorted(set(self.simulator_private_http_hosts)))
        for hostname in allowed:
            _require(
                _SIMULATOR_HOST.fullmatch(hostname) is not None,
                "simulator host is invalid",
            )
        _require(
            parsed.scheme == "https"
            or _loopback(parsed.hostname)
            or parsed.hostname in allowed,
            "plain HTTP is allowed only for loopback or an explicit simulator host",
        )
        _identifier(self.expected_oracle_id, "expected_oracle_id")
        _digest(
            self.expected_public_task_set_sha256,
            "expected_public_task_set_sha256",
        )
        token = _text(self.bearer_token, "bearer_token", maximum=4096)
        _require("\r" not in token and "\n" not in token, "bearer_token is invalid")
        _require(
            type(self.timeout_seconds) in {int, float}
            and float(self.timeout_seconds) > 0.0,
            "timeout_seconds must be positive",
        )
        _require(
            callable(self.verification_request_id_factory),
            "verification_request_id_factory must be callable",
        )
        object.__setattr__(self, "base_url", self.base_url.rstrip("/"))
        object.__setattr__(self, "simulator_private_http_hosts", allowed)

    def _opener(self) -> Any:
        parsed = urlsplit(self.base_url)
        handlers: list[Any] = [_RejectRedirects()]
        if (
            _loopback(parsed.hostname)
            or parsed.hostname in self.simulator_private_http_hosts
        ):
            handlers.insert(0, ProxyHandler({}))
        return build_opener(*handlers)

    def _request(
        self,
        path: str,
        *,
        method: str,
        value: Mapping[str, Any] | None = None,
        verification_request_id: str | None = None,
    ) -> dict[str, Any]:
        headers: dict[str, str] = {"Accept": "application/json"}
        payload: bytes | None = None
        if value is not None:
            payload = _canonical_bytes(value)
            _require(
                len(payload) <= _DEFAULT_MAX_REQUEST_BYTES,
                "verification request exceeds the client byte limit",
            )
            headers.update(
                {
                    "Authorization": f"Bearer {self.bearer_token}",
                    "Content-Type": "application/json",
                    "X-Pathfinder-Oracle-Verification-Version": (
                        N1_REMOTE_VERIFICATION_API_VERSION
                    ),
                }
            )
            if verification_request_id is not None:
                headers["Idempotency-Key"] = verification_request_id
        request = Request(
            f"{self.base_url}{path}",
            data=payload,
            headers=headers,
            method=method,
        )
        try:
            with self._opener().open(
                request,
                timeout=float(self.timeout_seconds),
            ) as response:
                _require(
                    response.status == HTTPStatus.OK,
                    "verification service returned a non-success status",
                )
                _require(
                    response.headers.get_content_type() == "application/json",
                    "verification response Content-Type is invalid",
                )
                _require(
                    response.headers.get("Cache-Control") == "no-store",
                    "verification response may be cached",
                )
                raw = response.read(_DEFAULT_MAX_RESPONSE_BYTES + 1)
                _require(
                    len(raw) <= _DEFAULT_MAX_RESPONSE_BYTES,
                    "verification response is too large",
                )
        except HTTPError as exc:
            raise N1RemoteVerificationHTTPError(
                f"N1 verification returned HTTP {exc.code}"
            ) from exc
        except URLError as exc:
            raise N1RemoteVerificationHTTPError(
                "N1 verification request failed"
            ) from exc
        return _json_object(raw, "verification HTTP response")

    def health(self) -> dict[str, Any]:
        value = self._request("/healthz", method="GET")
        _require(set(value) == _HEALTH_FIELDS, "verification health fields changed")
        _require(
            value.get("schema_version") == N1_REMOTE_VERIFICATION_API_VERSION
            and value.get("status") == "ok"
            and value.get("node_id") == N1_NODE_ID,
            "verification service identity is invalid",
        )
        _require(
            value.get("oracle_id") == self.expected_oracle_id,
            "verification health oracle_id mismatch",
        )
        _require(
            value.get("public_task_set_sha256")
            == self.expected_public_task_set_sha256,
            "verification health public task set mismatch",
        )
        _require(
            value.get("verification_mode")
            == "n1-local-hidden-package-and-hmac"
            and value.get("durable_replay_rejection") is True
            and value.get("hidden_answer_returned") is False
            and value.get("credentials_recorded") is False,
            "verification health reports unsafe state",
        )
        assert_hidden_oracle_fields_absent(value)
        return value

    def verify(
        self,
        *,
        request: Mapping[str, Any],
        result: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        request_value = _mapping_copy(request, "score request")
        result_value = _mapping_copy(result, "score result")
        assert_hidden_oracle_fields_absent(request_value)
        assert_hidden_oracle_fields_absent(result_value)
        _require(
            request_value.get("oracle_id") == self.expected_oracle_id
            and result_value.get("oracle_id") == self.expected_oracle_id,
            "score evidence oracle identity mismatch",
        )
        _require(
            result_value.get("public_task_set_sha256")
            == self.expected_public_task_set_sha256,
            "score evidence public task identity mismatch",
        )
        verification_request_id = _digest(
            self.verification_request_id_factory(),
            "verification_request_id",
        )
        envelope = {
            "schema_version": N1_REMOTE_VERIFICATION_REQUEST_SCHEMA_VERSION,
            "verification_request_id": verification_request_id,
            "oracle_id": self.expected_oracle_id,
            "public_task_set_sha256": self.expected_public_task_set_sha256,
            "score_request_sha256": _sha256_value(request_value),
            "score_result_sha256": _sha256_value(result_value),
            "score_request": request_value,
            "score_result": result_value,
            "credentials_recorded": False,
        }
        before = self.health()
        attestation = self._request(
            "/v1/oracle/verify-score",
            method="POST",
            value=envelope,
            verification_request_id=verification_request_id,
        )
        checked = _validate_attestation(
            attestation,
            envelope=envelope,
            request=request_value,
            result=result_value,
        )
        after = self.health()
        _require(before == after, "verification service identity changed")
        return checked


def _validate_attestation(
    value: Any,
    *,
    envelope: Mapping[str, Any],
    request: Mapping[str, Any],
    result: Mapping[str, Any],
) -> dict[str, Any]:
    attestation = _mapping_copy(value, "verification attestation")
    _require(
        set(attestation) == _ATTESTATION_FIELDS,
        "verification attestation fields changed",
    )
    _require(
        attestation.get("schema_version")
        == N1_REMOTE_VERIFICATION_ATTESTATION_SCHEMA_VERSION
        and attestation.get("status") == "VERIFIED"
        and attestation.get("node_id") == N1_NODE_ID,
        "verification attestation identity is invalid",
    )
    for name in (
        "verification_request_id",
        "oracle_id",
        "public_task_set_sha256",
        "score_request_sha256",
        "score_result_sha256",
    ):
        _require(
            attestation.get(name) == envelope.get(name),
            f"verification attestation {name} mismatch",
        )
    for name in (
        "score_request_id",
        "evaluation_unit_id",
        "run_id",
        "trial_id",
    ):
        _require(
            attestation.get(name) == request.get(name)
            and attestation.get(name) == result.get(name),
            f"verification attestation {name} mismatch",
        )
    for name in (
        "request_sha256",
        "prediction_sha256",
        "result_content_sha256",
        "score_evidence_hmac_sha256",
    ):
        _require(
            attestation.get(name) == result.get(name),
            f"verification attestation {name} mismatch",
        )
        _digest(attestation.get(name), name)
    _require(
        type(attestation.get("correct")) is bool
        and attestation.get("correct") is result.get("correct")
        and attestation.get("score") == result.get("score")
        and attestation.get("score")
        == (1.0 if attestation["correct"] else 0.0),
        "verification attestation score mismatch",
    )
    _require(
        attestation.get("replay_detected") is False
        and attestation.get("hidden_answer_returned") is False
        and attestation.get("credentials_recorded") is False
        and attestation.get("eligible_for_scientific_claims") is False,
        "verification attestation reports unsafe provenance",
    )
    supplied = _digest(
        attestation.get("attestation_sha256"),
        "attestation_sha256",
    )
    core = dict(attestation)
    del core["attestation_sha256"]
    _require(
        hmac.compare_digest(supplied, _attestation_digest(core)),
        "verification attestation content digest mismatch",
    )
    assert_hidden_oracle_fields_absent(attestation)
    return attestation


__all__ = [
    "N1_REMOTE_VERIFICATION_API_VERSION",
    "N1_REMOTE_VERIFICATION_ATTESTATION_SCHEMA_VERSION",
    "N1_REMOTE_VERIFICATION_REQUEST_SCHEMA_VERSION",
    "N1_REMOTE_VERIFICATION_STORE_SCHEMA_VERSION",
    "N1RemoteScoreEvidenceVerifier",
    "N1RemoteScoreVerificationService",
    "N1RemoteVerificationCapacityError",
    "N1RemoteVerificationError",
    "N1RemoteVerificationHTTPError",
    "N1RemoteVerificationHTTPServer",
    "N1RemoteVerificationReplayError",
    "create_n1_remote_verification_http_server",
]
