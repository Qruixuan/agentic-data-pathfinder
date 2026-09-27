"""Offline-verified route offers and one-choice handoff for PPD experiments.

This module never contacts FlowMesh, Data Agents, N6, or N1. The caller must
verify the runtime admission and all live pre-submit gates before executing a
handoff with ``FlowMeshSemanticTrialExecutor``. Historical schedules are
useful test fixtures, not fresh run identities.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from contextlib import closing
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import json
from pathlib import Path
import re
import sqlite3
from typing import Any, Mapping

from ...rsi_exam.ten_route_multiq_plan import (
    SCHEMA, SCHEDULE, load_verified_multiq_plan,
    observations_for_schema, ten_route_trial_key,
)


_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
_FAMILIES = {
    "R": "raw",
    "I": "indexed-raw",
    "D": "remote-derived",
    "DC": "local-cache-derived",
}
_DESIGNS = {
    "D_base": frozenset({"R", "D"}),
    "D_index": frozenset({"R", "D", "I"}),
    "D_cache": frozenset({"R", "D", "DC"}),
    "D_joint": frozenset({"R", "D", "I", "DC"}),
}


class RouteActionBridgeError(ValueError):
    """An action, price, cache observation, or admission is not bound."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise RouteActionBridgeError(message)


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _sha(value: Any) -> str:
    return sha256(_canonical(value)).hexdigest()


@dataclass(frozen=True)
class RouteQuote:
    """A pre-execution estimate, never the realized cost of this query."""

    incremental_usd: str
    expected_latency_ms: int
    price_basis_sha256: str
    source_trace_count: int

    def validate(self) -> None:
        _require(isinstance(self.incremental_usd, str),
                 "route quote must use a decimal string")
        try:
            amount = Decimal(self.incremental_usd)
        except (InvalidOperation, TypeError) as exc:
            raise RouteActionBridgeError("route quote is not decimal") from exc
        _require(amount.is_finite() and amount >= 0,
                 "route quote must be finite and nonnegative")
        _require(type(self.expected_latency_ms) is int
                 and self.expected_latency_ms >= 0,
                 "route latency estimate is invalid")
        _require(type(self.source_trace_count) is int
                 and self.source_trace_count > 0,
                 "route quote lacks measured source traces")
        _require(isinstance(self.price_basis_sha256, str)
                 and _DIGEST.fullmatch(self.price_basis_sha256),
                 "route quote lacks a frozen price basis")


@dataclass(frozen=True)
class CacheObservation:
    """Read-only cache observation supplied by a separate trusted verifier."""

    cache_episode_id: str
    state: str
    evidence_sha256: str

    def validate(self) -> None:
        _require(self.state in {"miss", "hit"},
                 "cache state must be miss or hit")
        _require(isinstance(self.cache_episode_id, str)
                 and _ID.fullmatch(self.cache_episode_id),
                 "cache episode identity is invalid")
        _require(isinstance(self.evidence_sha256, str)
                 and _DIGEST.fullmatch(self.evidence_sha256),
                 "cache state lacks bound observation evidence")


@dataclass(frozen=True)
class RouteOffer:
    action_id: str
    arm_id: str
    executor_node_id: str
    cache_state: str | None
    cache_evidence_sha256: str | None
    incremental_usd: str
    expected_latency_ms: int
    price_basis_sha256: str
    source_trace_count: int


@dataclass(frozen=True)
class RouteOfferSet:
    question_id: str
    physical_design_id: str
    execution_namespace: str
    plan_sha256: str
    price_basis_sha256: str
    offers: tuple[RouteOffer, ...]
    offer_set_sha256: str


@dataclass(frozen=True)
class RouteChoice:
    session_id: str
    question_id: str
    physical_design_id: str
    action_id: str
    plan_sha256: str
    price_basis_sha256: str
    trial_key: str
    run_id: str
    cache_state: str | None
    cache_episode_id: str | None
    offer_set_sha256: str
    choice_sha256: str


@dataclass(frozen=True)
class RouteHandoff:
    """Arguments for the existing executor; this object does not submit."""

    run_id: str
    cache_episode_id: str | None
    trial: Mapping[str, Any]
    idempotency_key: str
    choice_sha256: str


class RouteActionBridge:
    """Bind public route choices to one canonical ten-route schedule."""

    def __init__(
        self, plan_dir: str | Path, choice_db: str | Path,
        *, price_basis_sha256: str, execution_namespace: str,
    ) -> None:
        _require(isinstance(price_basis_sha256, str)
                 and _DIGEST.fullmatch(price_basis_sha256),
                 "frozen price basis is missing")
        _require(isinstance(execution_namespace, str)
                 and _ID.fullmatch(execution_namespace)
                 and len(execution_namespace) <= 64,
                 "fresh execution namespace is required")
        root = Path(plan_dir)
        manifest, questions, report = load_verified_multiq_plan(root)
        _require(manifest["schema_version"] == SCHEMA,
                 "route actions require the complete ten-route plan")
        self.plan_sha256 = report["plan_sha256"]
        self.experiment_id = manifest["experiment_id"]
        self._questions = {row["question_id"]: row for row in questions}
        self._schedule = {
            row["question_id"]: row for row in (
                json.loads(line) for line in (root / SCHEDULE)
                .read_bytes().splitlines()
            )
        }
        _require(len(self._questions) == len(self._schedule),
                 "schedule question identities repeat")
        self._choice_db = Path(choice_db)
        self.price_basis_sha256 = price_basis_sha256
        self.execution_namespace = execution_namespace

    def _episode_id(self, question_id: str, node: str) -> str:
        object_id = self._questions[question_id]["object_id"]
        suffix = _sha({
            "domain": "pathfinder.route-action-cache-episode/v1",
            "execution_namespace": self.execution_namespace,
            "object_id": object_id,
            "node_id": node,
        })[:24]
        return f"pf-episode-{self.execution_namespace}-{suffix}"

    def _run_id(self, session_id: str, action_id: str) -> str:
        suffix = _sha({
            "domain": "pathfinder.route-action-run/v1",
            "execution_namespace": self.execution_namespace,
            "session_id": session_id,
            "action_id": action_id,
        })[:24]
        return f"pf-route-{self.execution_namespace}-{suffix}"

    @staticmethod
    def supported_physical_designs() -> tuple[str, ...]:
        return tuple(_DESIGNS)

    def public_question_identity(self, question_id: str) -> tuple[str, str]:
        """Return only the public object and task commitment for a question."""
        _require(question_id in self._questions, "question is not in plan")
        row = self._questions[question_id]
        return row["object_id"], row["public_task_sha256"]

    def public_question(self, question_id: str) -> dict[str, Any]:
        """Return only fields from the canonically verified public plan."""
        _require(question_id in self._questions, "question is not in plan")
        row = self._questions[question_id]
        return {
            key: row[key] for key in (
                "question_id", "object_id", "stratum", "question",
                "answer_options", "public_task_sha256",
            )
        }

    def cache_episode_ids(self, question_id: str) -> dict[str, str]:
        """Return fresh per-object episodes, reusable across its questions."""
        self._slots(question_id)
        return {
            node: self._episode_id(question_id, node)
            for node in ("N7", "N8")
        }

    def load_choice(self, session_id: str) -> RouteChoice:
        """Read one previously committed choice without exposing its trial."""
        _require(isinstance(session_id, str) and _ID.fullmatch(session_id),
                 "session identity is invalid")
        _require(self._choice_db.is_file(), "route choice store is absent")
        with closing(sqlite3.connect(self._choice_db)) as connection:
            try:
                row = connection.execute(
                    "SELECT choice_json FROM route_choices WHERE session_id = ?",
                    (session_id,),
                ).fetchone()
            except sqlite3.OperationalError as exc:
                raise RouteActionBridgeError(
                    "route choice store is not initialized"
                ) from exc
        _require(row is not None, "session has no committed route choice")
        choice = RouteChoice(**json.loads(row[0]))
        _require(choice.plan_sha256 == self.plan_sha256
                 and choice.price_basis_sha256 == self.price_basis_sha256
                 and choice.run_id == self._run_id(
                     choice.session_id, choice.action_id,
                 ),
                 "committed choice uses another plan or price basis")
        return choice

    def _slots(self, question_id: str) -> dict[tuple[str, str | None], dict]:
        _require(question_id in self._questions, "question is not in plan")
        row = self._schedule[question_id]
        expected = {
            (design, state): (node, arm, repetition)
            for design, node, arm, repetition, state
            in observations_for_schema(SCHEMA)
        }
        slots = {(s["design_id"], s["cache_expectation"]): s
                 for s in row["route_slots"]}
        _require(len(slots) == len(expected) and set(slots) == set(expected),
                 "route observation coverage changed")
        for key, slot in slots.items():
            node, arm, repetition = expected[key]
            _require((slot["executor_node_id"], slot["arm_id"],
                      slot["repetition"]) == (node, arm, repetition),
                     "route observation differs from canonical action")
        return slots

    def _offers_and_slots(
        self, question_id: str, physical_design_id: str,
        quotes: Mapping[tuple[str, str | None], RouteQuote],
        cache: Mapping[str, CacheObservation],
    ) -> tuple[tuple[RouteOffer, ...], dict[str, dict]]:
        _require(physical_design_id in _DESIGNS,
                 "physical design is unsupported")
        slots = self._slots(question_id)
        allowed = _DESIGNS[physical_design_id]
        offers = []
        chosen_slots = {}
        bases = set()
        for action_number in range(8):
            action_id = f"D{action_number}"
            canonical = slots[(action_id, "miss" if action_id in
                               {"D3", "D7"} else None)]
            arm = canonical["arm_id"]
            if arm not in allowed:
                continue
            state = None
            evidence = None
            if arm == "DC":
                node = canonical["executor_node_id"]
                observation = cache.get(node)
                _require(isinstance(observation, CacheObservation),
                         "cache offer lacks a trusted state observation")
                observation.validate()
                state = observation.state
                selected = slots[(action_id, state)]
                _require(self._episode_id(question_id, node)
                         == observation.cache_episode_id,
                         "cache observation names a different episode")
                evidence = observation.evidence_sha256
            else:
                selected = canonical
            quote = quotes.get((action_id, state))
            _require(isinstance(quote, RouteQuote),
                     "available route has no measured quote")
            quote.validate()
            _require(quote.price_basis_sha256 == self.price_basis_sha256,
                     "route quote uses a stale price basis")
            bases.add(quote.price_basis_sha256)
            offers.append(RouteOffer(
                action_id=action_id, arm_id=arm,
                executor_node_id=selected["executor_node_id"],
                cache_state=state, cache_evidence_sha256=evidence,
                incremental_usd=quote.incremental_usd,
                expected_latency_ms=quote.expected_latency_ms,
                price_basis_sha256=quote.price_basis_sha256,
                source_trace_count=quote.source_trace_count,
            ))
            chosen_slots[action_id] = selected
        _require(len(bases) == 1, "offers use different price bases")
        return tuple(offers), chosen_slots

    def _offer_set(self, question_id: str, physical_design_id: str,
                   offers: tuple[RouteOffer, ...]) -> RouteOfferSet:
        digest = _sha({
            "domain": "pathfinder.route-action-offers/v1",
            "execution_namespace": self.execution_namespace,
            "plan_sha256": self.plan_sha256,
            "question_id": question_id,
            "physical_design_id": physical_design_id,
            "offers": [asdict(offer) for offer in offers],
        })
        return RouteOfferSet(
            question_id=question_id,
            physical_design_id=physical_design_id,
            execution_namespace=self.execution_namespace,
            plan_sha256=self.plan_sha256,
            price_basis_sha256=self.price_basis_sha256,
            offers=offers, offer_set_sha256=digest,
        )

    def list_route_offers(
        self, *, question_id: str, physical_design_id: str,
        quotes: Mapping[tuple[str, str | None], RouteQuote],
        cache: Mapping[str, CacheObservation] | None = None,
    ) -> RouteOfferSet:
        offers, _ = self._offers_and_slots(
            question_id, physical_design_id, quotes, cache or {},
        )
        return self._offer_set(question_id, physical_design_id, offers)

    def commit_choice(
        self, *, session_id: str, question_id: str,
        physical_design_id: str, action_id: str,
        offer_set_sha256: str,
        quotes: Mapping[tuple[str, str | None], RouteQuote],
        cache: Mapping[str, CacheObservation] | None = None,
    ) -> RouteChoice:
        _require(isinstance(session_id, str) and _ID.fullmatch(session_id),
                 "session identity is invalid")
        offers, slots = self._offers_and_slots(
            question_id, physical_design_id, quotes, cache or {},
        )
        _require(action_id in slots, "action is not offered by design")
        slot = slots[action_id]
        trial_key = ten_route_trial_key(
            self.experiment_id, question_id, action_id, slot["repetition"],
        )
        offer_set_sha = self._offer_set(
            question_id, physical_design_id, offers,
        ).offer_set_sha256
        _require(offer_set_sha256 == offer_set_sha,
                 "Agent choice refers to a stale offer set")
        payload = {
            "session_id": session_id,
            "question_id": question_id,
            "physical_design_id": physical_design_id,
            "action_id": action_id,
            "plan_sha256": self.plan_sha256,
            "price_basis_sha256": self.price_basis_sha256,
            "trial_key": trial_key,
            "run_id": self._run_id(session_id, action_id),
            "cache_state": (
                "hit" if slot["arm_id"] == "DC"
                and slot["repetition"] == 1 else
                "miss" if slot["arm_id"] == "DC" else None
            ),
            "cache_episode_id": (
                self._episode_id(question_id, slot["executor_node_id"])
                if slot["arm_id"] == "DC" else None
            ),
            "offer_set_sha256": offer_set_sha,
        }
        choice = RouteChoice(**payload, choice_sha256=_sha({
            "domain": "pathfinder.route-action-choice/v1", **payload,
        }))
        self._choice_db.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(self._choice_db)) as connection:
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "CREATE TABLE IF NOT EXISTS route_choices "
                    "(session_id TEXT PRIMARY KEY, choice_json TEXT NOT NULL)"
                )
                old = connection.execute(
                    "SELECT choice_json FROM route_choices WHERE session_id = ?",
                    (session_id,),
                ).fetchone()
                if old is not None:
                    _require(json.loads(old[0]) == asdict(choice),
                             "session already committed a different choice")
                else:
                    connection.execute(
                        "INSERT INTO route_choices VALUES (?, ?)",
                        (session_id,
                         _canonical(asdict(choice)).decode("utf-8")),
                    )
        return choice

    def handoff(
        self, choice: RouteChoice, bound_trial: Mapping[str, Any],
    ) -> RouteHandoff:
        """Validate admission identity; caller must prove fresh/live gates."""

        _require(isinstance(bound_trial, Mapping),
                 "source-bound trial is absent")
        _require(isinstance(choice, RouteChoice), "route choice is invalid")
        _require(asdict(self.load_choice(choice.session_id)) == asdict(choice),
                 "choice was not durably committed")
        question = self._questions.get(choice.question_id)
        _require(question is not None, "choice question is not in plan")
        slot = self._slots(choice.question_id)[(
            choice.action_id, choice.cache_state,
        )]
        _require(
            choice.plan_sha256 == self.plan_sha256
            and choice.price_basis_sha256 == self.price_basis_sha256
            and choice.run_id == self._run_id(
                choice.session_id, choice.action_id,
            )
            and choice.cache_episode_id == (
                self._episode_id(
                    choice.question_id, slot["executor_node_id"],
                ) if slot["arm_id"] == "DC" else None
            )
            and choice.cache_state == slot["cache_expectation"]
            and choice.trial_key == ten_route_trial_key(
                self.experiment_id, choice.question_id,
                choice.action_id, slot["repetition"],
            )
            and choice.choice_sha256 == _sha({
                "domain": "pathfinder.route-action-choice/v1",
                **{key: value for key, value in asdict(choice).items()
                   if key != "choice_sha256"},
            }),
            "choice identity differs from the frozen slot",
        )
        _require(
            bound_trial.get("flowmesh_submission_authorized") is True
            and bound_trial.get("trial_key") == choice.trial_key
            and bound_trial.get("design_id") == choice.action_id
            and bound_trial.get("repetition") == slot["repetition"]
            and bound_trial.get("executor_node_id")
            == slot["executor_node_id"]
            and bound_trial.get("route_family")
            == _FAMILIES[slot["arm_id"]]
            and bound_trial.get("artifact_object_id")
            == question["object_id"]
            and bound_trial.get("public_task_binding_sha256")
            == question["public_task_sha256"]
            and bound_trial.get("route_coordinator_binding", {}).get(
                "service_contract_id"
            ) == f'{slot["executor_node_id"]}.execution-compute',
            "admitted trial differs from the chosen route",
        )
        return RouteHandoff(
            run_id=choice.run_id,
            cache_episode_id=choice.cache_episode_id,
            trial=bound_trial,
            idempotency_key=_sha({
                "domain": "pathfinder.route-action-dispatch/v1",
                "choice_sha256": choice.choice_sha256,
                "trial_key": choice.trial_key,
                "run_id": choice.run_id,
            }),
            choice_sha256=choice.choice_sha256,
        )
