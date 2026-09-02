"""Frozen task-scoring contracts for distributed Pathfinder pilots.

The development seed used a permissive accepted-substring compatibility
rule.  A public benchmark needs a deterministic primary score, so new pilots
may instead declare an option-ID exact-match rule.  Both rules live here so
execution and offline evaluation cannot drift.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping


ACCEPTED_SUBSTRING_SCORING_RULE = "accepted-answer-substring-match"
MULTIPLE_CHOICE_EXACT_SCORING_RULE = (
    "multiple-choice-option-id-exact-match-v1"
)
SUCCESS_SCORING_RULES = (
    ACCEPTED_SUBSTRING_SCORING_RULE,
    MULTIPLE_CHOICE_EXACT_SCORING_RULE,
)

_OPTION_ID = re.compile(r"[A-Z][A-Z0-9_-]{0,15}\Z")


class WorkloadScoringError(ValueError):
    """Raised when a workload cannot be scored by its frozen rule."""


@dataclass(frozen=True)
class AnswerOption:
    option_id: str
    text: str

    def to_public_dict(self) -> dict[str, str]:
        return {"option_id": self.option_id, "text": self.text}


@dataclass(frozen=True)
class WorkloadScoringContract:
    rule: str
    accepted_answer_substrings: tuple[str, ...] = ()
    answer_options: tuple[AnswerOption, ...] = ()
    correct_answer_id: str | None = None

    def record_fields(self) -> dict[str, Any]:
        """Labels copied into each canonical record for later auditing."""
        payload: dict[str, Any] = {"success_scoring_rule": self.rule}
        if self.rule == ACCEPTED_SUBSTRING_SCORING_RULE:
            payload["accepted_answer_substrings"] = list(
                self.accepted_answer_substrings
            )
        else:
            payload["answer_options"] = [
                option.to_public_dict() for option in self.answer_options
            ]
            payload["correct_answer_id"] = self.correct_answer_id
        return payload


def _nonempty_string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise WorkloadScoringError(f"{name} must be a non-empty string")
    return value.strip()


def load_workload_scoring_contract(
    workload: Mapping[str, Any],
    rule: str,
    *,
    name: str = "workload",
) -> WorkloadScoringContract:
    """Validate and normalize the labels required by ``rule``.

    Exact-match options are deliberately represented as an ordered array of
    ``{option_id, text}`` objects.  IDs must be short uppercase ASCII tokens;
    this prevents punctuation, prose extraction, or case folding from
    quietly becoming part of the leaderboard rule.
    """
    if rule not in SUCCESS_SCORING_RULES:
        raise WorkloadScoringError(f"unsupported success_scoring_rule: {rule}")
    _nonempty_string(workload.get("object_id"), f"{name}.object_id")
    _nonempty_string(workload.get("question"), f"{name}.question")

    if rule == ACCEPTED_SUBSTRING_SCORING_RULE:
        raw = workload.get("accepted_answer_substrings")
        if not isinstance(raw, list):
            raise WorkloadScoringError(
                f"{name}.accepted_answer_substrings must be an array"
            )
        answers = tuple(
            _nonempty_string(value, f"{name}.accepted_answer_substrings[{index}]")
            for index, value in enumerate(raw)
        )
        if len(set(answers)) != len(answers):
            raise WorkloadScoringError(
                f"{name}.accepted_answer_substrings contains duplicates"
            )
        return WorkloadScoringContract(
            rule=rule,
            accepted_answer_substrings=answers,
        )

    raw_options = workload.get("answer_options")
    if not isinstance(raw_options, list) or len(raw_options) < 2:
        raise WorkloadScoringError(
            f"{name}.answer_options must contain at least two ordered options"
        )
    options: list[AnswerOption] = []
    for index, raw in enumerate(raw_options):
        if not isinstance(raw, Mapping):
            raise WorkloadScoringError(
                f"{name}.answer_options[{index}] must be an object"
            )
        option_id = _nonempty_string(
            raw.get("option_id"),
            f"{name}.answer_options[{index}].option_id",
        )
        if _OPTION_ID.fullmatch(option_id) is None:
            raise WorkloadScoringError(
                f"{name}.answer_options[{index}].option_id must match "
                "[A-Z][A-Z0-9_-]{0,15}"
            )
        text = _nonempty_string(
            raw.get("text"),
            f"{name}.answer_options[{index}].text",
        )
        options.append(AnswerOption(option_id=option_id, text=text))
    ids = [option.option_id for option in options]
    if len(set(ids)) != len(ids):
        raise WorkloadScoringError(f"{name}.answer_options has duplicate IDs")
    correct = _nonempty_string(
        workload.get("correct_answer_id"),
        f"{name}.correct_answer_id",
    )
    if correct not in ids:
        raise WorkloadScoringError(
            f"{name}.correct_answer_id does not name a declared option"
        )
    legacy = workload.get("accepted_answer_substrings")
    if legacy not in (None, []):
        raise WorkloadScoringError(
            f"{name} mixes exact-match labels with "
            "accepted_answer_substrings"
        )
    return WorkloadScoringContract(
        rule=rule,
        answer_options=tuple(options),
        correct_answer_id=correct,
    )


def validate_workload_manifest(
    workloads: Mapping[str, Mapping[str, Any]],
    workload_ids: tuple[str, ...],
    rule: str,
) -> dict[str, WorkloadScoringContract]:
    """Validate exactly the planned workloads and their independent units."""
    missing = sorted(set(workload_ids) - set(workloads))
    if missing:
        raise WorkloadScoringError(
            "no workload definition for: " + ", ".join(missing)
        )
    contracts: dict[str, WorkloadScoringContract] = {}
    objects: dict[str, str] = {}
    for workload_id in workload_ids:
        workload = workloads[workload_id]
        if not isinstance(workload, Mapping):
            raise WorkloadScoringError(
                f"workloads[{workload_id!r}] must be an object"
            )
        contract = load_workload_scoring_contract(
            workload,
            rule,
            name=f"workloads[{workload_id!r}]",
        )
        if rule == MULTIPLE_CHOICE_EXACT_SCORING_RULE:
            object_id = str(workload["object_id"]).strip()
            previous = objects.get(object_id)
            if previous is not None:
                raise WorkloadScoringError(
                    f"workloads {previous!r} and {workload_id!r} share "
                    f"object_id {object_id!r}; independent-unit count would "
                    "be inflated"
                )
            objects[object_id] = workload_id
        contracts[workload_id] = contract
    return contracts


def evaluate_workload_answer(
    answer: str,
    contract: WorkloadScoringContract,
) -> bool | None:
    """Apply the frozen rule without heuristic answer extraction."""
    if contract.rule == ACCEPTED_SUBSTRING_SCORING_RULE:
        if not contract.accepted_answer_substrings:
            return None
        normalized = " ".join(answer.casefold().split())
        return any(
            " ".join(candidate.casefold().split()) in normalized
            for candidate in contract.accepted_answer_substrings
        )
    # Leading/trailing whitespace is transport formatting.  Everything else,
    # including case, punctuation, and explanatory prose, makes the response
    # invalid and therefore incorrect.
    return answer.strip() == contract.correct_answer_id


def render_workload_question(
    workload: Mapping[str, Any],
    contract: WorkloadScoringContract,
) -> str:
    """Render the deterministic Agent-facing question for a scoring rule."""
    question = str(workload["question"]).strip()
    if contract.rule == ACCEPTED_SUBSTRING_SCORING_RULE:
        return question
    options = "\n".join(
        f"[{option.option_id}] {option.text}"
        for option in contract.answer_options
    )
    return (
        f"{question}\n\nOptions:\n{options}\n\n"
        "Return exactly one option ID and no other text."
    )
