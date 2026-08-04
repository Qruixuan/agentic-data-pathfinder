from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path
from statistics import mean
from typing import Iterable

from .models import SessionObservation


class JsonlTelemetryStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)

    def reset(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text("", encoding="utf-8")

    def append(self, observation: SessionObservation) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(
                json.dumps(
                    observation.to_dict(),
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            handle.write("\n")


def summarize_observations(
    observations: Iterable[SessionObservation],
    representation_ids: Iterable[str],
) -> list[dict[str, object]]:
    groups: dict[
        tuple[str, str, str, float],
        list[SessionObservation],
    ] = defaultdict(list)
    for observation in observations:
        key = (
            observation.design_id,
            observation.task_class_id,
            observation.quote_profile_id,
            observation.latency_multiplier,
        )
        groups[key].append(observation)

    rows: list[dict[str, object]] = []
    for key in sorted(groups):
        design_id, task_id, profile_id, latency_multiplier = key
        sessions = groups[key]
        for rep_id in representation_ids:
            offered = [
                offer
                for session in sessions
                for offer in session.offers
                if offer.representation_id == rep_id
            ]
            selected = [
                session
                for session in sessions
                if session.selected_representation_id == rep_id
            ]
            rows.append(
                {
                    "design_id": design_id,
                    "task_class_id": task_id,
                    "quote_profile_id": profile_id,
                    "latency_multiplier": latency_multiplier,
                    "representation_id": rep_id,
                    "sessions": len(sessions),
                    "offered_count": len(offered),
                    "affordable_count": sum(
                        1 for offer in offered if offer.affordable
                    ),
                    "mean_quoted_price": (
                        round(mean(offer.quoted_price for offer in offered), 6)
                        if offered
                        else ""
                    ),
                    "access_count": len(selected),
                    "access_rate": round(len(selected) / len(sessions), 6),
                    "terminal_success_rate": round(
                        sum(1 for session in sessions if session.terminal_success)
                        / len(sessions),
                        6,
                    ),
                    "mean_felt_latency_ms_when_selected": (
                        round(
                            mean(
                                session.felt_latency_ms
                                for session in selected
                                if session.felt_latency_ms is not None
                            ),
                            6,
                        )
                        if selected
                        else ""
                    ),
                    "mean_realized_cost_when_selected": (
                        round(mean(session.realized_cost for session in selected), 6)
                        if selected
                        else ""
                    ),
                    "mean_session_value": round(
                        mean(session.session_value for session in sessions),
                        6,
                    ),
                }
            )
    return rows


def write_summary_csv(rows: list[dict[str, object]], path: str | Path) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        destination.write_text("", encoding="utf-8")
        return
    with destination.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def write_manifest(data: dict[str, object], path: str | Path) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(data, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
