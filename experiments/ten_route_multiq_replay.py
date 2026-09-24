"""Fail-closed offline episode replay for measured ten-route multiq rows.

Collection and source-bound verification remain separate. This module only
selects exact measured outcomes compatible with a policy's current finite
cache state; it cannot invent a route, model answer, latency, or bill.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Callable, Mapping, Sequence
from decimal import Decimal, InvalidOperation
from typing import Any

from pathfinder.rsi_exam.cache_episode_state import ReplayCacheState


REPRESENTATIONS = ("multimodal_digest", "sampled_frame_bundle")
ARMS = ("R", "I", "D", "DC")
NODES = ("N7", "N8")
_BUILD = {"R": ("raw",), "I": ("raw", "index"),
          "D": ("raw", "derived"), "DC": ("raw", "derived")}


def _money(value: object, name: str) -> Decimal | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a decimal USD string or null")
    try:
        amount = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"{name} is not decimal USD") from exc
    if not amount.is_finite() or amount < 0:
        raise ValueError(f"{name} is not nonnegative finite USD")
    return amount


def _usd(value: Decimal) -> str:
    return format(value.quantize(Decimal("0.000000001")), "f")


class TenRouteEpisodeReplay:
    """One ordered policy episode, with separate N7/N8 LRU caches."""

    def __init__(
        self, *, schedule: Sequence[Mapping[str, Any]],
        observations: Sequence[Mapping[str, Any]],
        artifact_by_object: Mapping[str, Mapping[str, Mapping[str, Any]]],
        build_cost_by_object: Mapping[str, Mapping[str, str | None]],
        capacity_by_node: Mapping[str, int], namespace: str,
    ) -> None:
        if set(capacity_by_node) != set(NODES):
            raise ValueError("both node capacities are required")
        self._cache = {node: ReplayCacheState(
            node_id=node, capacity_bytes=capacity_by_node[node],
        ) for node in NODES}
        self._schedule = list(schedule)
        if len(self._schedule) != len({row["question_id"]
                                       for row in self._schedule}):
            raise ValueError("schedule question IDs repeat")
        if [row["ordinal"] for row in self._schedule] != list(
            range(len(self._schedule))
        ):
            raise ValueError("schedule ordinals are not contiguous")
        self._outcomes: dict[tuple[str, str], list[Mapping[str, Any]]] = (
            defaultdict(list)
        )
        ids = set()
        for row in observations:
            action = row["action_id"]
            if action not in {f"{node}/{arm}" for node in NODES
                              for arm in ARMS}:
                raise ValueError("measured action is outside the eight actions")
            if row["outcome_id"] in ids:
                raise ValueError("measured outcome IDs repeat")
            ids.add(row["outcome_id"])
            self._outcomes[row["question_id"], action].append(row)
        self._artifacts = artifact_by_object
        self._build_costs = build_cost_by_object
        self._namespace = namespace
        self._built: set[tuple[str, str]] = set()
        self._seen: Counter[str] = Counter()
        self._position = 0
        self._known_total = Decimal(0)
        self._unknown: set[str] = set()

    @property
    def has_next(self) -> bool:
        return self._position < len(self._schedule)

    def observation(self) -> dict[str, Any]:
        if not self.has_next:
            raise ValueError("episode is complete")
        question = self._schedule[self._position]
        object_id = question["object_id"]
        artifacts = self._artifacts.get(object_id)
        if artifacts is None or set(artifacts) != set(REPRESENTATIONS):
            raise ValueError("public cacheable artifact identity is absent")
        return {
            "question_id": question["question_id"],
            "object_id": object_id,
            "stratum": question["stratum"],
            "prior_queries_for_video": self._seen[object_id],
            "cacheable_sizes": {
                rep: artifacts[rep]["size_bytes"]
                for rep in REPRESENTATIONS
            },
            "built_components": sorted(
                component for video, component in self._built
                if video == object_id
            ),
            "cache_by_node": {node: cache.snapshot()
                              for node, cache in self._cache.items()},
            "available_actions": [f"{node}/{arm}" for node in NODES
                                  for arm in ARMS],
            # No future question IDs, future video IDs, hidden labels,
            # outcomes or remaining-query count appear in the observation.
        }

    def _simulate_cache_events(
        self, *, candidate: Mapping[str, Any], object_id: str,
        branch: ReplayCacheState,
    ) -> bool:
        events = candidate.get("cache_events")
        if not isinstance(events, list):
            return False
        artifacts = self._artifacts[object_id]
        lookups: dict[str, bool] = {}
        stores: set[str] = set()
        for event in events:
            if not isinstance(event, dict):
                return False
            rep = event.get("representation_id")
            if rep not in REPRESENTATIONS:
                return False
            artifact = artifacts[rep]
            if event.get("kind") == "lookup" and rep not in lookups:
                observed = event.get("hit")
                if type(observed) is not bool:
                    return False
                actual = branch.lookup(
                    namespace=self._namespace, object_id=object_id,
                    representation_id=rep,
                    expected_sha256=artifact["sha256"],
                )
                if actual is not observed:
                    return False
                lookups[rep] = actual
            elif (event.get("kind") == "store" and rep in lookups
                  and not lookups[rep] and rep not in stores):
                evicted = branch.put(
                    namespace=self._namespace, object_id=object_id,
                    representation_id=rep,
                    content_sha256=artifact["sha256"],
                    size_bytes=artifact["size_bytes"],
                )
                actual = [(row.object_id, row.representation_id)
                          for row in evicted]
                if event.get("evicted") != [list(row) for row in actual]:
                    return False
                stores.add(rep)
            else:
                return False
        return set(lookups) == set(REPRESENTATIONS) and stores == {
            rep for rep, hit in lookups.items() if not hit
        }

    def step(self, action_id: str) -> dict[str, Any]:
        public = self.observation()
        if action_id not in public["available_actions"]:
            return {"status": "UNSUPPORTED_ACTION", "state_changed": False,
                    "reason": "action is outside the frozen action space"}
        node, arm = action_id.split("/")
        question_id, object_id = public["question_id"], public["object_id"]
        matched = []
        for candidate in self._outcomes.get((question_id, action_id), []):
            if candidate.get("object_id") != object_id:
                continue
            branch = self._cache[node].fork()
            if arm == "DC":
                if not self._simulate_cache_events(
                    candidate=candidate, object_id=object_id, branch=branch,
                ):
                    continue
            elif candidate.get("cache_events") != []:
                continue
            matched.append((candidate, branch))
        if len(matched) != 1:
            return {"status": "UNSUPPORTED_ACTION", "state_changed": False,
                    "reason": ("no exact measured state" if not matched
                               else "measured state is ambiguous")}
        candidate, branch = matched[0]
        success = candidate.get("task_success")
        if success is not None and type(success) is not bool:
            raise ValueError("measured task outcome is invalid")
        route_cost = _money(candidate.get("route_cost_usd"),
                            "measured route cost")
        build_rows = self._build_costs.get(object_id)
        if build_rows is None:
            raise ValueError("video build-cost record is absent")
        newly_built = [component for component in _BUILD[arm]
                       if (object_id, component) not in self._built]
        known_step = route_cost or Decimal(0)
        unknown = []
        if route_cost is None:
            unknown.append(f"route/{candidate['outcome_id']}")
        for component in newly_built:
            charge = _money(build_rows.get(component),
                            f"build/{object_id}/{component}")
            if charge is None:
                unknown.append(f"build/{object_id}/{component}")
            else:
                known_step += charge
        self._cache[node] = branch
        self._built.update((object_id, name) for name in newly_built)
        self._seen[object_id] += 1
        self._position += 1
        self._known_total += known_step
        self._unknown.update(unknown)
        return {
            "status": "REPLAYED",
            "question_id": question_id,
            "object_id": object_id,
            "action_id": action_id,
            "outcome_id": candidate["outcome_id"],
            "task_success": success,
            "elapsed_ms": candidate.get("elapsed_ms"),
            "newly_built_components": newly_built,
            "known_step_cost_usd": _usd(known_step),
            "complete_step_cost_usd": (
                _usd(known_step) if not unknown else None
            ),
            "missing_cost_fields": unknown,
            "cache_after": branch.snapshot() if arm == "DC" else None,
        }

    def result(self, steps: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        complete = self._position == len(self._schedule) and all(
            step["status"] == "REPLAYED" for step in steps
        )
        return {
            "status": "COMPLETE" if complete else "UNSUPPORTED_ACTION",
            "question_count": self._position,
            "known_list_cost_usd": _usd(self._known_total),
            "complete_list_cost_usd": (
                _usd(self._known_total) if complete and not self._unknown
                else None
            ),
            "missing_cost_fields": sorted(self._unknown),
            "correct": sum(step.get("task_success") is True for step in steps),
            "incorrect": sum(step.get("task_success") is False for step in steps),
            "unavailable": sum(step.get("task_success") is None
                               for step in steps if step["status"] == "REPLAYED"),
            "steps": list(steps),
            "external_calls_made": False,
            "hidden_outcomes_exposed_to_policy": False,
            "source_bound_evidence_verified": False,
        }


def run_policy(episode: TenRouteEpisodeReplay,
               choose: Callable[[Mapping[str, Any]], str]) -> dict[str, Any]:
    """Replay a fixed policy, revealing one public observation at a time."""

    steps = []
    while episode.has_next:
        observation = episode.observation()
        step = episode.step(choose(observation))
        steps.append(step)
        if step["status"] != "REPLAYED":
            break
    return episode.result(steps)


def fixed_policy(arm: str, *, node: str = "N7") -> Callable[[Mapping[str, Any]], str]:
    if node not in NODES or arm not in ARMS:
        raise ValueError("fixed policy action is invalid")
    return lambda observation: f"{node}/{arm}"


def no_eviction_admission_policy(*, node: str = "N7") -> Callable[
    [Mapping[str, Any]], str
]:
    """Admit only if the current video fits without evicting another entry."""

    if node not in NODES:
        raise ValueError("capacity-aware policy node is invalid")

    def choose(observation: Mapping[str, Any]) -> str:
        cache = observation["cache_by_node"][node]
        present = {row["representation_id"] for row in cache["entries"]
                   if row["object_id"] == observation["object_id"]}
        missing = set(REPRESENTATIONS) - present
        needed = sum(observation["cacheable_sizes"][rep] for rep in missing)
        if cache["used_bytes"] + needed <= cache["capacity_bytes"]:
            return f"{node}/DC"
        return f"{node}/D"

    return choose
