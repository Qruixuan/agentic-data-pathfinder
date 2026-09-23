from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

from pathfinder.integrations.flowmesh.semantic_matrix_trial import (
    FlowMeshSemanticTrialError,
    GenericSemanticRouteRequestHandler,
    SEMANTIC_ROUTE_EPISODE_REQUEST_SCHEMA_VERSION,
    build_semantic_route_request,
    validate_semantic_route_request,
)
from pathfinder.simulator.full_flow_route_adapters import (
    HttpArtifactCacheRouteAdapter,
    SQLiteCacheLineageStore,
    semantic_cache_episode_namespace,
    semantic_cache_namespace,
)
from pathfinder.simulator.full_flow_semantic_route_runtime import ArtifactAccess
from pathfinder.simulator.full_flow_semantic_route_service_factory import (
    FrozenCatalogBoundSemanticRouteRequestHandler,
    FullFlowSemanticRouteServiceFactoryError,
)
from tests.test_flowmesh_semantic_matrix_trial import _fixtures
from tests.test_simulator_full_flow_route_adapters import (
    BUNDLE,
    _CacheClient,
    _identity,
    _trial,
)


class _Coordinator:
    def __init__(self) -> None:
        self.kwargs = None

    def execute(self, **kwargs):
        self.kwargs = kwargs
        return {"status": "test-only"}


class InterleavedCacheEpisodeTest(unittest.TestCase):
    def test_frozen_handler_rejects_unbound_episode_and_run(self) -> None:
        _source, trial, stages = _fixtures("N7")
        trial["route_family"] = "local-cache-derived"
        request = build_semantic_route_request(
            run_id="frozen-route-one",
            idempotency_key=hashlib.sha256(b"frozen-route-one").hexdigest(),
            bound_trial=trial,
            bound_stages=stages,
            cache_episode_id="frozen-episode-dc",
        )
        coordinator = _Coordinator()
        delegate = GenericSemanticRouteRequestHandler(coordinator)
        kwargs = {
            "logical_node_id": "N7",
            "delegate": delegate,
            "bound_trials": [trial],
            "bound_stages": stages,
        }
        unbound = FrozenCatalogBoundSemanticRouteRequestHandler(**kwargs)
        with self.assertRaisesRegex(
            FullFlowSemanticRouteServiceFactoryError,
            "absent from the verified run binding",
        ):
            unbound.execute(request)
        self.assertIsNone(coordinator.kwargs)
        bound = FrozenCatalogBoundSemanticRouteRequestHandler(
            **kwargs,
            bound_cache_episodes={
                ("frozen-route-one", trial["trial_key"]): "frozen-episode-dc"
            },
        )
        self.assertEqual("test-only", bound.execute(request)["status"])
        different_run = build_semantic_route_request(
            run_id="frozen-route-two",
            idempotency_key=hashlib.sha256(b"frozen-route-two").hexdigest(),
            bound_trial=trial,
            bound_stages=stages,
            cache_episode_id="frozen-episode-dc",
        )
        with self.assertRaisesRegex(
            FullFlowSemanticRouteServiceFactoryError,
            "absent from the verified run binding",
        ):
            bound.execute(different_run)

    def test_signed_episode_is_bound_and_only_accepted_for_cache(self) -> None:
        _source, trial, stages = _fixtures("N7")
        trial["route_family"] = "local-cache-derived"
        request = build_semantic_route_request(
            run_id="unique-route-001",
            idempotency_key=hashlib.sha256(b"route-001").hexdigest(),
            bound_trial=trial,
            bound_stages=stages,
            cache_episode_id="multiq-episode-dc",
        )
        self.assertEqual(
            SEMANTIC_ROUTE_EPISODE_REQUEST_SCHEMA_VERSION,
            request["schema_version"],
        )
        coordinator = _Coordinator()
        handler = GenericSemanticRouteRequestHandler(coordinator)
        handler.execute(request)
        self.assertEqual(
            "multiq-episode-dc", coordinator.kwargs["cache_episode_id"]
        )
        tampered = dict(request, cache_episode_id="other-episode")
        with self.assertRaisesRegex(
            FlowMeshSemanticTrialError, "digest changed"
        ):
            validate_semantic_route_request(tampered)
        trial["route_family"] = "remote-derived"
        with self.assertRaisesRegex(
            FlowMeshSemanticTrialError, "requires a cache route"
        ):
            build_semantic_route_request(
                run_id="unique-route-002",
                idempotency_key=hashlib.sha256(b"route-002").hexdigest(),
                bound_trial=trial,
                bound_stages=stages,
                cache_episode_id="multiq-episode-dc",
            )

    def test_episode_scope_is_separate_from_run_scope(self) -> None:
        episode = semantic_cache_episode_namespace("multiq-episode-dc")
        self.assertEqual(
            episode, semantic_cache_episode_namespace("multiq-episode-dc")
        )
        self.assertNotEqual(
            episode, semantic_cache_episode_namespace("another-episode")
        )
        self.assertNotEqual(episode, semantic_cache_namespace("multiq-episode-dc"))

    def test_two_questions_share_only_the_declared_episode(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            clients = {
                node: _CacheClient(node, f"{node.lower()}-cache")
                for node in ("N7", "N8")
            }
            adapter = HttpArtifactCacheRouteAdapter(
                clients=clients,
                runtime_epoch_probes={
                    node: lambda node=node: {
                        "status": "ok", "node_id": node,
                        "runtime_epoch": ("1" if node == "N7" else "2") * 32,
                        "credentials_recorded": False,
                    }
                    for node in ("N7", "N8")
                },
                lineage=SQLiteCacheLineageStore(
                    Path(temporary) / "lineage.sqlite3"
                ),
            )
            identity = _identity("sampled_frame_bundle", BUNDLE)
            first = _trial(route="local-cache-derived", repetition=0)
            first["trial_key"] = "public-question-one"
            miss = adapter.lookup(
                run_id="route-one", cache_episode_id="episode-dc",
                trial=first,
                stage={"logical_node_ids": ["N7"], "stage_key": "lookup"},
                identity=identity,
            )
            self.assertEqual("miss", miss.branch)
            adapter.insert(
                run_id="route-one", cache_episode_id="episode-dc",
                trial=first,
                stage={"logical_node_ids": ["N7"], "stage_key": "insert"},
                identity=identity, lookup=miss,
                artifact=ArtifactAccess(
                    source_identity=identity, payload=BUNDLE
                ),
            )
            second = _trial(route="local-cache-derived", repetition=0)
            second["trial_key"] = "public-question-two"
            hit = adapter.lookup(
                run_id="route-two", cache_episode_id="episode-dc",
                trial=second,
                stage={"logical_node_ids": ["N7"], "stage_key": "lookup"},
                identity=identity,
            )
            self.assertEqual("hit", hit.branch)
            self.assertEqual("public-question-one", hit.source_insert_trial_key)
            other = adapter.lookup(
                run_id="route-three", cache_episode_id="other-episode",
                trial=second,
                stage={"logical_node_ids": ["N7"], "stage_key": "lookup"},
                identity=identity,
            )
            self.assertEqual("miss", other.branch)
            clients["N7"].values.clear()  # Simulates capacity eviction.
            evicted = adapter.lookup(
                run_id="route-four", cache_episode_id="episode-dc",
                trial=second,
                stage={"logical_node_ids": ["N7"], "stage_key": "lookup"},
                identity=identity,
            )
            self.assertEqual("miss", evicted.branch)
            self.assertIsNone(evicted.source_insert_trial_key)


if __name__ == "__main__":
    unittest.main()
