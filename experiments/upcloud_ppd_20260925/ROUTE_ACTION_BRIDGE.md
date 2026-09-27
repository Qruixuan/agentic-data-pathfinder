# Route-action connector (offline implementation)

`pathfinder/integrations/flowmesh/route_action_bridge.py` connects the
existing ten-route multi-question plan to an Agent-facing choice. It does
not implement another route runner or change FlowMesh.

The bridge canonically verifies the public plan on load. For each question,
it exposes one action per D0–D7 allowed by `D_base`, `D_index`, `D_cache`, or
`D_joint`. D3 and D7 remain single choices: a separately verified cache
observation selects their frozen miss or hit slot. The Agent never chooses
miss versus hit. Every available action needs a quote from the same frozen
price basis; an absent or stale quote is an error, not a zero price.

`commit_choice` writes one choice per session in SQLite. Repeating the exact
choice is idempotent; changing the action, price set, physical design or
cache observation is rejected. The Agent must return the digest of the offer
set it saw; a cache or quote change between listing and commit is rejected.
`handoff` checks the chosen identity against the admitted trial and returns
`run_id`, `cache_episode_id`, trial and
idempotency key for the existing `FlowMeshSemanticTrialExecutor`. It does
not call that executor.

The bridge now requires an explicit, fresh `execution_namespace`. Run IDs
are derived from that namespace and the committed session/action; they are
never copied from the historical schedule. Cache episodes are derived from
the namespace, video object and N7/N8 node, so different questions on one
video can observe a real reuse opportunity while videos and experiments
remain isolated. The frozen trial key is still reused as an admission
identity; a new live admission must validate its source bindings before
handoff.

`route_action_gateway.py` adds a separate, persistent route session protocol.
It binds the public question/object, asks injected verified providers for
quotes and cache observations, and exposes only `list_route_offers` and
`commit_route_choice` to the Agent. The tool response deliberately omits
run IDs, trial keys and any hidden score. The runner can retrieve the bound
handoff only after commitment. The existing representation Gateway and its
five tools are unchanged. `mcp_server.py` registers the two new tools only
when a `RouteActionGateway` is explicitly supplied; no environment setting
silently activates them.

`route_action_workflow.py` reuses the existing pinned FlowMesh Agent graph
with a selection-only prompt. The dedicated Qwen config exposes only the two
route tools, names `list_route_offers` as its first tool, and asks the Agent
to commit a route, **not** answer the video question. A later route execution
will make the separate N6 inference request.

The historical 400-route plan is used only as a read-only regression fixture.
Its run IDs are already used and must never be submitted again. A new
authenticated `/v1/cache/status` endpoint now verifies resident bytes and
returns metadata without downloading the object, writing a HIT/MISS event or
changing LRU. `LiveCacheStatusReader` probes every component in a separately
verified artifact catalog; partial presence is a miss. The runtime remains
the authority for insertion lineage and rechecks actual state during use.

`FrozenRouteQuoteSource` now recomputes every offer from an exact-file-set,
checksum-bound cost-only development projection and rate card. The bounded
historical t60 pilot has 60 N6 usage joins, zero unpriced attempts and six
samples for each of the ten action/cache states. The current draft package at
`artifacts/route-action-quotes-t60-dev-20260927-v2-draft/` prices N6 tokens,
index-only query embedding and route-time allocation of the current nine-VM
plan, including measured batch overhead. The v1 draft is retained as an
earlier attempt that omitted that overhead. Its rate card reflects the
expanded N7 `DEV-2xCPU-8GB` plan; the older
VM snapshot cannot be reused unchanged. It is a **prebuilt route-execution
estimate**, not the complete episode/build cost or an observed invoice.
`full_episode_cost_complete=false` is explicit. The 400-route accounting
remains partial and is not silently substituted as the source.

The package is an engineering draft bound to a public plan whose canonical
status is `VERIFIED_TEN_ROUTE_MULTIQ_PLAN_NOT_ADMITTED`; the changed quote
source is not yet deployed. Its ten means are static across
questions, and six observations per state do not establish a stable cache
latency benefit. Do not submit a workflow solely because this loader passes.

An opt-in `--route-action-preview-config` now constructs the route Gateway
from independently pinned plan, trace and rate-card digests. It is restricted
to a loopback listener and to `preview_only=true`; it neither authorizes a
route handoff nor submits a FlowMesh workflow. The no-model regression
registers a public session, lists the four `D_base` offers and commits one
choice using the real development quote package. Cache designs still require
an independently verified live cache reader and are not enabled by this
preview bootstrap.

Before live use, the operator still needs a **new** source-bound
plan/admission, a separately verified design/episode ledger for build,
storage occupancy, transitions and Agent-model cost, a verified artifact
catalog supplied to the status reader, an admission-gated live Gateway
bootstrap, cache-service deployment of the new status endpoint, and
all runbook pre-submit gates. None of these is implicit. This is not a
deployment or a claim that live PPD route choice is ready.
