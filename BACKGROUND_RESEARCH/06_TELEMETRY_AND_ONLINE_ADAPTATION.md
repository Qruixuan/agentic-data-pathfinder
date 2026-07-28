# D6. Automated Experiments, Reveal, and Safe Adaptation

## 1. Revised Role

The previous direction treated Structured Autoresearch as a broad
evidence-acquisition loop. The mentor's paper skeleton makes its role more
specific:

```text
observations -> Adaptive Workload Model
             -> OED Commit / Reveal / Hold
             -> restoration or certified deployment
             -> escalation when ambiguity is structural
```

D6 supplies experiment-selection and safe-execution mechanisms. The main
research question is now whether those mechanisms can reveal censored,
design-dependent workload behavior at acceptable cost.

## 2. Representative Work

| Work | Established capability | Implication |
|---|---|---|
| [iTuned, VLDB 2009](https://www.vldb.org/pvldb/vol2/vldb09-193.pdf) | Adaptive low-overhead database experiment planning | Active evidence acquisition is not new |
| [Ernest, NSDI 2016](https://pages.cs.wisc.edu/~shivaram/publications/ernest-nsdi.pdf) | Structural models and optimal experiment design | Information-efficient structured sampling is established |
| [CherryPick, NSDI 2017](https://www.usenix.org/conference/nsdi17/technical-sessions/presentation/alipourfard/) | Bayesian optimization with few cloud-configuration trials | Strong plan-level black-box baseline |
| [To Tune or Not to Tune, VLDB 2006](https://www.microsoft.com/en-us/research/publication/to-tune-or-not-to-tune-a-lightweight-physical-design-alerter/) | Low-cost bounds for deciding whether tuning is worthwhile | Hold/refusal is not novel by itself |
| [UDO, PVLDB 2021](https://www.vldb.org/pvldb/vol14/p3402-wang.pdf) | Heavy/light trial ordering to amortize transitions | Strong stateful-experiment baseline |
| [MLOS in Action, PVLDB 2024](https://www.vldb.org/pvldb/vol17/p4269-kroth.pdf) | Reusable experiment infrastructure and result management | Experiment manager and evidence store are substrate |
| [KEA, 2021](https://arxiv.org/abs/2106.11445) | Observational tuning plus cautious production flighting | Passive evidence plus canaries is established |
| [OnlineTune, 2022](https://arxiv.org/abs/2203.14473) | Contextual Bayesian optimization with safety constraints | Safe subspace exploration under drift is mature |
| [SelfTune, NSDI 2023](https://www.usenix.org/conference/nsdi23/presentation/karthikeyan) | Online production tuning through simple interfaces | General self-tuning is not a contribution |
| [OPPerTune, NSDI 2024](https://www.usenix.org/conference/nsdi24/presentation/somashekar) | Automatic choice of tuning scope with low disruption | Scope selection and low-disruption adaptation are strong baselines |
| [Oracle Automatic Indexing, PVLDB 2025](https://www.vldb.org/pvldb/vol18/p4924-chakkappen.pdf) | Isolated validation, incremental deployment, regression protection, cleanup | Safe physical-design lifecycle is mature |

Bandit physical-design systems in D5 and performative/selective-feedback work
in D8 are also required comparisons.

## 3. Mature Mechanisms to Reuse

The following are implementation patterns, not standalone claims:

- active sampling and optimal experiment design;
- Bayesian and bandit exploration;
- child plans, canaries, and namespaces;
- deployment gates and regression protection;
- rollback and restoration;
- experiment provenance and replay;
- transition-aware trial ordering;
- drift detection and anti-thrashing; and
- stopping when expected information or improvement value is too low.

## 4. What Changes Under Performative Physical Design

### 4.1 The observation target is counterfactual workload

A conventional canary asks how a known workload performs under a candidate.
A Pathfinder Reveal also asks whether the candidate makes a representation
accessible and thereby changes task choice, success, or future demand.

Telemetry must record the complete offered set, not only the selected path.
For every task class and representation it must distinguish:

- quoted access price `p_qv(D)`;
- felt latency after selection; and
- realized physical cost.

Quote interventions and real physical interventions should be matched on
`p_qv`; otherwise an apparent construct-validity result can be caused by
different offers rather than equivalent access signals.

### 4.2 A probe changes both knowledge and physical state

A Reveal may create a digest, embedding, replica, or transform output. After
restoring `D_safe`, the observation remains; a valid artifact may also remain
eligible for future adoption.

The full probe cost is:

```text
forward transition
+ probe execution
+ foreground degradation
+ restoration
- defensible reusable-artifact value
```

Canary isolation limits blast radius but does not make this cost zero.

### 4.3 Safe deployment and exploration are different sequences

The certified safe sequence contains only committed designs. Reveal executions
are exploratory excursions and require separate loss and safety reporting.
Restoring `D_safe` after a probe does not retroactively make probe foreground
loss part of a no-regression guarantee.

### 4.4 Some uncertainty is not probeable

OED must expose:

- `G_cert`: candidates that can be conservatively evaluated;
- `G_probe`: candidates reachable by an allowed bounded probe; and
- `G_other`: candidates outside the current certificate and intervention set.

Ordinary probes cannot justify a global claim while `G_other` is hidden.
Escalation may expand the observation or candidate space, or the system may
Hold.

### 4.5 Reveal resolution is indexed by predeclared price levels

For each task-class/representation pair, declare before the run:

```text
P_qv = { p_qv(D) : D in D_gov and p_qv(D) is within the access gate }
```

The general Reveal-resolution bound is `sum_{q,v} |P_qv|`. It becomes
`|Q||V|` only if every executed Reveal is pair-canonical, and becomes `|V|`
only if every executed Reveal is simultaneously canonical for all affording
classes. The selection policy should therefore prefer:

1. simultaneously canonical Reveals;
2. pair-canonical Reveals; and
3. any remaining budget-feasible Reveal.

The recorded tier is part of the theorem audit. A uniform affordability gate
does not by itself justify the smaller bounds.

### 4.6 Quote sufficiency is a gate; exogeneity is a schedule contract

The current AWM assumes the response can be written as `eta(p(D))` and
`rho(p(D))`. A factorial experiment must hold the quote fixed while varying
felt latency. If the conditional effect is material, either reserve latency
tightly per quote or expand the response model; until then, scalar-price
certificates and Reveal counts are invalid.

Queued sessions impose the exogeneity needed by the current theory. This is
not an empirical falsification gate. Instead, evaluation should deliberately
introduce re-arrivals or other feedback and report how quickly the guarantees
and decisions degrade outside the imposed scope.

## 5. Defensible OED/Autoresearch Boundary

The proposed increment is not "AI runs experiments." It is:

> Given a coupled ambiguity set for design-induced agent behavior and a
> stateful representation path, select a Commit, Reveal, or Hold action while
> accounting for transition, restoration, and reusable probe artifacts.

The following elements must jointly matter:

- task-class and affordability structure;
- substitution-aware AWM bounds;
- candidate differences over `M/L/E`;
- fixed, class-specific executable price-level sets `P_qv`;
- candidate-relative certificates;
- restoration to `D_safe`; and
- artifact-aware probe cost.

If a standard bandit or Bayesian tuner performs equally well, these additional
mechanisms are not justified.

## 6. Escalation Ladder

When AWM bounds are too wide and no ordinary Reveal has positive robust value,
Pathfinder may:

1. run an operator or path microbenchmark;
2. partially materialize a representation;
3. widen the task/session sample;
4. use another predeclared access profile;
5. refine a task class or substitution group;
6. expand candidate generation; or
7. request external resolution of a semantic or policy ambiguity.

Every escalation must name the decision bound it can change. Otherwise it is
generic measurement and should be refused.

## 7. Required Baselines and Metrics

Compare from the same `D_safe`, history, artifacts, candidate space, and full
budget:

1. fixed analytical planning;
2. passive observation;
3. random feasible probes;
4. plan-level Bayesian optimization;
5. contextual/combinatorial bandits;
6. UDO-style heavy/light ordering;
7. AWM without active Reveal;
8. OED without artifact reuse;
9. full AWM + OED + escalation; and
10. a reduced exhaustive oracle.

Report:

- safe design value versus cumulative full exploration cost;
- Commit/Reveal/Hold and escalation counts;
- simultaneously canonical, pair-canonical, and fallback Reveal counts;
- resolved `P_qv` levels and the applicable Reveal bound;
- false-safe and false-hopeless decisions;
- bound coverage and width;
- candidate-value, incumbent-value, and transition-cost contributions to
  `delta_t`;
- forward, foreground, and restoration costs;
- quoted `p_qv`, felt latency, realized cost, and their conditional effects;
- safe-sequence regressions;
- retained observations and adopted probe artifacts; and
- the fraction and oracle value of `G_other`.

## 8. Main Takeaway

Automated experiments, canaries, rollback, safe tuning, and transition-aware
trial ordering are mature. Pathfinder's possible increment is a Reveal policy
for censored endogenous demand under coupled ambiguity, where probes alter a
costly representation graph and certificates are explicitly limited to the
modeled candidate pools.
