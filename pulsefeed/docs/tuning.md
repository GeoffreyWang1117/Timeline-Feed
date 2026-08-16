# Tuning reference

Every knob, what it actually controls, and how to tell you have it wrong.

Before changing anything: **the frontier sweep is the tool for threshold
questions.** `python -m harness.frontier` runs the whole system at ten
thresholds on one fixed trace and prints cost against coverage. Guessing is
slower than measuring here.

---

## 1. Trigger — `UtilityWeights`, `CompositePolicy`

```
utility = α·importance + β·novelty + γ·uncertainty + δ·risk − λ·(tokens/2000)
invoke if utility > θ
```

| knob | default | effect | symptom of a wrong value |
|---|---|---|---|
| `alpha` (importance) | 1.0 | weight on the cheap score | lower it if the lexicon is the only thing deciding |
| `beta` (novelty) | 0.35 | weight on "unlike anything recent" | too high → novel chatter outranks repeat incidents |
| `gamma` (uncertainty) | 0.45 | **pays to resolve what the cheap layer cannot settle** | at 0 you only ever confirm what you already knew |
| `delta` (risk) | 0.9 | weight on the risk lexicon | too high → the system is a keyword alerter |
| `lam` (cost) | 0.30 | penalty on estimated tokens | too high → long events never enriched regardless of value |
| `cost_normalizer` | 2000 | tokens counting as "one unit of cost" | set near your median enrichment size |

### Thresholds

| policy | knob | default | meaning |
|---|---|---|---|
| `BudgetAwarePolicy` | `theta_min` / `theta_max` | 0.40 / 0.85 | θ at full budget / at zero budget |
| `LoadAwarePolicy` | `theta_idle` / `theta_saturated` | 0.35 / 0.90 | θ at empty queue / at full queue |
| `DeadlineAwarePolicy` | `base_theta` | 0.45 | floor; the veto does the real work |
| `DeadlineAwarePolicy` | `safety_margin` | 1.15 | how much slack before refusing on freshness |
| `CompositePolicy` | `boundary_check_discount` | 0.6 | θ multiplier for ambiguous-merge clusters |
| `CompositePolicy` | `audit_sample_rate` | 0.01 | fraction of **rejections** enriched for measurement |

**Composition: the maximum threshold binds.** Raising `theta_idle` above
`theta_max` silently makes the budget policy irrelevant.

**Do not set `audit_sample_rate` to 0 in production.** It costs ~1% of your
calls and it is the only thing that turns "we don't miss important events" from
an assertion into a measurement. It is also the unbiased half of your training
data ([training.md](training.md)).

### Choosing θ

From a real sweep (6,396-event trace):

| θ | calls | cost | enriched coverage | storylines | coverage per call |
|---|---|---|---|---|---|
| 0.05 | 137 | $0.1186 | 97.4% | 100% | 0.71% |
| 0.15 | 85 | $0.0738 | 81.6% | 100% | 0.96% |
| 0.35 | 36 | $0.0380 | 71.0% | 100% | 1.97% |
| 0.55 | 32 | $0.0350 | 68.4% | 80% | 2.14% |
| 0.75 | 17 | $0.0139 | 28.9% | 80% | 1.70% |

An 8x spend dial spanning the whole useful range: at θ=0.05 the system reaches
97.4% enriched coverage and every storyline for $0.12 — still 36x cheaper than
enriching everything. Marginal returns fall about 3x across the sweep. With 38
important events across 5 storylines, individual points are noisy; use the
shape.

At θ = 0.95 a nonzero floor of calls remains: those are safety-rule bypasses.
Critical events are never subject to the budget dial.

---

## 2. Coalescing — `CoalescerConfig`

| knob | default | effect |
|---|---|---|
| `window_seconds` | 90 | max span of one cluster |
| `idle_close_seconds` | 20 | quiet period that closes a cluster |
| `max_cluster_size` | 25 | members before forced close |
| `merge_similarity` | 0.80 | at or above: confidently the same thing |
| `ambiguous_similarity` | 0.58 | below: confidently different |
| `max_open_per_entity` | 6 | concurrent conversations tracked per entity |
| `deadline_guard_seconds` | 5 | close early rather than miss freshness |

**These are grouping parameters only.** They used to double as latency
parameters — a cluster reached the feed only when it closed — until provisional
publication decoupled them. Do not re-tighten the window to "make the feed
faster"; it will not, and it will cost grouping.

**`max_open_per_entity` matters more than it looks.** At 1, an entity carrying
several interleaved conversations (any busy channel) closes its cluster on
almost every event and nothing coalesces. Raising it from 1 to 6 cut the item
count from 4,718 to 1,712 on the standard trace.

The gap between `ambiguous_similarity` and `merge_similarity` is the band where
the LLM is asked to resolve the boundary. Narrowing it to zero removes the
system's best-justified class of call.

**Symptom of too-loose merging:** unrelated events in one row; `needs_boundary_check`
firing constantly. **Too-tight:** near-duplicate rows in the feed, high item count,
LLM calls spent on repeats.

---

## 3. Scheduler — `SchedulerConfig`

| knob | default | effect |
|---|---|---|
| `capacity[P0]` | 64 | reserved lane; only P0 uses it |
| `capacity[P1]` | 256 | queued; falls back to cheap path when full |
| `capacity[P2]` | 512 | triggers harder coalescing before filling |
| `capacity[P3]` | 128 | shed first |
| `weights` | 8:4:2:1 | deficit round robin shares |
| `coalesce_pressure_ratio` | 0.6 | fill fraction that turns on aggressive coalescing |

**Weights, not strict priority.** Strict priority starves P3 forever under a
sustained P0 stream. There is a test that fails if someone "simplifies" this.

Total capacity should be roughly *worker_count × service_rate × the longest
freshness deadline you care about*. Beyond that you are queueing work that will
expire before it is served.

**In practice the queue barely fills.** At 45.6x measured offered load the peak
depth is 0 — coalescing and threshold raising absorb bursts upstream. If your
queue is deep, the trigger is admitting too much: lower θ before enlarging the
queue.

---

## 4. Budget — `TenantPlan`

| knob | default | effect |
|---|---|---|
| `daily_token_budget` | 500k | hard ceiling; also drives `BudgetAwarePolicy` |
| `daily_usd_budget` | $5 | whichever binds first |
| `reserved_fraction_p0` | 0.15 | slice only P0 may spend |
| `max_concurrent_llm_jobs` | 4 | provider concurrency |

The reservation is the point: a noisy Tuesday must not be able to consume the
capacity an incident will need. Non-P0 work spends down to the floor; P0 spends
the whole allowance.

Presets: `TenantPlan.free()` (5k tokens, fallback model only),
`.pro()` (500k), `.enterprise()` (configurable, 25% reserved).

---

## 5. Provider — `ResilienceConfig`, `BreakerConfig`

| knob | default | effect |
|---|---|---|
| `max_attempts` | 3 | attempts per provider |
| `base_backoff` / `max_backoff` | 0.25 / 4.0 | exponential backoff seconds |
| `jitter` | 0.5 | ±fraction, spreads retries across replicas |
| `per_attempt_timeout` | 10.0 | wall-clock ceiling per call |
| `retry_budget_ratio` | 0.2 | retries allowed as a fraction of normal traffic |
| `failure_threshold` | 5 | consecutive failures that trip the breaker |
| `success_threshold` | 2 | probe successes that close it |
| `recovery_timeout` | 20.0 | seconds before probing again |
| `recovery_jitter` | 0.3 | spreads reopen attempts |

**`retry_budget_ratio` is the one people remove and regret.** Unbounded retry is
how a degraded provider becomes a dead one: every client triples its load
exactly when the provider can least take it.

**Set `per_attempt_timeout` below your freshness deadlines.** A provider that
always exceeds its timeout is *down*, not slow, and will trip the breaker — that
is correct, but it means the deadline logic never runs. The two paths are
different and both need to work.

---

## 6. Scorer — `ScorerWeights`, `ScorerConfig`

| knob | default | note |
|---|---|---|
| `bias` | −3.6 | sets the base rate |
| `source_priority` | 2.2 | |
| `risk` | 2.6 | a fitted model independently lands at 2.6–3.8 |
| `novelty` | 0.7 | **deliberately weak** — see below |
| `user_relevance` | 1.5 | a fitted model wants 3.3–3.8; likely underweighted |
| `burst` | 0.9 | |
| `duplication` | −1.8 | |
| `recency` | 0.5 | **does nothing** — the feature is constant |
| `novelty_window` | 256 | embeddings retained per tenant for novelty |
| `burst_saturation` | 8 | events/entity/window meaning "bursting" |

Novelty is weak on purpose: almost every event is novel the first time it is
seen, so a large novelty weight makes "importance" mostly measure "arrived
recently" — the chronological baseline wearing a hat.

Two known defects, documented rather than silently patched: `novelty` and
`duplication` are exactly `1 − x` of each other, and `recency` is constant
because events are scored at ingest. Fixing either changes `FEATURE_NAMES` and
invalidates every fitted model, so it wants a deliberate migration.

---

## 7. Pipeline — `PipelineConfig`

| knob | default | effect |
|---|---|---|
| `worker_count` | 4 | concurrent enrichments |
| `tick_interval` | 5.0 | how often idle clusters are closed |
| `episode_interval` | 120.0 | rollup pass frequency |
| `timeline_capacity` | 2000 | rows retained per tenant |
| `feature_memory` | 20000 | scored vectors kept for training/debug |
| `audit_sample_rate` | 0.01 | passed to the default policy |

`tick_interval` bounds how long the *last* event on a quiet entity waits before
its cluster closes. It no longer bounds time-to-feed (provisional publication
does that), but it does bound time-to-enrichment.

---

## 8. Ranking

Not currently configurable — the weights live in `_cheap_rank` and
`_enriched_rank`. Present values:

```
cheap    = 0.40·importance + 0.26·risk + 0.26·user_relevance + 0.08·recency
enriched = 0.55·severity + 0.30·cheap + 0.15·confidence
entity boost (read time): degraded +0.18, recovering +0.08
```

The entity boost is applied **at read time**, not baked in at publish, because
the verdict that a service is degraded usually lands after the routine-looking
event was already written.

This is the weakest part of the system: at K=20 it trails LLM-everything 81.6%
vs 94.7%, with relevant rows at ranks 1–7 and then not again until 24.

---

## 9. A tuning checklist

1. **Reproduce first.** `python -m harness.replay` on a trace shaped like your
   traffic. If you cannot reproduce the problem there, you are about to tune
   against an anecdote.
2. **Change one thing.** The threshold policies compose by max; two changes can
   cancel and look like no effect.
3. **Check calibration, not just ranking.** All three threshold policies read
   the score as a probability.
4. **Re-run the chaos suite.** `python -m harness.failure_injection`. Several
   knobs (queue capacity, breaker thresholds, deadlines) only show their real
   effect under failure.
5. **Watch `skip_reasons`.** "Fewer LLM calls" has at least four distinct causes
   — below threshold, over budget, would be stale, queue full — and they call
   for opposite fixes.
