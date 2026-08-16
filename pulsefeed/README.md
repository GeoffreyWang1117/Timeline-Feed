# PulseFeed

**Event-triggered LLM enrichment for real-time activity feeds.**

An LLM call is a scarce, slow, failure-prone online resource. It should not be
a step that every piece of data passes through. PulseFeed treats invocation as
a *control action*: a decision, made per event, about whether this particular
thing is worth spending one on right now — given the remaining budget, the
current queue depth, and how long the answer stays worth having.

```
Event sources
     │
     ▼
Streaming ingestion ─────────────────────────────┐  never blocks on the LLM
     │                                           │
     ▼                                           │
Cheap feature extraction   (100% of traffic)     │
     │                                           │
     ▼                                           │
Coalescing / dedup         (cheap grouping)      │
     │                                           │
     ▼                                           │
Trigger / admission policy                       │
     │                    ╲                      │
  cheap path            LLM path                 │
     │                     │                     │
     │              bounded priority queue       │
     │                     │                     │
     │              semantic enrichment          │
     ╲                    ╱                      │
      ──── entity memory ────                    │
              │                                  │
       ranking / clustering                      │
              │                                  │
     hierarchical summaries                      │
              │                                  │
        user timeline  ◀───────────────────────── ┘
```

The feed is complete at every point in that diagram. An event that is never
enriched still becomes a ranked, deduplicated timeline item immediately;
enrichment upgrades the item in place when it lands. **Losing the LLM entirely
costs the feed its prose, not its contents.**

---

## Results

All numbers below are produced by `python -m harness.replay` on a seeded
synthetic trace of **6,396 events over 15 simulated minutes** (2 events/s
baseline, 20x bursts, 3 planted incidents, 38 ground-truth-important events).
Nothing is hardcoded; re-running reproduces them.

### Cost vs quality

| arm | LLM calls | tokens | cost | recall@20 | prec@20 | enriched cov | storylines | items |
|---|---|---|---|---|---|---|---|---|
| A. Chronological | 0 | 0 | $0.0000 | 7.9% | 15% | 0% | 0% | 6,396 |
| B. Rule + embedding | 0 | 0 | $0.0000 | 52.6% | 100% | 0% | 0% | 1,730 |
| C. LLM-everything | 4,905 | 2,218,794 | $3.3282 | **94.7%** | 95% | 65.8% | 100% | 4,603 |
| **D. PulseFeed** | **48** | **28,317** | **$0.0425** | 81.6% | 60% | 60.5% | 80% | 1,712 |

### Recall as the page grows, and the scroll depth it costs

| arm | R@10 | R@20 | R@50 | items→50% | items→80% | items→95% |
|---|---|---|---|---|---|---|
| A. Chronological | 7.9% | 7.9% | 10.5% | 3,159 | 4,903 | 6,150 |
| B. Rule + embedding | 26.3% | 52.6% | 92.1% | 19 | 47 | 63 |
| C. LLM-everything | 44.7% | 94.7% | 94.7% | 12 | 20 | 419 |
| **D. PulseFeed** | **76.3%** | 81.6% | **97.4%** | **2** | **19** | **35** |

### System behaviour under the same load

| arm | p50 e2e | p95 e2e | p99 queue wait | max queue | shed as stale |
|---|---|---|---|---|---|
| A. Chronological | 0.00s | 0.00s | 0.00s | 0 | 0 |
| B. Rule + embedding | 0.00s | 0.00s | 0.00s | 0 | 0 |
| C. LLM-everything | 0.00s | 67.30s | 74.31s | 1,748 | 1,500 |
| **D. PulseFeed** | **0.00s** | **0.00s** | **0.00s** | **2** | **0** |

**Reading these honestly:**

- **99.0% fewer LLM calls than enriching everything (48 vs 4,905), at 1.3% of
  the cost.**
- **PulseFeed dominates on scroll depth.** Two items to reach half the important
  events (C needs 12, B needs 19); 35 to reach 95% where C needs **419**. One
  episode row carries a whole incident instead of spending twenty rows on it.
- **LLM-everything wins recall@20 (94.7% vs 81.6%).** That is a real loss, not a
  metric artifact: between ranks 8 and 23 PulseFeed's feed contains items that
  should have been displaced. K=20 is the one slice where spreading events
  thinly across rows pays, and C buys it at 102x the calls with a queue that
  collapses.
- **LLM-everything cannot keep up.** 1,748 items queued, p99 wait 74s, and 1,500
  shed as stale before ever being served. PulseFeed's queue peaks at 2.
- **precision@20 60% vs 95%** — same cause as the recall@20 gap; the ranker still
  has room.
- **PulseFeed covers 80% of planted storylines against 100%.** Admission control
  does miss things. That is the trade, measured rather than asserted.
- **With no LLM at all** (arm B — exactly what happens when the provider is
  down) the feed still reaches 52.6% recall@20 and 100% total coverage, and it
  is the *same* 1,730-item feed shape, just without prose.

### Cost–quality frontier

`python -m harness.frontier` sweeps the utility threshold from 0.05 to 0.95 on
one fixed trace, holding every other component constant.

| θ | LLM calls | cost | enriched cov | cov/call | recall@20 |
|---|---|---|---|---|---|
| 0.05 | 133 | $0.1031 | 63.2% | 0.47% | 97.4% |
| 0.15 | 83 | $0.0658 | 68.4% | 0.82% | 86.8% |
| 0.25 | 52 | $0.0417 | 60.5% | 1.16% | 81.6% |
| 0.35 | 34 | $0.0291 | 60.5% | 1.78% | 81.6% |
| 0.55 | 28 | $0.0238 | 50.0% | 1.79% | 79.0% |
| 0.75 | 15 | $0.0119 | 28.9% | 1.93% | 65.8% |
| 0.95 | 15 | $0.0119 | 28.9% | 1.93% | 65.8% |

- The threshold is a **9x spend dial** on one unchanged trace, and marginal
  returns fall about 6x from the cheap end (1.93% coverage per call) to the
  expensive one (0.29% for the marginal call between the extremes).
- **Even at θ=0.95 — admit essentially nothing — 15 calls still happen.** Those
  are the hard safety-rule bypasses. Critical events are never subject to the
  budget dial.
- **recall@20 does move with θ** (65.8% → 97.4%), but only because episodes are
  built from LLM-produced cluster summaries and episodes pack many events into
  one row. The gain is *packing*, not better judgement about what matters.
- **The curve is noisy and not monotonic.** Coverage peaks at θ=0.15 rather than
  at the cheapest threshold; storyline coverage wanders between 60% and 100%.
  With 38 important events across 5 storylines the sample is far too small for
  those wiggles to mean anything — the *shape* is the finding, not any point on
  it.

---

## What changed my mind while building this

Results that contradicted the obvious pitch, or my own earlier writeup:

**Before hierarchical episodes were wired in, the LLM made no difference to
recall at all.** recall@20 was identical at every threshold in the sweep — what
surfaced was decided entirely by the cheap scorer and the coalescer, and
enrichment only changed how well it read. Adding episode rollups changed that,
but the mechanism is *packing*, not judgement: episodes fit more events into one
slot. The LLM's contribution to retrieval is entirely second-order.

**Load-aware admission works so well that the load-shedding path never runs.**
At 45.6x measured offered load the peak queue depth is **0** — coalescing and
threshold raising absorb the burst entirely upstream, so the bounded queue's
overflow policies sit idle and the load-aware term never even engages. Good
behaviour, bad testing: there is now a separate scenario that strips
load-awareness and starves capacity purely to prove the shedding order (P3
first, P0 never) actually holds.

**One open cluster per entity meant a busy channel never coalesced.** Keying
clusters on entity is right, but a Slack channel carries several unrelated
conversations at once, so every event closed the previous cluster as
`topic_changed` and the feed filled with near-duplicate one-line rows. Matching
against several concurrent clusters per entity cut PulseFeed's item count from
4,718 to 1,712 on the same trace.

**Fixing that then broke latency, which exposed a design flaw underneath.**
Clusters that used to be closed early by topic changes now lived out their full
window, and since a cluster only reached the feed *when it closed*, p95
time-to-feed went 18s → 45s. Retuning the window traded the grouping back. The
actual bug was that one parameter controlled both grouping quality and
freshness. Publishing a provisional row when a cluster opens and refreshing it
in place as events join decouples them: p50/p95/p99 time-to-feed are now 0.00s
*and* the grouping window is as wide as it wants to be.

**The in-memory event bus silently ignored `min_idle_ms`.** It was written to
mirror the Redis semantics so bugs would surface in tests rather than in
production — and then it handed a consumer's own in-flight batch back to it on
the next loop, double-processing every event. The bus-crash failure scenario
caught it (2,657 events published, 5,146 ingested). Honouring idle time fixed it
to exactly 2,657.

### Learned scorer vs hand-tuned weights

`python -m harness.train` collects features in stream order, splits by time,
fits with inverse-propensity weights, calibrates on a third split, and compares
against the hand-set weights **at equal LLM budget**.

| labels | ECE (hand → learned) | recall@1% (hand → learned) |
|---|---|---|
| ground truth, 5.2k events / 62 pos | 0.047 → **0.009** | 0.72 → 0.56 |
| ground truth, 41k events / 254 pos | 0.046 → **0.007** | 0.89 → 0.88 |
| LLM teacher + audit, 1.6k examples | 0.057 → **0.017** | 0.14 → **0.29** |

- **Calibration improves 5–7x every time**, which the ranking metrics do not
  show and which matters more than they do — the budget-, load- and
  deadline-aware thresholds all read the score as a probability.
- **At 62 positives the hand-tuned prior still wins on ranking.** At 254 the gap
  closes. That is an argument for the continuous production label stream, not
  against learning.
- **On teacher labels — the production shape — the fit doubles recall@1%.**
- The fit independently agrees with the manual diagnosis above: it wants
  `user_relevance` at 3.3–3.8 against the hand-set 1.5.

It also found two defects in the feature set that were invisible until there was
a fit to inspect: `novelty` and `duplication` correlate at exactly **−1.0** (one
is `1 − other`), and `recency` is **constant** because events are scored at
ingest, so the weight it has been carrying does nothing.

The training report ends in a `VERDICT` that refuses to ship a ranking win that
costs calibration. On ground-truth labels it currently says *keep baseline*, and
that is the correct answer.

Deploying a fitted model is one environment variable:

```bash
python -m harness.train --duration 1800 --incidents 12 --out models/scorer.json
PULSEFEED_SCORER_MODEL=models/scorer.json uvicorn pulsefeed.api:create_app --factory
```

A missing or incompatible model file falls back to the hand-tuned weights rather
than failing to start; `/readyz` reports which scorer is live.

**Not yet done:** no training on real traffic and no GPU work. The transformer
path (`learning/encoder.py` — MiniLM embeddings, hybrid embedding+features head)
is written and tested without torch, but never fitted. See DESIGN.md §8c for the
staged plan.

### Failure injection

`python -m harness.failure_injection` — **7/7 scenarios pass.**

| scenario | what is asserted | result |
|---|---|---|
| provider outage | 557 items still published *during* the outage; breaker opens; enrichment resumes after recovery; every event accounted for | pass |
| traffic burst (50x) | offered load 3.3→151.5 events/s (**45.6x**); enrichment rate rises only 0.025→0.235/s; 15,872 events coalesced; **peak queue depth 0** | pass |
| load shedding | load-awareness stripped, capacity starved: P2 and P3 queues saturate, 89 P3 items shed, **P0 and P1 never dropped** | pass |
| slow provider (6s/call, 1 worker) | 82 admissions refused by the deadline policy; breaker stays *closed* (this is the slow path, not the outage path); feed covers every event | pass |
| poison events | secrets never reach the prompt; delimiters defanged; fabricated citations stripped and counted; invented fields dropped | pass |
| broker interruption | 5,017 events buffered and replayed across a 120s outage, zero lost | pass |
| bus crash recovery | a consumer dies holding 100 unacked deliveries; a replacement reclaims all 100; 2,657 published → 2,657 ingested, nothing stranded | pass |

---

## Quick start

No dependencies are required for the core, the tests, or any experiment — it
runs on the standard library.

```bash
cd pulsefeed

python demo.py                      # the worked example, annotated
python -m pytest tests/ -q          # 178 tests
python -m harness.replay            # the A/B/C/D experiment
python -m harness.frontier          # cost-quality sweep
python -m harness.failure_injection # chaos scenarios
python -m harness.train              # fit the cheap scorer, compare to hand-tuned
```

Optional extras:

```bash
pip install -e '.[all]'             # FastAPI, httpx, prometheus-client
uvicorn pulsefeed.api:create_app --factory

# Durable ingestion + persistence, tested against real servers:
pip install -e '.[store]'           # redis, asyncpg
docker compose up -d                # redis + postgres with pgvector
PULSEFEED_TEST_REDIS_URL=redis://localhost:6379/0 \
PULSEFEED_TEST_PG_DSN=postgresql://pulsefeed:pulsefeed@localhost:5432/pulsefeed \
  python -m pytest tests/test_store.py -q
```

Those integration tests skip when the environment variables are unset. They are
not mocked — a mocked integration test that asserts a client library was called
proves nothing about the semantics that matter (ack, reclaim, redelivery,
vector search).

### What the demo shows

15 events go in — a checkout incident spread over four sources, buried in
telemetry, CI noise and chatter. Out comes:

```
1. [error] (LLM, 5 events, score 0.84)
   checkout-service: 5 related events over 459s, starting with "checkout
   latency rises to 700ms"; a rollback was performed; the condition has
   since recovered.
     └─ grafana: checkout latency rises to 700ms
     └─ slack: engineer mentions DB timeout in checkout
     └─ grafana: checkout latency rises to 1.5s
     └─ slack: starting rollback of deployment #813
     └─ ... and 1 more

...

  events ingested       15
  LLM calls made        6
  cost                  $0.0041
```

The wording is mechanical because the demo runs a deterministic mock provider
rather than a real model — see limitations. The *structure* is the point: one
ranked item for the incident, every source event still attached as evidence.

---

## How it works

Six mechanisms, in the order an event meets them.

**1 · Cheap scoring** — a keyword lexicon, source-priority table, hashed
embedding and a ten-term linear model produce `importance`, `novelty`,
`uncertainty`, `risk` plus a P0–P3 scheduling class. No network, no model
server. This runs on 100% of traffic, so its cost bounds the whole system's.

**2 · Coalescing** — same entity + close in time + high similarity → one
cluster. Numbers are bucketed by magnitude so `94%` and `96%` read as the same
fact. Clusters in the *ambiguous* similarity band are flagged
`needs_boundary_check` and get a discounted threshold — a question the cheap
layer has proven it cannot answer is the best possible use of a call.

**3 · Triggering** — `utility = α·importance + β·novelty + γ·uncertainty +
δ·risk − λ·cost`, against a threshold that moves with remaining budget, queue
depth, and whether the answer would still be fresh on arrival. Hard safety
rules bypass all of it; ~1% of *rejections* are audited to measure the
false-negative rate from data.

**4 · Bounded scheduling** — per-class queues, per-class overflow policies,
deficit round robin at 8:4:2:1 (not strict priority — that starves P3 forever).

**5 · Resilient providers** — circuit breaker, retry budget, deadline-aware
retry (a retry landing after the deadline is not a second chance, it is a
second waste), and a fallback chain: hosted model → local vLLM → deterministic
mock.

**6 · Memory and hierarchy** — bounded per-entity state keeps prompt cost flat
as the feed ages; `event → cluster → episode → digest` keeps summarisation
cheap. Corrected conclusions *supersede* rather than overwrite, so "what did
the system believe at 12:05" stays answerable.

**7 · A learned first stage** — the hand-set weights can be replaced by a fitted
logistic model over the same nine features, distilled from the LLM's own
annotations. The hard part is not the model, it is the labels: annotations only
exist for events the trigger admitted, so training on them alone launders the
current scorer's blind spots into learned coefficients. The audit samples — an
unbiased draw from the *rejected* region — are reweighted by inverse propensity
to correct that.

**8 · Durability** — an `EventBus` (Redis Streams) buffers ingestion with
at-least-once delivery, explicit acks and reclaim of work abandoned by dead
consumers; a `PersistenceSink` (Postgres, with pgvector when present) is the
durable record. The bus fails *closed* — buffer and replay, because a lost raw
event is unrecoverable. The sink fails *open* — keep serving and count the
failure, because a delayed write is not.

Full rationale, including the decisions that turned out wrong and why, is in
**[DESIGN.md](DESIGN.md)**.

---

## Limitations

The experiment is worth exactly what its caveats allow.

1. **Traces are synthetic.** Labels are honest — planted by construction,
   never read by PulseFeed, never seen by the provider — but the traffic shape
   was chosen by us.
2. **The mock provider is not a language model.** It does real analysis over
   clusters and entity memory, so the LLM arms' advantage is structural rather
   than rigged, but its prose is mechanical and it cannot be wrong in the
   interesting ways a real model can. **Nothing here measures summary quality.**
3. **Episodes are built from cluster summaries**, so cheap-path events do not
   join them — in the demo, "deployment #813 completed" is causally central but
   sits outside the incident episode.
4. **Precision@20 is worse than LLM-everything's.** The ranker has room.
5. **Serving is still in-process.** Redis Streams and Postgres/pgvector are now
   implemented and tested against real servers, but persistence is
   *write-through*: the in-memory structures remain the read path, and nothing
   reloads them on restart. Durable ingestion and a durable record exist;
   durable *serving* does not.
6. **Single process.** Partitioning by `tenant_id`/`entity_id` is a design
   intention, not running code.

---

## Relationship to the original Timeline-Feed

This repository already contained a TypeScript/Express social-media timeline —
follow graph, hybrid push/pull fanout via Kafka, Redis sorted-set timelines,
MongoDB persistence. That service is **untouched** and still builds; PulseFeed
lives alongside it in `pulsefeed/`.

What carried over is the architectural shape (ingest → queue → worker → store →
read API, with metrics around it) and, usefully, its ranking model: a Redis
sorted set scored by timestamp *is* a chronological feed, which is exactly
experiment arm A. What did not carry over is everything domain-specific — a
follower graph has no analogue when the unit is an event about an entity rather
than a post by a user. Python was chosen because the target stack (asyncio,
FastAPI, pgvector) is Python and the work is I/O-bound orchestration.

---

## Layout

```
pulsefeed/
├── pulsefeed/
│   ├── models.py        Event, EventFeatures, SemanticAnnotation, Summary
│   ├── scoring.py       cheap first-stage scorer
│   ├── embedding.py     dependency-free hashed embeddings
│   ├── coalescer.py     dedup, clustering, ambiguity band
│   ├── trigger.py       fixed / budget / load / deadline / composite policies
│   ├── budget.py        per-tenant ledgers with a P0 reservation
│   ├── scheduler.py     bounded weighted-fair queue
│   ├── memory.py        entity memory
│   ├── summarize.py     hierarchy + supersede semantics
│   ├── pipeline.py      the orchestrator
│   ├── clock.py         real / scaled / virtual / manual clocks
│   ├── metrics.py       Prometheus (optional)
│   ├── api.py           FastAPI (optional)
│   ├── ingest.py        bus-driven ingest worker (consume → ingest → ack)
│   ├── llm/             provider ABC, mock, OpenAI-compatible, breaker, prompts
│   ├── learning/        dataset + IPS weighting, logistic fit, calibration,
│   │                    evaluation, collection, GPU encoder path
│   └── store/           EventBus + PersistenceSink; memory, Redis, Postgres
├── harness/
│   ├── trace.py             seeded traces with ground-truth labels
│   ├── baselines.py         the four arms
│   ├── replay.py            the experiment
│   ├── frontier.py          cost-quality sweep
│   ├── train.py             fit + evaluate the cheap scorer
│   └── failure_injection.py chaos scenarios
├── tests/                   178 tests (+14 needing Redis/Postgres)
├── demo.py
└── DESIGN.md
```
