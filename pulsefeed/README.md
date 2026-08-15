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

| arm | LLM calls | tokens | cost | recall@20 | recall(all) | enriched cov | storylines |
|---|---|---|---|---|---|---|---|
| A. Chronological | 0 | 0 | $0.0000 | 7.9% | 100% | 0% | 0% |
| B. Rule + embedding | 0 | 0 | $0.0000 | 55.3% | 100% | 0% | 0% |
| C. LLM-everything | 4,905 | 2,218,794 | $3.3282 | 73.7% | 100% | 65.8% | 100% |
| **D. PulseFeed** | **72** | **37,915** | **$0.0569** | **76.3%** | 100% | 60.5% | 80% |

### Recall as the page grows

| arm | R@10 | R@20 | R@50 |
|---|---|---|---|
| A. Chronological | 7.9% | 7.9% | 10.5% |
| B. Rule + embedding | 26.3% | 55.3% | 79.0% |
| C. LLM-everything | 44.7% | 73.7% | 94.7% |
| **D. PulseFeed** | **76.3%** | **76.3%** | 89.5% |

### System behaviour under the same load

| arm | p95 e2e | p99 queue wait | max queue | shed as stale |
|---|---|---|---|---|
| A. Chronological | 0.00s | 0.00s | 0 | 0 |
| B. Rule + embedding | 18.30s | 0.00s | 0 | 0 |
| C. LLM-everything | 67.30s | 74.31s | 1,748 | 1,500 |
| **D. PulseFeed** | 18.43s | **0.00s** | **1** | **0** |

**Reading these honestly:**

- **98.5% fewer LLM calls than enriching everything (72 vs 4,905), at slightly
  better recall@20** (76.3% vs 73.7%) and 1.7% of the cost.
- **PulseFeed's advantage is sharpest on a small page.** At K=10 it reaches
  76.3% where LLM-everything reaches 44.7%, because one episode item carries a
  whole incident while LLM-everything spends ten slots on ten separate lines.
- **LLM-everything cannot keep up.** Its queue reaches 1,748 items, p99 wait
  74s, and 1,500 items are shed as stale before they are ever served. Its
  higher R@50 is real, but it is buying it at 58x the cost with a collapsed
  tail.
- **PulseFeed's precision@20 (50%) is lower than LLM-everything's (80%).** With
  episodes packing many events per slot, fewer slots need to be relevant to
  reach high recall — the remainder is noise that a better ranker should
  displace. This is a genuine weakness, not a rounding artifact.
- **PulseFeed covers 80% of planted storylines against LLM-everything's 100%.**
  Admission control does miss things. That is the trade being made, and it is
  measured rather than asserted.
- **With no LLM at all** (arm B — the exact behaviour when the provider is
  down) the feed still reaches 55.3% recall@20 and 100% total coverage.

### Cost–quality frontier

`python -m harness.frontier` sweeps the utility threshold from 0.05 to 0.95 on
one fixed trace, holding every other component constant.

| θ | LLM calls | cost | enriched cov | cov/call | recall@20 |
|---|---|---|---|---|---|
| 0.05 | 185 | $0.1365 | 63.2% | 0.34% | 100.0% |
| 0.15 | 84 | $0.0644 | 65.8% | 0.78% | 100.0% |
| 0.35 | 34 | $0.0291 | 60.5% | 1.78% | 73.7% |
| 0.55 | 28 | $0.0239 | 50.0% | 1.79% | 73.7% |
| 0.75 | 15 | $0.0119 | 28.9% | 1.93% | 65.8% |
| 0.95 | 15 | $0.0119 | 28.9% | 1.93% | 65.8% |

- The threshold is a **12x spend dial** on one unchanged trace, and marginal
  returns fall about 10x from the cheap end (1.93% coverage per call) to the
  expensive one (0.20% for the marginal call between the extremes).
- **Even at θ=0.95 — admit essentially nothing — 15 calls still happen.** Those
  are the hard safety-rule bypasses. Critical events are never subject to the
  budget dial.
- **The curve is noisy and not monotonic.** Coverage peaks at θ=0.15 rather than
  at the cheapest threshold; storyline coverage wanders between 60% and 100%.
  With 38 important events across 5 storylines the sample is far too small for
  those wiggles to mean anything — the *shape* is the finding, not any point on
  it.

---

## What changed my mind while building this

Two results worth recording because they contradict the obvious pitch:

**Before hierarchical episodes were wired in, the LLM made no difference to
recall at all.** recall@20 was identical at every threshold in the sweep — what
surfaced was decided entirely by the cheap scorer and the coalescer, and
enrichment only changed how well it read. Adding episode rollups changed that
(recall@20 now spans 65.8%→100%), but the mechanism is *packing*, not
judgement: episodes fit more events into one slot. The LLM's contribution to
retrieval is entirely second-order.

**Load-aware admission works so well that the load-shedding path never runs.**
At 50x offered load the peak queue depth is 1 — coalescing and threshold
raising absorb the burst upstream, so the bounded queue's overflow policies sit
idle. Good behaviour, bad testing: there is now a separate scenario that strips
load-awareness and starves capacity purely to prove the shedding order (P3
first, P0 never) actually holds.

### Failure injection

`python -m harness.failure_injection` — **6/6 scenarios pass.**

| scenario | what is asserted | result |
|---|---|---|
| provider outage | feed keeps producing; breaker opens; enrichment resumes after recovery | pass |
| traffic burst (50x) | offered load 3.3→151 events/s; invocation rate stays flat; peak queue depth **1**; every important event still surfaces | pass |
| load shedding | with load-awareness stripped and capacity starved: P2/P3 queues saturate, 323 P3 items shed, **P0 and P1 never dropped** | pass |
| slow provider (6s/call) | 2,457 admissions refused by the deadline policy; breaker stays *closed* (this is the slow path, not the outage path); feed covers every event | pass |
| poison events | secrets never reach the prompt; delimiters defanged; fabricated citations stripped and counted; invented fields dropped | pass |
| broker interruption | 5,017 events buffered and replayed across a 120s outage, zero lost | pass |

---

## Quick start

No dependencies are required for the core, the tests, or any experiment — it
runs on the standard library.

```bash
cd pulsefeed

python demo.py                      # the worked example, annotated
python -m pytest tests/ -q          # 98 tests
python -m harness.replay            # the A/B/C/D experiment
python -m harness.frontier          # cost-quality sweep
python -m harness.failure_injection # chaos scenarios
```

Optional extras:

```bash
pip install -e '.[all]'             # FastAPI, httpx, prometheus-client
uvicorn pulsefeed.api:create_app --factory
```

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
5. **Storage is in-process.** Redis Streams and Postgres/pgvector sit behind
   interfaces but are not implemented; there is no durability across restarts.
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
│   └── llm/             provider ABC, mock, OpenAI-compatible, breaker, prompts
├── harness/
│   ├── trace.py             seeded traces with ground-truth labels
│   ├── baselines.py         the four arms
│   ├── replay.py            the experiment
│   ├── frontier.py          cost-quality sweep
│   └── failure_injection.py chaos scenarios
├── tests/                   98 tests
├── demo.py
└── DESIGN.md
```
