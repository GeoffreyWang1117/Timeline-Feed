# Architecture

## 1. The shape

```
                    ┌──────────────────────────────────────────────┐
  producers ───────▶│  EventBus (Redis Streams)                    │
                    │  at-least-once · explicit ack · bounded       │
                    └───────────────────┬──────────────────────────┘
                                        │  IngestWorker
                                        ▼
┌───────────────────────────────────────────────────────────────────────┐
│ INGEST PATH — no network, never blocks on the LLM                     │
│                                                                       │
│  CheapScorer          9 features on 100% of traffic                   │
│       │               importance · novelty · uncertainty · risk       │
│       ▼                                                               │
│  Coalescer            same entity + close in time + similar text      │
│       │               emits a provisional row the moment it opens     │
│       ▼                                                               │
│  Trigger              utility vs a threshold that moves with          │
│       │               budget, queue depth, freshness deadline         │
│       ├──────────────────────────┐                                    │
│  cheap path                 LLM path                                  │
└───────┼──────────────────────────┼────────────────────────────────────┘
        │                          ▼
        │              ┌───────────────────────────┐
        │              │ BoundedPriorityScheduler  │  P0..P3, DRR 8:4:2:1
        │              │ per-class caps + policies │  bounded, never grows
        │              └────────────┬──────────────┘
        │                           ▼
        │              ┌───────────────────────────┐
        │              │ ResilientProvider         │  breaker · retry budget
        │              │ hosted → local → mock     │  deadline-aware retry
        │              └────────────┬──────────────┘
        │                           ▼
        │              SemanticAnnotation (cites event ids; never mutates them)
        │                           │
        └───────────┬───────────────┘
                    ▼
        ┌───────────────────────────┐        ┌────────────────────────┐
        │ EntityStore               │◀──────▶│ PersistenceSink        │
        │ bounded per-entity state  │        │ Postgres + pgvector    │
        └───────────┬───────────────┘        │ write-through, fail-open│
                    ▼                        └────────────────────────┘
        ┌───────────────────────────┐
        │ SummaryStore + Rollup     │  event → micro → cluster → episode → digest
        │ supersede, never overwrite│
        └───────────┬───────────────┘
                    ▼
              Timeline (ranked, every row expandable to raw events)
```

## 2. Two invariants

Everything else is a consequence of these.

### 2.1 Raw events are canonical; model output is annotation

> A raw `Event` is truth. Anything a model produces is a `SemanticAnnotation`
> that *references* events by id and never mutates or replaces them.

Consequences:

- A hallucinated summary is a wrong opinion attached to intact facts. Every row
  expands back into its evidence (`GET /v1/items/{id}/evidence`).
- `source_event_ids` is validated as a **subset** of the ids supplied to that
  call. A model cannot attribute its output to events it was never shown,
  including another tenant's. Fabricated ids are stripped *and counted*.
- Losing the provider costs summaries, never events.

### 2.2 Ingestion never waits on the LLM

`ingest()` scores, coalesces, decides, and returns. It opens no sockets.

A cluster that is *not* admitted becomes a timeline row immediately. A cluster
that *is* admitted also becomes one immediately — enrichment upgrades the row in
place when it lands. The feed is therefore complete at every instant, and the
model only ever changes how well it reads.

## 3. The path of one event

| stage | what happens | cost |
|---|---|---|
| **bus** | published, durably buffered, delivered at least once | one Redis append |
| **score** | 9 named features, no network | ~microseconds |
| **persist** | write-through to Postgres, fail-open | one insert, off the read path |
| **coalesce** | matched against open clusters for its entity | one cosine per open cluster |
| **publish** | provisional row appears in the feed | dict write |
| **trigger** | utility vs threshold; safety rules bypass; ~1% of rejections audited | arithmetic |
| **queue** | admitted work enters a bounded per-class queue | O(1) |
| **enrich** | one LLM call over the cluster + entity memory | the expensive part |
| **annotate** | validated, stored, folded into entity state, row upgraded | — |
| **roll up** | periodically rebuilt into an episode | one cheap LLM call per changed episode |

## 4. Components

### 4.1 `CheapScorer` — the only thing that touches 100% of traffic

A source-priority table, a keyword lexicon, a hashed embedding, and a nine-term
logistic model. No LLM, no network, no model server.

This is the design answer to *"why not let the LLM score everything?"*: the
first stage's cost multiplies by total event volume, so it bounds the system's
throughput ceiling, its latency floor, and its availability. It must stay cheap
even when that means being dumb, because everything it cannot settle is
escalated to something that is not.

Outputs `importance`, `novelty`, `uncertainty`, `risk`, plus a P0–P3 scheduling
class. **Uncertainty is not a confidence score to be maximised** — it is an
input to spending. High uncertainty means the cheap layer *cannot* answer, and
those are the calls with the best expected return.

Swappable: `CheapScorer(model=...)` takes any `ScoreModel`, including a fitted
one (see [training.md](training.md)).

### 4.2 `Coalescer` — dedup before the trigger, not after

Four "CPU above 94%" readings are one fact. Sending them to a model four times
buys four copies of the same sentence.

Grouping uses only cheap signals: same entity, close in time, high embedding
similarity. Numbers are bucketed by magnitude so `94%` and `96%` read as the
same fact rather than as two novel ones.

Three similarity bands:

| similarity | action |
|---|---|
| ≥ 0.80 | confidently the same thing — merge silently |
| 0.58 – 0.80 | **merge, but flag `needs_boundary_check`** |
| < 0.58 | confidently different — open a separate cluster |

The middle band is the single best use of an LLM call in the system: a question
the cheap layer has *proven* it cannot answer ("is this the same incident, or a
second one?"). Those clusters get a discounted threshold at the trigger.

Several clusters stay open per entity, because one entity — a busy Slack
channel — carries several unrelated conversations at once.

### 4.3 Trigger — the actual idea

```
utility = α·importance + β·novelty + γ·uncertainty + δ·risk − λ·cost
invoke if utility > θ(budget, load, deadline)
```

Four composable policies:

| policy | θ depends on | why |
|---|---|---|
| fixed | nothing | baseline to measure the others against |
| budget-aware | remaining daily allowance | the day's last tokens go to the day's best events |
| load-aware | queue depth (quadratic) | permissive when idle, decisive when saturated |
| deadline-aware | estimated wait vs freshness window | refuses work whose answer would arrive dead |

**Composition rule: the maximum threshold binds, and an explicit veto is final.**

Two escape hatches sit outside the calculation entirely:

- **Hard safety rules** bypass every threshold (P0, risk ≥ 0.95, explicit
  escalation). Thresholds are statistical; incidents are not.
- **Audit sampling** enriches ~1% of *rejected* clusters and discards the
  results, purely so the false-negative rate is measured rather than assumed.
  Without it, a first stage that quietly degrades looks exactly like one that
  works.

### 4.4 `BoundedPriorityScheduler` — bounded, and fair

10,000 events/min arriving against ~100 events/min of capacity. Queueing the
other 9,900 turns a throughput problem into an unbounded-memory problem *and* an
unbounded-latency one.

| class | overflow behaviour |
|---|---|
| P0 critical incident | reserved lane; never shed for load |
| P1 direct user action | queued; falls back to the cheap path when full |
| P2 normal project event | triggers harder coalescing before it fills |
| P3 telemetry | dropped first, and dropped quietly |

Dispatch is **deficit round robin at 8:4:2:1, not strict priority.** Strict
priority is the intuitive choice and it is wrong: a sustained P0 stream starves
everything below it forever.

### 4.5 `ResilientProvider` — an unreliable dependency, bounded

Circuit breaker, retry budget, deadline-aware retry, and a fallback chain of
hosted model → local vLLM → deterministic mock.

The load-bearing idea: **a retry is not automatically useful work.** Retrying a
request whose freshness window has closed spends a worker slot producing
something nobody wants, while live events wait behind it. Every attempt
re-checks the deadline.

### 4.6 `EntityStore` — state, not log

A feed that is only a list makes the model re-read history to answer "is this
new?" every time. Instead each entity carries a small bounded record: current
state, recent events, last conclusion, severity.

The model receives *the current cluster plus that entity's memory*, never the
timeline. Token cost per call is therefore flat in the age of the feed, while
the model still gets continuity.

Entity state also feeds **ranking**, applied at read time: an event that looked
routine when it arrived becomes interesting the moment its service is declared
degraded, and that verdict usually lands after the row was written.

### 4.7 Hierarchy and supersede

```
raw event → micro → cluster → episode → hourly → daily
```

Never one giant prompt. Each level summarises the level below, so a daily digest
is a handful of calls over already-compressed text, and every node keeps
`source_event_ids` so any line expands back to raw facts.

Episodes stay **open** while their entity keeps producing material; each pass
rebuilds the episode over the full window and supersedes the previous version,
so the story grows in place. Cheap-path events on an entity with an open episode
emit a micro summary so they can join it — that is how "deployment #813
completed", lexically dull and causally central, ends up inside the incident
about rolling it back.

When the system was **wrong** — 12:05 "suspected database issue", 12:25 "root
cause confirmed: DNS" — the old summary is not overwritten. It is marked
`SUPERSEDED`, linked to its replacement, and kept. The correction inherits the
original's source events, because a corrected root cause still has to explain
the evidence that produced the wrong one.

### 4.8 Storage — two interfaces, opposite failure policies

| | `EventBus` | `PersistenceSink` |
|---|---|---|
| role | ingestion boundary | durable record |
| implementation | Redis Streams | Postgres (+ pgvector) |
| on failure | **fails closed** — buffer and replay | **fails open** — keep serving, count it |
| why | a lost raw event is unrecoverable | a delayed write is recoverable |

Persistence is **write-through**: the in-memory structures are the serving path,
and the sink is the record behind them. Putting a database on the read path of a
feed whose entire premise is surviving its dependencies would be self-defeating.

`pipeline.restore(tenant)` rebuilds serving state after a restart — events (so
restored rows can still expand to evidence), entity memory (so ranking stays
aware of what is on fire), and *active* summaries. Superseded beliefs stay on
disk for the audit trail and are not resurrected.

## 5. Security posture

Feed content is hostile by definition — anyone who can post in a watched channel
can put text in front of the model. The defences are structural, not persuasive:

- events render as delimited **data**, with delimiter-like sequences in content
  defanged (`</event>` → `‹/event>`);
- the untrusted-data framing is repeated in the user message adjacent to the
  data, not left to the system prompt alone;
- secrets are redacted on the way **in**, so they never reach the provider;
- the response schema is fixed and validated — unknown enums fall back, numbers
  clamp, lists bound, model-invented fields are dropped;
- **no tools are exposed**, so there is nothing to call. Actions are
  *recommended* (`actionability`), never executed.

The worst outcome of a successful injection is a wrong summary over correct,
inspectable events.

## 6. Concurrency and time

The pipeline is single-event-loop asyncio: the workload is Redis, Postgres and
HTTP, all I/O-bound, so threads would buy contention rather than throughput.

All time is read through a `Clock`:

| clock | used by |
|---|---|
| `RealClock` | production |
| `VirtualClock` | the replay harness — discrete-event simulation, exact simulated durations, runs as fast as the CPU allows |
| `ScaledClock` | wall-clock demos |
| `ManualClock` | unit tests |

`VirtualClock` imposes one rule on the whole codebase: **nothing may call
`asyncio.sleep` with a nonzero duration directly.** All waiting goes through the
clock, otherwise a task stays permanently runnable and simulated time can never
conclude that everything is parked. (This forced one real improvement: the
scheduler's `dequeue` waits on an event instead of polling with a timeout, which
is better under real time too.)

## 7. Limitations

1. **Single process.** Partitioning by `tenant_id`/`entity_id` is a design
   intention, not running code.
2. **Serving state is memory-bounded.** `restore()` reloads a capped window, not
   the entire history.
3. **Ranking trails LLM-everything at K=20** (81.6% vs 94.7%). Diagnosed in
   DESIGN.md §8a.
4. **`novelty` and `duplication` are perfectly collinear; `recency` is
   constant.** Found by the training diagnostics, deliberately not removed —
   that changes `FEATURE_NAMES` and deserves a migration.
5. **The learned scorer has never seen real traffic or a GPU.**
