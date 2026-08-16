# PulseFeed — design notes

Companion to the README. This is the "why is it built this way" document: the
decisions that were genuinely contested, and what would have to change to undo
them.

---

## 0. What this is, relative to the original project

The repository already contained a Timeline Feed: a TypeScript/Express service
with MongoDB, Redis and Kafka implementing a social-media home feed —
follow graph, hybrid push/pull fanout, Redis sorted-set timelines keyed by
timestamp, cursor pagination, rate limiting, Prometheus metrics. About 6.3k
lines, and a competent implementation of a well-understood problem.

What carries over is the *shape*: ingest → queue → worker → store → read API,
with metrics and containers around it. Also, usefully, the ranking model —
`ZADD timeline:user <timestamp> <postId>` is a chronological feed, which is
precisely experiment arm A.

What does not carry over is everything domain-specific. The follow graph, the
celebrity/normal fanout split, the post model — none of it has an analogue in
an event-stream feed where the unit is an *event about an entity*, not a *post
by a user followed by a reader*. So PulseFeed is a new service alongside the
old one rather than a refactor of it, in Python because the plan's target stack
(asyncio, FastAPI, pgvector) is Python and because the interesting work here is
I/O-bound orchestration.

The legacy TypeScript service is untouched and still builds.

---

## 1. Why the first stage is not an LLM

The first stage runs on 100% of traffic. Its cost, latency and failure
behaviour therefore bound the entire system's — whatever it is, the system can
never ingest faster than it, never be cheaper than it, and is never more
available than it.

So the first stage is a keyword lexicon, a source-priority table, a hashed
embedding and a ten-term linear model. It is not good at nuance. It does not
have to be: its only job is to be *right about what is obviously boring* and to
know when it is unsure. Everything it cannot settle is escalated.

The linear model is deliberately shaped like a fitted logistic regression with
hand-set coefficients (`ScorerWeights`), so replacing intuition with training
data is a change to ten numbers rather than a rewrite.

**Uncertainty deserves special mention.** It is not a confidence score to be
maximised; it is an input to spending. High uncertainty means the cheap layer
*cannot* answer, and those are the calls with the best expected return. A
system that only escalates high-importance events spends its budget confirming
things it already knew.

---

## 2. The trigger is a control problem, not a filter

    utility = α·importance + β·novelty + γ·uncertainty + δ·risk − λ·cost
    invoke if utility > θ(budget, load, deadline)

The threshold is a function, not a constant, and that is the whole idea. Four
policies compose:

| policy | θ depends on | why |
|---|---|---|
| fixed | nothing | baseline to measure the others against |
| budget-aware | remaining daily allowance | the day's last tokens go to the day's best events |
| load-aware | queue depth (quadratic) | permissive when idle, decisive when saturated |
| deadline-aware | estimated wait vs freshness window | refuses work whose answer would arrive dead |

Composition rule: **the maximum threshold binds, and an explicit veto is
final.**

That second clause was a bug before it was a rule. The first version treated
"this sub-policy said no" as a veto, which collapsed the composite into
whichever sub-policy happened to be strictest and silently rejected ~48% of all
clusters. Sub-policies now raise a `veto` flag for genuine categorical refusals
(only the deadline policy does), and ordinary below-threshold answers just
contribute their threshold to the max. There is a regression test named after
the failure.

**Deadline-awareness is the policy that makes this a decision system rather
than a queue.** An incident summary produced four minutes after the incident
resolved is not partially valuable — it is worthless, and the tokens it burned
came out of something still live. The same logic governs retries: a retry that
lands after the deadline is not a second chance, it is a second waste. Both
`ResilientProvider.enrich` and the scheduler re-check deadlines rather than
assuming work stays worth doing.

### The two escape hatches

Thresholds are statistical and incidents are not. Two mechanisms sit outside
the utility calculation entirely:

**Hard safety rules** bypass every threshold. P0 priority, risk ≥ 0.95, or an
explicit `force_enrich` flag get through regardless of budget, load or score.
The frontier sweep shows this working: at θ=0.95 — admit essentially nothing —
LLM calls plateau at a nonzero floor, and those are the bypasses.

**Audit sampling** enriches ~1% of *rejected* clusters and throws the results
away. They never reach a user's timeline (a test enforces this — if audit
output leaked into the feed, the measurement would be measuring itself). Their
only purpose is to estimate the false-negative rate from data, because a
first-stage scorer that quietly degrades looks *exactly* like one that is
working. Without this, "we don't miss important events" is an assertion.

---

## 3. Coalescing, and the ambiguity band

Four "CPU above 94%" readings are one fact. Sending them to an LLM four times
buys four copies of the same sentence.

Cheap grouping: same entity, close in time, high embedding similarity. The
embedder buckets numbers by magnitude (`94%` and `96%` both become `<num1>`),
without which every telemetry sample looks novel and coalescing never fires.

The interesting part is the middle. Three bands:

- similarity ≥ 0.80 — confidently the same thing, merge silently
- 0.58 – 0.80 — **merge, but flag `needs_boundary_check`**
- < 0.58 — confidently different, close the cluster and start a new one

The middle band is the best possible use of an LLM call: a question the cheap
layer has *proven* it cannot answer ("is this the same incident, or a second
one?"). Those clusters get a discounted threshold in the trigger.

Two things force a cluster out early regardless of its window: a P0 member (a
SEV1 must not sit in a batching window — this was a bug, caught by a test that
checked a P0 *opening* a cluster rather than joining one), and reaching
`max_cluster_size` (holding a full cluster open buys latency and nothing else,
which is also what makes `max_cluster_size=1` a clean no-coalescing baseline).

Under load the coalescer widens its windows instead of the queue growing. This
degrades *resolution*, not *coverage* — the same events, described in fewer,
coarser items. It is the first thing given up, long before anything is dropped.

### Several clusters open per entity

Keying clusters on entity is right; keeping only *one* open per entity was not.
A busy Slack channel is a single entity carrying several unrelated conversations
at once, so with one open cluster every arriving event was dissimilar to the
current one, closed it as `topic_changed`, and opened its own — and nothing ever
coalesced. The feed filled with near-duplicate single-line rows: five separate
"sharing a good article on distributed tracing" items in one page.

A new event is now matched against *all* clusters open for its entity and joins
the best one, with a bounded number (`max_open_per_entity`) and the stalest
evicted when that bound is hit. On the standard trace this cut PulseFeed's item
count from 4,718 to 1,712 and lifted R@50 from 89.5% to 97.4%.

### Publishing early, grouping late

The fix above immediately broke something else, and the breakage was more
interesting than the fix. Clusters that used to be closed early by topic changes
now lived out their full window — and because a cluster reached the feed only
*when it closed*, p95 time-to-feed went from 18s to 45s. Narrowing the window
gave the latency back and took the grouping away with it.

The real problem was that `window_seconds` controlled two unrelated things:
how well events group, and how long a reader waits to see anything. Those want
opposite values.

They are now decoupled. `drain_touched()` hands the pipeline a provisional
snapshot every time a cluster opens *or grows*, and the pipeline writes or
refreshes that cluster's single row in place. An event is visible the moment it
is ingested; the window becomes purely a grouping parameter. Measured
time-to-feed went to 0.00s at p50, p95 *and* p99, with the wide window and its
grouping quality kept.

One subtlety worth recording: the first version of this published only on
*open*. p95 barely moved, because every event that joined an existing cluster
still waited for the close. Covering growth as well as opening was the actual
fix.

---

## 4. Raw events are canonical; LLM output is annotation

Stated as an invariant because everything else leans on it:

> A raw `Event` is truth. Anything a model produces is a `SemanticAnnotation`
> that *references* events by id and never mutates or replaces them.

Consequences:

- A hallucinated summary is a wrong opinion attached to intact facts. Every
  item expands back to its evidence (`GET /v1/items/{id}/evidence`).
- `source_event_ids` is validated as a **subset** of the ids supplied to that
  call. A model cannot attribute its output to events it was never shown,
  including another tenant's. Fabricated ids are stripped *and counted*.
- Losing the provider costs summaries, never events.

### Prompt injection

Feed content is hostile by definition — anyone who can post in a watched Slack
channel can put text in front of the model. The defences are structural, not
persuasive:

- events render as delimited data, with delimiter-like sequences in content
  defanged (`</event>` → `‹/event>`);
- the untrusted-data framing is repeated in the user message adjacent to the
  data, not left to the system prompt alone — injections routinely exploit the
  distance between a rule at the top of a context and hostile text far below it
  (this gap was found by a test);
- secrets are redacted on the way *in*, so they never reach the provider;
- the response schema is fixed and validated: unknown enums fall back, numbers
  clamp, lists bound, model-invented fields are dropped;
- no tools are exposed, so there is nothing to call. Actions are
  *recommended* (`actionability`), never executed.

The worst outcome of a successful injection is a wrong summary over correct,
inspectable events.

---

## 5. Bounded, weighted-fair scheduling

10,000 events/min arriving against ~100 events/min of LLM capacity. Queueing
the other 9,900 converts a throughput problem into an unbounded-memory problem
*and* an unbounded-latency one.

Per-class bounds with per-class overflow policies:

| class | overflow behaviour |
|---|---|
| P0 critical | reserved lane; never shed for load |
| P1 user action | queue; falls back to the cheap path when full |
| P2 normal | triggers harder coalescing before it fills |
| P3 telemetry | dropped first, and dropped quietly |

Dispatch is **deficit round robin at 8:4:2:1, not strict priority.** Strict
priority is the intuitive choice and it is wrong: a sustained P0 stream starves
everything below it forever. Weighted fairness gives the important classes most
of the capacity while guaranteeing every class a floor. There is a test that
fails under strict priority.

An observation worth stating plainly, because the failure-injection results
show it: **with load-aware admission enabled, the bounded queue never
overflows.** At a measured 45.6x offered load (3.3 → 151.5 events/s) the peak
queue depth is *zero* — coalescing folded 15,872 events away and the utility
threshold refused the rest, so the load-aware term never engaged and the
overflow policies never ran. That is good behaviour and bad testing, so there
is a separate scenario that disables load-awareness and starves capacity purely
to exercise the shedding path and confirm the ordering (P3 sheds 89 items, P2
falls back to the cheap path, P0 and P1 are never dropped).

---

## 6. Entity memory bounds prompt cost

A feed that is only a list makes the model re-read history to answer "is this
new?" every time. Instead each entity — Checkout Service, Deployment #813 —
carries a small bounded record: current state, recent events, last conclusion,
severity.

The model receives *the current cluster plus that entity's memory*, never the
timeline. Token cost per call is therefore flat in the age of the feed, while
the model still gets continuity because memory carries forward what previous
calls concluded. The store is LRU-bounded; an unbounded memory is an unbounded
prompt budget.

---

## 7. Hierarchy and supersede

    raw event → micro → cluster → episode → hourly → daily

Never one giant prompt. Each level summarises the level below, so a daily
digest is a handful of calls over already-compressed text, and every node keeps
`source_event_ids` so any line expands back to raw facts.

Episodes are **open** while their entity keeps producing material. The first
implementation rolled up only summaries it had not seen yet, which fragmented
one incident into one episode per rollup pass — worse than not rolling up at
all. Each pass now rebuilds the episode over the full window and supersedes the
previous version, so the story grows in place. A signature check means an
unchanged episode is never re-narrated, so a quiet tick costs nothing.

### Letting cheap events into the story

Episodes were built only from LLM-written cluster summaries, so an event that
never earned a call could not be part of the story it belonged to. In the demo
that meant "deployment #813 completed" — lexically boring, causally central —
sat as its own row *outside* the incident episode about rolling back deployment
#813.

A cheap cluster now emits a MICRO summary, and so becomes eligible for rollup,
**only if its entity already has an open episode.** The rule is narrow on
purpose: emitting one for every cheap cluster would put the whole firehose into
the candidate set and turn episodes into digests of noise. "This entity is in
the middle of a story" is the cheapest available evidence that an otherwise-dull
event belongs in it.

Measured effect on the standard trace: rows to reach 80% important-event recall
fell from 19 to **9**, R@10 rose 76.3% → 84.2%, at a cost of three extra LLM
calls (48 → 51).

This also exposed a bug in the *metric*. `enriched_important_coverage` was
computed over the visible feed, so absorbing enriched rows behind an episode
made real coverage rise from 60.5% to 65.8% while the measurement *fell* to
47.4% — the enrichment had merely been tidied away under a parent. Coverage is
now measured over the expanded feed. Summarising well should not score as losing
enrichment.

### Supersede

And when the system was **wrong** — 12:05 "suspected database issue", 12:25
"root cause confirmed: DNS" — the old summary is not overwritten. It is marked
`SUPERSEDED`, linked to its replacement, and kept. The correction inherits the
original's source events, because a corrected root cause still has to explain
the evidence that produced the wrong one. "What did the system believe at
12:05, and why" stays answerable.

Contradiction detection is deliberately conservative (shared evidence plus a
severity shift of ≥2 ranks, or an explicit correction link). A missed supersede
leaves a stale summary visible, which is bad; a false supersede destroys a
correct one, which is worse.

---

## 7a. The system did not live up to its own thesis, in twelve places

The thesis is boundedness: bounded queues, bounded entity memory, bounded
prompts. An audit prompted by the question "what *else* grows with event
count?" found twelve per-event structures with no cap at all — the event map,
the annotation map, five index maps, three timing maps, both latency sample
lists, the scorer's per-entity burst windows (one deque per entity ever seen),
the scheduler's wait samples, and the summary store.

Every finite harness run looked identical to a bounded system. That is the
uncomfortable part: **a structure that grows forever and a structure that is
bounded are indistinguishable in any test that ends.** Only an explicit cap
assertion separates them, so the fix started with a test that ingests several
multiples of the retention cap and asserts each structure held less than all of
it (`tests/test_bounded_memory.py`).

The bounds themselves are deliberately boring — oldest-first eviction, one pop
per insert, amortised O(1) — because the interesting choices are what eviction
*means* at each site:

- Evicting an old **event** degrades `evidence_for` for ancient rows (which
  already tolerated missing ids) and nothing else; the durable record in the
  sink is unaffected.
- Evicting a **burst window** just resets that entity's burst score.
- The **summary store** evicts superseded summaries first — already replaced in
  the live feed, the cheapest possible loss — and the full history stays in the
  sink.
- **Latency samples** became rolling windows, which is also the more useful
  number operationally.

The audit also found `_rolled_up`, a set that was added to on every rollup and
never read. Deleted rather than bounded.

---

## 7b. Auth, rate limiting, and the digest cap-stone

Three late additions that close documented gaps rather than break new ground;
the design choices worth recording:

**The key decides the tenant.** With `PULSEFEED_API_KEYS` set, a key bound to
`acme` acts as `acme` no matter what the request body claims — the claim is
*contained*, not rejected, because rejecting turns every misconfigured producer
into an outage while overriding merely keeps it inside its own box. Isolation
becomes a property of the credential instead of a request-body honour system.
Auth precedes rate limiting, so a 401 cannot burn a tenant's token bucket; key
comparison is `hmac.compare_digest` over a scan rather than a dict lookup,
because at tens of keys the scan is free and the dict lookup is a timing side
channel. With no keys configured everything is open — the dev default — and
`/readyz` says `"disabled (dev mode)"` out loud, because an internet-facing
deployment running open must not look identical to a working setup.

**Rate limits are per (tenant, operation class)** token buckets, bounded like
everything else (the bucket map evicts LRU past a cap — the limiter must not be
the leak). Batch elements are charged individually; a batch is not a way around
the per-event rate.

**The digest is a read, so it does not write.** `GET /v1/digest` builds the
hourly/daily rollup on demand — episodes in the window first, then leaves not
already absorbed by one, so nothing is told twice — and does *not* persist by
default: a dashboard polling every 30 seconds must not append 2,880 summaries a
day to the audit trail. A scheduled job that wants the digest on the record
passes `persist=True`. Extractive by default, LLM narration opt-in behind the
same budget gate as episodes.

---

## 8. Simulated time, and why the first attempt was wrong

The harness needs to replay 15 minutes of traffic in seconds while reporting
latency in meaningful units.

**First attempt — `ScaledClock`:** real time divided by a scale factor. It does
not survive contact with a real trace. Every `asyncio.sleep` overshoots by
roughly a millisecond of scheduling overhead, and at 50x that millisecond is 50
simulated milliseconds. Across thousands of events the simulated clock ran far
ahead of the trace, events began arriving "after" their own freshness
deadlines, and ~48% of clusters were rejected as stale. The measurements were
corrupt in a way that looked like a policy result.

**Second attempt — `VirtualClock`:** discrete-event simulation. Time moves only
when nothing is runnable. `sleep()` parks the caller on a heap keyed by wake
time; a driver drains the ready queue and, when every task is parked, jumps the
clock to the earliest wake time. Simulated durations are exact, the run goes as
fast as the CPU allows, and there is no scale factor to tune.

The requirement it imposes: nothing under test may call `asyncio.sleep` with a
nonzero duration directly. That forced one real change — the scheduler's
`dequeue` used a timeout-poll, which kept workers permanently runnable and made
"everything is parked" unprovable. It now waits on an event. That is a better
design under real time too.

`ScaledClock` is kept for wall-clock demos; `RealClock` is what production
uses.

---

## 8a. Ranking, and two things it was getting wrong

Ranking is the weakest part of the system and the diagnosis is worth keeping
because both faults were invisible in the aggregate metrics and obvious the
moment the top 40 rows were printed with their scores.

**User relevance was decaying with novelty.** `@alice can you review this?`
scored 0.400 at rank 4 and 0.251 at rank 34 — identical intent, very different
score. `user_relevance` feeds into `importance`, but so does novelty, so the
fifth similarly-worded mention looked less important than the first. Being
addressed directly is categorical; it does not get less relevant because the
phrasing is stale. It is now applied outside `importance` and weighted heavily.

**Entity memory was never consulted when ranking.** "deployment #813 completed"
is lexically boring and a keyword scorer has no way to know better — but the
same sentence about a service *currently in a degraded state* is the most
interesting line in the feed. The state was already tracked and used only for
prompting. It now contributes a boost, applied **at read time** rather than at
publish time, because the verdict that a service is degraded usually lands after
the routine-looking event was already written.

Both helped, and neither closed the gap: at K=20 PulseFeed still trails
LLM-everything (81.6% vs 94.7%), with relevant items at ranks 1–7 and then not
again until 24. Ranks 8–23 are still occupied by novel-but-worthless chatter.
Novelty is doing too much work in `importance` and the fix is probably a trained
classifier rather than more hand-tuned weights.

---

## 8c. Learning the first stage

§8a ended with "the fix is probably a trained classifier rather than more
hand-tuned weights." `pulsefeed/learning/` is that, built and measured. It has
not yet been trained on real traffic or on a GPU — the code is finished, the
hardware phase is not.

### The label problem comes before the model problem

The obvious plan is free labels: the LLM already annotates every admitted
cluster, so severity ≥ ERROR is a positive. Continuous, unlimited, no
annotators.

It is also a feedback loop. **Labels only exist for events the current scorer
chose to admit.** A model trained on them learns to reproduce the teacher on
the region the student already selects; whatever the current scorer
systematically misses stays missed, never enters the training set, and every
offline metric looks excellent while the blind spot is laundered into learned
coefficients.

The audit samples fix it. The trigger already enriches ~1% of *rejected*
clusters and discards the results, purely to measure false negatives — an
unbiased draw from exactly the invisible region. Weighting them by inverse
propensity (1/0.01 = 100, capped) makes the union an unbiased estimate of the
stream. The effect is visible in the collection summary: raw base rate 1.12%,
weighted 0.34%, because admitted clusters are enriched at 100% while rejections
are sampled at 30%.

Three label sources, with different biases, all supported:
`GROUND_TRUTH` (synthetic only), `LLM_TEACHER` (biased, free, continuous),
`AUDIT` (unbiased, rare, expensive).

### Methodology, and the two ways it went wrong first

- **Split by time, not at random.** Near-duplicate events arrive seconds apart —
  that is the coalescer's whole premise — so a random split puts one copy in
  train and its twin in validation and reports memorisation.
- **Three splits, not two.** Calibration needs held-out data, and evaluating on
  the split the calibrator saw flatters exactly the metric the thresholds depend
  on.
- **Do not z-score these features.** This one cost a debugging session. All nine
  are bounded to [0,1] already; `risk` is zero for most events so its training
  std is ~0.06, and dividing by that gives it an effective range of ~17. The
  logit saturated, hundreds of events tied at p = 1.0, and the top of the
  ranking — the only part the trigger reads — became arbitrary. recall@1% fell
  from 0.48 to 0.11. Standardisation is retained, off by default, documented.
- **Statistics must not mutate the data.** The first version standardised
  examples in place *and* had the model standardise at inference, double-applying
  the transform. A unit test comparing two paths that should agree caught it.

### Two defects in the feature set, found by the diagnostics

The training report flags collinear and constant features, and immediately
found both:

- `novelty` and `duplication` correlate at **exactly −1.0** — one is defined as
  `1 - other`. A fit splits one effect across two coefficients, so neither is
  individually interpretable, which matters when humans read them next to the
  hand-set values.
- `recency` is **constant**. Events are scored at ingest, so their age is always
  ~0 and the decay term is always 1.0. The hand-tuned weights give it 0.5, which
  has never done anything.

Neither was visible before there was a fit to inspect.

### Results, including where the fit loses

| labels | ECE (hand → learned) | recall@1% (hand → learned) |
|---|---|---|
| ground truth, 5.2k events / 62 pos | 0.047 → 0.009 | 0.72 → 0.56 |
| ground truth, 41k events / 254 pos | 0.046 → 0.007 | 0.89 → 0.88 |
| LLM teacher + audit, 1.6k examples | 0.057 → 0.017 | 0.14 → **0.29** |

Reading these honestly:

- **Calibration improves 5-7x, consistently.** Ranking metrics do not show this,
  and it matters more than they do: the budget-, load- and deadline-aware
  thresholds all consume the score as a probability, so a well-ranked but
  overconfident model quietly breaks all three.
- **At small data, hand-tuned priors beat the fit on ranking.** Sixty-two
  positives cannot outvote a well-chosen prior. At 254 positives the gap
  closes to nothing. This is an argument for the continuous production label
  stream, not against learning.
- **On teacher labels — the production shape — the fit wins**, doubling
  recall@1%.
- **The learned coefficients independently agree with the manual diagnosis.**
  §8a concluded by hand that `user_relevance` was underweighted; the fit puts it
  at 3.3–3.8 against the hand-set 1.5. It also agrees closely on `risk`
  (3.8 vs 2.6) and wants far more `novelty − duplication` separation.

### The teacher's objective is not the user's

Under `LLM_TEACHER` labels the `user_relevance` coefficient collapses to ~0 —
because the teacher labels by *severity*, and a direct "@alice can you review
this?" is severity `info`. It is important to Alice and unimportant to a
severity classifier.

Distillation inherits whatever the teacher was asked, so "what counts as
important" has to be decided at the prompt, not at the fit. Either the teacher
is asked about relevance as well as severity, or user-relevance stays a
hand-set rule outside the learned model. Currently it is the latter, by default.

### Hardware roadmap, in cost order

Each step must beat the previous on `recall_at_budget` and must not regress
calibration — otherwise the hardware is buying something the thresholds cannot
use.

1. **Now, CPU.** Logistic regression over nine features. Implemented, evaluated
   above. Microseconds per event, no model server.
2. **Next, GPU batch.** `TransformerEmbedder` (MiniLM) replacing the hashed
   embedder behind the same interface. The hashed embedder is adequate for
   near-duplicate detection and poor at semantics — "connection pool exhausted"
   and "too many open DB handles" are one event sharing almost no tokens — so
   this should improve coalescing *and* novelty at once. Embedding is batchable
   and cacheable, so throughput is set by batch size, not per-event latency.
3. **Then, GPU.** `HybridScoreModel`: a linear head over
   [sentence embedding ‖ cheap features], distilled from teacher labels. Linear
   over a frozen encoder on purpose — it fits in seconds from cached embeddings
   and establishes whether the embedding carries signal *before* anyone spends
   GPU hours proving it does. The cheap features stay: source priority, burst and
   duplication are not recoverable from text.
4. **Only if 1–3 leave something on the table.** Fine-tune the encoder.

The interfaces for steps 2 and 3 are written and tested (without torch); step 4
is not started.

---

## 8b. Durability: bus and sink

Two interfaces, deliberately separate because their failure modes are opposite.

**`EventBus`** is the ingestion boundary — Redis Streams today, the same five
methods for Kafka later. At-least-once with explicit acks: an event is not
considered handled until the pipeline says so, so a worker that dies mid-flight
costs latency, not data. `reclaim_stale` exists because without it, deliveries
stranded by a dead consumer stay pending forever — durable, with nobody coming
to collect them. Retention is bounded (`MAXLEN ~`), since an unbounded stream
converts a slow consumer into a Redis OOM.

**`PersistenceSink`** is the durable record — Postgres, with pgvector when the
extension is present and a `real[]` column plus in-Python cosine when it is not.
The fallback is genuinely slower and says so; it exists so the system runs on
stock Postgres, not to pretend the two are equivalent. Every write is an upsert,
because at-least-once delivery means redelivery must be boring.

Policy: the bus fails **closed** (buffer and replay — a lost raw event is
unrecoverable), the sink fails **open** (keep serving, count the failure — a
delayed write is recoverable, and putting a database on the read path of a feed
whose entire premise is surviving its dependencies would be self-defeating).

### Reading it back

A durable record nobody reads back is a backup, not a database. `restore()`
rebuilds serving state after a restart, and *what it declines to restore* is
the interesting half:

- **Raw events** — restored, so a restored summary can still expand into the
  facts it cites. A row whose evidence 404s is worse than no row.
- **Entity memory** — restored, so the first event about a service that was
  degraded five minutes ago is not ranked as though nothing had happened to it.
- **Active summaries** → timeline rows. **Superseded ones are not restored.**
  They stay on disk for the audit trail; resurrecting them would put retracted
  conclusions back in front of users.
- **Open coalescing clusters** — deliberately not restored. They were in-flight
  working state rather than a record, and the events inside them are durable in
  the bus and will be redelivered if never acknowledged.

Restored rows are ranked from severity and confidence alone, because the cheap
features that produced the original score were never persisted and inventing
them would be worse than ranking conservatively.

The in-memory implementations model the same semantics on purpose, so bugs
surface in tests. That paid off immediately and embarrassingly: the in-memory
bus ignored `min_idle_ms` in `reclaim_stale` and handed a consumer its own
in-flight batch back on the next loop. The bus-crash scenario caught it — 2,657
events published, 5,146 ingested. Honouring idle time (and refusing to reclaim
from yourself) fixed it to exactly 2,657.

### The wiring that wasn't there

One more entry for the honesty ledger. For two full development rounds after
the storage layer landed, `create_app()` — the factory every deployment
instruction pointed at — read the LLM key, the budgets, and the worker count
from the environment, and silently ignored the backends. `docker compose up`
next to `uvicorn` produced a system that *looked* durable and kept everything
in process memory; the operations doc even described restore procedures the
served process could never run. The storage layer was tested, documented, and
unreachable from the deployment path.

The fix (deployment round): `PULSEFEED_PG_DSN` and `PULSEFEED_REDIS_URL` wire
the sink and bus into the factory, restore runs at boot, and two asymmetric
failure policies apply. A configured-but-unreachable DSN **fails the boot** —
the serving path stays fail-open (a database dying mid-flight must not stop
the feed), but an operator who asked for durability at startup must not
silently get amnesia instead. And with a bus configured, `POST /v1/events`
answers 202 only after the event is in Redis, or 503 when it is not — the
fail-closed publish the harness had always simulated, finally on the real
path. `pulsefeed-preflight` exists for the same reason from the other side:
every fallback in the system is silent by design, so deployment needed the one
place where a configured-but-broken dependency fails loudly, before traffic.

---

## 9. Known limitations

Stated plainly, because the experiment is only worth what its caveats allow.

1. **Traces are synthetic.** Labels are honest (planted by construction; nothing
   in PulseFeed reads them, and the mock provider never sees them), but the
   traffic shape was chosen by us. Swapping in a captured GitHub/Slack export
   means writing a loader that emits `Event` objects — nothing else changes.

2. **The mock provider is not a language model.** It does real work — reads the
   cluster, derives severity from the language, notices deploy→degradation→
   rollback ordering, uses entity memory — so the LLM arms' advantage comes from
   a structural fact a real model would also enjoy. But its prose is mechanical,
   and it cannot be wrong in the interesting ways a real model can. Nothing here
   measures summary *quality*.

3. **Enrichment affects retrieval only indirectly, and the frontier is noisy.**
   Before episodes were wired in, recall@20 was *identical at every threshold* —
   what surfaced was decided entirely by the cheap scorer and the coalescer, and
   the LLM only changed how well it read. Wiring in episode rollups changed that:
   recall@20 now ranges 65.8%→100% across the sweep, because episodes pack many
   events into one slot and episodes are built from cluster summaries, which
   require calls. So the LLM's effect on retrieval is real but entirely
   second-order — it comes from *packing*, not from better judgement about what
   matters.

   The coverage curve looked non-monotonic for a while — peaking at θ=0.15
   rather than at the cheapest threshold — and that turned out to be the metric
   bug described in §7, not a property of the system. Measured over the expanded
   feed the curve is clean: an 8x spend range, 17 calls / 28.9% coverage up to
   137 calls / 97.4%, with marginal returns falling roughly 3x. Treat the shape
   as the finding; with 38 important events across 5 storylines, individual
   points are still noisy.

4. **Ranking trails at K=20.** 84.2% against LLM-everything's 94.7%. Diagnosed
   in §8a. The learned scorer (§8c) is the intended fix and does not yet deliver
   it on ground-truth labels; it does improve calibration substantially and wins
   on teacher labels.

5a. **The learned scorer has never seen real traffic or a GPU.** Everything in
   §8c was fitted on synthetic traces. The transformer path is written and
   tested (without torch) and never trained. Treat the coefficient values as
   evidence that the machinery works, not as a model anyone should deploy.

5b. **`novelty` and `duplication` are perfectly collinear, and `recency` is
   constant.** Found by the training diagnostics, left in place: removing them
   changes `FEATURE_NAMES`, which invalidates the hand-set weights and every
   fitted model at once. Worth doing deliberately, with a migration, rather than
   as a drive-by.

6. **Serving state is memory-bounded.** `restore()` rebuilds events, entity
   memory and active summaries after a restart, but from a capped window rather
   than all history, and the read path remains in-process by design.

7. **Single-process.** Horizontal scaling by `tenant_id`/`entity_id` partition is
   a design intention, not running code.

8. **No authentication.** `tenant_id` is taken from the request body. Tenant
   *isolation* is enforced everywhere it could leak — timeline reads, entity
   keys, vector search, annotation citations — but nothing verifies the caller
   is who they claim to be.

---

## 10. What would change my mind

- If a captured real trace showed the cheap scorer's recall collapsing on events
  whose importance is not lexically marked, the first stage would need a trained
  classifier, not better keywords. (§8c builds that classifier; on synthetic
  data it has not yet earned its place on ranking, only on calibration.)
- If the sentence encoder turns out not to beat hashed embeddings on
  ``recall_at_budget``, steps 3 and 4 of the hardware roadmap should be dropped
  rather than pursued — the cheap features would then be carrying essentially
  all the signal, and the honest conclusion is that this problem does not need a
  GPU.
- If audit sampling showed a materially nonzero miss rate at the default
  threshold, the default is wrong and should move — that is what the sampling is
  for.
- If real-model summaries proved wrong often enough that users stopped trusting
  the feed, the correct response is to surface confidence and evidence more
  aggressively, not to enrich less.
