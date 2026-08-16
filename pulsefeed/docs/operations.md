# Operations

A runbook. What to look at, what it means, and what to do about it.

---

## 1. Deploying

The full real-machine walkthrough — sizing, systemd, TLS, the GPU box, backup
and restore — is **[deployment.md](deployment.md)**. The short version:

```bash
pip install -e '.[all,store]'
docker compose up -d                     # redis + postgres with pgvector
cp .env.example .env                     # every variable documented inline
set -a && source .env && set +a
pulsefeed-preflight                      # verify what you configured actually works
uvicorn pulsefeed.api:create_app --factory --host 0.0.0.0 --port 8000
```

### Environment

| variable | default | purpose |
|---|---|---|
| `PULSEFEED_LLM_API_KEY` | — | hosted provider key |
| `PULSEFEED_LLM_BASE_URL` | OpenAI | any OpenAI-compatible endpoint |
| `PULSEFEED_LLM_MODEL` | `gpt-4o-mini` | primary model |
| `PULSEFEED_LOCAL_LLM_BASE_URL` | — | fallback (e.g. local vLLM) |
| `PULSEFEED_PG_DSN` | — | Postgres record of truth; unset = memory-only, **set-but-broken = refuse to boot** |
| `PULSEFEED_RESTORE` | 1 | restore state from the sink at boot |
| `PULSEFEED_RESTORE_TENANTS` | `PULSEFEED_TENANT` | comma-separated tenants to restore |
| `PULSEFEED_REDIS_URL` | — | Redis Streams bus; POST publishes to it, 503 when it is down |
| `PULSEFEED_STREAM` | `pulsefeed:events` | stream key |
| `PULSEFEED_CONSUMER` | hostname-pid | consumer name in the group |
| `PULSEFEED_SCORER_MODEL` | — | path to a fitted scorer JSON |
| `PULSEFEED_WORKERS` | 4 | concurrent enrichments |
| `PULSEFEED_AUDIT_RATE` | 0.01 | audit sampling rate |
| `PULSEFEED_TOKEN_BUDGET` | 500000 | daily tokens |
| `PULSEFEED_USD_BUDGET` | 5.0 | daily dollars |
| `PULSEFEED_API_KEYS` | — | `key:tenant` pairs; `key:*` = operator. Unset = auth **off** |
| `PULSEFEED_RATE_WRITE_PER_SECOND` | 200 | per-tenant write rate (burst 2x) |
| `PULSEFEED_RATE_READ_PER_SECOND` | 50 | per-tenant read rate (burst 2x) |

**Nothing above is required.** With no provider configured the system falls back
to a deterministic mock; with no `PULSEFEED_SCORER_MODEL` it uses hand-tuned
weights. It starts and serves either way — deliberately, because a feed that
refuses to boot without an LLM has the dependency exactly backwards. The one
deliberate exception: a *configured but unreachable* `PULSEFEED_PG_DSN` fails
the boot, because an operator who asked for durability must not silently get a
memory-only deployment.

### Verifying a deploy

```bash
pulsefeed-preflight               # checks everything configured, exit 0/1
curl -s localhost:8000/healthz    # {"status":"ok"}
curl -s localhost:8000/readyz     # enrichment + breaker + scorer + durability state
curl -s localhost:8000/metrics    # Prometheus exposition
```

`/readyz` reports `"scorer": "LogisticScoreModel"` or `"LinearScoreModel"`.
**Check this after deploying a fitted model** — a bad model file falls back
silently by design, and a silent fallback looks identical to a deliberate
hand-tuned deploy without this field.

It also reports `"auth": "enabled"` or `"disabled (dev mode)"`. **An
internet-facing deployment showing dev mode is a misconfiguration** that would
otherwise look exactly like a working setup.

`"durability"` works the same way: `{"sink": "postgres", "bus": "redis"}` on a
production box, plus `bus_lag` (unacknowledged deliveries — alert on it) and
the consumer's ingest counters when the bus is configured. `"sink": "memory"`
on a machine that was supposed to be durable means the DSN never reached the
process environment.

---

## 2. What to alert on

Ordered by "how badly do you want to be woken".

| alert | expression | why |
|---|---|---|
| **P0 work dropped** | `increase(pulsefeed_events_dropped_total{priority="P0"}[5m]) > 0` | should be structurally impossible; means the reserved lane overflowed |
| **Ingestion stalled** | `rate(pulsefeed_events_ingested_total[5m]) == 0` | the feed's core function; unrelated to the LLM |
| **Bus lag growing** | `IngestWorker.lag()` rising monotonically | consumers cannot keep up, or one died holding deliveries |
| **Provider circuit open** | `pulsefeed_circuit_breaker_state == 2` for > 10m | degraded, not down — but it has not recovered |
| **Budget exhausted early** | `pulsefeed_budget_remaining_fraction < 0.1` before 18:00 | tenant will get no enrichment for the rest of the day |
| **Audit miss rate rising** | `pulsefeed_audit_misses_total / pulsefeed_audit_samples_total` | the cheap scorer is missing important events |
| **Queue growing** | `pulsefeed_queue_depth` trending up over 30m | admission is letting in more than capacity |

**Do not alert on the LLM being down.** It is designed to be down sometimes; the
feed keeps working. Alert on the *feed* being down.

---

## 3. The dashboard that matters

Not QPS. The panel that shows the system is doing its job puts these on one
time axis:

```
offered load        ▁▂▃▅███▅▃▂▁      rises
LLM invocation rate ▁▁▁▂▂▂▂▂▁▁▁      stays flat          ← the point
trigger threshold   ▁▁▂▃▄▄▄▃▂▁▁      rises with pressure
queue depth         ▁▁▁▁▁▁▁▁▁▁▁      stays near zero
cost / hour         ▁▁▁▁▁▁▁▁▁▁▁      stays bounded
P0 recall           ██████████       flat
```

Offered load rising while invocation rate stays flat *is* the result. If
invocation rate tracks offered load, admission control is not working.

### Metric reference

| metric | type | labels |
|---|---|---|
| `pulsefeed_events_ingested_total` | counter | tenant, source |
| `pulsefeed_events_coalesced_total` | counter | tenant |
| `pulsefeed_events_triggered_total` | counter | tenant, reason |
| `pulsefeed_events_skipped_total` | counter | tenant, reason |
| `pulsefeed_events_dropped_total` | counter | tenant, priority, reason |
| `pulsefeed_trigger_threshold` | gauge | tenant |
| `pulsefeed_queue_depth` | gauge | priority |
| `pulsefeed_queue_wait_seconds` | histogram | priority |
| `pulsefeed_llm_invocations_total` | counter | tenant, provider, outcome |
| `pulsefeed_llm_tokens_total` | counter | tenant, provider |
| `pulsefeed_llm_cost_usd_total` | counter | tenant |
| `pulsefeed_provider_latency_seconds` | histogram | provider |
| `pulsefeed_circuit_breaker_state` | gauge | provider (0/1/2) |
| `pulsefeed_audit_samples_total` | counter | tenant |
| `pulsefeed_audit_misses_total` | counter | tenant |
| `pulsefeed_budget_remaining_fraction` | gauge | tenant |
| `pulsefeed_end_to_end_latency_seconds` | histogram | path |

---

## 4. Diagnosing

### "Enrichment stopped"

`GET /v1/stats` → `skip_reasons`. Four causes, opposite fixes:

| reason | meaning | fix |
|---|---|---|
| `utility_below_threshold` | working as designed | lower θ if you want more |
| `budget_exhausted` | tenant is out | raise the budget or the reservation |
| `deadline:would_miss_freshness_window` | queue wait exceeds value window | add workers, or accept it |
| `provider:circuit_open` | provider down | check `/readyz` |
| `queue_full_cheap_path` | backpressure | add workers or raise θ |

This is the single most useful diagnostic in the system. "Fewer LLM calls" from
the outside is four different problems.

### "The feed shows nonsense"

1. `/readyz` → which scorer is live? A fitted model can be bad.
2. Expand the row: `GET /v1/items/{id}/evidence`. If the evidence is sensible
   and the summary is not, it is a model problem. If the evidence is wrong, it
   is an ingestion problem.
3. Check `fabricated_event_ids` counts — a model citing events it was not shown
   is a much more serious signal than a clumsy summary.

### "Costs are higher than expected"

`GET /v1/stats` → `budget`, and `pulsefeed_llm_tokens_total` by provider.

Usual causes, in order of frequency:
1. Coalescing is not working — check `events_coalesced_total` against
   `events_ingested_total`. On the reference trace ~73% of events get coalesced
   away. Near zero means `max_open_per_entity` or the similarity band is wrong.
2. Episodes re-narrating unchanged content — should be prevented by the
   signature check; if `episodes_enriched` tracks `episodes_built`, it is not.
3. θ too low for the traffic.

### "It was wrong about an incident"

The belief history is on disk:

```
GET /v1/entities/{entity_id}
```

returns `belief_history` including **superseded** conclusions with timestamps.
"What did the system believe at 12:05, and why" is answerable; that is what the
supersede machinery is for.

---

## 5. Failure playbook

### Provider outage

**Expected behaviour, no action needed.** The breaker opens, the trigger stops
admitting, the feed continues on the cheap path. Verified by
`harness.failure_injection --scenarios outage`: 557 items still published
*during* the outage, every event accounted for, enrichment resumed on recovery.

Act only if it persists beyond your provider's SLA, or if `/readyz` still shows
`open` long after the provider recovered (suspect `recovery_timeout`).

### Traffic burst

**Expected behaviour.** Measured at 45.6x offered load (3.3 → 151.5 events/s):
enrichment rate rose only 0.025 → 0.235/s, 15,872 events coalesced away, peak
queue depth **0**, nothing dropped.

If the queue *does* grow, admission is not engaging — check that
`LoadAwarePolicy` is in the policy stack.

### Slow provider

Distinct from an outage and it matters. If injected latency exceeds the
provider's own timeout, every call fails and you are in the *outage* path
(breaker opens). If it stays inside the timeout, calls succeed slowly, queue
waits grow, and the **deadline policy** starts refusing work it cannot deliver
fresh — 82 admission refusals in the reference scenario, breaker still `closed`.

Confirm which path you are in via `/readyz` breaker state.

### Redis / bus outage

Producers' `publish` fails. Buffer upstream and retry — the bus fails **closed**
on purpose, because a lost raw event is unrecoverable.

On recovery the backlog drains. Verified: 5,017 events buffered and replayed
across a 120s outage, zero lost.

### A consumer died holding unacknowledged work

`reclaim_stale` hands abandoned deliveries to another consumer after
`reclaim_idle_ms`. Verified: a consumer dying with 100 unacked deliveries, all
100 reclaimed, 2,657 published → 2,657 ingested.

**Do not set `reclaim_idle_ms` too low.** Below the time a healthy consumer
takes to process a batch, consumers steal each other's in-flight work and
everything is processed twice. This was a real bug, caught by the chaos suite.

### Postgres outage

The sink fails **open**: the feed keeps serving from memory and
`pipeline.sink_failures` climbs. Writes during the outage are lost unless the
bus still holds the events.

On recovery, restart with `pipeline.restore(tenant)` to rebuild state — or keep
running, since the serving path never depended on it.

---

## 6. Restart

The API server does this automatically at boot when `PULSEFEED_PG_DSN` is set:
each tenant in `PULSEFEED_RESTORE_TENANTS` is restored before the first request
is served (disable with `PULSEFEED_RESTORE=0`). Embedding the pipeline
yourself, the same sequence is:

```python
pipeline = create_pipeline()
counts = await pipeline.restore("acme")   # events, entities, summaries, items
await pipeline.start()
```

Restores raw events (so restored rows can still expand to evidence), entity
memory (so ranking stays aware of what is degraded), and **active** summaries.
Superseded beliefs stay on disk and are not resurrected.

Open coalescing clusters are *not* restored — they were in-flight working state,
and their events are durable in the bus.

---

## 7. Multi-tenancy

Isolation is enforced at every layer that could leak: timeline reads are
tenant-scoped, entity memory is keyed by `(tenant, entity)`, vector search
filters by tenant, and annotation citations are validated against the ids
supplied to that call — so a model cannot attribute output to another tenant's
events even if it tries.

Budgets are per tenant with a P0 reservation, so one tenant's noisy day cannot
consume the capacity another tenant's incident will need.

When `PULSEFEED_API_KEYS` is set, the key decides the tenant — a key bound to
`acme` cannot read or write any other tenant's data regardless of what the
request claims — and per-tenant token buckets rate-limit reads and writes
separately (429 + `Retry-After` past the burst).

**Not implemented:** cross-tenant fairness in the scheduler (the queue is
shared).

---

## 8. Capacity planning

Rules of thumb from the reference trace:

- **Events → clusters:** ~73% of events coalesce away. Budget clusters, not
  events.
- **Clusters → calls:** ~1–3% of clusters get enriched at default θ.
- **Tokens per call:** ~550 average (cluster + entity memory + schema).
- **Worker count:** `worker_count × (1 / provider_latency)` is your service
  rate. At 4 workers and 400ms that is 10 calls/s — far above what the trigger
  admits, which is the intended ratio.

Scaling knob order, cheapest first: raise θ → widen coalescing →
add workers → raise budget.
