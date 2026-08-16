# HTTP API

```bash
uvicorn pulsefeed.api:create_app --factory
# OpenAPI: http://localhost:8000/docs
```

The surface is deliberately small. The interesting parts of PulseFeed are the
admission and degradation policies, not the HTTP shape.

---

## Authentication and rate limiting

**Off by default** — with no keys configured, every route is open. That is the
dev mode every quick start relies on, and `/readyz` reports
`"auth": "disabled (dev mode)"` so an internet-facing deployment running open
is visible instead of looking exactly like a working setup.

```bash
export PULSEFEED_API_KEYS="acme-key-1:acme,beta-key-1:beta,ops-key:*"
export PULSEFEED_RATE_WRITE_PER_SECOND=200   # optional; default 200/s, burst 2x
export PULSEFEED_RATE_READ_PER_SECOND=50     # optional; default 50/s, burst 2x
```

Send the key as `X-API-Key`. Semantics worth knowing:

- **The key decides the tenant.** A key bound to `acme` acts as `acme` no
  matter what `tenant_id` the body or query claims — the claim is contained,
  not rejected, so a misconfigured producer stays in its own box instead of
  becoming an outage. `key:*` makes an operator key that may choose a tenant.
- **Auth precedes rate limiting.** A 401 consumes no tokens, so an
  unauthenticated caller cannot burn a tenant's budget.
- **Rate limits are per (tenant, class)** — writes and reads have separate
  token buckets, charged against the tenant the *key* resolved to. Exceeding
  them returns `429` with a `Retry-After` header. Batch elements are charged
  individually; a batch is not a way around the per-event rate.
- Key comparison is constant-time (`hmac.compare_digest`).
- `/healthz`, `/readyz` and `/metrics` stay open: they are for the
  infrastructure, not for tenants.

---

## Ingest

### `POST /v1/events`

Accepts one event. **Returns as soon as it is accepted — never waits on the
LLM.** What `202 Accepted` means depends on how the server is deployed:

- **Bus mode** (`PULSEFEED_REDIS_URL` set): the event is in Redis before the
  202 — durably accepted, ingested asynchronously with at-least-once
  delivery. A bus outage returns **503** (fail closed; retry or buffer
  upstream) rather than accept-and-forget.
- **Direct mode** (no bus): the event is scored and routed in-process before
  the 202 — lower latency, but an event in flight during a crash is lost.

Either way, 202 never means "enriched".

```json
{
  "source": "grafana",
  "content": "checkout latency rises to 700ms",
  "tenant_id": "acme",
  "entity_ids": ["checkout-service"],
  "actor": "alerting",
  "timestamp": 1700000000.0,
  "metadata": {"deadline_seconds": 45, "priority_override": 0}
}
```

| field | required | notes |
|---|---|---|
| `source` | yes | drives source priority and the default freshness deadline |
| `content` | yes | ≤ 8000 chars; treated as untrusted data throughout |
| `tenant_id` | no | defaults to `"default"`; the isolation boundary |
| `entity_ids` | no | ≤ 16; **the first one is the coalescing key** |
| `actor` | no | matched against the tenant's watched actors |
| `timestamp` | no | defaults to now |
| `metadata.deadline_seconds` | no | overrides the source's freshness window |
| `metadata.priority_override` | no | 0–3, forces a scheduling class |
| `metadata.force_enrich` | no | hard safety bypass — always enriched |

```json
{"event_id": "evt_a1b2c3d4e5f6", "status": "accepted"}
```

**Getting `entity_ids` right is the highest-leverage thing a producer does.**
Coalescing, entity memory, episode grouping and the ranking boost all key on the
first entity. Sending everything with no entity puts the whole stream in one
bucket named after its source.

### `POST /v1/events:batch`

Same shape, an array, ≤ 1000. Returns `{"accepted": n, "event_ids": [...]}`.

---

## Read

### `GET /v1/timeline`

| parameter | default | notes |
|---|---|---|
| `tenant_id` | `default` | |
| `limit` | 20 | 1–200 |
| `ranked` | true | false gives strict reverse-chronological |

```json
{
  "tenant_id": "acme",
  "count": 3,
  "degraded": false,
  "items": [
    {
      "item_id": "itm_...",
      "kind": "episode",
      "title": "checkout-service: 6 related events over 459s ...",
      "severity": "error",
      "rank_score": 0.91,
      "enriched": true,
      "degraded": false,
      "event_count": 6,
      "entity_ids": ["checkout-service"],
      "source_event_ids": ["evt_...", "evt_..."],
      "rolled_up_into": null
    }
  ]
}
```

- **`degraded: true`** means the primary provider's circuit is open. The feed is
  still complete; it just has fewer summaries.
- **`enriched: false`** rows are cheap-path descriptions. Perfectly valid feed
  content, not errors.
- **`kind`** is `event` | `cluster` | `episode`. Rows with `rolled_up_into` set
  are hidden by default (they live under an episode).

### `GET /v1/items/{item_id}/evidence`

Expands a row into the raw events it cites.

```json
{
  "item": {...},
  "evidence": [
    {"event_id": "evt_...", "source": "grafana", "content": "...", "timestamp": 1700000000.0}
  ]
}
```

**This endpoint is why hallucination is survivable.** No conclusion the system
shows is unfalsifiable — every row expands to the facts underneath it. `404` if
the item is unknown.

### `GET /v1/entities/{entity_id}`

Current state plus every belief ever held, superseded ones included.

```json
{
  "entity_id": "checkout-service",
  "state": "recovering",
  "severity": "error",
  "event_count": 6,
  "recent_summary": "...",
  "belief_history": [
    {"summary_id": "sum_1", "text": "suspected database issue",
     "status": "superseded", "superseded_by": "sum_2", "version": 1},
    {"summary_id": "sum_2", "text": "root cause confirmed: DNS failure",
     "status": "active", "supersedes": "sum_1", "version": 2}
  ]
}
```

This answers "what did the system believe at 12:05, and why" — corrections
supersede rather than overwrite.

---

### `GET /v1/digest`

One summary of everything that mattered in the last window — the top of the
`event → cluster → episode → digest` hierarchy.

| parameter | default | notes |
|---|---|---|
| `tenant_id` | `default` | |
| `window` | 3600 | seconds, up to 7 days |
| `level` | `hourly` | or `daily` |
| `narrate` | false | ask the model to write it as prose (budget-gated) |

```json
{
  "tenant_id": "acme",
  "window_seconds": 3600,
  "digest": {
    "level": "hourly",
    "text": "SEV1: payments-api error rate above threshold... (+4 related updates)",
    "severity": "critical",
    "source_event_ids": ["evt_...", "..."],
    "child_summary_ids": ["sum_...", "..."]
  }
}
```

Children are chosen to avoid double-telling: episodes in the window first, then
any cluster/micro summary **not already absorbed** by one of them. Extractive by
default, so it works with the provider down. **Not persisted** — a dashboard
refresh must not grow the audit trail. `digest: null` with a reason when the
window is empty.

---

## Operations

### `GET /v1/stats`

Pipeline, scheduler, provider, coalescer, budget and **`skip_reasons`** — the
most useful diagnostic in the system, because "fewer LLM calls" has four
distinct causes with opposite fixes. See
[operations.md §4](operations.md#4-diagnosing).

### `GET /healthz`

`{"status": "ok"}` — **does not depend on the provider.** A feed serving ranked
events with no summaries is doing its job. Use this for liveness.

### `GET /readyz`

```json
{
  "status": "ok",
  "enrichment": "degraded",
  "scorer": "LogisticScoreModel",
  "queue_depth": 3,
  "breakers": {"mock": "open"},
  "auth": "enabled",
  "durability": {"sink": "postgres", "bus": "redis", "sink_failures": 0,
                 "bus_lag": 0, "ingest": {"consumed": 512, "acked": 512}},
  "prometheus": true
}
```

`enrichment: degraded` is informational, not a reason to pull the instance from
a load balancer. `scorer` tells you whether a fitted model actually loaded — a
bad model file falls back silently by design.

### `GET /metrics`

Prometheus exposition. Metric list in
[operations.md §3](operations.md#3-the-dashboard-that-matters).

---

## Semantics worth knowing

**Ingestion is at-least-once when fronted by the bus.** `event_id` is assigned
by the producer path and every sink write is an upsert, so redelivery is a
no-op. Supply your own `event_id` if you have a natural one.

**`202` is not "enriched".** It is "durably accepted". Whether an event is ever
enriched depends on the trigger, and most events are not — that is the design.

**Rows change after they appear.** A row is published provisionally when its
cluster opens, updated as events join, upgraded in place when enrichment lands,
and possibly hidden behind an episode later. Clients should treat `item_id` as
stable and the content as mutable.

**There is no delete.** Corrections supersede.

---

## Not implemented

- Pagination cursors — `limit` only.
- WebSocket/SSE streaming of timeline updates.
- A delete or redaction endpoint (GDPR-style erasure would need to reach raw
  events, annotations, summaries and embeddings together).
