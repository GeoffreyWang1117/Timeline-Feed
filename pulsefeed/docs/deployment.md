# Deploying on real machines

A start-to-finish guide: from a bare Linux box to a durable, authenticated,
monitored PulseFeed taking real traffic — including the optional GPU machine
for a local model and scorer training. The Chinese version of this guide is
at [zh/deployment.md](zh/deployment.md).

Everything here was verified against the code in this repository; where a
behaviour is load-bearing (durability, restore, fail-closed ingest) there is a
test named for it in `tests/test_deploy_wiring.py`.

---

## 0. Pick a topology

PulseFeed degrades along a ladder, and so do its deployments. Start at the
smallest rung that answers your question; each rung is a strict superset of
the one before.

| rung | what runs | state survives restart? | good for |
|---|---|---|---|
| **T0 dev** | `uvicorn` alone | no | trying the API, demos |
| **T1 durable single box** | uvicorn + Redis + Postgres | yes | a real deployment for one team |
| **T2 split LLM** | T1 + a GPU box serving a local model | yes | provider independence, data locality |

There is deliberately no "T3: N stateless API replicas" rung — see
[§10 Scaling limits](#10-scaling-limits-read-before-adding-replicas) before
putting more than one API instance behind a load balancer.

**The LLM is never a deployment dependency.** Every rung boots and serves with
no provider configured (a deterministic mock enriches instead). Configure the
real provider when you are ready; nothing else changes.

---

## 1. Machine requirements

Measured behaviour, not aspiration: the reference replay pushes 6,396 events
through the full pipeline in ~27s on a single CPU core with the in-memory
backends — the cheap path is dict lookups and a 256-dim hashing embedder, and
the expensive path is rate-limited by design.

| component | minimum | comfortable | notes |
|---|---|---|---|
| API box CPU | 2 cores | 4 cores | the pipeline is one process; cores beyond ~4 buy little |
| API box RAM | 2 GB | 8 GB | bounded in-memory state: ~50k events + annotations + summaries per the retention caps |
| Postgres | shares the box | 2 cores / 4 GB | pgvector index build is the only heavy moment |
| Redis | shares the box | 512 MB `maxmemory` | the stream is an ingestion buffer, not the record |
| GPU box (optional) | — | 1× 24 GB GPU | enough for a 7B-class model under vLLM; smaller cards serve smaller models |
| OS | Linux, Python ≥ 3.10 | Ubuntu 22.04+ / Debian 12+ | anything systemd-based matches §5 |

---

## 2. Install

On the API box:

```bash
sudo apt-get update && sudo apt-get install -y python3-venv python3-pip git
git clone <your-fork-url> && cd Timeline-Feed/pulsefeed

python3 -m venv /opt/pulsefeed-venv
source /opt/pulsefeed-venv/bin/activate
pip install -e '.[all,store]'        # api + llm + metrics + redis/postgres clients
```

`.[all,store]` is the full server. The extras exist separately (`api`, `llm`,
`metrics`, `store`, `dev`, `gpu`) so a harness-only or library-only install
stays small; `gpu` is only for the transformer encoder path and belongs on the
GPU box, not here.

### Backing services

Option A — Docker (fastest to correct):

```bash
docker compose up -d          # redis:7 + pgvector/pgvector:pg16, healthchecked
```

Option B — native packages:

```bash
sudo apt-get install -y redis-server postgresql-16 postgresql-16-pgvector
sudo -u postgres psql -c "CREATE USER pulsefeed PASSWORD 'CHANGE_ME'"
sudo -u postgres psql -c "CREATE DATABASE pulsefeed OWNER pulsefeed"
```

Two Redis settings matter (`docker-compose.yml` already sets both):

- `appendonly yes` — the stream must survive a Redis restart, or "202 means
  durably accepted" is a lie.
- `maxmemory-policy noeviction` — a full Redis must reject writes loudly
  (producers see the failure and retry) rather than silently drop a stream.

pgvector is recommended, not required: on stock Postgres the sink degrades to
a `real[]` column with in-Python cosine — correct, slower. Preflight tells you
which one you have.

---

## 3. Configure

```bash
cp .env.example .env && $EDITOR .env
```

`.env.example` documents every variable. The complete reference:

| variable | default | purpose |
|---|---|---|
| `PULSEFEED_LLM_API_KEY` | — | hosted provider key |
| `PULSEFEED_LLM_BASE_URL` | OpenAI | any OpenAI-compatible endpoint |
| `PULSEFEED_LLM_MODEL` | `gpt-4o-mini` | primary model |
| `PULSEFEED_LOCAL_LLM_BASE_URL` | — | fallback endpoint (e.g. vLLM on the GPU box) |
| `PULSEFEED_LOCAL_LLM_API_KEY` | — | fallback key, if the endpoint wants one |
| `PULSEFEED_LOCAL_LLM_MODEL` | `local-model` | model name the fallback serves |
| `PULSEFEED_PG_DSN` | — | Postgres record of truth; unset = memory-only |
| `PULSEFEED_RESTORE` | `1` | restore state from the sink at boot (`0` disables) |
| `PULSEFEED_RESTORE_TENANTS` | `PULSEFEED_TENANT` | comma-separated tenants to restore |
| `PULSEFEED_REDIS_URL` | — | Redis Streams bus; unset = direct in-process ingest |
| `PULSEFEED_STREAM` | `pulsefeed:events` | stream key |
| `PULSEFEED_CONSUMER` | hostname-pid | consumer name in the group |
| `PULSEFEED_SCORER_MODEL` | — | fitted scorer JSON from a training run |
| `PULSEFEED_TENANT` | `default` | tenant registered at boot |
| `PULSEFEED_TOKEN_BUDGET` | `500000` | daily tokens |
| `PULSEFEED_USD_BUDGET` | `5.0` | daily dollars |
| `PULSEFEED_WORKERS` | `4` | concurrent enrichments |
| `PULSEFEED_AUDIT_RATE` | `0.01` | fraction of rejections re-checked |
| `PULSEFEED_API_KEYS` | — | `key:tenant` pairs; `key:*` = operator; **unset = auth off** |
| `PULSEFEED_RATE_WRITE_PER_SECOND` | `200` | per-tenant write rate (burst 2×) |
| `PULSEFEED_RATE_READ_PER_SECOND` | `50` | per-tenant read rate (burst 2×) |
| `PULSEFEED_SKIP_PREFLIGHT` | — | container image only: skip the boot-time preflight |

Semantics that are easy to get wrong:

- **`PULSEFEED_PG_DSN` set ⇒ fail-closed boot.** If the DSN is configured but
  unreachable, the server refuses to start. An operator who asked for
  durability must not get a silently amnesiac deployment that looks healthy
  until its first restart.
- **`PULSEFEED_REDIS_URL` set ⇒ `POST /v1/events` publishes to the stream**
  and answers 202 only once the event is in Redis; an in-process consumer
  ingests it with at-least-once delivery. If Redis is down, ingestion returns
  **503** — buffer and retry upstream. Unset, ingestion is direct and
  in-process (lower latency, no crash-recovery for in-flight events).
- **Generate real keys:** `python -c "import secrets; print(secrets.token_urlsafe(32))"`
  per key. The key decides the tenant — see [api.md](api.md#authentication-and-rate-limiting).

---

## 4. Preflight

```bash
set -a && source .env && set +a       # export what the server will see
pulsefeed-preflight                   # or: python -m pulsefeed.preflight
```

Preflight reads the same variables the server reads and verifies each
configured dependency actually works: Redis round-trips a stream entry,
Postgres accepts a real write and reports whether pgvector is present, each
configured LLM endpoint answers a one-token completion (with latency), the
scorer model file loads against the current feature contract, every numeric
variable parses. Unconfigured components are *skips*, not failures — each skip
line states the fallback you are choosing.

Exit code 0/1, `--json` for scripts. Gate your deploy on it:

```bash
pulsefeed-preflight || { echo "not deploying onto a broken environment"; exit 1; }
```

The scorer check exists because the server deliberately falls back *silently*
on a bad model file (a bad file must degrade ranking, not block boot);
preflight is where the same file fails *loudly*, before traffic.

---

## 5. Run under systemd

`/etc/systemd/system/pulsefeed.service`:

```ini
[Unit]
Description=PulseFeed API
After=network-online.target redis-server.service postgresql.service
Wants=network-online.target

[Service]
Type=exec
User=pulsefeed
Group=pulsefeed
WorkingDirectory=/opt/Timeline-Feed/pulsefeed
EnvironmentFile=/etc/pulsefeed/env
ExecStartPre=/opt/pulsefeed-venv/bin/pulsefeed-preflight
ExecStart=/opt/pulsefeed-venv/bin/uvicorn pulsefeed.api:create_app --factory \
    --host 127.0.0.1 --port 8000
Restart=on-failure
RestartSec=3

# Hardening — the process needs nothing beyond outbound sockets.
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
ReadWritePaths=

[Install]
WantedBy=multi-user.target
```

```bash
sudo useradd --system --no-create-home pulsefeed
sudo install -d -m 750 -o pulsefeed /etc/pulsefeed
sudo cp .env /etc/pulsefeed/env && sudo chown pulsefeed /etc/pulsefeed/env && sudo chmod 600 /etc/pulsefeed/env
sudo systemctl daemon-reload && sudo systemctl enable --now pulsefeed
journalctl -u pulsefeed -f
```

Notes:

- `ExecStartPre` runs preflight on every start: a box whose Postgres died
  overnight fails to boot with a named reason in the journal instead of
  serving amnesiac traffic.
- `--host 127.0.0.1` on purpose — TLS and the outside world are the reverse
  proxy's job (§6).
- **One instance.** Do not template this unit into `pulsefeed@1,2,3` — §10.
- Restarts are cheap by design: state restores from Postgres at boot
  (`restore()` per configured tenant) and unacknowledged bus deliveries are
  redelivered. What a restart loses: open coalescing clusters (their events
  are in the bus and redeliver) and in-memory latency samples.

### The docker path instead

```bash
docker compose --profile app up -d    # builds the image, waits on healthchecks
```

The image runs preflight at container start (set `PULSEFEED_SKIP_PREFLIGHT=1`
under an orchestrator that restarts on failure anyway), runs as a non-root
user, and bakes in no secrets — configuration arrives via `.env` /
environment, a fitted scorer via a volume mount.

---

## 6. Expose it: reverse proxy, TLS, keys

Never put the bare port on the internet. nginx in front:

```nginx
server {
    listen 443 ssl;
    server_name pulsefeed.example.com;
    ssl_certificate     /etc/letsencrypt/live/pulsefeed.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/pulsefeed.example.com/privkey.pem;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        client_max_body_size 2m;         # a 1000-event batch fits comfortably
    }

    # Ops endpoints are for your infrastructure, not the internet.
    location ~ ^/(metrics|healthz|readyz)$ {
        allow 10.0.0.0/8;                # your monitoring network
        deny all;
        proxy_pass http://127.0.0.1:8000;
    }
}
```

Then confirm the deployment is actually closed:

```bash
curl -s https://pulsefeed.example.com/readyz | python3 -m json.tool
#   "auth": "enabled"           ← if this says "disabled (dev mode)", stop here
#   "durability": {"sink": "postgres", "bus": "redis", ...}
```

`/readyz` states auth and durability precisely because a misconfigured
deployment (auth off, or memory-only on a box that was supposed to be durable)
otherwise looks identical to a working one.

---

## 7. The GPU box: local model + training

Optional, two independent uses. Both were explicitly designed to be a
configuration change, not a rewrite — the provider layer speaks the OpenAI
chat-completions shape to anything.

### 7a. Serve a local model with vLLM

On the GPU machine:

```bash
python3 -m venv ~/vllm-venv && source ~/vllm-venv/bin/activate
pip install vllm
python -m vllm.entrypoints.openai.api_server \
    --model Qwen/Qwen2.5-7B-Instruct \
    --host 0.0.0.0 --port 8001 \
    --max-model-len 8192
```

On the API box, wire it as the **fallback** (used when the hosted provider's
circuit opens):

```bash
PULSEFEED_LOCAL_LLM_BASE_URL=http://<gpu-box>:8001/v1
PULSEFEED_LOCAL_LLM_MODEL=Qwen/Qwen2.5-7B-Instruct
```

or as the **primary** (data never leaves your network; the deterministic mock
remains the last-resort fallback):

```bash
PULSEFEED_LLM_BASE_URL=http://<gpu-box>:8001/v1
PULSEFEED_LLM_API_KEY=            # vLLM needs none unless you set --api-key
PULSEFEED_LLM_MODEL=Qwen/Qwen2.5-7B-Instruct
```

Then verify from the API box — this is exactly what preflight's LLM probe
does, so just run it:

```bash
pulsefeed-preflight        # "llm fallback ... answered in NNNms"
```

Sizing note: enrichment traffic is *deliberately* small — the trigger admits
~1–3% of clusters at default θ, and the reference trace produced 51 calls
averaging ~550 tokens over 8 simulated hours. A single mid-range GPU is not a
bottleneck; latency (which feeds the deadline policy) matters more than
throughput. Measure your own p95 via `pulsefeed_provider_latency_seconds`
before buying hardware. The model named above is an example, not a
recommendation — swap in whatever your card fits and your evaluation prefers.

### 7b. Train the scorer on real traffic

The full methodology (label bias, inverse-propensity weighting, time-based
splits, the calibration ship gate) is [training.md](training.md); the
deployment-side loop is:

```bash
# 1. Collect on the API box while running normally (training.md §7):
#    TrainingCollector(pipeline).attach() → data/train.jsonl

# 2. Move the dataset to the GPU box (or any box; step 3 is CPU-fine today)
scp data/train.jsonl gpu-box:pulsefeed-data/

# 3. Train and export
pulsefeed-train --source pipeline --out scorer.json --json

# 4. Ship it back and verify BEFORE restarting — the server falls back
#    silently on a bad file; preflight and /readyz are your two loud checks
scp gpu-box:scorer.json /etc/pulsefeed/scorer.json
PULSEFEED_SCORER_MODEL=/etc/pulsefeed/scorer.json pulsefeed-preflight
sudo systemctl restart pulsefeed
curl -s localhost:8000/readyz | grep -o '"scorer": "[^"]*"'
#   "scorer": "LogisticScoreModel"   ← not "LinearScoreModel"
```

Roll back by unsetting `PULSEFEED_SCORER_MODEL` and restarting. The GPU
embedding path (training.md §8, steps 2–4) runs on this same box when you get
to it; today's logistic scorer trains in seconds on CPU.

---

## 8. Monitoring

Prometheus scrape config:

```yaml
scrape_configs:
  - job_name: pulsefeed
    scrape_interval: 15s
    static_configs:
      - targets: ["<api-box>:8000"]
```

Wire the alerts from [operations.md §2](operations.md#2-what-to-alert-on) —
P0 drops, ingestion stalled, bus lag, breaker stuck open, budget exhausted
early, audit-miss rate. Build the one dashboard from
[operations.md §3](operations.md#3-the-dashboard-that-matters): offered load
rising while LLM invocation rate stays flat *is* the system working.

With the bus configured, `/readyz` additionally reports `durability.bus_lag`
(unacknowledged deliveries — the number to alert on) and the consumer's
ingest counters.

---

## 9. Backup, restore, upgrades

**What to back up: Postgres. Only Postgres.** The sink is the record of truth
— events, annotations, summaries (superseded ones included), entity memory.
Redis holds the in-flight ingestion buffer (AOF makes it restart-safe, but a
lost Redis loses only unprocessed deliveries, and producers should be
retrying those); everything in process memory rebuilds from the sink.

```bash
pg_dump -Fc -U pulsefeed pulsefeed > pulsefeed-$(date +%F).dump   # cron this
```

**Disaster recovery** = restore the dump, start the service. Boot-time
`restore()` reloads events, entity memory, and active summaries per configured
tenant; superseded beliefs stay queryable in their belief histories without
being resurrected into the feed. Verified end-to-end through the factory in
`tests/test_deploy_wiring.py::test_restart_restores_from_the_sink`.

**Upgrades:**

```bash
cd /opt/Timeline-Feed/pulsefeed && git pull
source /opt/pulsefeed-venv/bin/activate && pip install -e '.[all,store]'
pulsefeed-preflight && sudo systemctl restart pulsefeed
```

Schema migrations are idempotent `CREATE ... IF NOT EXISTS` run at connect;
there is no separate migration step at this stage of the project. The restart
window is the p99 of `restore()` plus process start — seconds, during which
the reverse proxy returns 502s and producers retry against the bus semantics
(or buffer, if you run without the bus).

**Post-deploy smoke test** (put it in your deploy script):

```bash
BASE=https://pulsefeed.example.com; KEY=<an-api-key>
curl -sf $BASE/healthz > /dev/null
curl -sf $BASE/readyz | python3 -c "
import json,sys; r=json.load(sys.stdin)
assert r['auth'] == 'enabled', 'AUTH IS OFF'
assert r['durability']['sink'] == 'postgres', 'MEMORY-ONLY SINK'
print('readyz ok:', r['scorer'], r['durability'])"
curl -sf -X POST $BASE/v1/events -H "x-api-key: $KEY" -H 'content-type: application/json' \
     -d '{"source":"deploy-smoke","content":"deployment smoke test"}' > /dev/null
curl -sf "$BASE/v1/timeline?limit=1" -H "x-api-key: $KEY" > /dev/null && echo "smoke ok"
```

---

## 10. Scaling limits (read before adding replicas)

Stated plainly because this is where a naive "just add replicas" deployment
breaks:

**The serving path is one process.** Timeline reads come from bounded
in-process memory (that is why p99 read latency is dict-lookup shaped). Two
API replicas behind one load balancer means two divergent feeds: each consumer
in the Redis group gets a *partition* of the stream, so each replica holds
partial state, and reads bounce between them.

What actually scales today:

- **Vertically** — comfortable to tens of thousands of events/hour per the
  burst measurements (45.6× offered-load spike absorbed with peak queue 0).
- **By tenant sharding** — tenants are fully isolated by design, so run
  instance A for tenants 1–N and instance B for the rest, each with its own
  stream (`PULSEFEED_STREAM`) or its own Redis DB, routed at the proxy by key.
- **The write path alone** scales horizontally today: N stateless
  publish-only boxes could feed one consumer — but the consumer, and
  therefore serving, stays singular.

True replica scaling needs the timeline read path served from the sink
(Postgres) instead of process memory. The storage layer was built for that;
the read path has not been, and pretending otherwise in this guide would be
worse than saying it out loud.

---

## 11. Troubleshooting boots

| symptom | likely cause | fix |
|---|---|---|
| exits at start, journal shows preflight ✗ | a configured backend is unreachable | fix or unset that variable; preflight names it |
| `RuntimeError: asyncpg is required` | DSN set but `store` extra missing | `pip install -e '.[all,store]'` |
| boots, but `/readyz` durability says `memory` on a box that should be durable | `PULSEFEED_PG_DSN` not exported into the unit's environment | check `EnvironmentFile`, `systemctl show pulsefeed -p Environment` |
| POST returns 503 | bus configured, Redis down — fail-closed as designed | restore Redis; producers retry |
| POST 202 but nothing appears in the timeline | bus mode: consumer dead? | `/readyz` → `durability.ingest` counters and `bus_lag` |
| `/readyz` scorer says `LinearScoreModel` after deploying a model | bad/mismatched model file, silent fallback | run preflight with the same env; it fails loudly with the reason |
| 401 on everything | key not sent as `X-API-Key`, or not in `PULSEFEED_API_KEYS` | check header name; keys are compared exactly |
| 429 with `Retry-After` | per-tenant token bucket empty | back off per the header; raise `PULSEFEED_RATE_*` if the limit is wrong |
| first boot slow on a large database | `restore()` reloading events | expected; bound it with a smaller retention or `PULSEFEED_RESTORE=0` for a cold start |
