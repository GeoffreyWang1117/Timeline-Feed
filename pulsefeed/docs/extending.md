# Extending PulseFeed

Every extension point is a small interface with a working implementation to copy
from. The pattern throughout: **the thing you plug in cannot tell what else is
plugged in**, so a swap is a constructor argument rather than a refactor.

---

## 1. A new event source

No code required. Sources are strings; teach the scorer what they mean.

```python
from pulsefeed.scoring import SOURCE_PRIORITY, TELEMETRY_SOURCES
from pulsefeed.models import DEFAULT_DEADLINES

SOURCE_PRIORITY["jira"] = 0.45        # signal density, 0..1
DEFAULT_DEADLINES["jira"] = 1800.0    # how long an answer stays worth having
# TELEMETRY_SOURCES.add("jira")       # only if high-volume and individually dull
```

Then publish events with `source="jira"`.

**Get `entity_ids` right.** The first entity is the coalescing key, the memory
key, the episode grouping key and the ranking-boost key. A Jira event should
carry the *project* or the *issue*, not `"jira"`.

**Set a deadline that reflects reality.** It is not a timeout; it is a statement
about when the answer stops being worth its cost. Pager alerts: 30s. Weekly
digests: hours.

### Adding domain vocabulary

```python
from pulsefeed.scoring import RISK_TERMS, RECOVERY_TERMS, ACTION_TERMS

RISK_TERMS["quarantined"] = 0.8
RECOVERY_TERMS.add("remediated")
ACTION_TERMS.add("failover")
```

Deliberately small and inspectable: an on-call engineer should be able to read
these and predict what the system will treat as serious. If your lexicon is
growing past a screenful, that is the signal to fit a model instead
([training.md](training.md)).

---

## 2. A new LLM provider

```python
from pulsefeed.llm.provider import LLMProvider, ProviderResult, ProviderUnavailable

class MyProvider(LLMProvider):
    name = "anthropic"          # appears in metrics and breaker state
    model = "claude-sonnet-4"

    async def complete(self, request) -> ProviderResult:
        try:
            response = await self._client.post(..., json={
                "system": SYSTEM_PROMPT,
                "messages": [{"role": "user", "content": request.user_prompt()}],
            })
        except TimeoutError as exc:
            raise ProviderTimeout(str(exc)) from exc
        except Exception as exc:
            raise ProviderUnavailable(str(exc)) from exc

        return ProviderResult(
            payload=response["content"],      # dict or JSON string; both fine
            model=self.model,
            tokens_used=response["usage"]["total_tokens"],
            cost_usd=...,
        )
```

Then chain it:

```python
provider = ResilientProvider(MyProvider(), fallbacks=[local_vllm, MockProvider()])
```

**Raise the right exception type — it changes behaviour.**

| exception | meaning | wrapper's response |
|---|---|---|
| `ProviderUnavailable` | this provider is down | stop retrying it, move to the next |
| `ProviderTimeout` | slow, maybe transient | retry within budget and deadline |
| `ProviderError` | bad response, provider alive | retry within budget |

**Do not validate the response yourself.** Return the raw payload;
`ResilientProvider` runs it through `validate_annotation_payload`, which enforces
the schema and strips citations of events the model was not shown. Bypassing
that bypasses the containment property.

**Always keep a fallback that cannot fail.** `MockProvider` is deterministic and
offline; ending the chain with it means enrichment degrades in quality rather
than disappearing.

---

## 3. A new trigger policy

```python
from pulsefeed.trigger import TriggerPolicy, TriggerContext, UtilityWeights

class TimeOfDayPolicy(TriggerPolicy):
    """Spend more freely during business hours."""
    name = "time_of_day"

    def __init__(self, weights=None):
        self.weights = weights or UtilityWeights()

    def threshold(self, features, ctx: TriggerContext) -> float:
        hour = time.gmtime(ctx.now).tm_hour
        return 0.35 if 8 <= hour < 20 else 0.60
```

Add it to the stack:

```python
policy = CompositePolicy(policies=[
    BudgetAwarePolicy(ledger), LoadAwarePolicy(),
    DeadlineAwarePolicy(), TimeOfDayPolicy(),
])
```

**Composition: the maximum threshold binds.** Your policy can only make the
system more conservative. To make it *less* conservative you must lower the
others — this is intentional, so that adding a policy can never accidentally
open the floodgates.

**Vetoes are different.** Returning a `TriggerDecision` with `veto=True` is
categorical and final, and only the deadline policy currently does it. Do not
reach for it because your threshold "really means it": that conflation was a
real bug that silently rejected 48% of all clusters.

---

## 4. A new storage backend

Implement `EventBus` (five methods) or `PersistenceSink`, or both.

```python
class KafkaBus(EventBus):
    async def publish(self, event) -> str: ...
    async def consume(self, group, consumer, count, block_ms) -> List[DeliveredEvent]: ...
    async def ack(self, group, delivery_ids) -> int: ...
    async def reclaim_stale(self, group, consumer, min_idle_ms, count) -> List[DeliveredEvent]: ...
    async def pending_count(self, group) -> int: ...
```

The interface was drawn to be Kafka-shaped precisely so this is a swap and not a
migration. Consumer groups, explicit offsets and rebalancing map onto it
directly.

**Three things are not optional:**

1. **Explicit ack.** An event is handled only when the pipeline says so.
2. **`reclaim_stale` must honour `min_idle_ms`.** Handing back work a *live*
   consumer is holding causes double processing — this was a real bug
   (2,657 events ingested 5,146 times) caught by the chaos suite.
3. **Bounded retention.** An unbounded stream turns a slow consumer into an
   out-of-memory failure one component upstream.

For a sink, **every write must be an upsert**: at-least-once delivery means
redelivery has to be boring.

Test against the real thing:

```bash
PULSEFEED_TEST_REDIS_URL=... PULSEFEED_TEST_PG_DSN=... pytest tests/test_store.py
```

Those tests skip when the variables are unset. A mocked integration test that
asserts a client library was called proves nothing about ack, reclaim,
redelivery or vector search.

---

## 5. A new embedder

```python
from pulsefeed.embedding import Embedder

class MyEmbedder(Embedder):
    dim = 768
    def embed(self, text: str) -> List[float]: ...
    def embed_many(self, texts): ...     # batch: this is the real entry point
```

Used by coalescing (near-duplicate detection) and novelty. `TransformerEmbedder`
in `learning/encoder.py` is a worked example.

**Override `embed_many`.** Called one item at a time, a GPU encoder is *slower*
than the hashed one — kernel launch and transfer dominate at batch size 1.

**Cache aggressively.** Feeds are duplicate-heavy; that is the entire premise of
the coalescer.

**Changing the embedder changes what "similar" means.** Re-tune
`merge_similarity` and `ambiguous_similarity` afterwards — a better encoder
generally makes similarities cluster higher, and the old thresholds will
over-merge.

---

## 6. A new scorer model

```python
class MyScoreModel:
    def predict_proba(self, features, embedding=None) -> float: ...
```

`features` has exactly the keys in `FEATURE_NAMES`; `embedding` is the event's
text vector. Then `CheapScorer(model=MyScoreModel())`.

**Output a calibrated probability, not just a good ranking.** The budget-, load-
and deadline-aware thresholds all read it as a probability. A model that ranks
perfectly and is systematically overconfident silently breaks all three. See
[training.md](training.md) for the calibration stage.

**This runs on 100% of traffic.** Whatever you put here bounds the system's
throughput, latency floor and availability. If it needs a model server, it
belongs at the *second* stage, not the first.

---

## 7. A new experiment arm

```python
from harness.baselines import ARMS, ArmSpec

ARMS.append(ArmSpec(
    key="E",
    name="Learned scorer",
    description="PulseFeed with a fitted first stage.",
    coalescing=True, policy_kind="pulsefeed",
))
```

Then `python -m harness.replay --arms A,D,E`.

All arms run the *same* pipeline with different configuration, so a comparison
isolates policy rather than implementation. Keep it that way: an arm that runs
different code is measuring the code.

---

## 8. A new failure scenario

```python
async def scenario_dns_flap() -> ScenarioResult:
    result = ScenarioResult("dns flap", passed=True)
    events = generate_trace(TraceConfig(duration_seconds=600, seed=42))

    def break_dns(pipeline):
        pipeline.provider.providers[0].set_failure_rate(0.5)

    pipeline, _ = await _run_with_injections(events, [(200.0, break_dns)])

    result.check("feed kept producing", len(pipeline.timeline("acme")) > 0)
    result.findings = {"enriched": pipeline.stats.enriched}
    return result

SCENARIOS["dns"] = scenario_dns_flap
```

**Assert the invariant, not the number.** "Every important event still reached
the feed" survives a retune; "exactly 47 items were shed" does not.

**Make sure the scenario can fail.** Two of these originally passed trivially —
one because the queue never filled, another because the injected latency
exceeded the provider timeout and exercised the *outage* path instead of the
*slow* path. Both were rewritten to actually test what they claimed. When you
add a scenario, break the system on purpose first and confirm it goes red.

---

## 9. What not to extend

- **Do not add tool calling to the enrichment prompt.** Feed content is
  untrusted input; the reason a successful injection can only produce a wrong
  summary is that there is nothing to call.
- **Do not let annotations mutate events.** Every guarantee in this system rests
  on raw events being canonical.
- **Do not serve reads from the sink.** Putting a database on the read path of a
  feed whose premise is surviving its dependencies defeats the purpose.
- **Do not call `asyncio.sleep` with a nonzero duration.** Use the clock, or
  simulated-time replay silently breaks.
