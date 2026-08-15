"""A single readable run of the worked example from the project plan.

    python demo.py

Seven events arrive over nine minutes. A chronological feed shows seven lines.
PulseFeed groups them, decides which are worth an LLM call, and produces a feed
you can read in one breath — with every line expandable back to the raw events
it came from.

This is the "look, it works" script. The claims live in ``harness/replay.py``.
"""

from __future__ import annotations

import asyncio

from pulsefeed.budget import BudgetLedger, TenantPlan
from pulsefeed.clock import VirtualClock
from pulsefeed.llm.provider import MockConfig, MockProvider, ResilientProvider
from pulsefeed.models import Event
from pulsefeed.pipeline import PipelineConfig, PulseFeedPipeline
from pulsefeed.scoring import CheapScorer, TenantAffinity
from pulsefeed.trigger import build_default_policy

T0 = 1_700_000_000.0
TENANT = "acme"

SCRIPT = [
    (0, "grafana", "checkout latency rises to 700ms", "checkout-service"),
    (60, "slack", "engineer mentions DB timeout in checkout", "checkout-service"),
    (120, "grafana", "connection pool saturation on db-primary", "db-primary"),
    (240, "github", "deployment #813 completed", "checkout-service"),
    (300, "grafana", "checkout latency rises to 1.5s", "checkout-service"),
    (360, "slack", "starting rollback of deployment #813", "checkout-service"),
    (480, "grafana", "checkout latency returned to normal, resolved", "checkout-service"),
]

# Background chatter, so the incident has to be found rather than handed over.
NOISE = [
    (30, "telemetry", "search-service cpu at 34%", "search-service"),
    (45, "telemetry", "search-service cpu at 36%", "search-service"),
    (75, "telemetry", "search-service cpu at 35%", "search-service"),
    (90, "ci", "build #442 passed on main", "web-frontend"),
    (150, "slack", "anyone up for lunch?", "general"),
    (200, "github", "merged PR #221: update readme", "web-frontend"),
    (260, "rss", "industry blog post on platform engineering", "news"),
    (400, "telemetry", "search-service cpu at 33%", "search-service"),
]


def build_events():
    rows = [(t, s, c, e, True) for t, s, c, e in SCRIPT]
    rows += [(t, s, c, e, False) for t, s, c, e in NOISE]
    rows.sort(key=lambda r: r[0])
    return [
        Event(
            source=source,
            content=content,
            tenant_id=TENANT,
            entity_ids=[entity],
            timestamp=T0 + offset,
            metadata={"incident": is_incident},
        )
        for offset, source, content, entity, is_incident in rows
    ]


def build_pipeline(clock):
    ledger = BudgetLedger()
    ledger.register(TenantPlan.pro(TENANT))
    scorer = CheapScorer()
    scorer.set_affinity(TENANT, TenantAffinity(watched_entities={"checkout-service"}))
    return PulseFeedPipeline(
        provider=ResilientProvider(
            MockProvider(MockConfig(base_latency=0.3), clock=clock), clock=clock
        ),
        config=PipelineConfig(worker_count=2, audit_sample_rate=0.0),
        clock=clock,
        ledger=ledger,
        scorer=scorer,
        policy=build_default_policy(ledger, audit_sample_rate=0.0),
    )


async def main() -> None:
    events = build_events()
    clock = VirtualClock(origin=T0)
    pipeline = build_pipeline(clock)

    done = False

    async def feed():
        nonlocal done
        try:
            for event in events:
                delay = event.timestamp - clock.now()
                if delay > 0:
                    await clock.sleep(delay)
                await pipeline.ingest(event)
            await pipeline.flush()
        finally:
            done = True

    await pipeline.start()
    task = asyncio.create_task(feed())
    try:
        await clock.run(lambda: done and pipeline.idle)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await pipeline.stop(drain=False)

    print("=" * 78)
    print(f"RAW EVENT STREAM — what a chronological feed shows ({len(events)} lines)")
    print("=" * 78)
    for event in events:
        mark = "!" if event.metadata.get("incident") else " "
        clock_str = f"{int((event.timestamp - T0) // 60):02d}:{int((event.timestamp - T0) % 60):02d}"
        print(f" {mark} {clock_str}  [{event.source:<9}] {event.content}")

    items = pipeline.timeline(TENANT, limit=10)
    print()
    print("=" * 78)
    print(f"PULSEFEED TIMELINE — top {len(items)} ranked items")
    print("=" * 78)
    for i, item in enumerate(items, 1):
        badge = "LLM" if item.enriched else "cheap"
        print(
            f"\n{i}. [{item.severity.value:<8}] ({badge}, {item.event_count} "
            f"event{'s' if item.event_count != 1 else ''}, score {item.rank_score:.2f})"
        )
        print(f"   {item.title}")
        evidence = pipeline.evidence_for(item)
        for event in evidence[:4]:
            print(f"     └─ {event.source}: {event.content[:64]}")
        if len(evidence) > 4:
            print(f"     └─ ... and {len(evidence) - 4} more")

    memory = pipeline.entities.get(TENANT, "checkout-service")
    if memory:
        print()
        print("=" * 78)
        print("ENTITY MEMORY — checkout-service")
        print("=" * 78)
        print(memory.context_block())

    stats = pipeline.stats
    print()
    print("=" * 78)
    print("WHAT IT COST")
    print("=" * 78)
    print(f"  events ingested       {stats.events_ingested}")
    print(f"  clusters formed       {stats.clusters_emitted}")
    print(f"  events coalesced away {stats.coalesced_events}")
    print(f"  LLM calls made        {stats.enriched}")
    print(f"  LLM calls skipped     {stats.skipped}")
    print(f"  tokens                {stats.tokens_used:,}")
    print(f"  cost                  ${stats.cost_usd:.4f}")
    print(f"  skip reasons          {dict(pipeline.skip_reasons)}")
    print()
    print(
        f"  → {stats.enriched} LLM call(s) for {stats.events_ingested} events "
        f"({stats.enriched / max(1, stats.events_ingested):.1%} invocation rate)"
    )


if __name__ == "__main__":
    asyncio.run(main())
