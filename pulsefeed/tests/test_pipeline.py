"""End-to-end pipeline behaviour under simulated time."""

from __future__ import annotations

import pytest

from pulsefeed.budget import BudgetLedger, TenantPlan
from pulsefeed.clock import VirtualClock
from pulsefeed.coalescer import Coalescer, CoalescerConfig
from pulsefeed.llm.provider import MockConfig, MockProvider, ResilientProvider
from pulsefeed.metrics import PulseFeedMetrics
from pulsefeed.models import Event, PathTaken, Priority, Severity
from pulsefeed.pipeline import PipelineConfig, PulseFeedPipeline
from pulsefeed.scheduler import BoundedPriorityScheduler, SchedulerConfig
from pulsefeed.scoring import CheapScorer, TenantAffinity
from pulsefeed.trigger import build_default_policy

T0 = 1_700_000_000.0

# The worked example from the project plan.
INCIDENT = [
    (60, "grafana", "checkout latency rises to 700ms", "checkout"),
    (120, "slack", "engineer mentions DB timeout in checkout", "checkout"),
    (180, "grafana", "connection pool saturation on db-primary", "db-primary"),
    (300, "github", "deployment #813 completed", "checkout"),
    (360, "grafana", "checkout latency rises to 1.5s", "checkout"),
    (420, "slack", "starting rollback of deployment #813", "checkout"),
    (540, "grafana", "checkout latency returned to normal, resolved", "checkout"),
]


def build(clock, **overrides):
    ledger = BudgetLedger()
    ledger.register(
        TenantPlan(
            "t",
            daily_token_budget=overrides.pop("token_budget", 5_000_000),
            daily_usd_budget=overrides.pop("usd_budget", 50.0),
        )
    )
    scorer = CheapScorer()
    scorer.set_affinity("t", TenantAffinity(watched_actors={"alice"}))
    pipeline = PulseFeedPipeline(
        provider=overrides.pop(
            "provider",
            ResilientProvider(
                MockProvider(MockConfig(base_latency=0.2), clock=clock), clock=clock
            ),
        ),
        config=PipelineConfig(
            worker_count=overrides.pop("workers", 2),
            audit_sample_rate=overrides.pop("audit_rate", 0.0),
        ),
        clock=clock,
        ledger=ledger,
        scorer=scorer,
        coalescer=overrides.pop("coalescer", Coalescer(CoalescerConfig())),
        scheduler=overrides.pop(
            "scheduler", BoundedPriorityScheduler(SchedulerConfig(), clock=clock)
        ),
        policy=overrides.pop("policy", build_default_policy(ledger, audit_sample_rate=0.0)),
        metrics=PulseFeedMetrics(),
    )
    return pipeline


async def run(pipeline, clock, events):
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

    import asyncio

    await pipeline.start()
    task = asyncio.create_task(feed())
    try:
        await clock.run(lambda: done and pipeline.idle)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await pipeline.stop(drain=False)


def incident_events(tenant="t"):
    return [
        Event(
            source=source,
            content=content,
            tenant_id=tenant,
            entity_ids=[entity],
            timestamp=T0 + offset,
        )
        for offset, source, content, entity in INCIDENT
    ]


class TestPipelineHappyPath:
    @pytest.mark.asyncio
    async def test_incident_is_enriched_and_traceable(self):
        clock = VirtualClock(origin=T0)
        pipeline = build(clock)
        events = incident_events()
        await run(pipeline, clock, events)

        items = pipeline.timeline("t", limit=50)
        assert items, "the feed produced nothing"

        covered = set()
        for item in items:
            covered.update(item.source_event_ids)
        assert covered == {e.event_id for e in events}, "an event vanished"

        assert pipeline.stats.enriched > 0, "nothing in the incident was enriched"
        enriched = [i for i in items if i.enriched]
        assert enriched

        # Every conclusion expands back to the raw facts behind it.
        for item in enriched:
            evidence = pipeline.evidence_for(item)
            assert evidence
            assert len(evidence) == len(item.source_event_ids)

    @pytest.mark.asyncio
    async def test_entity_memory_accumulates_across_the_incident(self):
        clock = VirtualClock(origin=T0)
        pipeline = build(clock)
        await run(pipeline, clock, incident_events())

        memory = pipeline.entities.get("t", "checkout")
        assert memory is not None
        assert memory.event_count >= 5
        assert memory.recent_summary  # an LLM conclusion was folded in

    @pytest.mark.asyncio
    async def test_noise_does_not_trigger_the_llm(self):
        clock = VirtualClock(origin=T0)
        pipeline = build(clock)
        noise = [
            Event(
                source="telemetry",
                content=f"web-frontend cpu at {30 + i % 5}%",
                tenant_id="t",
                entity_ids=["web-frontend"],
                timestamp=T0 + i * 30,
            )
            for i in range(40)
        ]
        await run(pipeline, clock, noise)

        assert pipeline.stats.enriched == 0, "spent LLM calls on pure telemetry noise"
        assert pipeline.stats.events_ingested == 40
        covered = set()
        for item in pipeline.timeline("t", limit=10 ** 6):
            covered.update(item.source_event_ids)
        assert len(covered) == 40, "noise was dropped rather than cheaply summarised"


class TestDegradation:
    @pytest.mark.asyncio
    async def test_feed_survives_a_total_provider_outage(self):
        """The core claim: with the LLM gone, the timeline still works."""
        clock = VirtualClock(origin=T0)
        mock = MockProvider(MockConfig(base_latency=0.2), clock=clock)
        mock.set_outage(True)
        pipeline = build(clock, provider=ResilientProvider(mock, clock=clock))

        events = incident_events()
        await run(pipeline, clock, events)

        items = pipeline.timeline("t", limit=50)
        covered = set()
        for item in items:
            covered.update(item.source_event_ids)

        assert covered == {e.event_id for e in events}
        assert pipeline.stats.enriched == 0
        assert all(not i.enriched for i in items)
        assert all(i.title for i in items), "cheap descriptions must be non-empty"
        # And the severity signal survives without the model.
        assert any(i.severity.rank >= Severity.WARNING.rank for i in items)

    @pytest.mark.asyncio
    async def test_budget_exhaustion_stops_spending_but_not_the_feed(self):
        clock = VirtualClock(origin=T0)
        pipeline = build(clock, token_budget=1, usd_budget=0.000001)
        events = incident_events()
        await run(pipeline, clock, events)

        assert pipeline.stats.enriched == 0
        assert pipeline.skip_reasons.get("budget_exhausted", 0) > 0
        covered = set()
        for item in pipeline.timeline("t", limit=50):
            covered.update(item.source_event_ids)
        assert covered == {e.event_id for e in events}

    @pytest.mark.asyncio
    async def test_p0_bypasses_an_exhausted_shared_budget(self):
        """The reservation exists so a noisy day cannot consume the capacity an
        incident will need."""
        ledger = BudgetLedger()
        ledger.register(
            TenantPlan(
                "t",
                daily_token_budget=100_000,
                daily_usd_budget=1.0,
                reserved_fraction_p0=0.5,
            )
        )
        # Spend everything a non-P0 request is allowed to touch.
        ledger.charge("t", 50_000, 0.5, T0)

        assert not ledger.can_afford("t", 1000, 0.01, Priority.P2, T0)
        assert ledger.can_afford("t", 1000, 0.01, Priority.P0, T0)


class TestPathAccounting:
    @pytest.mark.asyncio
    async def test_every_event_lands_on_a_known_path(self):
        clock = VirtualClock(origin=T0)
        pipeline = build(clock)
        events = incident_events() + [
            Event(
                source="telemetry",
                content=f"db cpu {80 + i}%",
                tenant_id="t",
                entity_ids=["db-primary"],
                timestamp=T0 + 600 + i * 3,
            )
            for i in range(10)
        ]
        await run(pipeline, clock, events)

        paths = {e.event_id: pipeline.path_of(e.event_id) for e in events}
        assert set(paths.values()) <= {
            PathTaken.CHEAP,
            PathTaken.LLM,
            PathTaken.LLM_FAILED,
            PathTaken.DROPPED,
        }
        assert PathTaken.LLM in paths.values()
        assert PathTaken.CHEAP in paths.values()

    @pytest.mark.asyncio
    async def test_snapshot_is_serialisable_and_complete(self):
        clock = VirtualClock(origin=T0)
        pipeline = build(clock)
        await run(pipeline, clock, incident_events())
        snapshot = pipeline.snapshot()
        for key in (
            "stats",
            "skip_reasons",
            "scheduler",
            "queue_wait",
            "provider",
            "coalescer",
            "end_to_end_llm",
        ):
            assert key in snapshot
        import json

        json.dumps(snapshot, default=str)


class TestAuditSampling:
    @pytest.mark.asyncio
    async def test_audit_results_never_reach_the_timeline(self):
        """Audit calls measure the trigger's false negatives. If their output
        leaked into the feed, the measurement would be measuring itself."""
        clock = VirtualClock(origin=T0)
        pipeline = build(clock, audit_rate=1.0)
        pipeline.policy = build_default_policy(pipeline.ledger, audit_sample_rate=1.0)

        noise = [
            Event(
                source="telemetry",
                content=f"web cpu at {20 + i}%",
                tenant_id="t",
                entity_ids=["web"],
                timestamp=T0 + i * 40,
            )
            for i in range(12)
        ]
        await run(pipeline, clock, noise)

        assert pipeline.stats.audit_samples > 0
        assert all(not i.enriched for i in pipeline.timeline("t", limit=100))

    @pytest.mark.asyncio
    async def test_miss_estimate_is_zero_without_samples(self):
        clock = VirtualClock(origin=T0)
        pipeline = build(clock, audit_rate=0.0)
        await run(pipeline, clock, incident_events())
        assert pipeline.estimated_missed_important_events() == 0.0
