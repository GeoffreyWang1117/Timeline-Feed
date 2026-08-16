"""Every per-event structure must be bounded.

The system's thesis is boundedness — bounded queues, bounded entity memory,
bounded prompts. This test holds the *bookkeeping* to the same standard: run
several times more events than the configured retention through a pipeline and
assert that nothing kept a per-event record for all of them.

This is the test the replay harness could never write for us: harness runs are
finite, so a structure that grows forever looks identical to one that is
bounded. Only an explicit cap assertion tells them apart.
"""

from __future__ import annotations

import asyncio

import pytest

from pulsefeed.clock import VirtualClock
from pulsefeed.models import Event
from pulsefeed.pipeline import PipelineConfig, PulseFeedPipeline
from pulsefeed.scoring import CheapScorer, ScorerConfig

T0 = 1_700_000_000.0

# Small caps so the test is fast and the margins are unambiguous.
RETENTION = 200
CAPACITY = 100


def build(clock) -> PulseFeedPipeline:
    return PulseFeedPipeline(
        config=PipelineConfig(
            worker_count=1,
            audit_sample_rate=0.0,
            timeline_capacity=CAPACITY,
            event_retention=RETENTION,
            latency_sample_cap=RETENTION,
            feature_memory=RETENTION,
        ),
        clock=clock,
    )


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

    await pipeline.start()
    task = asyncio.create_task(feed())
    try:
        await clock.run(lambda: done and pipeline.idle)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await pipeline.stop(drain=False)


def mixed_events(n: int):
    """A stream that exercises every path: incidents, mentions, telemetry.

    Distinct entities per block so the coalescer cannot fold everything into a
    handful of clusters — the point is to force one bookkeeping entry per
    event/cluster and see whether anything retains all of them.
    """
    out = []
    for i in range(n):
        kind = i % 10
        if kind == 0:
            source, content = "pagerduty", f"SEV1 outage on svc-{i}, pool exhausted"
        elif kind < 4:
            source, content = "grafana", f"latency spike {500 + i}ms on svc-{i}"
        else:
            source, content = "telemetry", f"svc-{i} cpu at {30 + i % 60}%"
        out.append(
            Event(
                source=source,
                content=content,
                tenant_id="t",
                entity_ids=[f"svc-{i}"],
                timestamp=T0 + i * 3.0,
            )
        )
    return out


class TestPipelineBookkeepingIsBounded:
    @pytest.mark.asyncio
    async def test_no_structure_retains_every_event(self):
        clock = VirtualClock(origin=T0)
        pipeline = build(clock)
        n = RETENTION * 4
        await run(pipeline, clock, mixed_events(n))

        assert pipeline.stats.events_ingested == n

        # Retention-capped stores: allowed to hold up to the cap plus a small
        # eviction batch, never everything.
        slack = RETENTION * 2
        for name in (
            "events",
            "annotations",
            "_path_by_event",
            "_ingest_time",
            "_visible_at",
            "last_features",
        ):
            size = len(getattr(pipeline, name))
            assert size <= slack, f"pipeline.{name} grew to {size} for {n} events"

        # Index maps must not outlive the items they index.
        for name in (
            "_item_by_cluster",
            "_cluster_summary",
            "_item_by_summary",
        ):
            size = len(getattr(pipeline, name))
            assert size <= slack, f"pipeline.{name} grew to {size} for {n} events"

        # Latency samples are a rolling window, not a transcript.
        for path, samples in pipeline.end_to_end_samples.items():
            assert len(samples) <= RETENTION, (
                f"end_to_end_samples[{path}] kept {len(samples)} samples"
            )

    @pytest.mark.asyncio
    async def test_summary_store_is_bounded(self):
        clock = VirtualClock(origin=T0)
        pipeline = build(clock)
        pipeline.summaries.max_summaries = 50
        await run(pipeline, clock, mixed_events(RETENTION * 4))
        assert len(pipeline.summaries) <= 60  # cap plus eviction slack


class TestScorerStateIsBounded:
    def test_entity_burst_windows_do_not_accumulate_dead_entities(self):
        """Every new entity used to add a deque that lived forever."""
        scorer = CheapScorer(ScorerConfig(max_tracked_entities=100))
        for i in range(1000):
            scorer.score(
                Event(
                    source="telemetry",
                    content=f"svc-{i} cpu at 40%",
                    tenant_id="t",
                    entity_ids=[f"svc-{i}"],
                    timestamp=T0 + i,
                ),
                now=T0 + i,
            )
        assert len(scorer._entity_events) <= 100

    def test_novelty_windows_bound_tenant_count(self):
        """Per-tenant windows were bounded; the set of tenants was not."""
        scorer = CheapScorer(ScorerConfig(max_tracked_tenants=20))
        for i in range(200):
            scorer.score(
                Event(
                    source="slack",
                    content=f"hello {i}",
                    tenant_id=f"tenant-{i}",
                    timestamp=T0 + i,
                ),
                now=T0 + i,
            )
        assert len(scorer._recent_vectors) <= 20


class TestSchedulerSamplesAreBounded:
    def test_wait_samples_roll(self):
        from pulsefeed.clock import ManualClock
        from pulsefeed.models import EventCluster, EventFeatures, Priority
        from pulsefeed.scheduler import (
            BoundedPriorityScheduler,
            SchedulerConfig,
            WorkItem,
        )
        from pulsefeed.trigger import TriggerDecision

        clock = ManualClock(T0)
        sched = BoundedPriorityScheduler(
            SchedulerConfig(capacity={p: 10_000 for p in Priority}),
            clock=clock,
            wait_sample_cap=100,
        )
        for i in range(1000):
            event = Event(source="s", content="x", tenant_id="t", timestamp=clock.now())
            cluster = EventCluster(
                cluster_id=f"c{i}",
                tenant_id="t",
                entity_id="e",
                events=[event],
                features=EventFeatures("x", priority=Priority.P2),
                opened_at=clock.now(),
                closed_at=clock.now(),
            )
            sched.enqueue(
                WorkItem(
                    cluster=cluster,
                    priority=Priority.P2,
                    decision=TriggerDecision(invoke=True, reason="test"),
                    enqueued_at=clock.now(),
                    deadline_at=clock.now() + 1000,
                )
            )
            assert sched.try_dequeue() is not None
        assert len(sched.wait_samples) <= 100
        # Percentiles still work over the rolling window.
        assert sched.wait_percentiles()["count"] <= 100
