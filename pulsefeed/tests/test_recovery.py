"""Restart recovery, and cheap events joining episodes."""

from __future__ import annotations

import asyncio

import pytest

from pulsefeed.clock import VirtualClock
from pulsefeed.models import (
    Event,
    Severity,
    Summary,
    SummaryLevel,
    SummaryStatus,
    new_id,
)
from pulsefeed.pipeline import PipelineConfig, PulseFeedPipeline
from pulsefeed.store import InMemorySink

T0 = 1_700_000_000.0

INCIDENT = [
    (0, "grafana", "checkout latency rises to 700ms", "checkout"),
    (60, "slack", "engineer mentions DB timeout in checkout", "checkout"),
    (150, "github", "deployment #813 completed", "checkout"),
    (200, "grafana", "checkout latency rises to 1.5s", "checkout"),
    (280, "slack", "starting rollback of deployment #813", "checkout"),
    (400, "grafana", "checkout latency returned to normal, resolved", "checkout"),
]


def incident_events(tenant: str = "t"):
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


def build(clock, sink=None, **overrides) -> PulseFeedPipeline:
    return PulseFeedPipeline(
        config=PipelineConfig(
            worker_count=overrides.pop("workers", 2),
            audit_sample_rate=0.0,
            episode_interval=overrides.pop("episode_interval", 60.0),
        ),
        clock=clock,
        sink=sink,
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


class TestEpisodeAbsorbsCheapEvents:
    @pytest.mark.asyncio
    async def test_a_boring_but_central_event_joins_its_incident(self):
        """"deployment #813 completed" is lexically dull and causally central.

        It never earns an LLM call on its own, and before this it appeared as a
        separate row outside the very episode that describes rolling it back.
        """
        clock = VirtualClock(origin=T0)
        pipeline = build(clock)
        events = incident_events()
        await run(pipeline, clock, events)

        deploy = next(e for e in events if "deployment #813 completed" in e.content)
        episodes = [
            item
            for item in pipeline.timeline("t", limit=50)
            if item.kind == "episode"
        ]
        assert episodes, "no episode was formed"
        assert any(
            deploy.event_id in episode.source_event_ids for episode in episodes
        ), "the deploy stayed outside the incident it belongs to"

    @pytest.mark.asyncio
    async def test_quiet_entities_do_not_get_micro_summaries(self):
        """The rule is narrow on purpose. Emitting a micro summary for every
        cheap cluster would turn episodes into digests of noise."""
        clock = VirtualClock(origin=T0)
        pipeline = build(clock)
        noise = [
            Event(
                source="telemetry",
                content=f"web cpu at {30 + i % 4}%",
                tenant_id="t",
                entity_ids=["web"],
                timestamp=T0 + i * 45,
            )
            for i in range(30)
        ]
        await run(pipeline, clock, noise)

        assert pipeline.stats.micro_summaries == 0
        assert pipeline.stats.enriched == 0

    @pytest.mark.asyncio
    async def test_micro_summaries_are_marked_as_cheap_not_enriched(self):
        clock = VirtualClock(origin=T0)
        pipeline = build(clock)
        await run(pipeline, clock, incident_events())

        micro = [
            s
            for s in pipeline.summaries.active(SummaryLevel.MICRO, "t")
            if s.model == "cheap"
        ]
        for summary in micro:
            assert summary.confidence < 0.5, "a description is not a conclusion"


class TestRestartRecovery:
    async def _run_and_restore(self, sink):
        clock = VirtualClock(origin=T0)
        first = build(clock, sink=sink)
        await run(first, clock, incident_events())

        # A fresh process, same durable record.
        restored = build(VirtualClock(origin=T0 + 10_000), sink=sink)
        counts = await restored.restore("t")
        return first, restored, counts

    @pytest.mark.asyncio
    async def test_events_entities_and_summaries_come_back(self):
        sink = InMemorySink()
        first, restored, counts = await self._run_and_restore(sink)

        assert counts["events"] == first.stats.events_ingested
        assert counts["entities"] > 0
        assert counts["summaries"] > 0
        assert restored.timeline("t", limit=50), "restored feed is empty"

    @pytest.mark.asyncio
    async def test_restored_items_can_still_expand_to_evidence(self):
        """A restored conclusion whose evidence 404s is worse than no row."""
        sink = InMemorySink()
        _, restored, _ = await self._run_and_restore(sink)

        for item in restored.timeline("t", limit=50):
            evidence = restored.evidence_for(item)
            assert evidence, f"item {item.title!r} lost its evidence"
            assert len(evidence) == len(item.source_event_ids)

    @pytest.mark.asyncio
    async def test_entity_state_survives_so_ranking_stays_aware(self):
        sink = InMemorySink()
        _, restored, _ = await self._run_and_restore(sink)

        memory = restored.entities.get("t", "checkout")
        assert memory is not None
        assert memory.current_state in ("degraded", "recovering")
        assert memory.event_count > 0

    @pytest.mark.asyncio
    async def test_superseded_beliefs_are_not_resurrected(self):
        """They stay on disk for the audit trail; they do not come back as rows."""
        sink = InMemorySink()
        retracted = Summary(
            summary_id=new_id("sum"),
            tenant_id="t",
            level=SummaryLevel.CLUSTER,
            text="suspected database issue",
            source_event_ids=["e1"],
            entity_ids=["checkout"],
            severity=Severity.CRITICAL,
            status=SummaryStatus.SUPERSEDED,
            generated_at=T0,
        )
        await sink.save_summary(retracted)

        restored = build(VirtualClock(origin=T0), sink=sink)
        await restored.restore("t")
        titles = [i.title for i in restored.timeline("t", limit=50)]
        assert "suspected database issue" not in titles

    @pytest.mark.asyncio
    async def test_restore_is_a_no_op_without_a_sink(self):
        restored = build(VirtualClock(origin=T0))
        counts = await restored.restore("t")
        assert counts == {"events": 0, "entities": 0, "summaries": 0, "items": 0}

    @pytest.mark.asyncio
    async def test_restored_pipeline_keeps_ingesting(self):
        """Recovery must hand back a working pipeline, not a museum."""
        sink = InMemorySink()
        _, restored, _ = await self._run_and_restore(sink)
        before = len(restored.timeline("t", limit=10 ** 6))

        clock = restored.clock
        new_event = Event(
            source="pagerduty",
            content="SEV1 checkout outage, connection pool exhausted",
            tenant_id="t",
            entity_ids=["checkout"],
            timestamp=clock.now(),
        )
        # flush() can await the provider (episode narration), so under a virtual
        # clock it has to run as a task *alongside* the driver rather than
        # before it — otherwise it parks on a timer nobody is there to fire.
        await run(restored, clock, [new_event])

        after = restored.timeline("t", limit=10 ** 6)
        assert len(after) > before
        covered = set()
        for item in after:
            covered.update(item.source_event_ids)
        assert new_event.event_id in covered
