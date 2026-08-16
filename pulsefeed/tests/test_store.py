"""Storage tests.

The in-memory implementations always run. The Redis and Postgres tests run
against real servers when ``PULSEFEED_TEST_REDIS_URL`` / ``PULSEFEED_TEST_PG_DSN``
are set, and skip otherwise — a skipped integration test is honest, a mocked one
that asserts a client library was called is not.
"""

from __future__ import annotations

import os

import pytest

from pulsefeed.clock import VirtualClock
from pulsefeed.embedding import HashingEmbedder
from pulsefeed.ingest import IngestConfig, IngestWorker
from pulsefeed.memory import EntityMemory
from pulsefeed.models import (
    Event,
    EventFeatures,
    Priority,
    SemanticAnnotation,
    Severity,
    Summary,
    SummaryLevel,
    SummaryStatus,
    new_id,
)
from pulsefeed.pipeline import PipelineConfig, PulseFeedPipeline
from pulsefeed.store import InMemoryEventBus, InMemorySink

REDIS_URL = os.environ.get("PULSEFEED_TEST_REDIS_URL")
PG_DSN = os.environ.get("PULSEFEED_TEST_PG_DSN")

T0 = 1_700_000_000.0


def ev(content: str = "checkout latency high", entity: str = "checkout") -> Event:
    return Event(
        source="grafana",
        content=content,
        tenant_id="t",
        entity_ids=[entity],
        timestamp=T0,
    )


def annotation(event_ids) -> SemanticAnnotation:
    return SemanticAnnotation(
        annotation_id=new_id("ann"),
        tenant_id="t",
        summary="checkout is degraded",
        source_event_ids=list(event_ids),
        category="incident",
        severity=Severity.ERROR,
        entities=["checkout"],
        confidence=0.8,
        model="mock",
        provider="mock",
        tokens_used=120,
        cost_usd=0.0002,
    )


def summary(text="checkout degraded", events=("e1",)) -> Summary:
    return Summary(
        summary_id=new_id("sum"),
        tenant_id="t",
        level=SummaryLevel.CLUSTER,
        text=text,
        source_event_ids=list(events),
        entity_ids=["checkout"],
        severity=Severity.ERROR,
        confidence=0.8,
        model="mock",
        generated_at=T0,
    )


# --------------------------------------------------------------------------
# Bus semantics — asserted against the in-memory bus and, when available, Redis
# --------------------------------------------------------------------------


class TestInMemoryBus:
    @pytest.mark.asyncio
    async def test_publish_then_consume(self):
        bus = InMemoryEventBus()
        event = ev()
        await bus.publish(event)
        delivered = await bus.consume("g", "c")
        assert [d.event.event_id for d in delivered] == [event.event_id]

    @pytest.mark.asyncio
    async def test_unacked_deliveries_stay_pending(self):
        bus = InMemoryEventBus()
        await bus.publish(ev())
        await bus.consume("g", "c")
        assert await bus.pending_count("g") == 1
        await bus.consume("g", "c")  # no redelivery without reclaim
        assert await bus.pending_count("g") == 1

    @pytest.mark.asyncio
    async def test_ack_clears_pending(self):
        bus = InMemoryEventBus()
        await bus.publish(ev())
        delivered = await bus.consume("g", "c")
        assert await bus.ack("g", [d.delivery_id for d in delivered]) == 1
        assert await bus.pending_count("g") == 0

    @pytest.mark.asyncio
    async def test_reclaim_returns_work_a_dead_consumer_abandoned(self):
        bus = InMemoryEventBus()
        await bus.publish(ev())
        await bus.consume("g", "dead-consumer")
        reclaimed = await bus.reclaim_stale("g", "live-consumer", min_idle_ms=0)
        assert len(reclaimed) == 1
        assert reclaimed[0].delivery_count > 1

    @pytest.mark.asyncio
    async def test_groups_are_independent(self):
        bus = InMemoryEventBus()
        await bus.publish(ev())
        assert len(await bus.consume("a", "c")) == 1
        assert len(await bus.consume("b", "c")) == 1

    @pytest.mark.asyncio
    async def test_buffer_is_bounded(self):
        """An unbounded ingestion buffer just relocates the OOM."""
        bus = InMemoryEventBus(maxlen=10)
        for i in range(25):
            await bus.publish(ev(f"event {i}"))
        assert bus.dropped == 15


class TestInMemorySink:
    @pytest.mark.asyncio
    async def test_round_trips_an_event(self):
        sink = InMemorySink()
        event = ev()
        await sink.save_event(event, EventFeatures(event.event_id, importance=0.7))
        loaded = await sink.load_events("t")
        assert [e.event_id for e in loaded] == [event.event_id]

    @pytest.mark.asyncio
    async def test_similarity_search(self):
        sink, embedder = InMemorySink(), HashingEmbedder()
        near = ev("database connection pool exhausted")
        far = ev("anyone up for lunch?", "general")
        for event in (near, far):
            await sink.save_event(event, embedding=embedder.embed(event.content))
        results = await sink.similar_events(
            "t", embedder.embed("connection pool is exhausted"), limit=1
        )
        assert results[0][0] == near.event_id

    @pytest.mark.asyncio
    async def test_tenant_isolation_in_search(self):
        sink, embedder = InMemorySink(), HashingEmbedder()
        other = Event(source="s", content="secret plan", tenant_id="other")
        await sink.save_event(other, embedding=embedder.embed(other.content))
        assert await sink.similar_events("t", embedder.embed("secret plan")) == []

    @pytest.mark.asyncio
    async def test_entity_history_includes_superseded(self):
        sink = InMemorySink()
        old = summary("suspected database issue")
        old.status = SummaryStatus.SUPERSEDED
        await sink.save_summary(old)
        await sink.save_summary(summary("root cause was DNS"))
        history = await sink.entity_history("t", "checkout")
        assert len(history) == 2


# --------------------------------------------------------------------------
# Pipeline integration
# --------------------------------------------------------------------------


class TestWriteThrough:
    @pytest.mark.asyncio
    async def test_pipeline_persists_events_and_conclusions(self):
        clock = VirtualClock(origin=T0)
        sink = InMemorySink()
        pipeline = PulseFeedPipeline(
            config=PipelineConfig(worker_count=1, audit_sample_rate=0.0),
            clock=clock,
            sink=sink,
        )
        events = [
            Event(
                source="pagerduty",
                content="SEV1 checkout outage, connection pool exhausted",
                tenant_id="t",
                entity_ids=["checkout"],
                timestamp=T0 + i * 30,
            )
            for i in range(3)
        ]

        import asyncio

        done = False

        async def feed():
            nonlocal done
            try:
                for event in events:
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

        assert len(sink.events) == 3
        assert len(sink.embeddings) == 3
        assert pipeline.sink_failures == 0
        assert sink.annotations, "no annotation was persisted"
        assert sink.summaries, "no summary was persisted"

    @pytest.mark.asyncio
    async def test_a_broken_sink_does_not_stop_the_feed(self):
        """Persistence is fail-open: the serving path does not depend on it."""

        class BrokenSink(InMemorySink):
            async def save_event(self, *args, **kwargs):
                raise RuntimeError("disk on fire")

        clock = VirtualClock(origin=T0)
        pipeline = PulseFeedPipeline(
            config=PipelineConfig(worker_count=1, audit_sample_rate=0.0),
            clock=clock,
            sink=BrokenSink(),
        )
        await pipeline.ingest(ev())
        assert pipeline.sink_failures == 1
        assert pipeline.stats.events_ingested == 1
        assert pipeline.timeline("t", limit=10)


class TestIngestWorker:
    @pytest.mark.asyncio
    async def test_consumes_bus_into_pipeline_and_acks(self):
        clock = VirtualClock(origin=T0)
        bus = InMemoryEventBus()
        pipeline = PulseFeedPipeline(
            config=PipelineConfig(worker_count=1, audit_sample_rate=0.0), clock=clock
        )
        worker = IngestWorker(bus, pipeline, IngestConfig(block_ms=0))

        for i in range(5):
            await bus.publish(ev(f"checkout latency {700 + i}ms"))

        processed = await worker.drain_once()
        assert processed == 5
        assert pipeline.stats.events_ingested == 5
        assert await bus.pending_count("pulsefeed") == 0

    @pytest.mark.asyncio
    async def test_failed_processing_leaves_the_event_unacked(self):
        """Redelivery beats silent loss."""
        clock = VirtualClock(origin=T0)
        bus = InMemoryEventBus()
        pipeline = PulseFeedPipeline(
            config=PipelineConfig(worker_count=1, audit_sample_rate=0.0), clock=clock
        )

        async def explode(_event):
            raise RuntimeError("processing failed")

        pipeline.ingest = explode  # type: ignore[assignment]
        worker = IngestWorker(bus, pipeline, IngestConfig(block_ms=0))
        await bus.publish(ev())

        assert await worker.drain_once() == 0
        assert worker.stats["failed"] == 1
        assert await bus.pending_count("pulsefeed") == 1


# --------------------------------------------------------------------------
# Real backends
# --------------------------------------------------------------------------

redis_test = pytest.mark.skipif(
    not REDIS_URL, reason="set PULSEFEED_TEST_REDIS_URL to run Redis tests"
)
pg_test = pytest.mark.skipif(
    not PG_DSN, reason="set PULSEFEED_TEST_PG_DSN to run Postgres tests"
)


@redis_test
class TestRedisStreamBus:
    @pytest.fixture
    async def bus(self):
        from pulsefeed.store import RedisStreamBus

        instance = RedisStreamBus(
            url=REDIS_URL, stream=f"pulsefeed:test:{new_id('s')}"
        )
        yield instance
        client = await instance._redis()
        await client.delete(instance.stream)
        await instance.close()

    @pytest.mark.asyncio
    async def test_round_trip_preserves_the_event(self, bus):
        event = ev("connection pool exhausted on db-primary")
        await bus.publish(event)
        delivered = await bus.consume("g", "c", block_ms=100)
        assert len(delivered) == 1
        restored = delivered[0].event
        assert restored.event_id == event.event_id
        assert restored.content == event.content
        assert restored.entity_ids == event.entity_ids
        assert restored.timestamp == pytest.approx(event.timestamp)

    @pytest.mark.asyncio
    async def test_ack_removes_from_pending(self, bus):
        await bus.publish(ev())
        delivered = await bus.consume("g", "c", block_ms=100)
        assert await bus.pending_count("g") == 1
        await bus.ack("g", [d.delivery_id for d in delivered])
        assert await bus.pending_count("g") == 0

    @pytest.mark.asyncio
    async def test_unacked_work_is_reclaimable_by_another_consumer(self, bus):
        await bus.publish(ev())
        await bus.consume("g", "consumer-that-dies", block_ms=100)
        reclaimed = await bus.reclaim_stale("g", "survivor", min_idle_ms=0)
        assert len(reclaimed) == 1

    @pytest.mark.asyncio
    async def test_replay_after_restart(self, bus):
        """Durability: a new consumer group sees everything from the start."""
        for i in range(10):
            await bus.publish(ev(f"event {i}"))
        await bus.consume("group-a", "c", count=10, block_ms=100)
        fresh = await bus.consume("group-b", "c", count=10, block_ms=100)
        assert len(fresh) == 10

    @pytest.mark.asyncio
    async def test_pipelined_publish(self, bus):
        ids = await bus.publish_many([ev(f"batch {i}") for i in range(50)])
        assert len(ids) == 50
        assert await bus.length() == 50

    @pytest.mark.asyncio
    async def test_stream_length_is_bounded(self, bus):
        bus.maxlen = 100
        await bus.publish_many([ev(f"e{i}") for i in range(500)])
        await bus.trim(100)
        assert await bus.length() <= 200  # approximate trimming


@pg_test
class TestPostgresSink:
    @pytest.fixture
    async def sink(self):
        from pulsefeed.store import PostgresSink

        instance = await PostgresSink(dsn=PG_DSN).connect()
        async with instance._pool.acquire() as conn:
            await conn.execute(
                "TRUNCATE pf_events, pf_annotations, pf_summaries, pf_entity_memory"
            )
        yield instance
        await instance.close()

    @pytest.mark.asyncio
    async def test_pgvector_is_actually_in_use(self, sink):
        assert sink.has_pgvector, "expected the vector extension to be available"

    @pytest.mark.asyncio
    async def test_event_round_trip(self, sink):
        event = ev("connection pool exhausted")
        await sink.save_event(
            event,
            EventFeatures(event.event_id, importance=0.8, risk=0.9, priority=Priority.P0),
            HashingEmbedder().embed(event.content),
        )
        loaded = await sink.load_events("t")
        assert len(loaded) == 1
        assert loaded[0].event_id == event.event_id
        assert loaded[0].content == event.content
        assert loaded[0].entity_ids == ["checkout"]

    @pytest.mark.asyncio
    async def test_redelivery_is_idempotent(self, sink):
        """At-least-once delivery means the same event will arrive twice."""
        event = ev()
        await sink.save_event(event)
        await sink.save_event(event)
        assert len(await sink.load_events("t")) == 1

    @pytest.mark.asyncio
    async def test_vector_search_finds_the_near_neighbour(self, sink):
        embedder = HashingEmbedder()
        near = ev("database connection pool exhausted on primary")
        far = ev("anyone up for lunch today?", "general")
        for event in (near, far):
            await sink.save_event(event, embedding=embedder.embed(event.content))
        results = await sink.similar_events(
            "t", embedder.embed("the connection pool is exhausted"), limit=2
        )
        assert results, "vector search returned nothing"
        assert results[0][0] == near.event_id
        assert results[0][1] > 0.3

    @pytest.mark.asyncio
    async def test_annotation_and_summary_persist(self, sink):
        event = ev()
        await sink.save_event(event)
        await sink.save_annotation(annotation([event.event_id]))
        await sink.save_summary(summary(events=[event.event_id]))
        history = await sink.entity_history("t", "checkout")
        assert len(history) == 1
        assert history[0].source_event_ids == [event.event_id]

    @pytest.mark.asyncio
    async def test_supersede_is_persisted_as_an_update(self, sink):
        old = summary("suspected database issue")
        await sink.save_summary(old)
        old.status = SummaryStatus.SUPERSEDED
        old.superseded_by = "sum_new"
        await sink.save_summary(old)

        new = summary("root cause confirmed: DNS failure")
        new.supersedes = old.summary_id
        new.version = 2
        await sink.save_summary(new)

        history = await sink.entity_history("t", "checkout")
        assert len(history) == 2
        superseded = [s for s in history if s.status is SummaryStatus.SUPERSEDED]
        assert len(superseded) == 1
        assert superseded[0].superseded_by == "sum_new"

    @pytest.mark.asyncio
    async def test_entity_memory_upserts(self, sink):
        memory = EntityMemory(entity_id="checkout", tenant_id="t")
        memory.current_state = "degraded"
        memory.severity = Severity.CRITICAL
        memory.event_count = 7
        await sink.save_entity(memory)
        memory.current_state = "recovering"
        memory.event_count = 9
        await sink.save_entity(memory)

        async with sink._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM pf_entity_memory WHERE tenant_id='t' "
                "AND entity_id='checkout'"
            )
        assert row["current_state"] == "recovering"
        assert row["event_count"] == 9

    @pytest.mark.asyncio
    async def test_health_reports_pgvector(self, sink):
        health = await sink.health()
        assert health["healthy"] is True
        assert health["pgvector"] is True
