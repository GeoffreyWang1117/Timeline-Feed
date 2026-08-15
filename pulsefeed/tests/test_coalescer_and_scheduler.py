"""Tests for coalescing, the bounded scheduler, and the circuit breaker."""

from __future__ import annotations

import asyncio

import pytest

from pulsefeed.clock import ManualClock
from pulsefeed.coalescer import Coalescer, CoalescerConfig, describe_cluster
from pulsefeed.embedding import HashingEmbedder, cosine
from pulsefeed.llm.circuit_breaker import (
    BreakerConfig,
    BreakerState,
    CircuitBreaker,
    RetryBudget,
)
from pulsefeed.models import Event, EventCluster, EventFeatures, Priority
from pulsefeed.scheduler import (
    Admission,
    BoundedPriorityScheduler,
    SchedulerConfig,
    WorkItem,
    percentiles,
)
from pulsefeed.scoring import CheapScorer
from pulsefeed.trigger import TriggerDecision

T0 = 1_700_000_000.0


def make_event(content: str, entity: str = "svc", ts: float = T0, source="telemetry"):
    return Event(
        source=source, content=content, tenant_id="t", entity_ids=[entity], timestamp=ts
    )


class TestEmbedding:
    def test_identical_text_is_identical_vector(self):
        e = HashingEmbedder()
        assert cosine(e.embed("cpu at 90%"), e.embed("cpu at 90%")) == pytest.approx(1.0)

    def test_unrelated_text_is_dissimilar(self):
        e = HashingEmbedder()
        sim = cosine(e.embed("database connection pool exhausted"), e.embed("lunch?"))
        assert sim < 0.3

    def test_empty_text_does_not_crash(self):
        e = HashingEmbedder()
        assert cosine(e.embed(""), e.embed("anything")) == 0.0


class TestCoalescer:
    def _offer(self, coalescer, scorer, event, now=None):
        features = scorer.score(event, now=now or event.timestamp)
        vector = scorer.embed(event.content)
        return coalescer.offer(event, features, vector, now=now or event.timestamp)

    def test_duplicate_storm_collapses_to_one_cluster(self):
        coalescer, scorer = Coalescer(), CheapScorer()
        for i in range(10):
            self._offer(
                coalescer, scorer, make_event(f"db cpu usage 9{i % 4}%", "db", T0 + i * 2)
            )
        clusters = coalescer.flush(T0 + 30)
        assert len(clusters) == 1
        assert clusters[0].size == 10
        assert clusters[0].coalesced_count == 9

    def test_topic_change_starts_a_new_cluster(self):
        coalescer, scorer = Coalescer(), CheapScorer()
        self._offer(coalescer, scorer, make_event("db cpu usage 90%", "db", T0))
        emitted = self._offer(
            coalescer,
            scorer,
            make_event("db connection pool exhausted, queries failing", "db", T0 + 2),
        )
        assert emitted[0].close_reason == "topic_changed"
        assert emitted[0].size == 1  # the original cluster, not merged into

    def test_max_size_one_disables_coalescing_without_adding_latency(self):
        coalescer = Coalescer(CoalescerConfig(max_cluster_size=1))
        scorer = CheapScorer()
        emitted = self._offer(coalescer, scorer, make_event("anything", "db", T0))
        # Emitted immediately rather than held open for a window it can't use.
        assert len(emitted) == 1
        assert emitted[0].size == 1
        assert coalescer.open_cluster_count() == 0

    def test_p0_never_waits_out_the_coalescing_window(self):
        """A SEV1 must not sit in a batching window. Holds whether the P0 opens
        a cluster or joins one."""
        scorer = CheapScorer()
        p0 = make_event(
            "SEV1 checkout outage paging on-call", "checkout", T0, "pagerduty"
        )

        fresh = Coalescer()
        emitted = self._offer(fresh, scorer, p0)
        assert [c.close_reason for c in emitted] == ["p0_member"]
        assert fresh.open_cluster_count() == 0

        joining = Coalescer()
        self._offer(
            joining,
            scorer,
            make_event("checkout latency high", "checkout", T0, "grafana"),
        )
        emitted = self._offer(joining, scorer, p0, now=T0 + 3)
        assert any(c.close_reason == "p0_member" for c in emitted)
        assert joining.open_cluster_count() == 0

    def test_idle_timeout_closes_open_clusters(self):
        coalescer, scorer = Coalescer(CoalescerConfig(idle_close_seconds=10)), CheapScorer()
        self._offer(coalescer, scorer, make_event("db cpu usage 90%", "db", T0))
        assert coalescer.tick(T0 + 5) == []
        assert len(coalescer.tick(T0 + 11)) == 1

    def test_aggressive_mode_widens_the_window(self):
        config = CoalescerConfig(window_seconds=10, idle_close_seconds=5)
        coalescer, scorer = Coalescer(config), CheapScorer()
        coalescer.set_aggressive(True)
        self._offer(coalescer, scorer, make_event("db cpu usage 90%", "db", T0))
        # Would have closed at +5s normally; aggressive mode holds it longer.
        assert coalescer.tick(T0 + 8) == []

    def test_ambiguous_similarity_flags_a_boundary_check(self):
        """The band between 'clearly the same' and 'clearly different' is the
        only place an LLM is genuinely needed for grouping."""
        config = CoalescerConfig(merge_similarity=0.99, ambiguous_similarity=0.02)
        coalescer, scorer = Coalescer(config), CheapScorer()
        self._offer(coalescer, scorer, make_event("db cpu usage 90%", "db", T0))
        self._offer(
            coalescer, scorer, make_event("db replication lag rising", "db", T0 + 2)
        )
        clusters = coalescer.flush(T0 + 30)
        assert clusters[0].size == 2
        assert clusters[0].needs_boundary_check
        assert coalescer.stats["boundary_checks"] == 1

    def test_confident_merge_does_not_flag_a_boundary_check(self):
        coalescer, scorer = Coalescer(), CheapScorer()
        self._offer(coalescer, scorer, make_event("db cpu usage 90%", "db", T0))
        self._offer(coalescer, scorer, make_event("db cpu usage 91%", "db", T0 + 2))
        clusters = coalescer.flush(T0 + 30)
        assert clusters[0].size == 2
        assert not clusters[0].needs_boundary_check

    def test_cheap_description_is_informative_without_an_llm(self):
        coalescer, scorer = Coalescer(), CheapScorer()
        for i in range(4):
            self._offer(
                coalescer, scorer, make_event(f"db cpu usage 9{i}%", "db", T0 + i * 2)
            )
        cluster = coalescer.flush(T0 + 30)[0]
        text = describe_cluster(cluster)
        assert "+3 similar events" in text
        assert "db" in text

    def test_cluster_deadline_is_the_most_urgent_member(self):
        coalescer, scorer = Coalescer(), CheapScorer()
        self._offer(coalescer, scorer, make_event("cpu 90%", "db", T0, "github"))
        self._offer(coalescer, scorer, make_event("cpu 91%", "db", T0 + 1, "github"))
        cluster = coalescer.flush(T0 + 30)[0]
        assert cluster.deadline() == min(e.expires_at() for e in cluster.events)


def work(priority: Priority, clock: ManualClock, deadline_offset: float = 300.0):
    event = make_event("x", "e", clock.now())
    cluster = EventCluster(
        cluster_id=f"c-{priority.name}-{clock.now()}",
        tenant_id="t",
        entity_id="e",
        events=[event],
        features=EventFeatures("x", priority=priority),
        opened_at=clock.now(),
        closed_at=clock.now(),
    )
    return WorkItem(
        cluster=cluster,
        priority=priority,
        decision=TriggerDecision(invoke=True, reason="test"),
        enqueued_at=clock.now(),
        deadline_at=clock.now() + deadline_offset,
    )


class TestScheduler:
    def test_admits_until_class_capacity(self):
        clock = ManualClock(T0)
        sched = BoundedPriorityScheduler(
            SchedulerConfig(capacity={p: 2 for p in Priority}), clock=clock
        )
        assert sched.enqueue(work(Priority.P2, clock)).admission is Admission.ADMITTED
        assert sched.enqueue(work(Priority.P2, clock)).admission is Admission.ADMITTED
        assert (
            sched.enqueue(work(Priority.P2, clock)).admission
            is Admission.REJECTED_COALESCE
        )

    def test_p3_is_dropped_first(self):
        clock = ManualClock(T0)
        sched = BoundedPriorityScheduler(
            SchedulerConfig(capacity={p: 1 for p in Priority}), clock=clock
        )
        sched.enqueue(work(Priority.P3, clock))
        result = sched.enqueue(work(Priority.P3, clock))
        assert result.admission is Admission.DROPPED
        assert sched.dropped_by_class[Priority.P3] == 1

    def test_p0_is_always_admitted(self):
        clock = ManualClock(T0)
        sched = BoundedPriorityScheduler(
            SchedulerConfig(capacity={p: 1 for p in Priority}), clock=clock
        )
        sched.enqueue(work(Priority.P0, clock))
        result = sched.enqueue(work(Priority.P0, clock))
        assert result.admission is Admission.ADMITTED
        assert sched.depth_of(Priority.P0) == 1  # bounded: oldest gave way

    def test_stale_work_is_refused_at_the_door(self):
        clock = ManualClock(T0)
        sched = BoundedPriorityScheduler(clock=clock)
        item = work(Priority.P1, clock, deadline_offset=10)
        clock.advance(20)
        assert sched.enqueue(item).admission is Admission.REJECTED_STALE

    def test_stale_work_is_dropped_on_dequeue(self):
        clock = ManualClock(T0)
        sched = BoundedPriorityScheduler(clock=clock)
        sched.enqueue(work(Priority.P1, clock, deadline_offset=10))
        clock.advance(30)
        assert sched.try_dequeue() is None
        assert sched.stats["expired_on_dequeue"] == 1

    def test_weighted_fairness_prevents_starvation(self):
        """Strict priority would starve P3 forever under a P0 stream. Deficit
        round robin gives it a guaranteed floor."""
        clock = ManualClock(T0)
        sched = BoundedPriorityScheduler(
            SchedulerConfig(capacity={p: 200 for p in Priority}), clock=clock
        )
        for _ in range(100):
            for p in Priority:
                sched.enqueue(work(p, clock))

        served = {p: 0 for p in Priority}
        for _ in range(60):
            item = sched.try_dequeue()
            assert item is not None
            served[item.priority] += 1

        assert served[Priority.P3] > 0, "lowest class was starved"
        assert served[Priority.P0] > served[Priority.P3]
        # 8:4:2:1 weights over 60 dispatches ≈ 32:16:8:4.
        assert served[Priority.P0] == pytest.approx(32, abs=4)
        assert served[Priority.P3] == pytest.approx(4, abs=3)

    def test_estimated_wait_grows_with_depth(self):
        clock = ManualClock(T0)
        sched = BoundedPriorityScheduler(clock=clock)
        empty = sched.estimated_wait(Priority.P2, service_rate=10.0)
        for _ in range(50):
            sched.enqueue(work(Priority.P2, clock))
        loaded = sched.estimated_wait(Priority.P2, service_rate=10.0)
        assert loaded > empty

    @pytest.mark.asyncio
    async def test_dequeue_wakes_on_enqueue(self):
        clock = ManualClock(T0)
        sched = BoundedPriorityScheduler(clock=clock)
        task = asyncio.create_task(sched.dequeue())
        await asyncio.sleep(0)
        sched.enqueue(work(Priority.P1, clock))
        item = await asyncio.wait_for(task, timeout=1.0)
        assert item is not None


class TestPercentiles:
    def test_empty(self):
        assert percentiles([])["p99"] == 0.0

    def test_ordering(self):
        p = percentiles([float(i) for i in range(1, 101)])
        assert p["p50"] <= p["p95"] <= p["p99"] <= p["max"]
        assert p["count"] == 100


class TestCircuitBreaker:
    def test_opens_after_consecutive_failures(self):
        cb = CircuitBreaker(config=BreakerConfig(failure_threshold=3))
        for _ in range(3):
            assert cb.allows_request()
            cb.record_failure()
        assert cb.state is BreakerState.OPEN
        assert not cb.allows_request()
        assert not cb.is_healthy

    def test_success_resets_the_failure_run(self):
        cb = CircuitBreaker(config=BreakerConfig(failure_threshold=3))
        cb.record_failure()
        cb.record_failure()
        cb.record_success()
        cb.record_failure()
        assert cb.state is BreakerState.CLOSED

    def test_half_open_probe_then_close(self):
        t = [0.0]
        cb = CircuitBreaker(
            config=BreakerConfig(
                failure_threshold=1, success_threshold=2, recovery_timeout=10,
                recovery_jitter=0.0,
            ),
            clock=lambda: t[0],
        )
        cb.record_failure()
        assert cb.state is BreakerState.OPEN
        t[0] = 11.0
        assert cb.allows_request()
        assert cb.state is BreakerState.HALF_OPEN
        cb.record_success()
        cb.allows_request()
        cb.record_success()
        assert cb.state is BreakerState.CLOSED

    def test_half_open_failure_reopens(self):
        t = [0.0]
        cb = CircuitBreaker(
            config=BreakerConfig(
                failure_threshold=1, recovery_timeout=10, recovery_jitter=0.0
            ),
            clock=lambda: t[0],
        )
        cb.record_failure()
        t[0] = 11.0
        cb.allows_request()
        cb.record_failure()
        assert cb.state is BreakerState.OPEN


class TestRetryBudget:
    def test_caps_retries_relative_to_traffic(self):
        budget = RetryBudget(ratio=0.1, min_tokens=2)
        assert budget.try_consume()
        assert budget.try_consume()
        assert not budget.try_consume()
        for _ in range(20):
            budget.record_request()
        assert budget.try_consume()
