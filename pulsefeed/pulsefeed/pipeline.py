"""The PulseFeed pipeline: everything wired together.

Shape of one event's journey:

    ingest -> cheap score -> coalesce -> trigger -> {cheap path | LLM queue}
                                                          |
                                          workers -> enrich -> annotate
                                                          |
                                    entity memory + summaries + timeline

Two properties this module exists to guarantee:

**Ingestion never waits on the LLM.** ``ingest()`` scores, coalesces, decides,
and returns. It touches no network. A cluster that is not admitted still
becomes a timeline item immediately via the cheap path, and a cluster that *is*
admitted also becomes one immediately — enrichment upgrades the item in place
when it lands. So the feed is complete at all times and the LLM only ever
changes how well it reads.

**Degrade enrichment before degrading ingestion.** Every overload response in
here — harder coalescing, raised thresholds, load shedding — removes semantic
polish first. Losing the provider entirely costs the feed its summaries and
nothing else.
"""

from __future__ import annotations

import asyncio
import time
from collections import Counter
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

from .budget import BudgetLedger, TenantPlan
from .clock import Clock, RealClock
from .coalescer import Coalescer, CoalescerConfig, describe_cluster
from .llm.prompts import EnrichmentRequest
from .llm.provider import (
    EnrichmentUnavailable,
    MockProvider,
    ResilientProvider,
)
from .memory import EntityStore
from .metrics import CIRCUIT_STATE_VALUES, PulseFeedMetrics
from .models import (
    Event,
    EventCluster,
    PathTaken,
    Priority,
    SemanticAnnotation,
    Severity,
    Summary,
    SummaryLevel,
    TimelineItem,
    new_id,
)
from .scheduler import (
    Admission,
    BoundedPriorityScheduler,
    SchedulerConfig,
    WorkItem,
    percentiles,
)
from .scoring import CheapScorer, ScorerConfig
from .summarize import (
    Rollup,
    SummaryStore,
    detect_contradiction,
    summary_from_annotation,
)
from .store.base import NullSink, PersistenceSink
from .trigger import CompositePolicy, TriggerContext, build_default_policy


@dataclass
class PipelineConfig:
    worker_count: int = 4
    audit_sample_rate: float = 0.01
    tick_interval: float = 5.0          # simulated seconds between coalescer ticks
    latency_ewma_alpha: float = 0.2
    initial_service_latency: float = 0.4
    timeline_capacity: int = 2000       # items retained per tenant
    episode_interval: float = 120.0     # simulated seconds between rollup passes
    enable_metrics: bool = True
    seed: int = 1337


@dataclass
class PipelineStats:
    events_ingested: int = 0
    clusters_emitted: int = 0
    triggered: int = 0
    skipped: int = 0
    dropped: int = 0
    enriched: int = 0
    enrichment_failed: int = 0
    audit_samples: int = 0
    audit_important_misses: int = 0
    tokens_used: int = 0
    cost_usd: float = 0.0
    coalesced_events: int = 0
    supersedes: int = 0
    episodes_built: int = 0
    episodes_enriched: int = 0

    def as_dict(self) -> Dict[str, Any]:
        return {
            k: (round(v, 6) if isinstance(v, float) else v)
            for k, v in self.__dict__.items()
        }


class PulseFeedPipeline:
    def __init__(
        self,
        provider: Optional[ResilientProvider] = None,
        *,
        config: Optional[PipelineConfig] = None,
        clock: Optional[Clock] = None,
        ledger: Optional[BudgetLedger] = None,
        scorer: Optional[CheapScorer] = None,
        coalescer: Optional[Coalescer] = None,
        scheduler: Optional[BoundedPriorityScheduler] = None,
        policy: Optional[CompositePolicy] = None,
        metrics: Optional[PulseFeedMetrics] = None,
        sink: Optional["PersistenceSink"] = None,
    ) -> None:
        self.config = config or PipelineConfig()
        self.clock = clock or RealClock()
        self.ledger = ledger or BudgetLedger()
        self.scorer = scorer or CheapScorer(ScorerConfig())
        self.coalescer = coalescer or Coalescer(CoalescerConfig())
        self.scheduler = scheduler or BoundedPriorityScheduler(
            SchedulerConfig(), clock=self.clock
        )
        self.provider = provider or ResilientProvider(
            MockProvider(clock=self.clock), clock=self.clock
        )
        self.policy = policy or build_default_policy(
            self.ledger,
            audit_sample_rate=self.config.audit_sample_rate,
            seed=self.config.seed,
        )
        self.metrics = metrics or (
            PulseFeedMetrics() if self.config.enable_metrics else PulseFeedMetrics()
        )

        self.entities = EntityStore()
        self.summaries = SummaryStore()
        self.rollup = Rollup(self.summaries)
        # Write-through: the in-process structures stay the serving path and the
        # sink is the durable record behind them. Serving from the sink would
        # put a database on the read path of a feed that must survive its
        # dependencies, which is the opposite of the point.
        self.sink: PersistenceSink = sink or NullSink()
        self.sink_failures = 0

        self.stats = PipelineStats()
        self.events: Dict[str, Event] = {}
        self.annotations: Dict[str, SemanticAnnotation] = {}
        self._timeline: Dict[str, Dict[str, TimelineItem]] = {}
        self._item_by_cluster: Dict[str, TimelineItem] = {}
        self._cluster_summary: Dict[str, Summary] = {}
        self._item_by_summary: Dict[str, TimelineItem] = {}
        self._rolled_up: set = set()
        # (tenant, entity) -> (child signature, current episode summary)
        self._open_episodes: Dict[tuple, tuple] = {}
        self._path_by_event: Dict[str, PathTaken] = {}
        self._ingest_time: Dict[str, float] = {}
        self._visible_at: Dict[str, float] = {}
        self.end_to_end_samples: Dict[str, List[float]] = {"cheap": [], "llm": []}

        # Why work was turned away, kept in-process so a test or a dashboard can
        # tell "we were over budget" apart from "the answer would have been
        # stale" apart from "the queue was full" — three very different problems
        # that all look like "fewer LLM calls" from the outside.
        self.skip_reasons: Counter = Counter()
        self.last_threshold = 0.0
        self.last_utility = 0.0

        self._service_latency = self.config.initial_service_latency
        self._inflight = 0
        self._workers: List[asyncio.Task] = []
        self._ticker: Optional[asyncio.Task] = None
        self._running = False

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._workers = [
            asyncio.create_task(self._worker(i), name=f"pulsefeed-worker-{i}")
            for i in range(self.config.worker_count)
        ]
        self._ticker = asyncio.create_task(self._tick_loop(), name="pulsefeed-ticker")

    async def flush(self) -> None:
        """Close every open cluster and push it through the trigger.

        Separate from ``stop`` so a simulated-time replay can flush, let its
        clock driver run the queue to empty, and only then shut down.
        """
        for cluster in self.coalescer.flush(self.clock.now()):
            await self._handle_cluster(cluster)
        # Wait for in-flight enrichment before rolling up, so episodes are built
        # from finished conclusions rather than half of them.
        for _ in range(20_000):
            if self.idle:
                break
            await asyncio.sleep(0)
        for tenant_id in list(self._timeline):
            self.stats.episodes_built += len(await self.build_episodes(tenant_id))

    @property
    def inflight(self) -> int:
        """Enrichments currently being processed by a worker."""
        return self._inflight

    @property
    def idle(self) -> bool:
        return self.scheduler.depth == 0 and self._inflight == 0

    async def stop(self, drain: bool = True, drain_timeout: float = 30.0) -> None:
        """Shut down. Draining first is what makes replay results complete."""
        if not self._running:
            return
        if drain:
            await self.flush()
            deadline = time.monotonic() + drain_timeout
            while not self.idle and time.monotonic() < deadline:
                await asyncio.sleep(0.01)
        self._running = False
        self.scheduler.close()
        if self._ticker is not None:
            self._ticker.cancel()
        for task in self._workers:
            task.cancel()
        await asyncio.gather(*self._workers, self._ticker, return_exceptions=True)
        self._workers = []
        self._ticker = None

    async def _tick_loop(self) -> None:
        """Closes idle clusters even when no new events arrive.

        Without this, the last event on a quiet entity sits in an open cluster
        forever and never reaches the timeline.
        """
        last_rollup = self.clock.now()
        try:
            while self._running:
                await self.clock.sleep(self.config.tick_interval)
                for cluster in self.coalescer.tick(self.clock.now()):
                    await self._handle_cluster(cluster)
                if self.clock.now() - last_rollup >= self.config.episode_interval:
                    last_rollup = self.clock.now()
                    for tenant_id in list(self._timeline):
                        self.stats.episodes_built += len(
                            await self.build_episodes(tenant_id)
                        )
                self._refresh_gauges()
        except asyncio.CancelledError:  # pragma: no cover - shutdown path
            pass

    # ------------------------------------------------------------------
    # ingest (hot path — no network, no awaiting the LLM)
    # ------------------------------------------------------------------

    async def ingest(self, event: Event) -> None:
        now = self.clock.now()
        self.events[event.event_id] = event
        self._ingest_time[event.event_id] = now
        self.stats.events_ingested += 1
        self.metrics.events_ingested.labels(event.tenant_id, event.source).inc()

        features = self.scorer.score(event, now=now)
        self.entities.observe(event)

        vector = self.scorer.embed(event.content)
        await self._persist(self.sink.save_event(event, features, vector))
        for cluster in self.coalescer.offer(event, features, vector, now=now):
            await self._handle_cluster(cluster)
        # Anything that just opened gets a provisional row immediately, so
        # time-to-feed is bounded by ingest rather than by how long the
        # coalescer waits to see whether more events are coming.
        for provisional in self.coalescer.drain_touched(now):
            self._publish_cheap_item(provisional, now, provisional=True)

    async def ingest_many(self, events: Sequence[Event]) -> None:
        for event in events:
            await self.ingest(event)

    async def _persist(self, coro) -> None:
        """Await a sink write, treating failure as degradation rather than loss.

        The sink is *fail-open*: a database that is down must not stop the feed,
        because the serving path does not depend on it and a dropped write is
        recoverable from the bus. Failures are counted so "persistence is
        broken" is visible instead of silent.
        """
        try:
            await coro
        except Exception:
            self.sink_failures += 1

    # ------------------------------------------------------------------
    # trigger
    # ------------------------------------------------------------------

    async def _handle_cluster(self, cluster: EventCluster) -> None:
        now = self.clock.now()
        self.stats.clusters_emitted += 1
        self.stats.coalesced_events += cluster.coalesced_count
        self.metrics.clusters_emitted.labels(
            cluster.tenant_id, cluster.close_reason
        ).inc()
        if cluster.coalesced_count:
            self.metrics.events_coalesced.labels(cluster.tenant_id).inc(
                cluster.coalesced_count
            )

        # Publish the cheap item first, unconditionally. The feed is now correct
        # and complete regardless of what happens to the LLM path.
        self._publish_cheap_item(cluster, now)

        ctx = self._trigger_context(cluster, now)
        decision = self.policy.decide_event(
            cluster.representative, cluster.features, ctx
        )
        self.metrics.trigger_threshold.labels(cluster.tenant_id).set(decision.threshold)
        self.metrics.trigger_utility.observe(decision.utility)
        self.last_threshold = decision.threshold
        self.last_utility = decision.utility

        if not decision.invoke:
            self._record_skip(cluster, decision.reason)
            return

        if not self.ledger.can_afford(
            cluster.tenant_id,
            cluster.features.estimated_llm_tokens,
            cluster.features.estimated_cost_usd,
            cluster.features.priority,
            now,
        ):
            self._record_skip(cluster, "budget_exhausted")
            return

        work = WorkItem(
            cluster=cluster,
            priority=cluster.features.priority,
            decision=decision,
            enqueued_at=now,
            deadline_at=cluster.deadline(),
            audit=decision.audit,
        )
        result = self.scheduler.enqueue(work)

        if result.admission is Admission.ADMITTED:
            self.stats.triggered += 1
            self.metrics.events_triggered.labels(
                cluster.tenant_id, decision.reason
            ).inc()
            if decision.audit:
                self.stats.audit_samples += 1
                self.metrics.audit_samples.labels(cluster.tenant_id).inc()
            if result.evicted is not None:
                self._record_drop(result.evicted.cluster, "evicted_stale")
        elif result.admission is Admission.REJECTED_COALESCE:
            # Backpressure, not loss: the cluster still shows on the cheap path
            # and the coalescer is told to group harder from here on.
            self.coalescer.set_aggressive(True)
            self._record_skip(cluster, "queue_full_cheap_path")
        elif result.admission is Admission.REJECTED_STALE:
            self._record_skip(cluster, "stale_at_enqueue")
        else:
            self._record_drop(cluster, "load_shed")

        self._refresh_gauges()

    def _trigger_context(self, cluster: EventCluster, now: float) -> TriggerContext:
        service_rate = self.config.worker_count / max(0.05, self._service_latency)
        return TriggerContext(
            now=now,
            queue_depth=self.scheduler.depth,
            queue_capacity=self.scheduler.capacity,
            estimated_wait_seconds=self.scheduler.estimated_wait(
                cluster.features.priority, service_rate
            ),
            inflight=len(self._workers),
            provider_healthy=self.provider.any_healthy,
            tenant_id=cluster.tenant_id,
            deadline_at=cluster.deadline(),
            needs_boundary_check=cluster.needs_boundary_check,
        )

    def _record_skip(self, cluster: EventCluster, reason: str) -> None:
        self.stats.skipped += 1
        self.skip_reasons[reason] += 1
        self.metrics.events_skipped.labels(cluster.tenant_id, reason).inc()
        for event in cluster.events:
            self._path_by_event.setdefault(event.event_id, PathTaken.CHEAP)

    def _record_drop(self, cluster: EventCluster, reason: str) -> None:
        self.stats.dropped += 1
        self.metrics.events_dropped.labels(
            cluster.tenant_id, cluster.features.priority.label, reason
        ).inc()
        for event in cluster.events:
            self._path_by_event[event.event_id] = PathTaken.DROPPED

    # ------------------------------------------------------------------
    # workers (LLM path)
    # ------------------------------------------------------------------

    async def _worker(self, index: int) -> None:
        try:
            while self._running:
                item = await self.scheduler.dequeue()
                if item is None:
                    continue
                self._inflight += 1
                try:
                    await self._process(item)
                finally:
                    self._inflight -= 1
        except asyncio.CancelledError:  # pragma: no cover - shutdown path
            pass

    async def _process(self, item: WorkItem) -> None:
        cluster = item.cluster
        now = self.clock.now()
        self.metrics.queue_wait_seconds.labels(item.priority.label).observe(
            item.wait_seconds(now)
        )

        request = EnrichmentRequest(
            tenant_id=cluster.tenant_id,
            events=list(cluster.events),
            entity_context=self.entities.context_for(
                cluster.tenant_id, [cluster.entity_id]
            ),
            needs_boundary_check=cluster.needs_boundary_check,
            deadline_at=item.deadline_at,
            priority_label=item.priority.label,
            cluster_id=cluster.cluster_id,
            audit=item.audit,
        )

        try:
            outcome = await self.provider.enrich(request)
        except EnrichmentUnavailable as exc:
            self.stats.enrichment_failed += 1
            self.metrics.llm_invocations.labels(
                cluster.tenant_id, self.provider.providers[0].name, "failed"
            ).inc()
            self.metrics.provider_errors.labels(
                self.provider.providers[0].name, type(exc).__name__
            ).inc()
            for event in cluster.events:
                self._path_by_event[event.event_id] = PathTaken.LLM_FAILED
            self._refresh_gauges()
            return

        annotation = outcome.annotation
        self._service_latency = (
            self.config.latency_ewma_alpha * outcome.latency_seconds
            + (1 - self.config.latency_ewma_alpha) * self._service_latency
        )

        self.ledger.charge(
            cluster.tenant_id, annotation.tokens_used, annotation.cost_usd, now
        )
        self.stats.enriched += 1
        self.stats.tokens_used += annotation.tokens_used
        self.stats.cost_usd += annotation.cost_usd
        self.metrics.llm_invocations.labels(
            cluster.tenant_id, outcome.provider_name, "success"
        ).inc()
        self.metrics.llm_tokens.labels(cluster.tenant_id, outcome.provider_name).inc(
            annotation.tokens_used
        )
        self.metrics.llm_cost_usd.labels(cluster.tenant_id).inc(annotation.cost_usd)
        self.metrics.provider_latency.labels(outcome.provider_name).observe(
            outcome.latency_seconds
        )
        self.metrics.enrichment_tokens.observe(annotation.tokens_used)

        annotation.degraded = outcome.fallback_used
        self.annotations[annotation.annotation_id] = annotation
        self.entities.apply_annotation(annotation)
        await self._persist(self.sink.save_annotation(annotation))
        for entity_id in annotation.entities:
            memory = self.entities.get(cluster.tenant_id, entity_id)
            if memory is not None:
                await self._persist(self.sink.save_entity(memory))

        if item.audit:
            # Audit results never reach the user's timeline. Their only job is
            # to tell us how often the cheap layer was wrong to say no.
            self._record_audit_result(cluster, annotation)
            return

        summary = self._store_summary(cluster, annotation)
        await self._persist(self.sink.save_summary(summary))
        prior = self.summaries.get(summary.supersedes) if summary.supersedes else None
        if prior is not None:
            # Persist the superseded row too: the correction is only auditable
            # if the belief it replaced is on disk with its new status.
            await self._persist(self.sink.save_summary(prior))
        self._upgrade_timeline_item(cluster, annotation, now)
        for event in cluster.events:
            self._path_by_event[event.event_id] = PathTaken.LLM
            ingested = self._ingest_time.get(event.event_id)
            if ingested is not None:
                self.end_to_end_samples["llm"].append(max(0.0, now - ingested))
        self.metrics.end_to_end_latency.labels("llm").observe(
            max(0.0, now - item.enqueued_at)
        )
        self._refresh_gauges()

    def _record_audit_result(
        self, cluster: EventCluster, annotation: SemanticAnnotation
    ) -> None:
        """Estimate the false-negative rate of the cheap scorer.

        An audited cluster the cheap layer rejected, which the model then calls
        error-or-worse, is a miss. Scaled by the sampling rate this gives a
        population estimate of how much the first stage is losing — the number
        that would otherwise be pure assertion.
        """
        if annotation.severity.rank >= Severity.ERROR.rank:
            self.stats.audit_important_misses += 1
            self.metrics.audit_misses.labels(cluster.tenant_id).inc()
        if self.stats.audit_samples:
            miss_rate = self.stats.audit_important_misses / self.stats.audit_samples
            self.metrics.important_event_recall.labels(cluster.tenant_id).set(
                1.0 - miss_rate
            )

    def estimated_missed_important_events(self) -> float:
        """Population estimate of important events the trigger rejected."""
        if not self.stats.audit_samples:
            return 0.0
        miss_rate = self.stats.audit_important_misses / self.stats.audit_samples
        return miss_rate * self.stats.skipped

    # ------------------------------------------------------------------
    # summaries and timeline
    # ------------------------------------------------------------------

    def _store_summary(
        self, cluster: EventCluster, annotation: SemanticAnnotation
    ) -> Summary:
        summary = summary_from_annotation(annotation, SummaryLevel.CLUSTER)
        if not summary.entity_ids:
            summary.entity_ids = [cluster.entity_id]

        prior = self._find_contradicted_summary(cluster, annotation)
        if prior is not None:
            self.summaries.supersede(prior.summary_id, summary)
            self.stats.supersedes += 1
        else:
            self.summaries.add(summary)
        self._cluster_summary[cluster.cluster_id] = summary
        item = self._item_by_cluster.get(cluster.cluster_id)
        if item is not None:
            self._item_by_summary[summary.summary_id] = item
        return summary

    # ------------------------------------------------------------------
    # hierarchy: cluster summaries -> episodes
    # ------------------------------------------------------------------

    async def build_episodes(self, tenant_id: str) -> List[Summary]:
        """Roll related cluster summaries up into episodes.

        This is where a feed stops being a list and starts being a story. Six
        separate lines about checkout-service — latency, timeouts, a deploy, a
        rollback, recovery — become one episode that says what happened, with
        all six still attached as evidence.

        Episodes are *open* while their entity keeps producing related material.
        A naive implementation that rolled up only the summaries it had not seen
        yet would fragment one incident into an episode per rollup pass, which
        is worse than not rolling up at all. Instead each pass rebuilds the
        episode over the full window and supersedes the previous version, so the
        story grows in place and the earlier, partial telling stays on the
        record.

        It is also the cheapest LLM call in the system: it reads a handful of
        already-compressed summaries rather than the raw events beneath them,
        which is the entire point of the hierarchy. And the signature check
        means an unchanged episode is never re-narrated, so a quiet tick costs
        nothing.
        """
        by_entity: Dict[str, List[Summary]] = {}
        for summary in self.summaries.active(SummaryLevel.CLUSTER, tenant_id):
            entity = summary.entity_ids[0] if summary.entity_ids else "__none__"
            by_entity.setdefault(entity, []).append(summary)

        built: List[Summary] = []
        window = self.rollup.config.episode_window_seconds
        minimum = self.rollup.config.min_children_for_rollup

        for entity, summaries in by_entity.items():
            summaries.sort(key=lambda s: s.generated_at)
            group = [
                s
                for s in summaries
                if summaries[-1].generated_at - s.generated_at <= window
            ][-self.rollup.config.max_children_per_rollup :]
            if len(group) < minimum:
                continue

            key = (tenant_id, entity)
            signature = tuple(s.summary_id for s in group)
            previous = self._open_episodes.get(key)
            if previous is not None and previous[0] == signature:
                continue  # nothing new since the last pass

            episode = self.rollup.extractive_rollup(group, SummaryLevel.EPISODE)
            text = await self._generate_episode_text(group, SummaryLevel.EPISODE)
            if text:
                episode.text = text
                episode.model = "llm"

            if previous is not None:
                self.summaries.supersede(previous[1].summary_id, episode)
            else:
                self.summaries.add(episode)

            self._open_episodes[key] = (signature, episode)
            self._publish_episode_item(episode, group, replacing=previous)
            built.append(episode)
        return built

    async def _generate_episode_text(
        self, children: Sequence[Summary], level: SummaryLevel
    ) -> Optional[str]:
        """Ask the model to narrate an episode, if it is worth doing and affordable.

        Returning ``None`` (or raising) falls back to the extractive rollup, so
        digests keep working when the provider does not.
        """
        if not children:
            return None
        tenant_id = children[0].tenant_id
        severity = max((c.severity for c in children), key=lambda s: s.rank)
        if severity.rank < Severity.WARNING.rank:
            return None  # a quiet stretch does not need prose
        if not self.provider.any_healthy:
            return None

        estimated_tokens = sum(len(c.text) for c in children) // 3 + 300
        if not self.ledger.can_afford(
            tenant_id, estimated_tokens, 0.0, Priority.P1, self.clock.now()
        ):
            return None

        # The children are summaries, not raw events, so they are presented as
        # synthetic events whose ids are the summary ids. Citations therefore
        # stay inside the set we supplied, exactly as at the cluster level.
        synthetic = [
            Event(
                source="pulsefeed-summary",
                content=c.text,
                tenant_id=tenant_id,
                event_id=c.summary_id,
                timestamp=c.generated_at,
                entity_ids=list(c.entity_ids),
            )
            for c in children
        ]
        request = EnrichmentRequest(
            tenant_id=tenant_id,
            events=synthetic,
            entity_context=self.entities.context_for(
                tenant_id, children[0].entity_ids or []
            ),
            priority_label="P1",
            metadata={"level": level.value},
        )
        try:
            outcome = await self.provider.enrich(request)
        except EnrichmentUnavailable:
            return None

        annotation = outcome.annotation
        self.ledger.charge(
            tenant_id, annotation.tokens_used, annotation.cost_usd, self.clock.now()
        )
        self.stats.episodes_enriched += 1
        self.stats.tokens_used += annotation.tokens_used
        self.stats.cost_usd += annotation.cost_usd
        self.metrics.llm_tokens.labels(tenant_id, outcome.provider_name).inc(
            annotation.tokens_used
        )
        self.metrics.llm_cost_usd.labels(tenant_id).inc(annotation.cost_usd)
        return annotation.summary

    def _publish_episode_item(
        self,
        episode: Summary,
        children: Sequence[Summary],
        replacing: Optional[tuple] = None,
    ) -> None:
        # Extending an open episode updates its existing item rather than
        # publishing a second one, so the feed does not accumulate successive
        # retellings of the same incident.
        existing = (
            self._item_by_summary.get(replacing[1].summary_id)
            if replacing is not None
            else None
        )
        if existing is not None:
            existing.title = episode.text
            existing.severity = episode.severity
            existing.source_event_ids = list(episode.source_event_ids)
            existing.event_count = len(episode.source_event_ids)
            existing.enriched = episode.model not in (
                "extractive",
                "extractive-fallback",
            )
            self._item_by_summary[episode.summary_id] = existing
            self._attach_children(existing, children)
            return

        item = TimelineItem(
            item_id=new_id("itm"),
            tenant_id=episode.tenant_id,
            title=episode.text,
            timestamp=episode.generated_at,
            rank_score=0.0,
            source_event_ids=list(episode.source_event_ids),
            kind="episode",
            severity=episode.severity,
            enriched=episode.model not in ("extractive", "extractive-fallback"),
            event_count=len(episode.source_event_ids),
            entity_ids=list(episode.entity_ids),
        )
        self._attach_children(item, children)
        self._item_by_summary[episode.summary_id] = item
        self._timeline.setdefault(episode.tenant_id, {})[item.item_id] = item
        self.metrics.timeline_items.labels(
            episode.tenant_id, "episode", str(item.enriched).lower()
        ).inc()

    def _attach_children(
        self, item: TimelineItem, children: Sequence[Summary]
    ) -> None:
        """Hide the children behind the episode and rank the episode above them.

        An episode ranks just above the loudest thing it contains, so rolling
        up can never bury the event that made the episode worth reading.
        """
        child_items = [
            self._item_by_summary[c.summary_id]
            for c in children
            if c.summary_id in self._item_by_summary
        ]
        best_child = max((i.base_rank for i in child_items), default=0.0)
        item.base_rank = min(1.0, best_child + 0.05)
        item.rank_score = item.base_rank
        for child_item in child_items:
            child_item.rolled_up_into = item.item_id
            self._rolled_up.add(child_item.item_id)

    def _find_contradicted_summary(
        self, cluster: EventCluster, annotation: SemanticAnnotation
    ) -> Optional[Summary]:
        candidates = self.summaries.history_for_entity(
            cluster.tenant_id, cluster.entity_id
        )
        for prior in reversed(candidates):
            if prior.status.value != "active":
                continue
            if detect_contradiction(prior, annotation):
                return prior
        return None

    def _publish_cheap_item(
        self, cluster: EventCluster, now: float, provisional: bool = False
    ) -> TimelineItem:
        """Write (or refresh) the LLM-free row for a cluster.

        Called twice per cluster in the normal case: once provisionally when it
        opens, once when it closes with its full membership. The second call
        updates the same row rather than adding another, so a growing happening
        occupies one slot in the feed throughout its life.
        """
        existing = self._item_by_cluster.get(cluster.cluster_id)
        if existing is not None:
            existing.title = describe_cluster(cluster)
            existing.event_count = cluster.size
            existing.source_event_ids = cluster.event_ids
            existing.kind = "cluster" if cluster.size > 1 else "event"
            if not existing.enriched:
                existing.severity = self._cheap_severity(cluster.features.risk)
                existing.base_rank = self._cheap_rank(cluster, now)
                existing.rank_score = existing.base_rank
            self._record_visible(cluster, now)
            return existing

        features = cluster.features
        item = TimelineItem(
            item_id=new_id("itm"),
            tenant_id=cluster.tenant_id,
            title=describe_cluster(cluster),
            timestamp=cluster.representative.timestamp,
            rank_score=self._cheap_rank(cluster, now),
            base_rank=self._cheap_rank(cluster, now),
            source_event_ids=cluster.event_ids,
            kind="cluster" if cluster.size > 1 else "event",
            severity=self._cheap_severity(features.risk),
            enriched=False,
            event_count=cluster.size,
            entity_ids=[cluster.entity_id],
        )
        self._timeline.setdefault(cluster.tenant_id, {})[item.item_id] = item
        self._item_by_cluster[cluster.cluster_id] = item
        self.metrics.timeline_items.labels(cluster.tenant_id, item.kind, "false").inc()
        self._record_visible(cluster, now)
        self._trim_timeline(cluster.tenant_id)
        return item

    def _record_visible(self, cluster: EventCluster, now: float) -> None:
        """Record time-to-feed, once per event, at its first appearance.

        Measured at first appearance rather than at cluster close: an event is
        visible to the reader from the moment its provisional row exists, and
        counting the later in-place updates instead would report a latency no
        user experiences.
        """
        for event in cluster.events:
            if event.event_id in self._visible_at:
                continue
            self._visible_at[event.event_id] = now
            ingested = self._ingest_time.get(event.event_id)
            if ingested is not None:
                latency = max(0.0, now - ingested)
                self.end_to_end_samples["cheap"].append(latency)
                self.metrics.end_to_end_latency.labels("cheap").observe(latency)

    def _upgrade_timeline_item(
        self, cluster: EventCluster, annotation: SemanticAnnotation, now: float
    ) -> None:
        item = self._item_by_cluster.get(cluster.cluster_id)
        if item is None:  # pragma: no cover - cheap item is always published first
            return
        item.title = annotation.summary
        item.severity = annotation.severity
        item.enriched = True
        item.degraded = annotation.degraded
        item.base_rank = self._enriched_rank(cluster, annotation, now)
        item.rank_score = item.base_rank
        self.metrics.timeline_items.labels(cluster.tenant_id, item.kind, "true").inc()

    @staticmethod
    def _cheap_severity(risk: float) -> Severity:
        if risk >= 0.9:
            return Severity.CRITICAL
        if risk >= 0.65:
            return Severity.ERROR
        if risk >= 0.45:
            return Severity.WARNING
        return Severity.INFO

    def _cheap_rank(self, cluster: EventCluster, now: float) -> float:
        """Rank a cluster without an LLM.

        Two terms here are not obvious and both come from reading a bad ranking
        rather than from theory:

        ``user_relevance`` is applied *outside* ``importance`` and weighted
        heavily. It is already an input to importance, but importance is also
        driven by novelty, so the fifth "@alice can you review this" scored far
        below the first despite being exactly as relevant to Alice. Being
        addressed directly is categorical, not a function of how fresh the
        phrasing is.

        ``entity_boost`` is the first use of entity memory in ranking rather
        than in prompting. "deployment #813 completed" is lexically boring and
        the cheap scorer has no way to know better — but the same sentence about
        a service that is *currently degraded* is the most interesting line in
        the feed. State the model already tracks, finally consulted.
        """
        f = cluster.features
        age_hours = max(0.0, (now - cluster.representative.timestamp) / 3600.0)
        recency = 1.0 / (1.0 + age_hours)
        return (
            0.40 * f.importance
            + 0.26 * f.risk
            + 0.26 * f.user_relevance
            + 0.08 * recency
        )

    def _entity_boost(self, tenant_id: str, entity_ids: Sequence[str]) -> float:
        """How much an entity's *current* state raises an otherwise-dull item.

        Applied at read time rather than baked in at publish time. An event that
        looked routine when it arrived becomes interesting the moment its
        service is declared degraded, and that verdict usually lands after the
        event was already published.
        """
        best = 0.0
        for entity_id in entity_ids:
            memory = self.entities.get(tenant_id, entity_id)
            if memory is None:
                continue
            if memory.current_state == "degraded":
                best = max(best, 0.18)
            elif memory.current_state == "recovering":
                best = max(best, 0.08)
        return best

    def _enriched_rank(
        self, cluster: EventCluster, annotation: SemanticAnnotation, now: float
    ) -> float:
        severity_norm = annotation.severity.rank / 4.0
        base = self._cheap_rank(cluster, now)
        # Enrichment mostly re-ranks by what the model concluded, but never
        # discards the cheap signal entirely: a confident model that is wrong
        # should not be able to bury a high-risk event on its own.
        return 0.55 * severity_norm + 0.30 * base + 0.15 * annotation.confidence

    def _trim_timeline(self, tenant_id: str) -> None:
        items = self._timeline.get(tenant_id, {})
        if len(items) <= self.config.timeline_capacity:
            return
        ordered = sorted(items.values(), key=lambda i: i.timestamp)
        for item in ordered[: len(items) - self.config.timeline_capacity]:
            items.pop(item.item_id, None)

    # ------------------------------------------------------------------
    # read API
    # ------------------------------------------------------------------

    def timeline(
        self,
        tenant_id: str = "default",
        limit: int = 20,
        ranked: bool = True,
        expand_rolled_up: bool = False,
    ) -> List[TimelineItem]:
        """The feed.

        By default an item absorbed into an episode is hidden, so the reader
        sees the story rather than its seven constituent lines. Pass
        ``expand_rolled_up=True`` to get everything — which is what coverage
        accounting and the evidence endpoints want.
        """
        items = [
            i
            for i in self._timeline.get(tenant_id, {}).values()
            if expand_rolled_up or i.rolled_up_into is None
        ]
        if ranked:
            for item in items:
                item.rank_score = min(
                    1.0,
                    item.base_rank + self._entity_boost(tenant_id, item.entity_ids),
                )
            items.sort(key=lambda i: (-i.rank_score, -i.timestamp))
        else:
            items.sort(key=lambda i: -i.timestamp)
        return items[:limit]

    def evidence_for(self, item: TimelineItem) -> List[Event]:
        """Expand any timeline item back into the raw events behind it."""
        return [
            self.events[eid] for eid in item.source_event_ids if eid in self.events
        ]

    def entity_history(self, tenant_id: str, entity_id: str) -> List[Summary]:
        return self.summaries.history_for_entity(tenant_id, entity_id)

    def register_tenant(self, plan: TenantPlan) -> None:
        self.ledger.register(plan)

    # ------------------------------------------------------------------
    # observability
    # ------------------------------------------------------------------

    def _refresh_gauges(self) -> None:
        for priority in Priority:
            self.metrics.queue_depth.labels(priority.label).set(
                self.scheduler.depth_of(priority)
            )
        for name, breaker in self.provider.breakers.items():
            self.metrics.circuit_state.labels(name).set(
                CIRCUIT_STATE_VALUES[breaker.state.value]
            )
        for tenant_id in self._timeline:
            self.metrics.budget_remaining.labels(tenant_id).set(
                self.ledger.remaining_fraction(tenant_id, self.clock.now())
            )

    def snapshot(self) -> Dict[str, Any]:
        return {
            "stats": self.stats.as_dict(),
            "skip_reasons": dict(self.skip_reasons),
            "scheduler": self.scheduler.snapshot(),
            "queue_wait": self.scheduler.wait_percentiles(),
            "provider": self.provider.snapshot(),
            "coalescer": dict(self.coalescer.stats),
            "entities": len(self.entities),
            "summaries": len(self.summaries),
            "end_to_end_cheap": percentiles(self.end_to_end_samples["cheap"]),
            "end_to_end_llm": percentiles(self.end_to_end_samples["llm"]),
            "estimated_missed_important": round(
                self.estimated_missed_important_events(), 2
            ),
        }

    def path_of(self, event_id: str) -> PathTaken:
        return self._path_by_event.get(event_id, PathTaken.CHEAP)


def build_pipeline(
    *,
    clock: Optional[Clock] = None,
    worker_count: int = 4,
    audit_sample_rate: float = 0.01,
    provider: Optional[ResilientProvider] = None,
    plan: Optional[TenantPlan] = None,
) -> PulseFeedPipeline:
    """Convenience constructor used by the API, the demo and the harness."""
    clock = clock or RealClock()
    ledger = BudgetLedger()
    if plan is not None:
        ledger.register(plan)
    return PulseFeedPipeline(
        provider=provider,
        config=PipelineConfig(
            worker_count=worker_count, audit_sample_rate=audit_sample_rate
        ),
        clock=clock,
        ledger=ledger,
    )
