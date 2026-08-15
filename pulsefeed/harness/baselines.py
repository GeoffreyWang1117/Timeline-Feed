"""The four experiment arms.

    A  Chronological       every event is an item, ordered by recency
    B  Rule / embedding    cheap scoring + coalescing, no LLM at all
    C  LLM-everything      one enrichment call per event, no admission control
    D  PulseFeed           coalescing + utility trigger + budget + backpressure

All four run through the *same* pipeline code with different configuration, so
the comparison isolates policy rather than implementation. Arm B is worth
noting twice: it is both the strongest non-LLM baseline and an exact model of
PulseFeed running with the provider down, which is how the "is the core feed
still reliable without an LLM?" question gets a number instead of a promise.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

from pulsefeed.budget import BudgetLedger, TenantPlan
from pulsefeed.clock import Clock, VirtualClock
from pulsefeed.coalescer import Coalescer, CoalescerConfig
from pulsefeed.llm.provider import MockConfig, MockProvider, ResilienceConfig, ResilientProvider
from pulsefeed.metrics import PulseFeedMetrics
from pulsefeed.models import Event, EventFeatures, Priority, TimelineItem
from pulsefeed.pipeline import PipelineConfig, PulseFeedPipeline
from pulsefeed.scheduler import BoundedPriorityScheduler, SchedulerConfig, percentiles
from pulsefeed.scoring import CheapScorer, TenantAffinity
from pulsefeed.trigger import (
    CompositePolicy,
    TriggerContext,
    TriggerDecision,
    build_default_policy,
    compute_utility,
)


class NeverInvokePolicy(CompositePolicy):
    """Arms A and B: the LLM is never consulted."""

    name = "never"

    def __init__(self) -> None:
        super().__init__(
            policies=[_NullPolicy()], safety_rules=[], audit_sample_rate=0.0
        )

    def decide_event(self, event, features, ctx) -> TriggerDecision:
        return TriggerDecision(
            invoke=False, reason="arm_disables_llm", utility=0.0, threshold=1.0
        )


class AlwaysInvokePolicy(CompositePolicy):
    """Arm C: no admission control whatsoever."""

    name = "always"

    def __init__(self) -> None:
        super().__init__(
            policies=[_NullPolicy()], safety_rules=[], audit_sample_rate=0.0
        )

    def decide_event(self, event, features, ctx) -> TriggerDecision:
        return TriggerDecision(
            invoke=True,
            reason="arm_enriches_everything",
            utility=compute_utility(features, self.weights),
            threshold=0.0,
        )


class _NullPolicy:
    name = "null"

    def threshold(self, features: EventFeatures, ctx: TriggerContext) -> float:
        return 0.5

    def decide(self, features: EventFeatures, ctx: TriggerContext) -> TriggerDecision:
        return TriggerDecision(invoke=False, reason="null", utility=0.0, threshold=0.5)


NO_COALESCING = CoalescerConfig(max_cluster_size=1)

UNBOUNDED_SCHEDULER = SchedulerConfig(
    capacity={
        Priority.P0: 100_000,
        Priority.P1: 100_000,
        Priority.P2: 100_000,
        Priority.P3: 100_000,
    }
)


@dataclass
class ArmSpec:
    key: str
    name: str
    description: str
    coalescing: bool = True
    policy_kind: str = "pulsefeed"     # never | always | pulsefeed
    bounded_queue: bool = True
    ranked: bool = True
    budget_tokens: int = 3_000_000
    budget_usd: float = 30.0
    worker_count: int = 4
    audit_sample_rate: float = 0.01


ARMS: List[ArmSpec] = [
    ArmSpec(
        key="A",
        name="Chronological",
        description="Traditional feed: every event shown, newest first.",
        coalescing=False,
        policy_kind="never",
        ranked=False,
    ),
    ArmSpec(
        key="B",
        name="Rule + embedding",
        description=(
            "Cheap scorer ranking with embedding coalescing, no LLM. "
            "Also models PulseFeed with the provider fully down."
        ),
        coalescing=True,
        policy_kind="never",
        ranked=True,
    ),
    ArmSpec(
        key="C",
        name="LLM-everything",
        description="One enrichment call per event, unbounded queue, no budget.",
        coalescing=False,
        policy_kind="always",
        bounded_queue=False,
        budget_tokens=10_000_000_000,
        budget_usd=1_000_000.0,
        audit_sample_rate=0.0,
    ),
    ArmSpec(
        key="D",
        name="PulseFeed",
        description=(
            "Coalescing + utility trigger + budget/load/deadline awareness + "
            "bounded weighted-fair queue."
        ),
    ),
]


@dataclass
class ArmResult:
    key: str
    name: str
    description: str
    events_ingested: int = 0
    timeline_items: int = 0
    llm_invocations: int = 0
    llm_successes: int = 0
    llm_failures: int = 0
    tokens_used: int = 0
    cost_usd: float = 0.0
    recall_at_k: Dict[int, float] = field(default_factory=dict)
    precision_at_k: Dict[int, float] = field(default_factory=dict)
    recall_total: float = 0.0
    enriched_recall_at_k: Dict[int, float] = field(default_factory=dict)
    # Of all ground-truth-important events, how many are presented anywhere in
    # the feed with an LLM-written explanation rather than a raw line?
    enriched_important_coverage: float = 0.0
    # Fraction of planted storylines (incidents, security events, mentions) with
    # at least one enriched item. Catches the failure where a system enriches
    # plenty of events but misses whole incidents.
    storyline_coverage: float = 0.0
    stale_shed: int = 0
    duplicate_reduction: float = 0.0
    queue_wait: Dict[str, float] = field(default_factory=dict)
    end_to_end: Dict[str, float] = field(default_factory=dict)
    max_queue_depth: int = 0
    dropped: int = 0
    skipped: int = 0
    coalesced_events: int = 0
    estimated_missed_important: float = 0.0
    wall_seconds: float = 0.0
    extra: Dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, Any]:
        out = dict(self.__dict__)
        out["recall_at_k"] = {str(k): v for k, v in self.recall_at_k.items()}
        out["precision_at_k"] = {str(k): v for k, v in self.precision_at_k.items()}
        out["enriched_recall_at_k"] = {
            str(k): v for k, v in self.enriched_recall_at_k.items()
        }
        return out


def build_arm_pipeline(
    spec: ArmSpec,
    clock: Clock,
    *,
    tenant_id: str = "acme",
    mock_config: Optional[MockConfig] = None,
    seed: int = 1337,
) -> PulseFeedPipeline:
    ledger = BudgetLedger()
    ledger.register(
        TenantPlan(
            tenant_id=tenant_id,
            daily_token_budget=spec.budget_tokens,
            daily_usd_budget=spec.budget_usd,
            max_concurrent_llm_jobs=spec.worker_count,
        )
    )

    provider = ResilientProvider(
        MockProvider(mock_config or MockConfig(), clock=clock),
        config=ResilienceConfig(),
        clock=clock,
    )

    if spec.policy_kind == "never":
        policy = NeverInvokePolicy()
    elif spec.policy_kind == "always":
        policy = AlwaysInvokePolicy()
    else:
        policy = build_default_policy(
            ledger, audit_sample_rate=spec.audit_sample_rate, seed=seed
        )

    # The trace labels "@alice" messages as important, so the tenant is
    # configured to watch alice. Without this the cheap scorer has no way to
    # know a mention matters, and every arm would be penalised equally for it.
    scorer = CheapScorer()
    scorer.set_affinity(tenant_id, TenantAffinity(watched_actors={"alice"}))

    coalescer = Coalescer(CoalescerConfig() if spec.coalescing else NO_COALESCING)
    scheduler = BoundedPriorityScheduler(
        SchedulerConfig() if spec.bounded_queue else UNBOUNDED_SCHEDULER, clock=clock
    )

    return PulseFeedPipeline(
        provider=provider,
        config=PipelineConfig(
            worker_count=spec.worker_count,
            audit_sample_rate=spec.audit_sample_rate,
            seed=seed,
            # Retention would otherwise cap coverage for every arm at the same
            # number and hide the differences the experiment is measuring.
            timeline_capacity=10_000_000,
        ),
        clock=clock,
        ledger=ledger,
        scorer=scorer,
        coalescer=coalescer,
        scheduler=scheduler,
        policy=policy,
        metrics=PulseFeedMetrics(),
    )


async def feed_events(
    pipeline: PulseFeedPipeline, events: Sequence[Event], clock: Clock
) -> None:
    """Replay events at their recorded inter-arrival times.

    Pacing matters: feeding the whole trace instantly would measure throughput,
    not queueing, and queueing is the phenomenon under test.
    """
    for event in events:
        delay = event.timestamp - clock.now()
        if delay > 0:
            await clock.sleep(delay)
        await pipeline.ingest(event)


def evaluate(
    pipeline: PulseFeedPipeline,
    spec: ArmSpec,
    events: Sequence[Event],
    important_ids: set,
    k_values: Sequence[int] = (10, 20, 50),
    tenant_id: str = "acme",
) -> ArmResult:
    """Score one arm's output against the trace's ground truth.

    ``recall@K`` is the metric that matters: of the events a human would have
    wanted to see, how many are accounted for by the K items the feed actually
    shows? An item covers every event it cites, which is why coalescing helps —
    one slot can carry a whole incident rather than one line of it.
    """
    all_items = pipeline.timeline(tenant_id, limit=10 ** 9, ranked=spec.ranked)
    result = ArmResult(
        key=spec.key,
        name=spec.name,
        description=spec.description,
        events_ingested=pipeline.stats.events_ingested,
        timeline_items=len(all_items),
        llm_invocations=pipeline.provider.stats["requests"],
        llm_successes=pipeline.stats.enriched,
        llm_failures=pipeline.stats.enrichment_failed,
        tokens_used=pipeline.stats.tokens_used,
        cost_usd=round(pipeline.stats.cost_usd, 4),
        dropped=pipeline.stats.dropped,
        skipped=pipeline.stats.skipped,
        coalesced_events=pipeline.stats.coalesced_events,
        estimated_missed_important=round(
            pipeline.estimated_missed_important_events(), 2
        ),
    )

    total_important = max(1, len(important_ids))
    for k in k_values:
        top = all_items[:k]
        covered = set()
        for item in top:
            covered.update(item.source_event_ids)
        hits = covered & important_ids
        result.recall_at_k[k] = round(len(hits) / total_important, 4)
        relevant_items = sum(
            1 for item in top if set(item.source_event_ids) & important_ids
        )
        result.precision_at_k[k] = round(relevant_items / max(1, len(top)), 4)

        enriched_covered = set()
        for item in top:
            if item.enriched:
                enriched_covered.update(item.source_event_ids)
        result.enriched_recall_at_k[k] = round(
            len(enriched_covered & important_ids) / total_important, 4
        )

    all_covered = set()
    enriched_covered_all = set()
    for item in all_items:
        all_covered.update(item.source_event_ids)
        if item.enriched:
            enriched_covered_all.update(item.source_event_ids)
    result.recall_total = round(len(all_covered & important_ids) / total_important, 4)
    result.enriched_important_coverage = round(
        len(enriched_covered_all & important_ids) / total_important, 4
    )

    storylines: Dict[str, List[str]] = {}
    for event in events:
        name = event.metadata.get("storyline")
        if name:
            storylines.setdefault(str(name), []).append(event.event_id)
    if storylines:
        hit = sum(
            1
            for ids in storylines.values()
            if enriched_covered_all & set(ids)
        )
        result.storyline_coverage = round(hit / len(storylines), 4)

    sched_stats = pipeline.scheduler.stats
    result.stale_shed = (
        sched_stats["rejected_stale"] + sched_stats["expired_on_dequeue"]
    )

    if events:
        result.duplicate_reduction = round(1.0 - len(all_items) / len(events), 4)

    result.queue_wait = pipeline.scheduler.wait_percentiles()
    result.end_to_end = percentiles(
        pipeline.end_to_end_samples["cheap"] + pipeline.end_to_end_samples["llm"]
    )
    result.max_queue_depth = pipeline.scheduler.max_depth_seen
    result.extra = {
        "scheduler": pipeline.scheduler.snapshot(),
        "provider": pipeline.provider.snapshot(),
        "coalescer": dict(pipeline.coalescer.stats),
        "budget": pipeline.ledger.snapshot(tenant_id),
        "supersedes": pipeline.stats.supersedes,
        "audit_samples": pipeline.stats.audit_samples,
        "audit_important_misses": pipeline.stats.audit_important_misses,
    }
    return result


async def run_arm(
    spec: ArmSpec,
    events: Sequence[Event],
    important_ids: set,
    *,
    tenant_id: str = "acme",
    mock_config: Optional[MockConfig] = None,
    seed: int = 1337,
    k_values: Sequence[int] = (10, 20, 50),
    on_pipeline=None,
) -> ArmResult:
    """Run one arm end to end under simulated time.

    Structure: the feeder and the pipeline's workers are ordinary tasks; the
    ``VirtualClock`` driver advances simulated time only when every one of them
    is parked. The run therefore takes exactly as long as the CPU needs while
    reporting latencies in exact simulated seconds.
    """
    import time as _time

    origin = events[0].timestamp if events else 1_700_000_000.0
    clock = VirtualClock(origin=origin)
    pipeline = build_arm_pipeline(
        spec, clock, tenant_id=tenant_id, mock_config=mock_config, seed=seed
    )
    if on_pipeline is not None:
        on_pipeline(pipeline, clock)

    started = _time.monotonic()
    await pipeline.start()

    feeder_done = False

    async def feeder() -> None:
        nonlocal feeder_done
        try:
            await feed_events(pipeline, events, clock)
            await pipeline.flush()
        finally:
            feeder_done = True

    feeder_task = asyncio.create_task(feeder())
    try:
        await clock.run(lambda: feeder_done and pipeline.idle)
    finally:
        feeder_task.cancel()
        await asyncio.gather(feeder_task, return_exceptions=True)
        await pipeline.stop(drain=False)
    wall = _time.monotonic() - started

    result = evaluate(pipeline, spec, events, important_ids, k_values, tenant_id)
    result.wall_seconds = round(wall, 2)
    result.extra["simulated_seconds"] = round(clock.now() - origin, 1)
    return result
