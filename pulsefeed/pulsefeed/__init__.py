"""PulseFeed — event-triggered LLM enrichment for real-time activity feeds.

The organising idea: an LLM call is a scarce online control action, not a step
every piece of data must pass through. Everything in this package exists to
decide which events deserve one, when, at what budget, and what happens to the
feed when the answer is "none of them" because the provider is down.
"""

from .budget import BudgetLedger, TenantPlan
from .clock import ManualClock, RealClock, ScaledClock
from .coalescer import Coalescer, CoalescerConfig
from .memory import EntityMemory, EntityStore
from .models import (
    Event,
    EventCluster,
    EventFeatures,
    Priority,
    SemanticAnnotation,
    Severity,
    Summary,
    SummaryLevel,
    TimelineItem,
)
from .pipeline import PipelineConfig, PulseFeedPipeline, build_pipeline
from .scheduler import BoundedPriorityScheduler, SchedulerConfig
from .scoring import CheapScorer, ScorerConfig
from .summarize import SummaryStore
from .trigger import (
    BudgetAwarePolicy,
    CompositePolicy,
    DeadlineAwarePolicy,
    FixedThresholdPolicy,
    LoadAwarePolicy,
    TriggerContext,
    TriggerDecision,
    build_default_policy,
)

__version__ = "0.1.0"

__all__ = [
    "BudgetLedger",
    "TenantPlan",
    "RealClock",
    "ScaledClock",
    "ManualClock",
    "Coalescer",
    "CoalescerConfig",
    "EntityMemory",
    "EntityStore",
    "Event",
    "EventCluster",
    "EventFeatures",
    "Priority",
    "SemanticAnnotation",
    "Severity",
    "Summary",
    "SummaryLevel",
    "TimelineItem",
    "PipelineConfig",
    "PulseFeedPipeline",
    "build_pipeline",
    "BoundedPriorityScheduler",
    "SchedulerConfig",
    "CheapScorer",
    "ScorerConfig",
    "SummaryStore",
    "BudgetAwarePolicy",
    "CompositePolicy",
    "DeadlineAwarePolicy",
    "FixedThresholdPolicy",
    "LoadAwarePolicy",
    "TriggerContext",
    "TriggerDecision",
    "build_default_policy",
    "__version__",
]
