"""Core data model for PulseFeed.

Design invariant that everything else depends on:

    A raw ``Event`` is canonical truth. Anything an LLM produces is a
    ``SemanticAnnotation`` that *references* events by id and never mutates or
    replaces them.

That invariant is what makes hallucination survivable: a wrong annotation is a
wrong opinion attached to a set of facts that are still intact underneath it,
and every annotation can be expanded back into the raw events it claims to
summarise.
"""

from __future__ import annotations

import enum
import time
import uuid
from dataclasses import dataclass, field, replace
from typing import Any, Dict, Iterable, List, Optional, Sequence


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


class Priority(enum.IntEnum):
    """Scheduling class. Lower value == more important (P0 wins ties)."""

    P0 = 0  # critical incident
    P1 = 1  # direct user action / mention
    P2 = 2  # normal project event
    P3 = 3  # telemetry / low-value activity

    @property
    def label(self) -> str:
        return self.name


class Severity(enum.Enum):
    UNKNOWN = "unknown"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

    @property
    def rank(self) -> int:
        return {
            Severity.UNKNOWN: 0,
            Severity.INFO: 1,
            Severity.WARNING: 2,
            Severity.ERROR: 3,
            Severity.CRITICAL: 4,
        }[self]


class SummaryLevel(enum.Enum):
    MICRO = "micro"
    CLUSTER = "cluster"
    EPISODE = "episode"
    HOURLY = "hourly"
    DAILY = "daily"


class SummaryStatus(enum.Enum):
    ACTIVE = "active"
    SUPERSEDED = "superseded"


class PathTaken(enum.Enum):
    """Which branch of the pipeline an event actually went down."""

    CHEAP = "cheap"
    LLM = "llm"
    LLM_FAILED = "llm_failed"  # admitted, but enrichment never landed
    DROPPED = "dropped"
    COALESCED = "coalesced"  # folded into another event's cluster


# Freshness deadlines by source class, in seconds. An event whose value window
# has closed is not worth an LLM call no matter how interesting it looked.
DEFAULT_DEADLINES: Dict[str, float] = {
    "pagerduty": 30.0,
    "grafana": 45.0,
    "alertmanager": 30.0,
    "slack": 120.0,
    "discord": 120.0,
    "github": 600.0,
    "ci": 300.0,
    "email": 900.0,
    "rss": 1800.0,
    "telemetry": 60.0,
}
DEFAULT_DEADLINE = 300.0


@dataclass(frozen=True)
class Event:
    """An immutable fact that arrived from some upstream system."""

    source: str
    content: str
    tenant_id: str = "default"
    event_id: str = field(default_factory=lambda: new_id("evt"))
    timestamp: float = field(default_factory=time.time)
    actor: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    entity_ids: List[str] = field(default_factory=list)
    raw_payload: Dict[str, Any] = field(default_factory=dict)

    @property
    def primary_entity(self) -> str:
        """The entity this event is 'about'. Coalescing keys on this."""
        return self.entity_ids[0] if self.entity_ids else f"__source__:{self.source}"

    @property
    def deadline_seconds(self) -> float:
        explicit = self.metadata.get("deadline_seconds")
        if explicit is not None:
            return float(explicit)
        return DEFAULT_DEADLINES.get(self.source, DEFAULT_DEADLINE)

    def expires_at(self) -> float:
        return self.timestamp + self.deadline_seconds

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "tenant_id": self.tenant_id,
            "source": self.source,
            "timestamp": self.timestamp,
            "actor": self.actor,
            "content": self.content,
            "metadata": dict(self.metadata),
            "entity_ids": list(self.entity_ids),
        }


@dataclass
class EventFeatures:
    """Output of the cheap first-stage scorer.

    Every field here must be computable without touching an LLM: this runs on
    100% of traffic, so its cost and latency bound the whole system's ceiling.
    """

    event_id: str
    importance: float = 0.0
    novelty: float = 0.0
    uncertainty: float = 0.0
    risk: float = 0.0
    user_relevance: float = 0.0
    burst_score: float = 0.0
    duplication_score: float = 0.0
    estimated_llm_tokens: int = 0
    estimated_cost_usd: float = 0.0
    priority: Priority = Priority.P2
    signals: Dict[str, float] = field(default_factory=dict)

    def clamped(self) -> "EventFeatures":
        def c(x: float) -> float:
            return max(0.0, min(1.0, x))

        return replace(
            self,
            importance=c(self.importance),
            novelty=c(self.novelty),
            uncertainty=c(self.uncertainty),
            risk=c(self.risk),
            user_relevance=c(self.user_relevance),
            burst_score=c(self.burst_score),
            duplication_score=c(self.duplication_score),
        )


@dataclass
class EventCluster:
    """A group of events the cheap layer believes describe one happening.

    ``needs_boundary_check`` marks the case the cheap layer could not resolve:
    the members are similar enough to be suspicious but not similar enough to
    merge confidently. That is exactly the kind of question worth spending an
    LLM call on.
    """

    cluster_id: str
    tenant_id: str
    entity_id: str
    events: List[Event]
    features: EventFeatures
    opened_at: float
    closed_at: float
    needs_boundary_check: bool = False
    close_reason: str = "idle"

    @property
    def size(self) -> int:
        return len(self.events)

    @property
    def representative(self) -> Event:
        return self.events[0]

    @property
    def event_ids(self) -> List[str]:
        return [e.event_id for e in self.events]

    @property
    def coalesced_count(self) -> int:
        """How many LLM calls the cheap layer avoided by grouping these."""
        return max(0, len(self.events) - 1)

    def deadline(self) -> float:
        """A cluster is only as patient as its most urgent member."""
        return min(e.expires_at() for e in self.events)


@dataclass
class SemanticAnnotation:
    """LLM output. An opinion about events, never a replacement for them."""

    annotation_id: str
    tenant_id: str
    summary: str
    source_event_ids: List[str]
    category: str = "unknown"
    severity: Severity = Severity.UNKNOWN
    entities: List[str] = field(default_factory=list)
    actionability: str = "none"
    causal_links: List[str] = field(default_factory=list)
    confidence: float = 0.0
    model: str = "unknown"
    provider: str = "unknown"
    version: str = "v1"
    generated_at: float = field(default_factory=time.time)
    tokens_used: int = 0
    cost_usd: float = 0.0
    degraded: bool = False  # produced by a fallback path, not the primary model

    def to_dict(self) -> Dict[str, Any]:
        return {
            "annotation_id": self.annotation_id,
            "tenant_id": self.tenant_id,
            "summary": self.summary,
            "category": self.category,
            "severity": self.severity.value,
            "entities": list(self.entities),
            "actionability": self.actionability,
            "causal_links": list(self.causal_links),
            "confidence": self.confidence,
            "source_event_ids": list(self.source_event_ids),
            "model": self.model,
            "provider": self.provider,
            "generated_at": self.generated_at,
            "tokens_used": self.tokens_used,
            "cost_usd": self.cost_usd,
            "degraded": self.degraded,
        }


@dataclass
class Summary:
    """A node in the summarisation hierarchy.

    Summaries are versioned rather than overwritten. When later evidence
    contradicts an earlier conclusion the old summary is marked SUPERSEDED and
    kept, so "what did the system believe at 12:05, and why" stays answerable.
    """

    summary_id: str
    tenant_id: str
    level: SummaryLevel
    text: str
    source_event_ids: List[str]
    entity_ids: List[str] = field(default_factory=list)
    child_summary_ids: List[str] = field(default_factory=list)
    severity: Severity = Severity.UNKNOWN
    confidence: float = 0.0
    model: str = "unknown"
    version: int = 1
    generated_at: float = field(default_factory=time.time)
    status: SummaryStatus = SummaryStatus.ACTIVE
    supersedes: Optional[str] = None
    superseded_by: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "summary_id": self.summary_id,
            "tenant_id": self.tenant_id,
            "level": self.level.value,
            "text": self.text,
            "severity": self.severity.value,
            "confidence": self.confidence,
            "source_event_ids": list(self.source_event_ids),
            "entity_ids": list(self.entity_ids),
            "child_summary_ids": list(self.child_summary_ids),
            "version": self.version,
            "status": self.status.value,
            "supersedes": self.supersedes,
            "superseded_by": self.superseded_by,
            "generated_at": self.generated_at,
            "model": self.model,
        }


@dataclass
class TimelineItem:
    """What the user actually sees. Always traceable back to raw events."""

    item_id: str
    tenant_id: str
    title: str
    timestamp: float
    rank_score: float
    source_event_ids: List[str]
    kind: str = "event"  # event | cluster | episode | digest
    severity: Severity = Severity.UNKNOWN
    enriched: bool = False
    degraded: bool = False
    event_count: int = 1
    entity_ids: List[str] = field(default_factory=list)
    # Set when a higher level of the hierarchy has absorbed this item. The item
    # is kept — it is still the evidence trail for the episode above it — but it
    # no longer competes for a slot in the default feed.
    rolled_up_into: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "item_id": self.item_id,
            "kind": self.kind,
            "rolled_up_into": self.rolled_up_into,
            "title": self.title,
            "timestamp": self.timestamp,
            "severity": self.severity.value,
            "rank_score": round(self.rank_score, 4),
            "enriched": self.enriched,
            "degraded": self.degraded,
            "event_count": self.event_count,
            "entity_ids": list(self.entity_ids),
            "source_event_ids": list(self.source_event_ids),
        }


def covered_event_ids(items: Iterable[TimelineItem]) -> set:
    """Which raw events a set of timeline items actually accounts for."""
    covered: set = set()
    for item in items:
        covered.update(item.source_event_ids)
    return covered


def dedupe_preserving_order(values: Sequence[str]) -> List[str]:
    seen: set = set()
    out: List[str] = []
    for v in values:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out
