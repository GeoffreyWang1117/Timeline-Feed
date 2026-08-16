"""Entity memory: the timeline's state, as opposed to its log.

A feed that is only a list of events makes the LLM re-read history to answer
"is this new?" every single time. Instead PulseFeed keeps a small, bounded
record per entity — Checkout Service, Deployment #813, Incident #27 — and feeds
the model *the current event plus that entity's memory*, not the timeline.

Two consequences, both load-bearing:

* prompt size stops growing with the age of the feed, so token cost per call is
  bounded no matter how long the system has been running;
* the model gets continuity anyway, because the memory carries forward what
  previous calls concluded.
"""

from __future__ import annotations

from collections import OrderedDict, deque
from dataclasses import dataclass, field
from typing import Deque, List, Optional, Sequence

from .models import Event, SemanticAnnotation, Severity


@dataclass
class EntityMemory:
    entity_id: str
    tenant_id: str
    kind: str = "unknown"
    current_state: str = "unknown"
    recent_event_ids: Deque[str] = field(default_factory=lambda: deque(maxlen=32))
    recent_contents: Deque[str] = field(default_factory=lambda: deque(maxlen=8))
    recent_summary: str = ""
    historical_incidents: List[str] = field(default_factory=list)
    severity: Severity = Severity.UNKNOWN
    last_update: float = 0.0
    event_count: int = 0
    relevance: float = 0.0

    def observe(self, event: Event) -> None:
        self.recent_event_ids.append(event.event_id)
        self.recent_contents.append(event.content)
        self.last_update = max(self.last_update, event.timestamp)
        self.event_count += 1
        if self.kind == "unknown":
            self.kind = str(event.metadata.get("entity_kind", event.source))

    def apply(self, annotation: SemanticAnnotation) -> None:
        """Fold an LLM conclusion into state. Never touches the raw events."""
        self.recent_summary = annotation.summary
        self.last_update = max(self.last_update, annotation.generated_at)
        if annotation.severity.rank >= self.severity.rank:
            self.severity = annotation.severity
        if annotation.severity in (Severity.CRITICAL, Severity.ERROR):
            self.current_state = "degraded"
            self.historical_incidents.append(annotation.annotation_id)
            # Bounded: an entity with a long history keeps only its last few.
            del self.historical_incidents[:-10]
        elif annotation.severity in (Severity.INFO,) and self.current_state == "degraded":
            self.current_state = "recovering"

    def context_block(self, max_chars: int = 600) -> str:
        """Compact, plain-text memory for prompt construction."""
        parts = [
            f"entity: {self.entity_id}",
            f"kind: {self.kind}",
            f"state: {self.current_state}",
            f"severity: {self.severity.value}",
            f"events_seen: {self.event_count}",
        ]
        if self.recent_summary:
            parts.append(f"last_conclusion: {self.recent_summary}")
        if self.recent_contents:
            recent = " | ".join(list(self.recent_contents)[-3:])
            parts.append(f"recent: {recent}")
        block = "\n".join(parts)
        return block[:max_chars]


class EntityStore:
    """Bounded LRU store of entity memories, keyed by (tenant, entity).

    Bounded on purpose: an unbounded memory is an unbounded prompt budget and an
    unbounded heap. Eviction is by least-recently-used, which in practice means
    entities that have gone quiet.
    """

    def __init__(self, max_entities: int = 10_000) -> None:
        self.max_entities = max_entities
        self._store: "OrderedDict[tuple, EntityMemory]" = OrderedDict()
        self.evictions = 0

    def get(self, tenant_id: str, entity_id: str) -> Optional[EntityMemory]:
        key = (tenant_id, entity_id)
        mem = self._store.get(key)
        if mem is not None:
            self._store.move_to_end(key)
        return mem

    def get_or_create(self, tenant_id: str, entity_id: str) -> EntityMemory:
        mem = self.get(tenant_id, entity_id)
        if mem is None:
            mem = EntityMemory(entity_id=entity_id, tenant_id=tenant_id)
            self._store[(tenant_id, entity_id)] = mem
            self._evict_if_needed()
        return mem

    def observe(self, event: Event) -> List[EntityMemory]:
        """Record an event against every entity it names."""
        entity_ids = event.entity_ids or [event.primary_entity]
        memories = []
        for entity_id in entity_ids:
            mem = self.get_or_create(event.tenant_id, entity_id)
            mem.observe(event)
            memories.append(mem)
        return memories

    def apply_annotation(self, annotation: SemanticAnnotation) -> None:
        for entity_id in annotation.entities:
            mem = self.get(annotation.tenant_id, entity_id)
            if mem is not None:
                mem.apply(annotation)

    def context_for(
        self, tenant_id: str, entity_ids: Sequence[str], max_entities: int = 3
    ) -> str:
        """Assemble prompt context for the entities an event touches."""
        blocks = []
        for entity_id in list(entity_ids)[:max_entities]:
            mem = self.get(tenant_id, entity_id)
            if mem is not None and mem.event_count > 0:
                blocks.append(mem.context_block())
        return "\n---\n".join(blocks)

    def _evict_if_needed(self) -> None:
        while len(self._store) > self.max_entities:
            self._store.popitem(last=False)
            self.evictions += 1

    def __len__(self) -> int:
        return len(self._store)

    def all(self) -> List[EntityMemory]:
        return list(self._store.values())

    def reset(self) -> None:
        self._store.clear()
        self.evictions = 0
