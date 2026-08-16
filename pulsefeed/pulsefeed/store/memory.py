"""In-process implementations of the storage interfaces.

These are not toys. They are the default, they are what the tests and the replay
harness run against, and they model the same at-least-once semantics as the
Redis implementation — including a pending list and stale-delivery reclaim — so
a bug in how the pipeline handles redelivery shows up here rather than only in
production.

What they do not provide is durability across a process restart. That is the
entire reason the Redis and Postgres implementations exist.
"""

from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

from ..embedding import cosine
from ..memory import EntityMemory
from ..models import Event, EventFeatures, SemanticAnnotation, Summary
from .base import DeliveredEvent, EventBus, PersistenceSink


@dataclass
class _Pending:
    event: Event
    delivered_at: float
    consumer: str
    delivery_count: int


class InMemoryEventBus(EventBus):
    """At-least-once bus backed by a bounded deque.

    ``maxlen`` matters: an unbounded ingestion buffer just moves the
    out-of-memory failure one component upstream. When the buffer is full the
    oldest *unconsumed* entries are dropped and counted, which is a visible,
    measurable loss rather than a silent stall.
    """

    def __init__(self, maxlen: int = 100_000) -> None:
        self.maxlen = maxlen
        self._log: "OrderedDict[str, Event]" = OrderedDict()
        self._offsets: Dict[str, int] = {}
        self._pending: Dict[str, Dict[str, _Pending]] = {}
        self._seq = 0
        self._clock = asyncio.get_event_loop
        self.dropped = 0
        self._waiters: List[asyncio.Future] = []

    def _next_id(self) -> str:
        self._seq += 1
        return f"{self._seq:012d}-0"

    async def publish(self, event: Event) -> str:
        delivery_id = self._next_id()
        self._log[delivery_id] = event
        while len(self._log) > self.maxlen:
            self._log.popitem(last=False)
            self.dropped += 1
        for waiter in self._waiters:
            if not waiter.done():
                waiter.set_result(None)
        self._waiters.clear()
        return delivery_id

    async def consume(
        self, group: str, consumer: str, count: int = 32, block_ms: int = 1000
    ) -> List[DeliveredEvent]:
        pending = self._pending.setdefault(group, {})
        offset = self._offsets.get(group, 0)
        items = list(self._log.items())

        now = time.monotonic()
        out: List[DeliveredEvent] = []
        index = offset
        while index < len(items) and len(out) < count:
            delivery_id, event = items[index]
            index += 1
            pending[delivery_id] = _Pending(event, now, consumer, 1)
            out.append(DeliveredEvent(delivery_id, event, 1))
        self._offsets[group] = index
        return out

    async def ack(self, group: str, delivery_ids: Sequence[str]) -> int:
        pending = self._pending.setdefault(group, {})
        acked = 0
        for delivery_id in delivery_ids:
            if pending.pop(delivery_id, None) is not None:
                acked += 1
        return acked

    async def reclaim_stale(
        self, group: str, consumer: str, min_idle_ms: int = 60_000, count: int = 32
    ) -> List[DeliveredEvent]:
        """Hand back only deliveries that have gone *idle*.

        Honouring ``min_idle_ms`` is the whole safety property here. An earlier
        version ignored it and returned everything pending, which meant a
        consumer's own in-flight batch was handed back to it on the next loop
        and every event was processed twice. Redelivery is supposed to be the
        response to a dead consumer, not to a busy one.
        """
        pending = self._pending.setdefault(group, {})
        now = time.monotonic()
        out = []
        for delivery_id, entry in list(pending.items()):
            if len(out) >= count:
                break
            if (now - entry.delivered_at) * 1000.0 < min_idle_ms:
                continue
            if entry.consumer == consumer:
                continue  # our own work; not abandoned
            entry.delivery_count += 1
            entry.consumer = consumer
            entry.delivered_at = now
            out.append(DeliveredEvent(delivery_id, entry.event, entry.delivery_count))
        return out

    async def pending_count(self, group: str) -> int:
        return len(self._pending.get(group, {}))

    def backlog(self, group: str) -> int:
        """Published but not yet delivered to this group."""
        return max(0, len(self._log) - self._offsets.get(group, 0))


class InMemorySink(PersistenceSink):
    """Bounded in-process record with brute-force vector search."""

    def __init__(self, max_events: int = 200_000) -> None:
        self.max_events = max_events
        self.events: "OrderedDict[str, Event]" = OrderedDict()
        self.embeddings: Dict[str, List[float]] = {}
        self.features: Dict[str, EventFeatures] = {}
        self.annotations: Dict[str, SemanticAnnotation] = {}
        self.summaries: Dict[str, Summary] = {}
        self.entities: Dict[Tuple[str, str], EntityMemory] = {}
        self.writes = 0

    async def save_event(
        self,
        event: Event,
        features: Optional[EventFeatures] = None,
        embedding: Optional[Sequence[float]] = None,
    ) -> None:
        self.events[event.event_id] = event
        if features is not None:
            self.features[event.event_id] = features
        if embedding is not None:
            self.embeddings[event.event_id] = list(embedding)
        self.writes += 1
        while len(self.events) > self.max_events:
            evicted, _ = self.events.popitem(last=False)
            self.embeddings.pop(evicted, None)
            self.features.pop(evicted, None)

    async def save_annotation(self, annotation: SemanticAnnotation) -> None:
        self.annotations[annotation.annotation_id] = annotation
        self.writes += 1

    async def save_summary(self, summary: Summary) -> None:
        self.summaries[summary.summary_id] = summary
        self.writes += 1

    async def save_entity(self, memory: EntityMemory) -> None:
        self.entities[(memory.tenant_id, memory.entity_id)] = memory
        self.writes += 1

    async def load_events(self, tenant_id: str, limit: int = 100) -> List[Event]:
        matching = [e for e in self.events.values() if e.tenant_id == tenant_id]
        return sorted(matching, key=lambda e: -e.timestamp)[:limit]

    async def similar_events(
        self, tenant_id: str, embedding: Sequence[float], limit: int = 10
    ) -> List[Tuple[str, float]]:
        scored = [
            (event_id, cosine(embedding, vector))
            for event_id, vector in self.embeddings.items()
            if self.events.get(event_id) is not None
            and self.events[event_id].tenant_id == tenant_id
        ]
        scored.sort(key=lambda pair: -pair[1])
        return scored[:limit]

    async def entity_history(self, tenant_id: str, entity_id: str) -> List[Summary]:
        return sorted(
            (
                s
                for s in self.summaries.values()
                if s.tenant_id == tenant_id and entity_id in s.entity_ids
            ),
            key=lambda s: s.generated_at,
        )
