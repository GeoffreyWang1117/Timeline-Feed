"""Storage interfaces.

Two separate concerns, kept apart on purpose:

``EventBus`` is the **ingestion boundary** — durable buffering, at-least-once
delivery, replay after a crash. Redis Streams implements it today; Kafka would
implement the same four methods. Nothing above this interface knows which.

``PersistenceSink`` is the **record** — events, annotations, summaries and
entity state written durably so a restart does not lose the feed's history.

The split matters because the failure modes differ. Losing the bus means
ingestion stalls and events buffer upstream; losing the sink means the serving
path keeps working from memory while the durable record falls behind. PulseFeed
fails *closed* on the bus (buffer and replay rather than accept-and-forget,
because a lost raw event is unrecoverable) and *open* on the sink (keep serving,
log the failure, because a delayed write is recoverable).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from ..memory import EntityMemory
from ..models import Event, EventFeatures, SemanticAnnotation, Summary


@dataclass
class DeliveredEvent:
    """An event handed to a consumer, plus the receipt needed to acknowledge it."""

    delivery_id: str
    event: Event
    delivery_count: int = 1


class EventBus(ABC):
    """Durable ingestion buffer with at-least-once delivery.

    At-least-once, not exactly-once: a consumer that dies mid-processing will
    see its events again. That is why ``Event.event_id`` is assigned by the
    producer and every sink write is an upsert — redelivery has to be boring.
    """

    @abstractmethod
    async def publish(self, event: Event) -> str:
        """Append an event. Returns the bus-assigned delivery id."""

    @abstractmethod
    async def consume(
        self, group: str, consumer: str, count: int = 32, block_ms: int = 1000
    ) -> List[DeliveredEvent]:
        """Read up to ``count`` undelivered events for this consumer group."""

    @abstractmethod
    async def ack(self, group: str, delivery_ids: Sequence[str]) -> int:
        """Mark deliveries as processed so they are never redelivered."""

    @abstractmethod
    async def reclaim_stale(
        self, group: str, consumer: str, min_idle_ms: int = 60_000, count: int = 32
    ) -> List[DeliveredEvent]:
        """Take over deliveries a dead consumer never acknowledged.

        Without this, a worker that crashes between ``consume`` and ``ack``
        strands its events in the pending list forever — the events are durable
        but nobody is ever going to process them, which is the worst of both.
        """

    @abstractmethod
    async def pending_count(self, group: str) -> int:
        """Events delivered but not yet acknowledged. This is the lag signal."""

    async def close(self) -> None:
        return None


class PersistenceSink(ABC):
    """Durable record of what the pipeline produced."""

    @abstractmethod
    async def save_event(
        self,
        event: Event,
        features: Optional[EventFeatures] = None,
        embedding: Optional[Sequence[float]] = None,
    ) -> None:
        ...

    @abstractmethod
    async def save_annotation(self, annotation: SemanticAnnotation) -> None:
        ...

    @abstractmethod
    async def save_summary(self, summary: Summary) -> None:
        ...

    @abstractmethod
    async def save_entity(self, memory: EntityMemory) -> None:
        ...

    @abstractmethod
    async def load_events(
        self, tenant_id: str, limit: int = 100
    ) -> List[Event]:
        ...

    @abstractmethod
    async def similar_events(
        self, tenant_id: str, embedding: Sequence[float], limit: int = 10
    ) -> List[Tuple[str, float]]:
        """Nearest neighbours by embedding. Returns (event_id, similarity)."""

    @abstractmethod
    async def entity_history(self, tenant_id: str, entity_id: str) -> List[Summary]:
        """Every belief ever held about an entity, superseded ones included."""

    async def health(self) -> Dict[str, Any]:
        return {"healthy": True}

    async def close(self) -> None:
        return None


class NullSink(PersistenceSink):
    """Discards everything. The default, so persistence stays opt-in."""

    async def save_event(self, event, features=None, embedding=None) -> None:
        return None

    async def save_annotation(self, annotation) -> None:
        return None

    async def save_summary(self, summary) -> None:
        return None

    async def save_entity(self, memory) -> None:
        return None

    async def load_events(self, tenant_id: str, limit: int = 100) -> List[Event]:
        return []

    async def similar_events(self, tenant_id, embedding, limit=10):
        return []

    async def entity_history(self, tenant_id: str, entity_id: str) -> List[Summary]:
        return []
