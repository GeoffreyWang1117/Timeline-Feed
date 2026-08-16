"""Storage: the durability boundary.

    EventBus         ingestion buffer with at-least-once delivery and replay
    PersistenceSink  durable record of events, annotations, summaries, entities

In-process implementations are the default so nothing here is required to run
PulseFeed. Redis Streams and PostgreSQL/pgvector implementations are imported
lazily, because their client libraries are optional dependencies and a missing
``redis`` package must not stop the core from importing.
"""

from .base import DeliveredEvent, EventBus, NullSink, PersistenceSink
from .memory import InMemoryEventBus, InMemorySink

__all__ = [
    "DeliveredEvent",
    "EventBus",
    "NullSink",
    "PersistenceSink",
    "InMemoryEventBus",
    "InMemorySink",
    "RedisStreamBus",
    "PostgresSink",
]


def __getattr__(name: str):
    """Lazily import the backends that need optional third-party clients."""
    if name == "RedisStreamBus":
        from .redis_bus import RedisStreamBus

        return RedisStreamBus
    if name == "PostgresSink":
        from .postgres import PostgresSink

        return PostgresSink
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
