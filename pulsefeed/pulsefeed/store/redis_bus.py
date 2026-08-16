"""Redis Streams ingestion bus.

Chosen over Kafka for the first version because the operational cost is a
rounding error and the semantics PulseFeed actually needs — append, consumer
groups, explicit ack, replay of unacknowledged entries, bounded retention — are
all present. The interface is deliberately narrow (publish / consume / ack /
reclaim / pending) precisely so that swapping in Kafka later is an
implementation detail rather than a migration: nothing above ``EventBus`` knows
which broker is underneath.

Three things here are load-bearing rather than decorative:

* **Explicit ack.** An event is not considered handled until the pipeline says
  so, so a worker that dies mid-processing loses nothing.
* **`reclaim_stale`.** Without it, deliveries stranded by a dead consumer stay
  pending forever — durable, but with nobody coming to collect them.
* **Bounded retention (`MAXLEN ~`).** An unbounded stream converts a slow
  consumer into a Redis OOM. Approximate trimming keeps it cheap.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Sequence

from ..models import Event
from .base import DeliveredEvent, EventBus


def event_to_fields(event: Event) -> Dict[str, str]:
    """Flatten an Event into the string map a stream entry holds."""
    return {
        "event_id": event.event_id,
        "tenant_id": event.tenant_id,
        "source": event.source,
        "timestamp": repr(event.timestamp),
        "actor": event.actor or "",
        "content": event.content,
        "entity_ids": json.dumps(event.entity_ids),
        "metadata": json.dumps(event.metadata, default=str),
    }


def fields_to_event(fields: Dict[Any, Any]) -> Event:
    def get(key: str, default: str = "") -> str:
        value = fields.get(key, fields.get(key.encode(), default))
        return value.decode() if isinstance(value, bytes) else value

    return Event(
        event_id=get("event_id"),
        tenant_id=get("tenant_id", "default"),
        source=get("source", "unknown"),
        content=get("content"),
        timestamp=float(get("timestamp", "0") or 0.0),
        actor=get("actor") or None,
        entity_ids=json.loads(get("entity_ids", "[]") or "[]"),
        metadata=json.loads(get("metadata", "{}") or "{}"),
    )


class RedisStreamBus(EventBus):
    """``EventBus`` over a single Redis stream with consumer groups."""

    def __init__(
        self,
        url: str = "redis://localhost:6379/0",
        stream: str = "pulsefeed:events",
        maxlen: int = 1_000_000,
        client: Any = None,
    ) -> None:
        self.url = url
        self.stream = stream
        self.maxlen = maxlen
        self._client = client
        self._groups_created: set = set()

    async def _redis(self):
        if self._client is None:
            try:
                import redis.asyncio as aioredis
            except ImportError as exc:  # pragma: no cover - env dependent
                raise RuntimeError(
                    "redis is required for RedisStreamBus: pip install 'pulsefeed[store]'"
                ) from exc
            self._client = aioredis.from_url(self.url, decode_responses=True)
        return self._client

    async def ensure_group(self, group: str) -> None:
        """Create the consumer group, tolerating the race where it exists.

        ``MKSTREAM`` matters: consumers usually start before the first producer,
        and without it the group creation fails on a stream that does not exist
        yet.
        """
        if group in self._groups_created:
            return
        client = await self._redis()
        try:
            await client.xgroup_create(self.stream, group, id="0", mkstream=True)
        except Exception as exc:  # redis raises BUSYGROUP if it already exists
            if "BUSYGROUP" not in str(exc):
                raise
        self._groups_created.add(group)

    async def publish(self, event: Event) -> str:
        client = await self._redis()
        return await client.xadd(
            self.stream,
            event_to_fields(event),
            maxlen=self.maxlen,
            approximate=True,
        )

    async def publish_many(self, events: Sequence[Event]) -> List[str]:
        """Pipelined publish. One round trip instead of N."""
        client = await self._redis()
        async with client.pipeline(transaction=False) as pipe:
            for event in events:
                pipe.xadd(
                    self.stream,
                    event_to_fields(event),
                    maxlen=self.maxlen,
                    approximate=True,
                )
            return await pipe.execute()

    async def consume(
        self, group: str, consumer: str, count: int = 32, block_ms: int = 1000
    ) -> List[DeliveredEvent]:
        await self.ensure_group(group)
        client = await self._redis()
        response = await client.xreadgroup(
            groupname=group,
            consumername=consumer,
            streams={self.stream: ">"},
            count=count,
            block=block_ms,
        )
        out: List[DeliveredEvent] = []
        for _stream, entries in response or []:
            for delivery_id, fields in entries:
                out.append(DeliveredEvent(delivery_id, fields_to_event(fields)))
        return out

    async def ack(self, group: str, delivery_ids: Sequence[str]) -> int:
        if not delivery_ids:
            return 0
        client = await self._redis()
        return await client.xack(self.stream, group, *delivery_ids)

    async def reclaim_stale(
        self, group: str, consumer: str, min_idle_ms: int = 60_000, count: int = 32
    ) -> List[DeliveredEvent]:
        await self.ensure_group(group)
        client = await self._redis()
        result = await client.xautoclaim(
            name=self.stream,
            groupname=group,
            consumername=consumer,
            min_idle_time=min_idle_ms,
            start_id="0-0",
            count=count,
        )
        # xautoclaim returns (next_cursor, claimed, [deleted]) across versions.
        claimed = result[1] if isinstance(result, (list, tuple)) and len(result) > 1 else []
        return [
            DeliveredEvent(delivery_id, fields_to_event(fields), delivery_count=2)
            for delivery_id, fields in claimed
            if fields
        ]

    async def pending_count(self, group: str) -> int:
        await self.ensure_group(group)
        client = await self._redis()
        info = await client.xpending(self.stream, group)
        if isinstance(info, dict):
            return int(info.get("pending", 0))
        return int(info[0]) if info else 0

    async def length(self) -> int:
        client = await self._redis()
        return await client.xlen(self.stream)

    async def trim(self, maxlen: Optional[int] = None) -> int:
        client = await self._redis()
        return await client.xtrim(
            self.stream, maxlen=maxlen or self.maxlen, approximate=True
        )

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
