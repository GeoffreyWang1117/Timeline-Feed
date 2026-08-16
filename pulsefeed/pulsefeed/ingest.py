"""Bus-driven ingestion.

Producers publish to an ``EventBus``; this worker consumes and feeds the
pipeline, acknowledging only after the event is safely through. That ordering is
the whole point — an event is redelivered if the process dies mid-flight, so a
crash costs latency rather than data.

It also closes the gap the failure-injection harness could only simulate before:
"Redis went away for two minutes" is now a real thing that can happen to a real
component, and the answer is that the producer's publish fails (so it retries or
buffers) while the consumer's backlog drains on reconnect.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import List, Optional

from .clock import Clock, RealClock
from .pipeline import PulseFeedPipeline
from .store.base import EventBus


@dataclass
class IngestConfig:
    group: str = "pulsefeed"
    consumer: str = "worker-0"
    batch_size: int = 64
    block_ms: int = 500
    # How long a delivery may sit unacknowledged before another consumer may
    # take it over. Too low and healthy-but-slow consumers get their work
    # stolen; too high and a crashed consumer's events wait that long.
    reclaim_idle_ms: int = 30_000
    reclaim_every: int = 50  # consume loops between reclaim sweeps


class IngestWorker:
    """Consumes an ``EventBus`` into a pipeline, acking after processing."""

    def __init__(
        self,
        bus: EventBus,
        pipeline: PulseFeedPipeline,
        config: Optional[IngestConfig] = None,
        clock: Optional[Clock] = None,
    ) -> None:
        self.bus = bus
        self.pipeline = pipeline
        self.config = config or IngestConfig()
        self.clock = clock or RealClock()
        self.stats = {
            "consumed": 0,
            "acked": 0,
            "reclaimed": 0,
            "failed": 0,
            "loops": 0,
        }
        self._task: Optional[asyncio.Task] = None
        self._running = False

    async def drain_once(self) -> int:
        """One consume/process/ack cycle. Returns events processed."""
        self.stats["loops"] += 1

        deliveries = await self.bus.consume(
            self.config.group,
            self.config.consumer,
            count=self.config.batch_size,
            block_ms=self.config.block_ms,
        )

        if self.stats["loops"] % self.config.reclaim_every == 0:
            stale = await self.bus.reclaim_stale(
                self.config.group,
                self.config.consumer,
                min_idle_ms=self.config.reclaim_idle_ms,
                count=self.config.batch_size,
            )
            if stale:
                self.stats["reclaimed"] += len(stale)
                deliveries = list(deliveries) + list(stale)

        if not deliveries:
            return 0

        acked: List[str] = []
        for delivery in deliveries:
            try:
                await self.pipeline.ingest(delivery.event)
            except Exception:
                # Leave it unacknowledged: it will be redelivered rather than
                # silently lost. A permanently poisonous event will keep coming
                # back, which is loud, and loud beats invisible.
                self.stats["failed"] += 1
                continue
            acked.append(delivery.delivery_id)

        self.stats["consumed"] += len(deliveries)
        if acked:
            self.stats["acked"] += await self.bus.ack(self.config.group, acked)
        return len(acked)

    async def run(self) -> None:
        self._running = True
        try:
            while self._running:
                processed = await self.drain_once()
                if processed == 0:
                    await asyncio.sleep(0)
        except asyncio.CancelledError:  # pragma: no cover - shutdown path
            pass

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self.run(), name="pulsefeed-ingest")

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    async def lag(self) -> int:
        """Unacknowledged deliveries. The number to alert on."""
        return await self.bus.pending_count(self.config.group)
