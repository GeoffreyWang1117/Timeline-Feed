"""Bounded, weighted-fair scheduling for LLM work.

The numbers that motivate this module: 10,000 events/min arriving, ~100
events/min of LLM capacity. The wrong answer is to queue the other 9,900 —
that converts a throughput problem into an unbounded-memory problem and an
unbounded-latency one, and every item that finally gets served is answering a
question nobody remembers asking.

So the queue is bounded per priority class, and overflow is handled *per class*
with different policies:

    P0  critical incident   reserved capacity, never dropped for load
    P1  direct user action  queued; rejected to the cheap path when full
    P2  normal project      triggers harder coalescing before it fills
    P3  telemetry           dropped first, and dropped silently

Dispatch is deficit round robin at 8:4:2:1 rather than strict priority. Strict
priority is the intuitive choice and it is wrong: a sustained P0 stream would
starve P1-P3 forever. Weighted fairness gives the important classes most of the
capacity while guaranteeing everyone a floor.
"""

from __future__ import annotations

import asyncio
import enum
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional

from .clock import Clock, RealClock
from .models import EventCluster, Priority
from .trigger import TriggerDecision


class Admission(enum.Enum):
    ADMITTED = "admitted"
    DROPPED = "dropped"                # class queue full, load-shed
    REJECTED_COALESCE = "coalesce"     # pushback: group harder, try again later
    REJECTED_STALE = "stale"           # deadline already passed at enqueue


@dataclass
class WorkItem:
    cluster: EventCluster
    priority: Priority
    decision: TriggerDecision
    enqueued_at: float
    deadline_at: float
    audit: bool = False

    def is_expired(self, now: float) -> bool:
        return now >= self.deadline_at

    def wait_seconds(self, now: float) -> float:
        return max(0.0, now - self.enqueued_at)


@dataclass
class SchedulerConfig:
    capacity: Dict[Priority, int] = field(
        default_factory=lambda: {
            Priority.P0: 64,   # reserved; only P0 may use it
            Priority.P1: 256,
            Priority.P2: 512,
            Priority.P3: 128,
        }
    )
    weights: Dict[Priority, int] = field(
        default_factory=lambda: {
            Priority.P0: 8,
            Priority.P1: 4,
            Priority.P2: 2,
            Priority.P3: 1,
        }
    )
    # Fraction of P2 capacity at which the scheduler asks the coalescer to
    # group more aggressively. Backpressure should start before the cliff.
    coalesce_pressure_ratio: float = 0.6
    drop_pressure_ratio: float = 0.9


@dataclass
class AdmissionResult:
    admission: Admission
    reason: str = ""
    evicted: Optional[WorkItem] = None


class BoundedPriorityScheduler:
    """Bounded multi-class queue with deficit round-robin dispatch."""

    def __init__(
        self,
        config: Optional[SchedulerConfig] = None,
        clock: Optional[Clock] = None,
        wait_sample_cap: int = 10_000,
    ) -> None:
        self.config = config or SchedulerConfig()
        self.clock = clock or RealClock()
        self._queues: Dict[Priority, Deque[WorkItem]] = {
            p: deque() for p in Priority
        }
        self._credits: Dict[Priority, int] = dict(self.config.weights)
        self._not_empty = asyncio.Event()
        self._closed = False
        self.stats = {
            "admitted": 0,
            "dropped": 0,
            "rejected_coalesce": 0,
            "rejected_stale": 0,
            "expired_on_dequeue": 0,
            "dispatched": 0,
        }
        self.dropped_by_class: Dict[Priority, int] = {p: 0 for p in Priority}
        # Rolling window, not a transcript: percentiles over the most recent
        # dispatches are also the more useful number operationally.
        self.wait_samples: Deque[float] = deque(maxlen=wait_sample_cap)
        self.max_depth_seen = 0
        # Per class, because classes saturate independently: P3 can be full and
        # shedding while total depth is nowhere near total capacity.
        self.max_depth_by_class: Dict[Priority, int] = {p: 0 for p in Priority}

    # -- introspection -----------------------------------------------------

    @property
    def depth(self) -> int:
        return sum(len(q) for q in self._queues.values())

    @property
    def capacity(self) -> int:
        return sum(self.config.capacity.values())

    def depth_of(self, priority: Priority) -> int:
        return len(self._queues[priority])

    def depth_by_class(self) -> Dict[str, int]:
        return {p.label: len(self._queues[p]) for p in Priority}

    @property
    def under_coalesce_pressure(self) -> bool:
        """Whether the pipeline should switch the coalescer to aggressive mode."""
        p2_cap = self.config.capacity[Priority.P2]
        p3_cap = self.config.capacity[Priority.P3]
        ratio = self.config.coalesce_pressure_ratio
        return (
            len(self._queues[Priority.P2]) >= p2_cap * ratio
            or len(self._queues[Priority.P3]) >= p3_cap * ratio
        )

    def estimated_wait(self, priority: Priority, service_rate: float) -> float:
        """Rough queueing delay for a new item of this class.

        Feeds the deadline-aware trigger: an event whose estimated wait exceeds
        its freshness window is refused admission rather than queued to rot.
        """
        if service_rate <= 0:
            return float("inf")
        weights = self.config.weights
        total_weight = sum(weights.values())
        share = weights[priority] / total_weight
        ahead = sum(
            len(self._queues[p]) for p in Priority if p.value <= priority.value
        )
        return ahead / max(1e-6, service_rate * max(share, 1e-3))

    # -- admission ---------------------------------------------------------

    def enqueue(self, item: WorkItem) -> AdmissionResult:
        now = self.clock.now()
        if item.is_expired(now):
            self.stats["rejected_stale"] += 1
            return AdmissionResult(Admission.REJECTED_STALE, "deadline already passed")

        queue = self._queues[item.priority]
        capacity = self.config.capacity[item.priority]

        if len(queue) < capacity:
            queue.append(item)
            self.stats["admitted"] += 1
            self.max_depth_seen = max(self.max_depth_seen, self.depth)
            self.max_depth_by_class[item.priority] = max(
                self.max_depth_by_class[item.priority], len(queue)
            )
            self._not_empty.set()
            return AdmissionResult(Admission.ADMITTED)

        return self._handle_overflow(item, queue, now)

    def _handle_overflow(
        self, item: WorkItem, queue: Deque[WorkItem], now: float
    ) -> AdmissionResult:
        priority = item.priority

        if priority is Priority.P3:
            # Telemetry is aggregate-able by nature; one dropped sample is not
            # one lost fact. Drop and count.
            self.stats["dropped"] += 1
            self.dropped_by_class[priority] += 1
            return AdmissionResult(Admission.DROPPED, "p3 load shed")

        if priority is Priority.P2:
            # Reclaim space from anything already stale before shedding.
            evicted = self._evict_expired(queue, now)
            if evicted is not None:
                queue.append(item)
                self.stats["admitted"] += 1
                self._not_empty.set()
                return AdmissionResult(Admission.ADMITTED, "replaced stale item", evicted)
            self.stats["rejected_coalesce"] += 1
            return AdmissionResult(
                Admission.REJECTED_COALESCE, "p2 full; coalesce and retry"
            )

        if priority is Priority.P1:
            evicted = self._evict_expired(queue, now)
            if evicted is not None:
                queue.append(item)
                self.stats["admitted"] += 1
                self._not_empty.set()
                return AdmissionResult(Admission.ADMITTED, "replaced stale item", evicted)
            self.stats["rejected_coalesce"] += 1
            return AdmissionResult(
                Admission.REJECTED_COALESCE, "p1 full; falling back to cheap path"
            )

        # P0. The reserved lane is full, which means many simultaneous critical
        # events. Memory still has to stay bounded, so the *oldest* P0 gives way
        # — it has had the longest to be useful and is closest to its deadline.
        evicted = self._evict_expired(queue, now)
        if evicted is None and queue:
            evicted = queue.popleft()
            self.stats["dropped"] += 1
            self.dropped_by_class[Priority.P0] += 1
        queue.append(item)
        self.stats["admitted"] += 1
        self._not_empty.set()
        return AdmissionResult(Admission.ADMITTED, "p0 reserved lane evicted oldest", evicted)

    def _evict_expired(self, queue: Deque[WorkItem], now: float) -> Optional[WorkItem]:
        for _ in range(len(queue)):
            candidate = queue[0]
            if candidate.is_expired(now):
                queue.popleft()
                self.stats["expired_on_dequeue"] += 1
                return candidate
            queue.rotate(-1)
        return None

    # -- dispatch ----------------------------------------------------------

    def _select_queue(self) -> Optional[Priority]:
        """Deficit round robin over non-empty classes."""
        for _ in range(2):  # at most one refill pass
            for priority in Priority:  # P0..P3, ties broken by importance
                if self._credits[priority] > 0 and self._queues[priority]:
                    self._credits[priority] -= 1
                    return priority
            if not any(self._queues[p] for p in Priority):
                return None
            self._credits = dict(self.config.weights)
        return None

    def try_dequeue(self) -> Optional[WorkItem]:
        """Non-blocking dequeue. Drops items whose deadline passed while queued."""
        now = self.clock.now()
        while True:
            priority = self._select_queue()
            if priority is None:
                self._not_empty.clear()
                return None
            item = self._queues[priority].popleft()
            if item.is_expired(now):
                self.stats["expired_on_dequeue"] += 1
                continue
            self.stats["dispatched"] += 1
            self.wait_samples.append(item.wait_seconds(now))
            if self.depth == 0:
                self._not_empty.clear()
            return item

    async def dequeue(self) -> Optional[WorkItem]:
        """Blocking dequeue. Returns None once the scheduler is closed and drained.

        Waits on an event rather than polling with a timeout: a real-time poll
        would make workers permanently runnable, which breaks simulated-time
        replay (the clock could never conclude that everything is parked).
        """
        while not self._closed:
            item = self.try_dequeue()
            if item is not None:
                return item
            await self._not_empty.wait()
        return self.try_dequeue()

    def close(self) -> None:
        self._closed = True
        self._not_empty.set()

    # -- reporting ---------------------------------------------------------

    def snapshot(self) -> Dict[str, object]:
        return {
            "depth": self.depth,
            "capacity": self.capacity,
            "max_depth_seen": self.max_depth_seen,
            "max_depth_by_class": {
                p.label: d for p, d in self.max_depth_by_class.items()
            },
            "by_class": self.depth_by_class(),
            "dropped_by_class": {p.label: c for p, c in self.dropped_by_class.items()},
            "stats": dict(self.stats),
        }

    def wait_percentiles(self) -> Dict[str, float]:
        return percentiles(list(self.wait_samples))

    def reset(self) -> None:
        for q in self._queues.values():
            q.clear()
        self._credits = dict(self.config.weights)
        self._closed = False
        self._not_empty.clear()
        self.wait_samples.clear()
        self.max_depth_seen = 0
        for p in self.max_depth_by_class:
            self.max_depth_by_class[p] = 0
        for k in self.stats:
            self.stats[k] = 0
        for p in self.dropped_by_class:
            self.dropped_by_class[p] = 0


def percentiles(samples: List[float]) -> Dict[str, float]:
    """p50/p95/p99 by nearest-rank. Small sample sizes are the common case."""
    if not samples:
        return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0, "count": 0}
    ordered = sorted(samples)

    def at(q: float) -> float:
        index = min(len(ordered) - 1, max(0, int(round(q * len(ordered) + 0.5)) - 1))
        return ordered[index]

    return {
        "p50": round(at(0.50), 4),
        "p95": round(at(0.95), 4),
        "p99": round(at(0.99), 4),
        "max": round(ordered[-1], 4),
        "count": len(ordered),
    }
