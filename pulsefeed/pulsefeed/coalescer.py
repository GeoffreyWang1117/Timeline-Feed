"""Cheap deduplication and coalescing, upstream of the trigger.

The dominant failure mode of a real feed is too many events, not too few. Four
consecutive "CPU above 94%" readings are one fact, and sending them to an LLM
four times buys four copies of the same sentence at four times the price.

So events are grouped before the trigger sees them, using only cheap signals:
same entity, close in time, high embedding similarity. The interesting part is
the middle band — similar enough to be suspicious, not similar enough to merge
confidently. Those clusters are flagged ``needs_boundary_check`` and become the
best possible use of an LLM call: a question the cheap layer provably cannot
answer ("are these the same incident, or a second one?").
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from .embedding import cosine
from .models import Event, EventCluster, EventFeatures, Priority, new_id
from .scoring import aggregate_features


@dataclass
class CoalescerConfig:
    window_seconds: float = 90.0        # max span of one cluster
    idle_close_seconds: float = 20.0    # quiet period that closes a cluster
    max_cluster_size: int = 25
    merge_similarity: float = 0.80      # at or above: confidently the same thing
    ambiguous_similarity: float = 0.58  # below: confidently different
    deadline_guard_seconds: float = 5.0 # close early rather than miss freshness
    aggressive_window_multiplier: float = 4.0
    aggressive_similarity_drop: float = 0.18


@dataclass
class _OpenCluster:
    cluster_id: str
    tenant_id: str
    entity_id: str
    events: List[Event] = field(default_factory=list)
    features: List[EventFeatures] = field(default_factory=list)
    centroid: List[float] = field(default_factory=list)
    opened_at: float = 0.0
    last_event_at: float = 0.0
    needs_boundary_check: bool = False

    def add(self, event: Event, features: EventFeatures, vector: List[float]) -> None:
        n = len(self.events)
        if not self.centroid:
            self.centroid = list(vector)
        else:
            # Running mean, then renormalise so cosine stays meaningful.
            self.centroid = [
                (c * n + v) / (n + 1) for c, v in zip(self.centroid, vector)
            ]
            norm = sum(c * c for c in self.centroid) ** 0.5
            if norm > 0:
                self.centroid = [c / norm for c in self.centroid]
        self.events.append(event)
        self.features.append(features)
        self.last_event_at = max(self.last_event_at, event.timestamp)

    def deadline(self) -> float:
        return min(e.expires_at() for e in self.events)


class Coalescer:
    """Windowed, entity-keyed clustering with an explicit ambiguity band."""

    def __init__(self, config: Optional[CoalescerConfig] = None) -> None:
        self.config = config or CoalescerConfig()
        self._open: Dict[Tuple[str, str], _OpenCluster] = {}
        self._aggressive = False
        self.stats = {
            "events_in": 0,
            "clusters_out": 0,
            "events_coalesced": 0,
            "boundary_checks": 0,
        }

    # -- backpressure knob -------------------------------------------------

    def set_aggressive(self, enabled: bool) -> None:
        """Under load, coalesce harder: wider windows, looser similarity.

        This is a *degradation of resolution*, not of coverage — nothing is
        dropped, the same events are simply described in fewer, coarser items.
        It is the first thing the system gives up when overloaded, well before
        it considers dropping anything.
        """
        self._aggressive = enabled

    def _window(self) -> float:
        w = self.config.window_seconds
        return w * self.config.aggressive_window_multiplier if self._aggressive else w

    def _merge_threshold(self) -> float:
        t = self.config.merge_similarity
        return t - self.config.aggressive_similarity_drop if self._aggressive else t

    def _idle_timeout(self) -> float:
        t = self.config.idle_close_seconds
        return t * self.config.aggressive_window_multiplier if self._aggressive else t

    # -- main API ----------------------------------------------------------

    def offer(
        self,
        event: Event,
        features: EventFeatures,
        vector: List[float],
        now: Optional[float] = None,
    ) -> List[EventCluster]:
        """Feed one event in; get back any clusters that closed as a result."""
        now = now if now is not None else event.timestamp
        self.stats["events_in"] += 1
        emitted = self._close_expired(now)

        key = (event.tenant_id, event.primary_entity)
        open_cluster = self._open.get(key)

        if open_cluster is None:
            self._open[key] = self._new_cluster(event, features, vector)
            emitted.extend(self._close_if_urgent_or_full(key, features, now))
            return emitted

        similarity = cosine(vector, open_cluster.centroid)
        span = event.timestamp - open_cluster.opened_at
        merge_threshold = self._merge_threshold()

        too_old = span > self._window()
        too_big = len(open_cluster.events) >= self.config.max_cluster_size
        clearly_different = similarity < self.config.ambiguous_similarity

        if too_old or too_big or clearly_different:
            reason = (
                "window_expired"
                if too_old
                else "max_size"
                if too_big
                else "topic_changed"
            )
            emitted.append(self._close(key, now, reason))
            self._open[key] = self._new_cluster(event, features, vector)
            emitted.extend(self._close_if_urgent_or_full(key, features, now))
            return emitted

        if similarity < merge_threshold:
            # The ambiguity band. Merge provisionally, but mark the cluster so
            # the LLM is asked whether the merge was right.
            open_cluster.needs_boundary_check = True
            self.stats["boundary_checks"] += 1

        open_cluster.add(event, features, vector)
        emitted.extend(self._close_if_urgent_or_full(key, features, now))
        return emitted

    def _close_if_urgent_or_full(
        self, key: Tuple[str, str], features: EventFeatures, now: float
    ) -> List[EventCluster]:
        """Release a cluster that should not wait out its window.

        Two reasons to close early. A cluster inherits the impatience of its
        most urgent member, so a P0 anywhere in it — including a P0 that opened
        it — goes straight through rather than sitting out the batching window;
        the whole point of coalescing is to save money on things that can wait,
        and a SEV1 cannot. And a cluster at ``max_cluster_size`` can accept
        nothing more, so holding it open buys latency and nothing else (which is
        also what makes ``max_cluster_size=1`` a clean no-coalescing baseline).
        """
        cluster = self._open.get(key)
        if cluster is None:
            return []
        if features.priority == Priority.P0:
            return [self._close(key, now, "p0_member")]
        if len(cluster.events) >= self.config.max_cluster_size:
            return [self._close(key, now, "max_size")]
        return []

    def tick(self, now: Optional[float] = None) -> List[EventCluster]:
        """Close clusters that have gone quiet or are approaching a deadline."""
        return self._close_expired(now if now is not None else time.time())

    def flush(self, now: Optional[float] = None) -> List[EventCluster]:
        """Close everything. Used at end of a replay or on shutdown."""
        now = now if now is not None else time.time()
        return [self._close(key, now, "flush") for key in list(self._open.keys())]

    def open_cluster_count(self) -> int:
        return len(self._open)

    # -- internals ---------------------------------------------------------

    def _new_cluster(
        self, event: Event, features: EventFeatures, vector: List[float]
    ) -> _OpenCluster:
        cluster = _OpenCluster(
            cluster_id=new_id("clu"),
            tenant_id=event.tenant_id,
            entity_id=event.primary_entity,
            opened_at=event.timestamp,
            last_event_at=event.timestamp,
        )
        cluster.add(event, features, vector)
        return cluster

    def _close_expired(self, now: float) -> List[EventCluster]:
        out: List[EventCluster] = []
        idle_timeout = self._idle_timeout()
        window = self._window()
        for key, cluster in list(self._open.items()):
            idle = now - cluster.last_event_at
            span = now - cluster.opened_at
            near_deadline = (
                cluster.deadline() - now <= self.config.deadline_guard_seconds
            )
            if idle >= idle_timeout:
                out.append(self._close(key, now, "idle"))
            elif span >= window:
                out.append(self._close(key, now, "window_expired"))
            elif near_deadline:
                out.append(self._close(key, now, "deadline_guard"))
        return out

    def _close(self, key: Tuple[str, str], now: float, reason: str) -> EventCluster:
        cluster = self._open.pop(key)
        self.stats["clusters_out"] += 1
        self.stats["events_coalesced"] += max(0, len(cluster.events) - 1)
        return EventCluster(
            cluster_id=cluster.cluster_id,
            tenant_id=cluster.tenant_id,
            entity_id=cluster.entity_id,
            events=list(cluster.events),
            features=aggregate_features(cluster.features),
            opened_at=cluster.opened_at,
            closed_at=now,
            needs_boundary_check=cluster.needs_boundary_check,
            close_reason=reason,
        )

    def reset(self) -> None:
        self._open.clear()
        self._aggressive = False
        for k in self.stats:
            self.stats[k] = 0


def describe_cluster(cluster: EventCluster) -> str:
    """A cheap, LLM-free description. This is what the degraded path shows.

    It has to be good enough that losing the LLM entirely is a downgrade in
    polish rather than a loss of information.
    """
    head = cluster.representative
    if cluster.size == 1:
        return head.content
    span = max(1, int(cluster.closed_at - cluster.opened_at))
    return (
        f"{head.content} "
        f"(+{cluster.size - 1} similar {'event' if cluster.size == 2 else 'events'} "
        f"on {cluster.entity_id} over {span}s)"
    )
