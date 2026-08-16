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
    # Purely grouping parameters. They used to double as latency parameters —
    # a cluster reached the feed only when it closed, so a wider window meant a
    # slower feed — which made every value here a compromise between grouping
    # quality and freshness. Provisional publication on open (see
    # ``drain_touched``) removed that coupling, so these can be set for grouping
    # alone.
    window_seconds: float = 90.0        # max span of one cluster
    idle_close_seconds: float = 20.0    # quiet period that closes a cluster
    max_cluster_size: int = 25
    merge_similarity: float = 0.80      # at or above: confidently the same thing
    ambiguous_similarity: float = 0.58  # below: confidently different
    deadline_guard_seconds: float = 5.0 # close early rather than miss freshness
    aggressive_window_multiplier: float = 4.0
    aggressive_similarity_drop: float = 0.18
    # Concurrent conversations tracked per entity. A busy channel carries
    # several unrelated threads at once; one open cluster per entity means each
    # new topic evicts the last and nothing ever groups.
    max_open_per_entity: int = 6


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
                (c * n + v) / (n + 1) for c, v in zip(self.centroid, vector, strict=True)
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
        self._open: Dict[Tuple[str, str], List[_OpenCluster]] = {}
        self._opened: List[_OpenCluster] = []
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
        """Feed one event in; get back any clusters that closed as a result.

        A new event is matched against *all* clusters currently open for its
        entity, not just the most recent one. That distinction turned out to
        matter a lot: an entity like a busy Slack channel carries many unrelated
        conversations at once, and a single-open-cluster design closed the
        current cluster as ``topic_changed`` on almost every event, so nothing
        ever coalesced and the feed filled with near-duplicate one-line items.
        Several concurrent clusters per entity let interleaved conversations
        each accumulate.
        """
        now = now if now is not None else event.timestamp
        self.stats["events_in"] += 1
        emitted = self._close_expired(now)

        key = (event.tenant_id, event.primary_entity)
        candidates = self._open.setdefault(key, [])
        merge_threshold = self._merge_threshold()
        window = self._window()

        best: Optional[_OpenCluster] = None
        best_similarity = -1.0
        for cluster in candidates:
            if event.timestamp - cluster.opened_at > window:
                continue
            if len(cluster.events) >= self.config.max_cluster_size:
                continue
            similarity = cosine(vector, cluster.centroid)
            if similarity > best_similarity:
                best, best_similarity = cluster, similarity

        if best is None or best_similarity < self.config.ambiguous_similarity:
            # Nothing open is plausibly about the same thing. Open a new cluster,
            # evicting the stalest if this entity is already at its limit.
            if len(candidates) >= self.config.max_open_per_entity:
                oldest = min(candidates, key=lambda c: c.last_event_at)
                emitted.append(self._close_cluster(key, oldest, now, "max_open"))
            cluster = self._new_cluster(event, features, vector)
            self._open.setdefault(key, []).append(cluster)
            self._touch(cluster)
        else:
            if best_similarity < merge_threshold:
                # The ambiguity band. Merge provisionally, but mark the cluster
                # so the LLM is asked whether the merge was right.
                best.needs_boundary_check = True
                self.stats["boundary_checks"] += 1
            best.add(event, features, vector)
            cluster = best
            self._touch(cluster)

        emitted.extend(self._close_if_urgent_or_full(key, cluster, features, now))
        return emitted

    def _close_if_urgent_or_full(
        self,
        key: Tuple[str, str],
        cluster: _OpenCluster,
        features: EventFeatures,
        now: float,
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
        if cluster not in self._open.get(key, []):
            return []
        if features.priority == Priority.P0:
            return [self._close_cluster(key, cluster, now, "p0_member")]
        if len(cluster.events) >= self.config.max_cluster_size:
            return [self._close_cluster(key, cluster, now, "max_size")]
        return []

    def tick(self, now: Optional[float] = None) -> List[EventCluster]:
        """Close clusters that have gone quiet or are approaching a deadline."""
        return self._close_expired(now if now is not None else time.time())

    def flush(self, now: Optional[float] = None) -> List[EventCluster]:
        """Close everything. Used at end of a replay or on shutdown."""
        now = now if now is not None else time.time()
        out: List[EventCluster] = []
        for key in list(self._open.keys()):
            for cluster in list(self._open.get(key, ())):
                out.append(self._close_cluster(key, cluster, now, "flush"))
        return out

    def _touch(self, cluster: "_OpenCluster") -> None:
        if cluster not in self._opened:
            self._opened.append(cluster)

    def drain_touched(self, now: Optional[float] = None) -> List[EventCluster]:
        """Provisional snapshots of clusters that opened *or grew* since the
        last call.

        This is what breaks the tie between grouping quality and freshness. A
        cluster used to reach the feed only when it closed, so every second of
        coalescing window was a second of publication delay — widening the
        window to group better pushed p95 time-to-feed from 18s to 45s, and
        narrowing it again gave the grouping back.

        Publishing provisionally and refreshing the same row in place decouples
        the two: the window becomes purely a grouping parameter, and an event is
        visible as soon as it is ingested. Note this has to cover *growth*, not
        just opening — an earlier version only published on open, which left
        every event that joined an existing cluster waiting for the close and
        barely moved p95.
        """
        now = now if now is not None else time.time()
        snapshots = [self._snapshot(c, now, "provisional") for c in self._opened]
        self._opened.clear()
        return snapshots

    def open_cluster_count(self) -> int:
        return sum(len(clusters) for clusters in self._open.values())

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
        for key in list(self._open.keys()):
            # _close_cluster removes the key once its last cluster goes, so this
            # must re-read rather than hold a reference across the loop.
            for cluster in list(self._open.get(key, ())):
                idle = now - cluster.last_event_at
                span = now - cluster.opened_at
                near_deadline = (
                    cluster.deadline() - now <= self.config.deadline_guard_seconds
                )
                if idle >= idle_timeout:
                    out.append(self._close_cluster(key, cluster, now, "idle"))
                elif span >= window:
                    out.append(self._close_cluster(key, cluster, now, "window_expired"))
                elif near_deadline:
                    out.append(self._close_cluster(key, cluster, now, "deadline_guard"))
        return out

    def _close_cluster(
        self,
        key: Tuple[str, str],
        cluster: "_OpenCluster",
        now: float,
        reason: str,
    ) -> EventCluster:
        clusters = self._open.get(key, [])
        if cluster in clusters:
            clusters.remove(cluster)
        if key in self._open and not clusters:
            del self._open[key]
        self.stats["clusters_out"] += 1
        self.stats["events_coalesced"] += max(0, len(cluster.events) - 1)
        return self._snapshot(cluster, now, reason)

    def _snapshot(
        self, cluster: "_OpenCluster", now: float, reason: str
    ) -> EventCluster:
        """Immutable view of a cluster's members as of now.

        Used both for closing and for provisional publication, so a provisional
        item and its final version are the same shape and share a cluster_id —
        which is what lets the feed update the row in place instead of
        publishing the same happening twice.
        """
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
        self._opened.clear()
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
