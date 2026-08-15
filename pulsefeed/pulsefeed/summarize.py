"""Hierarchical summarisation with supersede semantics.

Never one giant prompt over a thousand events. Instead a tree:

    raw event -> micro -> cluster -> episode -> hourly -> daily

Each level summarises the level below, so a daily digest costs a handful of
calls over already-compressed text rather than one enormous call over raw logs,
and every node keeps ``source_event_ids`` so any line in the digest expands
back to the facts it came from.

The other half of this module is what happens when the system was *wrong*. At
12:05 it believed the cause was the database; at 12:25 it turns out to be DNS.
The old summary is not overwritten — it is marked SUPERSEDED and linked to its
replacement. What the system believed, and when, stays on the record.
"""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence

from .models import (
    SemanticAnnotation,
    Severity,
    Summary,
    SummaryLevel,
    SummaryStatus,
    dedupe_preserving_order,
    new_id,
)

# Which level rolls up into which.
PARENT_LEVEL: Dict[SummaryLevel, Optional[SummaryLevel]] = {
    SummaryLevel.MICRO: SummaryLevel.CLUSTER,
    SummaryLevel.CLUSTER: SummaryLevel.EPISODE,
    SummaryLevel.EPISODE: SummaryLevel.HOURLY,
    SummaryLevel.HOURLY: SummaryLevel.DAILY,
    SummaryLevel.DAILY: None,
}


class SummaryStore:
    """Append-only store of summaries with explicit supersede links."""

    def __init__(self) -> None:
        self._by_id: Dict[str, Summary] = {}
        self._by_level: Dict[SummaryLevel, List[str]] = defaultdict(list)
        self._by_entity: Dict[tuple, List[str]] = defaultdict(list)

    def add(self, summary: Summary) -> Summary:
        self._by_id[summary.summary_id] = summary
        self._by_level[summary.level].append(summary.summary_id)
        for entity_id in summary.entity_ids:
            self._by_entity[(summary.tenant_id, entity_id)].append(summary.summary_id)
        return summary

    def get(self, summary_id: str) -> Optional[Summary]:
        return self._by_id.get(summary_id)

    def active(
        self, level: Optional[SummaryLevel] = None, tenant_id: Optional[str] = None
    ) -> List[Summary]:
        ids = (
            self._by_level[level]
            if level is not None
            else [sid for lvl in self._by_level for sid in self._by_level[lvl]]
        )
        out = []
        for sid in ids:
            s = self._by_id[sid]
            if s.status is not SummaryStatus.ACTIVE:
                continue
            if tenant_id is not None and s.tenant_id != tenant_id:
                continue
            out.append(s)
        return out

    def history_for_entity(self, tenant_id: str, entity_id: str) -> List[Summary]:
        """Every belief ever held about this entity, superseded ones included."""
        return [self._by_id[sid] for sid in self._by_entity[(tenant_id, entity_id)]]

    def supersede(self, old_summary_id: str, new_summary: Summary) -> Summary:
        """Replace a conclusion while keeping the one it replaced.

        The new summary inherits the old one's source events as well as its own:
        a corrected root cause still explains the events that motivated the
        original, wrong, conclusion.
        """
        old = self._by_id.get(old_summary_id)
        if old is None:
            return self.add(new_summary)

        old.status = SummaryStatus.SUPERSEDED
        old.superseded_by = new_summary.summary_id
        new_summary.supersedes = old.summary_id
        new_summary.version = old.version + 1
        new_summary.source_event_ids = dedupe_preserving_order(
            list(old.source_event_ids) + list(new_summary.source_event_ids)
        )
        return self.add(new_summary)

    def __len__(self) -> int:
        return len(self._by_id)

    def reset(self) -> None:
        self._by_id.clear()
        self._by_level.clear()
        self._by_entity.clear()


def summary_from_annotation(
    annotation: SemanticAnnotation, level: SummaryLevel = SummaryLevel.CLUSTER
) -> Summary:
    return Summary(
        summary_id=new_id("sum"),
        tenant_id=annotation.tenant_id,
        level=level,
        text=annotation.summary,
        source_event_ids=list(annotation.source_event_ids),
        entity_ids=list(annotation.entities),
        severity=annotation.severity,
        confidence=annotation.confidence,
        model=annotation.model,
        generated_at=annotation.generated_at,
    )


@dataclass
class RollupConfig:
    episode_window_seconds: float = 1800.0
    min_children_for_rollup: int = 2
    max_children_per_rollup: int = 20


class Rollup:
    """Builds higher-level summaries out of lower-level ones.

    The text-generation step is pluggable: pass an async ``generate`` callable
    to use an LLM, or leave it out and get a deterministic extractive rollup.
    The extractive path is not a placeholder — it is the degraded mode that
    keeps digests working when the provider is down.
    """

    def __init__(
        self,
        store: SummaryStore,
        config: Optional[RollupConfig] = None,
    ) -> None:
        self.store = store
        self.config = config or RollupConfig()

    def group_for_episodes(
        self, summaries: Sequence[Summary]
    ) -> List[List[Summary]]:
        """Group cluster summaries into episodes by entity and time proximity."""
        by_entity: Dict[tuple, List[Summary]] = defaultdict(list)
        for s in summaries:
            key = (s.tenant_id, s.entity_ids[0] if s.entity_ids else "__none__")
            by_entity[key].append(s)

        groups: List[List[Summary]] = []
        for items in by_entity.values():
            items.sort(key=lambda s: s.generated_at)
            current: List[Summary] = []
            for s in items:
                if not current:
                    current = [s]
                    continue
                if (
                    s.generated_at - current[0].generated_at
                    <= self.config.episode_window_seconds
                    and len(current) < self.config.max_children_per_rollup
                ):
                    current.append(s)
                else:
                    groups.append(current)
                    current = [s]
            if current:
                groups.append(current)
        return [g for g in groups if len(g) >= self.config.min_children_for_rollup]

    def extractive_rollup(
        self, children: Sequence[Summary], level: SummaryLevel
    ) -> Summary:
        """Deterministic, LLM-free rollup. Always available."""
        if not children:
            raise ValueError("cannot roll up zero summaries")
        tenant_id = children[0].tenant_id
        severity = max((c.severity for c in children), key=lambda s: s.rank)
        entity_ids = dedupe_preserving_order(
            [e for c in children for e in c.entity_ids]
        )
        source_event_ids = dedupe_preserving_order(
            [e for c in children for e in c.source_event_ids]
        )
        # Lead with the most severe child, then the most recent, which is how a
        # human skims an incident channel.
        ordered = sorted(
            children, key=lambda c: (-c.severity.rank, -c.generated_at)
        )
        head = ordered[0].text
        extra = len(children) - 1
        text = head if extra == 0 else f"{head} (+{extra} related updates)"

        return Summary(
            summary_id=new_id("sum"),
            tenant_id=tenant_id,
            level=level,
            text=text,
            source_event_ids=source_event_ids,
            entity_ids=entity_ids,
            child_summary_ids=[c.summary_id for c in children],
            severity=severity,
            confidence=min(c.confidence for c in children) if children else 0.0,
            model="extractive",
            generated_at=max(c.generated_at for c in children),
        )

    async def rollup(
        self,
        children: Sequence[Summary],
        level: SummaryLevel,
        generate=None,
    ) -> Summary:
        """Roll up, using ``generate`` when available and falling back cleanly."""
        summary = self.extractive_rollup(children, level)
        if generate is None:
            return self.store.add(summary)
        try:
            text = await generate(children, level)
            if text:
                summary.text = text
                summary.model = "llm"
        except Exception:
            # A failed rollup degrades to the extractive text rather than
            # losing the digest entirely.
            summary.model = "extractive-fallback"
        return self.store.add(summary)

    def build_episodes(self, tenant_id: Optional[str] = None) -> List[Summary]:
        clusters = self.store.active(SummaryLevel.CLUSTER, tenant_id)
        episodes = []
        for group in self.group_for_episodes(clusters):
            episodes.append(
                self.store.add(self.extractive_rollup(group, SummaryLevel.EPISODE))
            )
        return episodes

    def build_digest(
        self, level: SummaryLevel = SummaryLevel.DAILY, tenant_id: Optional[str] = None
    ) -> Optional[Summary]:
        child_level = SummaryLevel.EPISODE if level is SummaryLevel.DAILY else SummaryLevel.CLUSTER
        children = self.store.active(child_level, tenant_id)
        if not children:
            children = self.store.active(SummaryLevel.CLUSTER, tenant_id)
        if not children:
            return None
        children = sorted(children, key=lambda c: -c.severity.rank)[
            : self.config.max_children_per_rollup
        ]
        return self.store.add(self.extractive_rollup(children, level))


def detect_contradiction(old: Summary, new_annotation: SemanticAnnotation) -> bool:
    """Cheap heuristic for "the new evidence overturns the old conclusion".

    Two triggers: the new annotation explicitly claims to correct something, or
    it covers the same events with a materially different severity. Cheap and
    conservative on purpose — a missed supersede leaves a stale summary visible,
    which is bad, but a false supersede destroys a correct one, which is worse.
    """
    if old.summary_id in (new_annotation.causal_links or []):
        return True
    overlap = set(old.source_event_ids) & set(new_annotation.source_event_ids)
    if not overlap:
        return False
    return abs(old.severity.rank - new_annotation.severity.rank) >= 2
