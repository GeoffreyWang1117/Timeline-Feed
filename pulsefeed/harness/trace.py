"""Synthetic event traces with ground-truth importance labels.

Honesty note, because the experiment is only worth as much as this file:

* Labels come from *construction*, not from scoring. The generator plants
  incident storylines and marks the events it planted; nothing in PulseFeed
  ever reads ``ground_truth_important``, and the mock provider never sees it.
* Noise is generated from the same vocabulary distributions as real feeds tend
  to have — mostly telemetry, some routine repo activity, some chatter — so a
  system can't win by exploiting an artificially clean separation.
* Every trace is seeded, so a reported number can be reproduced exactly.

The traces are synthetic, which bounds what the results can claim: they measure
the mechanism (does admission control preserve recall while cutting calls?) on
traffic whose shape we chose. Swapping in a captured GitHub/Slack export is a
matter of writing a loader that produces the same ``Event`` objects.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

from pulsefeed.models import Event

SERVICES = [
    "checkout-service",
    "payments-api",
    "search-service",
    "auth-service",
    "notification-worker",
]
DATABASES = ["db-primary", "db-replica-a", "cache-cluster"]
REPOS = ["web-frontend", "billing-core", "infra-terraform"]
PEOPLE = ["alice", "bob", "carol", "dan", "erin"]

CHATTER = [
    "anyone up for lunch?",
    "nice work on the demo yesterday",
    "the offsite doc is in the drive folder",
    "reminder: standup moved to 10:15",
    "who owns the staging environment rotation this week?",
    "coffee machine on floor 3 is fixed",
    "sharing a good article on distributed tracing",
    "welcome to the team!",
    "I'll be out friday afternoon",
    "can someone review my draft PR when free",
]

ROUTINE_GITHUB = [
    "opened PR #{n}: update dependency lockfile",
    "merged PR #{n}: refactor helper utilities",
    "commented on issue #{n}: agreed, will take a look",
    "closed issue #{n} as completed",
    "pushed 3 commits to feature/{repo}-cleanup",
    "opened PR #{n}: add unit tests for {repo}",
]

ROUTINE_CI = [
    "build #{n} passed on main",
    "test suite completed in {n}s, all green",
    "nightly job finished successfully",
    "lint check passed for PR #{n}",
]


@dataclass
class TraceConfig:
    duration_seconds: float = 900.0
    base_events_per_second: float = 2.0
    burst_multiplier: float = 25.0
    # Bursts are expressed as fractions of the trace so a shorter run still
    # exercises overload rather than silently skipping it.
    burst_fractions: List[Tuple[float, float]] = field(
        default_factory=lambda: [(0.34, 0.40), (0.70, 0.77)]
    )
    burst_windows: Optional[List[Tuple[float, float]]] = None
    incident_count: int = 3
    duplicate_storm_count: int = 4
    duplicate_storm_length: int = 30
    security_event_count: int = 2
    mention_count: int = 12
    tenant_id: str = "acme"
    start_timestamp: float = 1_700_000_000.0
    seed: int = 20260815


class TraceGenerator:
    def __init__(self, config: Optional[TraceConfig] = None) -> None:
        self.config = config or TraceConfig()
        self._rng = random.Random(self.config.seed)
        self._counter = 0

    # -- helpers -----------------------------------------------------------

    def _event(
        self,
        offset: float,
        source: str,
        content: str,
        entity: str,
        *,
        important: bool = False,
        actor: Optional[str] = None,
        storyline: Optional[str] = None,
        extra_metadata: Optional[Dict[str, object]] = None,
    ) -> Event:
        self._counter += 1
        metadata: Dict[str, object] = {
            "ground_truth_important": important,
            "seq": self._counter,
        }
        if storyline:
            metadata["storyline"] = storyline
        if extra_metadata:
            metadata.update(extra_metadata)
        return Event(
            source=source,
            content=content,
            tenant_id=self.config.tenant_id,
            timestamp=self.config.start_timestamp + offset,
            actor=actor,
            entity_ids=[entity],
            metadata=metadata,
        )

    def burst_windows(self) -> List[Tuple[float, float]]:
        if self.config.burst_windows is not None:
            return self.config.burst_windows
        duration = self.config.duration_seconds
        return [(a * duration, b * duration) for a, b in self.config.burst_fractions]

    def _in_burst(self, offset: float) -> bool:
        return any(a <= offset <= b for a, b in self.burst_windows())

    # -- generators --------------------------------------------------------

    def _noise(self) -> List[Event]:
        """Background traffic. None of it is important by construction."""
        events: List[Event] = []
        rng = self._rng
        offset = 0.0
        duration = self.config.duration_seconds

        while offset < duration:
            rate = self.config.base_events_per_second
            if self._in_burst(offset):
                rate *= self.config.burst_multiplier
            offset += rng.expovariate(rate)
            if offset >= duration:
                break

            roll = rng.random()
            if roll < 0.55:
                service = rng.choice(SERVICES + DATABASES)
                metric = rng.choice(["cpu", "memory", "disk io", "connections"])
                value = rng.randint(20, 70)
                events.append(
                    self._event(
                        offset,
                        "telemetry",
                        f"{service} {metric} at {value}%",
                        service,
                    )
                )
            elif roll < 0.75:
                repo = rng.choice(REPOS)
                template = rng.choice(ROUTINE_GITHUB)
                events.append(
                    self._event(
                        offset,
                        "github",
                        template.format(n=rng.randint(100, 999), repo=repo),
                        repo,
                        actor=rng.choice(PEOPLE),
                    )
                )
            elif roll < 0.87:
                events.append(
                    self._event(
                        offset,
                        "slack",
                        rng.choice(CHATTER),
                        "general",
                        actor=rng.choice(PEOPLE),
                    )
                )
            elif roll < 0.96:
                repo = rng.choice(REPOS)
                events.append(
                    self._event(
                        offset,
                        "ci",
                        rng.choice(ROUTINE_CI).format(n=rng.randint(10, 900)),
                        repo,
                    )
                )
            else:
                events.append(
                    self._event(
                        offset,
                        "rss",
                        f"industry blog post #{rng.randint(1, 400)} on platform engineering",
                        "news",
                    )
                )
        return events

    def _incident(self, start: float, index: int) -> List[Event]:
        """A correlated storyline. Every event in it is ground-truth important.

        This is the thing a feed exists to surface: seven events that are one
        story, spread across four sources, arriving amid hundreds of unrelated
        ones.
        """
        rng = self._rng
        service = rng.choice(SERVICES)
        database = rng.choice(DATABASES)
        deploy_id = rng.randint(700, 999)
        engineer = rng.choice(PEOPLE)
        storyline = f"incident-{index}"

        def at(delta: float) -> float:
            return start + delta

        steps = [
            (0.0, "grafana", f"{service} p95 latency rises to 700ms", service),
            (
                55.0,
                "slack",
                f"seeing DB timeout errors from {service}, anyone else?",
                service,
            ),
            (
                110.0,
                "grafana",
                f"{database} connection pool saturation, 98% utilised",
                database,
            ),
            (
                200.0,
                "github",
                f"deployment #{deploy_id} completed for {service}",
                service,
            ),
            (255.0, "grafana", f"{service} p95 latency rises to 1.5s", service),
            (
                320.0,
                "pagerduty",
                f"SEV1: {service} error rate above threshold, paging on-call",
                service,
            ),
            (
                375.0,
                "slack",
                f"starting rollback of deployment #{deploy_id}",
                service,
            ),
            (
                480.0,
                "grafana",
                f"{service} latency returned to normal, incident resolved",
                service,
            ),
        ]

        return [
            self._event(
                at(delta),
                source,
                content,
                entity,
                important=True,
                actor=engineer if source == "slack" else None,
                storyline=storyline,
            )
            for delta, source, content, entity in steps
        ]

    def _duplicate_storm(self, start: float) -> List[Event]:
        """The coalescing test: one fact repeated many times.

        Not important individually — but a system that spends one LLM call per
        sample here has no budget left for anything that is.
        """
        rng = self._rng
        service = rng.choice(SERVICES + DATABASES)
        base = rng.randint(88, 96)
        return [
            self._event(
                start + i * rng.uniform(1.5, 4.0),
                "telemetry",
                f"{service} cpu usage {base + rng.randint(0, 3)}%",
                service,
            )
            for i in range(self.config.duplicate_storm_length)
        ]

    def _security_events(self) -> List[Event]:
        rng = self._rng
        out = []
        for _ in range(self.config.security_event_count):
            offset = rng.uniform(60, self.config.duration_seconds - 60)
            service = rng.choice(SERVICES)
            out.append(
                self._event(
                    offset,
                    "alertmanager",
                    f"possible credential leak detected in {service} logs, "
                    "rotating keys",
                    service,
                    important=True,
                    storyline="security",
                )
            )
        return out

    def _mentions(self) -> List[Event]:
        """Direct mentions of the watched user: important, but not incidents."""
        rng = self._rng
        out = []
        for _ in range(self.config.mention_count):
            offset = rng.uniform(30, self.config.duration_seconds - 30)
            out.append(
                self._event(
                    offset,
                    "slack",
                    f"@alice can you review the {rng.choice(REPOS)} change before EOD?",
                    "general",
                    important=True,
                    actor=rng.choice([p for p in PEOPLE if p != "alice"]),
                    storyline="mention",
                )
            )
        return out

    # -- entry point -------------------------------------------------------

    def generate(self) -> List[Event]:
        rng = self._rng
        events = self._noise()

        span = self.config.duration_seconds
        for i in range(self.config.incident_count):
            # Spread incidents out, keeping each fully inside the trace window.
            start = span * (i + 0.5) / (self.config.incident_count + 0.6)
            events.extend(self._incident(start, i))

        for _ in range(self.config.duplicate_storm_count):
            events.extend(self._duplicate_storm(rng.uniform(30, span - 150)))

        events.extend(self._security_events())
        events.extend(self._mentions())
        events.sort(key=lambda e: (e.timestamp, e.metadata.get("seq", 0)))
        return events


def important_event_ids(events: Sequence[Event]) -> set:
    return {
        e.event_id for e in events if e.metadata.get("ground_truth_important")
    }


def trace_summary(events: Sequence[Event]) -> Dict[str, object]:
    by_source: Dict[str, int] = {}
    for e in events:
        by_source[e.source] = by_source.get(e.source, 0) + 1
    important = important_event_ids(events)
    span = (events[-1].timestamp - events[0].timestamp) if events else 0.0
    return {
        "events": len(events),
        "important_events": len(important),
        "span_seconds": round(span, 1),
        "events_per_second": round(len(events) / span, 2) if span else 0.0,
        "by_source": dict(sorted(by_source.items(), key=lambda kv: -kv[1])),
    }


def generate_trace(config: Optional[TraceConfig] = None) -> List[Event]:
    return TraceGenerator(config).generate()
