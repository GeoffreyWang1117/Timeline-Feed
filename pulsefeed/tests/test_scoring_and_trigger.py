"""Tests for the cheap scorer and the trigger policies."""

from __future__ import annotations

import pytest

from pulsefeed.budget import BudgetLedger, TenantPlan
from pulsefeed.models import Event, EventFeatures, Priority
from pulsefeed.scoring import CheapScorer, TenantAffinity, aggregate_features
from pulsefeed.trigger import (
    BudgetAwarePolicy,
    CompositePolicy,
    DeadlineAwarePolicy,
    FixedThresholdPolicy,
    LoadAwarePolicy,
    TriggerContext,
    UtilityWeights,
    compute_utility,
)

T0 = 1_700_000_000.0


def ev(source: str, content: str, entity: str = "svc", **kw) -> Event:
    return Event(
        source=source,
        content=content,
        tenant_id="t",
        entity_ids=[entity],
        timestamp=kw.pop("timestamp", T0),
        **kw,
    )


class TestCheapScorer:
    def test_incident_outranks_chatter(self):
        scorer = CheapScorer()
        incident = scorer.score(
            ev("pagerduty", "SEV1 checkout outage, connection pool exhausted"), now=T0
        )
        chatter = scorer.score(ev("slack", "anyone up for lunch?", "general"), now=T0)
        assert incident.importance > chatter.importance
        assert incident.risk > chatter.risk
        assert incident.priority == Priority.P0

    def test_telemetry_is_p3(self):
        scorer = CheapScorer()
        f = scorer.score(ev("telemetry", "db-primary cpu at 41%", "db-primary"), now=T0)
        assert f.priority == Priority.P3

    def test_repeated_telemetry_loses_novelty(self):
        scorer = CheapScorer()
        first = scorer.score(ev("telemetry", "db cpu usage 94%", "db"), now=T0)
        second = scorer.score(ev("telemetry", "db cpu usage 95%", "db"), now=T0 + 5)
        assert second.novelty < first.novelty
        assert second.duplication_score > first.duplication_score
        assert second.importance < first.importance

    def test_number_bucketing_makes_near_duplicates_similar(self):
        """94% and 96% must not read as two different facts."""
        scorer = CheapScorer()
        scorer.score(ev("telemetry", "checkout cpu usage 94%"), now=T0)
        near = scorer.score(ev("telemetry", "checkout cpu usage 96%"), now=T0 + 3)
        assert near.duplication_score > 0.9

    def test_watched_actor_raises_relevance_and_priority(self):
        scorer = CheapScorer()
        scorer.set_affinity("t", TenantAffinity(watched_actors={"alice"}))
        mention = scorer.score(
            ev("slack", "@alice can you review this?", "general"), now=T0
        )
        assert mention.user_relevance >= 0.5
        assert mention.priority == Priority.P1

    def test_burst_score_saturates(self):
        scorer = CheapScorer()
        last = None
        for i in range(12):
            last = scorer.score(
                ev("telemetry", f"svc metric sample {i}", "svc"), now=T0 + i
            )
        assert last is not None
        assert last.burst_score == pytest.approx(1.0)

    def test_priority_override_is_respected(self):
        scorer = CheapScorer()
        f = scorer.score(
            ev("rss", "a boring post", "news", metadata={"priority_override": 0}),
            now=T0,
        )
        assert f.priority == Priority.P0

    def test_scores_are_bounded(self):
        scorer = CheapScorer()
        for source in ("pagerduty", "telemetry", "slack", "unknown-source"):
            f = scorer.score(ev(source, "critical outage " * 50), now=T0)
            for value in (f.importance, f.novelty, f.uncertainty, f.risk):
                assert 0.0 <= value <= 1.0

    def test_aggregate_takes_worst_case(self):
        a = EventFeatures("a", importance=0.2, risk=0.1, priority=Priority.P3,
                          estimated_llm_tokens=100)
        b = EventFeatures("b", importance=0.9, risk=0.8, priority=Priority.P1,
                          estimated_llm_tokens=150)
        agg = aggregate_features([a, b])
        assert agg.importance == pytest.approx(0.9)
        assert agg.risk == pytest.approx(0.8)
        assert agg.priority == Priority.P1  # most important member wins
        assert agg.estimated_llm_tokens == 250


class TestUtility:
    def test_cost_reduces_utility(self):
        w = UtilityWeights()
        cheap = EventFeatures("a", importance=0.8, estimated_llm_tokens=200)
        pricey = EventFeatures("b", importance=0.8, estimated_llm_tokens=20_000)
        assert compute_utility(cheap, w) > compute_utility(pricey, w)

    def test_uncertainty_contributes(self):
        w = UtilityWeights()
        certain = EventFeatures("a", importance=0.5, uncertainty=0.0)
        unsure = EventFeatures("b", importance=0.5, uncertainty=1.0)
        assert compute_utility(unsure, w) > compute_utility(certain, w)


class TestPolicies:
    def ctx(self, **kw) -> TriggerContext:
        kw.setdefault("now", T0)
        kw.setdefault("tenant_id", "t")
        return TriggerContext(**kw)

    def test_fixed_threshold_ignores_load(self):
        policy = FixedThresholdPolicy(theta=0.5)
        f = EventFeatures("a", importance=0.9)
        idle = policy.threshold(f, self.ctx(queue_depth=0, queue_capacity=100))
        busy = policy.threshold(f, self.ctx(queue_depth=99, queue_capacity=100))
        assert idle == busy == 0.5

    def test_budget_policy_raises_threshold_as_budget_drains(self):
        ledger = BudgetLedger()
        ledger.register(TenantPlan("t", daily_token_budget=1000, daily_usd_budget=100.0))
        policy = BudgetAwarePolicy(ledger)
        f = EventFeatures("a")

        full = policy.threshold(f, self.ctx())
        ledger.charge("t", 900, 0.0, T0)
        nearly_empty = policy.threshold(f, self.ctx())

        assert full == pytest.approx(0.40, abs=0.01)
        assert nearly_empty > 0.80
        assert nearly_empty > full

    def test_load_policy_is_convex_in_load(self):
        policy = LoadAwarePolicy()
        f = EventFeatures("a")
        half = policy.threshold(f, self.ctx(queue_depth=50, queue_capacity=100))
        full = policy.threshold(f, self.ctx(queue_depth=100, queue_capacity=100))
        idle = policy.threshold(f, self.ctx(queue_depth=0, queue_capacity=100))
        # Quadratic: a half-full queue costs much less than half the penalty.
        assert half - idle < (full - idle) / 2
        assert full > half > idle

    def test_deadline_policy_vetoes_stale_work(self):
        policy = DeadlineAwarePolicy()
        f = EventFeatures("a", importance=1.0, risk=1.0)
        decision = policy.decide(f, self.ctx(deadline_at=T0 - 1))
        assert not decision.invoke
        assert decision.veto
        assert "stale" in decision.reason

    def test_deadline_policy_vetoes_work_that_would_land_late(self):
        policy = DeadlineAwarePolicy()
        f = EventFeatures("a", importance=1.0, risk=1.0)
        decision = policy.decide(
            f, self.ctx(deadline_at=T0 + 10, estimated_wait_seconds=30)
        )
        assert not decision.invoke
        assert decision.veto

    def test_deadline_policy_allows_work_that_fits(self):
        policy = DeadlineAwarePolicy()
        f = EventFeatures("a", importance=1.0, risk=1.0)
        decision = policy.decide(
            f, self.ctx(deadline_at=T0 + 100, estimated_wait_seconds=2)
        )
        assert decision.invoke


class TestCompositePolicy:
    def ctx(self, **kw) -> TriggerContext:
        kw.setdefault("now", T0)
        kw.setdefault("tenant_id", "t")
        return TriggerContext(**kw)

    def make(self, **kw) -> CompositePolicy:
        ledger = BudgetLedger()
        ledger.register(TenantPlan("t"))
        kw.setdefault("audit_sample_rate", 0.0)
        return CompositePolicy(
            policies=[BudgetAwarePolicy(ledger), LoadAwarePolicy(), DeadlineAwarePolicy()],
            **kw,
        )

    def test_safety_rule_bypasses_every_threshold(self):
        policy = self.make()
        event = ev("pagerduty", "SEV1 outage")
        # Deliberately terrible utility; P0 must still get through.
        features = EventFeatures("a", importance=0.0, priority=Priority.P0)
        decision = policy.decide_event(
            event, features, self.ctx(queue_depth=999, queue_capacity=1000)
        )
        assert decision.invoke
        assert decision.bypass
        assert decision.reason.startswith("safety:")

    def test_deadline_veto_beats_high_utility(self):
        policy = self.make()
        features = EventFeatures("a", importance=1.0, risk=0.9, priority=Priority.P2)
        decision = policy.decide_event(
            ev("grafana", "latency spike"), features, self.ctx(deadline_at=T0 - 5)
        )
        assert not decision.invoke
        assert decision.veto

    def test_low_utility_below_threshold_is_rejected(self):
        policy = self.make()
        features = EventFeatures("a", importance=0.05, priority=Priority.P3)
        decision = policy.decide_event(
            ev("telemetry", "cpu 12%"), features, self.ctx()
        )
        assert not decision.invoke

    def test_composite_takes_the_strictest_threshold(self):
        """A sub-policy saying 'no' must not by itself veto — only the max
        threshold binds. This is the bug the veto flag exists to prevent."""
        policy = self.make()
        features = EventFeatures("a", importance=0.95, risk=0.9, priority=Priority.P2)
        decision = policy.decide_event(
            ev("grafana", "checkout latency 1.5s"),
            features,
            self.ctx(deadline_at=T0 + 600, estimated_wait_seconds=1),
        )
        assert decision.invoke, decision.reason

    def test_boundary_check_gets_a_discount(self):
        policy = self.make(boundary_check_discount=0.5)
        # Utility lands between 0.5*theta and theta: rejected normally, admitted
        # when the cheap layer admits it could not resolve the grouping.
        features = EventFeatures("a", importance=0.9, priority=Priority.P2)
        plain = policy.decide_event(ev("slack", "hmm"), features, self.ctx())
        ambiguous = policy.decide_event(
            ev("slack", "hmm"), features, self.ctx(needs_boundary_check=True)
        )
        assert not plain.invoke
        assert ambiguous.invoke
        assert ambiguous.reason == "boundary_check"

    def test_audit_sampling_admits_some_rejected_events(self):
        policy = self.make(audit_sample_rate=1.0)  # sample everything
        features = EventFeatures("a", importance=0.0, priority=Priority.P3)
        decision = policy.decide_event(ev("telemetry", "cpu 3%"), features, self.ctx())
        assert decision.invoke
        assert decision.audit
        assert decision.reason == "audit_sample"

    def test_provider_down_stops_admitting_non_bypass_work(self):
        policy = self.make()
        features = EventFeatures("a", importance=0.9, risk=0.8, priority=Priority.P2)
        decision = policy.decide_event(
            ev("grafana", "latency"), features, self.ctx(provider_healthy=False)
        )
        assert not decision.invoke
        assert decision.reason == "provider:circuit_open"
