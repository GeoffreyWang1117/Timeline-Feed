"""Stage two: event-triggered LLM invocation.

This is the research core. Every policy here answers one question — *is this
particular event worth one unit of a scarce, slow, failure-prone resource right
now* — and they differ only in what "right now" is allowed to depend on:

    FixedThreshold      nothing (baseline)
    BudgetAware         how much of today's allowance is left
    LoadAware           how deep the LLM queue is
    DeadlineAware       whether the answer would still be fresh on arrival

``CompositePolicy`` stacks them and adds the two things that make the whole
scheme safe to run: hard safety rules that bypass every threshold, and random
audit sampling of *rejected* events so the false-negative rate is measured
rather than assumed.
"""

from __future__ import annotations

import random
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, List, Optional, Sequence

from .budget import BudgetLedger
from .models import Event, EventFeatures, Priority


@dataclass
class TriggerContext:
    """Everything a policy may look at besides the event itself."""

    now: float
    queue_depth: int = 0
    queue_capacity: int = 1
    estimated_wait_seconds: float = 0.0
    inflight: int = 0
    provider_healthy: bool = True
    tenant_id: str = "default"
    deadline_at: Optional[float] = None
    # Set when the cheap layer merged events it was not confident about. The
    # LLM is being asked to resolve a boundary, not to summarise for polish.
    needs_boundary_check: bool = False

    @property
    def load(self) -> float:
        if self.queue_capacity <= 0:
            return 1.0
        return max(0.0, min(1.0, self.queue_depth / float(self.queue_capacity)))


@dataclass
class TriggerDecision:
    invoke: bool
    reason: str
    utility: float = 0.0
    threshold: float = 0.0
    audit: bool = False  # invoked purely to measure what we would have missed
    bypass: bool = False  # invoked via a hard safety rule, ignoring thresholds
    # A veto is categorical rather than scalar: no amount of utility overrides
    # it. Only the deadline policy issues one, and only because a late answer
    # has no value to trade against its cost.
    veto: bool = False

    def as_dict(self) -> dict:
        return {
            "invoke": self.invoke,
            "reason": self.reason,
            "utility": round(self.utility, 4),
            "threshold": round(self.threshold, 4),
            "audit": self.audit,
            "bypass": self.bypass,
            "veto": self.veto,
        }


@dataclass
class UtilityWeights:
    """utility = a*importance + b*novelty + c*uncertainty + d*risk - lam*cost."""

    alpha: float = 1.0    # importance
    beta: float = 0.35    # novelty
    gamma: float = 0.45   # uncertainty — we pay to resolve what we cannot settle
    delta: float = 0.9    # risk
    lam: float = 0.30     # cost penalty
    cost_normalizer: float = 2000.0  # tokens that count as "one unit of cost"


def compute_utility(features: EventFeatures, weights: UtilityWeights) -> float:
    """Expected semantic value per unit of compute, on roughly a 0..1 scale."""
    normalized_cost = min(
        1.0, features.estimated_llm_tokens / max(1.0, weights.cost_normalizer)
    )
    raw = (
        weights.alpha * features.importance
        + weights.beta * features.novelty
        + weights.gamma * features.uncertainty
        + weights.delta * features.risk
        - weights.lam * normalized_cost
    )
    denominator = weights.alpha + weights.beta + weights.gamma + weights.delta
    return max(0.0, raw / denominator)


# --------------------------------------------------------------------------
# Policies
# --------------------------------------------------------------------------


class TriggerPolicy(ABC):
    name = "policy"

    @abstractmethod
    def threshold(self, features: EventFeatures, ctx: TriggerContext) -> float:
        """The bar this policy wants utility to clear."""

    def decide(self, features: EventFeatures, ctx: TriggerContext) -> TriggerDecision:
        weights = getattr(self, "weights", UtilityWeights())
        utility = compute_utility(features, weights)
        theta = self.threshold(features, ctx)
        return TriggerDecision(
            invoke=utility > theta,
            reason=f"{self.name}:{'above' if utility > theta else 'below'}_threshold",
            utility=utility,
            threshold=theta,
        )


class FixedThresholdPolicy(TriggerPolicy):
    """Baseline. Ignores everything about system state."""

    name = "fixed"

    def __init__(self, theta: float = 0.5, weights: Optional[UtilityWeights] = None):
        self.theta = theta
        self.weights = weights or UtilityWeights()

    def threshold(self, features: EventFeatures, ctx: TriggerContext) -> float:
        return self.theta


class BudgetAwarePolicy(TriggerPolicy):
    """Raises the bar as the tenant's daily allowance drains.

    With the defaults, ~90% budget remaining gives a threshold near 0.45 and
    ~5% remaining gives near 0.83: late in the day only genuinely important
    events still clear the bar.
    """

    name = "budget"

    def __init__(
        self,
        ledger: BudgetLedger,
        theta_min: float = 0.40,
        theta_max: float = 0.85,
        curve: float = 1.0,
        weights: Optional[UtilityWeights] = None,
    ):
        self.ledger = ledger
        self.theta_min = theta_min
        self.theta_max = theta_max
        self.curve = curve
        self.weights = weights or UtilityWeights()

    def threshold(self, features: EventFeatures, ctx: TriggerContext) -> float:
        remaining = self.ledger.remaining_fraction(ctx.tenant_id, ctx.now)
        span = self.theta_max - self.theta_min
        return self.theta_min + span * ((1.0 - remaining) ** self.curve)


class LoadAwarePolicy(TriggerPolicy):
    """Raises the bar as the LLM queue fills.

    Quadratic rather than linear so a half-full queue is barely felt while a
    nearly-full one is decisive — the system stays permissive during normal
    operation and gets selective exactly when it must.
    """

    name = "load"

    def __init__(
        self,
        theta_idle: float = 0.35,
        theta_saturated: float = 0.90,
        weights: Optional[UtilityWeights] = None,
    ):
        self.theta_idle = theta_idle
        self.theta_saturated = theta_saturated
        self.weights = weights or UtilityWeights()

    def threshold(self, features: EventFeatures, ctx: TriggerContext) -> float:
        span = self.theta_saturated - self.theta_idle
        return self.theta_idle + span * (ctx.load ** 2)


class DeadlineAwarePolicy(TriggerPolicy):
    """Refuses work whose answer would arrive after it stopped mattering.

    This is the policy that separates a queue from a decision system. An
    incident summary produced four minutes after the incident resolved is not
    partially valuable — it is worthless, and the tokens it burned were taken
    from something that was still live.
    """

    name = "deadline"

    def __init__(
        self,
        base_theta: float = 0.45,
        safety_margin: float = 1.15,
        weights: Optional[UtilityWeights] = None,
    ):
        self.base_theta = base_theta
        self.safety_margin = safety_margin
        self.weights = weights or UtilityWeights()

    def threshold(self, features: EventFeatures, ctx: TriggerContext) -> float:
        return self.base_theta

    def decide(self, features: EventFeatures, ctx: TriggerContext) -> TriggerDecision:
        decision = super().decide(features, ctx)
        if ctx.deadline_at is None:
            return decision
        time_left = ctx.deadline_at - ctx.now
        if time_left <= 0:
            return TriggerDecision(
                invoke=False,
                reason="deadline:already_stale",
                utility=decision.utility,
                threshold=decision.threshold,
                veto=True,
            )
        if ctx.estimated_wait_seconds * self.safety_margin > time_left:
            return TriggerDecision(
                invoke=False,
                reason="deadline:would_miss_freshness_window",
                utility=decision.utility,
                threshold=decision.threshold,
                veto=True,
            )
        return decision


@dataclass
class SafetyRule:
    """A hard bypass. Evaluated before any threshold, and never overridden.

    These exist because a learned or tuned score being wrong about an outage is
    unacceptable in a way that being wrong about a CI notification is not.
    """

    name: str
    predicate: Callable[[Event, EventFeatures], bool]


DEFAULT_SAFETY_RULES: List[SafetyRule] = [
    SafetyRule("p0_priority", lambda e, f: f.priority == Priority.P0),
    SafetyRule("critical_risk", lambda e, f: f.risk >= 0.95),
    SafetyRule(
        "explicit_escalation",
        lambda e, f: bool(e.metadata.get("force_enrich")),
    ),
]


class CompositePolicy(TriggerPolicy):
    """The policy PulseFeed actually runs.

    Composition rule: the effective threshold is the *maximum* over sub-policies
    (the most conservative constraint binds), a hard veto from any sub-policy is
    final, and safety rules bypass the whole calculation.

    On top of that sits audit sampling. A fraction of *rejected* events are sent
    to the LLM anyway and flagged ``audit=True``. Their results never change the
    user's timeline; they exist so ``important_event_recall`` can be estimated
    from data instead of hoped for. Without this, a first-stage scorer that
    silently degrades looks exactly like one that is working.
    """

    name = "pulsefeed"

    def __init__(
        self,
        policies: Sequence[TriggerPolicy],
        safety_rules: Optional[Sequence[SafetyRule]] = None,
        audit_sample_rate: float = 0.01,
        seed: int = 1337,
        weights: Optional[UtilityWeights] = None,
        skip_when_provider_down: bool = True,
        boundary_check_discount: float = 0.6,
    ):
        if not policies:
            raise ValueError("CompositePolicy needs at least one sub-policy")
        self.policies = list(policies)
        self.boundary_check_discount = boundary_check_discount
        self.safety_rules = list(
            safety_rules if safety_rules is not None else DEFAULT_SAFETY_RULES
        )
        self.audit_sample_rate = audit_sample_rate
        self.weights = weights or UtilityWeights()
        self.skip_when_provider_down = skip_when_provider_down
        self._rng = random.Random(seed)

    def threshold(self, features: EventFeatures, ctx: TriggerContext) -> float:
        return max(p.threshold(features, ctx) for p in self.policies)

    def decide_event(
        self, event: Event, features: EventFeatures, ctx: TriggerContext
    ) -> TriggerDecision:
        utility = compute_utility(features, self.weights)

        for rule in self.safety_rules:
            if rule.predicate(event, features):
                return TriggerDecision(
                    invoke=True,
                    reason=f"safety:{rule.name}",
                    utility=utility,
                    threshold=0.0,
                    bypass=True,
                )

        # A veto from any sub-policy (deadline, mainly) is final: no amount of
        # utility makes a stale answer useful. Note this checks the explicit
        # veto flag, not merely "the sub-policy said no" — every sub-policy says
        # no to most events, and treating that as categorical would collapse the
        # composite into whichever sub-policy happened to be strictest.
        for policy in self.policies:
            sub = policy.decide(features, ctx)
            if sub.veto:
                return TriggerDecision(
                    invoke=False,
                    reason=sub.reason,
                    utility=utility,
                    threshold=sub.threshold,
                    veto=True,
                )

        if self.skip_when_provider_down and not ctx.provider_healthy:
            return TriggerDecision(
                invoke=False,
                reason="provider:circuit_open",
                utility=utility,
                threshold=1.0,
            )

        theta = self.threshold(features, ctx)
        if utility > theta:
            return TriggerDecision(
                invoke=True, reason="utility_above_threshold", utility=utility, threshold=theta
            )

        # An unresolved grouping boundary is worth a discount on the threshold:
        # the cheap layer has already told us it cannot answer this one, which
        # is the best available evidence that a model would add something.
        if ctx.needs_boundary_check and utility > theta * self.boundary_check_discount:
            return TriggerDecision(
                invoke=True, reason="boundary_check", utility=utility, threshold=theta
            )

        if self.audit_sample_rate > 0 and self._rng.random() < self.audit_sample_rate:
            return TriggerDecision(
                invoke=True,
                reason="audit_sample",
                utility=utility,
                threshold=theta,
                audit=True,
            )

        return TriggerDecision(
            invoke=False, reason="utility_below_threshold", utility=utility, threshold=theta
        )

    def decide(self, features: EventFeatures, ctx: TriggerContext) -> TriggerDecision:
        """Feature-only path, for callers without the originating Event."""
        stub = Event(source="unknown", content="", tenant_id=ctx.tenant_id)
        return self.decide_event(stub, features, ctx)


def build_default_policy(
    ledger: BudgetLedger,
    *,
    audit_sample_rate: float = 0.01,
    seed: int = 1337,
) -> CompositePolicy:
    """The stack used by the PulseFeed pipeline and by experiment arm D."""
    return CompositePolicy(
        policies=[
            BudgetAwarePolicy(ledger),
            LoadAwarePolicy(),
            DeadlineAwarePolicy(),
        ],
        audit_sample_rate=audit_sample_rate,
        seed=seed,
    )
