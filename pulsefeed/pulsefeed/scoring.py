"""Stage one: the cheap event scorer.

This is the only component that touches 100% of traffic, so it is built from
things that cost nothing interesting: a source-priority table, a keyword
lexicon, recency decay, a hashed embedding for novelty, and a small linear
model over those signals. No LLM, no network call, no model server.

The design answer to "why not just let the LLM score everything?" lives here:
first-stage cost multiplies by total event volume, so it must stay O(cheap).
The LLM is a sparse oracle consulted about hard cases, not a mandatory step in
the ingest path.
"""

from __future__ import annotations

import math
import re
import time
from collections import deque
from dataclasses import dataclass, field
from typing import (
    Deque,
    Dict,
    List,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    Tuple,
)

from .embedding import Embedder, HashingEmbedder, max_similarity
from .models import Event, EventFeatures, Priority


def sigmoid(x: float) -> float:
    if x >= 0:
        return 1.0 / (1.0 + math.exp(-x))
    z = math.exp(x)
    return z / (1.0 + z)


# --------------------------------------------------------------------------
# Lexicons. Deliberately small and inspectable: an on-call engineer should be
# able to read this and predict what the system will consider serious.
# --------------------------------------------------------------------------

RISK_TERMS: Dict[str, float] = {
    "outage": 1.0,
    "down": 0.8,
    "incident": 0.95,
    "critical": 1.0,
    "sev1": 1.0,
    "sev0": 1.0,
    "paging": 0.9,
    "saturation": 0.85,
    "saturated": 0.85,
    "exhausted": 0.85,
    "timeout": 0.75,
    "timeouts": 0.75,
    "failing": 0.7,
    "failed": 0.65,
    "failure": 0.75,
    "error": 0.6,
    "errors": 0.65,
    "5xx": 0.85,
    "oom": 0.9,
    "crash": 0.85,
    "crashloop": 0.95,
    "rollback": 0.8,
    "regression": 0.7,
    "breach": 0.95,
    "leak": 0.8,
    "corrupt": 0.9,
    "data-loss": 1.0,
    "unavailable": 0.85,
    "degraded": 0.7,
    "spike": 0.55,
    "latency": 0.5,
    "slow": 0.45,
    "retry": 0.4,
    "deadlock": 0.85,
}

RECOVERY_TERMS = {
    "resolved",
    "recovered",
    "restored",
    "normal",
    "mitigated",
    "healthy",
    "green",
    "succeeded",
    "passed",
}

ACTION_TERMS = {
    "deploy",
    "deployed",
    "deployment",
    "rollback",
    "revert",
    "merge",
    "merged",
    "release",
    "released",
    "migration",
    "restart",
    "scale",
    "hotfix",
}

# Sources differ wildly in signal density. A pager alert deserves attention by
# default; a metrics tick does not.
SOURCE_PRIORITY: Dict[str, float] = {
    "pagerduty": 0.95,
    "alertmanager": 0.9,
    "grafana": 0.8,
    "slack": 0.55,
    "discord": 0.5,
    "github": 0.5,
    "ci": 0.5,
    "email": 0.4,
    "rss": 0.3,
    "telemetry": 0.2,
}
DEFAULT_SOURCE_PRIORITY = 0.4

# Sources whose events are, by nature, high-volume and individually low-value.
TELEMETRY_SOURCES = {"telemetry", "metrics", "rss"}

_MENTION_RE = re.compile(r"@([a-z0-9_.-]+)", re.IGNORECASE)


# The feature vector, named and ordered. This tuple is the contract between
# training and inference: ``CheapScorer.extract`` produces exactly these keys,
# a learned model consumes exactly these keys, and any disagreement is a loud
# KeyError rather than a silently mis-weighted feature.
FEATURE_NAMES: Tuple[str, ...] = (
    "source_priority",
    "risk",
    "recency",
    "novelty",
    "user_relevance",
    "burst",
    "duplication",
    "recovery",
    "action",
)


@dataclass
class ScorerWeights:
    """Linear model over cheap signals.

    These are hand-set for the first version, exactly as the plan calls for.
    They are in one place and in the same shape a fitted logistic regression
    would produce, so swapping in trained coefficients later is a data change
    rather than a code change.
    """

    bias: float = -3.6
    source_priority: float = 2.2
    risk: float = 2.6
    recency: float = 0.5
    # Novelty is deliberately weak. Almost every event is novel the first time
    # it is seen, so a large novelty weight makes "importance" mostly measure
    # "arrived recently" — which is the chronological baseline wearing a hat.
    novelty: float = 0.7
    user_relevance: float = 1.5
    burst: float = 0.9
    duplication: float = -1.8
    recovery: float = 0.5
    action: float = 0.7

    def coefficients(self) -> Dict[str, float]:
        return {name: getattr(self, name) for name in FEATURE_NAMES}

    @classmethod
    def from_coefficients(
        cls, bias: float, coefficients: Mapping[str, float]
    ) -> "ScorerWeights":
        """Build from a fitted model's output.

        Missing coefficients are an error rather than a zero: a training run
        that silently dropped a feature should not quietly ship as a model that
        ignores it.
        """
        missing = [n for n in FEATURE_NAMES if n not in coefficients]
        if missing:
            raise ValueError(f"missing coefficients for: {missing}")
        return cls(bias=bias, **{n: float(coefficients[n]) for n in FEATURE_NAMES})


@dataclass
class Extracted:
    """One event's features, as seen by both training and inference."""

    values: Dict[str, float]      # exactly FEATURE_NAMES
    extras: Dict[str, float]      # diagnostics, not model inputs
    embedding: List[float]        # the text vector, for embedding-aware models

    def vector(self) -> List[float]:
        return [self.values[name] for name in FEATURE_NAMES]


class ScoreModel(Protocol):
    """Anything that turns an event's cheap features into P(important).

    ``embedding`` is passed to every model but ignored by the linear one. It is
    in the signature so that a learned text model — a MiniLM head running on a
    GPU, say — is a drop-in replacement rather than a change to the call site.
    """

    def predict_proba(
        self,
        features: Mapping[str, float],
        embedding: Optional[Sequence[float]] = None,
    ) -> float:
        ...


class LinearScoreModel:
    """Logistic model over ``FEATURE_NAMES``. The default and the fallback.

    Whether its coefficients were hand-set or fitted, inference is identical —
    which is the point of keeping the hand-tuned version in this exact shape.
    """

    def __init__(self, weights: Optional[ScorerWeights] = None) -> None:
        self.weights = weights or ScorerWeights()

    def predict_proba(
        self,
        features: Mapping[str, float],
        embedding: Optional[Sequence[float]] = None,
    ) -> float:
        w = self.weights
        logit = w.bias + sum(
            getattr(w, name) * features[name] for name in FEATURE_NAMES
        )
        return sigmoid(logit)


@dataclass
class ScorerConfig:
    weights: ScorerWeights = field(default_factory=ScorerWeights)
    novelty_window: int = 256          # embeddings retained per tenant
    burst_window_seconds: float = 120.0
    burst_saturation: int = 8          # events/entity/window that means "bursting"
    recency_half_life: float = 900.0   # seconds
    tokens_per_char: float = 0.27      # rough English tokenisation ratio
    # Rolling-state caps. Burst windows are keyed by entity and novelty windows
    # by tenant; without these caps every entity and tenant ever seen kept its
    # key alive forever. Evicting oldest-first just resets that entity's burst
    # score / that tenant's novelty window — a scoring degradation, not a leak.
    max_tracked_entities: int = 50_000
    max_tracked_tenants: int = 1_000
    context_token_overhead: int = 320  # system prompt + entity memory
    usd_per_1k_tokens: float = 0.0015


@dataclass
class TenantAffinity:
    """What a given tenant has told us it cares about."""

    watched_entities: set = field(default_factory=set)
    watched_actors: set = field(default_factory=set)
    watched_keywords: set = field(default_factory=set)
    muted_sources: set = field(default_factory=set)


class CheapScorer:
    """Turns a raw Event into EventFeatures using only cheap signals."""

    def __init__(
        self,
        config: Optional[ScorerConfig] = None,
        embedder: Optional[Embedder] = None,
        model: Optional[ScoreModel] = None,
    ) -> None:
        self.config = config or ScorerConfig()
        # Hand-set weights unless a fitted model is supplied. Both implement the
        # same one-method interface, so nothing downstream can tell which is in
        # use — including the trigger, which keeps treating the output as a
        # probability because both are calibrated to be one.
        self.model: ScoreModel = model or LinearScoreModel(self.config.weights)
        self.embedder = embedder or HashingEmbedder()
        self._recent_vectors: Dict[str, Deque[List[float]]] = {}
        self._entity_events: Dict[Tuple[str, str], Deque[float]] = {}
        self._affinity: Dict[str, TenantAffinity] = {}

    # -- configuration ----------------------------------------------------

    def set_affinity(self, tenant_id: str, affinity: TenantAffinity) -> None:
        self._affinity[tenant_id] = affinity

    def affinity(self, tenant_id: str) -> TenantAffinity:
        return self._affinity.setdefault(tenant_id, TenantAffinity())

    # -- individual signals ------------------------------------------------

    def _risk_score(self, tokens: Sequence[str]) -> Tuple[float, bool]:
        """Peak risk term plus a small bonus for corroborating terms.

        Peak rather than sum: one "outage" is decisive, and summing would let a
        paragraph of mild words outrank it.
        """
        hits = [RISK_TERMS[t] for t in tokens if t in RISK_TERMS]
        if not hits:
            return 0.0, False
        peak = max(hits)
        corroboration = min(0.15, 0.05 * (len(hits) - 1))
        return min(1.0, peak + corroboration), True

    def _recency(self, event: Event, now: float) -> float:
        age = max(0.0, now - event.timestamp)
        return math.exp(-age * math.log(2) / self.config.recency_half_life)

    def _novelty(self, tenant_id: str, vector: List[float]) -> Tuple[float, float]:
        """Returns (novelty, duplication) against this tenant's recent window."""
        window = self._recent_vectors.setdefault(
            tenant_id, deque(maxlen=self.config.novelty_window)
        )
        while len(self._recent_vectors) > self.config.max_tracked_tenants:
            self._recent_vectors.pop(next(iter(self._recent_vectors)))
        duplication = max(0.0, max_similarity(vector, window))
        window.append(vector)
        return 1.0 - duplication, duplication

    def _burst(self, event: Event, now: float) -> float:
        key = (event.tenant_id, event.primary_entity)
        window = self._entity_events.setdefault(key, deque())
        while len(self._entity_events) > self.config.max_tracked_entities:
            self._entity_events.pop(next(iter(self._entity_events)))
        window.append(event.timestamp)
        cutoff = now - self.config.burst_window_seconds
        while window and window[0] < cutoff:
            window.popleft()
        return min(1.0, len(window) / float(self.config.burst_saturation))

    def _user_relevance(self, event: Event, tokens: Sequence[str]) -> float:
        aff = self.affinity(event.tenant_id)
        score = 0.0
        if any(e in aff.watched_entities for e in event.entity_ids):
            score += 0.6
        if event.actor and event.actor in aff.watched_actors:
            score += 0.3
        if aff.watched_keywords and any(t in aff.watched_keywords for t in tokens):
            score += 0.3
        mentions = _MENTION_RE.findall(event.content)
        if any(m.lower() in aff.watched_actors for m in mentions):
            score += 0.5
        elif mentions:
            score += 0.15
        return min(1.0, score)

    def _uncertainty(
        self,
        *,
        importance: float,
        risk: float,
        has_risk_terms: bool,
        duplication: float,
        source_priority: float,
        token_count: int,
    ) -> float:
        """How much the cheap layer distrusts its own answer.

        High uncertainty is a *reason to spend an LLM call*, not a reason to
        skip one — it marks the cases where cheap signals genuinely cannot
        settle the question. Three things drive it:

        1. importance landing near the decision boundary (0.5),
        2. signals disagreeing (risk words from a source we usually ignore, or
           the reverse: a trusted source saying something bland),
        3. near-duplicate text that is not quite a duplicate, which is the
           classic "is this the same incident or a new one?" question.
        """
        boundary = 1.0 - 2.0 * abs(importance - 0.5)
        conflict = abs(risk - source_priority) if has_risk_terms else 0.0
        ambiguous_dup = 1.0 - 2.0 * abs(duplication - 0.72) if duplication > 0.45 else 0.0
        sparse_text = 0.15 if token_count < 4 else 0.0
        raw = (
            0.45 * boundary
            + 0.30 * max(0.0, conflict)
            + 0.20 * max(0.0, ambiguous_dup)
            + sparse_text
        )
        return max(0.0, min(1.0, raw))

    def _priority(
        self, event: Event, importance: float, risk: float, user_relevance: float
    ) -> Priority:
        """Scheduling class. Deliberately rule-based and auditable.

        Priority is a *service guarantee*, not a score. It decides who gets
        dropped when the system is on fire, so it must be explainable without
        reference to a model's internals.
        """
        if event.metadata.get("priority_override") is not None:
            return Priority(int(event.metadata["priority_override"]))
        if risk >= 0.85 and importance >= 0.5:
            return Priority.P0
        if event.source in ("pagerduty", "alertmanager") and risk >= 0.6:
            return Priority.P0
        if user_relevance >= 0.5:
            return Priority.P1
        if event.source in TELEMETRY_SOURCES and risk < 0.6:
            return Priority.P3
        if importance < 0.2 and risk < 0.4:
            return Priority.P3
        return Priority.P2

    def _estimate_cost(self, event: Event) -> Tuple[int, float]:
        tokens = int(
            len(event.content) * self.config.tokens_per_char
            + self.config.context_token_overhead
        )
        usd = tokens / 1000.0 * self.config.usd_per_1k_tokens
        return tokens, usd

    # -- main entry point --------------------------------------------------

    def extract(self, event: Event, now: Optional[float] = None) -> Extracted:
        """Compute the named feature vector, plus diagnostics.

        Returns ``(features, extras)`` where ``features`` has exactly the keys
        in ``FEATURE_NAMES`` and ``extras`` carries things needed downstream but
        not fed to the model (token count, whether risk terms fired at all).

        **This method has a side effect and the order of calls matters.**
        ``novelty`` and ``burst`` are computed against rolling per-tenant state
        that this call also updates, so features are a function of the stream so
        far, not of the event alone. Training data must therefore be collected
        by replaying events in order through a fresh scorer — computing features
        for a shuffled batch would produce novelty scores that could never occur
        at inference time.
        """
        now = now if now is not None else time.time()

        from .embedding import tokenize  # local import keeps module import cheap

        tokens = tokenize(event.content)
        token_set = set(tokens)

        source_priority = SOURCE_PRIORITY.get(event.source, DEFAULT_SOURCE_PRIORITY)
        if event.source in self.affinity(event.tenant_id).muted_sources:
            source_priority *= 0.3

        risk, has_risk_terms = self._risk_score(tokens)
        vector = self.embedder.embed(event.content)
        novelty, duplication = self._novelty(event.tenant_id, vector)

        features = {
            "source_priority": source_priority,
            "risk": risk,
            "recency": self._recency(event, now),
            "novelty": novelty,
            "user_relevance": self._user_relevance(event, tokens),
            "burst": self._burst(event, now),
            "duplication": duplication,
            "recovery": 1.0 if token_set & RECOVERY_TERMS else 0.0,
            "action": 1.0 if token_set & ACTION_TERMS else 0.0,
        }
        extras = {
            "token_count": float(len(tokens)),
            "has_risk_terms": 1.0 if has_risk_terms else 0.0,
        }
        return Extracted(values=features, extras=extras, embedding=vector)

    def score(self, event: Event, now: Optional[float] = None) -> EventFeatures:
        now = now if now is not None else time.time()
        extracted = self.extract(event, now)
        features, extras = extracted.values, extracted.extras

        importance = self.model.predict_proba(features, extracted.embedding)

        uncertainty = self._uncertainty(
            importance=importance,
            risk=features["risk"],
            has_risk_terms=bool(extras["has_risk_terms"]),
            duplication=features["duplication"],
            source_priority=features["source_priority"],
            token_count=int(extras["token_count"]),
        )
        priority = self._priority(
            event, importance, features["risk"], features["user_relevance"]
        )
        est_tokens, est_usd = self._estimate_cost(event)

        return EventFeatures(
            event_id=event.event_id,
            importance=importance,
            novelty=features["novelty"],
            uncertainty=uncertainty,
            risk=features["risk"],
            user_relevance=features["user_relevance"],
            burst_score=features["burst"],
            duplication_score=features["duplication"],
            estimated_llm_tokens=est_tokens,
            estimated_cost_usd=est_usd,
            priority=priority,
            # The full feature vector rides along verbatim. Training data is
            # collected from live scoring, so what the model sees at fit time is
            # byte-for-byte what it saw at inference time — no reimplementation
            # of feature extraction to drift out of sync.
            signals={**features, **extras},
        ).clamped()

    def embed(self, text: str) -> List[float]:
        return self.embedder.embed(text)

    def reset(self) -> None:
        """Clear rolling state. Used between replay runs so runs are independent."""
        self._recent_vectors.clear()
        self._entity_events.clear()


def aggregate_features(features: Sequence[EventFeatures]) -> EventFeatures:
    """Collapse a cluster's member features into one representative score.

    Importance/risk take the max because a cluster is as serious as its worst
    member; cost sums because the LLM has to read all of it.
    """
    if not features:
        raise ValueError("cannot aggregate empty feature list")
    head = features[0]
    return EventFeatures(
        event_id=head.event_id,
        importance=max(f.importance for f in features),
        novelty=max(f.novelty for f in features),
        uncertainty=max(f.uncertainty for f in features),
        risk=max(f.risk for f in features),
        user_relevance=max(f.user_relevance for f in features),
        burst_score=max(f.burst_score for f in features),
        duplication_score=min(f.duplication_score for f in features),
        estimated_llm_tokens=sum(f.estimated_llm_tokens for f in features),
        estimated_cost_usd=sum(f.estimated_cost_usd for f in features),
        priority=min((f.priority for f in features), default=head.priority),
        signals=dict(head.signals),
    ).clamped()
