"""Prometheus metrics, with a no-op fallback.

``prometheus_client`` is optional: the core has to run in a bare interpreter
(replay harness, unit tests, someone's laptop) without pulling in a metrics
stack. When it is absent these become cheap no-op objects with the same API.

The metric set is chosen around one question — *is the admission policy
working?* — rather than around request rate. The dashboard that matters shows
offered load rising while invocation rate falls, threshold rises, P0 recall
holds flat, and cost stays bounded. That shape is the evidence.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

try:  # pragma: no cover - depends on environment
    from prometheus_client import (
        CollectorRegistry,
        Counter,
        Gauge,
        Histogram,
        generate_latest,
    )

    PROMETHEUS_AVAILABLE = True
except ImportError:  # pragma: no cover - depends on environment
    PROMETHEUS_AVAILABLE = False

    class _NoOpMetric:
        def __init__(self, *args, **kwargs) -> None:
            self._value = 0.0

        def labels(self, *args, **kwargs) -> "_NoOpMetric":
            return self

        def inc(self, amount: float = 1.0) -> None:
            self._value += amount

        def dec(self, amount: float = 1.0) -> None:
            self._value -= amount

        def set(self, value: float) -> None:
            self._value = value

        def observe(self, value: float) -> None:
            self._value = value

    Counter = Gauge = Histogram = _NoOpMetric  # type: ignore[assignment,misc]
    CollectorRegistry = object  # type: ignore[assignment,misc]

    def generate_latest(registry: Any = None) -> bytes:  # type: ignore[misc]
        return b"# prometheus_client not installed\n"


LATENCY_BUCKETS = (0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0)
TOKEN_BUCKETS = (100, 250, 500, 1000, 2000, 4000, 8000, 16000)


class PulseFeedMetrics:
    """All PulseFeed metrics in one object, on a private registry."""

    def __init__(self, registry: Optional[Any] = None) -> None:
        if PROMETHEUS_AVAILABLE:
            self.registry = registry or CollectorRegistry()
            kw = {"registry": self.registry}
        else:
            self.registry = None
            kw = {}

        # -- ingest ---------------------------------------------------------
        self.events_ingested = Counter(
            "pulsefeed_events_ingested_total",
            "Events accepted into the pipeline",
            ["tenant", "source"],
            **kw,
        )
        self.events_coalesced = Counter(
            "pulsefeed_events_coalesced_total",
            "Events folded into an existing cluster instead of standing alone",
            ["tenant"],
            **kw,
        )
        self.clusters_emitted = Counter(
            "pulsefeed_clusters_emitted_total",
            "Clusters closed and passed to the trigger",
            ["tenant", "close_reason"],
            **kw,
        )

        # -- trigger --------------------------------------------------------
        self.events_triggered = Counter(
            "pulsefeed_events_triggered_total",
            "Clusters admitted for LLM enrichment",
            ["tenant", "reason"],
            **kw,
        )
        self.events_skipped = Counter(
            "pulsefeed_events_skipped_total",
            "Clusters routed to the cheap path instead of the LLM",
            ["tenant", "reason"],
            **kw,
        )
        self.trigger_threshold = Gauge(
            "pulsefeed_trigger_threshold",
            "Current effective utility threshold",
            ["tenant"],
            **kw,
        )
        self.trigger_utility = Histogram(
            "pulsefeed_trigger_utility",
            "Computed utility of evaluated clusters",
            buckets=(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0),
            **kw,
        )
        self.audit_samples = Counter(
            "pulsefeed_audit_samples_total",
            "Rejected clusters enriched anyway to measure false negatives",
            ["tenant"],
            **kw,
        )
        self.audit_misses = Counter(
            "pulsefeed_audit_misses_total",
            "Audit samples that turned out to be important (estimated misses)",
            ["tenant"],
            **kw,
        )

        # -- queue ----------------------------------------------------------
        self.queue_depth = Gauge(
            "pulsefeed_queue_depth", "Items waiting for an LLM worker", ["priority"], **kw
        )
        self.queue_wait_seconds = Histogram(
            "pulsefeed_queue_wait_seconds",
            "Time spent queued before dispatch",
            ["priority"],
            buckets=LATENCY_BUCKETS,
            **kw,
        )
        self.events_dropped = Counter(
            "pulsefeed_events_dropped_total",
            "Work shed under overload",
            ["tenant", "priority", "reason"],
            **kw,
        )

        # -- provider -------------------------------------------------------
        self.llm_invocations = Counter(
            "pulsefeed_llm_invocations_total",
            "Enrichment calls that reached a provider",
            ["tenant", "provider", "outcome"],
            **kw,
        )
        self.llm_tokens = Counter(
            "pulsefeed_llm_tokens_total", "Tokens consumed", ["tenant", "provider"], **kw
        )
        self.llm_cost_usd = Counter(
            "pulsefeed_llm_cost_usd_total", "Estimated spend", ["tenant"], **kw
        )
        self.provider_latency = Histogram(
            "pulsefeed_provider_latency_seconds",
            "Provider call latency",
            ["provider"],
            buckets=LATENCY_BUCKETS,
            **kw,
        )
        self.provider_errors = Counter(
            "pulsefeed_provider_errors_total",
            "Provider failures by kind",
            ["provider", "kind"],
            **kw,
        )
        self.circuit_state = Gauge(
            "pulsefeed_circuit_breaker_state",
            "0=closed 1=half_open 2=open",
            ["provider"],
            **kw,
        )
        self.enrichment_tokens = Histogram(
            "pulsefeed_enrichment_tokens",
            "Tokens per enrichment call",
            buckets=TOKEN_BUCKETS,
            **kw,
        )

        # -- output ---------------------------------------------------------
        self.timeline_items = Counter(
            "pulsefeed_timeline_items_total",
            "Items surfaced to users",
            ["tenant", "kind", "enriched"],
            **kw,
        )
        self.end_to_end_latency = Histogram(
            "pulsefeed_end_to_end_latency_seconds",
            "Ingest to timeline-visible latency",
            ["path"],
            buckets=LATENCY_BUCKETS,
            **kw,
        )
        self.important_event_recall = Gauge(
            "pulsefeed_important_event_recall",
            "Rolling estimate of important-event recall (from audit sampling)",
            ["tenant"],
            **kw,
        )
        self.budget_remaining = Gauge(
            "pulsefeed_budget_remaining_fraction",
            "Fraction of the tenant's daily allowance still available",
            ["tenant"],
            **kw,
        )

    def export(self) -> bytes:
        if PROMETHEUS_AVAILABLE:
            return generate_latest(self.registry)
        return generate_latest()


_DEFAULT: Optional[PulseFeedMetrics] = None


def default_metrics() -> PulseFeedMetrics:
    global _DEFAULT
    if _DEFAULT is None:
        _DEFAULT = PulseFeedMetrics()
    return _DEFAULT


CIRCUIT_STATE_VALUES: Dict[str, int] = {"closed": 0, "half_open": 1, "open": 2}
