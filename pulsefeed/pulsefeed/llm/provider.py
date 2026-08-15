"""LLM provider layer.

One interface, three implementations, and a resilient wrapper that turns an
unreliable dependency into a bounded one:

    LLMProvider              the contract
    MockProvider             deterministic, offline, injectable failures
    OpenAICompatibleProvider real HTTP, OpenAI-shaped, works with vLLM too
    ResilientProvider        breaker + deadline-aware retry + fallback chain

The wrapper is where the interesting policy lives. Its central claim is that
*a retry is not automatically useful work*: retrying a request whose freshness
window has already closed spends a worker slot to produce something nobody
wants, while live events wait behind it. So every attempt re-checks the
deadline, and the retry budget caps retries as a fraction of normal traffic so
a struggling provider is not finished off by its own clients.
"""

from __future__ import annotations

import asyncio
import json
import random
import re
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

from ..clock import Clock, RealClock
from ..models import Event, SemanticAnnotation, Severity, new_id
from .circuit_breaker import (
    BreakerConfig,
    CircuitBreaker,
    CircuitOpenError,
    RetryBudget,
)
from .prompts import (
    EnrichmentRequest,
    ValidationError,
    validate_annotation_payload,
)


class ProviderError(RuntimeError):
    """Provider failed in a way that may be worth retrying."""


class ProviderTimeout(ProviderError):
    pass


class ProviderUnavailable(ProviderError):
    """Provider is down. Not worth retrying against this provider right now."""


class EnrichmentUnavailable(RuntimeError):
    """Every provider failed or was skipped. Caller must degrade."""


@dataclass
class ProviderResult:
    payload: Any                # raw model output: dict or JSON string
    model: str
    tokens_used: int
    cost_usd: float = 0.0
    latency_seconds: float = 0.0


class LLMProvider(ABC):
    name: str = "provider"
    model: str = "unknown"

    @abstractmethod
    async def complete(self, request: EnrichmentRequest) -> ProviderResult:
        ...

    async def aclose(self) -> None:  # pragma: no cover - trivial default
        return None


# --------------------------------------------------------------------------
# Mock provider
# --------------------------------------------------------------------------

_SEVERITY_TERMS = {
    "critical": ("outage", "sev0", "sev1", "data-loss", "breach", "crashloop", "down"),
    "error": ("error", "errors", "failed", "failure", "timeout", "timeouts", "oom",
              "crash", "saturation", "saturated", "exhausted", "unavailable", "5xx"),
    "warning": ("degraded", "slow", "spike", "latency", "retry", "regression",
                "warning", "elevated"),
}
_RECOVERY_TERMS = ("resolved", "recovered", "restored", "normal", "mitigated", "healthy")
_DEPLOY_TERMS = ("deploy", "deployed", "deployment", "release", "released", "rollout")
_ROLLBACK_TERMS = ("rollback", "rolled", "revert", "reverted")


@dataclass
class MockConfig:
    """Knobs the failure-injection harness drives."""

    base_latency: float = 0.25       # simulated seconds
    latency_jitter: float = 0.15
    extra_latency: float = 0.0       # "slow provider" injection
    failure_rate: float = 0.0        # transient failures
    outage: bool = False             # hard outage: every call fails
    timeout: float = 8.0
    tokens_per_char: float = 0.27
    usd_per_1k_tokens: float = 0.0015
    seed: int = 7


class MockProvider(LLMProvider):
    """A deterministic stand-in that does real (if simple) analysis.

    It is not a stub returning canned text: it reads the events, derives
    severity from their language, notices deploy→degradation→rollback ordering
    within a cluster, and writes a narrative over the whole group. That matters
    for the experiments — the semantic advantage measured for the LLM arms comes
    from operating over a coalesced cluster plus entity memory, which is a real
    structural advantage a real model would also have, rather than from the mock
    peeking at ground-truth labels. It never sees the trace's importance labels.
    """

    name = "mock"
    model = "mock-analyst-v1"

    def __init__(
        self, config: Optional[MockConfig] = None, clock: Optional[Clock] = None
    ) -> None:
        self.config = config or MockConfig()
        self.clock = clock or RealClock()
        self._rng = random.Random(self.config.seed)
        self.calls = 0
        self.failures = 0

    # -- failure injection -------------------------------------------------

    def set_outage(self, enabled: bool) -> None:
        self.config.outage = enabled

    def set_extra_latency(self, seconds: float) -> None:
        self.config.extra_latency = seconds

    def set_failure_rate(self, rate: float) -> None:
        self.config.failure_rate = max(0.0, min(1.0, rate))

    # -- provider contract -------------------------------------------------

    async def complete(self, request: EnrichmentRequest) -> ProviderResult:
        self.calls += 1
        started = self.clock.now()

        latency = (
            self.config.base_latency
            + self._rng.uniform(0, self.config.latency_jitter)
            + self.config.extra_latency
        )

        if self.config.outage:
            # Model a connection that hangs and then fails, which is the
            # expensive shape of outage — not an instant refusal.
            await self.clock.sleep(min(latency, self.config.timeout))
            self.failures += 1
            raise ProviderUnavailable("mock provider outage")

        if latency > self.config.timeout:
            await self.clock.sleep(self.config.timeout)
            self.failures += 1
            raise ProviderTimeout(
                f"mock provider exceeded {self.config.timeout}s timeout"
            )

        await self.clock.sleep(latency)

        if self._rng.random() < self.config.failure_rate:
            self.failures += 1
            raise ProviderError("mock provider transient failure")

        payload = self._analyze(request)
        prompt_tokens = int(len(request.user_prompt()) * self.config.tokens_per_char)
        completion_tokens = int(len(json.dumps(payload)) * self.config.tokens_per_char)
        tokens = prompt_tokens + completion_tokens
        return ProviderResult(
            payload=payload,
            model=self.model,
            tokens_used=tokens,
            cost_usd=tokens / 1000.0 * self.config.usd_per_1k_tokens,
            latency_seconds=self.clock.now() - started,
        )

    # -- the "analysis" ----------------------------------------------------

    def _analyze(self, request: EnrichmentRequest) -> Dict[str, Any]:
        events = request.events
        text = " ".join(e.content for e in events).lower()
        words = set(re.findall(r"[a-z0-9_]+", text))

        recovered = bool(words & set(_RECOVERY_TERMS))
        severity = "info"
        for level in ("critical", "error", "warning"):
            if words & set(_SEVERITY_TERMS[level]):
                severity = level
                break
        if recovered and severity in ("warning", "error"):
            severity = "info"

        deployed = bool(words & set(_DEPLOY_TERMS))
        rolled_back = bool(words & set(_ROLLBACK_TERMS))

        if severity in ("critical", "error"):
            category = "incident"
        elif rolled_back or deployed:
            category = "deployment"
        elif recovered:
            category = "recovery"
        elif any(e.source in ("telemetry", "metrics") for e in events):
            category = "telemetry"
        elif any(e.source == "github" for e in events):
            category = "code_change"
        else:
            category = "discussion"

        entities: List[str] = []
        for e in events:
            for entity_id in e.entity_ids:
                if entity_id not in entities:
                    entities.append(entity_id)

        causal_links: List[str] = []
        if deployed and severity in ("warning", "error", "critical"):
            causal_links.append("deployment preceded degradation")
        if rolled_back and recovered:
            causal_links.append("rollback preceded recovery")

        summary = self._narrate(
            events, severity, recovered, deployed, rolled_back, request
        )

        actionability = {
            "critical": "act_now",
            "error": "investigate",
            "warning": "monitor",
            "info": "none",
        }[severity]

        # Confidence drops when the grouping itself was uncertain: an honest
        # model is less sure about a cluster it was asked to second-guess.
        confidence = 0.9 if not request.needs_boundary_check else 0.62
        if len(events) > 8:
            confidence -= 0.08

        return {
            "summary": summary,
            "category": category,
            "severity": severity,
            "entities": entities[:10],
            "actionability": actionability,
            "causal_links": causal_links,
            "confidence": round(max(0.1, confidence), 2),
            "source_event_ids": [e.event_id for e in events],
            "same_happening": len({e.primary_entity for e in events}) == 1,
        }

    def _narrate(
        self,
        events: Sequence[Event],
        severity: str,
        recovered: bool,
        deployed: bool,
        rolled_back: bool,
        request: EnrichmentRequest,
    ) -> str:
        entity = events[0].primary_entity
        first, last = events[0], events[-1]
        span = max(0, int(last.timestamp - first.timestamp))

        if len(events) == 1:
            head = first.content.rstrip(".")
        else:
            head = (
                f"{entity}: {len(events)} related events over {span}s, "
                f"starting with \"{first.content.rstrip('.')}\""
            )

        clauses = [head]
        if deployed and severity in ("warning", "error", "critical"):
            clauses.append("degradation followed a deployment")
        if rolled_back:
            clauses.append("a rollback was performed")
        if recovered:
            clauses.append("the condition has since recovered")
        elif severity in ("error", "critical"):
            clauses.append("the condition is ongoing")
        if request.entity_context and "state: degraded" in request.entity_context:
            clauses.append("this entity was already in a degraded state")

        return "; ".join(clauses) + "."


# --------------------------------------------------------------------------
# Real HTTP provider
# --------------------------------------------------------------------------


@dataclass
class OpenAIConfig:
    base_url: str = "https://api.openai.com/v1"
    api_key: str = ""
    model: str = "gpt-4o-mini"
    timeout: float = 15.0
    max_output_tokens: int = 600
    temperature: float = 0.0
    usd_per_1k_prompt_tokens: float = 0.00015
    usd_per_1k_completion_tokens: float = 0.0006


class OpenAICompatibleProvider(LLMProvider):
    """Talks to anything speaking the OpenAI chat-completions shape.

    That includes a local vLLM server, which is what makes "fall back to a local
    model when the hosted one is down" a configuration change rather than a
    rewrite. JSON mode is requested but never trusted — the response still goes
    through the same validation as every other provider's.
    """

    name = "openai"

    def __init__(self, config: OpenAIConfig, client: Any = None) -> None:
        self.config = config
        self.model = config.model
        self._client = client
        self.calls = 0
        self.failures = 0

    def _ensure_client(self):
        if self._client is None:
            try:
                import httpx
            except ImportError as exc:  # pragma: no cover - env dependent
                raise ProviderUnavailable(
                    "httpx is required for OpenAICompatibleProvider"
                ) from exc
            self._client = httpx.AsyncClient(timeout=self.config.timeout)
        return self._client

    async def complete(self, request: EnrichmentRequest) -> ProviderResult:
        from .prompts import SYSTEM_PROMPT

        client = self._ensure_client()
        self.calls += 1
        started = time.monotonic()

        body = {
            "model": self.config.model,
            "temperature": self.config.temperature,
            "max_tokens": self.config.max_output_tokens,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": request.user_prompt()},
            ],
        }
        headers = {"Content-Type": "application/json"}
        if self.config.api_key:
            headers["Authorization"] = f"Bearer {self.config.api_key}"

        try:
            response = await client.post(
                f"{self.config.base_url.rstrip('/')}/chat/completions",
                json=body,
                headers=headers,
                timeout=self.config.timeout,
            )
        except Exception as exc:
            self.failures += 1
            if "timeout" in type(exc).__name__.lower():
                raise ProviderTimeout(str(exc)) from exc
            raise ProviderUnavailable(str(exc)) from exc

        if response.status_code == 429 or response.status_code >= 500:
            self.failures += 1
            raise ProviderUnavailable(f"HTTP {response.status_code}")
        if response.status_code >= 400:
            self.failures += 1
            raise ProviderError(f"HTTP {response.status_code}: {response.text[:200]}")

        data = response.json()
        try:
            content = data["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as exc:
            self.failures += 1
            raise ProviderError(f"unexpected response shape: {exc}") from exc

        usage = data.get("usage") or {}
        prompt_tokens = int(usage.get("prompt_tokens", 0))
        completion_tokens = int(usage.get("completion_tokens", 0))
        tokens = int(usage.get("total_tokens", prompt_tokens + completion_tokens))
        cost = (
            prompt_tokens / 1000.0 * self.config.usd_per_1k_prompt_tokens
            + completion_tokens / 1000.0 * self.config.usd_per_1k_completion_tokens
        )
        return ProviderResult(
            payload=content,
            model=data.get("model", self.config.model),
            tokens_used=tokens,
            cost_usd=cost,
            latency_seconds=time.monotonic() - started,
        )

    async def aclose(self) -> None:
        if self._client is not None and hasattr(self._client, "aclose"):
            await self._client.aclose()


# --------------------------------------------------------------------------
# Resilient wrapper
# --------------------------------------------------------------------------


@dataclass
class ResilienceConfig:
    max_attempts: int = 3
    base_backoff: float = 0.25
    max_backoff: float = 4.0
    jitter: float = 0.5
    per_attempt_timeout: float = 10.0
    retry_budget_ratio: float = 0.2
    breaker: BreakerConfig = field(default_factory=BreakerConfig)


@dataclass
class EnrichmentOutcome:
    annotation: SemanticAnnotation
    attempts: int
    provider_name: str
    fallback_used: bool
    latency_seconds: float


class ResilientProvider:
    """Provider chain with breaker, capped retries and deadline awareness."""

    def __init__(
        self,
        primary: LLMProvider,
        fallbacks: Sequence[LLMProvider] = (),
        config: Optional[ResilienceConfig] = None,
        clock: Optional[Clock] = None,
        rng: Optional[random.Random] = None,
    ) -> None:
        self.config = config or ResilienceConfig()
        self.clock = clock or RealClock()
        self._rng = rng or random.Random(99)
        self.providers: List[LLMProvider] = [primary, *fallbacks]
        self.breakers: Dict[str, CircuitBreaker] = {
            p.name: CircuitBreaker(p.name, self.config.breaker, clock=self.clock.now)
            for p in self.providers
        }
        self.retry_budget = RetryBudget(self.config.retry_budget_ratio)
        self.stats = {
            "requests": 0,
            "successes": 0,
            "retries": 0,
            "fallbacks": 0,
            "deadline_abandons": 0,
            "validation_failures": 0,
            "exhausted": 0,
        }

    @property
    def primary_healthy(self) -> bool:
        """What the trigger consults: is the main path worth admitting work to?"""
        return self.breakers[self.providers[0].name].is_healthy

    @property
    def any_healthy(self) -> bool:
        return any(b.is_healthy for b in self.breakers.values())

    def _deadline_exceeded(self, request: EnrichmentRequest) -> bool:
        return (
            request.deadline_at is not None
            and self.clock.now() >= request.deadline_at
        )

    def _backoff(self, attempt: int) -> float:
        delay = min(self.config.max_backoff, self.config.base_backoff * (2 ** attempt))
        return delay * (1.0 + self._rng.uniform(-self.config.jitter, self.config.jitter))

    async def enrich(self, request: EnrichmentRequest) -> EnrichmentOutcome:
        """Run the chain. Raises EnrichmentUnavailable if nothing worked."""
        self.stats["requests"] += 1
        self.retry_budget.record_request()
        started = self.clock.now()
        attempts = 0
        last_error: Optional[Exception] = None

        for index, provider in enumerate(self.providers):
            breaker = self.breakers[provider.name]

            for attempt in range(self.config.max_attempts):
                if self._deadline_exceeded(request):
                    self.stats["deadline_abandons"] += 1
                    raise EnrichmentUnavailable(
                        "freshness deadline passed before enrichment completed"
                    )
                if not breaker.allows_request():
                    last_error = CircuitOpenError(f"{provider.name} circuit open")
                    break

                attempts += 1
                try:
                    result = await self._call_with_timeout(provider, request)
                except asyncio.TimeoutError as exc:
                    breaker.record_failure()
                    last_error = ProviderTimeout(str(exc))
                except ProviderUnavailable as exc:
                    breaker.record_failure()
                    last_error = exc
                    break  # this provider is down; move on rather than retry it
                except ProviderError as exc:
                    breaker.record_failure()
                    last_error = exc
                else:
                    breaker.record_success()
                    try:
                        annotation = self._to_annotation(request, result)
                    except ValidationError as exc:
                        # A malformed response is a failure of this attempt, not
                        # of the provider's availability.
                        self.stats["validation_failures"] += 1
                        last_error = exc
                    else:
                        self.stats["successes"] += 1
                        if index > 0:
                            self.stats["fallbacks"] += 1
                        return EnrichmentOutcome(
                            annotation=annotation,
                            attempts=attempts,
                            provider_name=provider.name,
                            fallback_used=index > 0,
                            latency_seconds=self.clock.now() - started,
                        )

                # Decide whether another attempt is worth making at all.
                if attempt + 1 >= self.config.max_attempts:
                    break
                if not self.retry_budget.try_consume():
                    break
                delay = self._backoff(attempt)
                if request.deadline_at is not None:
                    time_left = request.deadline_at - self.clock.now()
                    if delay >= time_left:
                        # Sleeping through the deadline and then working is the
                        # exact behaviour this system exists to avoid.
                        self.stats["deadline_abandons"] += 1
                        raise EnrichmentUnavailable(
                            "retry would land after the freshness deadline"
                        )
                self.stats["retries"] += 1
                await self.clock.sleep(delay)

        self.stats["exhausted"] += 1
        raise EnrichmentUnavailable(
            f"all providers failed (last error: {last_error})"
        )

    async def _call_with_timeout(
        self, provider: LLMProvider, request: EnrichmentRequest
    ) -> ProviderResult:
        """Apply the per-attempt timeout in whatever units the clock uses.

        Under simulated time there is no meaningful wall-clock budget to enforce
        — real elapsed time is near zero — so the provider's own timeout is the
        one that governs. Under a scaled clock the wall timeout is scaled to
        match; under a real clock it is used as-is.
        """
        timeout = self.config.per_attempt_timeout
        if isinstance(self.clock, RealClock):
            return await asyncio.wait_for(provider.complete(request), timeout=timeout)
        scale = getattr(self.clock, "scale", None)
        if scale is not None:
            return await asyncio.wait_for(
                provider.complete(request), timeout=timeout * scale
            )
        return await provider.complete(request)

    def _to_annotation(
        self, request: EnrichmentRequest, result: ProviderResult
    ) -> SemanticAnnotation:
        validated = validate_annotation_payload(
            result.payload, request.allowed_event_ids
        )
        return SemanticAnnotation(
            annotation_id=new_id("ann"),
            tenant_id=request.tenant_id,
            summary=validated["summary"],
            source_event_ids=validated["source_event_ids"],
            category=validated["category"],
            severity=Severity(validated["severity"]),
            entities=validated["entities"],
            actionability=validated["actionability"],
            causal_links=validated["causal_links"],
            confidence=validated["confidence"],
            model=result.model,
            provider=self.providers[0].name,
            generated_at=self.clock.now(),
            tokens_used=result.tokens_used,
            cost_usd=result.cost_usd,
        )

    def snapshot(self) -> Dict[str, Any]:
        return {
            "stats": dict(self.stats),
            "breakers": {name: b.snapshot() for name, b in self.breakers.items()},
            "retry_budget_denied": self.retry_budget.denied,
        }

    def reset(self) -> None:
        for breaker in self.breakers.values():
            breaker.reset()
        self.retry_budget.reset()
        for key in self.stats:
            self.stats[key] = 0

    async def aclose(self) -> None:
        for provider in self.providers:
            await provider.aclose()
