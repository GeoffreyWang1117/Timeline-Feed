"""Circuit breaker for LLM providers.

When a provider is down, continuing to send it requests is worse than useless:
each one burns a worker slot for the full timeout before failing, which is how
a provider outage turns into a queue collapse. The breaker converts slow
failures into fast ones so the pipeline can fall back while it still has time
to do something useful.
"""

from __future__ import annotations

import enum
import random
import time
from dataclasses import dataclass
from typing import Optional


class BreakerState(enum.Enum):
    CLOSED = "closed"        # normal operation
    OPEN = "open"            # failing fast, provider presumed down
    HALF_OPEN = "half_open"  # probing with a trickle of real traffic


class CircuitOpenError(RuntimeError):
    """Raised instead of calling a provider that is presumed down."""


@dataclass
class BreakerConfig:
    failure_threshold: int = 5        # consecutive failures that trip the breaker
    success_threshold: int = 2        # consecutive probe successes that close it
    recovery_timeout: float = 20.0    # seconds before probing again
    recovery_jitter: float = 0.3      # spread reopen attempts across replicas
    half_open_max_calls: int = 2      # probes allowed at once


class CircuitBreaker:
    def __init__(
        self,
        name: str = "provider",
        config: Optional[BreakerConfig] = None,
        clock=time.monotonic,
        rng: Optional[random.Random] = None,
    ) -> None:
        self.name = name
        self.config = config or BreakerConfig()
        self._clock = clock
        self._rng = rng or random.Random(4242)
        self.state = BreakerState.CLOSED
        self._consecutive_failures = 0
        self._consecutive_successes = 0
        self._opened_at = 0.0
        self._half_open_inflight = 0
        self._retry_after = 0.0
        self.transitions = 0
        self.rejected_calls = 0

    # -- gating ------------------------------------------------------------

    def allows_request(self) -> bool:
        now = self._clock()
        if self.state is BreakerState.CLOSED:
            return True
        if self.state is BreakerState.OPEN:
            if now - self._opened_at >= self._retry_after:
                self._transition(BreakerState.HALF_OPEN)
                self._half_open_inflight = 1
                return True
            self.rejected_calls += 1
            return False
        # HALF_OPEN: let a couple of probes through, reject the rest.
        if self._half_open_inflight < self.config.half_open_max_calls:
            self._half_open_inflight += 1
            return True
        self.rejected_calls += 1
        return False

    @property
    def is_healthy(self) -> bool:
        """What the trigger consults before admitting new LLM work."""
        return self.state is not BreakerState.OPEN

    # -- outcome reporting -------------------------------------------------

    def record_success(self) -> None:
        self._consecutive_failures = 0
        if self.state is BreakerState.HALF_OPEN:
            self._half_open_inflight = max(0, self._half_open_inflight - 1)
            self._consecutive_successes += 1
            if self._consecutive_successes >= self.config.success_threshold:
                self._transition(BreakerState.CLOSED)
        else:
            self._consecutive_successes = 0

    def record_failure(self) -> None:
        self._consecutive_successes = 0
        if self.state is BreakerState.HALF_OPEN:
            self._half_open_inflight = max(0, self._half_open_inflight - 1)
            self._transition(BreakerState.OPEN)
            return
        self._consecutive_failures += 1
        if self._consecutive_failures >= self.config.failure_threshold:
            self._transition(BreakerState.OPEN)

    # -- internals ---------------------------------------------------------

    def _transition(self, new_state: BreakerState) -> None:
        if new_state is self.state:
            return
        self.state = new_state
        self.transitions += 1
        if new_state is BreakerState.OPEN:
            self._opened_at = self._clock()
            jitter = 1.0 + self._rng.uniform(
                -self.config.recovery_jitter, self.config.recovery_jitter
            )
            self._retry_after = max(0.5, self.config.recovery_timeout * jitter)
            self._half_open_inflight = 0
            self._consecutive_successes = 0
        elif new_state is BreakerState.CLOSED:
            self._consecutive_failures = 0
            self._consecutive_successes = 0
            self._half_open_inflight = 0

    def snapshot(self) -> dict:
        return {
            "name": self.name,
            "state": self.state.value,
            "consecutive_failures": self._consecutive_failures,
            "transitions": self.transitions,
            "rejected_calls": self.rejected_calls,
        }

    def reset(self) -> None:
        self.state = BreakerState.CLOSED
        self._consecutive_failures = 0
        self._consecutive_successes = 0
        self._half_open_inflight = 0
        self.transitions = 0
        self.rejected_calls = 0


class RetryBudget:
    """Token bucket that caps retries as a fraction of normal traffic.

    Unbounded retry is how a degraded provider becomes a dead one: every client
    triples its load exactly when the provider can least handle it. This lets
    retries happen freely when they are rare and stops them when they are not.
    """

    def __init__(self, ratio: float = 0.2, min_tokens: float = 10.0) -> None:
        self.ratio = ratio
        self.min_tokens = min_tokens
        self._tokens = min_tokens
        self.denied = 0

    def record_request(self) -> None:
        self._tokens = min(self.min_tokens * 5, self._tokens + self.ratio)

    def try_consume(self) -> bool:
        if self._tokens >= 1.0:
            self._tokens -= 1.0
            return True
        self.denied += 1
        return False

    def reset(self) -> None:
        self._tokens = self.min_tokens
        self.denied = 0
