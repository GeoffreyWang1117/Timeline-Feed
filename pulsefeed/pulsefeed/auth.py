"""API-key authentication and per-tenant rate limiting.

Framework-free on purpose — both classes are plain objects with injectable
clocks, so the security behaviour is tested directly rather than through HTTP
fixtures, and the FastAPI layer stays a thin adapter.

Two documented holes this closes:

* **`tenant_id` came from the request body, unauthenticated.** Any caller could
  read or write any tenant's feed. Now, when keys are configured, the key
  *decides* the tenant: a key bound to `acme` can only act as `acme`, whatever
  the body says. Isolation moves from "please send the right tenant_id" to a
  property of the credential.
* **No rate limiting at the HTTP boundary.** The pipeline degrades gracefully
  under load, but nothing stopped one caller from consuming the whole ingest
  path. Token buckets per (tenant, operation class) now do.

Auth is **off when no keys are configured** — the open mode every quick start
and test relies on. That is a deliberate dev default, stated loudly in the docs:
anything internet-facing must set ``PULSEFEED_API_KEYS``.
"""

from __future__ import annotations

import hmac
import os
import time
from dataclasses import dataclass
from typing import Callable, Dict, Optional, Tuple

WILDCARD_TENANT = "*"


@dataclass
class Principal:
    """Who a request is acting as, after the key resolved."""

    tenant_id: str
    wildcard: bool = False  # may act for any tenant (operator keys)

    def effective_tenant(self, requested: Optional[str]) -> str:
        """The tenant this request is allowed to touch.

        A tenant-bound key *always* acts as its own tenant — a mismatching
        ``tenant_id`` in the body is overridden, not rejected, because rejecting
        would turn every misconfigured producer into an outage while overriding
        merely keeps it inside its own box. Wildcard keys pass the requested
        tenant through.
        """
        if self.wildcard:
            return requested or "default"
        return self.tenant_id


class ApiKeyRegistry:
    """Maps API keys to tenants.

    Configured from ``PULSEFEED_API_KEYS``, a comma-separated list of
    ``key:tenant`` pairs; ``key:*`` makes an operator key that may act for any
    tenant. An empty/unset variable disables authentication entirely.

    Lookups iterate every key with a constant-time comparison rather than a
    dict lookup. A dict would be O(1) — and would also make the comparison
    itself a timing side channel on key prefixes. At the number of keys this
    registry is for (tens), the scan is free.
    """

    def __init__(self, mapping: Optional[Dict[str, str]] = None) -> None:
        self._keys: Dict[str, str] = dict(mapping or {})

    @classmethod
    def from_env(cls, variable: str = "PULSEFEED_API_KEYS") -> "ApiKeyRegistry":
        raw = os.environ.get(variable, "").strip()
        mapping: Dict[str, str] = {}
        if raw:
            for pair in raw.split(","):
                pair = pair.strip()
                if not pair:
                    continue
                key, _, tenant = pair.partition(":")
                if key:
                    mapping[key] = tenant or "default"
        return cls(mapping)

    @property
    def enabled(self) -> bool:
        return bool(self._keys)

    def resolve(self, presented: Optional[str]) -> Optional[Principal]:
        """Constant-time-per-key resolution. None means unauthorised."""
        if not self.enabled:
            return Principal(tenant_id="default", wildcard=True)
        if not presented:
            return None
        for key, tenant in self._keys.items():
            if hmac.compare_digest(key, presented):
                return Principal(
                    tenant_id="default" if tenant == WILDCARD_TENANT else tenant,
                    wildcard=(tenant == WILDCARD_TENANT),
                )
        return None


@dataclass
class RateLimitConfig:
    # Writes are the expensive direction (they run the scorer and coalescer);
    # reads are dict lookups. Bursts are 2x sustained so a batch flush is not
    # punished for arriving as a batch.
    write_per_second: float = 200.0
    write_burst: float = 400.0
    read_per_second: float = 50.0
    read_burst: float = 100.0
    max_buckets: int = 10_000  # (tenant, class) pairs tracked; oldest evicted

    @classmethod
    def from_env(cls) -> "RateLimitConfig":
        cfg = cls()
        write = os.environ.get("PULSEFEED_RATE_WRITE_PER_SECOND")
        read = os.environ.get("PULSEFEED_RATE_READ_PER_SECOND")
        if write:
            cfg.write_per_second = float(write)
            cfg.write_burst = 2 * cfg.write_per_second
        if read:
            cfg.read_per_second = float(read)
            cfg.read_burst = 2 * cfg.read_per_second
        return cfg


@dataclass
class _Bucket:
    tokens: float
    updated: float


@dataclass
class RateDecision:
    allowed: bool
    retry_after: float = 0.0


class TenantRateLimiter:
    """Token buckets per (tenant, operation class).

    Same boundedness rule as everything else in this codebase: the bucket map
    itself is capped, evicting the least-recently-touched pair. A caller who
    reappears after eviction simply starts with a full bucket — a brief
    generosity, not a leak.
    """

    def __init__(
        self,
        config: Optional[RateLimitConfig] = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.config = config or RateLimitConfig()
        self._clock = clock
        self._buckets: Dict[Tuple[str, str], _Bucket] = {}
        self.denials = 0

    def _params(self, kind: str) -> Tuple[float, float]:
        if kind == "write":
            return self.config.write_per_second, self.config.write_burst
        return self.config.read_per_second, self.config.read_burst

    def check(self, tenant_id: str, kind: str, cost: float = 1.0) -> RateDecision:
        rate, burst = self._params(kind)
        if rate <= 0:
            return RateDecision(allowed=True)

        now = self._clock()
        key = (tenant_id, kind)
        bucket = self._buckets.pop(key, None)  # pop+reinsert keeps LRU order
        if bucket is None:
            bucket = _Bucket(tokens=burst, updated=now)
        else:
            bucket.tokens = min(burst, bucket.tokens + (now - bucket.updated) * rate)
            bucket.updated = now
        self._buckets[key] = bucket

        while len(self._buckets) > self.config.max_buckets:
            self._buckets.pop(next(iter(self._buckets)))

        if bucket.tokens >= cost:
            bucket.tokens -= cost
            return RateDecision(allowed=True)

        self.denials += 1
        deficit = cost - bucket.tokens
        return RateDecision(allowed=False, retry_after=deficit / rate)
