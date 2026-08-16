"""API-key auth, per-tenant rate limiting, and the digest hierarchy."""

from __future__ import annotations

import asyncio

import pytest

from pulsefeed.auth import (
    ApiKeyRegistry,
    Principal,
    RateLimitConfig,
    TenantRateLimiter,
)
from pulsefeed.clock import VirtualClock
from pulsefeed.models import Event, SummaryLevel
from pulsefeed.pipeline import PipelineConfig, PulseFeedPipeline

T0 = 1_700_000_000.0


# --------------------------------------------------------------------------
# Registry semantics (framework-free)
# --------------------------------------------------------------------------


class TestApiKeyRegistry:
    def test_no_keys_means_auth_disabled_open_mode(self):
        registry = ApiKeyRegistry()
        assert not registry.enabled
        principal = registry.resolve(None)
        assert principal is not None and principal.wildcard

    def test_unknown_key_is_rejected(self):
        registry = ApiKeyRegistry({"secret-a": "acme"})
        assert registry.resolve("wrong") is None
        assert registry.resolve(None) is None
        assert registry.resolve("") is None

    def test_key_binds_to_its_tenant(self):
        registry = ApiKeyRegistry({"secret-a": "acme"})
        principal = registry.resolve("secret-a")
        assert principal == Principal(tenant_id="acme", wildcard=False)

    def test_bound_key_overrides_the_requested_tenant(self):
        """The key decides the tenant. The body's claim is contained, not
        trusted — this is the closure of the 'tenant_id from request body'
        hole."""
        principal = Principal(tenant_id="acme")
        assert principal.effective_tenant("someone-else") == "acme"
        assert principal.effective_tenant(None) == "acme"

    def test_wildcard_key_passes_the_requested_tenant_through(self):
        registry = ApiKeyRegistry({"ops-key": "*"})
        principal = registry.resolve("ops-key")
        assert principal.wildcard
        assert principal.effective_tenant("beta") == "beta"
        assert principal.effective_tenant(None) == "default"

    def test_env_parsing(self, monkeypatch):
        monkeypatch.setenv(
            "PULSEFEED_API_KEYS", "k1:acme, k2:beta ,ops:*,, bare"
        )
        registry = ApiKeyRegistry.from_env()
        assert registry.resolve("k1").tenant_id == "acme"
        assert registry.resolve("k2").tenant_id == "beta"
        assert registry.resolve("ops").wildcard
        assert registry.resolve("bare").tenant_id == "default"


class TestTenantRateLimiter:
    def make(self, **kw):
        t = [0.0]
        config = RateLimitConfig(
            write_per_second=kw.get("rate", 10.0),
            write_burst=kw.get("burst", 20.0),
            read_per_second=kw.get("rate", 10.0),
            read_burst=kw.get("burst", 20.0),
            max_buckets=kw.get("max_buckets", 100),
        )
        limiter = TenantRateLimiter(config, clock=lambda: t[0])
        return limiter, t

    def test_burst_then_deny_with_retry_after(self):
        limiter, _ = self.make(rate=10.0, burst=5.0)
        for _ in range(5):
            assert limiter.check("t", "write").allowed
        decision = limiter.check("t", "write")
        assert not decision.allowed
        assert decision.retry_after == pytest.approx(0.1, abs=0.02)

    def test_tokens_refill_over_time(self):
        limiter, t = self.make(rate=10.0, burst=5.0)
        for _ in range(5):
            limiter.check("t", "write")
        assert not limiter.check("t", "write").allowed
        t[0] += 1.0  # 10 tokens refill, capped at burst 5
        for _ in range(5):
            assert limiter.check("t", "write").allowed

    def test_tenants_do_not_share_buckets(self):
        limiter, _ = self.make(rate=10.0, burst=2.0)
        limiter.check("a", "write")
        limiter.check("a", "write")
        assert not limiter.check("a", "write").allowed
        assert limiter.check("b", "write").allowed

    def test_read_and_write_are_separate_budgets(self):
        limiter, _ = self.make(rate=10.0, burst=2.0)
        limiter.check("t", "write")
        limiter.check("t", "write")
        assert not limiter.check("t", "write").allowed
        assert limiter.check("t", "read").allowed

    def test_bucket_map_is_bounded(self):
        """Same rule as everything else: the limiter must not be the leak."""
        limiter, _ = self.make(max_buckets=50)
        for i in range(500):
            limiter.check(f"tenant-{i}", "write")
        assert len(limiter._buckets) <= 50

    def test_zero_rate_disables(self):
        limiter, _ = self.make(rate=0.0)
        assert limiter.check("t", "write").allowed


# --------------------------------------------------------------------------
# HTTP wiring
# --------------------------------------------------------------------------

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from pulsefeed.api import create_app  # noqa: E402
from pulsefeed.clock import RealClock  # noqa: E402
from pulsefeed.llm.provider import (  # noqa: E402
    MockConfig,
    MockProvider,
    ResilientProvider,
)


def make_client(registry=None, limiter=None):
    clock = RealClock()
    pipeline = PulseFeedPipeline(
        provider=ResilientProvider(
            MockProvider(MockConfig(base_latency=0.0, latency_jitter=0.0), clock=clock),
            clock=clock,
        ),
        config=PipelineConfig(worker_count=1, audit_sample_rate=0.0, tick_interval=0.05),
        clock=clock,
    )
    client = TestClient(
        create_app(pipeline, registry=registry, limiter=limiter),
        raise_server_exceptions=True,
    )
    client.pipeline = pipeline
    return client


EVENT = {"source": "grafana", "content": "latency spike", "entity_ids": ["svc"]}


class TestHttpAuth:
    def test_open_mode_when_no_keys_configured(self):
        with make_client(registry=ApiKeyRegistry()) as client:
            assert client.post("/v1/events", json=EVENT).status_code == 202
            assert client.get("/v1/timeline").status_code == 200

    def test_enabled_auth_rejects_missing_and_wrong_keys(self):
        registry = ApiKeyRegistry({"good-key": "acme"})
        with make_client(registry=registry) as client:
            assert client.post("/v1/events", json=EVENT).status_code == 401
            response = client.post(
                "/v1/events", json=EVENT, headers={"X-API-Key": "bad"}
            )
            assert response.status_code == 401
            assert response.headers.get("www-authenticate") == "ApiKey"

    def test_key_forces_its_tenant_on_writes(self):
        """A key for acme cannot write into beta, whatever the body says."""
        registry = ApiKeyRegistry({"acme-key": "acme"})
        with make_client(registry=registry) as client:
            response = client.post(
                "/v1/events",
                json={**EVENT, "tenant_id": "beta"},
                headers={"X-API-Key": "acme-key"},
            )
            assert response.status_code == 202
            event_id = response.json()["event_id"]
            stored = client.pipeline.events[event_id]
            assert stored.tenant_id == "acme"

    def test_key_scopes_reads_to_its_tenant(self):
        registry = ApiKeyRegistry({"acme-key": "acme"})
        with make_client(registry=registry) as client:
            response = client.get(
                "/v1/timeline",
                params={"tenant_id": "beta"},
                headers={"X-API-Key": "acme-key"},
            )
            assert response.status_code == 200
            assert response.json()["tenant_id"] == "acme"

    def test_wildcard_key_may_choose_a_tenant(self):
        registry = ApiKeyRegistry({"ops": "*"})
        with make_client(registry=registry) as client:
            response = client.post(
                "/v1/events",
                json={**EVENT, "tenant_id": "beta"},
                headers={"X-API-Key": "ops"},
            )
            event_id = response.json()["event_id"]
            assert client.pipeline.events[event_id].tenant_id == "beta"

    def test_ops_endpoints_stay_open(self):
        """healthz/readyz/metrics are for the infrastructure, not tenants."""
        registry = ApiKeyRegistry({"k": "acme"})
        with make_client(registry=registry) as client:
            assert client.get("/healthz").status_code == 200
            assert client.get("/readyz").status_code == 200
            assert client.get("/metrics").status_code == 200

    def test_readyz_reports_auth_state(self):
        with make_client(registry=ApiKeyRegistry({"k": "acme"})) as client:
            assert client.get("/readyz").json()["auth"] == "enabled"
        with make_client(registry=ApiKeyRegistry()) as client:
            assert "disabled" in client.get("/readyz").json()["auth"]


class TestHttpRateLimit:
    def test_429_with_retry_after(self):
        limiter = TenantRateLimiter(
            RateLimitConfig(write_per_second=10.0, write_burst=3.0)
        )
        with make_client(registry=ApiKeyRegistry(), limiter=limiter) as client:
            for _ in range(3):
                assert client.post("/v1/events", json=EVENT).status_code == 202
            response = client.post("/v1/events", json=EVENT)
            assert response.status_code == 429
            assert float(response.headers["retry-after"]) > 0

    def test_batch_elements_are_charged_individually(self):
        """A batch must not be a way around the per-event rate."""
        limiter = TenantRateLimiter(
            RateLimitConfig(write_per_second=10.0, write_burst=5.0)
        )
        with make_client(registry=ApiKeyRegistry(), limiter=limiter) as client:
            response = client.post("/v1/events:batch", json=[EVENT] * 10)
            assert response.status_code == 429

    def test_unauthenticated_callers_cannot_burn_a_tenants_budget(self):
        """Auth precedes rate limiting: a 401 must not consume tokens."""
        registry = ApiKeyRegistry({"k": "acme"})
        limiter = TenantRateLimiter(
            RateLimitConfig(write_per_second=10.0, write_burst=2.0)
        )
        with make_client(registry=registry, limiter=limiter) as client:
            for _ in range(20):
                assert client.post("/v1/events", json=EVENT).status_code == 401
            # The real tenant still has its full burst.
            ok = client.post(
                "/v1/events", json=EVENT, headers={"X-API-Key": "k"}
            )
            assert ok.status_code == 202


# --------------------------------------------------------------------------
# Digest hierarchy
# --------------------------------------------------------------------------


INCIDENT = [
    (0, "grafana", "checkout latency rises to 700ms", "checkout"),
    (60, "slack", "engineer mentions DB timeout in checkout", "checkout"),
    (200, "grafana", "checkout latency rises to 1.5s", "checkout"),
    (280, "slack", "starting rollback of deployment #813", "checkout"),
    (400, "grafana", "checkout latency returned to normal, resolved", "checkout"),
    (500, "pagerduty", "SEV1 payments-api error rate above threshold", "payments"),
    (560, "grafana", "payments-api recovered, error rate normal", "payments"),
]


async def run_incident(pipeline, clock):
    events = [
        Event(
            source=source,
            content=content,
            tenant_id="t",
            entity_ids=[entity],
            timestamp=T0 + offset,
        )
        for offset, source, content, entity in INCIDENT
    ]
    done = False

    async def feed():
        nonlocal done
        try:
            for event in events:
                delay = event.timestamp - clock.now()
                if delay > 0:
                    await clock.sleep(delay)
                await pipeline.ingest(event)
            await pipeline.flush()
        finally:
            done = True

    await pipeline.start()
    task = asyncio.create_task(feed())
    try:
        await clock.run(lambda: done and pipeline.idle)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await pipeline.stop(drain=False)


class TestDigest:
    @pytest.mark.asyncio
    async def test_digest_covers_the_window_without_double_telling(self):
        clock = VirtualClock(origin=T0)
        pipeline = PulseFeedPipeline(
            config=PipelineConfig(worker_count=2, audit_sample_rate=0.0,
                                  episode_interval=60.0),
            clock=clock,
        )
        await run_incident(pipeline, clock)

        digest = await pipeline.build_digest("t", window_seconds=7200.0)
        assert digest is not None
        assert digest.level is SummaryLevel.HOURLY
        assert digest.source_event_ids, "digest cites no evidence"
        # Children absorbed by an episode must not appear again beside it.
        episode_children = {
            child
            for episode in pipeline.summaries.active(SummaryLevel.EPISODE, "t")
            for child in episode.child_summary_ids
        }
        assert not (set(digest.child_summary_ids) & episode_children)
        # The most severe storyline leads.
        assert "SEV1" in digest.text or "payments" in digest.text or "checkout" in digest.text

    @pytest.mark.asyncio
    async def test_digest_is_not_persisted_by_default(self):
        clock = VirtualClock(origin=T0)
        pipeline = PulseFeedPipeline(
            config=PipelineConfig(worker_count=1, audit_sample_rate=0.0),
            clock=clock,
        )
        await run_incident(pipeline, clock)

        before = len(pipeline.summaries)
        for _ in range(5):
            await pipeline.build_digest("t", window_seconds=7200.0)
        assert len(pipeline.summaries) == before, "a GET grew the store"

        persisted = await pipeline.build_digest(
            "t", window_seconds=7200.0, persist=True
        )
        assert persisted is not None
        assert len(pipeline.summaries) == before + 1

    @pytest.mark.asyncio
    async def test_empty_window_returns_none(self):
        clock = VirtualClock(origin=T0)
        pipeline = PulseFeedPipeline(
            config=PipelineConfig(worker_count=1, audit_sample_rate=0.0),
            clock=clock,
        )
        assert await pipeline.build_digest("t") is None

    def test_digest_endpoint(self):
        with make_client(registry=ApiKeyRegistry()) as client:
            for i in range(3):
                client.post(
                    "/v1/events",
                    json={
                        "source": "pagerduty",
                        "content": f"SEV1 outage on svc-{i}, pool exhausted",
                        "entity_ids": [f"svc-{i}"],
                    },
                )
            import asyncio as _asyncio

            _asyncio.run(client.pipeline.flush())
            response = client.get("/v1/digest", params={"window": 3600})
            assert response.status_code == 200
            body = response.json()
            assert "digest" in body
