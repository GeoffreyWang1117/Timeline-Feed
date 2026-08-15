"""HTTP surface tests. Skipped entirely when FastAPI is not installed."""

from __future__ import annotations

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from pulsefeed.api import create_app  # noqa: E402
from pulsefeed.budget import TenantPlan  # noqa: E402
from pulsefeed.clock import RealClock  # noqa: E402
from pulsefeed.llm.provider import (  # noqa: E402
    MockConfig,
    MockProvider,
    ResilientProvider,
)
from pulsefeed.pipeline import PipelineConfig, PulseFeedPipeline  # noqa: E402


@pytest.fixture
def client():
    clock = RealClock()
    pipeline = PulseFeedPipeline(
        provider=ResilientProvider(
            MockProvider(MockConfig(base_latency=0.0, latency_jitter=0.0), clock=clock),
            clock=clock,
        ),
        config=PipelineConfig(worker_count=1, audit_sample_rate=0.0, tick_interval=0.05),
        clock=clock,
    )
    pipeline.register_tenant(TenantPlan.pro("default"))
    with TestClient(create_app(pipeline)) as c:
        c.pipeline = pipeline
        yield c


def post_event(client, content, source="grafana", entity="checkout"):
    response = client.post(
        "/v1/events",
        json={"source": source, "content": content, "entity_ids": [entity]},
    )
    assert response.status_code == 202
    return response.json()["event_id"]


class TestIngest:
    def test_accepts_an_event(self, client):
        body = client.post(
            "/v1/events",
            json={"source": "slack", "content": "hello", "entity_ids": ["general"]},
        ).json()
        assert body["status"] == "accepted"
        assert body["event_id"].startswith("evt_")

    def test_rejects_a_malformed_event(self, client):
        assert client.post("/v1/events", json={"content": "no source"}).status_code == 422

    def test_rejects_oversized_content(self, client):
        response = client.post(
            "/v1/events", json={"source": "slack", "content": "x" * 20_000}
        )
        assert response.status_code == 422

    def test_batch_ingest(self, client):
        response = client.post(
            "/v1/events:batch",
            json=[
                {"source": "telemetry", "content": f"cpu at {i}%", "entity_ids": ["db"]}
                for i in range(5)
            ],
        )
        assert response.status_code == 202
        assert response.json()["accepted"] == 5


class TestRead:
    def test_timeline_is_available_immediately(self, client):
        """The feed must not wait on enrichment to be readable."""
        post_event(client, "checkout latency rises to 900ms")
        client.pipeline.coalescer.tick(client.pipeline.clock.now() + 1000)
        body = client.get("/v1/timeline").json()
        assert "items" in body
        assert "degraded" in body

    def test_evidence_expands_back_to_raw_events(self, client):
        post_event(client, "connection pool saturation on db-primary")
        import asyncio

        asyncio.run(client.pipeline.flush())

        items = client.get("/v1/timeline?limit=50").json()["items"]
        assert items
        item = items[0]
        evidence = client.get(f"/v1/items/{item['item_id']}/evidence").json()
        assert len(evidence["evidence"]) == len(item["source_event_ids"])
        assert all("content" in e for e in evidence["evidence"])

    def test_unknown_item_is_404(self, client):
        assert client.get("/v1/items/itm_nope/evidence").status_code == 404

    def test_unknown_entity_is_404(self, client):
        assert client.get("/v1/entities/nope").status_code == 404

    def test_entity_exposes_state_and_history(self, client):
        post_event(client, "checkout latency rises to 900ms")
        import asyncio

        asyncio.run(client.pipeline.flush())
        body = client.get("/v1/entities/checkout").json()
        assert body["entity_id"] == "checkout"
        assert "belief_history" in body
        assert body["event_count"] >= 1

    def test_stats_are_serialisable(self, client):
        post_event(client, "something happened")
        body = client.get("/v1/stats").json()
        for key in ("stats", "scheduler", "provider", "budget", "skip_reasons"):
            assert key in body


class TestOperational:
    def test_healthz_does_not_depend_on_the_llm(self, client):
        """A feed serving ranked events with no summaries is doing its job."""
        client.pipeline.provider.providers[0].set_outage(True)
        for _ in range(10):
            client.pipeline.provider.breakers["mock"].record_failure()
        assert client.get("/healthz").json()["status"] == "ok"

    def test_readyz_reports_degradation(self, client):
        for _ in range(10):
            client.pipeline.provider.breakers["mock"].record_failure()
        body = client.get("/readyz").json()
        assert body["enrichment"] == "degraded"
        assert body["breakers"]["mock"] == "open"

    def test_metrics_endpoint_exports(self, client):
        response = client.get("/metrics")
        assert response.status_code == 200
        assert "text/plain" in response.headers["content-type"]


class TestTenantIsolation:
    def test_timelines_do_not_leak_across_tenants(self, client):
        client.post(
            "/v1/events",
            json={
                "source": "slack",
                "content": "tenant a secret plan",
                "tenant_id": "a",
                "entity_ids": ["x"],
            },
        )
        import asyncio

        asyncio.run(client.pipeline.flush())
        other = client.get("/v1/timeline?tenant_id=b&limit=50").json()
        assert other["items"] == []
