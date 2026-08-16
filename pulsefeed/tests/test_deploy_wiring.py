"""The deployment wiring: env-configured durability in ``create_app``.

These tests exercise the exact path a real deployment uses — environment
variables, the app factory, lifespan startup — against real Redis and
Postgres. They exist because the gap they cover was real: the factory read
LLM and budget configuration from the environment but silently ignored the
durable backends, so ``docker compose up`` next to ``uvicorn`` produced a
deployment that *looked* durable and lost everything on restart.

Skipped without ``PULSEFEED_TEST_REDIS_URL`` / ``PULSEFEED_TEST_PG_DSN``.
"""

from __future__ import annotations

import os
import time
import uuid

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from pulsefeed.api import create_app  # noqa: E402

REDIS_URL = os.environ.get("PULSEFEED_TEST_REDIS_URL")
PG_DSN = os.environ.get("PULSEFEED_TEST_PG_DSN")

redis_test = pytest.mark.skipif(
    not REDIS_URL, reason="set PULSEFEED_TEST_REDIS_URL to run Redis tests"
)
pg_test = pytest.mark.skipif(
    not PG_DSN, reason="set PULSEFEED_TEST_PG_DSN to run Postgres tests"
)


def unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def wait_until(predicate, timeout: float = 10.0, interval: float = 0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def post_event(client, tenant, entity, content="checkout latency rising to 700ms"):
    response = client.post(
        "/v1/events",
        json={
            "source": "grafana",
            "content": content,
            "tenant_id": tenant,
            "entity_ids": [entity],
        },
    )
    assert response.status_code == 202, response.text
    return response.json()["event_id"]


@pg_test
class TestPostgresWiring:
    def test_env_dsn_attaches_a_real_sink_and_readyz_says_so(self, monkeypatch):
        tenant = unique("t")
        monkeypatch.setenv("PULSEFEED_PG_DSN", PG_DSN)
        monkeypatch.setenv("PULSEFEED_TENANT", tenant)
        monkeypatch.delenv("PULSEFEED_REDIS_URL", raising=False)

        with TestClient(create_app()) as client:
            ready = client.get("/readyz").json()
            assert ready["durability"]["sink"] == "postgres"
            assert ready["durability"]["bus"] == "none"
            event_id = post_event(client, tenant, unique("svc"))
            pipe = client.app.state.pipeline
            assert event_id in pipe.events
            assert pipe.sink_failures == 0
            assert pipe.sink.writes > 0

    def test_restart_restores_from_the_sink(self, monkeypatch):
        """The restart story, end to end through the factory: events written
        by one process are visible to the next one with no bus involved."""
        tenant = unique("t")
        entity = unique("svc")
        monkeypatch.setenv("PULSEFEED_PG_DSN", PG_DSN)
        monkeypatch.setenv("PULSEFEED_TENANT", tenant)
        monkeypatch.delenv("PULSEFEED_REDIS_URL", raising=False)

        with TestClient(create_app()) as client:
            ids = [post_event(client, tenant, entity, f"deploy step {i}") for i in range(5)]

        with TestClient(create_app()) as client:
            pipe = client.app.state.pipeline
            for event_id in ids:
                assert event_id in pipe.events, "event lost across restart"
            assert pipe.events[ids[0]].tenant_id == tenant

    def test_restore_can_be_disabled(self, monkeypatch):
        tenant = unique("t")
        monkeypatch.setenv("PULSEFEED_PG_DSN", PG_DSN)
        monkeypatch.setenv("PULSEFEED_TENANT", tenant)
        monkeypatch.delenv("PULSEFEED_REDIS_URL", raising=False)

        with TestClient(create_app()) as client:
            event_id = post_event(client, tenant, unique("svc"))

        monkeypatch.setenv("PULSEFEED_RESTORE", "0")
        with TestClient(create_app()) as client:
            assert event_id not in client.app.state.pipeline.events

    def test_injected_pipeline_ignores_env_backends(self, monkeypatch):
        """A caller who passes a pipeline owns its storage — the factory must
        not sneak a Postgres sink into a test or embedded deployment."""
        from pulsefeed.pipeline import PulseFeedPipeline

        monkeypatch.setenv("PULSEFEED_PG_DSN", PG_DSN)
        pipeline = PulseFeedPipeline()
        with TestClient(create_app(pipeline)) as client:
            ready = client.get("/readyz").json()
            assert ready["durability"]["sink"] == "memory"
            assert type(pipeline.sink).__name__ == "NullSink"


@redis_test
class TestRedisWiring:
    def test_posted_events_flow_through_the_bus_into_the_pipeline(self, monkeypatch):
        tenant = unique("t")
        stream = f"pulsefeed:test:{unique('s')}"
        monkeypatch.setenv("PULSEFEED_REDIS_URL", REDIS_URL)
        monkeypatch.setenv("PULSEFEED_STREAM", stream)
        monkeypatch.setenv("PULSEFEED_TENANT", tenant)
        monkeypatch.delenv("PULSEFEED_PG_DSN", raising=False)

        try:
            with TestClient(create_app()) as client:
                ready = client.get("/readyz").json()
                assert ready["durability"]["bus"] == "redis"
                event_id = post_event(client, tenant, unique("svc"))
                pipe = client.app.state.pipeline
                # 202 means "in Redis", not "ingested" — the consumer picks it
                # up asynchronously, so ingestion is eventually visible.
                assert wait_until(lambda: event_id in pipe.events), (
                    "event published to the bus was never consumed"
                )
                ready = client.get("/readyz").json()
                assert ready["durability"]["ingest"]["acked"] >= 1
        finally:
            self._delete_stream(stream)

    def test_bus_outage_fails_closed_with_503(self, monkeypatch):
        """A dead bus must refuse the event loudly. Accept-and-forget on a raw
        event is the one unrecoverable failure in the system."""
        monkeypatch.setenv("PULSEFEED_REDIS_URL", "redis://localhost:1/0")
        monkeypatch.delenv("PULSEFEED_PG_DSN", raising=False)

        with TestClient(create_app()) as client:
            response = client.post(
                "/v1/events",
                json={"source": "grafana", "content": "x", "tenant_id": "t"},
            )
            assert response.status_code == 503

    @staticmethod
    def _delete_stream(stream: str) -> None:
        import redis

        redis.from_url(REDIS_URL).delete(stream)
