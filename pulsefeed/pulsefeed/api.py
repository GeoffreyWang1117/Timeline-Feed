"""FastAPI surface.

Deliberately small. The interesting parts of PulseFeed are the admission and
degradation policies, not the HTTP shape, so this exposes just enough to drive
a demo and to let an operator see what the policy is doing:

    POST /v1/events            ingest one event
    POST /v1/events:batch      ingest many
    GET  /v1/timeline          the ranked feed
    GET  /v1/items/{id}/evidence   raw events behind an item
    GET  /v1/entities/{id}     entity memory + belief history
    GET  /v1/stats             pipeline, queue, provider, budget snapshot
    GET  /metrics              Prometheus exposition
    GET  /healthz /readyz      liveness and readiness

Note ``/healthz`` reports healthy while the LLM provider is down. That is the
correct answer, not a lenient one: enrichment is an enhancement, and a feed
serving ranked events with no summaries is doing its job. ``/readyz`` reports
the degradation so a dashboard can show it.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

try:
    from fastapi import Body, FastAPI, HTTPException, Query, Response
    from pydantic import BaseModel, Field

    FASTAPI_AVAILABLE = True
except ImportError:  # pragma: no cover - optional dependency
    FASTAPI_AVAILABLE = False
    BaseModel = object  # type: ignore[assignment,misc]

from .budget import TenantPlan
from .clock import RealClock
from .llm.provider import (
    MockConfig,
    MockProvider,
    OpenAICompatibleProvider,
    OpenAIConfig,
    ResilientProvider,
)
from .metrics import PROMETHEUS_AVAILABLE
from .models import Event
from .pipeline import PipelineConfig, PulseFeedPipeline


def build_provider(clock) -> ResilientProvider:
    """Primary from environment, with a local mock as the last line of defence.

    The fallback chain is the deployable form of "LLM is not a single point of
    failure": hosted model first, local vLLM second, deterministic mock last.
    """
    providers = []
    api_key = os.environ.get("PULSEFEED_LLM_API_KEY", "")
    base_url = os.environ.get("PULSEFEED_LLM_BASE_URL", "")
    model = os.environ.get("PULSEFEED_LLM_MODEL", "gpt-4o-mini")

    if api_key or base_url:
        providers.append(
            OpenAICompatibleProvider(
                OpenAIConfig(
                    base_url=base_url or "https://api.openai.com/v1",
                    api_key=api_key,
                    model=model,
                )
            )
        )

    local_url = os.environ.get("PULSEFEED_LOCAL_LLM_BASE_URL", "")
    if local_url:
        local = OpenAICompatibleProvider(
            OpenAIConfig(
                base_url=local_url,
                api_key=os.environ.get("PULSEFEED_LOCAL_LLM_API_KEY", ""),
                model=os.environ.get("PULSEFEED_LOCAL_LLM_MODEL", "local-model"),
            )
        )
        local.name = "local"
        providers.append(local)

    providers.append(MockProvider(MockConfig(), clock=clock))
    return ResilientProvider(providers[0], providers[1:], clock=clock)


def build_scorer():
    """Cheap scorer, using a fitted model when one is configured.

    ``PULSEFEED_SCORER_MODEL`` points at the JSON a training run wrote. If it is
    unset — or the file is unreadable, or was fitted against a different feature
    set — the hand-tuned weights are used instead. A bad model file degrades the
    ranking; it must never stop the service from starting.
    """
    from .scoring import CheapScorer

    path = os.environ.get("PULSEFEED_SCORER_MODEL", "")
    if not path:
        return CheapScorer()
    try:
        from .learning import LogisticScoreModel

        return CheapScorer(model=LogisticScoreModel.load(path))
    except Exception:
        return CheapScorer()


def create_pipeline() -> PulseFeedPipeline:
    clock = RealClock()
    pipeline = PulseFeedPipeline(
        scorer=build_scorer(),
        provider=build_provider(clock),
        config=PipelineConfig(
            worker_count=int(os.environ.get("PULSEFEED_WORKERS", "4")),
            audit_sample_rate=float(os.environ.get("PULSEFEED_AUDIT_RATE", "0.01")),
        ),
        clock=clock,
    )
    pipeline.register_tenant(
        TenantPlan(
            tenant_id=os.environ.get("PULSEFEED_TENANT", "default"),
            daily_token_budget=int(os.environ.get("PULSEFEED_TOKEN_BUDGET", "500000")),
            daily_usd_budget=float(os.environ.get("PULSEFEED_USD_BUDGET", "5.0")),
        )
    )
    return pipeline


if FASTAPI_AVAILABLE:

    class EventIn(BaseModel):
        source: str = Field(..., max_length=64)
        content: str = Field(..., max_length=8000)
        tenant_id: str = Field("default", max_length=64)
        timestamp: Optional[float] = None
        actor: Optional[str] = Field(None, max_length=128)
        entity_ids: List[str] = Field(default_factory=list, max_length=16)
        metadata: Dict[str, Any] = Field(default_factory=dict)

        def to_event(self, clock) -> Event:
            return Event(
                source=self.source,
                content=self.content,
                tenant_id=self.tenant_id,
                timestamp=self.timestamp
                if self.timestamp is not None
                else clock.now(),
                actor=self.actor,
                entity_ids=list(self.entity_ids),
                metadata=dict(self.metadata),
            )

    def create_app(pipeline: Optional[PulseFeedPipeline] = None) -> "FastAPI":
        pipe = pipeline or create_pipeline()

        @asynccontextmanager
        async def lifespan(app: "FastAPI"):
            await pipe.start()
            try:
                yield
            finally:
                await pipe.stop(drain=True)

        app = FastAPI(
            title="PulseFeed",
            version="0.1.0",
            description="Event-triggered LLM enrichment for real-time feeds",
            lifespan=lifespan,
        )
        app.state.pipeline = pipe

        @app.post("/v1/events", status_code=202)
        async def ingest_event(event: EventIn) -> Dict[str, str]:
            """Accept an event. Returns as soon as it is scored and routed —
            never waits on the LLM."""
            e = event.to_event(pipe.clock)
            await pipe.ingest(e)
            return {"event_id": e.event_id, "status": "accepted"}

        @app.post("/v1/events:batch", status_code=202)
        async def ingest_batch(
            events: List[EventIn] = Body(..., max_length=1000)
        ) -> Dict[str, Any]:
            ids = []
            for event in events:
                e = event.to_event(pipe.clock)
                await pipe.ingest(e)
                ids.append(e.event_id)
            return {"accepted": len(ids), "event_ids": ids}

        @app.get("/v1/timeline")
        async def get_timeline(
            tenant_id: str = Query("default"),
            limit: int = Query(20, ge=1, le=200),
            ranked: bool = Query(True),
        ) -> Dict[str, Any]:
            items = pipe.timeline(tenant_id, limit=limit, ranked=ranked)
            return {
                "tenant_id": tenant_id,
                "count": len(items),
                "degraded": not pipe.provider.primary_healthy,
                "items": [i.to_dict() for i in items],
            }

        @app.get("/v1/items/{item_id}/evidence")
        async def get_evidence(
            item_id: str, tenant_id: str = Query("default")
        ) -> Dict[str, Any]:
            """Expand a summary back into the raw events it claims to describe.

            This endpoint is why hallucination is survivable: no conclusion the
            system shows is unfalsifiable.
            """
            for item in pipe.timeline(tenant_id, limit=10 ** 9):
                if item.item_id == item_id:
                    return {
                        "item": item.to_dict(),
                        "evidence": [e.to_dict() for e in pipe.evidence_for(item)],
                    }
            raise HTTPException(status_code=404, detail="item not found")

        @app.get("/v1/entities/{entity_id}")
        async def get_entity(
            entity_id: str, tenant_id: str = Query("default")
        ) -> Dict[str, Any]:
            memory = pipe.entities.get(tenant_id, entity_id)
            if memory is None:
                raise HTTPException(status_code=404, detail="entity not found")
            history = pipe.entity_history(tenant_id, entity_id)
            return {
                "entity_id": entity_id,
                "state": memory.current_state,
                "severity": memory.severity.value,
                "event_count": memory.event_count,
                "recent_summary": memory.recent_summary,
                "belief_history": [s.to_dict() for s in history],
            }

        @app.get("/v1/stats")
        async def get_stats(tenant_id: str = Query("default")) -> Dict[str, Any]:
            snapshot = pipe.snapshot()
            snapshot["budget"] = pipe.ledger.snapshot(tenant_id)
            return snapshot

        @app.get("/metrics")
        async def metrics() -> Response:
            return Response(
                content=pipe.metrics.export(),
                media_type="text/plain; version=0.0.4; charset=utf-8",
            )

        @app.get("/healthz")
        async def healthz() -> Dict[str, Any]:
            # Intentionally does not depend on the provider: ingestion and the
            # ranked feed work without it.
            return {"status": "ok", "version": "0.1.0"}

        @app.get("/readyz")
        async def readyz() -> Dict[str, Any]:
            healthy = pipe.provider.primary_healthy
            model = getattr(pipe.scorer, "model", None)
            return {
                "status": "ok",
                "enrichment": "healthy" if healthy else "degraded",
                # Which scorer is live is the first thing to check when ranking
                # looks wrong, and a silently-failed model load looks identical
                # to a deliberate hand-tuned deployment without this.
                "scorer": type(model).__name__ if model else "unknown",
                "queue_depth": pipe.scheduler.depth,
                "breakers": {
                    name: b.state.value
                    for name, b in pipe.provider.breakers.items()
                },
                "prometheus": PROMETHEUS_AVAILABLE,
            }

        return app

else:  # pragma: no cover - optional dependency

    def create_app(pipeline: Optional[PulseFeedPipeline] = None):
        raise ImportError(
            "FastAPI is not installed. Install with: pip install 'pulsefeed[api]'"
        )


app = create_app() if FASTAPI_AVAILABLE and os.environ.get("PULSEFEED_AUTOAPP") else None
