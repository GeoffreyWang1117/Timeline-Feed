"""PostgreSQL persistence, with pgvector when it is available.

Schema notes worth stating up front:

* **Everything is an upsert.** The bus is at-least-once, so redelivery must be
  a no-op rather than a duplicate row or a constraint violation.
* **Annotations and summaries reference events by id array, never by foreign
  key.** They are opinions about facts; deleting a wrong annotation must not
  cascade into the facts it was wrong about, and a retention job that expires
  raw events must not silently delete the audit trail explaining them.
* **pgvector is optional.** If the extension is present, embeddings go in a
  ``vector`` column with a cosine index and similarity search happens in the
  database. If not, they go in ``real[]`` and search falls back to scanning in
  Python. The fallback is genuinely slower and says so — it exists so the
  system runs on a stock Postgres, not to pretend the two are equivalent.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Sequence, Tuple

from ..embedding import cosine
from ..memory import EntityMemory
from ..models import (
    Event,
    EventFeatures,
    SemanticAnnotation,
    Severity,
    Summary,
    SummaryLevel,
    SummaryStatus,
)
from .base import PersistenceSink

SCHEMA_VECTOR = """
CREATE TABLE IF NOT EXISTS pf_events (
    event_id      TEXT PRIMARY KEY,
    tenant_id     TEXT NOT NULL,
    source        TEXT NOT NULL,
    ts            DOUBLE PRECISION NOT NULL,
    actor         TEXT,
    content       TEXT NOT NULL,
    entity_ids    TEXT[] NOT NULL DEFAULT '{}',
    metadata      JSONB NOT NULL DEFAULT '{}'::jsonb,
    importance    REAL,
    risk          REAL,
    priority      SMALLINT,
    embedding     vector(%(dim)s)
);
CREATE INDEX IF NOT EXISTS pf_events_tenant_ts ON pf_events (tenant_id, ts DESC);
CREATE INDEX IF NOT EXISTS pf_events_entities ON pf_events USING GIN (entity_ids);
"""

SCHEMA_ARRAY = """
CREATE TABLE IF NOT EXISTS pf_events (
    event_id      TEXT PRIMARY KEY,
    tenant_id     TEXT NOT NULL,
    source        TEXT NOT NULL,
    ts            DOUBLE PRECISION NOT NULL,
    actor         TEXT,
    content       TEXT NOT NULL,
    entity_ids    TEXT[] NOT NULL DEFAULT '{}',
    metadata      JSONB NOT NULL DEFAULT '{}'::jsonb,
    importance    REAL,
    risk          REAL,
    priority      SMALLINT,
    embedding     REAL[]
);
CREATE INDEX IF NOT EXISTS pf_events_tenant_ts ON pf_events (tenant_id, ts DESC);
CREATE INDEX IF NOT EXISTS pf_events_entities ON pf_events USING GIN (entity_ids);
"""

SCHEMA_REST = """
CREATE TABLE IF NOT EXISTS pf_annotations (
    annotation_id    TEXT PRIMARY KEY,
    tenant_id        TEXT NOT NULL,
    summary          TEXT NOT NULL,
    category         TEXT,
    severity         TEXT,
    entities         TEXT[] NOT NULL DEFAULT '{}',
    actionability    TEXT,
    causal_links     TEXT[] NOT NULL DEFAULT '{}',
    confidence       REAL,
    source_event_ids TEXT[] NOT NULL DEFAULT '{}',
    model            TEXT,
    provider         TEXT,
    generated_at     DOUBLE PRECISION,
    tokens_used      INTEGER,
    cost_usd         DOUBLE PRECISION,
    degraded         BOOLEAN DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS pf_annotations_tenant ON pf_annotations (tenant_id, generated_at DESC);

CREATE TABLE IF NOT EXISTS pf_summaries (
    summary_id       TEXT PRIMARY KEY,
    tenant_id        TEXT NOT NULL,
    level            TEXT NOT NULL,
    body             TEXT NOT NULL,
    source_event_ids TEXT[] NOT NULL DEFAULT '{}',
    entity_ids       TEXT[] NOT NULL DEFAULT '{}',
    child_summary_ids TEXT[] NOT NULL DEFAULT '{}',
    severity         TEXT,
    confidence       REAL,
    model            TEXT,
    version          INTEGER DEFAULT 1,
    generated_at     DOUBLE PRECISION,
    status           TEXT DEFAULT 'active',
    supersedes       TEXT,
    superseded_by    TEXT
);
CREATE INDEX IF NOT EXISTS pf_summaries_entity ON pf_summaries USING GIN (entity_ids);
CREATE INDEX IF NOT EXISTS pf_summaries_status ON pf_summaries (tenant_id, level, status);

CREATE TABLE IF NOT EXISTS pf_entity_memory (
    tenant_id      TEXT NOT NULL,
    entity_id      TEXT NOT NULL,
    kind           TEXT,
    current_state  TEXT,
    severity       TEXT,
    recent_summary TEXT,
    event_count    INTEGER DEFAULT 0,
    last_update    DOUBLE PRECISION,
    PRIMARY KEY (tenant_id, entity_id)
);
"""


class PostgresSink(PersistenceSink):
    """Durable record over asyncpg."""

    def __init__(
        self,
        dsn: str = "postgresql://postgres@localhost:5432/postgres",
        embedding_dim: int = 256,
        pool: Any = None,
        min_size: int = 1,
        max_size: int = 8,
    ) -> None:
        self.dsn = dsn
        self.embedding_dim = embedding_dim
        self._pool = pool
        self._min_size = min_size
        self._max_size = max_size
        self.has_pgvector = False
        self.writes = 0

    # -- lifecycle ---------------------------------------------------------

    async def connect(self) -> "PostgresSink":
        if self._pool is None:
            try:
                import asyncpg
            except ImportError as exc:  # pragma: no cover - env dependent
                raise RuntimeError(
                    "asyncpg is required for PostgresSink: "
                    "pip install 'pulsefeed[store]'"
                ) from exc
            self._pool = await asyncpg.create_pool(
                self.dsn, min_size=self._min_size, max_size=self._max_size
            )
        await self.migrate()
        return self

    async def migrate(self) -> None:
        async with self._pool.acquire() as conn:
            try:
                await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
                self.has_pgvector = True
            except Exception:
                # Stock Postgres without the extension. Degrade rather than fail.
                self.has_pgvector = False

            schema = SCHEMA_VECTOR if self.has_pgvector else SCHEMA_ARRAY
            await conn.execute(schema % {"dim": self.embedding_dim})
            await conn.execute(SCHEMA_REST)

            if self.has_pgvector:
                # ivfflat needs a populated table to build well, so failing here
                # on an empty database is expected and harmless.
                try:
                    await conn.execute(
                        "CREATE INDEX IF NOT EXISTS pf_events_embedding "
                        "ON pf_events USING ivfflat (embedding vector_cosine_ops) "
                        "WITH (lists = 100)"
                    )
                except Exception:
                    pass

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    # -- encoding ----------------------------------------------------------

    def _encode_embedding(self, embedding: Optional[Sequence[float]]):
        if embedding is None:
            return None
        if self.has_pgvector:
            return "[" + ",".join(f"{v:.6f}" for v in embedding) + "]"
        return list(float(v) for v in embedding)

    # -- writes ------------------------------------------------------------

    async def save_event(
        self,
        event: Event,
        features: Optional[EventFeatures] = None,
        embedding: Optional[Sequence[float]] = None,
    ) -> None:
        payload = self._encode_embedding(embedding)
        cast = "::vector" if self.has_pgvector else "::real[]"
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO pf_events (
                    event_id, tenant_id, source, ts, actor, content,
                    entity_ids, metadata, importance, risk, priority, embedding
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10,$11,$12{cast})
                ON CONFLICT (event_id) DO NOTHING
                """,
                event.event_id,
                event.tenant_id,
                event.source,
                event.timestamp,
                event.actor,
                event.content,
                list(event.entity_ids),
                json.dumps(event.metadata, default=str),
                features.importance if features else None,
                features.risk if features else None,
                int(features.priority) if features else None,
                payload,
            )
        self.writes += 1

    async def save_annotation(self, annotation: SemanticAnnotation) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO pf_annotations (
                    annotation_id, tenant_id, summary, category, severity,
                    entities, actionability, causal_links, confidence,
                    source_event_ids, model, provider, generated_at,
                    tokens_used, cost_usd, degraded
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
                ON CONFLICT (annotation_id) DO NOTHING
                """,
                annotation.annotation_id,
                annotation.tenant_id,
                annotation.summary,
                annotation.category,
                annotation.severity.value,
                list(annotation.entities),
                annotation.actionability,
                list(annotation.causal_links),
                annotation.confidence,
                list(annotation.source_event_ids),
                annotation.model,
                annotation.provider,
                annotation.generated_at,
                annotation.tokens_used,
                annotation.cost_usd,
                annotation.degraded,
            )
        self.writes += 1

    async def save_summary(self, summary: Summary) -> None:
        """Upsert. Supersede flips ``status`` on a row that already exists."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO pf_summaries (
                    summary_id, tenant_id, level, body, source_event_ids,
                    entity_ids, child_summary_ids, severity, confidence, model,
                    version, generated_at, status, supersedes, superseded_by
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
                ON CONFLICT (summary_id) DO UPDATE SET
                    body = EXCLUDED.body,
                    status = EXCLUDED.status,
                    superseded_by = EXCLUDED.superseded_by,
                    version = EXCLUDED.version,
                    source_event_ids = EXCLUDED.source_event_ids
                """,
                summary.summary_id,
                summary.tenant_id,
                summary.level.value,
                summary.text,
                list(summary.source_event_ids),
                list(summary.entity_ids),
                list(summary.child_summary_ids),
                summary.severity.value,
                summary.confidence,
                summary.model,
                summary.version,
                summary.generated_at,
                summary.status.value,
                summary.supersedes,
                summary.superseded_by,
            )
        self.writes += 1

    async def save_entity(self, memory: EntityMemory) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO pf_entity_memory (
                    tenant_id, entity_id, kind, current_state, severity,
                    recent_summary, event_count, last_update
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                ON CONFLICT (tenant_id, entity_id) DO UPDATE SET
                    kind = EXCLUDED.kind,
                    current_state = EXCLUDED.current_state,
                    severity = EXCLUDED.severity,
                    recent_summary = EXCLUDED.recent_summary,
                    event_count = EXCLUDED.event_count,
                    last_update = EXCLUDED.last_update
                """,
                memory.tenant_id,
                memory.entity_id,
                memory.kind,
                memory.current_state,
                memory.severity.value,
                memory.recent_summary,
                memory.event_count,
                memory.last_update,
            )
        self.writes += 1

    # -- reads -------------------------------------------------------------

    async def load_events(self, tenant_id: str, limit: int = 100) -> List[Event]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT event_id, tenant_id, source, ts, actor, content, "
                "entity_ids, metadata FROM pf_events WHERE tenant_id = $1 "
                "ORDER BY ts DESC LIMIT $2",
                tenant_id,
                limit,
            )
        return [
            Event(
                event_id=row["event_id"],
                tenant_id=row["tenant_id"],
                source=row["source"],
                timestamp=row["ts"],
                actor=row["actor"],
                content=row["content"],
                entity_ids=list(row["entity_ids"] or []),
                metadata=json.loads(row["metadata"]) if row["metadata"] else {},
            )
            for row in rows
        ]

    async def similar_events(
        self, tenant_id: str, embedding: Sequence[float], limit: int = 10
    ) -> List[Tuple[str, float]]:
        if self.has_pgvector:
            vector = self._encode_embedding(embedding)
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT event_id, 1 - (embedding <=> $2::vector) AS similarity "
                    "FROM pf_events WHERE tenant_id = $1 AND embedding IS NOT NULL "
                    "ORDER BY embedding <=> $2::vector LIMIT $3",
                    tenant_id,
                    vector,
                    limit,
                )
            return [(row["event_id"], float(row["similarity"])) for row in rows]

        # Fallback: scan. Correct, and materially slower — do not mistake this
        # for a substitute for the index.
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT event_id, embedding FROM pf_events "
                "WHERE tenant_id = $1 AND embedding IS NOT NULL LIMIT 10000",
                tenant_id,
            )
        scored = [
            (row["event_id"], cosine(embedding, list(row["embedding"])))
            for row in rows
        ]
        scored.sort(key=lambda pair: -pair[1])
        return scored[:limit]

    async def entity_history(self, tenant_id: str, entity_id: str) -> List[Summary]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM pf_summaries WHERE tenant_id = $1 "
                "AND $2 = ANY(entity_ids) ORDER BY generated_at",
                tenant_id,
                entity_id,
            )
        return [self._row_to_summary(row) for row in rows]

    async def load_annotations(
        self, tenant_id: str, limit: int = 1000
    ) -> List[SemanticAnnotation]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM pf_annotations WHERE tenant_id = $1 "
                "ORDER BY generated_at DESC LIMIT $2",
                tenant_id,
                limit,
            )
        return [self._row_to_annotation(row) for row in rows]

    async def load_summaries(
        self, tenant_id: str, limit: int = 1000, active_only: bool = True
    ) -> List[Summary]:
        query = "SELECT * FROM pf_summaries WHERE tenant_id = $1"
        if active_only:
            query += " AND status = 'active'"
        query += " ORDER BY generated_at DESC LIMIT $2"
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, tenant_id, limit)
        return [self._row_to_summary(row) for row in rows]

    async def load_entities(
        self, tenant_id: str, limit: int = 10_000
    ) -> List[EntityMemory]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM pf_entity_memory WHERE tenant_id = $1 "
                "ORDER BY last_update DESC NULLS LAST LIMIT $2",
                tenant_id,
                limit,
            )
        restored = []
        for row in rows:
            memory = EntityMemory(
                entity_id=row["entity_id"],
                tenant_id=row["tenant_id"],
                kind=row["kind"] or "unknown",
                current_state=row["current_state"] or "unknown",
                recent_summary=row["recent_summary"] or "",
                severity=Severity(row["severity"] or "unknown"),
                event_count=row["event_count"] or 0,
                last_update=row["last_update"] or 0.0,
            )
            restored.append(memory)
        return restored

    def _row_to_annotation(self, row) -> SemanticAnnotation:
        return SemanticAnnotation(
            annotation_id=row["annotation_id"],
            tenant_id=row["tenant_id"],
            summary=row["summary"],
            source_event_ids=list(row["source_event_ids"] or []),
            category=row["category"] or "unknown",
            severity=Severity(row["severity"] or "unknown"),
            entities=list(row["entities"] or []),
            actionability=row["actionability"] or "none",
            causal_links=list(row["causal_links"] or []),
            confidence=row["confidence"] or 0.0,
            model=row["model"] or "unknown",
            provider=row["provider"] or "unknown",
            generated_at=row["generated_at"] or 0.0,
            tokens_used=row["tokens_used"] or 0,
            cost_usd=row["cost_usd"] or 0.0,
            degraded=bool(row["degraded"]),
        )

    def _row_to_summary(self, row) -> Summary:
        return Summary(
            summary_id=row["summary_id"],
            tenant_id=row["tenant_id"],
            level=SummaryLevel(row["level"]),
            text=row["body"],
            source_event_ids=list(row["source_event_ids"] or []),
            entity_ids=list(row["entity_ids"] or []),
            child_summary_ids=list(row["child_summary_ids"] or []),
            severity=Severity(row["severity"] or "unknown"),
            confidence=row["confidence"] or 0.0,
            model=row["model"] or "unknown",
            version=row["version"] or 1,
            generated_at=row["generated_at"] or 0.0,
            status=SummaryStatus(row["status"] or "active"),
            supersedes=row["supersedes"],
            superseded_by=row["superseded_by"],
        )

    async def health(self) -> Dict[str, Any]:
        try:
            async with self._pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            return {
                "healthy": True,
                "pgvector": self.has_pgvector,
                "writes": self.writes,
            }
        except Exception as exc:
            return {"healthy": False, "error": str(exc)}
