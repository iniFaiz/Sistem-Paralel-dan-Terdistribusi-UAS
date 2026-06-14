"""
FastAPI application – the public REST API for the Aggregator service.

Endpoints
---------
POST /publish        – Accept a single event or a batch.
GET  /events         – Retrieve unique, processed events (filterable by topic).
GET  /stats          – Live statistics (received / unique / duplicate / uptime).
GET  /health         – Readiness / liveness probe.

Lifecycle
---------
On startup the application:
1. Initialises the PostgreSQL pool and applies schema migrations.
2. Creates a configurable number of Redis Stream consumer workers.
3. Starts the outbox background processor.

On shutdown everything is drained gracefully.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import List, Optional, Union

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from app.config import Settings, get_settings
from app.consumer import get_redis_client, start_consumers, stop_consumers
from app.database import close_db, get_pool, init_db
from app.dedup import DedupResult, batch_insert_events, insert_event_if_unique
from app.models import (
    BatchPublishRequest,
    Event,
    EventListResponse,
    EventResponse,
    HealthResponse,
    PublishResponse,
    StatsResponse,
)
from app.outbox import start_outbox_processor, stop_outbox_processor

# ── Logging setup ───────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
    stream=sys.stdout,
)
logger = logging.getLogger("aggregator.main")

# ── Application state ──────────────────────────────────────────────────────

_settings: Settings = get_settings()
_start_time: float = 0.0


# ── Lifespan (startup / shutdown) ──────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage startup and shutdown of all subsystems."""
    global _start_time

    logger.info("=== Aggregator starting up ===")

    # 1. Database
    await init_db(_settings)

    # 2. Consumer workers (Redis → DB pipeline)
    await start_consumers(_settings)

    # 3. Outbox processor
    start_outbox_processor(_settings)

    _start_time = time.monotonic()
    logger.info("=== Aggregator ready ===")

    yield  # ← application is running

    logger.info("=== Aggregator shutting down ===")
    await stop_consumers()
    await stop_outbox_processor()
    await close_db()
    logger.info("=== Aggregator stopped ===")


# ── FastAPI app ─────────────────────────────────────────────────────────────

app = FastAPI(
    title="Pub-Sub Log Aggregator",
    version="1.0.0",
    description="High-throughput log aggregation with deduplication.",
    lifespan=lifespan,
)


# ── POST /publish ───────────────────────────────────────────────────────────

@app.post("/publish", response_model=PublishResponse)
async def publish(
    body: Union[Event, BatchPublishRequest],
) -> PublishResponse:
    """Publish a single event or a batch of events.

    Events are directly deduplicated into PostgreSQL so the API caller
    gets an immediate ``duplicates`` count.  The publisher service
    independently publishes events to the Redis Stream for the consumer
    workers, so we do **not** duplicate that here.

    Batch requests are processed **atomically** – if any event fails
    schema validation the entire batch is rejected.
    """
    events: List[Event]

    if isinstance(body, BatchPublishRequest):
        events = body.events
    else:
        events = [body]

    # ── Direct dedup into PostgreSQL ────────────────────────────────────
    if len(events) == 1:
        is_new = await insert_event_if_unique(events[0])
        result = DedupResult(
            received=1,
            inserted=1 if is_new else 0,
            duplicates=0 if is_new else 1,
        )
    else:
        result = await batch_insert_events(events)

    return PublishResponse(
        status="accepted",
        received=result.received,
        duplicates=result.duplicates,
        processed=result.inserted,
    )


# ── GET /events ─────────────────────────────────────────────────────────────

@app.get("/events", response_model=EventListResponse)
async def get_events(
    topic: Optional[str] = Query(None, description="Filter by topic."),
    limit: int = Query(100, ge=1, le=5000, description="Max results."),
    offset: int = Query(0, ge=0, description="Pagination offset."),
) -> EventListResponse:
    """Return unique processed events, optionally filtered by topic."""
    pool = get_pool()

    if topic:
        rows = await pool.fetch(
            """
            SELECT id, topic, event_id, timestamp, source, payload, processed_at
            FROM processed_events
            WHERE topic = $1
            ORDER BY processed_at DESC
            LIMIT $2 OFFSET $3;
            """,
            topic,
            limit,
            offset,
        )
    else:
        rows = await pool.fetch(
            """
            SELECT id, topic, event_id, timestamp, source, payload, processed_at
            FROM processed_events
            ORDER BY processed_at DESC
            LIMIT $1 OFFSET $2;
            """,
            limit,
            offset,
        )

    events = [
        EventResponse(
            id=r["id"],
            topic=r["topic"],
            event_id=r["event_id"],
            timestamp=r["timestamp"],
            source=r["source"],
            payload=json.loads(r["payload"]) if isinstance(r["payload"], str) else r["payload"],
            processed_at=r["processed_at"],
        )
        for r in rows
    ]

    return EventListResponse(topic=topic, count=len(events), events=events)


# ── GET /stats ──────────────────────────────────────────────────────────────

@app.get("/stats", response_model=StatsResponse)
async def get_stats() -> StatsResponse:
    """Return live aggregation statistics."""
    pool = get_pool()

    stats_row = await pool.fetchrow("SELECT * FROM stats WHERE id = 1;")
    topic_rows = await pool.fetch(
        "SELECT DISTINCT topic FROM processed_events ORDER BY topic;"
    )

    uptime = time.monotonic() - _start_time

    return StatsResponse(
        received=stats_row["received"] if stats_row else 0,
        unique_processed=stats_row["unique_processed"] if stats_row else 0,
        duplicate_dropped=stats_row["duplicate_dropped"] if stats_row else 0,
        topics=[r["topic"] for r in topic_rows],
        uptime_seconds=round(uptime, 2),
    )


# ── GET /health ─────────────────────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    """Readiness / liveness probe."""
    pg_ok = "ok"
    redis_ok = "ok"

    # Check Postgres
    try:
        pool = get_pool()
        await pool.fetchval("SELECT 1;")
    except Exception as exc:
        pg_ok = f"error: {exc}"

    # Check Redis
    try:
        redis_client: aioredis.Redis = await get_redis_client(_settings)
        await redis_client.ping()
    except Exception as exc:
        redis_ok = f"error: {exc}"

    overall = "healthy" if (pg_ok == "ok" and redis_ok == "ok") else "degraded"

    status_code = 200 if overall == "healthy" else 503
    resp = HealthResponse(status=overall, postgres=pg_ok, redis=redis_ok)

    if status_code != 200:
        return JSONResponse(content=resp.model_dump(), status_code=status_code)
    return resp
