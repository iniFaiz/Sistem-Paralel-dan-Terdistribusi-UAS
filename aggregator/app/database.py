"""
PostgreSQL connection pool and schema management.

Uses *asyncpg* for high-performance async access.
All schema objects are created idempotently on startup so the service
can be restarted safely against an existing database.
"""

from __future__ import annotations

import logging
from typing import Optional

import asyncpg

from app.config import Settings

logger = logging.getLogger("aggregator.database")

# Module-level pool reference (set during init, cleared on shutdown).
_pool: Optional[asyncpg.Pool] = None


# ── SQL DDL ─────────────────────────────────────────────────────────────────

_SCHEMA_SQL = """
-- Processed events with deduplication constraint
CREATE TABLE IF NOT EXISTS processed_events (
    id          SERIAL          PRIMARY KEY,
    topic       TEXT            NOT NULL,
    event_id    TEXT            NOT NULL,
    timestamp   TIMESTAMPTZ     NOT NULL,
    source      TEXT            NOT NULL,
    payload     JSONB           NOT NULL DEFAULT '{}'::jsonb,
    processed_at TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    UNIQUE (topic, event_id)
);

-- Index for fast topic-based queries
CREATE INDEX IF NOT EXISTS idx_processed_events_topic
    ON processed_events (topic, processed_at DESC);

-- Outbox table for reliable downstream delivery
CREATE TABLE IF NOT EXISTS outbox (
    id          SERIAL          PRIMARY KEY,
    topic       TEXT            NOT NULL,
    event_id    TEXT            NOT NULL,
    payload     JSONB           NOT NULL DEFAULT '{}'::jsonb,
    processed   BOOLEAN         NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_outbox_unprocessed
    ON outbox (processed, created_at)
    WHERE processed = FALSE;

-- Singleton stats row – enforced by CHECK constraint
CREATE TABLE IF NOT EXISTS stats (
    id                  INTEGER     PRIMARY KEY DEFAULT 1,
    received            BIGINT      NOT NULL DEFAULT 0,
    unique_processed    BIGINT      NOT NULL DEFAULT 0,
    duplicate_dropped   BIGINT      NOT NULL DEFAULT 0,
    CHECK (id = 1)
);

-- Seed the singleton row if it doesn't already exist.
INSERT INTO stats (id) VALUES (1) ON CONFLICT (id) DO NOTHING;
"""


# ── Pool lifecycle ──────────────────────────────────────────────────────────


async def init_db(settings: Settings) -> asyncpg.Pool:
    """Create the connection pool and apply the schema.

    The pool is stored in a module-level variable so it can be imported
    anywhere in the application via :func:`get_pool`.
    """
    global _pool

    logger.info("Connecting to PostgreSQL at %s …", settings.database_url)

    _pool = await asyncpg.create_pool(
        dsn=settings.database_url,
        min_size=settings.db_min_pool_size,
        max_size=settings.db_max_pool_size,
        command_timeout=30,
        # READ COMMITTED is the asyncpg/PostgreSQL default, but let's be
        # explicit so it is obvious during code review.
        server_settings={"default_transaction_isolation": "read committed"},
    )

    # Apply schema idempotently.
    async with _pool.acquire() as conn:
        await conn.execute(_SCHEMA_SQL)

    logger.info(
        "Database pool ready  (min=%d, max=%d)",
        settings.db_min_pool_size,
        settings.db_max_pool_size,
    )
    return _pool


async def close_db() -> None:
    """Drain and close the connection pool."""
    global _pool
    if _pool is not None:
        await _pool.close()
        logger.info("Database pool closed.")
        _pool = None


def get_pool() -> asyncpg.Pool:
    """Return the active connection pool.

    Raises:
        RuntimeError: If the pool has not been initialised yet.
    """
    if _pool is None:
        raise RuntimeError("Database pool is not initialised – call init_db() first.")
    return _pool
