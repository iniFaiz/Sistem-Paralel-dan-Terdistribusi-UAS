"""
Shared fixtures and helpers for the Pub-Sub Log Aggregator test suite.

Provides:
- HTTP client fixture targeting the aggregator API
- Sample event generators with unique IDs per test
- Database connection fixture for direct PostgreSQL verification
- Cleanup utilities to ensure test isolation
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from typing import Any, AsyncGenerator

import asyncpg
import httpx
import pytest


# ---------------------------------------------------------------------------
# Configuration – overridable via environment variables
# ---------------------------------------------------------------------------

BASE_URL: str = os.getenv("AGGREGATOR_URL", "http://localhost:8080")
PG_DSN: str = os.getenv(
    "PG_DSN",
    "postgresql://user:pass@localhost:5432/logdb",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_event(
    *,
    topic: str = "test-topic",
    event_id: str | None = None,
    source: str = "test-suite",
    payload: dict[str, Any] | None = None,
    timestamp: str | None = None,
) -> dict[str, Any]:
    """Build a single well-formed event dict with a unique *event_id*.

    Every call without an explicit *event_id* generates a new UUID so that
    tests never accidentally collide with each other.
    """
    return {
        "topic": topic,
        "event_id": event_id or str(uuid.uuid4()),
        "timestamp": timestamp or datetime.now(timezone.utc).isoformat(),
        "source": source,
        "payload": payload or {"info": "test-event"},
    }


def make_events(
    n: int,
    *,
    topic: str = "test-topic",
    source: str = "test-suite",
) -> list[dict[str, Any]]:
    """Generate *n* unique events for *topic*."""
    return [make_event(topic=topic, source=source) for _ in range(n)]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Yield an async HTTP client pointed at the aggregator service."""
    async with httpx.AsyncClient(
        base_url=BASE_URL,
        timeout=httpx.Timeout(30.0),
    ) as c:
        yield c


@pytest.fixture
async def db_pool() -> AsyncGenerator[asyncpg.Pool, None]:
    """Create and yield an asyncpg connection pool for direct DB queries."""
    pool = await asyncpg.create_pool(dsn=PG_DSN, min_size=1, max_size=5)
    assert pool is not None
    yield pool
    await pool.close()


@pytest.fixture
def unique_topic() -> str:
    """Return a random topic name to guarantee test isolation."""
    return f"test-{uuid.uuid4().hex[:12]}"


@pytest.fixture
def sample_event(unique_topic: str) -> dict[str, Any]:
    """Return a single sample event bound to the *unique_topic* fixture."""
    return make_event(topic=unique_topic)


@pytest.fixture
def sample_events(unique_topic: str) -> list[dict[str, Any]]:
    """Return a batch of 5 sample events bound to *unique_topic*."""
    return make_events(5, topic=unique_topic)


# ---------------------------------------------------------------------------
# Markers
# ---------------------------------------------------------------------------

def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers so pytest does not emit warnings."""
    config.addinivalue_line("markers", "integration: integration tests requiring running services")
    config.addinivalue_line("markers", "stress: stress / performance tests")
