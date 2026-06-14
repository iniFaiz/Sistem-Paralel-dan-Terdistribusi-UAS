"""
test_persistence.py – Data persistence tests.

Verifies that events are durably stored in PostgreSQL and that
deduplication state survives across multiple API calls (i.e. the
UNIQUE constraint is enforced at the database level, not just in memory).
"""

from __future__ import annotations

import asyncio
import uuid

import asyncpg
import httpx
import pytest

from conftest import make_event


pytestmark = pytest.mark.integration


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

async def _poll_events(
    client: httpx.AsyncClient,
    topic: str,
    *,
    expected: int,
    timeout: float = 10.0,
    interval: float = 0.3,
) -> list[dict]:
    deadline = asyncio.get_event_loop().time() + timeout
    events: list[dict] = []
    while asyncio.get_event_loop().time() < deadline:
        resp = await client.get("/events", params={"topic": topic})
        if resp.status_code == 200:
            body = resp.json()
            events = body if isinstance(body, list) else body.get("events", [])
            if len(events) >= expected:
                return events
        await asyncio.sleep(interval)
    return events


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #


async def test_events_persist_after_query(client: httpx.AsyncClient) -> None:
    """Publish events, query them via the API, then query again to confirm
    they are still present (not consumed / deleted on read).
    """
    topic = f"persist-query-{uuid.uuid4().hex[:8]}"
    events = [make_event(topic=topic) for _ in range(3)]

    resp = await client.post("/publish", json={"events": events})
    assert resp.status_code in (200, 201)

    # First read
    first_read = await _poll_events(client, topic, expected=3)
    assert len(first_read) == 3, f"First read: expected 3, got {len(first_read)}"

    # Second read – events must still be there
    second_resp = await client.get("/events", params={"topic": topic})
    assert second_resp.status_code == 200
    body = second_resp.json()
    second_read = body if isinstance(body, list) else body.get("events", [])
    assert len(second_read) == 3, (
        f"Second read: events disappeared – expected 3, got {len(second_read)}"
    )


async def test_dedup_persists(
    client: httpx.AsyncClient,
    db_pool: asyncpg.Pool,
) -> None:
    """Publish an event, verify it lands in the DB, then publish the same
    event again and confirm the row count has *not* increased.

    This proves the UNIQUE constraint is durable and not just an in-memory
    cache that could be lost on restart.
    """
    topic = f"persist-dedup-{uuid.uuid4().hex[:8]}"
    event = make_event(topic=topic)
    event_id = event["event_id"]

    # First publish
    r1 = await client.post("/publish", json=event)
    assert r1.status_code in (200, 201)

    # Wait for processing
    await _poll_events(client, topic, expected=1)

    # Direct DB check – expect exactly one row
    async with db_pool.acquire() as conn:
        count_1 = await conn.fetchval(
            "SELECT count(*) FROM processed_events WHERE topic = $1 AND event_id = $2",
            topic,
            event_id,
        )
    assert count_1 == 1, f"Expected 1 row after first publish, got {count_1}"

    # Second publish (duplicate)
    r2 = await client.post("/publish", json=event)
    assert r2.status_code in (200, 201)

    # Brief pause to let any async processing finish
    await asyncio.sleep(2)

    # DB must still contain exactly one row
    async with db_pool.acquire() as conn:
        count_2 = await conn.fetchval(
            "SELECT count(*) FROM processed_events WHERE topic = $1 AND event_id = $2",
            topic,
            event_id,
        )
    assert count_2 == 1, (
        f"Duplicate was not deduplicated – expected 1 row, got {count_2}"
    )


async def test_stats_persists(
    client: httpx.AsyncClient,
    db_pool: asyncpg.Pool,
) -> None:
    """Publish a set of events, query the stats endpoint, then query the
    database directly to verify the stats table matches exactly.
    """
    topic = f"persist-stats-{uuid.uuid4().hex[:8]}"
    events = [make_event(topic=topic) for _ in range(5)]

    # Publish 5 unique events
    r1 = await client.post("/publish", json={"events": events})
    assert r1.status_code in (200, 201)

    # Wait for processing
    await _poll_events(client, topic, expected=5)

    # Get stats from HTTP
    resp = await client.get("/stats")
    assert resp.status_code == 200
    stats_http = resp.json()

    # Get stats from DB directly
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT received, unique_processed, duplicate_dropped FROM stats WHERE id = 1"
        )

    assert row is not None
    assert row["received"] == stats_http["received"], (
        f"Expected {stats_http['received']} received in DB, got {row['received']}"
    )
    assert row["unique_processed"] == stats_http["unique_processed"], (
        f"Expected {stats_http['unique_processed']} unique in DB, got {row['unique_processed']}"
    )
    assert row["duplicate_dropped"] == stats_http["duplicate_dropped"], (
        f"Expected {stats_http['duplicate_dropped']} duplicates in DB, got {row['duplicate_dropped']}"
    )

