"""
test_stress.py – Stress and performance tests.

Validates that the system handles high-volume and high-duplicate workloads
within acceptable time bounds.
"""

from __future__ import annotations

import asyncio
import time
import uuid

import httpx
import pytest

from conftest import BASE_URL, make_event, make_events


pytestmark = [pytest.mark.integration, pytest.mark.stress]

BATCH_SIZE = 50  # events per HTTP request


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

async def _publish_batch(events: list[dict]) -> httpx.Response:
    """Publish a batch of events using a per-call client."""
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=60.0) as c:
        return await c.post("/publish", json={"events": events})


async def _poll_events(
    client: httpx.AsyncClient,
    topic: str,
    *,
    expected: int,
    timeout: float = 30.0,
    interval: float = 0.5,
) -> list[dict]:
    deadline = asyncio.get_event_loop().time() + timeout
    events: list[dict] = []
    while asyncio.get_event_loop().time() < deadline:
        resp = await client.get("/events", params={"topic": topic, "limit": expected})
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


async def test_bulk_events_performance(client: httpx.AsyncClient) -> None:
    """Send 1 000 unique events in batches and verify:
    1. All are accepted without errors.
    2. All are eventually queryable.
    3. Total wall-clock time is within a reasonable bound (< 60 s).
    """
    topic = f"stress-bulk-{uuid.uuid4().hex[:8]}"
    total = 1_000
    events = make_events(total, topic=topic)

    # Chunk into batches
    batches = [events[i : i + BATCH_SIZE] for i in range(0, total, BATCH_SIZE)]

    t0 = time.monotonic()

    # Fire all batches concurrently
    results = await asyncio.gather(
        *[_publish_batch(batch) for batch in batches],
        return_exceptions=True,
    )

    for r in results:
        if isinstance(r, Exception):
            pytest.fail(f"Batch publish raised: {r}")
        assert r.status_code in (200, 201), f"Unexpected status: {r.status_code}"

    # Wait for all events to be processed
    stored = await _poll_events(client, topic, expected=total, timeout=45.0)

    elapsed = time.monotonic() - t0

    assert len(stored) == total, (
        f"Expected {total} stored events, got {len(stored)} in {elapsed:.1f}s"
    )
    assert elapsed < 60.0, (
        f"Bulk publish took {elapsed:.1f}s – exceeds 60s threshold"
    )


async def test_high_duplicate_rate(client: httpx.AsyncClient) -> None:
    """Send 500 events with ~50 % duplicates (250 unique IDs, each sent
    twice).  Verify:
    - All 500 publishes are accepted.
    - Exactly 250 unique events are stored.
    - Stats reflect the correct duplicate count.
    """
    topic = f"stress-dup-{uuid.uuid4().hex[:8]}"
    unique_count = 250
    unique_events = make_events(unique_count, topic=topic)

    # Double them → 500 total publishes, 250 unique
    all_events = unique_events + unique_events

    # Capture baseline stats
    base_resp = await client.get("/stats")
    base = base_resp.json()
    base_received = base.get("received", base.get("total_received", 0))
    base_unique = base.get("unique_processed", base.get("unique", 0))
    base_dups = base.get("duplicate_dropped", base.get("duplicates", 0))

    # Chunk and send concurrently
    batches = [all_events[i : i + BATCH_SIZE] for i in range(0, len(all_events), BATCH_SIZE)]
    results = await asyncio.gather(
        *[_publish_batch(batch) for batch in batches],
        return_exceptions=True,
    )

    for r in results:
        if isinstance(r, Exception):
            pytest.fail(f"Publish raised: {r}")
        assert r.status_code in (200, 201), f"Unexpected status: {r.status_code}"

    # Verify stored events
    stored = await _poll_events(
        client, topic, expected=unique_count, timeout=30.0
    )
    assert len(stored) == unique_count, (
        f"Expected {unique_count} unique events, got {len(stored)}"
    )

    # Verify stats consistency
    await asyncio.sleep(3)
    stats_resp = await client.get("/stats")
    stats = stats_resp.json()

    received = stats.get("received", stats.get("total_received", 0)) - base_received
    unique = stats.get("unique_processed", stats.get("unique", 0)) - base_unique
    dups = stats.get("duplicate_dropped", stats.get("duplicates", 0)) - base_dups

    assert received == 500, f"Expected 500 received, got {received}"
    assert unique == unique_count, f"Expected {unique_count} unique, got {unique}"
    assert dups == unique_count, f"Expected {unique_count} duplicates, got {dups}"
