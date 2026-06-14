"""
test_concurrency.py – Concurrency and race-condition tests.

Validates that the system correctly handles concurrent writes:
- Duplicate events published simultaneously are still deduplicated.
- Many distinct events published in parallel are all persisted.
- Statistics remain consistent under concurrent load (received == unique + dups).
"""

from __future__ import annotations

import asyncio
import uuid

import httpx
import pytest

from conftest import BASE_URL, make_event, make_events


pytestmark = pytest.mark.integration

CONCURRENCY = 20  # number of parallel tasks per test


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

async def _publish_event(event: dict) -> httpx.Response:
    """Open a fresh client and publish a single event.

    Using a per-call client avoids connection-pool contention and better
    simulates independent callers racing against each other.
    """
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as c:
        return await c.post("/publish", json=event)


async def _poll_events(
    client: httpx.AsyncClient,
    topic: str,
    *,
    expected: int,
    timeout: float = 15.0,
    interval: float = 0.4,
) -> list[dict]:
    """Poll GET /events until *expected* results appear or timeout."""
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


async def test_concurrent_duplicate_publish(client: httpx.AsyncClient) -> None:
    """Fire the *same* event from N independent async tasks at the same time.
    Exactly one copy should be persisted.
    """
    topic = f"conc-dup-{uuid.uuid4().hex[:8]}"
    event = make_event(topic=topic)

    # Launch CONCURRENCY tasks simultaneously
    results = await asyncio.gather(
        *[_publish_event(event) for _ in range(CONCURRENCY)],
        return_exceptions=True,
    )

    # All publishes should succeed (duplicates are accepted, just not stored twice)
    for r in results:
        if isinstance(r, Exception):
            pytest.fail(f"Publish raised an exception: {r}")
        assert r.status_code in (200, 201, 409), f"Unexpected status: {r.status_code}"

    # Only one copy must exist
    events = await _poll_events(client, topic, expected=1)
    matching = [e for e in events if e.get("event_id") == event["event_id"]]
    assert len(matching) == 1, (
        f"Expected exactly 1 stored event under concurrent duplicate publish, "
        f"found {len(matching)}"
    )


async def test_concurrent_different_events(client: httpx.AsyncClient) -> None:
    """Publish N *distinct* events concurrently.  All should be persisted."""
    topic = f"conc-diff-{uuid.uuid4().hex[:8]}"
    events = make_events(CONCURRENCY, topic=topic)

    results = await asyncio.gather(
        *[_publish_event(e) for e in events],
        return_exceptions=True,
    )

    for r in results:
        if isinstance(r, Exception):
            pytest.fail(f"Publish raised an exception: {r}")
        assert r.status_code in (200, 201), f"Unexpected status: {r.status_code}"

    stored = await _poll_events(client, topic, expected=CONCURRENCY)
    assert len(stored) == CONCURRENCY, (
        f"Expected {CONCURRENCY} distinct events, got {len(stored)}"
    )


async def test_concurrent_stats_consistency(client: httpx.AsyncClient) -> None:
    """After a burst of mixed unique + duplicate events the invariant
    ``received == unique_processed + duplicate_dropped`` must hold.
    """
    topic = f"conc-stats-{uuid.uuid4().hex[:8]}"

    # Capture baseline stats
    baseline_resp = await client.get("/stats")
    assert baseline_resp.status_code == 200
    baseline = baseline_resp.json()
    base_received = baseline.get("received", baseline.get("total_received", 0))
    base_unique = baseline.get("unique_processed", baseline.get("unique", 0))
    base_dups = baseline.get("duplicate_dropped", baseline.get("duplicates", 0))

    # Build payload: 10 unique events, each sent twice → 20 publishes
    unique_events = make_events(10, topic=topic)
    all_publishes = unique_events + unique_events  # second copy = duplicates

    results = await asyncio.gather(
        *[_publish_event(e) for e in all_publishes],
        return_exceptions=True,
    )
    for r in results:
        if isinstance(r, Exception):
            pytest.fail(f"Publish raised: {r}")

    # Give the system a moment to finish async processing
    await asyncio.sleep(3)

    # Check stats
    stats_resp = await client.get("/stats")
    assert stats_resp.status_code == 200
    stats = stats_resp.json()

    received = stats.get("received", stats.get("total_received", 0)) - base_received
    unique = stats.get("unique_processed", stats.get("unique", 0)) - base_unique
    dups = stats.get("duplicate_dropped", stats.get("duplicates", 0)) - base_dups

    # Invariant: received = unique + duplicates
    assert received == unique + dups, (
        f"Stats inconsistency: received={received} != "
        f"unique({unique}) + duplicates({dups})"
    )
    # We expect exactly 10 unique, 10 duplicates
    assert unique == 10, f"Expected 10 unique events, got {unique}"
    assert dups == 10, f"Expected 10 duplicates, got {dups}"
