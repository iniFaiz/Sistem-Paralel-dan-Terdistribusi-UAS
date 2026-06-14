"""
test_stats.py – Statistics endpoint tests.

Verifies the GET /stats endpoint returns correct counters and metadata:
- Initial state (zero counters)
- Counters after publishing events (including duplicates)
- Topic list reflects all published topics
"""

from __future__ import annotations

import asyncio
import uuid

import httpx
import pytest

from conftest import make_event, make_events


pytestmark = pytest.mark.integration


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #


async def test_initial_stats(client: httpx.AsyncClient) -> None:
    """GET /stats must return a well-formed response with the expected
    keys.  On a fresh (or near-fresh) system the counters should be ≥ 0.
    """
    resp = await client.get("/stats")

    assert resp.status_code == 200, f"Stats endpoint failed: {resp.status_code}"
    stats = resp.json()

    # At minimum the response must contain these keys (name variations accepted)
    expected_keys = {"received", "unique_processed", "duplicate_dropped"}
    alt_keys = {"total_received", "unique", "duplicates"}

    present = set(stats.keys())
    has_primary = expected_keys.issubset(present)
    has_alt = alt_keys.issubset(present)

    assert has_primary or has_alt, (
        f"Stats response missing expected keys. Got: {present}"
    )

    # Uptime should be present
    assert "uptime" in stats or "uptime_seconds" in stats, (
        "Stats response must include uptime"
    )


async def test_stats_after_publish(client: httpx.AsyncClient) -> None:
    """After publishing a known set of unique + duplicate events the
    stats counters must reflect the correct counts.
    """
    topic = f"stats-pub-{uuid.uuid4().hex[:8]}"

    # Baseline
    base_resp = await client.get("/stats")
    base = base_resp.json()
    base_received = base.get("received", base.get("total_received", 0))
    base_unique = base.get("unique_processed", base.get("unique", 0))
    base_dups = base.get("duplicate_dropped", base.get("duplicates", 0))

    # Publish 5 unique events
    events = make_events(5, topic=topic)
    r1 = await client.post("/publish", json={"events": events})
    assert r1.status_code in (200, 201)

    # Publish 3 duplicates (re-send first 3 events)
    r2 = await client.post("/publish", json={"events": events[:3]})
    assert r2.status_code in (200, 201)

    # Allow async processing
    await asyncio.sleep(3)

    # Read updated stats
    stats_resp = await client.get("/stats")
    assert stats_resp.status_code == 200
    stats = stats_resp.json()

    received = stats.get("received", stats.get("total_received", 0)) - base_received
    unique = stats.get("unique_processed", stats.get("unique", 0)) - base_unique
    dups = stats.get("duplicate_dropped", stats.get("duplicates", 0)) - base_dups

    assert received == 8, f"Expected 8 received (5+3), got {received}"
    assert unique == 5, f"Expected 5 unique, got {unique}"
    assert dups == 3, f"Expected 3 duplicates, got {dups}"


async def test_stats_topics_list(client: httpx.AsyncClient) -> None:
    """Publishing to multiple distinct topics must cause all of them to
    appear in the stats topics list.
    """
    topic_a = f"stats-topics-a-{uuid.uuid4().hex[:8]}"
    topic_b = f"stats-topics-b-{uuid.uuid4().hex[:8]}"
    topic_c = f"stats-topics-c-{uuid.uuid4().hex[:8]}"

    for t in (topic_a, topic_b, topic_c):
        resp = await client.post("/publish", json=make_event(topic=t))
        assert resp.status_code in (200, 201)

    # Wait for processing
    await asyncio.sleep(3)

    stats_resp = await client.get("/stats")
    assert stats_resp.status_code == 200
    stats = stats_resp.json()

    topics = stats.get("topics", stats.get("topics_list", []))
    assert isinstance(topics, list), f"Expected topics to be a list, got {type(topics)}"

    for t in (topic_a, topic_b, topic_c):
        assert t in topics, f"Topic '{t}' not found in stats topics: {topics}"
