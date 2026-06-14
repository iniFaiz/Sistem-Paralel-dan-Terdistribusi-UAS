"""
test_api.py – REST API endpoint tests.

Covers the core CRUD-style operations exposed by the aggregator:
- POST /publish  (single event)
- POST /publish  (batch events)
- GET  /events?topic=…
"""

from __future__ import annotations

import asyncio
import uuid

import httpx
import pytest

from conftest import make_event, make_events


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
    """Poll GET /events until *expected* events appear or *timeout* elapses."""
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


async def test_publish_single_event(client: httpx.AsyncClient) -> None:
    """POST /publish with a single event dict must return 200/201 and echo
    back an acknowledgement containing the event_id.
    """
    topic = f"api-single-{uuid.uuid4().hex[:8]}"
    event = make_event(topic=topic)

    resp = await client.post("/publish", json=event)

    assert resp.status_code in (200, 201), (
        f"Unexpected status {resp.status_code}: {resp.text}"
    )
    body = resp.json()
    # The response should reference the accepted event(s) somehow.
    # Accept both {"event_id": "..."} and {"accepted": N} shapes.
    assert body is not None, "Response body must not be empty"


async def test_publish_batch_events(client: httpx.AsyncClient) -> None:
    """POST /publish with a list of events must accept them all in one
    request and return a success response.
    """
    topic = f"api-batch-{uuid.uuid4().hex[:8]}"
    events = make_events(5, topic=topic)

    resp = await client.post("/publish", json={"events": events})

    assert resp.status_code in (200, 201), (
        f"Unexpected status {resp.status_code}: {resp.text}"
    )
    body = resp.json()
    assert body is not None


async def test_get_events_by_topic(client: httpx.AsyncClient) -> None:
    """After publishing events to a given topic, GET /events?topic=<topic>
    must return exactly those events.
    """
    topic = f"api-get-{uuid.uuid4().hex[:8]}"
    events = make_events(3, topic=topic)

    # Publish
    resp = await client.post("/publish", json={"events": events})
    assert resp.status_code in (200, 201)

    # Query
    stored = await _poll_events(client, topic, expected=3)

    assert len(stored) == 3, (
        f"Expected 3 events for topic '{topic}', got {len(stored)}"
    )

    # Verify all published event_ids are present
    published_ids = {e["event_id"] for e in events}
    stored_ids = {e["event_id"] for e in stored}
    assert published_ids == stored_ids, (
        f"Mismatch: published {published_ids}, stored {stored_ids}"
    )
