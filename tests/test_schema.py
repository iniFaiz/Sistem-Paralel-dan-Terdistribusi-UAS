"""
test_schema.py – Event schema validation tests.

Verifies that the POST /publish endpoint enforces the expected JSON schema:
required fields (topic, event_id, timestamp, source, payload) and correct
timestamp format.
"""

from __future__ import annotations

import uuid

import httpx
import pytest

from conftest import make_event


pytestmark = pytest.mark.integration


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #


async def test_valid_event_schema(client: httpx.AsyncClient) -> None:
    """A well-formed event must be accepted with HTTP 200/201."""
    event = make_event(topic=f"schema-valid-{uuid.uuid4().hex[:8]}")

    resp = await client.post("/publish", json=event)

    assert resp.status_code in (200, 201), (
        f"Expected 200/201 for a valid event, got {resp.status_code}: {resp.text}"
    )


async def test_invalid_event_missing_fields(client: httpx.AsyncClient) -> None:
    """An event missing required fields must be rejected with HTTP 4xx.

    We omit *topic* and *event_id* – both are mandatory.
    """
    incomplete_event = {
        "source": "test-suite",
        "payload": {"info": "incomplete"},
        # missing: topic, event_id, timestamp
    }

    resp = await client.post("/publish", json=incomplete_event)

    assert 400 <= resp.status_code < 500, (
        f"Expected 4xx for missing required fields, got {resp.status_code}: {resp.text}"
    )


async def test_invalid_event_bad_timestamp(client: httpx.AsyncClient) -> None:
    """An event whose *timestamp* is not a valid ISO-8601 string must be
    rejected with HTTP 4xx.
    """
    bad_event = make_event(
        topic=f"schema-bad-ts-{uuid.uuid4().hex[:8]}",
        timestamp="not-a-timestamp",
    )

    resp = await client.post("/publish", json=bad_event)

    assert 400 <= resp.status_code < 500, (
        f"Expected 4xx for bad timestamp, got {resp.status_code}: {resp.text}"
    )
