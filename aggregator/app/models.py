"""
Pydantic models for the Aggregator service.

Defines request / response schemas used by the REST API and the
internal event-processing pipeline.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


# ── Core Event ──────────────────────────────────────────────────────────────


class Event(BaseModel):
    """A single log/event coming from a publisher."""

    topic: str = Field(
        ...,
        min_length=1,
        max_length=256,
        description="Logical topic / channel for the event.",
        examples=["sensor.temperature"],
    )
    event_id: str = Field(
        ...,
        min_length=1,
        max_length=256,
        description="Publisher-assigned unique id (UUID recommended).",
        examples=["550e8400-e29b-41d4-a716-446655440000"],
    )
    timestamp: datetime = Field(
        ...,
        description="ISO-8601 timestamp when the event was produced.",
    )
    source: str = Field(
        ...,
        min_length=1,
        max_length=256,
        description="Identifier of the producing system.",
        examples=["publisher-1"],
    )
    payload: Dict[str, Any] = Field(
        default_factory=dict,
        description="Arbitrary JSON payload.",
    )

    @field_validator("topic", "event_id", "source")
    @classmethod
    def strip_whitespace(cls, v: str) -> str:
        return v.strip()

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "topic": "sensor.temperature",
                    "event_id": "550e8400-e29b-41d4-a716-446655440000",
                    "timestamp": "2026-06-13T10:00:00Z",
                    "source": "publisher-1",
                    "payload": {"temperature": 22.5, "unit": "celsius"},
                }
            ]
        }
    }


# ── Requests ────────────────────────────────────────────────────────────────


class BatchPublishRequest(BaseModel):
    """Publish multiple events atomically."""

    events: List[Event] = Field(
        ...,
        min_length=1,
        max_length=5000,
        description="List of events to publish in a single batch.",
    )


# ── Responses ───────────────────────────────────────────────────────────────


class PublishResponse(BaseModel):
    """Response returned by POST /publish."""

    status: str = Field(
        ..., description="Overall result: 'accepted' or 'error'."
    )
    received: int = Field(
        ..., description="Total events received in this request."
    )
    duplicates: int = Field(
        ..., description="Events that were identified as duplicates."
    )
    processed: int = Field(
        ..., description="Events newly inserted (unique)."
    )


class EventResponse(BaseModel):
    """Single event returned by GET /events."""

    id: int
    topic: str
    event_id: str
    timestamp: datetime
    source: str
    payload: Dict[str, Any]
    processed_at: datetime


class EventListResponse(BaseModel):
    """Paginated list of events returned by GET /events."""

    topic: Optional[str] = None
    count: int
    events: List[EventResponse]


class StatsResponse(BaseModel):
    """Aggregator statistics returned by GET /stats."""

    received: int = Field(
        ..., description="Total events received (including duplicates)."
    )
    unique_processed: int = Field(
        ..., description="Events successfully stored (deduplicated)."
    )
    duplicate_dropped: int = Field(
        ..., description="Events dropped as duplicates."
    )
    topics: List[str] = Field(
        ..., description="Distinct topics seen so far."
    )
    uptime_seconds: float = Field(
        ..., description="Seconds since the aggregator started."
    )


class HealthResponse(BaseModel):
    """Health-check response."""

    status: str
    postgres: str
    redis: str
