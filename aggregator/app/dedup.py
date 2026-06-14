"""
Deduplication logic for the Aggregator service.

Uses PostgreSQL's ``INSERT … ON CONFLICT DO NOTHING`` on the
``(topic, event_id)`` unique constraint to guarantee exactly-once
processing semantics *per event*.

Every call runs inside a single transaction that:
1. Attempts the insert (dedup).
2. Writes to the *outbox* table when the event is new.
3. Updates the *stats* singleton row atomically.

This ensures the three tables are always consistent – even if the
process crashes mid-way, nothing is half-committed.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import List

import asyncpg

from app.database import get_pool
from app.models import Event

logger = logging.getLogger("aggregator.dedup")


@dataclass(slots=True)
class DedupResult:
    """Outcome of a deduplication attempt."""

    received: int = 0
    inserted: int = 0
    duplicates: int = 0


# ── Single-event dedup ─────────────────────────────────────────────────────

_INSERT_EVENT_SQL = """
INSERT INTO processed_events (topic, event_id, timestamp, source, payload)
VALUES ($1, $2, $3, $4, $5::jsonb)
ON CONFLICT (topic, event_id) DO NOTHING
RETURNING id;
"""

_INSERT_OUTBOX_SQL = """
INSERT INTO outbox (topic, event_id, payload)
VALUES ($1, $2, $3::jsonb);
"""

_UPDATE_STATS_NEW_SQL = """
UPDATE stats
SET received            = received + 1,
    unique_processed    = unique_processed + 1
WHERE id = 1;
"""

_UPDATE_STATS_DUP_SQL = """
UPDATE stats
SET received            = received + 1,
    duplicate_dropped   = duplicate_dropped + 1
WHERE id = 1;
"""


async def insert_event_if_unique(event: Event) -> bool:
    """Try to insert *event*; return ``True`` if it was new.

    The insert, outbox write, and stats update happen inside a single
    ``READ COMMITTED`` transaction.
    """
    pool = get_pool()
    payload_json = json.dumps(event.payload)

    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                _INSERT_EVENT_SQL,
                event.topic,
                event.event_id,
                event.timestamp,
                event.source,
                payload_json,
            )

            if row is not None:
                # New event – write to outbox + bump unique counter.
                await conn.execute(
                    _INSERT_OUTBOX_SQL,
                    event.topic,
                    event.event_id,
                    payload_json,
                )
                await conn.execute(_UPDATE_STATS_NEW_SQL)
                logger.debug(
                    "Event INSERTED  topic=%s event_id=%s",
                    event.topic,
                    event.event_id,
                )
                return True
            else:
                # Duplicate – only bump duplicate counter.
                await conn.execute(_UPDATE_STATS_DUP_SQL)
                logger.info(
                    "DUPLICATE detected – topic=%s event_id=%s (dropped)",
                    event.topic,
                    event.event_id,
                )
                return False


# ── Batch dedup (atomic) ───────────────────────────────────────────────────

async def batch_insert_events(events: List[Event]) -> DedupResult:
    """Process a list of events **atomically** (all-or-nothing).

    Every event in the batch is attempted inside a single transaction.
    Stats counters are updated with the aggregate totals at the end so
    the ``stats`` row is always consistent.
    """
    pool = get_pool()
    result = DedupResult(received=len(events))

    async with pool.acquire() as conn:
        async with conn.transaction():
            for ev in events:
                payload_json = json.dumps(ev.payload)

                row = await conn.fetchrow(
                    _INSERT_EVENT_SQL,
                    ev.topic,
                    ev.event_id,
                    ev.timestamp,
                    ev.source,
                    payload_json,
                )

                if row is not None:
                    result.inserted += 1
                    await conn.execute(
                        _INSERT_OUTBOX_SQL,
                        ev.topic,
                        ev.event_id,
                        payload_json,
                    )
                    logger.debug(
                        "Batch: INSERTED  topic=%s event_id=%s",
                        ev.topic,
                        ev.event_id,
                    )
                else:
                    result.duplicates += 1
                    logger.info(
                        "Batch: DUPLICATE detected – topic=%s event_id=%s (dropped)",
                        ev.topic,
                        ev.event_id,
                    )

            # Aggregate stats update – single UPDATE per batch keeps
            # contention on the singleton row to a minimum.
            await conn.execute(
                """
                UPDATE stats
                SET received          = received + $1,
                    unique_processed  = unique_processed + $2,
                    duplicate_dropped = duplicate_dropped + $3
                WHERE id = 1;
                """,
                result.received,
                result.inserted,
                result.duplicates,
            )

    logger.info(
        "Batch complete: received=%d inserted=%d duplicates=%d",
        result.received,
        result.inserted,
        result.duplicates,
    )
    return result
