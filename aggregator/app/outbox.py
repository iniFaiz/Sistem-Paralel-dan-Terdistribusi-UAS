"""
Outbox pattern implementation.

After an event is inserted into ``processed_events``, a corresponding
row is written to the ``outbox`` table *within the same transaction*
(see :mod:`app.dedup`).  A background coroutine periodically polls
the outbox and marks rows as processed.

This guarantees that downstream side-effects (e.g. pushing to another
stream, sending webhooks) are *eventually* executed even when the
process crashes right after the database commit.
"""

from __future__ import annotations

import asyncio
import logging
from typing import List, Optional

import asyncpg

from app.config import Settings
from app.database import get_pool

logger = logging.getLogger("aggregator.outbox")


# ── Outbox processor ───────────────────────────────────────────────────────

_FETCH_UNPROCESSED_SQL = """
SELECT id, topic, event_id, payload, created_at
FROM outbox
WHERE processed = FALSE
ORDER BY created_at
LIMIT $1
FOR UPDATE SKIP LOCKED;
"""

_MARK_PROCESSED_SQL = """
UPDATE outbox
SET processed = TRUE
WHERE id = ANY($1::int[]);
"""


async def process_outbox_batch(batch_size: int = 200) -> int:
    """Fetch up to *batch_size* unprocessed outbox rows and mark them.

    Returns the number of rows processed in this cycle.

    ``FOR UPDATE SKIP LOCKED`` ensures multiple outbox processors (if
    any) do not contend on the same rows.
    """
    pool = get_pool()

    async with pool.acquire() as conn:
        async with conn.transaction():
            rows: List[asyncpg.Record] = await conn.fetch(
                _FETCH_UNPROCESSED_SQL,
                batch_size,
            )
            if not rows:
                return 0

            ids = [r["id"] for r in rows]

            # ── Here you would perform side-effects (e.g. push to
            #    another stream, send HTTP callbacks, etc.).  For now
            #    we simply log and mark them as done.
            for row in rows:
                logger.debug(
                    "Outbox delivering topic=%s event_id=%s",
                    row["topic"],
                    row["event_id"],
                )

            await conn.execute(_MARK_PROCESSED_SQL, ids)

    logger.info("Outbox: processed %d items", len(rows))
    return len(rows)


# ── Background task ─────────────────────────────────────────────────────────

_outbox_task: Optional[asyncio.Task[None]] = None


async def _outbox_loop(settings: Settings) -> None:
    """Continuously poll the outbox table."""
    logger.info(
        "Outbox processor started  (interval=%.1fs, batch=%d)",
        settings.outbox_poll_interval,
        settings.outbox_batch_size,
    )
    while True:
        try:
            processed = await process_outbox_batch(settings.outbox_batch_size)
            # If we processed a full batch there might be more, so skip
            # the sleep and immediately loop.
            if processed < settings.outbox_batch_size:
                await asyncio.sleep(settings.outbox_poll_interval)
        except asyncio.CancelledError:
            logger.info("Outbox processor shutting down.")
            return
        except Exception:
            logger.exception("Outbox processor error – will retry.")
            await asyncio.sleep(settings.outbox_poll_interval)


def start_outbox_processor(settings: Settings) -> asyncio.Task[None]:
    """Launch the outbox polling loop as a background ``asyncio.Task``."""
    global _outbox_task
    _outbox_task = asyncio.create_task(
        _outbox_loop(settings), name="outbox-processor"
    )
    return _outbox_task


async def stop_outbox_processor() -> None:
    """Cancel the outbox background task and await its cleanup."""
    global _outbox_task
    if _outbox_task is not None and not _outbox_task.done():
        _outbox_task.cancel()
        try:
            await _outbox_task
        except asyncio.CancelledError:
            pass
        _outbox_task = None
        logger.info("Outbox processor stopped.")
