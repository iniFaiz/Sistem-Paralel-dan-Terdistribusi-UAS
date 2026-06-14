"""
Redis Streams consumer workers for the Aggregator service.

Each worker is an ``asyncio.Task`` that reads from a Redis Stream
using a consumer group.  Events are parsed, run through the dedup
pipeline, and *acknowledged* only after successful processing.
This gives us **at-least-once** delivery semantics.

On startup the consumer group is created idempotently (``XGROUP CREATE
… MKSTREAM``).  If it already exists the ``BUSYGROUP`` error is
silently ignored.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import redis.asyncio as aioredis

from app.config import Settings
from app.dedup import insert_event_if_unique
from app.models import Event

logger = logging.getLogger("aggregator.consumer")

# Module-level references so we can clean up on shutdown.
_redis: Optional[aioredis.Redis] = None
_worker_tasks: List[asyncio.Task[None]] = []


# ── Redis helpers ───────────────────────────────────────────────────────────


async def _get_redis(settings: Settings) -> aioredis.Redis:
    """Return (and cache) an async Redis client."""
    global _redis
    if _redis is None:
        _redis = aioredis.from_url(
            settings.broker_url,
            decode_responses=True,
            max_connections=settings.worker_count + 4,
        )
    return _redis


async def _ensure_consumer_group(
    client: aioredis.Redis,
    stream: str,
    group: str,
) -> None:
    """Create the consumer group idempotently."""
    try:
        await client.xgroup_create(
            name=stream,
            groupname=group,
            id="0",
            mkstream=True,
        )
        logger.info("Consumer group '%s' created on stream '%s'.", group, stream)
    except aioredis.ResponseError as exc:
        if "BUSYGROUP" in str(exc):
            logger.debug("Consumer group '%s' already exists.", group)
        else:
            raise


# ── Worker coroutine ────────────────────────────────────────────────────────


def _parse_event(data: Dict[str, Any]) -> Optional[Event]:
    """Parse a raw Redis Stream entry into an ``Event``."""
    try:
        # The publisher writes a single field "data" containing the
        # JSON-encoded event.  We also accept flat fields.
        if "data" in data:
            raw = json.loads(data["data"])
        else:
            raw = dict(data)
            if "payload" in raw and isinstance(raw["payload"], str):
                raw["payload"] = json.loads(raw["payload"])

        return Event(**raw)
    except Exception:
        logger.exception("Failed to parse event from stream entry: %s", data)
        return None


async def _worker(
    worker_id: int,
    settings: Settings,
) -> None:
    """Single consumer worker loop.

    Reads from the Redis Stream, deduplicates via PostgreSQL, and ACKs.
    """
    consumer_name = f"worker-{worker_id}"
    logger.info("Consumer worker %s starting …", consumer_name)

    client = await _get_redis(settings)
    stream = settings.redis_stream
    group = settings.redis_consumer_group
    batch = settings.consumer_batch_size
    block_ms = settings.consumer_block_ms

    while True:
        try:
            # ── Crash Recovery: Read pending messages first (ID "0") ─────────
            entries = await client.xreadgroup(
                groupname=group,
                consumername=consumer_name,
                streams={stream: "0"},
                count=batch,
            )

            # If no pending messages, read new messages (ID ">")
            if not entries or not entries[0][1]:
                entries = await client.xreadgroup(
                    groupname=group,
                    consumername=consumer_name,
                    streams={stream: ">"},
                    count=batch,
                    block=block_ms,
                )

            if not entries:
                continue

            for _stream_name, messages in entries:
                for msg_id, data in messages:
                    event = _parse_event(data)
                    if event is None:
                        # Malformed – acknowledge to avoid re-delivery.
                        await client.xack(stream, group, msg_id)
                        continue

                    try:
                        await insert_event_if_unique(event)
                    except Exception:
                        logger.exception(
                            "%s: DB error processing event_id=%s – will retry on next read",
                            consumer_name,
                            event.event_id,
                        )
                        # Do NOT ack – the message will be re-delivered.
                        continue

                    # Acknowledge successful processing.
                    await client.xack(stream, group, msg_id)

        except asyncio.CancelledError:
            logger.info("Consumer worker %s shutting down.", consumer_name)
            return
        except Exception:
            logger.exception(
                "Consumer worker %s encountered an error – restarting loop.",
                consumer_name,
            )
            await asyncio.sleep(1)


# ── Lifecycle ───────────────────────────────────────────────────────────────


async def start_consumers(settings: Settings) -> List[asyncio.Task[None]]:
    """Spin up *worker_count* consumer tasks and return them."""
    global _worker_tasks

    client = await _get_redis(settings)
    await _ensure_consumer_group(
        client, settings.redis_stream, settings.redis_consumer_group
    )

    _worker_tasks = [
        asyncio.create_task(
            _worker(i, settings),
            name=f"consumer-worker-{i}",
        )
        for i in range(settings.worker_count)
    ]
    logger.info("Started %d consumer workers.", settings.worker_count)
    return _worker_tasks


async def stop_consumers() -> None:
    """Cancel all worker tasks and close the Redis client."""
    global _redis, _worker_tasks

    for task in _worker_tasks:
        if not task.done():
            task.cancel()

    results = await asyncio.gather(*_worker_tasks, return_exceptions=True)
    for r in results:
        if isinstance(r, Exception) and not isinstance(r, asyncio.CancelledError):
            logger.error("Worker exited with error: %s", r)

    _worker_tasks.clear()

    if _redis is not None:
        await _redis.aclose()
        _redis = None

    logger.info("All consumer workers stopped.")


async def get_redis_client(settings: Settings) -> aioredis.Redis:
    """Public accessor – used by the API to publish into the stream."""
    return await _get_redis(settings)
