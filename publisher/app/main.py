"""
Event generator / simulator for the Pub-Sub Log Aggregator.

This is a **one-shot** process:
  1. Wait until the aggregator service is healthy.
  2. Generate EVENT_COUNT events (with ~DUPLICATE_RATE duplicates).
  3. Publish them to both:
       • the aggregator REST API  (POST /publish, batch mode)
       • a Redis Stream            (for consumer workers)
  4. Print a summary and exit.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

import httpx
import redis.asyncio as aioredis

from app import config

# ── Logging ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("publisher")


# ── Helpers ──────────────────────────────────────────────────────────────

def _iso_now() -> str:
    """Return current UTC time in ISO-8601 format."""
    return datetime.now(timezone.utc).isoformat()


def _generate_event(topic: str, source: str = "publisher-sim") -> Dict[str, Any]:
    """Create a single event payload."""
    return {
        "topic": topic,
        "event_id": str(uuid.uuid4()),
        "timestamp": _iso_now(),
        "source": source,
        "payload": {
            "message": f"Simulated event for {topic}",
            "severity": random.choice(["info", "warning", "error", "debug"]),
            "value": round(random.uniform(0, 1000), 2),
        },
    }


def _build_event_pool(
    count: int,
    duplicate_rate: float,
    topics: List[str],
) -> List[Dict[str, Any]]:
    """
    Build a list of *count* events where approximately *duplicate_rate*
    fraction are duplicates of earlier events.
    """
    unique_count = int(count * (1 - duplicate_rate))
    unique_events: List[Dict[str, Any]] = []
    all_events: List[Dict[str, Any]] = []

    logger.info(
        "Generating %d events (%d unique, ~%d duplicates)",
        count,
        unique_count,
        count - unique_count,
    )

    for _ in range(unique_count):
        topic = random.choice(topics)
        evt = _generate_event(topic)
        unique_events.append(evt)
        all_events.append(evt)

    # Fill remaining slots with duplicates chosen at random
    dup_count = count - unique_count
    for _ in range(dup_count):
        original = random.choice(unique_events)
        # Keep same event_id & topic but update timestamp to simulate re-send
        dup = {
            **original,
            "timestamp": _iso_now(),
            "source": "publisher-sim-retry",
        }
        all_events.append(dup)

    # Shuffle so duplicates are interleaved
    random.shuffle(all_events)
    return all_events


# ── Health check ─────────────────────────────────────────────────────────

async def _wait_for_aggregator(client: httpx.AsyncClient) -> None:
    """Block until the aggregator /stats endpoint responds 200."""
    logger.info("Waiting for aggregator to become healthy at %s …", config.HEALTH_URL)
    deadline = time.monotonic() + config.HEALTH_TIMEOUT
    backoff = 1.0

    while time.monotonic() < deadline:
        try:
            resp = await client.get(config.HEALTH_URL, timeout=5.0)
            if resp.status_code == 200:
                logger.info("Aggregator is healthy ✓")
                return
            logger.warning("Aggregator returned %d – retrying …", resp.status_code)
        except (httpx.ConnectError, httpx.TimeoutException, OSError) as exc:
            logger.warning("Aggregator not ready (%s) – retrying in %.0fs …", exc, backoff)

        await asyncio.sleep(backoff)
        backoff = min(backoff * 1.5, config.MAX_BACKOFF)

    logger.error("Aggregator did not become healthy within %ds", config.HEALTH_TIMEOUT)
    sys.exit(1)


# ── Publishing helpers ───────────────────────────────────────────────────

async def _publish_batch_http(
    client: httpx.AsyncClient,
    batch: List[Dict[str, Any]],
    semaphore: asyncio.Semaphore,
    stats: Dict[str, int],
) -> None:
    """
    POST a batch of events to the aggregator with exponential back-off.
    """
    payload = {"events": batch}
    backoff = config.INITIAL_BACKOFF

    for attempt in range(1, config.MAX_RETRIES + 1):
        async with semaphore:
            try:
                resp = await client.post(
                    config.TARGET_URL,
                    json=payload,
                    timeout=30.0,
                )
                if resp.status_code in (200, 201, 207):
                    stats["http_ok"] += len(batch)
                    return
                logger.warning(
                    "HTTP %d on attempt %d for batch of %d events",
                    resp.status_code,
                    attempt,
                    len(batch),
                )
            except (httpx.ConnectError, httpx.TimeoutException, OSError) as exc:
                logger.warning(
                    "HTTP error on attempt %d: %s – retrying in %.1fs",
                    attempt,
                    exc,
                    backoff,
                )

        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, config.MAX_BACKOFF)

    stats["http_fail"] += len(batch)
    logger.error("Gave up on batch of %d events after %d attempts", len(batch), config.MAX_RETRIES)


async def _publish_to_redis(
    rds: aioredis.Redis,
    events: List[Dict[str, Any]],
) -> int:
    """Publish every event to the Redis Stream. Returns count published."""
    published = 0
    for evt in events:
        try:
            await rds.xadd(
                config.STREAM_NAME,
                {"data": json.dumps(evt)},
            )
            published += 1
        except Exception as exc:
            logger.warning("Redis publish failed for event %s: %s", evt.get("event_id"), exc)
    return published


# ── Main orchestrator ────────────────────────────────────────────────────

async def run() -> None:  # noqa: C901
    """Entry-point: generate events, publish, report."""
    logger.info("=" * 60)
    logger.info("  Pub-Sub Log Aggregator – Publisher Simulator")
    logger.info("=" * 60)
    logger.info(
        "Config → events=%d  dup_rate=%.0f%%  batch=%d  topics=%s",
        config.EVENT_COUNT,
        config.DUPLICATE_RATE * 100,
        config.BATCH_SIZE,
        config.TOPICS,
    )

    # ── Build event pool ─────────────────────────────────────────────
    events = _build_event_pool(config.EVENT_COUNT, config.DUPLICATE_RATE, config.TOPICS)
    unique_ids = {(e["topic"], e["event_id"]) for e in events}
    total_events = len(events)
    unique_count = len(unique_ids)
    duplicate_count = total_events - unique_count

    logger.info(
        "Pool ready: %d total  |  %d unique  |  %d duplicates (%.1f%%)",
        total_events,
        unique_count,
        duplicate_count,
        (duplicate_count / total_events) * 100 if total_events else 0,
    )

    # ── Connections ──────────────────────────────────────────────────
    rds = aioredis.from_url(config.BROKER_URL, decode_responses=True)
    semaphore = asyncio.Semaphore(config.HTTP_CONCURRENCY)
    stats: Dict[str, int] = {"http_ok": 0, "http_fail": 0, "redis": 0}

    async with httpx.AsyncClient() as client:
        # Wait for aggregator
        await _wait_for_aggregator(client)

        t_start = time.perf_counter()

        # ── Publish to Redis Stream (sequential, fast) ───────────
        logger.info("Publishing %d events to Redis Stream '%s' …", total_events, config.STREAM_NAME)
        stats["redis"] = await _publish_to_redis(rds, events)
        logger.info("Redis Stream: %d events published", stats["redis"])

        # ── Publish to aggregator API (concurrent batches) ───────
        batches: List[List[Dict[str, Any]]] = [
            events[i : i + config.BATCH_SIZE]
            for i in range(0, total_events, config.BATCH_SIZE)
        ]
        logger.info(
            "Sending %d batches (size %d) to %s …",
            len(batches),
            config.BATCH_SIZE,
            config.TARGET_URL,
        )

        tasks = [
            _publish_batch_http(client, batch, semaphore, stats)
            for batch in batches
        ]
        await asyncio.gather(*tasks)

        elapsed = time.perf_counter() - t_start

    await rds.aclose()

    # ── Summary ──────────────────────────────────────────────────────
    throughput = total_events / elapsed if elapsed > 0 else 0
    logger.info("=" * 60)
    logger.info("  PUBLISHER SUMMARY")
    logger.info("=" * 60)
    logger.info("  Total events generated : %d", total_events)
    logger.info("  Unique event IDs       : %d", unique_count)
    logger.info("  Duplicate events       : %d (%.1f%%)", duplicate_count,
                (duplicate_count / total_events) * 100 if total_events else 0)
    logger.info("  HTTP successful        : %d", stats["http_ok"])
    logger.info("  HTTP failed            : %d", stats["http_fail"])
    logger.info("  Redis published        : %d", stats["redis"])
    logger.info("  Elapsed time           : %.2f s", elapsed)
    logger.info("  Throughput             : %.0f events/sec", throughput)
    logger.info("=" * 60)


# ── Entrypoint ───────────────────────────────────────────────────────────

def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("Publisher interrupted – shutting down.")
    except Exception:
        logger.exception("Publisher crashed")
        sys.exit(1)


if __name__ == "__main__":
    main()
