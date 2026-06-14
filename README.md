# Pub-Sub Log Aggregator

A distributed event processing system that demonstrates **publish-subscribe messaging**, **idempotent event processing**, and **deduplication** using Redis Streams and PostgreSQL.

> Built for the UAS (Ujian Akhir Semester) exam project.
>
> - **GitHub Repository**: [https://github.com/iniFaiz/Sistem-Paralel-dan-Terdistribusi-UAS](https://github.com/iniFaiz/Sistem-Paralel-dan-Terdistribusi-UAS)
> - **Video Demo**: [https://youtu.be/0tzC9IrfSoM](https://youtu.be/0tzC9IrfSoM)

---

## Architecture

```
┌────────────┐         HTTP POST /publish         ┌──────────────┐
│            │ ──────────────────────────────────▶ │              │
│  Publisher  │                                    │  Aggregator  │
│ (simulator) │ ──── Redis Stream (events) ──────▶ │  (FastAPI)   │
│            │                                    │              │
└────────────┘                                    └──────┬───────┘
                                                         │
                         ┌───────────────────────────────┤
                         │                               │
                    ┌────▼─────┐                  ┌──────▼───────┐
                    │  Redis 7  │                  │ PostgreSQL 16│
                    │ (broker)  │                  │  (storage)   │
                    │  Streams  │                  │  Dedup + OLTP│
                    └──────────┘                  └──────────────┘
```

### Data Flow

1. **Publisher** generates ~20,000 events with **~30% intentional duplicates**.
2. Events are sent via two channels:
   - **HTTP POST** to the Aggregator `/publish` endpoint (batch mode).
   - **Redis Stream** for consumer worker ingestion.
3. **Aggregator** consumer workers read from the Redis Stream.
4. All events are deduplicated using PostgreSQL `INSERT … ON CONFLICT DO NOTHING`.
5. Statistics are tracked transactionally for consistency.

### Key Design Patterns

| Pattern | Implementation |
|---|---|
| **Idempotency** | `UNIQUE(topic, event_id)` constraint in PostgreSQL |
| **Deduplication** | `INSERT … ON CONFLICT DO NOTHING` (atomic upsert) |
| **At-least-once delivery** | Redis consumer groups with `XACK` after processing |
| **Outbox pattern** | Event + outbox entry in same DB transaction |
| **Batch atomicity** | All-or-nothing batch inserts in a single transaction |
| **Crash tolerance** | Dedup state persists in PostgreSQL across restarts |

---

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) ≥ 20.10
- [Docker Compose](https://docs.docker.com/compose/install/) ≥ 2.0
- No other dependencies required (everything runs in containers)

---

## Quick Start

```bash
# Clone / navigate to project directory
cd G:\test

# Build and start all services
docker compose up --build

# Or run in detached mode
docker compose up --build -d

# View logs
docker compose logs -f

# Stop all services
docker compose down

# Stop and remove volumes (full reset)
docker compose down -v
```

The publisher will automatically:
1. Wait for the aggregator to become healthy.
2. Generate 20,000 events (~30% duplicates).
3. Publish events to both HTTP API and Redis Stream.
4. Log a summary and exit.

---

## API Endpoints

### `POST /publish`

Publish one or more events.

**Single event:**
```json
{
  "topic": "auth",
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2026-06-13T07:00:00Z",
  "source": "auth-service",
  "payload": {"action": "login", "user": "alice"}
}
```

**Batch events:**
```json
{
  "events": [
    {
      "topic": "auth",
      "event_id": "...",
      "timestamp": "...",
      "source": "auth-service",
      "payload": {}
    }
  ]
}
```

**Response (201):**
```json
{
  "status": "accepted",
  "received": 50,
  "duplicates": 12,
  "processed": 38
}
```

### `GET /events?topic=auth`

Retrieve unique processed events, optionally filtered by topic.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `topic` | string | — | Filter by topic |
| `limit` | int | 100 | Max results |
| `offset` | int | 0 | Pagination offset |

**Response (200):**
```json
{
  "topic": "auth",
  "count": 5,
  "events": [...]
}
```

### `GET /stats`

System statistics and health.

**Response (200):**
```json
{
  "received": 20000,
  "unique_processed": 14000,
  "duplicate_dropped": 6000,
  "topics": ["auth", "payment", "notification"],
  "uptime_seconds": 45.2
}
```

---

## Running Tests

```bash
# Run all tests (requires running services)
docker compose up -d storage broker aggregator
pip install -r aggregator/requirements.txt
pip install pytest pytest-asyncio httpx
pytest tests/ -v

# Run specific test modules
pytest tests/test_dedup.py -v
pytest tests/test_api.py -v
pytest tests/test_concurrency.py -v
pytest tests/test_persistence.py -v
pytest tests/test_schema.py -v
pytest tests/test_stats.py -v
```

---

## File Structure

```
G:\test\
├── aggregator/
│   ├── app/
│   │   ├── __init__.py        # Package init
│   │   ├── main.py            # FastAPI app, lifespan, endpoints
│   │   ├── models.py          # Pydantic models (Event, Stats, etc.)
│   │   ├── database.py        # PostgreSQL connection pool, schema DDL
│   │   ├── consumer.py        # Redis Stream consumer workers
│   │   ├── dedup.py           # Deduplication logic (ON CONFLICT)
│   │   ├── outbox.py          # Outbox pattern implementation
│   │   └── config.py          # Configuration from env vars
│   ├── Dockerfile             # python:3.11-slim, non-root
│   └── requirements.txt       # fastapi, uvicorn, asyncpg, redis, etc.
├── publisher/
│   ├── app/
│   │   ├── __init__.py        # Package init
│   │   ├── main.py            # Event generator / simulator
│   │   └── config.py          # Configuration from env vars
│   ├── Dockerfile             # python:3.11-slim, non-root
│   └── requirements.txt       # httpx, redis
├── tests/
│   ├── __init__.py
│   ├── conftest.py            # Shared fixtures
│   ├── test_dedup.py          # Deduplication tests
│   ├── test_api.py            # API endpoint tests
│   ├── test_concurrency.py    # Concurrent processing tests
│   ├── test_persistence.py    # Crash / restart persistence tests
│   ├── test_schema.py         # Schema validation tests
│   └── test_stats.py          # Statistics accuracy tests
├── docker-compose.yml         # Full orchestration
└── README.md                  # This file
```

---

## Design Decisions

### Why Redis Streams over Pub/Sub?

Redis Streams provide **persistent, replayable** message delivery with consumer groups, enabling at-least-once semantics. Traditional Redis Pub/Sub is fire-and-forget — messages are lost if no subscriber is listening.

### Why PostgreSQL for Deduplication?

PostgreSQL's `UNIQUE` constraints and `INSERT … ON CONFLICT DO NOTHING` provide **atomic, race-condition-free** deduplication. This is simpler and more reliable than application-level dedup with Redis `SETNX` which can suffer from TTL expiry and memory limits.

### Why Dual Publishing (HTTP + Redis Stream)?

- **HTTP API** is the primary ingestion path, demonstrating REST semantics and batch processing.
- **Redis Stream** demonstrates the pub-sub pattern with consumer groups and worker scaling.
- Both paths converge at the same PostgreSQL dedup layer, proving idempotency.

### Why READ COMMITTED Isolation?

`READ COMMITTED` with `UNIQUE` constraints is sufficient for deduplication. `SERIALIZABLE` would add unnecessary overhead. The unique constraint itself provides the atomicity guarantee we need.

---

## Performance Targets

| Metric | Target | Notes |
|--------|--------|-------|
| Total events | ≥ 20,000 | Configurable via `EVENT_COUNT` |
| Duplicate rate | ≥ 30% | Configurable via `DUPLICATE_RATE` |
| Consumer workers | 4 | Configurable via `WORKER_COUNT` |
| Processing time | < 60s | For 20k events on modern hardware |

---

## Video Demo

> 📹 **Video demo link**: [https://youtu.be/0tzC9IrfSoM](https://youtu.be/0tzC9IrfSoM)

---

## License

This project is created for academic purposes (UAS exam).
