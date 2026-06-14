"""
Configuration module for the Aggregator service.

Loads all settings from environment variables with sensible defaults
for Docker Compose deployment.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class Settings:
    """Immutable application settings loaded from environment variables."""

    # ── PostgreSQL ──────────────────────────────────────────────────────
    database_url: str = field(
        default_factory=lambda: os.getenv(
            "DATABASE_URL",
            "postgresql://user:pass@storage:5432/logdb",
        )
    )
    db_min_pool_size: int = field(
        default_factory=lambda: int(os.getenv("DB_MIN_POOL", "5"))
    )
    db_max_pool_size: int = field(
        default_factory=lambda: int(os.getenv("DB_MAX_POOL", "20"))
    )

    # ── Redis ───────────────────────────────────────────────────────────
    broker_url: str = field(
        default_factory=lambda: os.getenv("BROKER_URL", "redis://broker:6379")
    )
    redis_stream: str = field(
        default_factory=lambda: os.getenv("REDIS_STREAM", "events")
    )
    redis_consumer_group: str = field(
        default_factory=lambda: os.getenv("REDIS_CONSUMER_GROUP", "aggregator-group")
    )

    # ── Workers ─────────────────────────────────────────────────────────
    worker_count: int = field(
        default_factory=lambda: int(os.getenv("WORKER_COUNT", "4"))
    )
    consumer_batch_size: int = field(
        default_factory=lambda: int(os.getenv("CONSUMER_BATCH_SIZE", "100"))
    )
    consumer_block_ms: int = field(
        default_factory=lambda: int(os.getenv("CONSUMER_BLOCK_MS", "2000"))
    )

    # ── Outbox ──────────────────────────────────────────────────────────
    outbox_poll_interval: float = field(
        default_factory=lambda: float(os.getenv("OUTBOX_POLL_INTERVAL", "2.0"))
    )
    outbox_batch_size: int = field(
        default_factory=lambda: int(os.getenv("OUTBOX_BATCH_SIZE", "200"))
    )

    # ── HTTP Server ─────────────────────────────────────────────────────
    app_host: str = field(
        default_factory=lambda: os.getenv("APP_HOST", "0.0.0.0")
    )
    app_port: int = field(
        default_factory=lambda: int(os.getenv("APP_PORT", "8080"))
    )

    # ── Logging ─────────────────────────────────────────────────────────
    log_level: str = field(
        default_factory=lambda: os.getenv("LOG_LEVEL", "INFO").upper()
    )


def get_settings() -> Settings:
    """Return a fresh ``Settings`` instance (reads env vars at call time)."""
    return Settings()
