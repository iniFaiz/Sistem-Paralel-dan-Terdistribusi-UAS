"""
Publisher service configuration.

All settings are loaded from environment variables with sensible defaults
for Docker Compose deployment.
"""

import os
from typing import List


# ── Target aggregator endpoint ───────────────────────────────────────────
TARGET_URL: str = os.getenv("TARGET_URL", "http://aggregator:8080/publish")

# ── Redis broker connection ──────────────────────────────────────────────
BROKER_URL: str = os.getenv("BROKER_URL", "redis://broker:6379")

# ── Simulation parameters ───────────────────────────────────────────────
EVENT_COUNT: int = int(os.getenv("EVENT_COUNT", "20000"))
DUPLICATE_RATE: float = float(os.getenv("DUPLICATE_RATE", "0.3"))
BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "50"))

# ── Topics to simulate ──────────────────────────────────────────────────
TOPICS: List[str] = os.getenv(
    "TOPICS",
    "auth,payment,notification,analytics,inventory,shipping,audit,errors",
).split(",")

# ── Redis stream name ───────────────────────────────────────────────────
STREAM_NAME: str = os.getenv("STREAM_NAME", "events")

# ── Retry / back-off settings ───────────────────────────────────────────
MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "10"))
INITIAL_BACKOFF: float = float(os.getenv("INITIAL_BACKOFF", "1.0"))
MAX_BACKOFF: float = float(os.getenv("MAX_BACKOFF", "30.0"))

# ── Health-check wait (aggregator readiness) ─────────────────────────────
HEALTH_URL: str = os.getenv(
    "HEALTH_URL",
    TARGET_URL.rsplit("/", 1)[0] + "/stats",
)
HEALTH_TIMEOUT: int = int(os.getenv("HEALTH_TIMEOUT", "120"))

# ── Concurrency ─────────────────────────────────────────────────────────
HTTP_CONCURRENCY: int = int(os.getenv("HTTP_CONCURRENCY", "10"))
