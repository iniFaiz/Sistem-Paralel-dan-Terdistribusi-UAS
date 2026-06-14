FROM python:3.11-slim

LABEL maintainer="uas-exam"
LABEL description="Pub-Sub Log Aggregator – Event Publisher / Simulator"

# Prevent Python from buffering stdout/stderr
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Create non-root user
RUN groupadd -r publisher && useradd -r -g publisher -d /app -s /sbin/nologin publisher

# Install dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ ./app/

# Switch to non-root user
USER publisher

CMD ["python", "-m", "app.main"]
