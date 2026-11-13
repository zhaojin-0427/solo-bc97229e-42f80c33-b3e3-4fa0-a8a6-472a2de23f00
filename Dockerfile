FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Dependencies first for layer caching.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY examples ./examples
COPY scripts ./scripts

# Non-root user; the data directory is a mounted volume.
RUN useradd --create-home --uid 10001 appuser \
    && mkdir -p /data && chown -R appuser:appuser /app /data
USER appuser

ENV CARBON_DB_PATH=/data/carbon.db \
    LOG_LEVEL=INFO

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=5 \
    CMD python -c "import urllib.request;\
urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=2)" || exit 1

# One process, one worker thread: SQLite writes are serialised inside the app.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
