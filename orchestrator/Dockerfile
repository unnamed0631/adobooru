# Requires a modern Docker with BuildKit.
FROM python:3.12-slim

ARG WITH_PLAYWRIGHT=false

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    ca-certificates \
    libgomp1 \
    libatomic1 \
    tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt requirements-playwright.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# The Playwright fallback is optional because Chromium adds several hundred
# MB.  Enable with WITH_PLAYWRIGHT=true (or the compose variable of the same
# name) when gallery-dl cannot reach a target.
RUN if [ "$WITH_PLAYWRIGHT" = "true" ]; then \
    pip install --no-cache-dir -r requirements-playwright.txt \
    && playwright install --with-deps chromium \
    && chmod -R 755 /ms-playwright; \
    fi

COPY ingest_orchestrator.py ./
COPY adobooru ./adobooru

RUN useradd --create-home --uid 1000 app \
    && mkdir -p /var/lib/adobooru \
    && chown -R app:app /app /var/lib/adobooru

USER app

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "ingest_orchestrator.py", "serve"]
