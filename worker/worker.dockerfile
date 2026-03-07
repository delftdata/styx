FROM python:3.14.3-slim-trixie

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    VENV_PATH=/opt/venv \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONPATH="/app"

RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends \
    g++ \
    && python -m venv /opt/venv \
    && groupadd -r styx \
    && useradd -r -g styx -d /app -m styx \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY worker/requirements.txt /tmp/requirements.txt
RUN python -m pip install --upgrade pip && \
    python -m pip install -r /tmp/requirements.txt

COPY styx-package /tmp/styx-package
RUN python -m pip install /tmp/styx-package

COPY worker /app/worker
COPY worker/start-worker.sh /usr/local/bin/start-worker.sh

RUN chmod 755 /usr/local/bin/start-worker.sh && \
    chown -R styx:styx /app

USER styx

ARG epoch_size=100
ARG worker_threads=1
ARG enable_compression=true
ARG use_composite_keys=true
ARG use_fallback_cache=true

ENV SEQUENCE_MAX_SIZE=$epoch_size \
    WORKER_THREADS=$worker_threads \
    ENABLE_COMPRESSION=$enable_compression \
    USE_COMPOSITE_KEYS=$use_composite_keys \
    USE_FALLBACK_CACHE=$use_fallback_cache

CMD ["/usr/local/bin/start-worker.sh"]