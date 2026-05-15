FROM python:3.14.5-slim-trixie

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
RUN cd /tmp/styx-package && python setup.py build_ext --inplace && \
    python -m pip install /tmp/styx-package

COPY worker /app/worker

# Compile Cython extensions for hot-path acceleration
RUN python worker/setup.py build_ext --inplace || \
    echo "WARNING: Cython build failed — using pure-Python fallback"

COPY worker/start-worker.sh /usr/local/bin/start-worker.sh

RUN chmod 755 /usr/local/bin/start-worker.sh && \
    chown -R styx:styx /app

USER styx

ARG epoch_size=100
ARG worker_threads=1
ARG enable_compression=true
ARG use_composite_keys=true

ENV SEQUENCE_MAX_SIZE=$epoch_size \
    WORKER_THREADS=$worker_threads \
    ENABLE_COMPRESSION=$enable_compression \
    USE_COMPOSITE_KEYS=$use_composite_keys

CMD ["/usr/local/bin/start-worker.sh"]