FROM python:3.14.3-slim-trixie

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    VENV_PATH=/opt/venv \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONPATH="/app"

RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends \
    g++ \
    && python -m venv /opt/venv \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r styx && useradd -r -g styx -d /app -m styx

WORKDIR /app

COPY coordinator/requirements.txt /tmp/requirements.txt
RUN python -m pip install --upgrade pip && \
    python -m pip install -r /tmp/requirements.txt

COPY styx-package /tmp/styx-package
RUN python -m pip install /tmp/styx-package

COPY coordinator /app/coordinator
COPY coordinator/start-coordinator.sh /usr/local/bin/start-coordinator.sh
RUN chmod 755 /usr/local/bin/start-coordinator.sh && \
    chown -R styx:styx /app

USER styx

EXPOSE 8888

ARG enable_compression=true
ARG use_composite_keys=true

ENV ENABLE_COMPRESSION=$enable_compression \
    USE_COMPOSITE_KEYS=$use_composite_keys

CMD ["/usr/local/bin/start-coordinator.sh"]