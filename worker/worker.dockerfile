FROM python:3.13.2-slim-bookworm

# 1. Install dependencies, create user, and clean up in one step
RUN apt-get update && apt-get install -y --no-install-recommends \
    g++ \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r styx && useradd -rm -d /usr/local/styx -g styx styx

# Switch to non-root user as early as possible for better security
USER styx

ENV PATH="/usr/local/styx/.local/bin:${PATH}"
ENV PYTHONPATH="/usr/local/styx"

COPY --chown=styx:styx worker/requirements.txt /var/local/styx/
COPY --chown=styx:styx styx-package /var/local/styx-package/

RUN pip install --upgrade pip && \
    pip install --user -r /var/local/styx/requirements.txt && \
    pip install --user /var/local/styx-package/

WORKDIR /usr/local/styx

COPY --chown=styx:styx worker worker
COPY --chown=styx:styx worker/start-worker.sh /usr/local/bin/

RUN chmod a+x /usr/local/bin/start-worker.sh

ARG epoch_size=100
ENV SEQUENCE_MAX_SIZE=${epoch_size}

CMD ["/usr/local/bin/start-worker.sh"]
