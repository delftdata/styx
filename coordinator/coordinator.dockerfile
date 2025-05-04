FROM python:3.13.2-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    g++ \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r styx && useradd -rm -d /usr/local/styx -g styx styx

USER styx

ENV PATH="/usr/local/styx/.local/bin:${PATH}" \
    PYTHONPATH="/usr/local/styx"

COPY --chown=styx:styx coordinator/requirements.txt /var/local/styx/
COPY --chown=styx:styx styx-package /var/local/styx-package/
RUN pip install --upgrade pip && \
    pip install --user -r /var/local/styx/requirements.txt && \
    pip install --user /var/local/styx-package/

WORKDIR /usr/local/styx

COPY --chown=styx:styx coordinator coordinator
COPY --chown=styx:styx coordinator/start-coordinator.sh /usr/local/bin/

RUN chmod a+x /usr/local/bin/start-coordinator.sh

EXPOSE 8888

ARG max_operator_parallelism=10
ENV MAX_OPERATOR_PARALLELISM=${max_operator_parallelism}

CMD ["/usr/local/bin/start-coordinator.sh"]
