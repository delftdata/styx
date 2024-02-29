FROM python:3.11-alpine


RUN addgroup -S styx && adduser -S styx -G styx --home /usr/local/styx

USER styx

COPY --chown=styx:styx worker/requirements.txt /var/local/styx/
COPY --chown=styx:styx styx-package /var/local/styx-package/

ENV PATH="/usr/local/styx/.local/bin:${PATH}"

RUN pip install --upgrade pip \
    && pip install --user -r /var/local/styx/requirements.txt \
    && pip install --user ./var/local/styx-package/

WORKDIR /usr/local/styx

COPY --chown=styx:styx worker worker

COPY --chown=styx:styx worker/start-worker.sh /usr/local/bin/
RUN chmod a+x /usr/local/bin/start-worker.sh

ENV PYTHONPATH /usr/local/styx

CMD ["/usr/local/bin/start-worker.sh"]