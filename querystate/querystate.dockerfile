FROM python:3.13.0-slim-bookworm

RUN apt-get update && apt-get install -y git gcc

RUN groupadd styx \
    && useradd -m -d /usr/local/styx -g styx styx

USER styx

COPY --chown=styx:styx querystate/requirements.txt /var/local/styx/
COPY --chown=styx:styx styx-package /var/local/styx-package/

ENV PATH="/usr/local/styx/.local/bin:${PATH}"

RUN pip install --upgrade pip \
    && pip install --user -r /var/local/styx/requirements.txt \
    && pip install --user ./var/local/styx-package/

WORKDIR /usr/local/styx

COPY --chown=styx:styx querystate querystate

COPY --chown=styx:styx querystate/start-querystate.sh /usr/local/bin/
RUN chmod a+x /usr/local/bin/start-querystate.sh

ENV PYTHONPATH /usr/local/styx

CMD ["/usr/local/bin/start-querystate.sh"]

EXPOSE 8080