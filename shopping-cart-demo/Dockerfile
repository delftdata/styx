FROM python:3.13.2-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    g++ \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r styx && useradd -rm -d /usr/local/styx -g styx styx

USER styx

ENV PATH="/usr/local/styx/.local/bin:${PATH}"
ENV PYTHONPATH="/usr/local/styx"

COPY --chown=styx:styx shopping-cart-demo/requirements.txt /var/local/styx/
COPY --chown=styx:styx styx-package /var/local/styx-package/
RUN pip install --upgrade pip && \
    pip install --user -r /var/local/styx/requirements.txt && \
    pip install --user /var/local/styx-package/

WORKDIR /usr/local/styx/shopping-cart-demo

COPY --chown=styx shopping-cart-demo .

EXPOSE 8001
