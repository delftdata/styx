FROM python:3.13.1-slim-bookworm

COPY shopping-cart-demo/requirements.txt /var/local/styx/
COPY styx-package /var/local/styx-package/

RUN groupadd styx \
    && useradd -m -d /usr/local/styx -g styx styx

RUN pip install --upgrade pip \
    && pip install --prefix=/usr/local -r /var/local/styx/requirements.txt \
    && pip install --prefix=/usr/local ./var/local/styx-package/

WORKDIR /usr/local/styx/shopping-cart-demo

COPY --chown=styx shopping-cart-demo .

ENV PYTHONPATH /usr/local/styx

USER styx

# default port
EXPOSE 5000