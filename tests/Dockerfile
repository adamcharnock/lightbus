FROM redis:4-alpine as redis

FROM python:3.6-alpine3.6

RUN \
    apk add -U gcc python3-dev musl-dev && \
    rm -rf /var/cache/apk/*

COPY --from=redis /usr/local/bin/redis-* /usr/bin/

WORKDIR /lightbus
COPY setup.py VERSION ./
COPY tests/requirements.txt ./test-requirements.txt

RUN pip install . -r test-requirements.txt

COPY lightbus ./lightbus
COPY tests ./tests

CMD py.test
