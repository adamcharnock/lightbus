FROM alpine:3.6 as redis_etc
# Build redis unstable (until streams gets into a release), plus get the cc-test-reporter

ENV REDIS_VERSION 999.999.999
ENV REDIS_DOWNLOAD_URL https://github.com/antirez/redis/archive/5.0-rc4.tar.gz

# for redis-sentinel see: http://redis.io/topics/sentinel
RUN set -ex; \
	\
	apk add --no-cache --virtual .build-deps \
	    ca-certificates \
		coreutils \
		gcc \
		linux-headers \
		make \
		musl-dev \
		openssl \
		ca-certificates \
		wget \
	; \
	update-ca-certificates; \
	\
	wget -O redis.tar.gz "$REDIS_DOWNLOAD_URL"; \
	wget -O /cc-test-reporter https://codeclimate.com/downloads/test-reporter/test-reporter-latest-linux-amd64; \
	mkdir -p /usr/src/redis; \
	tar -xzf redis.tar.gz -C /usr/src/redis --strip-components=1; \
	rm redis.tar.gz; \
	\
# disable Redis protected mode [1] as it is unnecessary in context of Docker
# (ports are not automatically exposed when running inside Docker, but rather explicitly by specifying -p / -P)
# [1]: https://github.com/antirez/redis/commit/edd4d555df57dc84265fdfb4ef59a4678832f6da
	grep -q '^#define CONFIG_DEFAULT_PROTECTED_MODE 1$' /usr/src/redis/src/server.h; \
	sed -ri 's!^(#define CONFIG_DEFAULT_PROTECTED_MODE) 1$!\1 0!' /usr/src/redis/src/server.h; \
	grep -q '^#define CONFIG_DEFAULT_PROTECTED_MODE 0$' /usr/src/redis/src/server.h; \
# for future reference, we modify this directly in the source instead of just supplying a default configuration flag because apparently "if you specify any argument to redis-server, [it assumes] you are going to specify everything"
# see also https://github.com/docker-library/redis/issues/4#issuecomment-50780840
# (more exactly, this makes sure the default behavior of "save on SIGTERM" stays functional by default)
	\
	make -C /usr/src/redis -j "$(nproc)"; \
	make -C /usr/src/redis install; \
	\
	rm -r /usr/src/redis; \
	\
	apk del .build-deps


FROM python:3.6-alpine3.6

RUN \
    apk add -U gcc python3-dev musl-dev git && \
    rm -rf /var/cache/apk/*

# Postgres
RUN apk update && apk add postgresql-libs postgresql-dev

WORKDIR /lightbus

COPY --from=redis_etc /usr/local/bin/redis-* /usr/bin/
COPY --from=redis_etc /cc-test-reporter /cc-test-reporter
RUN chmod +x /cc-test-reporter

COPY setup.py VERSION requirements.txt ./
COPY tests/requirements.txt ./test-requirements.txt

RUN pip install -r requirements.txt .[development]

COPY .coveragerc .
COPY lightbus ./lightbus
COPY tests ./tests
# The examples are used in the autodiscovery tests
COPY lightbus_examples ./lightbus_examples
RUN mkdir .coverage

RUN \
    find lightbus/ -type f -name "*.py" -exec \
    sed -i 's/# test-hook: \([a-z_]*\)/from lightbus.utilities.testing import test_hook; test_hook("\1")/g' {} \;

CMD py.test
