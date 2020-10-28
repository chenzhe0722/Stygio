FROM openjdk:11-jre-slim

ENV KAFKA_HOME=/opt/kafka
ARG KAFKA_VERSION=2.8.0
ARG SCALA_VERSION=2.13

RUN set -eux; \
  apt-get -qq update; \
  DEBIAN_FRONTEND=noninteractive apt-get -yqq --no-install-recommends install wget; \
  rm -rf "/var/lib/apt/lists/*"; \
  mkdir -p "${KAFKA_HOME}"; \
  FILE="/tmp/app.tgz"; \
  for DIST_URL in \
    "https://www.apache.org/dyn/closer.cgi?action=download&filename=" \
    "https://www.apache.org/dist/" \
    "https://archive.apache.org/dist/"; do \
    if wget -qO "${FILE}" "${DIST_URL}kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" && [ -s "${FILE}" ]; then \
      break; \
    fi; \
  done; \
  tar -xf "${FILE}" -C "${KAFKA_HOME}" --strip-components=1; \
  rm "${FILE}"

COPY ["entrypoint", "/usr/local/bin/entrypoint"]
CMD ["entrypoint"]
