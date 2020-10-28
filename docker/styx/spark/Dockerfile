FROM chenzhe0722/styx:hive

ENV SPARK_HOME=/opt/spark PATH=${SPARK_HOME}/bin:${PATH}
ARG SPARK_VERSION=3.1.2

RUN set -eux; \
  mkdir -p "${SPARK_HOME}"; \
  FILE="/tmp/app.tgz"; \
  for DIST_URL in \
    "https://www.apache.org/dyn/closer.cgi?action=download&filename=" \
    "https://www.apache.org/dist/" \
    "https://archive.apache.org/dist/"; do \
    if wget -qO "${FILE}" "${DIST_URL}spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-without-hadoop.tgz" && [ -s "${FILE}" ]; then \
      break; \
    fi; \
  done; \
  tar -xf "${FILE}" -C "${SPARK_HOME}" --strip-components=1; \
  rm "${FILE}"

COPY ["entrypoint", "/usr/local/bin/entrypoint"]
