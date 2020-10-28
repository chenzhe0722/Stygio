FROM chenzhe0722/styx:hive

ENV SPARK_HOME=/opt/spark

RUN set -eux; \
  mkdir -p "${SPARK_HOME}"; \
  FILE="/tmp/spark.tgz"; \
  for DIST_URL in \
    "https://www.apache.org/dyn/closer.cgi?action=download&filename=" \
    "https://www.apache.org/dist/" \
    "https://archive.apache.org/dist/"; do \
    if wget -qO "${FILE}" "${DIST_URL}spark/spark-3.1.1/spark-3.1.1-bin-without-hadoop.tgz" && [ -s "${FILE}" ]; then \
      break; \
    fi; \
  done; \
  tar -xf "${FILE}" -C "${SPARK_HOME}" --strip-components=1; \
  rm "${FILE}"
