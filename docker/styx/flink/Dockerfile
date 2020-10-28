FROM chenzhe0722/styx:hive

ENV FLINK_HOME=/opt/flink
ARG FLINK_VERSION=1.13.1
ARG HIVE_VERSION=3.1.2
ARG SCALA_VERSION=2.12

RUN set -eux; \
  mkdir -p "${FLINK_HOME}"; \
  FILE="/tmp/app.tgz"; \
  for DIST_URL in \
    "https://www.apache.org/dyn/closer.cgi?action=download&filename=" \
    "https://www.apache.org/dist/" \
    "https://archive.apache.org/dist/"; do \
    if wget -qO "${FILE}" "${DIST_URL}flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-scala_${SCALA_VERSION}.tgz" && [ -s "${FILE}" ]; then \
      break; \
    fi; \
  done; \
  tar -xf "${FILE}" -C "${FLINK_HOME}" --strip-components=1; \
  rm "${FILE}"; \
  wget -qO "${FLINK_HOME}/lib/flink-sql-connector-hive-${HIVE_VERSION}_${SCALA_VERSION}-${FLINK_VERSION}.jar" "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-hive-${HIVE_VERSION}_${SCALA_VERSION}/${FLINK_VERSION}/flink-sql-connector-hive-${HIVE_VERSION}_${SCALA_VERSION}-${FLINK_VERSION}.jar"
