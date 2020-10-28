FROM chenzhe0722/styx:hadoop

ENV HIVE_HOME=/opt/hive
ARG HIVE_VERSION=3.1.2

RUN set -eux; \
  mkdir -p "${HIVE_HOME}"; \
  FILE="/tmp/app.tgz"; \
  for DIST_URL in \
    "https://www.apache.org/dyn/closer.cgi?action=download&filename=" \
    "https://www.apache.org/dist/" \
    "https://archive.apache.org/dist/"; do \
    if wget -qO "${FILE}" "${DIST_URL}hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz" && [ -s "${FILE}" ]; then \
      break; \
    fi; \
  done; \
  tar -xf "${FILE}" -C "${HIVE_HOME}" --strip-components=1; \
  rm "${FILE}"; \
  cp "${HADOOP_HOME}/share/hadoop/common/lib/guava-27.0-jre.jar" "${HIVE_HOME}/lib/guava-27.0-jre.jar"; \
  rm "${HIVE_HOME}/lib/guava-19.0.jar"

COPY ["entrypoint", "/usr/local/bin/entrypoint"]
