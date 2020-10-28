FROM chenzhe0722/styx:hadoop

ENV HIVE_HOME=/opt/hive

RUN set -eux; \
  mkdir -p "${HIVE_HOME}"; \
  wget -qO "/tmp/hive.tar.gz" "https://archive.apache.org/dist/hive/hive-3.1.2/apache-hive-3.1.2-bin.tar.gz"; \
  tar -xf "/tmp/hive.tar.gz" -C "${HIVE_HOME}" --strip-components=1; \
  rm "/tmp/hive.tar.gz"; \
  cp "${HADOOP_HOME}/share/hadoop/common/lib/guava-27.0-jre.jar" "${HIVE_HOME}/lib/guava-27.0-jre.jar"; \
  rm "${HIVE_HOME}/lib/guava-19.0.jar"
