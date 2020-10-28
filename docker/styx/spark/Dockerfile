FROM chenzhe0722/styx:hive

ENV SPARK_HOME=/opt/spark

RUN set -eux; \
  mkdir -p "${SPARK_HOME}"; \
  wget -qO "/tmp/spark.tgz" "https://archive.apache.org/dist/spark/spark-3.0.1/spark-3.0.1-bin-without-hadoop.tgz"; \
  tar -xf "/tmp/spark.tgz" -C "${SPARK_HOME}" --strip-components=1; \
  rm "/tmp/spark.tgz"
