FROM debian:stable-slim

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 HADOOP_HOME=/opt/hadoop

RUN set -eux; \
  mkdir -p /usr/share/man/man1; \
  apt-get -qq update; \
  DEBIAN_FRONTEND=noninteractive apt-get -yqq --no-install-recommends install wget tar ssh openjdk-11-jre-headless; \
  rm -rf "/var/lib/apt/lists/*"; \
  mkdir -p "${HADOOP_HOME}"; \
  wget -qO "/tmp/hadoop.tar.gz" "https://archive.apache.org/dist/hadoop/common/hadoop-3.3.0/hadoop-3.3.0.tar.gz"; \
  tar -xf "/tmp/hadoop.tar.gz" -C "${HADOOP_HOME}" --strip-components=1; \
  rm "/tmp/hadoop.tar.gz"
