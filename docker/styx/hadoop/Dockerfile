FROM openjdk:11-jre-slim

ENV HADOOP_HOME=/opt/hadoop

RUN set -eux; \
  apt-get -qq update; \
  DEBIAN_FRONTEND=noninteractive apt-get -yqq --no-install-recommends install wget krb5-user; \
  rm -rf "/var/lib/apt/lists/*"; \
  mkdir -p "${HADOOP_HOME}"; \
  FILE="/tmp/hadoop.tar.gz"; \
  case "$(dpkg --print-architecture)" in \
    amd64) \
      DIST_FILE="hadoop/common/hadoop-3.3.0/hadoop-3.3.0.tar.gz"; \
    ;; \
    arm64) \
      DIST_FILE="hadoop/common/hadoop-3.3.0/hadoop-3.3.0-aarch64.tar.gz"; \
    ;; \
    *) \
      echo "[Error] Unsupported architecture" >&2; \
      exit 1; \
    ;; \
  esac; \
  for DIST_URL in \
    "https://www.apache.org/dyn/closer.cgi?action=download&filename=" \
    "https://www.apache.org/dist/" \
    "https://archive.apache.org/dist/"; do \
    if wget -qO "${FILE}" "${DIST_URL}${DIST_FILE}" && [ -s "${FILE}" ]; then \
      break; \
    fi; \
  done; \
  tar -xf "${FILE}" -C "${HADOOP_HOME}" --strip-components=1; \
  rm "${FILE}"
