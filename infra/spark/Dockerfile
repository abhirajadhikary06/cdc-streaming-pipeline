# syntax=docker/dockerfile:1
# =============================================================================
# CDC Streaming Data Engineering Pipeline — custom Spark image
#
# Spark 3.5.0 (Hadoop 3 build) + Delta Lake 3.0.0 + Kafka structured
# streaming connector + Hadoop-AWS (S3A, used to talk to MinIO).
#
# This single image is reused for BOTH spark-master and spark-worker in
# infra/docker-compose.yml — the role is selected at container start via
# the entrypoint dispatcher below (`command: ["master"]` / `["worker"]`).
# =============================================================================
FROM python:3.10-slim

# --- OS packages -------------------------------------------------------
# default-jre-headless: Spark only needs a JRE to *run* (not a JDK to
#   compile), so this keeps the image smaller.
# curl: used to fetch Spark + jars below, and by the compose healthchecks.
# procps: Spark's daemon/launch scripts shell out to `ps`; without it
#   spark-class fails in slim/distroless-style images.
ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3

# --- Spark binary distribution ------------------------------------------
RUN curl -fL --retry 5 --retry-delay 3 \
    https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    -o /tmp/spark.tgz \
    && tar -xzf /tmp/spark.tgz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark \
    && rm /tmp/spark.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:${SPARK_HOME}/bin:${SPARK_HOME}/sbin
ENV PYTHONPATH=${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip
# Pin driver + executors to the same interpreter so UDFs behave consistently.
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Install the Python-side Delta package used by Silver layer imports.
RUN python -m pip install --no-cache-dir delta-spark==3.0.0

# --- JARs: Delta Lake, MinIO/S3A (Hadoop-AWS), Kafka structured streaming.
# Versions are the known-compatible set for Spark 3.5.0 / Hadoop 3.3.4:
#   - delta-spark/delta-storage 3.0.0  -> supports Spark 3.5.x
#   - hadoop-aws 3.3.4 + aws-java-sdk-bundle 1.12.262 -> matches Spark's
#     Hadoop-3 binary build exactly (mismatched hadoop-aws/hadoop-common
#     versions are the #1 cause of S3A "NoSuchMethodError" at runtime)
#   - spark-sql-kafka-0-10_2.12 3.5.0 + kafka-clients 3.4.1 + commons-pool2
#     2.11.1 + spark-token-provider-kafka-0-10_2.12 3.5.0 -> Spark 3.5.0's
#     own compiled Kafka dependency versions
# Placing them directly in $SPARK_HOME/jars means spark-submit/spark-class
# pick them up automatically — no --packages/--jars flags needed at runtime.
RUN mkdir -p /opt/spark/jars && \
    curl -fL -o /opt/spark/jars/delta-spark_2.12-3.0.0.jar https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/delta-spark_2.12-3.0.0.jar && \
    curl -fL -o /opt/spark/jars/delta-storage-3.0.0.jar https://repo1.maven.org/maven2/io/delta/delta-storage/3.0.0/delta-storage-3.0.0.jar && \
    curl -fL -o /opt/spark/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fL -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -fL -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -fL -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -fL -o /opt/spark/jars/kafka-clients-3.4.1.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -fL -o /opt/spark/jars/commons-pool2-2.11.1.jar https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# --- Entrypoint dispatcher ------------------------------------------------
# docker-compose passes the role as the container command:
#   spark-master -> command: ["master"]
#   spark-worker -> command: ["worker"]   (needs SPARK_MASTER_URL env var)
# You can also run one-off jobs against the running cluster, e.g.:
#   docker compose exec spark-master /opt/entrypoint.sh submit \
#     --master spark://spark-master:7077 \
#     /opt/spark-apps/streaming/bronze/bronze_cdc.py
COPY <<'EOF' /opt/entrypoint.sh
#!/bin/bash
set -e

MODE="${1:-shell}"
shift || true

case "$MODE" in
  master)
    exec "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.master.Master \
      --host "$(hostname)" \
      --port 7077 \
      --webui-port 8080
    ;;
  worker)
    : "${SPARK_MASTER_URL:?SPARK_MASTER_URL env var must be set for worker mode}"
    exec "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.worker.Worker \
      "${SPARK_MASTER_URL}" \
      --webui-port 8081 \
      ${SPARK_WORKER_CORES:+--cores "$SPARK_WORKER_CORES"} \
      ${SPARK_WORKER_MEMORY:+--memory "$SPARK_WORKER_MEMORY"}
    ;;
  submit)
    exec "${SPARK_HOME}/bin/spark-submit" "$@"
    ;;
  shell|bash)
    exec /bin/bash
    ;;
  *)
    exec "$MODE" "$@"
    ;;
esac
EOF
RUN chmod +x /opt/entrypoint.sh

# Streaming job code (bronze_cdc.py, silver_cdc.py, spark_session.py, ...)
# is bind-mounted here at runtime from ../streaming — see docker-compose.yml.
WORKDIR /opt/spark-apps

ENTRYPOINT ["/opt/entrypoint.sh"]
CMD ["shell"]