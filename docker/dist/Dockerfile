# Apilytics Distribution Image
# Pre-built image for zero-config quickstart
#
# Usage:
#   docker run -it ghcr.io/neutrinic/apilytics "SELECT name FROM api.default.pokemon LIMIT 5"

FROM registry.access.redhat.com/ubi9/ubi-minimal:9.8
LABEL maintainer="Apilytics"
LABEL org.opencontainers.image.source="https://github.com/Neutrinic/apilytics"
LABEL org.opencontainers.image.description="Query REST APIs with Spark SQL"

ARG SPARK_VERSION=4.2.0
ARG JDK_VERSION=21

# Install dependencies (including Python for PySpark)
#
# pandas is pinned below 3.0: PySpark warns it does not yet fully support pandas >= 3.0.0,
# and an unpinned install resolves to it.
#
# The pyarrow/grpcio/zstandard set is what Spark Connect's Python client needs. Without
# them `spark-pipelines` fails at import, so Spark Declarative Pipelines cannot run at all
# — SDP is Connect-only. Classic PySpark does not need them, which is why the gap went
# unnoticed.
# curl is not installed: UBI 9's curl-minimal provides it, and the full package conflicts.
RUN microdnf update -y && \
    microdnf install -y java-${JDK_VERSION}-openjdk-devel tar gzip procps-ng python3.11 python3.11-pip && \
    microdnf clean all && \
    ln -sf /usr/bin/python3.11 /usr/bin/python3 && \
    ln -sf /usr/bin/python3.11 /usr/bin/python && \
    python3 -m pip install --no-cache-dir jupyter "pandas<3.0.0" \
      "pyarrow>=18.0.0" "grpcio>=1.48.1" "grpcio-status>=1.48.1" \
      "googleapis-common-protos>=1.65.0" "zstandard>=0.25.0"

# Everything runs as `spark` (uid 185, as in Apache Spark's own images), not root.
# shadow-utils provides useradd, which UBI minimal lacks.
RUN microdnf install -y shadow-utils && microdnf clean all && \
    groupadd --gid 185 spark && \
    useradd --uid 185 --gid 185 --create-home --home-dir /home/spark --shell /bin/sh spark

# Install Almond (Scala kernel for Jupyter) as the spark user: the kernel's launcher is
# private to the user who installs it, and resolves its jars from their coursier cache.
ARG ALMOND_VERSION=0.14.5
ARG SCALA_KERNEL_VERSION=2.13.18
# A pinned release, fetched with -f so an HTTP error fails here rather than being saved and
# run as the launcher, and checked against its SHA-256 so a replaced asset fails too (#337).
# It used to come from git.io, GitHub's retired shortener, which points at an unpinned
# branch; 2.0.16 is the launcher that branch serves. Change both values together.
ARG COURSIER_VERSION=2.0.16
ARG COURSIER_SHA256=631e8fbc1a3beb71a7130539b3b01852cfbe02edb94c5e9c6785981a0aee1e9c
USER spark
WORKDIR /tmp
RUN curl -fsSL --retry 3 -o coursier \
      https://github.com/coursier/coursier/releases/download/v${COURSIER_VERSION}/coursier && \
    echo "${COURSIER_SHA256}  coursier" | sha256sum -c - && \
    chmod +x coursier && \
    ./coursier launch --fork almond:${ALMOND_VERSION} --scala ${SCALA_KERNEL_VERSION} -- --install && \
    rm coursier
USER root
WORKDIR /

# Environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-${JDK_VERSION}-openjdk
ENV SPARK_HOME=/opt/spark
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/python

# JVM options for Java 21 compatibility
ENV JAVA_OPTS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED"
ENV SPARK_DAEMON_JAVA_OPTS="$JAVA_OPTS"
ENV SPARK_SUBMIT_OPTS="$JAVA_OPTS"
# For Almond/Scala kernel
ENV JAVA_TOOL_OPTIONS="$JAVA_OPTS"

# Download and install Spark
RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz | \
    tar -xz -C /opt && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME}

# Configure Spark
RUN mkdir -p ${SPARK_HOME}/logs ${SPARK_HOME}/work

# Copy Apilytics JAR from build output
COPY target/scala-2.13/apilytics.jar ${SPARK_HOME}/jars/

# Copy examples (configs are in examples/pokeapi/, examples/github/)
COPY examples/ /opt/apilytics/examples/

# Copy entrypoint
COPY docker/dist/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# The spark user writes logs (Thrift), and notebooks saved from Jupyter land in the
# examples. Its home is the working directory, where spark-sql and Thrift create
# metastore_db and spark-warehouse.
RUN chown -R spark:spark ${SPARK_HOME}/logs ${SPARK_HOME}/work /opt/apilytics
USER spark
WORKDIR /home/spark

ENTRYPOINT ["/entrypoint.sh"]
