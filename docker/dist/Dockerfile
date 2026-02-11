# Apilytics Distribution Image
# Pre-built image for zero-config quickstart
#
# Usage:
#   docker run -it neutrinic/apilytics "SELECT name FROM api.default.pokemon LIMIT 5"

FROM registry.access.redhat.com/ubi8/ubi-minimal:8.10
LABEL maintainer="Apilytics"
LABEL org.opencontainers.image.source="https://github.com/Neutrinic/apilytics"
LABEL org.opencontainers.image.description="Query REST APIs with Spark SQL"

ARG SPARK_VERSION=4.0.0
ARG JDK_VERSION=21

# Install dependencies
RUN microdnf update -y && \
    microdnf install -y java-${JDK_VERSION}-openjdk-devel tar gzip curl procps && \
    microdnf clean all

# Environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-${JDK_VERSION}-openjdk
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# JVM options for Java 21 compatibility
ENV SPARK_DAEMON_JAVA_OPTS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED"
ENV SPARK_SUBMIT_OPTS="$SPARK_DAEMON_JAVA_OPTS"

# Download and install Spark
RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz | \
    tar -xz -C /opt && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME}

# Configure Spark
RUN mkdir -p ${SPARK_HOME}/logs ${SPARK_HOME}/work

# Copy Apilytics JAR (built externally and passed as build context)
COPY apilytics.jar ${SPARK_HOME}/jars/

# Copy bundled configs
COPY configs/ /opt/apilytics/configs/

# Copy entrypoint
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

WORKDIR /opt/spark

ENTRYPOINT ["/entrypoint.sh"]
