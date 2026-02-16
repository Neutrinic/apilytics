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

# Install dependencies (including Python for PySpark)
RUN microdnf update -y && \
    microdnf install -y java-${JDK_VERSION}-openjdk-devel tar gzip curl procps python3.11 python3.11-pip && \
    microdnf clean all && \
    ln -sf /usr/bin/python3.11 /usr/bin/python3 && \
    ln -sf /usr/bin/python3.11 /usr/bin/python && \
    pip3 install --no-cache-dir jupyter pandas

# Install Almond (Scala kernel for Jupyter)
ARG ALMOND_VERSION=0.14.0-RC15
ARG SCALA_KERNEL_VERSION=2.13.14
RUN curl -Lo coursier https://git.io/coursier-cli && \
    chmod +x coursier && \
    ./coursier launch --fork almond:${ALMOND_VERSION} --scala ${SCALA_KERNEL_VERSION} -- --install && \
    rm coursier

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

WORKDIR /opt/spark

ENTRYPOINT ["/entrypoint.sh"]
