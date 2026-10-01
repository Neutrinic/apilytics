FROM registry.access.redhat.com/ubi9/ubi-minimal:9.8
LABEL maintainer="APIlytics"

ARG SPARK_VERSION=4.2.0
ARG SCALA_VERSION=2.13
ARG JDK_VERSION=21

# Install dependencies (including Python for PySpark/Jupyter)
# curl is not installed: UBI 9's curl-minimal provides it, and the full package conflicts.
RUN microdnf update -y && \
    microdnf install -y java-${JDK_VERSION}-openjdk-devel tar gzip procps-ng python3.11 python3.11-pip && \
    microdnf clean all && \
    ln -sf /usr/bin/python3.11 /usr/bin/python3 && \
    ln -sf /usr/bin/python3.11 /usr/bin/python && \
    python3 -m pip install --no-cache-dir jupyter "pandas<3.0.0"

# Everything runs as `spark` (uid 185, as in Apache Spark's own images), not root.
# shadow-utils provides useradd, which UBI minimal lacks.
RUN microdnf install -y shadow-utils && microdnf clean all && \
    groupadd --gid 185 spark && \
    useradd --uid 185 --gid 185 --create-home --home-dir /home/spark --shell /bin/sh spark

# Install Almond (Scala kernel for Jupyter) as the spark user: the kernel's launcher is
# private to the user who installs it, and resolves its jars from their coursier cache.
ARG ALMOND_VERSION=0.14.5
ARG SCALA_KERNEL_VERSION=2.13.18
USER spark
WORKDIR /tmp
RUN curl -Lo coursier https://git.io/coursier-cli && \
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

# JVM options for Java 21 compatibility (includes Arrow memory module access)
ENV JAVA_OPTS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED"
ENV SPARK_DAEMON_JAVA_OPTS="$JAVA_OPTS"
ENV SPARK_SUBMIT_OPTS="$JAVA_OPTS"
# For Almond/Scala kernel
ENV JAVA_TOOL_OPTIONS="$JAVA_OPTS"

# Download and install Spark 4.2 with Scala 2.13
RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz | \
    tar -xz -C /opt && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME}

# Configure Spark
# Owned by spark before VOLUME, so new named volumes start out writable by it.
RUN mkdir -p ${SPARK_HOME}/logs ${SPARK_HOME}/work ${SPARK_HOME}/spark-events && \
    chown -R spark:spark ${SPARK_HOME}/logs ${SPARK_HOME}/work ${SPARK_HOME}/spark-events
COPY spark-defaults.conf ${SPARK_HOME}/conf/
COPY entrypoint.sh /

# Volumes for persistent storage
VOLUME ["/opt/spark/logs", "/opt/spark/work", "/opt/spark/spark-events"]

# Ports
# 8080: Master WebUI, 7077: Master port, 8081: Worker WebUI
# 18080: History Server, 4040: Application UI, 10000: Thrift/JDBC
EXPOSE 8080 7077 8081 18080 4040 10000

# Entrypoint script
RUN chmod +x /entrypoint.sh
# The working directory is where Thrift creates metastore_db and spark-warehouse.
USER spark
WORKDIR /home/spark
ENTRYPOINT ["/entrypoint.sh"]