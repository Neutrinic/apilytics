FROM registry.access.redhat.com/ubi8/ubi-minimal:8.10
LABEL maintainer="APIlytics"

ARG SPARK_VERSION=4.0.0
ARG SCALA_VERSION=2.13
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

# Download and install Spark 4.0 with Scala 2.13
RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz | \
    tar -xz -C /opt && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME}

# Configure Spark
RUN mkdir -p ${SPARK_HOME}/logs ${SPARK_HOME}/work ${SPARK_HOME}/spark-events
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
ENTRYPOINT ["/entrypoint.sh"]