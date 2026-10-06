#!/bin/bash
# Startup script for client VMs: Java 17, Spark, the apilytics jar, DuckDB and the benchmark
# scripts under /opt/bench. Writes /var/tmp/bench-ready when done.
set -euxo pipefail
[ -f /var/tmp/bench-ready ] && exit 0
meta() { curl -s -H 'Metadata-Flavor: Google' "http://metadata.google.internal/computeMetadata/v1/instance/attributes/$1"; }
bucket=$(meta bucket); spark=$(meta spark-version); jar=$(meta apilytics-jar-url)
export DEBIAN_FRONTEND=noninteractive
apt-get update -q && apt-get install -yq openjdk-17-jdk-headless python3-venv iperf3 bc time curl
curl -fsSL "https://archive.apache.org/dist/spark/spark-$spark/spark-$spark-bin-hadoop3.tgz" | tar -xz -C /opt
ln -sfn "/opt/spark-$spark-bin-hadoop3" /opt/spark
# The daemons write logs, pid files and executor work directories under SPARK_HOME.
chmod -R a+rwX "/opt/spark-$spark-bin-hadoop3"
mkdir -p /opt/bench && gcloud storage cp "$bucket/client/*" /opt/bench/
curl -fsSL -o /opt/bench/apilytics.jar "$jar"
python3 -m venv /opt/bench/venv && /opt/bench/venv/bin/pip install -q duckdb
chmod +x /opt/bench/*.sh
chown -R "$(ls /home | head -1)" /opt/bench 2>/dev/null || true
chmod -R a+rwX /opt/bench
touch /var/tmp/bench-ready
