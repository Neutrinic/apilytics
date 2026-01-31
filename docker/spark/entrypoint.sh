#!/bin/sh

set -e

role=${SPARK_ROLE:-""}

if [ -z "$role" ]; then
    echo "SPARK_ROLE must be set: master, worker, history"
    exit 1
fi

case $role in
    master)
        exec ${SPARK_HOME}/bin/spark-class org.apache.spark.deploy.master.Master \
            --host 0.0.0.0 \
            --port 7077 \
            --webui-port 8080
        ;;
    worker)
        if [ -z "$SPARK_MASTER_URL" ]; then
            echo "SPARK_MASTER_URL must be set for worker"
            exit 1
        fi
        exec ${SPARK_HOME}/bin/spark-class org.apache.spark.deploy.worker.Worker \
            --webui-port 8081 \
            ${SPARK_MASTER_URL}
        ;;
    history)
        exec ${SPARK_HOME}/bin/spark-class org.apache.spark.deploy.history.HistoryServer
        ;;
    *)
        echo "Unknown role: $role"
        exit 1
        ;;
esac