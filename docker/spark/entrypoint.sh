#!/bin/sh

set -e

role=${SPARK_ROLE:-""}

if [ -z "$role" ]; then
    echo "SPARK_ROLE must be set: master, worker, history, thriftserver"
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
    thriftserver)
        # Build catalog conf arguments from environment variables
        # CATALOGS=api,github,pokemon
        # CATALOG_API_CONFIG=/path/to/config.conf
        CATALOG_CONFS=""
        if [ -n "$CATALOGS" ]; then
            OLD_IFS="$IFS"
            IFS=','
            for catalog in $CATALOGS; do
                upper=$(echo "$catalog" | tr '[:lower:]' '[:upper:]')
                config_path=$(printenv "CATALOG_${upper}_CONFIG")
                if [ -n "$config_path" ]; then
                    CATALOG_CONFS="$CATALOG_CONFS --conf spark.sql.catalog.${catalog}=com.apilytics.spark.RESTCatalog"
                    CATALOG_CONFS="$CATALOG_CONFS --conf spark.sql.catalog.${catalog}.config=${config_path}"
                fi
            done
            IFS="$OLD_IFS"
        fi

        exec ${SPARK_HOME}/bin/spark-submit \
            --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 \
            --name "Thrift JDBC/ODBC Server" \
            --master ${SPARK_MASTER_URL:-local[*]} \
            --hiveconf hive.server2.thrift.port=10000 \
            --hiveconf hive.server2.thrift.bind.host=0.0.0.0 \
            $CATALOG_CONFS \
            spark-internal
        ;;
    jupyter)
        # Start Jupyter notebook with PySpark
        # Configure both pokeapi and github catalogs
        export PYSPARK_DRIVER_PYTHON=jupyter
        export PYSPARK_DRIVER_PYTHON_OPTS="notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root --notebook-dir=/opt/spark/examples/notebooks"

        exec ${SPARK_HOME}/bin/pyspark \
            --master ${SPARK_MASTER_URL:-local[*]} \
            --conf spark.sql.catalog.pokeapi=com.apilytics.spark.RESTCatalog \
            --conf spark.sql.catalog.pokeapi.config=/opt/spark/examples/pokeapi/pokeapi-config.conf \
            --conf spark.sql.catalog.github=com.apilytics.spark.RESTCatalog \
            --conf spark.sql.catalog.github.config=/opt/spark/examples/github/github-config.conf
        ;;
    *)
        echo "Unknown role: $role"
        exit 1
        ;;
esac