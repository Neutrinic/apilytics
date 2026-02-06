#!/bin/bash
# Spark shell launcher for Apilytics
# Usage: ./scripts/spark-shell.sh [example-name] [--skip-build] [--force]
# Example: ./scripts/spark-shell.sh pokeapi
# Example: ./scripts/spark-shell.sh github --skip-build

EXAMPLE="${1:-pokeapi}"
SKIP_BUILD=false
RESTART_DOCKER=false
FORCE=false

# Parse flags
for arg in "$@"; do
    case $arg in
        --skip-build) SKIP_BUILD=true ;;
        --force) FORCE=true ;;
        --restart) RESTART_DOCKER=true ;;
    esac
done

VALID_EXAMPLES=("pokeapi" "github")
if [[ ! " ${VALID_EXAMPLES[*]} " =~ " ${EXAMPLE} " ]]; then
    echo "Unknown example: $EXAMPLE. Available: ${VALID_EXAMPLES[*]}"
    exit 1
fi

JAR_FILE="target/scala-2.13/apilytics-0.1.0-SNAPSHOT.jar"

if [[ "$SKIP_BUILD" == "true" ]]; then
    echo "Skipping build..."
elif [[ "$FORCE" != "true" ]] && [[ -f "$JAR_FILE" ]]; then
    echo "JAR exists, skipping build. Use --force to rebuild."
else
    echo "Building JAR..."
    sbt clean assembly || exit 1
fi

if [[ "$RESTART_DOCKER" == "true" ]]; then
    echo "Restarting cluster..."
    docker compose -f docker/spark/compose.spark.yaml restart || exit 1
    sleep 3
fi

CONTAINER="${SPARK_MASTER_CONTAINER:-spark-master}"
JAR_PATH="/opt/spark/jars/apilytics.jar"
CONFIG_PATH="/opt/spark/examples/$EXAMPLE/$EXAMPLE-config.conf"

echo ""
echo "Starting spark-shell with $EXAMPLE config..."
echo "Container: $CONTAINER"
echo ""

# Example queries to try
echo "Example queries:"
if [[ "$EXAMPLE" == "pokeapi" ]]; then
    echo '  spark.sql("SELECT name, url FROM api.default.pokemon LIMIT 10").show()'
    echo '  spark.sql("SELECT * FROM api.default.pokemon WHERE name = '\''pikachu'\''").show()'
elif [[ "$EXAMPLE" == "github" ]]; then
    echo '  spark.sql("SELECT number, title, state FROM api.default.issues LIMIT 10").show()'
    echo '  spark.sql("SELECT number, title FROM api.default.issues WHERE state = '\''open'\''").show()'
fi
echo ""

docker exec -it "$CONTAINER" spark-shell \
    --jars "$JAR_PATH" \
    --conf "spark.sql.catalog.api=com.apilytics.spark.RESTCatalog" \
    --conf "spark.sql.catalog.api.config=$CONFIG_PATH"
