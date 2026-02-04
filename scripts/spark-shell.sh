#!/bin/bash
# Spark shell launcher for Apilytics
# Usage: ./scripts/spark-shell.sh [example-name] [--skip-build] [--force]
# Example: ./scripts/spark-shell.sh pokeapi
# Example: ./scripts/spark-shell.sh pokeapi --skip-build

EXAMPLE="${1:-pokeapi}"
SKIP_BUILD=false
FORCE=false

# Parse flags
for arg in "$@"; do
    case $arg in
        --skip-build) SKIP_BUILD=true ;;
        --force) FORCE=true ;;
    esac
done

VALID_EXAMPLES=("pokeapi" "github" "countries")
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
    sbt assembly || exit 1
fi

CONTAINER="${SPARK_MASTER_CONTAINER:-spark-master}"
JAR_PATH="/opt/spark/jars/apilytics.jar"
CONFIG_PATH="/opt/spark/examples/$EXAMPLE/$EXAMPLE-config.conf"

echo ""
echo "Starting spark-shell with $EXAMPLE config..."
echo "Container: $CONTAINER"
echo ""

# After shell starts, try:
#   spark.sql("SHOW TABLES IN api.default").show()
#   spark.sql("SELECT * FROM api.default.listPokemon").show(1)

docker exec -it "$CONTAINER" spark-shell \
    --jars "$JAR_PATH" \
    --conf "spark.sql.catalog.api=com.apilytics.spark.RESTCatalog" \
    --conf "spark.sql.catalog.api.config=$CONFIG_PATH"
