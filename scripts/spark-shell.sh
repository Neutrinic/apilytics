#!/bin/bash
# Spark shell launcher for Apilytics
# Usage: ./scripts/spark-shell.sh [example-name] [--skip-build] [--force] [--clean] [--restart]
# Example: ./scripts/spark-shell.sh pokeapi
# Example: ./scripts/spark-shell.sh github --skip-build
# Example: ./scripts/spark-shell.sh github --force --clean

EXAMPLE="${1:-pokeapi}"
SKIP_BUILD=false
RESTART_DOCKER=false
FORCE=false
CLEAN=false

# Parse flags
for arg in "$@"; do
    case $arg in
        --skip-build) SKIP_BUILD=true ;;
        --force) FORCE=true ;;
        --clean) CLEAN=true ;;
        --restart) RESTART_DOCKER=true ;;
    esac
done

VALID_EXAMPLES=("pokeapi" "github")
if [[ ! " ${VALID_EXAMPLES[*]} " =~ " ${EXAMPLE} " ]]; then
    echo "Unknown example: $EXAMPLE. Available: ${VALID_EXAMPLES[*]}"
    exit 1
fi

# Find the JAR file (handles version changes)
JAR_FILE=$(ls target/scala-2.13/apilytics-*.jar 2>/dev/null | head -1)

if [[ "$SKIP_BUILD" == "true" ]]; then
    echo "Skipping build..."
elif [[ "$FORCE" != "true" ]] && [[ -n "$JAR_FILE" ]]; then
    echo "JAR exists ($(basename "$JAR_FILE")), skipping build. Use --force to rebuild."
else
    echo "Building JAR..."
    if [[ "$CLEAN" == "true" ]]; then
        sbt clean assembly || exit 1
    else
        sbt assembly || exit 1
    fi
    # Re-find JAR after build
    JAR_FILE=$(ls target/scala-2.13/apilytics-*.jar 2>/dev/null | head -1)
fi

# Set env var for docker compose (used in volume mount)
if [[ -n "$JAR_FILE" ]]; then
    export APILYTICS_JAR="$JAR_FILE"
    echo "Using JAR: $(basename "$JAR_FILE")"
else
    echo "Error: No JAR found matching target/scala-2.13/apilytics-*.jar"
    exit 1
fi

CONTAINER="${SPARK_MASTER_CONTAINER:-spark-master}"

if [[ "$RESTART_DOCKER" == "true" ]]; then
    echo "Restarting cluster..."
    docker compose -f docker/spark/compose.spark.yaml restart || exit 1

    # Wait for container to be running
    echo "Waiting for $CONTAINER to be ready..."
    for i in {1..30}; do
        if [[ "$(docker inspect -f '{{.State.Running}}' "$CONTAINER" 2>/dev/null)" == "true" ]]; then
            echo "$CONTAINER is running"
            break
        fi
        sleep 1
    done

    if [[ "$(docker inspect -f '{{.State.Running}}' "$CONTAINER" 2>/dev/null)" != "true" ]]; then
        echo "Error: $CONTAINER failed to start within 30s"
        exit 1
    fi
fi

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
