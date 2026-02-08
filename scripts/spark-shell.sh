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

VALID_EXAMPLES=("pokeapi" "github" "slack" "multi")
if [[ ! " ${VALID_EXAMPLES[*]} " =~ " ${EXAMPLE} " ]]; then
    echo "Unknown example: $EXAMPLE. Available: ${VALID_EXAMPLES[*]}"
    exit 1
fi

# Check required env vars early (before build)
if [[ "$EXAMPLE" == "slack" ]] && [[ -z "$SLACK_BOT_TOKEN" ]]; then
    echo "Error: SLACK_BOT_TOKEN environment variable is required for Slack example"
    echo "Create a Slack app at https://api.slack.com/apps"
    echo 'Set it with: export SLACK_BOT_TOKEN="xoxb-..."'
    exit 1
fi

# JAR path (unversioned for simpler Docker mounts)
JAR_FILE="target/scala-2.13/apilytics.jar"

if [[ "$SKIP_BUILD" == "true" ]]; then
    echo "Skipping build..."
elif [[ "$FORCE" != "true" ]] && [[ -f "$JAR_FILE" ]]; then
    echo "JAR exists, skipping build. Use --force to rebuild."
else
    echo "Building JAR..."
    if [[ "$CLEAN" == "true" ]]; then
        sbt clean assembly || exit 1
    else
        sbt assembly || exit 1
    fi
fi

# Verify JAR exists
if [[ -f "$JAR_FILE" ]]; then
    echo "Using JAR: $JAR_FILE"
else
    echo "Error: JAR not found at $JAR_FILE"
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
elif [[ "$EXAMPLE" == "slack" ]]; then
    echo '  spark.sql("SELECT id, name FROM api.default.channels LIMIT 10").show()'
    echo '  spark.sql("SELECT id, name, real_name FROM api.default.users LIMIT 10").show()'
elif [[ "$EXAMPLE" == "multi" ]]; then
    echo '  -- Two catalogs: github.default.* and pokemon.default.*'
    echo '  spark.sql("SHOW TABLES IN github.default").show()'
    echo '  spark.sql("SHOW TABLES IN pokemon.default").show()'
    echo '  spark.sql("SELECT number, title FROM github.default.issues LIMIT 5").show()'
    echo '  spark.sql("SELECT name, url FROM pokemon.default.pokemon LIMIT 5").show()'
fi
echo ""

# Build env var args to pass to container (use array for proper quoting)
ENV_ARGS=()
[[ -n "$GITHUB_TOKEN" ]] && ENV_ARGS+=(-e "GITHUB_TOKEN=$GITHUB_TOKEN")
[[ -n "$SLACK_BOT_TOKEN" ]] && ENV_ARGS+=(-e "SLACK_BOT_TOKEN=$SLACK_BOT_TOKEN")

# Multi-catalog mode: load both github and pokemon catalogs
if [[ "$EXAMPLE" == "multi" ]]; then
    docker exec -it "${ENV_ARGS[@]}" "$CONTAINER" spark-shell \
        --jars "$JAR_PATH" \
        --conf "spark.sql.catalog.github=com.apilytics.spark.RESTCatalog" \
        --conf "spark.sql.catalog.github.config=/opt/spark/examples/github/github-config.conf" \
        --conf "spark.sql.catalog.pokemon=com.apilytics.spark.RESTCatalog" \
        --conf "spark.sql.catalog.pokemon.config=/opt/spark/examples/pokeapi/pokeapi-config.conf"
else
    docker exec -it "${ENV_ARGS[@]}" "$CONTAINER" spark-shell \
        --jars "$JAR_PATH" \
        --conf "spark.sql.catalog.api=com.apilytics.spark.RESTCatalog" \
        --conf "spark.sql.catalog.api.config=$CONFIG_PATH"
fi
