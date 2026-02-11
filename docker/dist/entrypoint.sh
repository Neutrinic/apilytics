#!/bin/bash
set -e

CONFIG_DIR="/opt/apilytics/configs"
DEFAULT_CONFIG="$CONFIG_DIR/pokeapi.conf"

# Parse arguments
CONFIG_FILE="$DEFAULT_CONFIG"
CATALOG_NAME="api"
SQL_QUERY=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --config)
      CONFIG_FILE="$2"
      shift 2
      ;;
    --catalog)
      CATALOG_NAME="$2"
      shift 2
      ;;
    --help|-h)
      echo "Apilytics - Query REST APIs with Spark SQL"
      echo ""
      echo "Usage: docker run -it neutrinic/apilytics [OPTIONS] [SQL_QUERY]"
      echo ""
      echo "Options:"
      echo "  --config FILE    Path to config file (default: pokeapi)"
      echo "  --catalog NAME   Catalog name (default: api)"
      echo "  -h, --help       Show this help message"
      echo ""
      echo "Examples:"
      echo "  # Interactive shell with PokeAPI"
      echo "  docker run -it neutrinic/apilytics"
      echo ""
      echo "  # Run a query"
      echo "  docker run -it neutrinic/apilytics \"SELECT name FROM api.default.pokemon LIMIT 5\""
      echo ""
      echo "  # Use custom config"
      echo "  docker run -it -v ./my.conf:/config.conf neutrinic/apilytics --config /config.conf"
      echo ""
      echo "Bundled configs:"
      echo "  /opt/apilytics/configs/pokeapi.conf       - PokeAPI (default)"
      echo "  /opt/apilytics/configs/github-public.conf - GitHub public repos"
      exit 0
      ;;
    *)
      SQL_QUERY="$1"
      shift
      ;;
  esac
done

# Build spark-sql command (JAR is already in $SPARK_HOME/jars/, no --jars needed)
SPARK_CMD="spark-sql"
SPARK_CMD="$SPARK_CMD --conf spark.sql.catalog.$CATALOG_NAME=com.apilytics.spark.RESTCatalog"
SPARK_CMD="$SPARK_CMD --conf spark.sql.catalog.$CATALOG_NAME.config=$CONFIG_FILE"

# Run query or interactive shell
if [[ -n "$SQL_QUERY" ]]; then
  exec $SPARK_CMD -e "$SQL_QUERY"
else
  exec $SPARK_CMD
fi
