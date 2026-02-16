#!/bin/bash
set -e

EXAMPLES_DIR="/opt/apilytics/examples"
DEFAULT_CONFIG="$EXAMPLES_DIR/pokeapi/pokeapi-config.conf"

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
      echo "Usage: docker run -it ghcr.io/neutrinic/apilytics [OPTIONS] [SQL_QUERY]"
      echo ""
      echo "Options:"
      echo "  --config FILE    Path to config file (default: /opt/apilytics/examples/pokeapi/pokeapi-config.conf)"
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
      echo "  # Use bundled Github config"
      echo "  docker run -it ghcr.io/neutrinic/apilytics --config /opt/apilytics/examples/github/github-config.conf"
      echo ""
      echo "  # Use custom config"
      echo "  docker run -it -v ./my.conf:/config.conf neutrinic/apilytics --config /config.conf"
      echo ""
      echo "Bundled configs:"
      echo "  /opt/apilytics/examples/pokeapi/pokeapi-config.conf  - PokeAPI (default)"
      echo "  /opt/apilytics/examples/github/github-config.conf    - GitHub public repos"
      echo ""
      echo "PySpark:"
      echo "  # Interactive PySpark shell"
      echo "  docker run -it neutrinic/apilytics pyspark"
      echo ""
      echo "  # Run a Python script"
      echo "  docker run -it neutrinic/apilytics spark-submit /opt/apilytics/examples/pyspark/basic.py"
      echo ""
      echo "Jupyter:"
      echo "  # Start Jupyter notebook server"
      echo "  docker run -p 8888:8888 neutrinic/apilytics jupyter"
      echo ""
      echo "Example Queries (PokeAPI - default):"
      echo "  -- List types"
      echo "  SELECT name FROM api.default.types;"
      echo ""
      echo "  -- List abilities"
      echo "  SELECT name FROM api.default.abilities LIMIT 10;"
      echo ""
      echo "  -- Count Pokemon"
      echo "  SELECT COUNT(*) FROM api.default.pokemon;"
      echo "Example Queries (GitHub - override):"
      echo "  -- List issues from octocat/Hello-World"
      echo "  SELECT number, title, state FROM api.default.issues LIMIT 10;"
      echo ""
      echo "  -- Filter by state"
      echo "  SELECT number, title FROM api.default.issues WHERE state = 'open' LIMIT 10;"
      echo ""
      echo "  -- Filter by date"
      echo "  SELECT count(*) FROM api.default.issues WHERE created_at >= '2026-01-01';"
      exit 0
      ;;
    pyspark)
      # Launch PySpark interactive shell with catalog configured
      exec pyspark \
        --conf "spark.sql.catalog.$CATALOG_NAME=com.apilytics.spark.RESTCatalog" \
        --conf "spark.sql.catalog.$CATALOG_NAME.config=$CONFIG_FILE"
      ;;
    python3|python)
      # Run Python script with PySpark
      shift
      exec python3 "$@"
      ;;
    spark-submit)
      # Run spark-submit with catalog configured
      shift
      exec spark-submit \
        --conf "spark.sql.catalog.$CATALOG_NAME=com.apilytics.spark.RESTCatalog" \
        --conf "spark.sql.catalog.$CATALOG_NAME.config=$CONFIG_FILE" \
        "$@"
      ;;
    jupyter)
      # Start Jupyter notebook server with both pokeapi and github catalogs
      export PYSPARK_DRIVER_PYTHON=jupyter
      export PYSPARK_DRIVER_PYTHON_OPTS="notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root --notebook-dir=/opt/apilytics/examples/notebooks"
      exec pyspark \
        --conf "spark.sql.catalog.pokeapi=com.apilytics.spark.RESTCatalog" \
        --conf "spark.sql.catalog.pokeapi.config=$EXAMPLES_DIR/pokeapi/pokeapi-config.conf" \
        --conf "spark.sql.catalog.github=com.apilytics.spark.RESTCatalog" \
        --conf "spark.sql.catalog.github.config=$EXAMPLES_DIR/github/github-config.conf"
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
