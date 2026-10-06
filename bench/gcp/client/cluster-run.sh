#!/bin/bash
# Runs the apilytics scan on the standalone cluster whose master is this VM, appending to
# results.tsv. One partition per executor core. CPU isn't recorded: it's spent on the workers.
# usage: cluster-run.sh <api-host> <size> <label> <workers> <cores-per-worker>
set -uo pipefail
host=$1; size=$2; label=$3; workers=$4; per_worker=$5
cd /opt/bench
base=http://$host:18600/taxi/$size/clean
cores=$((workers * per_worker)); p=$cores
out=/opt/bench/results.tsv
[ -f $out ] || printf 'label\tsize\tclient\tcores\tparallel\twall_s\tcpu_s\trows\ts_id\tok\n' > $out
read -r rows s_id < <(curl -fsS $base/manifest.json | python3 -c \
  "import json,sys; m=json.load(sys.stdin); print(m['row_count'], int(float(m['numeric']['benchmark_id']['sum'])))")
[ -n "${rows:-}" ] || { echo "API at $host unreachable" >&2; exit 1; }

per=$(( ( (rows + 5000 + p - 1) / p + 4999) / 5000 * 5000 ))
conf=/opt/bench/cluster-$size-$p.conf
cat > $conf <<C
openapi = "$base/openapi.yaml"
auth { type = none }
pagination { style = offset, offset-param = offset, page-size-param = limit, max-page-size = 5000, max-pages = 100000 }
http { max-retries = 5, max-backoff = 30s, timeout = 50s, compression = false }
tables { records { endpoint = "/offset", data-path = "/results", partition { type = offset, size = $per, count = $p } } }
C
{ echo "SELECT * FROM api.default.records LIMIT 5000;"
  for i in 1 2; do echo "SELECT count(*), sum(benchmark_id), round(sum(trip_distance),2), round(sum(fare_amount),2) FROM api.default.records;"; done
} > /tmp/cluster-scan.sql
# The master's own advertised URL, which uses its full internal DNS name.
url=$(curl -s localhost:8080/json/ | python3 -c 'import json,sys; print(json.load(sys.stdin)["url"])')
/opt/spark/bin/spark-sql --master "$url" \
  --total-executor-cores $cores --executor-cores $per_worker --executor-memory 20g --driver-memory 8g \
  --jars /opt/bench/apilytics.jar --conf spark.sql.catalogImplementation=in-memory --conf spark.ui.enabled=false \
  --conf spark.sql.catalog.api=com.apilytics.spark.RESTCatalog --conf spark.sql.catalog.api.config=$conf \
  -f /tmp/cluster-scan.sql > /tmp/cluster.out 2> /tmp/cluster.err
wall=$(grep -a -o 'Time taken: [0-9.]* seconds' /tmp/cluster.err | tail -1 | awk '{print $3}')
read -r n s _ < <(grep -a -E '^[0-9]+\s' /tmp/cluster.out | tail -1)
ok=no; [ "${n:-}" = "$rows" ] && [ "${s:-}" = "$s_id" ] && ok=yes
printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$label" "$size" "apilytics-cluster-${workers}w" "$cores" "$p" "$wall" "" "${n:-}" "${s:-}" "$ok" | tee -a $out
