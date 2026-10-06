#!/bin/bash
# All-columns pass: each client must produce every column, so neither can skip unused fields.
#   duckdb     auto-detected schema, sum(hash(record)), one thread per core
#   apilytics  sum(hash(*)) over every column, local[cores], and on the cluster if <workers> > 0
# Hashes differ between systems, so rows are checked on the count and the benchmark_id sum.
# usage: allcols.sh <api-host> <size> <label> [workers] [cores-per-worker]
set -uo pipefail
host=$1; size=$2; label=$3; workers=${4:-0}; per_worker=${5:-0}
cd /opt/bench
base=http://$host:18600/taxi/$size/clean
cores=$(nproc)
out=/opt/bench/results.tsv
[ -f $out ] || printf 'label\tsize\tclient\tcores\tparallel\twall_s\tcpu_s\trows\ts_id\tok\n' > $out
read -r rows s_id < <(curl -fsS $base/manifest.json | python3 -c \
  "import json,sys; m=json.load(sys.stdin); print(m['row_count'], int(float(m['numeric']['benchmark_id']['sum'])))")
[ -n "${rows:-}" ] || { echo "API at $host unreachable" >&2; exit 1; }
record() {  # client parallel wall cpu rows s_id
  local ok=no; [ "$5" = "$rows" ] && [ "$6" = "$s_id" ] && ok=yes
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$label" "$size" "$1" "$cores" "$2" "$3" "$4" "$5" "$6" "$ok" | tee -a $out
}

cat > /tmp/duck-all.py <<'PY'
import duckdb, json, resource, sys, time, urllib.request
base, threads = sys.argv[1], int(sys.argv[2])
rows = json.load(urllib.request.urlopen(base + "/manifest.json"))["row_count"]
urls = [f"{base}/offset?offset={o}&limit=5000" for o in range(0, rows, 5000)]
con = duckdb.connect()
con.execute(f"SET threads = {threads}")
con.execute("INSTALL httpfs; LOAD httpfs")
q = "SELECT count(*), sum(r.benchmark_id), sum(hash(r)) FROM (SELECT unnest(results) AS r FROM read_json($urls))"
before = resource.getrusage(resource.RUSAGE_SELF); t = time.time()
res = con.execute(q, {"urls": urls}).fetchone()
wall = time.time() - t; after = resource.getrusage(resource.RUSAGE_SELF)
cpu = (after.ru_utime + after.ru_stime) - (before.ru_utime + before.ru_stime)
print("\t".join([f"{wall:.1f}", f"{cpu:.1f}"] + [str(v) for v in res]))
PY
IFS=$'\t' read -r wall cpu n s _ < <(venv/bin/python /tmp/duck-all.py $base $cores)
record duckdb-allcols $cores "$wall" "$cpu" "$n" "$s"

per=$(( ( (rows + 5000 + cores - 1) / cores + 4999) / 5000 * 5000 ))
conf=/opt/bench/allcols-$size-$cores.conf
cat > $conf <<C
openapi = "$base/openapi.yaml"
auth { type = none }
pagination { style = offset, offset-param = offset, page-size-param = limit, max-page-size = 5000, max-pages = 100000 }
http { max-retries = 5, max-backoff = 30s, timeout = 50s, compression = false }
tables { records { endpoint = "/offset", data-path = "/results", partition { type = offset, size = $per, count = $cores } } }
C
echo "SELECT * FROM api.default.records LIMIT 5000;" > /tmp/warm.sql
{ cat /tmp/warm.sql; for i in 1 2; do echo "SELECT count(*), sum(benchmark_id), sum(hash(*)) FROM api.default.records;"; done; } > /tmp/all.sql
cpu_of() { awk '{printf "%.1f", $1 + $2}' /tmp/t; }
/usr/bin/time -f '%U %S' -o /tmp/t /opt/bench/spark-sql.sh $cores $conf /tmp/warm.sql > /dev/null 2>&1; startup=$(cpu_of)
/usr/bin/time -f '%U %S' -o /tmp/t /opt/bench/spark-sql.sh $cores $conf /tmp/all.sql > /tmp/all.out 2> /tmp/all.err; total=$(cpu_of)
wall=$(grep -a -o 'Time taken: [0-9.]* seconds' /tmp/all.err | tail -1 | awk '{print $3}')
cpu=$(echo "($total - $startup) / 2" | bc -l | xargs printf '%.1f')
read -r n s _ < <(grep -a -E '^[0-9]+\s' /tmp/all.out | tail -1)
record apilytics-allcols $cores "$wall" "$cpu" "$n" "$s"

if [ "$workers" -gt 0 ]; then
  total_cores=$((workers * per_worker))
  per=$(( ( (rows + 5000 + total_cores - 1) / total_cores + 4999) / 5000 * 5000 ))
  sed "s/size = [0-9]*, count = [0-9]*/size = $per, count = $total_cores/" $conf > /opt/bench/allcols-cluster.conf
  url=$(curl -s localhost:8080/json/ | python3 -c 'import json,sys; print(json.load(sys.stdin)["url"])')
  /opt/spark/bin/spark-sql --master "$url" \
    --total-executor-cores $total_cores --executor-cores $per_worker --executor-memory 20g --driver-memory 8g \
    --jars /opt/bench/apilytics.jar --conf spark.sql.catalogImplementation=in-memory --conf spark.ui.enabled=false \
    --conf spark.sql.catalog.api=com.apilytics.spark.RESTCatalog --conf spark.sql.catalog.api.config=/opt/bench/allcols-cluster.conf \
    -f /tmp/all.sql > /tmp/all-cluster.out 2> /tmp/all-cluster.err
  wall=$(grep -a -o 'Time taken: [0-9.]* seconds' /tmp/all-cluster.err | tail -1 | awk '{print $3}')
  read -r n s _ < <(grep -a -E '^[0-9]+\s' /tmp/all-cluster.out | tail -1)
  record "apilytics-allcols-cluster-${workers}w" $total_cores "$wall" "" "$n" "$s"
fi
