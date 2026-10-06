#!/bin/bash
# Benchmarks one dataset from this client VM against the API server, appending to results.tsv:
#   link   iperf3 with 1 and 8 streams
#   naive  Java HttpClient + Jackson tree, one page at a time
#   duckdb read_json over the page URLs, 1 thread and one per core
#   apilytics  local mode, 1 partition and one per core, scan only (warm), compression off
# usage: run.sh <api-host> <size> <label>     e.g. run.sh 10.138.0.5 10gb 10   (label: the link cap)
set -uo pipefail
host=$1; size=$2; label=$3
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

# Link
for p in 1 8; do
  # -R: the server sends, as the API does; the cap is on its outbound traffic.
  gbps=$(iperf3 -c "$host" -R -t 8 -P $p -J | python3 -c "import json,sys; print(round(json.load(sys.stdin)['end']['sum_received']['bits_per_second'] / 1e9, 2))")
  printf '%s\t%s\tiperf3\t%s\t%s\t%s Gbps\n' "$label" "$size" "$cores" "$p" "$gbps" | tee -a /opt/bench/links.tsv
done

# Naive Java: one stream
J=/opt/spark/jars; CP="$(ls $J/jackson-core-*.jar $J/jackson-databind-*.jar $J/jackson-annotations-*.jar | tr '\n' ':')"
line=$( { /usr/bin/time -f '%U %S' -o /tmp/t java -Xmx4g -cp "$CP" Naive.java $base tree; } )
cpu=$(awk '{printf "%.1f", $1 + $2}' /tmp/t)
wall=$(sed -n 's/.*secs=\([0-9.]*\).*/\1/p' <<< "$line"); n=$(sed -n 's/.* n=\([0-9]*\).*/\1/p' <<< "$line"); s=$(sed -n 's/.*s_id=\([0-9]*\).*/\1/p' <<< "$line")
record naive-java 1 "$wall" "$cpu" "$n" "$s"

# DuckDB
for t in 1 $cores; do
  IFS=$'\t' read -r wall cpu n s _ < <(venv/bin/python duck.py $base $t)
  record duckdb $t "$wall" "$cpu" "$n" "$s"
done

# apilytics: per-session startup CPU, measured once per core count, is subtracted from the scan's.
cpu_of() { awk '{printf "%.1f", $1 + $2}' /tmp/t; }
echo "SELECT * FROM api.default.records LIMIT 5000;" > /tmp/warm.sql
{ cat /tmp/warm.sql; for i in 1 2; do echo "SELECT count(*), sum(benchmark_id), round(sum(trip_distance),2), round(sum(fare_amount),2) FROM api.default.records;"; done; } > /tmp/scan.sql
for p in 1 $cores; do
  per=$(( ( (rows + 5000 + p - 1) / p + 4999) / 5000 * 5000 ))
  part=""; [ $p -gt 1 ] && part="partition { type = offset, size = $per, count = $p }"
  conf=/opt/bench/$size-$p.conf
  cat > $conf <<C
openapi = "$base/openapi.yaml"
auth { type = none }
pagination { style = offset, offset-param = offset, page-size-param = limit, max-page-size = 5000, max-pages = 100000 }
http { max-retries = 5, max-backoff = 30s, timeout = 50s, compression = false }
tables { records { endpoint = "/offset", data-path = "/results", $part } }
C
  /usr/bin/time -f '%U %S' -o /tmp/t /opt/bench/spark-sql.sh $p $conf /tmp/warm.sql > /dev/null 2>&1; startup=$(cpu_of)
  /usr/bin/time -f '%U %S' -o /tmp/t /opt/bench/spark-sql.sh $p $conf /tmp/scan.sql > /tmp/scan.out 2> /tmp/scan.err; total=$(cpu_of)
  # Two scans ran: report the second (warm) one's time, and half the CPU beyond startup.
  wall=$(grep -a -o 'Time taken: [0-9.]* seconds' /tmp/scan.err | tail -1 | awk '{print $3}')
  cpu=$(echo "($total - $startup) / 2" | bc -l | xargs printf '%.1f')
  read -r n s _ < <(grep -a -E '^[0-9]+\s' /tmp/scan.out | tail -1)
  record apilytics $p "$wall" "$cpu" "$n" "$s"
done
