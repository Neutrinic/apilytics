#!/bin/bash
# Run the apilytics scan on a Spark standalone cluster at each link cap and worker count. Client 0
# is the master and driver; workers are clients 1..N, which run no driver.
# usage: cluster.sh "<rates in gbit, or off>" "<worker counts>" "<sizes>"   e.g. cluster.sh "25" "2 4 8" "10gb"
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd); . "$here/env.sh"
rates=$1; counts=$2; sizes=$3
ip=$(gc compute instances describe "$SERVER" --zone "$ZONE" --format 'value(networkInterfaces[0].networkIP)' | tr -d '\r')
master=$CLIENT_PREFIX-0
per_worker=$(gc compute machine-types describe "$CLIENT_TYPE" --zone "$ZONE" --format 'value(guestCpus)' | tr -d '\r')
# /opt/spark is root's; the daemons keep their logs and pid files in /tmp instead.
spark_env='export SPARK_LOG_DIR=/tmp/spark-logs SPARK_PID_DIR=/tmp;'
on "$master" "$spark_env /opt/spark/sbin/stop-master.sh > /dev/null; /opt/spark/sbin/start-master.sh > /dev/null && echo master up"

url=$(on "$master" "curl -s localhost:8080/json/ | python3 -c 'import json,sys; print(json.load(sys.stdin)[\"url\"])'" | tr -d '\r')
echo "master $url"
alive() { on "$master" "curl -s localhost:8080/json/ | python3 -c 'import json,sys; print(json.load(sys.stdin)[\"aliveworkers\"])'" | tr -d '\r'; }
for count in $counts; do
  for i in $(seq 1 $((CLIENTS - 1))); do
    if [ "$i" -le "$count" ]; then cmd="$spark_env /opt/spark/sbin/start-worker.sh $url > /dev/null"
    else cmd="$spark_env /opt/spark/sbin/stop-worker.sh > /dev/null"; fi
    on "$CLIENT_PREFIX-$i" "$cmd" &
  done
  wait
  for try in $(seq 30); do [ "$(alive 2> /dev/null)" = "$count" ] && break; sleep 5; done
  [ "$(alive)" = "$count" ] || { echo "expected $count workers, master reports $(alive)" >&2; exit 1; }
  echo "$count workers alive"
  for rate in $rates; do
    "$here/shape.sh" "$rate"
    for size in $sizes; do on "$master" "/opt/bench/cluster-run.sh $ip $size $rate $count $per_worker"; done
  done
done
"$here/shape.sh" off
mkdir -p "$here/results"
MSYS2_ARG_CONV_EXCL="$CLIENT_PREFIX" gc compute scp --zone "$ZONE" "$master:/opt/bench/results.tsv" "$here/results/"
column -t -s $'\t' "$here/results/results.tsv"
