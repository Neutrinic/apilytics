#!/bin/bash
# Run the client benchmarks at each link cap, then copy the results here.
# usage: bench.sh "<rates in gbit, or off>" "<sizes>"      e.g. bench.sh "1 10" "2gb 10gb"
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd); . "$here/env.sh"
rates=$1; sizes=$2
ip=$(gc compute instances describe "$SERVER" --zone "$ZONE" --format 'value(networkInterfaces[0].networkIP)' | tr -d '\r')
for rate in $rates; do
  "$here/shape.sh" "$rate"
  for size in $sizes; do
    on "$CLIENT_PREFIX-0" "/opt/bench/run.sh $ip $size ${rate}"
  done
done
"$here/shape.sh" off
mkdir -p "$here/results"
# Git Bash would rewrite the remote paths as Windows ones.
for f in results links; do
  MSYS2_ARG_CONV_EXCL="$CLIENT_PREFIX" gc compute scp --zone "$ZONE" "$CLIENT_PREFIX-0:/opt/bench/$f.tsv" "$here/results/"
done
column -t -s $'\t' "$here/results/results.tsv"
