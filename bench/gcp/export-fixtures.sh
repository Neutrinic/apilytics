#!/bin/bash
# Copy the taxi fixtures from a running synthetic API to a local directory, as the gzip files it
# stores, for up.sh to upload.
# usage: SOURCE_API=http://host:18600 export-fixtures.sh <out-dir> [sizes...]   (default: 2gb 10gb)
set -euo pipefail
out=$1; shift
sizes=${*:-2gb 10gb}
api=${SOURCE_API:?Set SOURCE_API to the synthetic API, for example http://host:18600}
for size in $sizes; do
  key=taxi/$size/clean
  dir=$out/data/$key
  mkdir -p "$dir/offset/5000"
  curl -fsS "$api/$key/manifest.json" -o "$dir/manifest.json"
  curl -fsS "$api/$key/openapi.yaml" -o "$dir/openapi.yaml"
  rows=$(sed -n 's/.*"row_count": *\([0-9]*\).*/\1/p' "$dir/manifest.json")
  seq 0 5000 $((rows - 1)) | xargs -P 8 -I{} sh -c \
    "[ -s '$dir/offset/5000/{}.json.gz' ] || curl -fsS -H 'Accept-Encoding: gzip' '$api/$key/offset?offset={}&limit=5000' -o '$dir/offset/5000/{}.json.gz'"
  echo "$key: $(ls "$dir/offset/5000" | wc -l) pages, $(du -sh "$dir" | cut -f1)"
done
