#!/bin/bash
# Upload the fixtures and client scripts, create any missing VMs, and wait until they're set up.
# usage: up.sh <fixtures-dir>     (the directory export-fixtures.sh wrote)
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd); . "$here/env.sh"
fixtures=$1
[ -n "$PROJECT" ] || { echo "Set PROJECT, or a default project with gcloud config set project" >&2; exit 1; }
[ -d "$SYNTHETIC_REST/axum" ] || { echo "Set SYNTHETIC_REST to a checkout of the synthetic API" >&2; exit 1; }
region=${ZONE%-*}
gc storage buckets describe "$BUCKET" > /dev/null 2>&1 || gc storage buckets create "$BUCKET" --location "$region" --uniform-bucket-level-access
gc storage rsync -r "$fixtures/data" "$BUCKET/data"
tar -czf /tmp/axum.tar.gz -C "$SYNTHETIC_REST/axum" --exclude target .
gc storage cp /tmp/axum.tar.gz "$BUCKET/axum.tar.gz"
gc storage cp "$here/client/"* "$BUCKET/client/"

common=(--zone "$ZONE" --image-family "$IMAGE_FAMILY" --image-project "$IMAGE_PROJECT"
  --provisioning-model "$PROVISIONING" --min-cpu-platform "$MIN_CPU_PLATFORM"
  --boot-disk-size 30GB --boot-disk-type pd-balanced --scopes storage-ro)
if [ "$PROVISIONING" = SPOT ]; then common+=(--instance-termination-action DELETE); fi
exists() { gc compute instances describe "$1" --zone "$ZONE" > /dev/null 2>&1; }
# Only missing VMs are created, so a rerun replaces a deleted one and keeps the rest.
exists "$SERVER" || gc compute instances create "$SERVER" "${common[@]}" --machine-type "$SERVER_TYPE" \
  --metadata bucket="$BUCKET" --metadata-from-file startup-script="$here/server-startup.sh"
clients=$(for i in $(seq 0 $((CLIENTS - 1))); do echo "$CLIENT_PREFIX-$i"; done)
client_meta=(--metadata bucket="$BUCKET",spark-version="$SPARK_VERSION",apilytics-jar-url="$APILYTICS_JAR_URL"
  --metadata-from-file startup-script="$here/client-startup.sh")
exists "$CLIENT_PREFIX-0" ||
  gc compute instances create "$CLIENT_PREFIX-0" "${common[@]}" --machine-type "$DRIVER_TYPE" "${client_meta[@]}"
missing=$(for vm in $clients; do [ "$vm" = "$CLIENT_PREFIX-0" ] || exists "$vm" || echo "$vm"; done)
[ -z "$missing" ] || gc compute instances create $missing "${common[@]}" --machine-type "$CLIENT_TYPE" "${client_meta[@]}"

for vm in "$SERVER" $clients; do
  until on "$vm" 'test -f /var/tmp/bench-ready' 2> /dev/null; do sleep 20; done
  echo "$vm ready"
done
