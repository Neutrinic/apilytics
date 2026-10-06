#!/bin/bash
# Delete the benchmark VMs. With --all, also the bucket and everything in it.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd); . "$here/env.sh"
vms=$(gc compute instances list --filter "name=$SERVER OR name ~ ^$CLIENT_PREFIX-" --format 'value(name)' | tr -d '\r')
[ -n "$vms" ] && gc compute instances delete $vms --zone "$ZONE" --quiet
[ "${1:-}" = --all ] && gc storage rm -r "$BUCKET"
gc compute instances list --format 'value(name,status)'
