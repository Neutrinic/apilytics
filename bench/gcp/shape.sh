#!/bin/bash
# Cap the API server's outbound rate with a token bucket, or remove the cap.
# usage: shape.sh <gbit|off>
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd); . "$here/env.sh"
rate=$1
if [ "$rate" = off ]; then
  on "$SERVER" 'nic=$(ip route show default | awk "{print \$5}"); sudo tc qdisc del dev $nic root 2>/dev/null; tc qdisc show dev $nic | head -1'
else
  # Burst holds 10 ms at the target rate.
  on "$SERVER" "nic=\$(ip route show default | awk '{print \$5}'); sudo tc qdisc replace dev \$nic root tbf rate ${rate}gbit burst $((rate * 1250000)) latency 50ms && tc qdisc show dev \$nic | head -1"
fi
