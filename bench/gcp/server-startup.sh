#!/bin/bash
# Startup script for the API server VM: builds the Axum synthetic API and serves the fixtures
# from RAM, uncompressed, so serving costs no decompression. Writes /var/tmp/bench-ready when done.
set -euxo pipefail
[ -f /var/tmp/bench-ready ] && { systemctl start synthetic-rest || true; exit 0; }
bucket=$(curl -s -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/attributes/bucket)
ip=$(hostname -I | awk '{print $1}')
export DEBIAN_FRONTEND=noninteractive
apt-get update -q && apt-get install -yq build-essential pkg-config iperf3 curl
root=/srv/synthetic
mem_kb=$(awk '/MemTotal/ {print $2}' /proc/meminfo)
mkdir -p $root/data && mount -t tmpfs -o size=$(( mem_kb * 8 / 10 ))k tmpfs $root/data
gcloud storage cp -r "$bucket/data/*" $root/data/
# Plain copies beside the gzip ones: the server sends the plain file to a client that doesn't
# accept gzip, and the gzip file as is to one that does.
find $root/data -name '*.json.gz' -print0 | xargs -0 -P "$(nproc)" -n 64 gunzip -k
# The specs name the host they were exported from; point their servers at this one.
sed -i -E "s#http://[^/\"' ]+:18600#http://$ip:18600#g" $root/data/*/*/*/openapi.yaml

mkdir -p /opt/axum && gcloud storage cp "$bucket/axum.tar.gz" /tmp/ && tar -xzf /tmp/axum.tar.gz -C /opt/axum
export RUSTUP_HOME=/opt/rust CARGO_HOME=/opt/rust
toolchain=$(sed -n 's/^channel *= *"\(.*\)"/\1/p' /opt/axum/rust-toolchain.toml)
curl -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain "$toolchain" --profile minimal
(cd /opt/axum && /opt/rust/bin/cargo build --release --locked)
install -m 755 /opt/axum/target/release/synthetic-rest-axum /usr/local/bin/

cat > /etc/systemd/system/synthetic-rest.service <<UNIT
[Unit]
Description=Synthetic REST API (Axum)
After=network-online.target
[Service]
Environment=SYNTHETIC_ROOT=$root SYNTHETIC_LISTEN=0.0.0.0:18600 SYNTHETIC_PUBLIC_BASE=http://$ip:18600
ExecStart=/usr/local/bin/synthetic-rest-axum
LimitNOFILE=65536
Restart=on-failure
UNIT
cat > /etc/systemd/system/iperf3-bench.service <<UNIT
[Unit]
Description=iperf3 server for link checks
[Service]
ExecStart=/usr/bin/iperf3 -s
Restart=on-failure
UNIT
systemctl daemon-reload && systemctl enable --now synthetic-rest iperf3-bench
curl --retry 20 --retry-connrefused --retry-delay 1 -fsS http://127.0.0.1:18600/health
touch /var/tmp/bench-ready
