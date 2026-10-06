# Shared settings for the GCP benchmark scripts. Override any of them in the environment.
PROJECT=${PROJECT:-$(gcloud config get-value project 2> /dev/null | tr -d '\r')}
ZONE=${ZONE:-us-west1-b}
BUCKET=${BUCKET:-gs://$PROJECT-apilytics-bench}
SERVER=${SERVER:-bench-api}
SERVER_TYPE=${SERVER_TYPE:-n2-highmem-16}
CLIENT_PREFIX=${CLIENT_PREFIX:-bench-client}
# Client 0 runs the single-machine benchmarks and is the Spark master and driver; the others are
# Spark workers.
DRIVER_TYPE=${DRIVER_TYPE:-n2-standard-16}
CLIENT_TYPE=${CLIENT_TYPE:-n2-standard-8}
CLIENTS=${CLIENTS:-9}
# SPOT is cheaper, but GCE can delete a spot VM mid-run, as it did the API server on the first run.
PROVISIONING=${PROVISIONING:-STANDARD}
# N2 VMs land on Cascade Lake or Ice Lake unless pinned, and per-core results differ by up to 45%
# between them. Results are only comparable within one platform.
MIN_CPU_PLATFORM=${MIN_CPU_PLATFORM:-Intel Ice Lake}
IMAGE_FAMILY=${IMAGE_FAMILY:-ubuntu-2404-lts-amd64}
IMAGE_PROJECT=${IMAGE_PROJECT:-ubuntu-os-cloud}
SPARK_VERSION=${SPARK_VERSION:-4.0.4}
APILYTICS_JAR_URL=${APILYTICS_JAR_URL:-https://github.com/Neutrinic/apilytics/releases/download/dev/apilytics_2.13-dev.jar}
# A checkout of the synthetic API, whose axum/ directory up.sh builds on the server. See README.md.
SYNTHETIC_REST=${SYNTHETIC_REST:-}

gc() { gcloud --project "$PROJECT" "$@"; }
# Runs a command on a VM. It starts with "cd &&" because Git Bash on Windows rewrites an argument
# that starts with "/" into a Windows path.
on() { local vm=$1; shift; gcloud --project "$PROJECT" compute ssh "$vm" --zone "$ZONE" --quiet --command "cd && $*"; }
