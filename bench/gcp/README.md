# Reading a fast API: GCP benchmark

How fast apilytics reads a paginated JSON API when the network isn't the limit, compared with
two baselines that read the same pages:

- **naive Java**: one thread, the JDK's `HttpClient`, Jackson `readTree`, one page at a time.
- **DuckDB**: `read_json` over the list of page URLs, on one thread and on every core.
- **apilytics**: in Spark local mode with 1 partition and one per core, and on a Spark standalone
  cluster of 2, 4 and 8 workers.

Each run reads the same synthetic dataset and checks its row count and `benchmark_id` sum against
the dataset's manifest, so a fast wrong answer can't pass as a result. The API server's outbound
rate is capped with `tc` to compare link speeds (1, 10 and 25 Gbps, or uncapped).

Results so far are in [`results/`](results/README.md).

## What you need

- **`gcloud`**, signed in, with a project set (`gcloud config set project …`, or `PROJECT=…`).
- **Quota.** The full plan uses 96 vCPUs, 10 external IPs and 300 GB of SSD in one region:
  - `bench-api`: n2-highmem-16
  - `bench-client-0`: n2-standard-16, which runs the single-machine benchmarks and is the Spark master and driver
  - `bench-client-1` to `-8`: n2-standard-8 Spark workers

  New projects start at 12 to 16 vCPUs across all regions, and free-trial accounts can't raise
  that until they upgrade to a paid account. For the single-machine runs alone, `CLIENTS=1` with
  smaller machine types fits in 12 vCPUs.
- **The synthetic API.** It lives outside this repository. `export-fixtures.sh` copies its
  pre-rendered pages from a running instance, and `up.sh` builds its Axum server from a checkout,
  given as `SYNTHETIC_REST`. The server serves the pages from RAM, uncompressed, so serving costs
  almost no CPU. Note that it only accepts `limit` 500 or 5000.

## Running it

```bash
# 1. Copy the taxi fixtures (about 1.1 GB of gzip for 2gb and 10gb) from a running synthetic API.
SOURCE_API=http://host:18600 ./export-fixtures.sh ~/bench-fixtures 2gb 10gb

# 2. Upload them, create the VMs and wait for setup (about 5 minutes).
SYNTHETIC_REST=~/synthetic-rest ./up.sh ~/bench-fixtures

# 3. Single-machine benchmarks at each link cap, run from bench-client-0.
./bench.sh "1 10 25" "10gb"

# 4. The scan on the cluster, at each link cap and worker count.
./cluster.sh "10 25 off" "2 4 8" "10gb"

# 5. Delete the VMs. The bucket of fixtures stays for the next run; --all deletes it too.
./down.sh
```

`bench.sh` and `cluster.sh` copy the client's `results.tsv` into `results/`. Rename it after a run,
as the dated files there are, before the next run overwrites it.

The all-columns pass, where neither DuckDB nor apilytics can skip unused fields, has no wrapper:

```bash
. ./env.sh
ip=$(gc compute instances describe "$SERVER" --zone "$ZONE" --format 'value(networkInterfaces[0].networkIP)' | tr -d '\r')
./shape.sh 25
on bench-client-0 "/opt/bench/allcols.sh $ip 10gb 25 8 8"    # 8 workers of 8 cores; 0 0 skips the cluster
./shape.sh off
```

## Files

| File | What it does |
|---|---|
| `env.sh` | Settings, each overridable from the environment: project, zone, machine types, client count, Spark version, the apilytics jar (the `dev` build of main by default) |
| `export-fixtures.sh` | Copies fixtures from a running synthetic API |
| `up.sh` | Uploads fixtures and client scripts to a bucket, creates any missing VMs, waits for setup |
| `server-startup.sh` | API server boot: fixtures into tmpfs, builds and starts the Axum server and an iperf3 server |
| `client-startup.sh` | Client boot: Java 17, Spark, the apilytics jar, DuckDB |
| `shape.sh` | Caps the server's outbound rate with a token bucket (`tbf`), or removes the cap |
| `bench.sh` | Runs `client/run.sh` on bench-client-0 at each cap |
| `cluster.sh` | Starts the Spark master and N workers, runs `client/cluster-run.sh` at each cap |
| `down.sh` | Deletes the VMs, and with `--all` the bucket |
| `client/run.sh` | iperf3, naive Java, DuckDB and apilytics in local mode, against one dataset |
| `client/cluster-run.sh` | The apilytics scan on the standalone cluster, one partition per executor core |
| `client/allcols.sh` | The all-columns pass |
| `client/Naive.java`, `client/duck.py`, `client/spark-sql.sh` | The baselines, and the local-mode launcher |

## Reading the numbers

- **Time** for apilytics is the second of two scans in one Spark session (Spark's `Time taken`), so
  it leaves out JVM start, catalog load and JIT warm-up. The naive client and DuckDB are timed from
  their first request.
- **CPU** for local mode is the session's user and system time, minus a session that only loads
  the catalog, halved for the two scans. Cluster CPU is spent on the workers and isn't recorded.
- **CPU platform matters.** N2 VMs land on Cascade Lake or Ice Lake unless pinned, and per-core
  results differed by up to 45%. `MIN_CPU_PLATFORM` pins Ice Lake by default. Only compare results
  measured on the same platform.
- **The API server can become the limit** above about 12 Gbps. Nobody has measured its CPU during a
  run, so a 25 Gbps result that stops near 12–13 Gbps may be the server's ceiling, not the client's.

## Things that went wrong, so you don't repeat them

- **Spot VMs** are cheaper, but GCE deleted a spot API server mid-run. `PROVISIONING` defaults to
  `STANDARD`.
- **Git Bash on Windows** rewrites arguments that start with `/` into Windows paths, so `on` starts
  every remote command with `cd &&`, and the copies back use `MSYS2_ARG_CONV_EXCL`. gcloud's output
  there ends lines with `\r`, which the scripts strip, and its PuTTY-based `scp` copies one remote
  file per call.
- **Stopping a run** on Windows can leave its `gcloud` processes running, and two runs at once share
  the API server and the client's output files. Check for leftovers before starting another.
- **`pkill -f` inside an `on` command** matches the SSH session's own command line and kills it.
