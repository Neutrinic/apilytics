# Performance

How fast a scan runs depends mostly on the API. The numbers on this page come from
[apilytics-bench](https://github.com/Neutrinic/apilytics-bench). It serves a synthetic
paginated API whose answers are known, and reads it on Google Compute Engine at capped link
speeds. Its results also compare apilytics with DuckDB and a plain JVM client.

## What limits a read

1. **The API.** Each page costs a round trip, and most APIs add latency, rate limits or small
   pages. Against such an API, a scan spends most of its time waiting. More
   [partitions](partitioning.md) put more requests in flight, within the
   [rate limit](rate-limiting.md).
2. **One partition reads one page at a time.** It fetches a page, converts it, then fetches
   the next, so network and CPU take turns. Against a fast API, one partition read 60–90 MB/s
   of JSON, depending on the CPU, whatever the link speed.
3. **CPU.** With enough partitions to keep the link busy, the limit becomes converting JSON
   into Arrow batches. The benchmark measured 20–45 CPU-seconds per GB of JSON, depending on
   the CPU and on how many columns the query reads.

## Sizing

| Link | What kept up with it |
|---|---|
| 1 Gbps | 8 partitions on one 8-vCPU machine |
| 10 Gbps | 8 workers with 8 vCPUs each: 64 vCPUs |
| 25 Gbps | not reached: 64 vCPUs read about 13 Gbps |

- **Use a partition per core** you want busy. Offset partitioning splits any offset-paginated
  endpoint; see [Partitioning](partitioning.md).
- **Workers beat one large machine.** In the benchmark, two 8-vCPU workers read faster than one
  16-vCPU machine in local mode: 19.5 s against 27.7 s for 10 GB.
- **Select only the columns you need.** apilytics converts only the columns a query reads.
  Reading every column of the benchmark's dataset took 1.6 times the CPU of reading three.

## Is a scan waiting or computing?

Compare the scan stage's executor CPU time with its executor run time. Spark's monitoring REST
API reports both for each stage, as `executorCpuTime` (in nanoseconds) and `executorRunTime`
(in milliseconds), at `/api/v1/applications/<app-id>/stages`.

- **CPU time is a small share of run time:** the scan is waiting on the API. Add partitions,
  within the API's rate limit, or ask for larger pages with `max-page-size`.
- **CPU time is close to run time:** the scan is limited by CPU. Add executors or cores, and
  read fewer columns.

## Measured results

Reading 10 GB of JSON at a 25 Gbps cap, with `n2-standard-8` workers on Cascade Lake CPUs:

| Workers | vCPUs | Scan |
|---|---|---|
| 2 | 16 | 19.6 s |
| 4 | 32 | 10.5 s |
| 8 | 64 | 6.0 s |

Every run's row count and sum matched the dataset's manifest. The setup, the scripts and every
measured run are in [apilytics-bench](https://github.com/Neutrinic/apilytics-bench), so a run
can be repeated on other hardware.
