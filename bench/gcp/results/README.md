# Results

Taxi `clean` dataset, offset pagination with 5,000 rows a page, `http.compression = false`. The
three-column query is a `count(*)` and three sums; the all-columns query adds a hash over every
column. Every row here matched the dataset's manifest. The raw rows are in the `.tsv` files beside
this one.

## 2026-10-06: 96 vCPUs, 10 GB

API server n2-highmem-16; client 0 n2-standard-16; workers n2-standard-8, all on Cascade Lake.
Measured link: 9.6 Gbps at the 10 Gbps cap, 23.9 at 25, 31 uncapped.

**One 16-vCPU machine, at 25 Gbps:**

| Client | 3 columns | All columns |
|---|---|---|
| DuckDB, 16 threads | 6.6 s, 89 CPU-s | 9.2 s, 130 CPU-s |
| apilytics, `local[16]` | 27.7 s, 432 CPU-s | 46.4 s, 713 CPU-s |
| apilytics, 1 partition | 171 s | |
| DuckDB, 1 thread | 62 s | |
| naive Java | 98 s | |

**Cluster, three columns, scan time:**

| Workers | vCPUs | 10 Gbps | 25 Gbps | Uncapped |
|---|---|---|---|---|
| 2 | 16 | 19.5 s | 19.6 s | 20.5 s |
| 4 | 32 | 10.7 s | 10.5 s | 10.5 s |
| 8 | 64 | 8.8 s | 6.0 s | 6.1 s |

With all columns, 8 workers took 12.5 s.

## 2026-10-05: 12 vCPUs

API server n2-highmem-4 (10 Gbps outbound cap); client n2-standard-8. The CPU platform wasn't
recorded, and per-core times were about 30–45% faster than the 2026-10-06 run's, which suggests
Ice Lake. Measured link: 0.96 Gbps at the 1 Gbps cap, 9.4 at 10.

| Client | 10 GB at 1 Gbps | 10 GB at 10 Gbps |
|---|---|---|
| DuckDB, 8 threads | 84.8 s | 11.6 s, 71 CPU-s |
| apilytics, `local[8]` | 87.5 s | 29.3 s, 227 CPU-s |
| DuckDB, 1 thread | 85.5 s | 56.1 s |
| naive Java | 84.4 s | 76.4 s |
| apilytics, 1 partition | 119 s | 116.5 s |

## What they show

- **At 1 Gbps the link decides,** for every client but a single apilytics partition. That one is
  limited by fetching a page, converting it, then fetching the next: about 86 MB/s on Ice Lake,
  whatever the link.
- **Above that, apilytics is limited by CPU.** It uses about 4–5 times DuckDB's CPU per GB, with
  three columns or all of them. Most of it goes to parsing pages into a circe tree and converting
  values to Arrow.
- **It scales with workers:** doubling them roughly halves the scan, and 8 workers fill a
  10 Gbps link. 2 workers (16 vCPUs) were faster than `local[16]` on one machine with the same
  vCPU count.
- **Rule of thumb:** apilytics needs about 4 times DuckDB's cores for the same throughput. About
  64 vCPUs of cluster fill 10 Gbps.
