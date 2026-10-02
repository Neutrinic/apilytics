# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-08-26

First stable release. The version marks architectural stability rather than a burst of
features: the source layer is protocol-neutral, one jar covers the whole Spark 4.x line,
and the published coordinate is settled.

### Breaking

- **Unknown config keys are now rejected.** A config that loaded before because a
  misspelled key was silently ignored will now fail to load, naming the key. That is the
  point of the change — the ignored key meant the setting never applied — but it will
  break configs that appeared to work (#234).
- **Scala 2.13.18** minimum, required by SIP-51 given the dependency classpath.

The coordinate is unchanged: **`io.github.neutrinic:apilytics_2.13`**, as in 0.8.0. An
earlier plan to move the Spark line into the artifact name was reverted once one jar was
shown to cover Spark 4.0 through 4.2 (#232). Spark support widened rather than narrowed,
so no upgrade is required for existing users.

### Added

- **A documentation site**, neutrinic.github.io/apilytics, published on each release:
  - one version per minor release, with a version selector
  - the README's user documentation, now split into pages
  - examples included from `examples/`, which CI loads
  - the README cut back to a quick start and a link
  - deploy guides for every platform tested: standalone, YARN, Kubernetes and its Spark Operator, Databricks on AWS and Azure, EMR, and Dataproc
  - a reference for every config key (#274)

  It counts page views with a cookie-free Scarf pixel, which its Privacy page discloses
  (#272).
- **`format("apilytics")`** — the catalog's tables as a data source, for platforms that own
  the catalog namespace. On Databricks, every catalog name on Unity Catalog compute resolves
  to Unity Catalog and a `spark.sql.catalog.*` plugin is never loaded (#129).
  `spark.read.format("apilytics").option("config", ...).option("table", ...)`, `readStream`,
  and `CREATE TEMPORARY VIEW ... USING apilytics` all return the same table as the catalog,
  including pushdown and streaming (#257).
- **Offset partitioning** — `partition { type = "offset", size = 400, count = 4 }` splits an
  offset-paginated endpoint into windows read in parallel, for endpoints that offer no
  natural key to split on. Partition `i` covers `[i * size, (i + 1) * size)`: it starts the
  paginator at its own offset and stops it after one window. Verified against PokeAPI on a
  two-node cluster: 1351 records across four partitions, no duplication (#248).

- **Streaming reads.** REST endpoints can be consumed as a Structured Streaming
  micro-batch source, so a materialized view over an API can refresh incrementally
  instead of re-reading everything. Requires timestamp checkpointing: a pull-based API
  cannot report how much is available without being asked, so the offset is the clock and
  each batch filters on a "changed since" parameter. Cursor and offset checkpointing
  cannot express that and stay batch-only, reported at analysis rather than at run
  time (#36).
- **`Trigger.AvailableNow` support**, which Declarative Pipelines uses for every run. The
  end of a run is now fixed when it is prepared; reading the clock instead meant a run
  meant to drain and stop chased records arriving while it worked. Verified end to end:
  SDP streaming tables materialize from an API across repeated runs (#36).

- **Spark 4.0, 4.1 and 4.2 are all supported by one jar**, each built and tested in CI.
  The connector uses no API newer than 4.0, so the whole line works without version
  branching. 1.x targets Spark 4.x; 2.x is reserved for Spark 5 (#232).
- **jackson-databind is pinned per Spark version.** `jackson-module-scala` enforces a
  narrow databind range, and which range applies varies by patch, not just by line: 4.0.x
  wants [2.18, 2.19), 4.1.0–4.1.2 want [2.20, 2.21), and 4.1.3 onward wants
  [2.21, 2.22). One pin cannot serve them all, and an unrecognised Spark version fails the
  build rather than resolving to a guess (#232, #246).
- **Protocol-neutral source layer** (#191) — `core.source` defines how any protocol
  supplies tables and records, with REST as the first implementation. Spark-layer code no
  longer reaches into HTTP or OpenAPI types, which is what makes further protocols additive.
- **Per-table pagination** — `pagination { … }` inside a table overrides the source-level
  setting. Pagination belongs to an endpoint, not to an API (#217).
- **`prefetch-batches`** — bounds how many Arrow batches the reader holds ahead of Spark.
  Peak reader memory is now `prefetch-batches × arrow-batch-size`, both configurable (#37).
- **Working parent-child example** against a public API, plus documentation of the two
  traps that silently return zero rows (#217).
- **OSV vulnerability scanning** on every PR via a CycloneDX SBOM, matching by package
  coordinate rather than guessing CPEs (#199).
- **Config-relative spec paths** — a relative `openapi` now resolves against the directory
  holding the config file rather than the process working directory, so a spec bundled
  beside its config is found wherever the pair is mounted. URLs and absolute paths are
  unaffected (#225).
- **A documented compatibility contract** — the README now states what semver covers: the
  catalog class name, the config schema, the SQL surface and the artifact coordinate.
  Everything else under `com.apilytics.*` is internal (#225).
- **Spark Declarative Pipelines support**, with a worked example under `examples/sdp`.
  No connector code was needed: SDP has no custom source API, so pipelines read through
  the catalog like any other Spark job. The image now ships the Spark Connect Python
  client that SDP requires, and `spark-pipelines` is a recognised entrypoint command
  (#35).

### Fixed

- **Link-header pagination could send credentials to another host.** It followed any
  `next` URL a response named, and every request carries the source's authentication, so
  a `Link` header pointing elsewhere received the API's token, even over plain HTTP. With
  authentication configured, `next` links must now stay on the API's own origin; another
  origin fails the read. Relative `next` links are now resolved against the request
  (#288).
- **Offset pagination stepped by the page size, not the records received.** Three
  consequences:
  - an offset partition whose size wasn't a multiple of the page size read past its window
    into the next one (two windows of 150 over 300 records returned 350 rows)
  - a window stopped short when the API served smaller pages than asked for
  - an offset checkpoint after a short page saved the requested size, so records appended
    after the last one read were skipped

  Requests now ask only for what a window still needs, the offset advances by the records
  each page held, and a window reads until it's full (#292).
- **Enum partitioning overrode a filter pushed to its parameter.** `WHERE id = 1` with `id`
  pushed to `kind` returned every enum value's records, because planning replaced the
  pushed value after Spark had dropped the predicate. Such a query now reads one partition
  with its own value (#292).
- **A date-range partition's `range` must now be at least 1 millisecond.** A zero range
  never advanced, and planning grew its partition list without end (#292).
- **OAuth2 client credentials didn't work in Spark reads.** Every reader built its HTTP
  client without a token manager, so `auth.type = oauth2_client` with `client-id`,
  `client-secret` and `token-url` failed on the first request, asking for a pre-fetched
  token. Tokens are now fetched, renewed and refreshed on a 401 in table scans, COUNT
  pushdown and aggregate pushdown alike. An `oauth2_client` config missing part of the
  credentials now fails when it loads, naming what's missing (#276).
- **Retries escaped the rate limit.** A JSON request took one rate-limit permit before its
  first attempt, and its retries after 429, 5xx or network errors took none. So a burst of
  failures was retried above `http.rate-limit`, which is exactly when an API is asking for
  less. Every attempt now takes a permit (#279).
- **A failed read could move a batch checkpoint past undelivered records.** The state was
  saved in the page stream's finaliser, which also runs on failure and cancellation, so
  the next run skipped what the failed one never delivered. It's now saved only when the
  reading Spark task succeeds. A reading task that fails, fails afterwards in its own
  work, or is killed leaves the checkpoint where it was. It follows the reading task, not
  the whole query, so a failure in a later stage still comes after the checkpoint has
  moved; the checkpoints page says so (#280).
- **Batch joins couldn't run.** The loader requires a `join-strategy = batch` endpoint
  without a path placeholder, but the table required one to name its parent key column,
  so every batch join the loader accepted failed when the table was built. A batch join
  now names the column after `parent-key` (`_parent_id`) and reads end to end (#277).
- **The response cache could serve one catalog another's data.** It's shared by every
  source in a JVM, and its entries were keyed by request path and parameters only. Two
  catalogs requesting the same path on different hosts, or with different credentials,
  could be served each other's responses. Keys now include the host and a hash of the
  credentials (#278). They also include the URL's own query string: link-header
  pagination follows each next link with its query inside the URL, so later pages shared
  one entry, and page 3 was served page 2 (#286).
- **An unreachable host took minutes per request to fail.** ember's timeout does not
  cover opening the connection, so each attempt waited for the operating system's TCP
  connect timeout, about two minutes on Linux. On EMR Serverless without a VPC, where
  public APIs are unreachable, one task took 14 minutes to fail. Acquiring each response,
  including connecting, is now bounded by `http.timeout`, for data and OAuth2 token
  requests alike, and the error names the host it could not reach (#266).
- **A reader could start reading before it was fully constructed.** The reader base class
  started its producer fiber in its own constructor, which runs before a subclass's fields
  are assigned, so the producer could see a null Arrow schema. It surfaced as an
  intermittent `NullPointerException` in tests; on a cluster it would be an occasional
  failed task, mostly hidden by task retries. The producer now starts on the first read
  (#262).
- **apilytics' dependencies clashed with the copies Spark platforms ship.** Whichever copy
  of a library loads first wins, and that broke apilytics in four ways: jackson (#185);
  duplicate jars under `userClassPathFirst=true`, failing with
  `LinkageError: loader constraint violation ... org.slf4j.Logger` (#255); a YAML module
  missing on Dataproc Serverless, `NoClassDefFoundError: YAMLFactory` (#259); and on
  Databricks DBR 18 an older, repackaged cats that stopped http4s initialising, which
  **hung** the read until the platform killed the executor (#264). The published jar now
  bundles its dependencies relocated under `com.apilytics.shaded`, and its POM declares only
  Spark, so `--packages` resolves a single jar and no platform copy can stand in for ours.
  Libraries `spark-sql` itself depends on (jackson core, guava, commons-*, slf4j) are
  shared with Spark rather than bundled. If a clash ever does get through, apilytics now
  checks its HTTP stack on the driver and in each task and fails at once, naming the
  library, rather than hanging. CI verifies that every bundled class is relocated, and
  runs the published jar on `spark-sql`'s dependencies alone (#264).
- **Partitioning on a parameter pagination controls returned every record once per
  partition.** Both write the same query parameter and the paginator wins, so each
  partition walked the endpoint from its own first page to the end rather than covering a
  slice. Nothing errored: against PokeAPI with four `offset` partitions the query returned
  5404 rows for 1351 distinct records, and spent four times the API calls against the rate
  limit. Now rejected at config load, for enum `param` and date-range
  `start-param`/`end-param`, against whichever pagination that table actually uses (#248).

- **The build was broken against Spark 4.1.3.** The jackson-databind pin was keyed on the
  Spark line, assuming a line ships one `jackson-module-scala`. It does not: 4.1.0–4.1.2
  ship 2.20.x wanting `[2.20, 2.21)` while 4.1.3 onward ships 2.21.x wanting
  `[2.21, 2.22)`. The pin is
  now keyed on the full version, and the CI matrix covers the floor, the newest patch of
  each line, and both sides of that shift — a matrix of one patch per line could not see
  it (#246).

- **Retries multiplied requests and escaped the rate limiter.** Ember retries internally by
  default, beneath both our retry loop and the limiter, so every request was issued
  `3 × (max-retries + 1)` times — three times even with retries switched off. Because
  `rateLimiter.acquire` runs once per attempt *we* make, those extra requests were never
  counted: a source configured for 60 requests per hour could issue 180 against a flaky
  connection, with nothing in the limiter's view to show it. Ember's retry policy is now
  disabled so retrying happens in one place (#245).

- **Colliding pushed filters silently dropped a predicate.** Two predicates resolving to
  the same query parameter — `created_at >= X AND created_at <= Y` against a single
  `since` — kept only one value, while reporting both as pushed. Spark removes a pushed
  predicate from the plan and never re-checks it, so the discarded bound was applied
  nowhere and the query returned rows outside the range. The first claimant now wins and
  the rest stay local (#243).
- **Retries replayed a partly-delivered stream.** The retry handler wrapped the response
  body, not just its acquisition, so a connection lost after records had been emitted
  re-issued the request from the start — redelivering records the consumer already had and
  splicing a partial record onto the new response. Measured against a socket that resets
  mid-body: three requests where there should be one. Retrying now stops at the first byte
  handed to the consumer; a failure before that point is still retried (#243).
- **The README advertised aggregation pushdown for MIN, MAX and custom functions**, which
  the planner declines by design — their result type is not decidable at plan time (#243).

- **Streaming lost every record landing exactly on a batch boundary.** `since` is
  exclusive on most APIs and the batch end was exclusive too, making the window
  `(start, end)` — so a record whose timestamp equalled a boundary belonged to no batch:
  trimmed from the one that fetched it, then skipped by the next batch's `since`. Silent,
  permanent loss, once per boundary. Windows are now `(start, end]`. Against an API whose
  `since` is inclusive this yields a duplicate per boundary instead, which is the
  documented at-least-once behaviour — and a duplicate is recoverable where a loss is not
  (#36).

- **Streaming could silently drop records.** Offsets were rendered with `ISO_INSTANT`,
  which emits the clock's own precision, and record timestamps were compared as strings.
  `'.'` sorts below `'Z'`, so a second-precision record compared as *not* earlier than a
  fractional batch bound and was trimmed — then skipped by the next batch's `since` and
  lost rather than duplicated. Offsets are now fixed-width to the second, and timestamps
  are compared as instants (#36).
- **Streaming dropped records from second-precision APIs.** A batch ended at the current
  second, so records written later in that second — stamped with it, but not yet visible
  when the batch ran — equalled the batch end and were then excluded by the next batch's
  `since`. On a three-node cluster against a second-precision feed that was 273 of 1192
  records. Batches now end at the last fully elapsed second, for at most a second of
  added latency (#250).
- **`close()` could deadlock** — a cancelled reader's uncancelable finalizer parked forever
  offering its end-of-stream sentinel into a full queue, hanging the task rather than
  failing it. Fires whenever Spark stops early: a satisfied `LIMIT`, a failed task, a
  cancelled job (#203).
- **`COUNT(*)` pushdown crashed during optimization** — `readSchema` advertised the table's
  columns while the reader returned one aggregate value, so the query failed before reading
  anything (#212).
- **`SUM`/`AVG` pushdown** now works, with result types fixed at plan time so an API
  answering `42` on one call and `42.0` on the next still matches the schema (#213).
- **The rate limit could exceed itself** — a limit below the partition count fell back to
  1 rps per partition, allowing more than the configured total. Integer division also
  discarded the remainder silently. Shares now sum exactly, and an impossible split fails
  at planning (#205).
- **Parent-child joins returned nothing** when the child nested its records or paginated
  differently from the list endpoint (#217).
- **Arrow conversion was not actually lazy** — every batch in a page was allocated up front,
  so peak memory tracked page size and `arrow-batch-size` had no downward effect. Peak
  dropped ~70% at small batch sizes (#37).
- **Checkpoint store crashed on executors** under Spark 4.1+, where `SparkSession.active`
  changed the exception type it throws and silently defeated the fallback (#186).
- **jackson-databind pin broke Spark 4.1+** — it sat outside the range
  `jackson-module-scala` enforces, taking out Spark's error machinery (#185).
- **Releases would have carried the wrong version** — `version` was set literally in
  `build.sbt`, which outranks the value sbt-ci-release derives from the git tag. Tagging
  `v1.0.0` would have published `0.8.0`, and because the artifact was renamed there was no
  existing artifact for the repository to reject (#225).
- **The bundled Slack example never worked in the image** — its config named a spec under
  `/opt/spark/examples/`, but the image copies examples to `/opt/apilytics/examples/`
  (#225).
- **`pandas` was unpinned in the image** and resolved to 3.0.5, above the range PySpark
  supports. Now pinned below 3.0 (#35).
- **The PokeAPI examples fetched their own spec over the network** at query time, from the
  tip of the default branch, so editing the spec retroactively changed what a released
  example did. They now read the copy shipped beside them (#225).
- **Security pins never reached consumers** — `dependencyOverrides` is resolution-time only
  and is not published, so released artifacts resolved vulnerable transitive versions.
  Fixed at the source by upgrading `swagger-parser` (#188).

### Removed

- **Three unreachable row-based readers** (343 lines). Spark never calls `createReader`
  while `supportColumnarReads` is true, so they could not run — two had been orphaned when
  columnar reads arrived, one was unreachable from its first commit (#211).

### Changed

- Dependencies refreshed: http4s 0.23.38, cats-effect 3.7.1, fs2 3.14.0, circe 0.14.16,
  swagger-parser 2.1.48, typesafe-config 1.4.9. Arrow and netty are no longer declared:
  they come from Spark (#268).
- Build: sbt 1.13.0, sbt-assembly 2.5.0, sbt-ci-release 1.12.1, sbt-dependency-check 2.0.0
  (DependencyCheck 13) (#268).
- Docker images are based on UBI 9.8 rather than UBI 8.10, with Almond 0.14.5 and a
  Scala 2.13.18 kernel (#268).
- Docker images run as the non-root user `spark` (uid 185) rather than root, and Jupyter
  no longer needs `--allow-root`. The working directory, where spark-sql and Thrift
  create `metastore_db` and `spark-warehouse`, is now `/home/spark`. To upgrade an
  existing compose cluster, either start fresh with `docker compose down -v`, or keep the
  event history:
  1. Change the owner of the `spark-events` volume once:
     `docker run --rm -u 0 -v apilytics-spark_spark-events:/e --entrypoint chown apilytics-spark-spark-master -R 185:185 /e`
  2. Recreate the cluster with `docker compose up -d --build --renew-anon-volumes`.

  Without step 2, Compose reattaches the old image's root-owned `work` and `logs`
  volumes, and every executor fails with
  `Failed to create directory /opt/spark/work/app-...` (#270).
- OWASP dependency-check is now advisory and weekly rather than a merge gate. It identifies
  dependencies by guessing CPEs, and every suppression carried was a misidentification;
  OSV (#199) does the gating instead (#200).
- Security scans use the NVD bulk data feed instead of paging the API — runs went from
  timing out after hours to about seven minutes (#197).

### Known limitations

- `MIN`/`MAX` and custom aggregates are not pushed down; they fall back to a full scan with
  Spark computing the result (#213).
- Aggregate pushdown is REST-specific and will not carry to future protocols without
  further work.
- Filter pushdown covers `=`, `>`, `>=`, `<`, `<=` for columns declared in a table's
  `filters` config. Anything else filters client-side after a full scan.
- Read-only. No writes, no streaming source yet (#36).

## [0.8.0] - 2026-02-28

### Added
- **OWASP dependency check** in CI - weekly scans with CVSS 7+ threshold (#143)
- **Credential scrubbing** in error response bodies - strips GitHub, Slack, OpenAI, AWS, Bearer/Basic auth, and JWT tokens from `ApiError.responseBody` before logging (#161)
- **Plaintext HTTP warning** - logs WARN when `base-url` or `token-url` uses `http://` with auth configured (#163)
- **Security documentation** - credential management best practices in README, example configs annotated (#142)
- **Dependabot** for GitHub Actions dependency updates

### Security
- Redact sensitive query parameters (api_key, token, password, etc.) in `ApiError.getMessage` (#140)
- Limit error response body to 4 KB to prevent OOM from malicious servers (#141)
- Skip provided-scope deps in OWASP scan instead of maintaining suppressions (#170)

## [0.7.0] - 2026-02-20

### Added
- **Checkpoint support** for incremental reads - persist pagination state (cursor, timestamp, offset) across queries (#49)
- **Streaming REST support** - NDJSON and Server-Sent Events response formats (#67)
- **VARIANT schema mode** - single native VARIANT column for schema-free queries (#137)
- **Swagger 2.0 (OpenAPI 2.x) support** (#138)

### Fixed
- Deduplicate aggregation endpoint calls when multiple aggregations share the same endpoint (#131)
- Jupyter section in README updated to use dist image (#135)

## [0.6.0] - 2026-02-17

### Added
- **Jupyter notebook support** with dual catalog configuration
- **PySpark support** in Docker image (#127)

## [0.5.0] - 2026-02-15

### Added
- **Configurable aggregation pushdown** - SUM, AVG, MIN, MAX, COUNT, and custom functions (#126)
- **Query statistics reporting** via `SupportsReportStatistics` (#47)
- **Spark Thrift Server** for JDBC access (#121)

### Fixed
- Exploded views ignoring `data-path` configuration (#122)

## [0.4.0] - 2026-02-14

### Added
- **Maven Central publishing** - Available as `io.github.neutrinic:apilytics_2.13:0.4.0`
- **Batch join strategy** for parent-child tables - reduces API calls from O(n) to O(n/batch_size) for bulk lookups
- **COUNT(*) aggregation pushdown** - single API call instead of fetching all pages when count endpoint configured
- **Distributed rate limiting** - automatically divides rate limits across partitions to maintain overall throughput
- **Enum partitioning** - parallel reads by category/status values (e.g., `state=open`, `state=closed`)

### Fixed
- NPE in RESTColumnarPartitionReader when partition serialized across Spark executors

### Changed
- PartitionConfig refactored from flat case class to sealed trait (DateRange, Enum variants)

## [0.3.0] - 2026-02-12

### Added
- Date-range partitioning for parallel time-based reads
- Response caching with ETag/mtime validation
- OpenAPI spec caching for faster cold starts

## [0.2.0] - 2026-02-10

### Added
- Parent-child table joins (nested loop strategy)
- Filter pushdown for API query parameters
- Limit pushdown to stop pagination early
- VARIANT type support for ambiguous schemas

## [0.1.0] - 2026-02-08

### Added
- Initial release
- OpenAPI 3.x spec parsing
- Spark DataSource V2 catalog plugin
- Pagination support (link header, cursor, offset)
- Authentication (bearer, basic, header, OAuth2 client credentials)
- Arrow columnar format conversion
