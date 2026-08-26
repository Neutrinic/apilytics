# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-08-23

First stable release. The version marks architectural stability rather than a burst of
features: the source layer is now protocol-neutral, the Spark support policy is decided,
and the published coordinate is settled.

### Breaking

- **The artifact is renamed.** `io.github.neutrinic:apilytics_2.13` becomes
  **`io.github.neutrinic:apilytics-spark-4-2_2.13`**. The Spark line now lives in the
  artifact name so the version can stay ordinary semver describing this project's own
  changes, and so moving Spark requires an explicit edit to the dependency coordinate that
  no automated bump can cross (#219). The old coordinate stops receiving updates. **No code
  or config changes are required** — only the dependency line.
- **Spark 4.2 is required** (was 4.0/4.1). APIlytics tracks the current Spark release and
  does not maintain a compatibility floor; pin an older release to stay on an older Spark
  (#189).
- **Scala 2.13.18** minimum, required by SIP-51 given the dependency classpath.

### Added

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
  branching. The Spark line has been removed from the artifact name accordingly:
  `apilytics-spark-4-2` is again **`apilytics`**, with 1.x targeting Spark 4.x (#232).
- **jackson-databind is pinned per Spark line.** `jackson-module-scala` enforces a narrow
  databind range and each line ships a different one — 4.0 needs [2.18, 2.19), 4.1 needs
  [2.20, 2.21), 4.2 needs [2.21, 2.22) — so a single pin cannot serve them all (#232).

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

- **Streaming could silently drop records.** Offsets were rendered with `ISO_INSTANT`,
  which emits the clock's own precision, and record timestamps were compared as strings.
  `'.'` sorts below `'Z'`, so a second-precision record compared as *not* earlier than a
  fractional batch bound and was trimmed — then skipped by the next batch's `since` and
  lost rather than duplicated. Offsets are now fixed-width to the second, and timestamps
  are compared as instants (#36).

- **Misspelled config keys were silently ignored.** Every field was read with `hasPath`,
  so a typo was not an error — it was an absent key and a silent default. `tokn` instead
  of `token` under `type = bearer` produced no token and unauthenticated requests against
  a live API, with nothing in the logs to explain it. Unknown keys are now rejected at
  load, with the full dotted path of each (#234).

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

- Dependencies refreshed: Arrow 19.0.0, http4s 0.23.36, circe 0.14.16, fs2 3.13.0,
  swagger-parser 2.1.45, netty 4.2.17, typesafe-config 1.4.9.
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
