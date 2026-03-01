# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
