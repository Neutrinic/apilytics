# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
