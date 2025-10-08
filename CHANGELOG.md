# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2025-10-08

### Fixed

- **Split BAM record headers across BGZF blocks**: Implemented proper handling of BAM record headers that span BGZF block boundaries as per the SAM/BAM specification. Previously, these records were silently skipped during indexing. The fix detects incomplete headers (≤16 bytes) at block boundaries, carries them over to the next block, and processes them correctly. This affects all three indexing strategies (Work Stealing, Channel PC, and Constant Memory) with negligible performance impact (<0.1% overhead) since split records are rare. Code duplication was eliminated by introducing a shared helper function `process_incomplete_with_compressed_block()` used across all strategies.

### Changed

- Refactored `extract_flags_from_block_pooled()` to return tuple `(usize, Option<IncompleteHeader>)` instead of just record count
- All indexing strategies now handle boundary records in their natural pattern while maintaining performance characteristics

## [1.0.0] - Initial Release

### Added

- Fast BAM flag indexing with three strategies:
  - Work Stealing Strategy (fastest, 3.409s)
  - Channel Producer-Consumer Strategy
  - Constant Memory Strategy
- Parallel block processing for maximum CPU utilization
- Memory-mapped file I/O for efficient data access
- Thread-local decompression buffers to avoid allocations
- Compressed index format with delta encoding
- Query engine for filtered record retrieval
- Fast scan-only mode comparable to samtools performance
- Support for complex flag filtering with required/forbidden flags
