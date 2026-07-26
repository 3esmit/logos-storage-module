# Changelog

Notable Storage Module package changes are recorded here.

## [2.2.0] - Alpha

### Added

- Authoritative `lifecycleStatus()` state for host UIs to distinguish initialized, running, and pending Storage lifecycle states.

## [2.1.0] - Alpha

### Added

- Caller-correlated, versioned download API.
- Source-owned prerelease workflow for portable Linux AMD64 and macOS ARM64 packages.

### Fixed

- Bounded download inputs, cancellation, cleanup, and session lifecycle.
- Destination preservation and replacement behavior across supported platforms.
- Error reporting for failed backup replacement operations.
