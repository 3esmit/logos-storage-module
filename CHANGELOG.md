# Changelog

Notable Storage Module package changes are recorded here.

## [2.3.0] - Alpha

### Added

- Versioned `nodeStatus`, `nodeAction`, and `nodeChanged` lifecycle contract
  for correlated host control, ordered state observation, and safe retry.
- Lifecycle transition events for legacy Storage lifecycle calls, while keeping
  existing `lifecycleStatus`, `storageStart`, and `storageStop` APIs intact.

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
