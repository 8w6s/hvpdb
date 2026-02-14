# Changelog

## [1.0.6.post1] - 2026-02-14
### Fixed
- **Critical**: Fixed a deadlock in WAL auto-checkpointing where `commit()` held a lock while triggering a checkpoint that required the same lock.
- Validated nested transaction prevention and WAL checkpointing with regression tests.

## [1.0.6] - 2026-02-14
### Fixed
- **Critical**: Blocked nested transactions to prevent data corruption.
- **Critical**: Fixed WAL auto-checkpoint dead code (threshold check was missing).
- **Security**: Fixed TOCTOU race condition in file locking using atomic `os.open`.
- **Security**: Strengthened Argon2id parameters (memory_cost=256MB) to resist brute-force attacks.
- **Consistency**: Added emergency memory refresh on update failure to ensure RAM-WAL consistency.

## [1.0.5.post2] - 2026-02-14
### Fixed
- Fixed WAL auto-checkpoint dead code in `storage.py` (retry).

## [1.0.5.post1] - 2026-02-14
### Reverted
- Reverted to 1.0.5 codebase due to issues in 1.0.6 release.

## [1.0.5] - 2026-02-14
### Added
- Initial release with basic transaction and WAL support.
