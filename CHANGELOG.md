# Changelog

All notable changes to this project will be documented in this file.

## [1.0.4.post2] - 2026-02-05

### 🛡️ Security & Stability
- **CLI Security Audit**: Fixed a "Fail Open" vulnerability where the shell would exit with code 0 even if authentication failed in batch mode. Now it correctly exits with code 1.
- **Windows Compatibility**: Fixed `PermissionError` warnings related to file permissions (`os.chmod`) on Windows environments.
- **CI/CD**: Added a robust CI pipeline (`run_ci.py`) that includes environment checks, full test suite execution, and build verification.

## [1.0.4.post1] - 2026-02-05

### 🐛 Bug Fixes
- **Serialization**: Fixed `TypeError` when serializing `set`, `datetime`, and `uuid.UUID` objects. Added `default_serializer` to handle these types gracefully.

### 🔐 Security & UX
- **QR Code Upgrade**: The `gen-key` command now generates a QR code containing a URI (`hvpdb://setup?key=...`) instead of raw text, enabling easier integration with mobile apps and "Passkey-like" import workflows.

## [1.0.4] - 2026-02-04

### ♻️ Refactoring & Code Cleanup
- **CLI Shell Optimization**:
  - Removed redundant wrapper functions in `HVPShell` (e.g., `do_del` which merely called existing logic).
  - Standardized Alias System: Used direct function assignment (e.g., `do_cat = do_get`) instead of wrapper functions, reducing boilerplate code in the shell file by 30%.
  - Eliminated duplicate alias definitions for cleaner and more maintainable code.
  - Audited the entire codebase (`core.py`, `cli.py`) to ensure no redundant logic remains.

## [1.0.3] - 2026-02-04

### 🚀 New Features
- **Access Key Authentication**: Added file-based key login (`--access-key`) and random key generation (`gen-key`).
- **QR Code Support**: Added support for displaying Access Keys as QR Codes directly in the terminal (requires `qrcode`).
- **Interactive Shell Upgrade**: Integrated `prompt_toolkit` to replace standard `input()`.
  - Fixed prompt disappearance issue when deleting text on Windows.
  - Added command history support (saved at `~/.hvpdb_history`).
  - Improved typing experience with Emacs-style keybindings.

### ⚡ Performance & Optimization
- **Auto-Checkpoint**: Automatically merge WAL logs into the main file when log size exceeds 10MB to prevent disk bloat.
- **Interruptible Locking**: Fixed deadlock issues on Windows during file locking. Users can now interrupt operations immediately using `Ctrl+C`.

### 🛠️ Bug Fixes & Improvements
- **Storage**: Fixed header writing logic in WAL `truncate()` function.
- **Dependency**: Added `prompt_toolkit` to installation requirements.
- **CLI**: Updated help messages and cleaned up legacy warnings.

---
## [1.0.2] - 2026-01-14
- Initial Stable Release.
