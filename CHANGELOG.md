# HVPDB v1.0.8 — Hooks, GraphQL & Performance Analysis Release

## [1.0.8] - 2026-03-28 (Feature + Bugfix Release)

### 🐛 Bug Fixes (Critical)

**Exception Handling Improvements**
- Replaced 13 bare `except: pass` statements with proper `warnings.warn()` logging:
  - WAL file locking & cleanup (`wal.py`: 7 locations)
  - Storage initialization & reload callbacks (`storage.py`: 2 locations)
  - Replay/verification logic (`wal.py`: 4 locations)
- Now all silent failures generate proper warnings for debugging

**Memory Leak Prevention**
- Added `unregister_reload_callback()` method in `HVPStorage` to prevent callback buildup
- Implemented `__del__()` in `HVPGroup` to cleanup callbacks when group is garbage collected
- Fixed duplicate `_file_handle = None` statement in WAL `_close_internal()` method

**Security Hardening**
- Added null-checks for `self.security` in WAL `_write_entry()` and `write_batch()` methods
- Prevents AttributeError crash if security context not initialized
- Raises explicit `RuntimeError` with clear message instead of silent failure

### ✨ New Features

**1. Hooks/Triggers System (v1.0.8+)**
- `register_hook(hook_type, callback)` - Register document lifecycle callbacks
- `unregister_hook(hook_type, callback)` - Cleanup hooks
- Supported hooks:
  - `pre_insert`, `post_insert` - Insert lifecycle
  - `pre_update`, `post_update` - Update lifecycle
  - `pre_delete`, `post_delete` - Delete lifecycle
- Example:
  ```python
  db.users.register_hook('pre_insert', lambda doc: print(f"Inserting: {doc['_id']}"))
  db.users.insert({'name': 'Alice'})  # Prints: Inserting: <uuid>
  ```

**2. GraphQL API Endpoint (v1.0.8+)**
- Auto-generated GraphQL schema from database groups
- New endpoint: `POST /graphql`
- Queries:
  - `groups()` - List all available groups
  - `group_docs(group_name, query_json)` - Retrieve documents with optional filtering
- Graceful fallback if Strawberry library not installed
- Example:
  ```graphql
  query {
    groups
    groupDocs(groupName: "users", queryJson: "{\"role\": \"admin\"}")
  }
  ```

**3. Query Profiler & EXPLAIN (v1.0.8+)**
- `group.explain(query)` - Detailed query execution plan
  - Returns: execution strategy, index usage, estimated documents scanned, timing
  - Strategies: `unique_index`, `index_scan`, `full_scan`
- `group.profile(operation, query)` - Performance metrics
  - Operations: `find`, `insert`, `update`, `delete`
  - Metrics: execution time (ms), memory delta, status
- Example:
  ```python
  # Get query plan
  plan = db.users.explain({'email': 'alice@example.com'})
  print(f"Strategy: {plan['execution_strategy']}, Indexed: {plan['has_indexes']}")
  
  # Profile an operation
  perf = db.users.profile('find', {'role': 'admin'})
  print(f"Found {perf['docs_found']} docs in {perf['execution_time_ms']:.2f}ms")
  ```

**4. Default Values Support (v1.0.8+)**
- Schema fields with `default` or `default_factory` are automatically applied on insert
- Works with Pydantic models
- Example:
  ```python
  from pydantic import BaseModel, Field
  
  class User(BaseModel):
      name: str
      role: str = Field(default="user")  # Applied on insert
      created_at: datetime = Field(default_factory=datetime.now)
  
  db.users.insert({'name': 'Alice'})  # role='user' is auto-filled
  ```

### 📊 Summary

| Metric | Count |
|--------|-------|
| Bug fixes | 8 (exceptions, memory leaks, duplicate code, null checks) |
| New features | 4 (hooks, GraphQL, profiler, default values) |
| Warnings added | 13 locations |
| Test coverage | Existing suite covers new features |
| Breaking changes | None (fully backward compatible) |

> **Release Date**: 2026-03-28  
> **Diff Stats**: 15 files changed, +~800 insertions, −~50 deletions  
> **Python Version**: 3.7+  
> **Dependencies Added (Optional)**: `strawberry-graphql` for GraphQL support

---

# HVPDB v1.0.7 — Full Changelog (since v1.0.6)

## [1.0.7.post1] - 2026-02-17 (Hotfix)
### Fixed
- **TTL Reaper**: Fixed bug where expired documents were hidden from reaper.
- **Thread Safety**: Fixed race conditions in `find_iter` and `find_one` reload checks.
- **Stale Reads**: Fixed stale read vulnerability in `find_one` optimization.
- **Backup**: Fixed `db.backup()` failures on new databases (forced checkpoint).
### Added
- Internal support for `_include_expired` query parameter.

> **Release Date**: 2026-02-16  
> **Diff Stats**: 33 files changed, +6,076 insertions, −1,012 deletions

---

## 🔒 Security (Critical)

### Timing Attack Prevention
- Password verification (`_verify_password`) now always performs cryptographic work regardless of input validity
- Uses `secrets.compare_digest()` for constant-time comparison
- Prevents user enumeration via response timing analysis

### TOCTOU Race Condition Fix
- File locking now uses atomic `os.open()` with `O_CREAT | O_EXCL` flags
- Eliminates Time-Of-Check-To-Time-Of-Use race in lock acquisition

### Argon2id Hardening
- Increased `memory_cost` to 256MB (from default)
- Stronger resistance against GPU-based brute-force attacks

### Passkey / FIDO2 Authentication (NEW)
- **Files added**: `fido_native.py` (+330 lines), `passkey_auth.py` (+210), `passkey_store.py` (+90), `webauthn_server.py` (+201)
- Windows Hello integration (fingerprint, face recognition, PIN)
- External security key support (YubiKey, etc.)
- CLI commands: `gen-passkey`, `login-passkey`
- Shell flag: `hvpdb shell --passkey --user <username>`
- Credential storage in encrypted JSON (`hvpdb_passkeys.json`)

### Access Key Authentication (NEW)
- CLI commands: `gen-key --qr --save <file>`
- QR code generation for mobile scanning
- Shell flag: `hvpdb shell --access-key <file>`

---

## 🧵 Concurrency & Thread Safety

### Thread-Safe CRUD Operations
- All `HVPGroup` methods (`update`, `delete`, `find`, `find_iter`) now acquire `_thread_lock` (RLock)
- Eliminated `RuntimeError: dictionary changed size during iteration` in multi-threaded scenarios

### `find_iter()` — Thread-Safe Streaming (NEW)
- New generator method that snapshots results while holding lock
- Memory-efficient iteration over large result sets
- Prevents concurrent modification during iteration

### Multi-Process Stale Read Protection (NEW)
- Storage layer checks `st_mtime` before reads
- Automatic data reload when file is modified by another process
- `refresh(force=True)` for explicit resync

---

## 💾 Storage & WAL

### WAL Auto-Checkpoint Fix
- Fixed dead code where WAL threshold check was never triggered
- Checkpoint now fires correctly after configurable number of operations

### Deadlock Fix in WAL Checkpointing
- `commit()` no longer holds writer lock while triggering checkpoint
- Checkpoint scheduling moved outside critical section

### Nested Transaction Prevention
- `begin()` inside another `begin()` now raises `RuntimeError` immediately
- Previously caused silent data corruption

### Emergency Memory Refresh
- On update/delete failure in critical section, forces full memory resync from WAL
- Prevents RAM-WAL state divergence after partial failures

---

## 🖥️ Windows Robustness

### Atomic Write Retry (NEW)
- `os.replace()` now has configurable retry loop for `PermissionError`
- Handles conflicts with Windows Defender, Search Indexer, and other file scanners
- Default: 3 retries with exponential backoff

---

## 🐚 HVPShell Enhancements (+1,710 lines refactored)

### New Commands
| Command | Description |
|---------|-------------|
| `hunt` | Search with regex support (`hunt name=r:^Admin.*`) |
| `query` | Polyglot Query Engine (plugin) |
| `anchor` / `recall` | Bookmark navigation context |
| `fuse` | Merge data from another group |
| `sift` | Filter and analyze data patterns |
| `dedupe` | Detect and remove duplicates |
| `inhale` / `exhale` | Advanced import/export |
| `morph` | Batch update selected documents |
| `select` / `discard` | Multi-selection buffer |
| `monitor` | Real-time database activity |
| `benchmark` | Performance benchmarking |
| `validate` | Data integrity checks |
| `record` | Full WAL interaction (status/list/peek/undo/apply) |
| `become` | Switch user identity |
| `perm` | View permissions across groups |
| `edit` | Open document in system editor |
| `calc` | Built-in calculator |
| `getatour` | Interactive guided tour |

### Cleanup & Refactoring
- Removed duplicate method implementations
- Consolidated alias commands
- Improved error messages and help text

---

## 📡 CLI Additions

### New Commands
| Command | Description |
|---------|-------------|
| `gen-passkey` | Register FIDO2 Passkey |
| `login-passkey` | Authenticate with Passkey |
| `gen-key` | Generate Access Key (+ QR) |
| `config` | Set auth type |
| `diff` | Compare two documents |
| `jump` | Open shell in specific group |
| `dump` | Export query results to file |
| `pack` | Compress DB + WAL to `.hvpz` |
| `snapshot` | Point-in-time snapshot |
| `lock-status` | Check file lock contention |
| `meta` | View/set database metadata |
| `plugin list/info/doctor` | Plugin management |
| `wal status/dump/checkpoint` | WAL management |

---

## 🔌 Plugin System Improvements

- Entry point discovery via `importlib.metadata`
- Plugin health check (`plugin doctor <name>`)
- 8 built-in plugin categories: backup, http, migrate, observe, query, admin, tools, sync

---

## 📦 Connection & Deployment

### HVP URI Format
- Full URI parsing: `hvp://[user:pass@]host[:port][~shards]/db[?options]`
- Password masking for logs (`redacted-uri` command)
- Support for `@` in passwords via `rsplit`

### HTTP Server Hardening
- Timing-safe token comparison (`secrets.compare_digest`)
- Support for both `Authorization: Bearer` and `X-HVP-Key` headers
- Graceful hostname resolution fallback

---

## 🧪 Testing

### New Test Files
- `tests/test_core.py` (+72 lines) — Core CRUD and transaction tests
- `tests/conftest.py` (+23 lines) — Shared test fixtures
- `run_ci.py` (+52 lines) — CI pipeline runner

---

## 📄 Documentation

### New Documentation System
- `hvpdb_definitive_reference.md` — 600+ line complete reference manual
- GitBook documentation fully expanded: 20+ pages covering Architecture, WAL, Transactions, Indexing, Concurrency, Security, CLI, Python API, HTTP Server, Plugin System, Troubleshooting

---

## 📊 Summary

| Category | Changes |
|----------|---------|
| Security fixes | 5 (timing attack, TOCTOU, Argon2, Passkey, Access Key) |
| Concurrency fixes | 3 (thread-safe CRUD, stale reads, find_iter) |
| WAL/Storage fixes | 4 (auto-checkpoint, deadlock, nested tx, emergency refresh) |
| New Shell commands | 20+ |
| New CLI commands | 13+ |
| New source files | 6 (fido_native, passkey_auth, passkey_store, webauthn_server, test files) |
| Lines added | +6,076 |
| Lines removed | −1,012 |
