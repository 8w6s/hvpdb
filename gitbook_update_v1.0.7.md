# GitBook v1.0.7 Update Instructions

> **Mục đích**: File này chứa tất cả nội dung cần cập nhật trên GitBook (8w6s.gitbook.io/hvpdb) cho phiên bản v1.0.7.
> Copy từng phần và paste vào GitBook AI hoặc chỉnh thủ công.
> Release Date: 2026-02-16

---

## 📌 PAGE 1: Python API Reference

**GitBook Path**: `Reference > Python API`
**URL**: https://8w6s.gitbook.io/hvpdb/reference/python-api

### Nội dung cần THÊM vào cuối trang (hoặc xen kẽ vào đúng section):

---

### `find()` — Cập nhật signature

**Thay đổi**: Thêm tham số `skip` và hỗ trợ query operator `$regex`.

```python
group.find(query=None, limit=0, skip=0) -> List[dict]
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `query` | `dict` or `None` | `None` | Query criteria. Supports exact match and `$regex` operator |
| `limit` | `int` | `0` | Max documents to return (0 = unlimited) |
| `skip` | `int` | `0` | Number of results to skip (for pagination) |

**Regex Query Example:**
```python
# Find all users whose name starts with "Admin"
admins = db.users.find({"name": {"$regex": "^Admin.*"}})

# Case-insensitive search
results = db.users.find({"email": {"$regex": "(?i)@gmail\\.com$"}})
```

**Pagination Example:**
```python
# Page 1 (first 10)
page1 = db.users.find({}, limit=10, skip=0)

# Page 2 (next 10)
page2 = db.users.find({}, limit=10, skip=10)

# Page 3
page3 = db.users.find({}, limit=10, skip=20)
```

**Query Caching**: `find()` automatically caches results. Cache is invalidated on any write operation (insert, update, delete). Cache is limited to 1000 entries to prevent OOM.

---

### `find_iter()` — Thread-Safe Iterator

```python
group.find_iter(query=None) -> Generator[dict]
```

Memory-efficient streaming iterator. Thread-safe: snapshots results while holding lock.

```python
# Stream results without loading all into memory
for doc in db.logs.find_iter({"level": "ERROR"}):
    process(doc)
```

**Key behavior:**
- Acquires `_thread_lock` (RLock) during iteration
- Checks `st_mtime` for external file changes before reading (stale-read protection)
- Automatically skips expired (TTL) and soft-deleted documents
- Uses indexes when available (unique, composite, standard)

---

### `find_one()` — Single Document Lookup

```python
group.find_one(query: dict) -> Optional[dict]
```

Returns the first matching document, or `None`.

```python
user = db.users.find_one({"email": "admin@example.com"})
```

**Optimizations:**
- Direct `O(1)` lookup when query is `{"_id": "..."}` (single key)
- Uses unique index when available for single-field queries
- Falls back to `find(query, limit=1)` for complex queries
- Skips expired and soft-deleted documents automatically

---

### `bulk_insert()` — Batch Insert

```python
group.bulk_insert(docs: List[dict]) -> List[dict]
```

Insert multiple documents in a **single atomic transaction**.

| Parameter | Type | Description |
|-----------|------|-------------|
| `docs` | `List[dict]` | List of document dictionaries |

**Returns**: List of inserted documents with generated `_id` and `_created_at`.

```python
users = [
    {"name": "Alice", "role": "admin"},
    {"name": "Bob", "role": "user"},
    {"name": "Charlie", "role": "user"}
]
result = db.users.bulk_insert(users)
# All 3 inserts happen in one transaction
# If any fails, all are rolled back
```

---

### `bulk_update()` — Batch Update

```python
group.bulk_update(query: dict, update_data: dict) -> int
```

Update all documents matching `query` in a **single atomic transaction**.

| Parameter | Type | Description |
|-----------|------|-------------|
| `query` | `dict` | Criteria to match documents |
| `update_data` | `dict` | Fields to merge into matching documents |

**Returns**: Number of documents updated.

```python
# Deactivate all expired trial users
count = db.users.bulk_update(
    {"plan": "trial", "expired": True},
    {"active": False, "reason": "trial_expired"}
)
print(f"Deactivated {count} users")
```

---

### `bulk_delete()` — Batch Delete

```python
group.bulk_delete(query: dict) -> int
```

Delete all documents matching `query` in a **single atomic transaction**.

| Parameter | Type | Description |
|-----------|------|-------------|
| `query` | `dict` | Criteria for documents to delete |

**Returns**: Number of documents deleted.

```python
# Remove all logs older than 30 days
count = db.logs.bulk_delete({"age_days": {"$gt": 30}})
print(f"Deleted {count} old logs")
```

---

### `soft_delete()` & `undelete()` — Soft Delete Pattern

```python
group.soft_delete(query: dict) -> int
group.undelete(query: dict) -> int
```

Mark documents as deleted without physically removing them.

| Method | Description |
|--------|-------------|
| `soft_delete(query)` | Sets `_deleted: True` on matching documents |
| `undelete(query)` | Sets `_deleted: False` on matching documents |

```python
# Soft delete a user
db.users.soft_delete({"email": "bad@example.com"})

# User won't appear in normal queries
all_users = db.users.find({})  # bad@example.com is excluded

# But can be found explicitly
deleted = db.users.find({"_deleted": True})

# Restore the user
db.users.undelete({"email": "bad@example.com"})
```

**Behavior**:
- `find()`, `find_iter()`, and `find_one()` automatically skip documents with `_deleted: True`
- To query soft-deleted documents, include `{"_deleted": True}` in the query
- Soft-deleted documents still occupy space — use `delete()` for permanent removal

---

### `set_computed_field()` — Dynamic Computed Fields

```python
group.set_computed_field(name: str, func: callable)
```

Register a function that automatically computes a field's value during `insert()`.

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | `str` | Field name to compute |
| `func` | `callable` | Function that receives the document dict and returns the value |

```python
# Auto-compute a "full_name" field on insert
db.users.set_computed_field("full_name", lambda doc: f"{doc.get('first', '')} {doc.get('last', '')}")

db.users.insert({"first": "John", "last": "Doe"})
# Document stored as: {"first": "John", "last": "Doe", "full_name": "John Doe", "_id": "...", ...}

# Auto-compute a hash
import hashlib
db.files.set_computed_field("checksum", lambda doc: hashlib.md5(doc.get("content", b"")).hexdigest())
```

**Note**: Computed fields are evaluated only during `insert()`, not during `update()`.

---

### `resolve_ref()` — Database Reference (DBRef)

```python
group.resolve_ref(ref: dict) -> Optional[dict]
```

Resolve a cross-group document reference.

| Parameter | Type | Description |
|-----------|------|-------------|
| `ref` | `dict` | DBRef object: `{"$ref": "group_name", "$id": "document_id"}` |

**Returns**: Referenced document, or `None` if not found.

```python
# Store a reference in a document
order = db.orders.insert({
    "product": "Widget",
    "customer": {"$ref": "users", "$id": "abc-123"}
})

# Later, resolve the reference
customer = db.orders.resolve_ref(order["customer"])
print(customer["name"])  # "John Doe"
```

---

### TTL Documents (Time-To-Live)

Insert documents that **automatically expire** after a specified duration.

```python
# Insert a session that expires in 1 hour (3600 seconds)
db.sessions.insert({
    "user_id": "abc-123",
    "token": "xyz",
    "ttl": 3600  # seconds
})
# Document stored with: _expires_at = current_time + 3600
```

| Field | Description |
|-------|-------------|
| `ttl` (input) | Seconds until expiration. Removed from stored document. |
| `_expires_at` (auto) | Unix timestamp when document expires |

**TTL Reaper**: A background daemon thread runs every **60 seconds** and permanently deletes expired documents.

**Behavior**:
- `find()`, `find_iter()`, and `find_one()` automatically skip expired documents
- The reaper physically deletes them periodically
- TTL reaper only runs in durable (non-cluster) mode

---

### `create_index()` — Cập nhật signature (Partial Indexes)

```python
group.create_index(field, unique=False, persist=True, condition=None)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `field` | `str` or `list` | — | Field name, or list of fields for composite index |
| `unique` | `bool` | `False` | Enforce uniqueness |
| `persist` | `bool` | `True` | Save index definition to storage |
| `condition` | `dict` or `None` | `None` | **NEW**: Only index docs matching this query |

**Partial Index Example:**
```python
# Only index active users (save memory by not indexing inactive ones)
db.users.create_index("email", unique=True, condition={"active": True})
```

**Composite Index Example:**
```python
# Create a composite index on (country, city)
db.locations.create_index(["country", "city"])

# Query using the composite index
results = db.locations.find({"country": "JP", "city": "Tokyo"})
```

---

### `db.backup()` — Point-in-Time Snapshot

```python
db.backup(path: str)
```

Create a consistent snapshot of the database file.

| Parameter | Type | Description |
|-----------|------|-------------|
| `path` | `str` | Output file path (`.hvp` extension added if missing) |

```python
db.backup("./backups/mydb_2026-02-16.hvp")
```

**Implementation**: Uses `shutil.copy2()` under a **reader lock** to ensure consistency.

---

### `db.repair()` — Auto-Repair

```python
db.repair() -> bool
```

Attempt to repair corrupted storage or WAL.

**Returns**: `True` if repair succeeded, `False` if failed.

```python
if db.repair():
    print("Database repaired successfully")
else:
    print("Repair failed — check backup")
```

**Process**: Acquires writer lock → reload data → re-save clean file → truncate WAL.

---

### `db.refresh()` — Manual Resync

```python
db.refresh(force=False)
```

Re-read the database file from disk.

| Parameter | Type | Description |
|-----------|------|-------------|
| `force` | `bool` | If `True`, reloads even if there are unsaved changes |

```python
# After another process modifies the database
db.refresh()
```

---

### HVPDB Constructor — Cập nhật

```python
HVPDB(path: str, password: str = None, durable: bool = True)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `path` | `str` | — | File path (`.hvp`), cluster directory (`.hvdb`), or `hvp://` URI |
| `password` | `str` | `None` | Password. Falls back to `HVPDB_PASSWORD` env var |
| `durable` | `bool` | `True` | Enable WAL-based durability |

**v1.0.7 Changes:**
- TTL reaper thread starts automatically when `durable=True` (non-cluster mode)
- Stale-read protection: checks `st_mtime` before reads
- Thread-safe: internal `_thread_lock` (RLock) protects all operations

---

## 📌 PAGE 2: Core Concepts

**GitBook Path**: `Concepts > Core Concepts`
**URL**: https://8w6s.gitbook.io/hvpdb/concepts/core-concepts

### Nội dung cần THÊM:

---

### TTL (Time-To-Live) Documents

HVPDB supports automatic document expiration. Set a `ttl` field (in seconds) during insertion, and the document will be:

1. **Filtered out** of queries immediately after expiration
2. **Physically deleted** by a background reaper thread (runs every 60 seconds)

```python
# Cache entry that auto-expires in 5 minutes
db.cache.insert({"key": "homepage", "html": "<h1>Hello</h1>", "ttl": 300})
```

### Soft Delete

Documents can be "soft deleted" by marking them with `_deleted: True` instead of physically removing them. This allows for:
- **Audit trails** — track what was deleted and when
- **Recovery** — `undelete()` restores documents
- **Referential integrity** — other documents can still reference soft-deleted docs

### Computed Fields

Register functions that automatically compute field values during insertion:

```python
db.users.set_computed_field("display_name", lambda d: f"{d['first']} {d['last']}")
```

Computed fields are evaluated only on `insert()`, not on `update()`.

### Query Caching

`find()` caches results in memory. The cache is:
- **Invalidated** on any write operation (insert, update, delete, rollback)
- **Limited** to 1000 entries to prevent OOM
- **Per-group** — each group has its own cache

### Lazy Loading (Cluster Mode)

In cluster mode (`.hvdb`), groups are loaded on-demand when first accessed via `db.group("name")`. This reduces memory usage for databases with many groups.

### Database References (DBRef)

Cross-group references use the format `{"$ref": "group_name", "$id": "doc_id"}`. Resolve them with `group.resolve_ref(ref)`.

---

## 📌 PAGE 3: Indexing & Query Execution

**GitBook Path**: `Concepts > Indexing and Query Execution`
**URL**: https://8w6s.gitbook.io/hvpdb/concepts/indexing-and-query-execution

### Nội dung cần THÊM:

---

### Query Operators

HVPDB v1.0.7 supports the following query operators:

| Operator | Example | Description |
|----------|---------|-------------|
| Exact Match | `{"name": "Alice"}` | Default equality check |
| `$regex` | `{"name": {"$regex": "^A.*"}}` | Regular expression matching (uses `re.search`) |

> **Note**: Additional operators (`$gt`, `$lt`, `$in`, `$ne`) are planned for future releases.

### Partial Indexes

Create indexes that only include documents matching a condition:

```python
# Only index premium users
db.users.create_index("email", condition={"plan": "premium"})
```

**Benefits**:
- Smaller index size (less memory)
- Faster index maintenance
- Useful for frequently queried subsets

### Composite Indexes

Create indexes on multiple fields:

```python
db.events.create_index(["year", "month"])
```

**Index Lookup Strategy** (in order of priority):
1. **Unique Index** — `O(1)` direct lookup
2. **Standard Index** — `O(1)` lookup + intersection for multiple indexed fields
3. **Partial Index** — Filtered index with condition check
4. **Full Scan** — Iterates all documents (fallback)

### Query Cache

`find()` results are cached using a JSON-serialized query key. The cache is:
- Automatically invalidated on insert/update/delete/rollback
- Limited to 1000 entries per group
- Bypassed by `find_iter()` (always fresh results)

---

## 📌 PAGE 4: Backup & Recovery

**GitBook Path**: `Operations > Backup and Recovery`
**URL**: https://8w6s.gitbook.io/hvpdb/operations/backup-and-recovery

### Nội dung cần THÊM — Python API Section:

---

### Python API

In addition to CLI commands, HVPDB provides Python API methods:

```python
from hvpdb import HVPDB

db = HVPDB("mydb.hvp", "password")

# --- Backup ---
# Create a point-in-time snapshot (consistent copy under reader lock)
db.backup("./backups/mydb_snapshot.hvp")

# --- Repair ---
# Attempt to fix corrupted database or WAL
success = db.repair()  # Returns True/False
```

### Storage Format Version 3

v1.0.7 introduces **Storage Version 3** with the following changes:

| Feature | v2 (old) | v3 (new) |
|---------|----------|----------|
| Compression | Zstandard (implicit) | Zstandard with explicit type byte in header |
| Header | `HVPDB` + version(2B) + salt(16B) + KDF params | + compression_type(1B) |
| AAD | Header fields only | Header + compression type |
| Backward compat | — | Can still **read** v2 files |

**File Format (v3)**:
```
[HVPDB magic (5B)]
[Version: 3 (2B)]
[Salt (16B)]
[KDF params length (2B)]
[KDF params (msgpack)]
[Compression type (1B)]  ← NEW in v3
[Nonce (12B)]
[Ciphertext (AES-256-GCM)]
```

---

## 📌 PAGE 5: Concurrency

**GitBook Path**: `Concepts > Concurrency`
**URL**: https://8w6s.gitbook.io/hvpdb/concepts/concurrency

### Nội dung cần THÊM:

---

### Thread Safety (v1.0.7)

All `HVPGroup` methods now acquire a `threading.RLock` before executing:

| Operation | Thread-Safe | Lock Type |
|-----------|-------------|-----------|
| `insert()` | ✅ | `_thread_lock` (RLock) |
| `update()` | ✅ | `_thread_lock` (RLock) |
| `delete()` | ✅ | `_thread_lock` (RLock) |
| `find()` | ✅ | `_thread_lock` (RLock) |
| `find_iter()` | ✅ | `_thread_lock` (RLock) + snapshot |
| `bulk_insert()` | ✅ | Transaction-wrapped |
| `bulk_update()` | ✅ | Transaction-wrapped |
| `bulk_delete()` | ✅ | Transaction-wrapped |

### Stale Read Protection

In multi-process deployments, HVPDB now detects when the database file has been modified by another process:

```python
# Process A writes data
db_a = HVPDB("shared.hvp", "pass")
db_a.group("users").insert({"name": "Alice"})
db_a.commit()

# Process B reads — automatically detects the file change via st_mtime
db_b = HVPDB("shared.hvp", "pass")
users = db_b.group("users").find({})  # Sees Alice (auto-reloaded)
```

**How it works**:
1. Before every `find_iter()`, calls `storage.check_reload()`
2. `check_reload()` compares `os.path.getmtime()` with the last known mtime
3. If file is newer, reloads data from disk (under reader lock)

### Atomic Write Improvements (Windows)

The `os.replace()` operation now has:
- **20 retries** (up from 5) with exponential backoff
- **Fallback** to `os.remove()` + `os.rename()` for older platforms
- Handles conflicts with Windows Defender, Search Indexer, and other file scanners

---

## 📌 PAGE 6: Security

**GitBook Path**: `Concepts > Security` and `Security Advanced`

### Nội dung cần THÊM:

---

### Password Verification — Timing Attack Prevention

`_verify_password()` now always performs cryptographic work regardless of input validity:
- Uses `secrets.compare_digest()` for constant-time comparison
- Prevents user enumeration via response timing analysis

### TOCTOU Race Condition Fix

File locking now uses atomic `os.open()` with `O_CREAT | O_EXCL` flags, eliminating the Time-Of-Check-To-Time-Of-Use race condition during lock acquisition.

### Passkey / FIDO2 Authentication

HVPDB v1.0.7 supports passwordless authentication via FIDO2/WebAuthn:

**Windows Hello Integration:**
```bash
# Generate a passkey using Windows Hello (fingerprint/face/PIN)
hvpdb gen-passkey admin@example.com --native

# Login with passkey
hvpdb login-passkey admin@example.com
```

**Shell with Passkey:**
```bash
hvpdb shell mydb.hvp --passkey --user admin
```

**How it works:**
1. Calls Windows Hello API via `fido2` library
2. Stores credential in encrypted `hvpdb_passkeys.json`
3. Verification uses the stored public key
4. No password is stored — only the FIDO2 credential

### Access Key Authentication

Generate portable access keys for automated or remote access:

```bash
# Generate access key with QR code
hvpdb gen-key --qr --save access_key.json

# Login with access key
hvpdb shell mydb.hvp --access-key access_key.json
```

### Argon2id Hardening

Key derivation now uses:
- `memory_cost`: 256 MB (increased from default)
- `time_cost`: 4 iterations
- `parallelism`: 2 threads
- Stronger resistance against GPU-based brute-force attacks

---

## 📌 PAGE 7: CLI Reference

**GitBook Path**: `Reference > CLI`

### Nội dung cần THÊM — New Commands:

---

### Database Management

| Command | Usage | Description |
|---------|-------|-------------|
| `hvpdb init <target>` | `hvpdb init mydb.hvp` | Initialize new database |
| `hvpdb compact <target>` | `hvpdb compact mydb.hvp` | Re-save to reclaim space |
| `hvpdb snapshot <target> -o <path>` | `hvpdb snapshot mydb.hvp -o backup.hvp` | Point-in-time snapshot |
| `hvpdb pack <target> -o <path>` | `hvpdb pack mydb.hvp -o archive.hvpz` | Pack DB + WAL into `.hvpz` |
| `hvpdb repair <target>` | `hvpdb repair mydb.hvp --force` | Attempt to repair corruption |
| `hvpdb restore <backup> --to <target>` | `hvpdb restore backup.hvp --to mydb.hvp` | Restore from backup |
| `hvpdb doctor <target>` | `hvpdb doctor mydb.hvp` | Health check |
| `hvpdb verify <target>` | `hvpdb verify mydb.hvp --deep` | Integrity verification |

### CRUD Operations

| Command | Usage | Description |
|---------|-------|-------------|
| `hvpdb insert <target> <group> <json>` | `hvpdb insert mydb.hvp users '{"name":"Alice"}'` | Insert document |
| `hvpdb find <target> <group> [query] [limit]` | `hvpdb find mydb.hvp users '{"name":"Alice"}' 10` | Find documents |
| `hvpdb delete <target> <group> <id>` | `hvpdb delete mydb.hvp users abc-123` | Delete by ID |

### Group Management

| Command | Usage | Description |
|---------|-------|-------------|
| `hvpdb create-group <target> <name>` | `hvpdb create-group mydb.hvp orders` | Create a group |
| `hvpdb drop-group <target> <name>` | `hvpdb drop-group mydb.hvp orders` | Delete a group |
| `hvpdb drop-db <target>` | `hvpdb drop-db mydb.hvp` | Destroy entire database |
| `hvpdb import <target> <file> [group]` | `hvpdb import mydb.hvp data.json users` | Import JSON |

### Security

| Command | Usage | Description |
|---------|-------|-------------|
| `hvpdb passwd <target>` | `hvpdb passwd mydb.hvp` | Change password |
| `hvpdb gen-passkey <user>` | `hvpdb gen-passkey admin --native` | Register FIDO2 Passkey |
| `hvpdb login-passkey <user>` | `hvpdb login-passkey admin` | Authenticate with Passkey |
| `hvpdb gen-key` | `hvpdb gen-key --qr --save key.json` | Generate Access Key |
| `hvpdb config <target>` | `hvpdb config mydb.hvp --auth-type passkey` | Set auth type |

### WAL Management

| Command | Usage | Description |
|---------|-------|-------------|
| `hvpdb wal status <target>` | `hvpdb wal status mydb.hvp` | WAL statistics |
| `hvpdb wal dump <target>` | `hvpdb wal dump mydb.hvp --limit 50` | Dump decrypted WAL entries |
| `hvpdb wal checkpoint <target>` | `hvpdb wal checkpoint mydb.hvp` | Flush WAL to storage |

### Plugin Management

| Command | Usage | Description |
|---------|-------|-------------|
| `hvpdb plugin list` | | List installed plugins |
| `hvpdb plugin info <name>` | `hvpdb plugin info query` | Plugin details |
| `hvpdb plugin doctor <name>` | `hvpdb plugin doctor query` | Plugin health check |

### Utilities

| Command | Usage | Description |
|---------|-------|-------------|
| `hvpdb meta <target> [key] [value]` | `hvpdb meta mydb.hvp version 1.0` | View/set metadata |
| `hvpdb lock-status <target>` | `hvpdb lock-status mydb.hvp` | Check file locks |
| `hvpdb env` | | Show environment variables |
| `hvpdb redacted-uri <uri>` | `hvpdb redacted-uri hvp://user:pass@host/db` | Redact password |
| `hvpdb --version` | | Show version |

---

## 📌 PAGE 8: HVPShell Commands

**GitBook Path**: `Reference > Shell Commands`

### Nội dung cần THÊM — New Shell Commands in v1.0.7:

---

| Command | Syntax | Description |
|---------|--------|-------------|
| `hunt` | `hunt name=r:^Admin.*` | Search with regex support |
| `query` | `query SELECT * FROM users` | Polyglot Query Engine (requires `hvpdb-query` plugin) |
| `anchor` | `anchor myspot` | Bookmark current navigation context |
| `recall` | `recall myspot` | Return to bookmarked context |
| `fuse` | `fuse source_group` | Merge data from another group |
| `sift` | `sift field=value` | Filter and analyze data patterns |
| `dedupe` | `dedupe field_name` | Detect and remove duplicates |
| `inhale` | `inhale data.json` | Advanced import (JSON, CSV) |
| `exhale` | `exhale output.json` | Advanced export |
| `morph` | `morph field=new_value` | Batch update selected documents |
| `select` | `select query` | Add match results to multi-selection buffer |
| `discard` | `discard` | Clear multi-selection buffer |
| `monitor` | `monitor` | Real-time database activity |
| `benchmark` | `benchmark 1000` | Performance benchmarking (N operations) |
| `validate` | `validate` | Data integrity checks |
| `record` | `record status\|list\|peek\|undo\|apply` | Full WAL interaction |
| `become` | `become admin` | Switch user identity |
| `perm` | `perm` | View permissions across groups |
| `edit` | `edit doc_id` | Open document in system editor |
| `calc` | `calc 1+1` | Built-in calculator |
| `getatour` | `getatour` | Interactive guided tour |

### Shell Launch Options

```bash
# Standard launch
hvpdb shell mydb.hvp

# Jump directly to a group
hvpdb jump mydb.hvp users

# Launch with Passkey authentication
hvpdb shell mydb.hvp --passkey --user admin

# Launch with Access Key
hvpdb shell mydb.hvp --access-key key.json
```

---

## 📌 PAGE 9: Maintenance

**GitBook Path**: `Operations > Maintenance`

### Nội dung cần THÊM:

---

### TTL Reaper

HVPDB runs a background daemon thread that cleans up expired TTL documents:

- **Interval**: Every 60 seconds
- **Action**: Finds documents where `_expires_at < current_time` and permanently deletes them
- **Scope**: All groups in the database
- **Thread**: Daemon thread (auto-stops when main process exits)
- **Error handling**: Errors are logged as warnings, never crash the main process

### Auto-Repair via Python API

```python
success = db.repair()
```

Process:
1. Acquires **writer lock**
2. Reloads data from disk
3. Re-saves a clean copy
4. Truncates the WAL
5. Returns `True` on success

---

## 📌 PAGE 10: What's New in v1.0.7

**GitBook Path**: (New page — tạo mới)
**Suggested Path**: `Releases > v1.0.7`

---

# What's New in v1.0.7

> **Release Date**: 2026-02-16
> **Diff**: +2,158 insertions, −236 deletions

## Highlights

### 🔒 Security
- **Timing attack prevention** — constant-time password verification
- **TOCTOU fix** — atomic file lock acquisition
- **Argon2id hardening** — 256 MB memory cost
- **Passkey/FIDO2 auth** — Windows Hello, security keys
- **Access Key auth** — QR code generation for mobile

### ⚡ Core Engine (Batch 1)
- **Regex queries** — `{"$regex": "pattern"}`
- **Pagination** — `skip` parameter on `find()`
- **Bulk operations** — `bulk_insert()`, `bulk_update()`, `bulk_delete()`
- **Query caching** — automatic with smart invalidation
- **Soft delete** — `soft_delete()` / `undelete()`
- **Computed fields** — `set_computed_field()`
- **DBRef support** — `resolve_ref()`
- **TTL documents** — auto-expire with background reaper
- **Partial indexes** — `create_index(condition={...})`

### 💾 Storage Engine (Batch 2)
- **Storage v3 format** — compression type in header, backward compatible
- **Backup API** — `db.backup(path)` with reader lock
- **Auto-repair** — `db.repair()` with writer lock
- **Stale-read prevention** — `st_mtime` checking

### 🧵 Concurrency
- Thread-safe all CRUD operations via `_thread_lock` (RLock)
- `find_iter()` snapshots results while holding lock
- Atomic write retry: 20 attempts (up from 5)

### 🐚 Shell & CLI
- 20+ new shell commands
- 13+ new CLI commands
- Plugin management (`plugin list/info/doctor`)
- WAL management (`wal status/dump/checkpoint`)

---

## 📋 Tóm tắt — Thứ tự ưu tiên update GitBook

| # | Page | Priority | Action |
|---|------|----------|--------|
| 1 | Python API | 🔴 HIGH | Add 11+ methods/features |
| 2 | CLI Reference | 🔴 HIGH | Add all new commands |
| 3 | Shell Commands | 🟡 MED | Add 20+ new commands |
| 4 | Core Concepts | 🟡 MED | Add TTL, soft delete, computed fields, DBRef |
| 5 | Indexing & Query | 🟡 MED | Add regex, partial indexes, cache |
| 6 | Concurrency | 🟡 MED | Add thread safety, stale reads |
| 7 | B&R | 🟡 MED | Add Python API + storage v3 |
| 8 | Security | 🟡 MED | Add Passkey, Access Key, timing attack |
| 9 | Maintenance | 🟢 LOW | Add TTL reaper, repair API |
| 10 | What's New | 🟢 LOW | New page (optional) |
