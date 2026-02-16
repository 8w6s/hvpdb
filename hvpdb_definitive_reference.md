# HVPDB — Definitive Reference Manual

> **Version**: 1.0.7  
> **Engine**: High Velocity Python Database  
> **License**: Proprietary  

---

## Table of Contents

1. [Installation & Quick Start](#1-installation--quick-start)
2. [Architecture Overview](#2-architecture-overview)
3. [Python API Reference](#3-python-api-reference)
4. [CLI Command Reference](#4-cli-command-reference)
5. [Interactive Shell (HVPShell)](#5-interactive-shell-hvpshell)
6. [HTTP Server & REST API](#6-http-server--rest-api)
7. [Connection URI Format](#7-connection-uri-format)
8. [Transactions (ACID)](#8-transactions-acid)
9. [Write-Ahead Log (WAL)](#9-write-ahead-log-wal)
10. [Security & Authentication](#10-security--authentication)
11. [Cluster Mode](#11-cluster-mode)
12. [Plugin System](#12-plugin-system)
13. [Operations & Maintenance](#13-operations--maintenance)
14. [Troubleshooting](#14-troubleshooting)
15. [Release Notes v1.0.7](#15-release-notes-v107)

---

## 1. Installation & Quick Start

### Install
```bash
pip install hvpdb
```

### Optional Dependencies
```bash
pip install hvpdb[passkey]    # FIDO2/Passkey support
pip install hvpdb[server]     # FastAPI HTTP server
pip install hvpdb[full]       # All extras
```

### Create Your First Database
```python
from hvpdb import HVPDB

db = HVPDB("my_app.hvp", password="my_secret")
users = db.group("users")
users.insert({"name": "Alice", "role": "admin"})
db.commit()
db.close()
```

### From the CLI
```bash
hvpdb init my_app.hvp
hvpdb shell my_app.hvp
```

---

## 2. Architecture Overview

```
┌───────────────────────────────────────────────────────────┐
│  Application Layer                                        |
│  ┌─────────┐  ┌────────────┐  ┌──────────┐              │
│  │ Python  │  │  CLI/Shell │  │ HTTP API │              │
│  │   API   │  │  (Typer)   │  │ (FastAPI)│              │
│  └────┬────┘  └─────┬──────┘  └────┬─────┘              │
│       └─────────────┼──────────────┘                      │
│                     ▼                                     │
│  ┌──────────────────────────────────────────────────┐    │
│  │  HVPDB Core (core.py)                             │    │
│  │  ├── HVPGroup (CRUD, Indexing, Schema Validation) │    │
│  │  ├── HVPTransaction (ACID, Buffered Ops)          │    │
│  │  └── Thread Lock (RLock per instance)              │    │
│  └──────────────────┬───────────────────────────────┘    │
│                     ▼                                     │
│  ┌──────────────────────────────────────────────────┐    │
│  │  Storage Layer (storage.py)                       │    │
│  │  ├── AES-256-GCM Encryption                       │    │
│  │  ├── Argon2id Key Derivation                      │    │
│  │  ├── Write-Ahead Log (wal.py)                     │    │
│  │  ├── File Locking (portalocker)                   │    │
│  │  └── Auto-Reload (st_mtime check)                 │    │
│  └──────────────────┬───────────────────────────────┘    │
│                     ▼                                     │
│  ┌──────────────┐  ┌──────────────────┐                  │
│  │ Single File  │  │ Cluster Mode     │                  │
│  │  (.hvp)      │  │  (.hvdb dir)     │                  │
│  └──────────────┘  └──────────────────┘                  │
└───────────────────────────────────────────────────────────┘
```

- **Single File Mode** (`.hvp`): All groups in one encrypted file. Best for < 1GB databases.
- **Cluster Mode** (`.hvdb`): Each group = separate file. Best for large datasets, parallel I/O.

---

## 3. Python API Reference

### 3.1 Class: `HVPDB`

#### Constructor
```python
HVPDB(path: str, password: str, durable: bool = True)
```
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `path`    | str  | *required* | Path to `.hvp` file or `.hvdb` directory |
| `password`| str  | *required* | Master encryption password (Argon2id derived) |
| `durable` | bool | `True`     | If True, fsync after each write |

#### Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `group` | `group(name: str, schema=None) -> HVPGroup` | Get or create a named collection. Optional Pydantic schema for validation. |
| `commit` | `commit() -> None` | Persist all pending dirty changes to disk. |
| `refresh` | `refresh(force: bool = False) -> None` | Reload data from disk. `force=True` discards unsaved changes. |
| `close` | `close() -> None` | Commit, close WAL, clear encryption keys from memory. |
| `begin` | `begin() -> HVPTransaction` | Start an ACID transaction (context manager). |
| `get_all_groups` | `get_all_groups() -> List[str]` | List all group names. |
| `drop_group` | `drop_group(name: str) -> None` | Permanently delete a group and its data. |
| `authenticate` | `authenticate(username: str, password: str) -> bool` | Verify user credentials. |
| `check_permission` | `check_permission(username: str, group_name: str) -> bool` | Check user access to a group. |
| `change_password` | `change_password(new_password: str, auth_type: str = None) -> None` | Re-encrypt database with new password. |
| `set` | `set(key: str, value: Any) -> None` | Store a global key-value pair. |
| `get` | `get(key: str, default=None) -> Any` | Retrieve a global key-value pair. |
| `help` | `help() -> None` | Print usage help. |

#### Dynamic Group Access
```python
# These two are equivalent:
db.group("users")
db.users
```

---

### 3.2 Class: `HVPGroup`

Represents a collection (similar to a MongoDB collection or SQL table).

#### CRUD Operations

| Method | Signature | Returns | Description |
|--------|-----------|---------|-------------|
| `insert` | `insert(data: dict) -> dict` | Inserted doc with `_id`, `_created_at`, `_updated_at` | Insert a new document. Validates against schema if set. |
| `find` | `find(query: dict = None, limit: int = 0) -> List[dict]` | List of matching documents | Find documents by field equality. `limit=0` means all. |
| `find_one` | `find_one(query: dict) -> Optional[dict]` | Single document or `None` | Find first matching document. Uses index if available. |
| `find_iter` | `find_iter(query: dict = None) -> Generator` | Yields documents | **v1.0.7**: Thread-safe streaming iterator with snapshot. |
| `get_all` | `get_all() -> List[dict]` | All documents | Retrieve every document in the group. |
| `get_all_iter` | `get_all_iter() -> Generator` | Yields documents | Memory-efficient iteration over all documents. |
| `update` | `update(query: dict, update_data: dict) -> int` | Count updated | Update all matching documents. Thread-safe with RLock. |
| `delete` | `delete(query: dict) -> int` | Count deleted | Delete all matching documents. Thread-safe with RLock. |
| `count` | `count(query: dict = None) -> int` | Document count | Count documents matching query (or all). |
| `append` | `append(op: str, data: dict) -> None` | None | Low-level: directly append an operation to the WAL. `op` = `'insert'`, `'update'`, or `'delete'`. |

#### Indexing

| Method | Signature | Description |
|--------|-----------|-------------|
| `create_index` | `create_index(field, unique: bool = False, persist: bool = True)` | Create an in-memory index. Supports single field (`str`) or composite (`tuple`). |

**Index Usage Example:**
```python
users = db.group("users")
users.create_index("email", unique=True)     # Unique single-field index
users.create_index(("city", "age"))           # Composite index

# Queries automatically use the index:
users.find({"email": "alice@example.com"})    # O(1) lookup via index
users.find({"city": "Tokyo", "age": 30})      # Composite index hit
```

#### Audit Trail
```python
# Get change history for a document (or entire group)
trail = users.get_audit_trail(doc_id="abc123", limit=100)
# Returns: [{"op": "insert", "data": {...}, "txn_id": "...", "ts": 1234567890}, ...]
```

#### Schema Validation
```python
from pydantic import BaseModel

class UserSchema(BaseModel):
    name: str
    email: str
    age: int

users = db.group("users", schema=UserSchema)
users.insert({"name": "Bob", "email": "bob@test.com", "age": "not_a_number"})
# Raises: ValidationError
```

---

### 3.3 Class: `HVPTransaction`

```python
with db.begin() as txn:
    txn.group("accounts").update({"id": "A"}, {"balance": 900})
    txn.group("accounts").update({"id": "B"}, {"balance": 1100})
    # Atomic commit on exit
    # Automatic rollback on exception
```

| Method | Description |
|--------|-------------|
| `group(name)` | Get a transactional proxy for a group |
| `commit()` | Manually commit (normally automatic) |
| `rollback()` | Manually rollback (normally automatic on exception) |

**Rules:**
- Nested transactions are **not supported** (raises `RuntimeError`).
- Cluster mode does **not support** transactions.
- All operations are buffered until commit, then applied atomically to WAL + memory.

---

## 4. CLI Command Reference

Run with `hvpdb <command>` or `python -m hvpdb.cli <command>`.

### 4.1 Database Lifecycle

| Command | Usage | Description |
|---------|-------|-------------|
| `init` | `hvpdb init <path> [password]` | Create a new encrypted database (`.hvp` or `.hvdb`). |
| `drop-db` | `hvpdb drop-db <path>` | **Permanently delete** a database and all associated files (`.hvp`, `.log`, `.lock`). |
| `compact` | `hvpdb compact <path> [password]` | Re-save to reclaim space and optimize storage. |
| `doctor` | `hvpdb doctor <path>` | Run health checks on the database file. |
| `verify` | `hvpdb verify <path> [password] [--deep]` | Verify database and WAL integrity. `--deep` for thorough check. |
| `repair` | `hvpdb repair <path> [--force]` | Attempt recovery from corruption. `--force` for risky repairs. |
| `stats` | `hvpdb stats <path> [password]` | Show document counts, group stats, storage metrics. |
| `lock-status` | `hvpdb lock-status <path>` | Check if database files are locked by another process. |
| `env` | `hvpdb env` | Display environment info (OS, Python version, installed plugins). |
| `version` | `hvpdb version` | Show HVPDB version. |

### 4.2 Data Operations

| Command | Usage | Description |
|---------|-------|-------------|
| `insert` | `hvpdb insert <path> <group> '<json>'` | Insert a document directly from CLI. |
| `find` | `hvpdb find <path> <group> '<json_query>' [limit]` | Query documents. |
| `delete` | `hvpdb delete <path> <group> <doc_id>` | Delete a document by ID. |
| `diff` | `hvpdb diff <path> <group> <id1> <id2>` | Show diff between two documents. |
| `dump` | `hvpdb dump <path> <group> '<query>' <output.json>` | Export query results to a file. |
| `import` | `hvpdb import <path> <file.json> [group]` | Import data from JSON file. |
| `export` | `hvpdb export <path> [output.json]` | Export entire database to JSON. |

### 4.3 Group Management

| Command | Usage | Description |
|---------|-------|-------------|
| `create-group` | `hvpdb create-group <path> <name>` | Create a new empty group. |
| `drop-group` | `hvpdb drop-group <path> <name>` | Delete a group and all its data. |
| `jump` | `hvpdb jump <path> <group>` | Open the shell directly in a specific group. |

### 4.4 Security & Authentication

| Command | Usage | Description |
|---------|-------|-------------|
| `passwd` | `hvpdb passwd <path>` | Change the master encryption password. |
| `config` | `hvpdb config <path> --auth-type <type>` | Set auth type: `password`, `access_key`, `passkey`. |
| `gen-key` | `hvpdb gen-key [--qr] [--save <file>]` | Generate a secure Access Key. Optionally show as QR code. |
| `gen-passkey` | `hvpdb gen-passkey <user> [--native]` | Register a FIDO2 Passkey via Windows Hello or QR code. |
| `login-passkey` | `hvpdb login-passkey <user> [--native]` | Authenticate with a registered Passkey. |
| `redacted-uri` | `hvpdb redacted-uri <uri>` | Mask passwords in a connection URI for safe logging. |

### 4.5 User & Permission Management

| Command | Usage | Description |
|---------|-------|-------------|
| `create-user` | `hvpdb create-user <path> <username> [db_pass] [user_pass] [role]` | Create a user. Role: `user` or `admin`. |
| `list-users` | `hvpdb list-users <path> [password]` | List all registered users and their roles. |
| `grant` | `hvpdb grant <path> <username> <group>` | Grant a user access to a specific group. |
| `revoke` | `hvpdb revoke <path> <username> <group>` | Revoke a user's access to a group. |

### 4.6 Backup & Recovery

| Command | Usage | Description |
|---------|-------|-------------|
| `backup` | `hvpdb backup <path> [output]` | Create a safe, consistent backup. |
| `restore` | `hvpdb restore <backup_file> --to <target>` | Restore from backup. `--force` to overwrite. |
| `snapshot` | `hvpdb snapshot <path> --out <file>` | Create a point-in-time snapshot. |
| `pack` | `hvpdb pack <path> --out <file.hvpz>` | Pack database + WAL into a compressed `.hvpz` archive. |

### 4.7 WAL Management

| Command | Usage | Description |
|---------|-------|-------------|
| `wal status` | `hvpdb wal status <path>` | Show WAL file statistics (entry count, size). |
| `wal dump` | `hvpdb wal dump <path> [password] [--limit N]` | Dump decrypted WAL entries for debugging. |
| `wal checkpoint` | `hvpdb wal checkpoint <path> [password]` | Manually flush WAL entries into the main storage file. |

### 4.8 Plugin Management

| Command | Usage | Description |
|---------|-------|-------------|
| `plugin list` | `hvpdb plugin list` | List all installed plugins. |
| `plugin info` | `hvpdb plugin info <name>` | Show details about a specific plugin. |
| `plugin doctor` | `hvpdb plugin doctor <name>` | Run health checks on a plugin. |

### 4.9 Metadata

| Command | Usage | Description |
|---------|-------|-------------|
| `meta` | `hvpdb meta <path> [key] [value]` | View or set database metadata. Use `--unset` to remove. |

### 4.10 Server Deployment

| Command | Usage | Description |
|---------|-------|-------------|
| `deploy` | `hvpdb deploy <path> [port] [host]` | Launch FastAPI HTTP server. Default port: 2321. |
| `shell` | `hvpdb shell [path] [--passkey] [--access-key <file>]` | Launch interactive shell. Supports one-liner commands with `+`. |

---

## 5. Interactive Shell (HVPShell)

Launch: `hvpdb shell [path]` or `hvpdb shell` then `connect <path>`.

The shell provides **80+ commands** organized by category.

### 5.1 Connection & Navigation

| Command | Usage | Description |
|---------|-------|-------------|
| `connect` | `connect <path> [password]` | Connect to a database file. |
| `disconnect` | `disconnect` | Close the current connection. |
| `target` | `target <group>` | Set the active group. Prompt changes to `hvpdb:group >`. |
| `unfocus` | `unfocus` | Clear the active group context. |
| `switch` | `switch` | Toggle to the previously focused group. |
| `jump` | `jump <group>` | Navigate directly into a group. |
| `back` | `back` | Return to the previous context. |
| `anchor` | `anchor` | Bookmark the current context (group/document). |
| `recall` | `recall` | Return to the anchored context. |
| `status` | `status` | Show connection status, group, storage health. |
| `context` | `context` | Display current database and group info. |
| `scan` | `scan` | List all groups with document counts. |
| `tree` | `tree` | Display tree view of the entire database structure. |

### 5.2 Reading & Querying

| Command | Usage | Description |
|---------|-------|-------------|
| `peek` | `peek [limit]` | Preview documents in the current group. |
| `show` | `show` / `show at <id>` / `show full` | List documents, show a specific one, or show with all fields. |
| `get` | `get <doc_id>` | Retrieve and display a document by ID. |
| `find` | `find <key>=<value> [key2=value2 ...]` | Search by field equality. |
| `grep` | `grep <key>=<value>` | Quick search (single field). |
| `hunt` | `hunt <key>=<value> [key=r:<regex>]` | Advanced search with **regex support**. Example: `hunt name=r:^Admin.*` |
| `query` | `query <query_string>` | Execute complex queries via the **Polyglot Query Engine** (requires `hvpdb_query` plugin). |
| `sample` | `sample [n]` | Display a random sample of documents. |
| `random` | `random` | Show one random document. |
| `count` | `count [key=value]` | Count documents (optionally filtered). |
| `distinct` | `distinct <field>` | List unique values for a field. |
| `freq` | `freq <field>` | Show frequency distribution of a field's values. |
| `stats` | `stats <field>` | Statistical summary for numeric fields (min, max, avg, median). |
| `fields` | `fields` | List all unique field names in the current group. |
| `schema` | `schema` | Infer and display the group's document schema. |

### 5.3 Creating & Modifying Data

| Command | Usage | Description |
|---------|-------|-------------|
| `create` | `create <key>=<value> [key2=value2 ...]` | Insert a new document. |
| `make` | `make <key>=<value>` / `make group:<name>` / `make` (interactive) | Create a document or group. Supports interactive mode. |
| `set` | `set <id> <json_string>` | Create or fully replace a document by ID. |
| `patch` | `patch <id> <json_string>` | Partially update a document (JSON Merge Patch). |
| `update` | `update <key>=<value> [key2=value2 ...]` | Update documents matching current selection. |
| `change` | `change <key> <old_val> <new_val>` | Mass update: replace a specific value in a field across documents. |
| `unset` | `unset <field>` / `unset <id> <field>` | Remove a field from a document. |
| `replace` | `replace <json_string>` | Replace the entire content of the current document. |
| `edit` | `edit <doc_id>` | Open a document in your system's default text editor. |

### 5.4 Deleting Data

| Command | Usage | Description |
|---------|-------|-------------|
| `del` | `del <doc_id>` | Delete a single document by ID. |
| `nuke` | `nuke <group_name>` | **Permanently delete** an entire group. Requires confirmation. |
| `drop` | `drop <group_name>` | Alias for `nuke`. |
| `truncate` | `truncate` | Delete **all** documents in the current group (keep the group). |
| `throw` | `throw` | Delete the currently selected document(s). |

### 5.5 Selection & Batch Operations

| Command | Usage | Description |
|---------|-------|-------------|
| `pick` | `pick <index>` | Select a document from the last search results by number. |
| `select` | `select all` / `select <index>` / `select <start>-<end>` / `select clear` | Multi-select documents for batch operations. |
| `discard` | `discard all` / `discard <index>` | Remove documents from the selection buffer. |
| `morph` | `morph <key>=<value> [key2=value2 ...]` | Update all selected documents. |
| `throw` | `throw` | Delete all selected documents. |

### 5.6 Group Management

| Command | Usage | Description |
|---------|-------|-------------|
| `creategroup` | `creategroup <name>` | Create a new group (collection). |
| `clone` | `clone <source> <target>` | Clone a group to a new name. |
| `clonegroup` | `clonegroup <target>` | Clone the current group. |
| `rename` | `rename <new_name>` | Rename the current group. |
| `move` | `move <doc_id> <target_group>` / `move <target_group>` | Move a document to another group. |
| `moveid` | `moveid <id> <target_group>` | Move by explicit document ID. |
| `copy` | `copy <doc_id> <target_group>` / `copy <target_group>` | Copy a document to another group. |
| `copyid` | `copyid <id> <target_group>` | Copy by explicit document ID. |

### 5.7 Indexing

| Command | Usage | Description |
|---------|-------|-------------|
| `index` | `index <field> [unique]` | Create an index on a field. Add `unique` for unique constraint. |

### 5.8 Data Import/Export

| Command | Usage | Description |
|---------|-------|-------------|
| `import` | `import <file.json>` | Import data from JSON into the current group. |
| `export` | `export <file.json>` | Export current group to JSON. |
| `inhale` | `inhale <file>` | Advanced import (supports multiple formats). |
| `exhale` | `exhale <file>` | Advanced export (supports multiple formats). |

### 5.9 Version History & Recovery

| Command | Usage | Description |
|---------|-------|-------------|
| `trace` | `trace` | View audit trail/history for the selected document. |
| `timeline` | `timeline` | Show version history for the current group or document. |
| `revert` | `revert` | Roll back changes to a previous state. |
| `reapply` | `reapply` | Re-apply a previously reverted change. |
| `snapshot` | `snapshot <file>` | Create a point-in-time backup. |
| `restore` | `restore` | Instructions for restoring from a snapshot. |
| `backup` | `backup <destination>` | Create a backup of the current database. |

### 5.10 WAL & Transaction Management

| Command | Usage | Description |
|---------|-------|-------------|
| `record status` | `record status [on\|off]` | Check or toggle record mode. |
| `record list` | `record list [limit]` | List recent WAL transactions. |
| `record peek` | `record peek <seq>` | Inspect a specific transaction by sequence number. |
| `record undo` | `record undo <seq>` | Revert a specific transaction. |
| `record apply` | `record apply <seq>` | Re-apply a specific transaction. |
| `checkpoint` | `checkpoint` | Manually trigger fsync/WAL flush. |
| `recover` | `recover` | Trigger automated WAL recovery. |

### 5.11 Data Quality

| Command | Usage | Description |
|---------|-------|-------------|
| `sift` | `sift <criteria>` | Filter and analyze data patterns. |
| `fuse` | `fuse <source_group>` | Merge data from another group into the current one. |
| `dedupe` | `dedupe` | Detect and remove duplicate documents. |
| `validate` | `validate` | Run integrity checks on data. |
| `verify` | `verify` | Verify database health. |

### 5.12 Security & User Management

| Command | Usage | Description |
|---------|-------|-------------|
| `whoami` | `whoami` | Display currently authenticated user. |
| `become` | `become <username> [password]` | Switch to a different user identity. |
| `perm` | `perm` | Check current user's permissions across all groups. |
| `user list` | `user list` | List all users. |
| `user create` | `user create <username> [password] [role]` | Create a user. |
| `user drop` | `user drop <username>` | Delete a user. |
| `grant` | `grant <username> <group>` | Grant group access. |
| `revoke` | `revoke <username> <group>` | Revoke group access. |
| `crypt` | `crypt` | Change database password/encryption settings. |
| `lock` / `seal` | `lock` | Enable write-protection (read-only mode). |
| `unlock` / `unseal` | `unlock` | Disable write-protection. |
| `guard` | `guard` | Enable write-protection guard mode. |
| `confirm` | `confirm <level>` | Set confirmation level for destructive operations. |

### 5.13 Monitoring & Diagnostics

| Command | Usage | Description |
|---------|-------|-------------|
| `monitor` | `monitor` | Real-time database activity monitoring. Press `Ctrl+C` to stop. |
| `doctor` | `doctor` | Run database diagnostics and health checks. |
| `diagnose` | `diagnose` | Detailed diagnostic analysis. |
| `benchmark` | `benchmark` | Run insert/find/update performance benchmarks. |
| `scout` | `scout` | Scan for patterns or metadata anomalies. |
| `scry` | `scry` | Inspect schema and internal structure. |

### 5.14 Productivity & Help

| Command | Usage | Description |
|---------|-------|-------------|
| `history` | `history` | Show command history (passwords redacted). |
| `tour` / `getatour` | `tour` | Guided interactive tour of HVPDB features. |
| `cheatsheet` | `cheatsheet` | Quick reference of all commands. |
| `examples` / `example` | `examples` / `example <command>` | Usage examples. |
| `explain` | `explain <command>` | Explain a command's purpose. |
| `how` | `how <command>` | Explain the flow of a command. |
| `why` | `why` | Automated analysis of errors or unexpected behavior. |
| `tips` | `tips` | Random tip for efficient usage. |
| `teach` | `teach` | Enable teacher mode (explains each action). |
| `calc` | `calc <expression>` | Built-in calculator. |
| `type` | `type <text>` | Typewriter-style text output. |
| `chronos` | `chronos` | Display current system time. |
| `clear` / `cls` | `clear` | Clear the terminal. |
| `version` | `version` | Show HVPDB version. |
| `save` | `save` / `save auto on\|off` | Manual save or toggle auto-save. |
| `refresh` / `revive` | `refresh` | Reload database from disk. |
| `vacuum` / `drain` | `vacuum` | Trigger compaction. |
| `config` / `tune` | `config` | View or modify settings. |
| `quit` / `vanish` | `quit` | Save and exit. |

### 5.15 Command Aliases

| Alias | Maps To |
|-------|---------|
| `pulse` | `status` |
| `ignite` | `connect` |
| `vanish` | `quit` |
| `freeze` | `save` (checkpoint) |
| `revive` | `refresh` |
| `drain` | `vacuum` |
| `seal` | `lock` |
| `unseal` | `unlock` |
| `track` | `history` |
| `drop` | `nuke` |

---

## 6. HTTP Server & REST API

### Start Server
```bash
hvpdb deploy my_db.hvp 9000
# or
hvpdb deploy my_db.hvp 9000 0.0.0.0   # bind to all interfaces
```
Requires `HVPDB_PASSWORD` env var or pass `--password`.

### Endpoints

| Method | Endpoint | Body | Description |
|--------|----------|------|-------------|
| `GET` | `/` | — | Health check. Returns `{"server": "HVPDB", "status": "running"}` |
| `GET` | `/groups` | — | List all groups. |
| `POST` | `/group/{name}/find` | `{"query": {"field": "value"}}` | Find documents. |
| `POST` | `/group/{name}/insert` | `{"data": {"field": "value"}}` | Insert a document. |
| `POST` | `/group/{name}/update` | `{"query": {...}, "update": {...}}` | Update documents. |
| `DELETE` | `/group/{name}/delete` | `{"query": {"field": "value"}}` | Delete documents. |
| `DELETE` | `/group/{name}/drop` | — | Drop entire group. |

### Authentication
All endpoints (except `/`) require authentication via:
- **Header**: `Authorization: Bearer <password>`
- **Header**: `X-HVP-Key: <password>`

### Example with `curl`
```bash
# Insert a user
curl -X POST http://localhost:9000/group/users/insert \
  -H "Authorization: Bearer my_secret" \
  -H "Content-Type: application/json" \
  -d '{"data": {"name": "Alice", "role": "admin"}}'

# Query users
curl -X POST http://localhost:9000/group/users/find \
  -H "Authorization: Bearer my_secret" \
  -H "Content-Type: application/json" \
  -d '{"query": {"role": "admin"}}'
```

---

## 7. Connection URI Format

HVPDB supports a custom URI scheme for connections:

```
hvp://[user:password@]host[:port][~shard1,shard2]/database[?option=value]
```

### Examples
```
hvp://localhost/my_app.hvp
hvp://admin:secret@192.168.1.10:9000/production.hvp
hvp://cluster1~shard_a,shard_b/data.hvdb?durable=true
```

### Parsing (Python)
```python
from hvpdb.uri import HVPURI

info = HVPURI.parse("hvp://admin:secret@localhost:9000/mydb.hvp")
print(info.username)   # "admin"
print(info.database)   # "mydb.hvp"
print(info.connection_string)  # hvp://admin:****@localhost:9000/mydb.hvp
```

---

## 8. Transactions (ACID)

HVPDB provides **full ACID** transactions with Write-Ahead Logging.

### Python API
```python
with db.begin() as txn:
    accounts = txn.group("accounts")
    accounts.update({"id": "A"}, {"balance": 900})
    accounts.update({"id": "B"}, {"balance": 1100})
    # Commit is automatic on block exit
    # Rollback is automatic on exception
```

### How It Works
1. **Begin**: Allocates a new `txn_id` in the WAL.
2. **Buffer**: All inserts/updates/deletes are buffered (not applied to memory).
3. **Validate**: On commit, checks for ID conflicts and unique constraint violations.
4. **WAL Write**: Writes all operations atomically to the encrypted WAL.
5. **Memory Apply**: Applies changes to in-memory state under the writer lock.
6. **Checkpoint**: Periodically flushes WAL to main storage file.

### Limitations
- **No nested transactions** (raises `RuntimeError`).
- **No cluster mode** (transactions require a single storage file).

---

## 9. Write-Ahead Log (WAL)

Every write operation is first recorded in an encrypted WAL before being applied.

### WAL Operations
- `insert`: New document creation.
- `update`: Document modification with before-image (for rollback).
- `delete`: Document removal with backup copy.
- `drop_group`: Entire group deletion.

### Auto-Checkpoint
WAL entries are periodically flushed to the main storage file to keep the WAL size manageable.

### Manual WAL Commands
```bash
hvpdb wal status my_db.hvp       # Check WAL health
hvpdb wal dump my_db.hvp pw 200  # Inspect entries
hvpdb wal checkpoint my_db.hvp   # Force flush
```

### Crash Recovery
On next `HVPDB()` open after a crash:
1. Uncommitted transactions are rolled back.
2. Committed but un-checkpointed entries are replayed.
3. No data is lost. No manual intervention needed.

---

## 10. Security & Authentication

### 10.1 Encryption
- **Algorithm**: AES-256-GCM (Authenticated Encryption with Associated Data).
- **Key Derivation**: Argon2id (memory-hard, GPU-resistant).
- **WAL Protection**: Each WAL entry is individually encrypted.
- **At-Rest**: The entire `.hvp` file is encrypted. Decryption requires the master password.

### 10.2 Password-Based Auth
```python
db = HVPDB("secure.hvp", password="strong_password_here")
```

### 10.3 Access Key Auth
```bash
hvpdb gen-key --save my_key.hvpk --qr   # Generate and optionally show QR
hvpdb shell my_db.hvp --access-key my_key.hvpk
```

### 10.4 Passkey/FIDO2 Auth (Windows Hello)
```bash
# Register biometric credential
hvpdb gen-passkey admin_user --native

# Login with Windows Hello (fingerprint/face)
hvpdb login-passkey admin_user --native

# Launch shell with Passkey
hvpdb shell my_db.hvp --passkey --user admin_user
```

**How It Works:**
1. Uses the `fido2` library to communicate with Windows WebAuthn API.
2. Credential is derived from biometric verification (no password stored).
3. Supports Windows Hello (face, fingerprint, PIN) and external security keys (YubiKey).

### 10.5 User Management
```python
# Python API
db.authenticate("alice", "password123")  # Returns True/False
db.check_permission("alice", "users")    # Returns True/False
```

```bash
# CLI
hvpdb create-user my_db.hvp alice db_pass user_pass admin
hvpdb grant my_db.hvp alice users
hvpdb revoke my_db.hvp alice secret_data
hvpdb list-users my_db.hvp
```

### 10.6 Timing Attack Prevention
Password verification always performs cryptographic work regardless of whether the user exists, preventing timing-based enumeration attacks.

---

## 11. Cluster Mode

For databases exceeding 1GB, switch to Cluster Mode.

### Create a Cluster
```bash
hvpdb init my_cluster.hvdb
```
This creates a **directory** instead of a single file.

### How It Works
- Each `group()` call creates/opens a separate `.hvp` file inside the directory.
- Groups can be backed up, restored, and dropped independently.
- `drop_group()` in cluster mode = deleting a file (O(1) operation).
- Parallel I/O across groups (different files = different locks).

### Limitations
- Transactions span only a single group (no cross-group atomicity).
- Slightly higher overhead for opening many small groups.

---

## 12. Plugin System

HVPDB supports plugins via Python entry points and local module discovery.

### Built-in Plugin Commands
| Plugin | Description |
|--------|-------------|
| `backup` | Extended backup capabilities |
| `http` | REST API extensions |
| `migrate` | Data migration tools |
| `observe` | Monitoring and alerting |
| `query` | Polyglot Query Engine (SQL-like syntax) |
| `admin` | Admin & Audit Tools |
| `tools` | Developer Tools & Debugging |
| `sync` | Data Import/Export Connectors |

### Managing Plugins
```bash
hvpdb plugin list              # See all installed plugins
hvpdb plugin info <name>       # Plugin details
hvpdb plugin doctor <name>     # Run plugin health checks
```

### Creating a Plugin
Register via `setup.py` entry points:
```python
# setup.py
setup(
    name="hvpdb-my-plugin",
    entry_points={
        "hvpdb_plugins": [
            "my_plugin = my_plugin_module:setup",
        ],
    },
)
```

---

## 13. Operations & Maintenance

### 13.1 Compaction
```bash
hvpdb compact my_db.hvp my_password
```
Removes dead space from deleted documents. Run weekly for write-heavy databases.

### 13.2 Safe Backup
```bash
hvpdb backup my_db.hvp backup_2024.hvp
hvpdb pack my_db.hvp --out archive.hvpz    # Compressed archive with WAL
```

### 13.3 Restore
```bash
hvpdb restore backup_2024.hvp --to production.hvp --force
```

### 13.4 Integrity Verification
```bash
hvpdb verify my_db.hvp --deep    # Full cryptographic verification
hvpdb doctor my_db.hvp            # Quick health check
```

### 13.5 Password Rotation
```bash
hvpdb passwd my_db.hvp
# Prompts for current password, then new password
# Re-encrypts entire database with new key
```

### 13.6 Monitoring (Shell)
```
hvpdb > monitor
# Real-time activity log... Press Ctrl+C to stop
hvpdb > benchmark
# Insert: 12,450 ops/sec | Find: 45,200 ops/sec | Update: 8,900 ops/sec
```

---

## 14. Troubleshooting

### Common Issues

| Problem | Cause | Solution |
|---------|-------|----------|
| `PermissionError` on Windows | Antivirus or indexer locking the file | v1.0.7 has automatic retry. Exclude `.hvp` from AV scanning. |
| `RuntimeError: dictionary changed size` | Concurrent iteration without snapshot | Upgrade to v1.0.7 (uses `find_iter` with snapshot). |
| Stale reads in multi-process | Reader not detecting writer changes | v1.0.7 auto-reloads based on `st_mtime`. Call `db.refresh()` manually if needed. |
| `RuntimeError: Nested transactions` | `begin()` called inside another `begin()` | Flatten transaction blocks or restructure code. |
| Database file grows continuously | WAL not being checkpointed | Run `hvpdb wal checkpoint` or `hvpdb compact`. |
| Slow queries | No index on queried field | Run `users.create_index("email")` in Python or `index email` in shell. |

### Environment Debug
```bash
hvpdb env           # Show OS, Python, installed plugins
hvpdb lock-status my_db.hvp   # Check for lock contention
```

---

## 15. Release Notes v1.0.7

### Core Stability Fixes (Zero-Error Target)

1. **Thread Safety**: All CRUD methods (`update`, `delete`, `find_iter`) now acquire `_thread_lock` (RLock). Eliminates `RuntimeError` during concurrent access.

2. **Stale Read Protection**: Storage layer checks `st_mtime` before reads. If the file was modified by another process, data is automatically reloaded.

3. **Windows Atomic Write Robustness**: `PermissionError` during `os.replace()` is now handled with a configurable retry loop, resolving conflicts with antivirus and indexing services.

### New Features
- `find_iter()`: Thread-safe generator for streaming large result sets.
- Passkey/FIDO2 support via Windows Hello.
- Access Key authentication with QR code generation.

### Compatibility
- Python 3.8+
- Windows, Linux, macOS, Android (Termux)
