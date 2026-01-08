# HVPDB (High Velocity Python Database)

<div align="center">

[![PyPI version](https://badge.fury.io/py/hvpdb.svg)](https://badge.fury.io/py/hvpdb)
[![Python Versions](https://img.shields.io/pypi/pyversions/hvpdb.svg)](https://pypi.org/project/hvpdb/)
[![License](https://img.shields.io/badge/License-Proprietary-red.svg)](LICENSE)
[![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

**The Secure, Embedded, NoSQL Database for Modern Python Apps.**

[Features](#key-features) • [Installation](#installation) • [Quick Start](#quick-start) • [CLI Power](#cli-power) • [Benchmarks](#benchmarks)

</div>

---

## 🚀 What is HVPDB?

**HVPDB** is a high-performance, embedded NoSQL database designed for Python applications that require **speed**, **security**, and **reliability** without the overhead of a dedicated server (like MongoDB or PostgreSQL).

It combines the simplicity of SQLite with the flexibility of Document Stores, powered by modern tech stack: **MsgPack** serialization, **ZStandard** compression, and **AES-GCM** encryption.

> "Think of it as SQLite met MongoDB, got married, and went to the gym." 💪

## ✨ Key Features

- **🔒 Military-Grade Security:** Native AES-256-GCM encryption at rest. Your data is encrypted before it touches the disk.
- **⚡ Blazing Fast:** Optimized Write-Ahead Log (WAL) v2 with batched commits and ZStandard compression.
- **📦 Transaction Support:**
Atomic and consistent transactions with WAL-backed commit and rollback.
Designed for local and embedded workloads.
- **🐚 Power Ops Shell:** Interactive CLI with syntax highlighting, auto-completion, and live data inspection.
- **🕸️ Thread-Safe:** Built with `contextvars` for safe concurrency in AsyncIO/FastAPI/Flask environments.
- **🔍 Smart Query Optimizer:** Uses Set Intersection algorithms for O(1) lookups on indexed fields.
- **🌐 HTTP Server:** Built-in REST API server to expose your DB over the network instantly.

## 📦 Installation

```bash
pip install hvpdb
```

## ⚡ Quick Start (Python API)

```python
from hvpdb import HVPDB

# 1. Connect (Auto-creates ./hvp/mydb/mydb.hvp)
db = HVPDB("mydb", password="super_secret_key")

# 2. Get a Group (Collection)
users = db.group("users")

# 3. Insert Data (Atomic & Durable)
users.insert({
    "name": "Alice",
    "email": "alice@example.com",
    "role": "admin",
    "stats": {"login_count": 42}
})

# 4. Create Index (Unique Constraint)
users.create_index("email", unique=True)

# 5. Fast Query
admin = users.find_one({"role": "admin"})
print(f"Found admin: {admin['name']}")

# 6. Transaction (ACID)
with db.begin() as txn:
    users.insert({"name": "Bob"})
    users.insert({"name": "Charlie"})
    # If code fails here, Bob and Charlie are rolled back!

db.close()
```

## 🖥️ CLI Power (HVPShell)

HVPDB comes with a beautiful, rich-text terminal interface.

**1. Initialize a Database:**
```bash
hvpdb init my_project "secret123"
```

**2. Enter the Shell:**
```bash
hvpdb shell my_project "secret123"
```

**3. Interactive Magic:**
```bash
hvpdb [my_project] > target users
hvpdb [my_project] [users] > make name="Dave" role="dev"
hvpdb [my_project] [users] > hunt role="dev"
hvpdb [my_project] [users] > peek full
hvpdb [my_project] [users] > stats login_count
```

**4. Instant API Server:**
```bash
hvpdb deploy my_project 8080
# Serving HTTP API at http://0.0.0.0:8080 🚀
```

## 📊 Benchmarks

Running on standard hardware (NVMe SSD, Ryzen 7):

| Operation | Ops/Sec | Note |
|-----------|---------|------|
| **Write** | ~14,000 | Batched WAL Commit |
| **Read**  | ~500k+  | Memory-mapped + Cache |
| **Index** | O(1)    | Hash Map Lookup |

*> Benchmarks generated using `hvpdb shell > benchmark`*

## 📂 Project Structure

When you initialize a database named `myapp`, HVPDB keeps it organized locally:

```text
./hvp/
  └── myapp/
      ├── myapp.hvp          # Main Data File (Encrypted & Compressed)
      ├── myapp.hvp.log      # Write-Ahead Log (Crash Recovery)
      ├── myapp.hvp.lock     # Process Lock
      └── myapp.hvp.writelock
```
## 👥 Who is HVPDB for?

- Solo developers & indie hackers
- Small to medium backend teams
- CLI-first workflows
- Local-first apps
- Embedded tools & internal services

## 📜 License

**Proprietary / Closed Source**
- ✅ Free for personal and commercial usage in your projects.
- ❌ **No Forking:** You may not redistribute modified source code.
- ❌ **No Resale:** You may not sell this software as a standalone product.

Copyright © 2026 8w6s. All rights reserved.
