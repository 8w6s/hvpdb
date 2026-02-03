# HVPDB (High Velocity Python Database)

<div align="center">


[![PyPI version](https://badge.fury.io/py/hvpdb.svg)](https://badge.fury.io/py/hvpdb)
[![Python Versions](https://img.shields.io/pypi/pyversions/hvpdb.svg)](https://pypi.org/project/hvpdb/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

**The Secure, Embedded, NoSQL Database for Modern Python Apps.**

[Overview](#-what-is-hvpdb) •
[Concepts](#-core-concepts-important--read-this-first) •
[Installation](#-installation) •
[Quick Start](#-quick-start-python-api) •
[Benchmarks](#-benchmarks)

</div>

---

## 🚀 What is HVPDB?

**HVPDB** is a **local-first, embedded NoSQL database** for Python.

It is designed for developers who want:

- A **simple database they fully control**
- **Strong encryption by default**
- **High-speed local reads/writes**
- No external DB server (no MongoDB / Postgres daemon)

HVPDB combines ideas from:

- **SQLite** → embedded, file-based  
- **MongoDB** → document-oriented  
- **Linux tools** → CLI-first workflow  

Under the hood, it uses:

- **MsgPack** for fast serialization  
- **ZStandard** for compression  
- **AES-256-GCM** for authenticated encryption  

> *A private, encrypted data store you ship with your app.*

---

## 🧠 Core Concepts (Important – Read This First)

### 1. Embedded, Not a Server

HVPDB runs **inside your Python process**.

- ❌ No background daemon  
- ❌ No TCP port unless you explicitly `deploy`  
- ✅ Just a file + Python API  

```python
from hvpdb import HVPDB
db = HVPDB("mydb", password="secret")
```

---

### 2. Group ≠ Collection ≠ Table

HVPDB intentionally avoids SQL/Mongo terminology.

HVPDB Term	Meaning

Database	A folder containing encrypted files
Group	A logical set of documents
Document	A Python dict with _id

```python
users = db.group("users")
users.insert({"name": "Alice"})
```

---

### 3. Memory-First, Disk-Safe

Reads are memory-first → extremely fast

Writes go through WAL (Write-Ahead Log)

Disk data is always encrypted


Result:

Fast developer experience

Safe crash recovery

Predictable performance



---

### 4. Two Ways to Use HVPDB

Mode	Purpose

Python API	Application logic
HVPShell	Human interaction, debugging, inspection


> HVPShell is a database editor, not the database itself.




---

✨ Key Features

🔒 Encryption by Default
AES-256-GCM encryption at rest.

⚡ High Performance
WAL v2, batched commits, compressed storage.

📦 Transactions (ACID)
Atomic commits with rollback support.

🐚 HVPShell (Ops Shell)
Interactive CLI with:

auto-complete

rich tables

schema inference

audit & history tools


🕸️ Thread-Safe
Safe for FastAPI / AsyncIO / Flask via contextvars.

🌐 Optional HTTP Server
Expose your DB via REST when needed.



---

## 📦 Installation
```python
pip install hvpdb
```

---

## ⚡ Quick Start (Python API)
```python
from hvpdb import HVPDB

db = HVPDB("mydb", password="super_secret_key")

users = db.group("users")

users.insert({
    "name": "Alice",
    "email": "alice@example.com",
    "role": "admin"
})

users.create_index("email", unique=True)

admin = users.find_one({"role": "admin"})
print(admin["name"])

with db.begin():
    users.insert({"name": "Bob"})
    users.insert({"name": "Charlie"})

db.close()
```

---

## 🖥️ CLI Power (HVPShell)

Initialize database
```bash
hvpdb init my_project "secret123"
```
Enter shell
```bash
hvpdb shell my_project "secret123"
```
Example workflow
```bash
target users
make name="Dave" role="dev"
hunt role="dev"
peek
stats login_count
```
Deploy as API (optional)
```bash
hvpdb deploy my_project 8080
```

---

## 📊 Benchmarks

Running on mid-range hardware (Ryzen 7, NVMe SSD):

| Operation | Ops/Sec      | Note                  |
|-----------|--------------|-----------------------|
| Write     | ~14,000      | Batched WAL Commit    |
| Read      | ~500k+       | Memory-first Cache    |
| Index Lookup | O(1)      | Hash Map              |

*Generated via `hvpdb shell > benchmark`*


---

## 📂 Project Structure
```
./hvp/
  └── myapp/
      ├── myapp.hvp          # Encrypted data
      ├── myapp.hvp.log      # WAL
      ├── myapp.hvp.lock
      └── myapp.hvp.writelock
```
Each database lives in its own folder for clarity and isolation.


---

## 👥 Who is HVPDB for?

- Solo developers

- Indie hackers

- Internal tools

- Security-sensitive apps

- CLI-first workflows


**Not designed for:**

- Massive distributed clusters

- Multi-terabyte analytics

- Replacing PostgreSQL at scale

## ⚠️ Known Limitations

- **Termux / Android**: Due to filesystem restrictions (lack of full `chmod`/`flock` support on shared storage/FUSE), you may encounter `[Errno 38] Function not implemented` or permission warnings. This is a platform limitation that cannot be fully resolved in version 1.0.2.

---

📜 License

Apache License 2.0

✅ Free for personal and commercial use

✅ Modification and redistribution allowed

❗ You may NOT use the name HVPDB, logo, or branding to promote derived products


See LICENSE and TRADEMARK.md for details.

Copyright © 2026 8w6s
