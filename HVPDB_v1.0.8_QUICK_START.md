# HVPDB v1.0.8 - Quick Reference Guide

## 1️⃣ Hooks/Triggers

### Register a Hook
```python
from hvpdb import HVPDB

db = HVPDB("mydb.hvp", password="secret")

# Simple hook
def on_user_created(doc):
    print(f"✓ User created: {doc['name']}")

db.users.register_hook('post_insert', on_user_created)

# Insert will trigger the hook
db.users.insert({'name': 'Alice', 'email': 'alice@example.com'})
# Output: ✓ User created: Alice
```

### All Hook Types
```python
# Pre-operation hooks (can modify or validate)
db.users.register_hook('pre_insert', lambda doc: None)
db.users.register_hook('pre_update', lambda doc: None)
db.users.register_hook('pre_delete', lambda doc: None)

# Post-operation hooks (notification/logging)
db.users.register_hook('post_insert', lambda doc: None)
db.users.register_hook('post_update', lambda doc: None)
db.users.register_hook('post_delete', lambda doc: None)
```

### Unregister Hook
```python
db.users.unregister_hook('post_insert', on_user_created)
```

---

## 2️⃣ GraphQL API

### Enable (Automatic)
```python
from hvpdb.server import start_server

start_server("mydb.hvp", password="secret", host="0.0.0.0", port=2321)
# GraphQL available at: http://localhost:2321/graphql
```

### Query Examples
```graphql
# Get all groups
{
  groups
}

# Fetch documents with filtering
{
  groupDocs(
    groupName: "users"
    queryJson: "{\"role\": \"admin\"}"
  )
}
```

### Using with Apollo Client (JavaScript)
```javascript
import { gql, ApolloClient, InMemoryCache, HttpLink } from '@apollo/client';

const client = new ApolloClient({
  link: new HttpLink({
    uri: 'http://localhost:2321/graphql',
    headers: {
      'X-HVP-Key': 'your-password'
    }
  }),
  cache: new InMemoryCache(),
});

const QUERY = gql`
  {
    groups
    groupDocs(groupName: "users", queryJson: "{}")
  }
`;

client.query({ query: QUERY }).then(result => {
  console.log(result.data);
});
```

---

## 3️⃣ Query Explain & Profiling

### EXPLAIN - Get Query Plan
```python
# Analyze query strategy
query = {'email': 'test@example.com'}
plan = db.users.explain(query)

print(f"Strategy: {plan['execution_strategy']}")
# Output: Strategy: unique_index

print(f"Indexed: {plan['has_indexes']}")
# Output: Indexed: True

print(f"Index used: {plan['index_usage']}")
# Output: Index used: [{'type': 'unique', 'field': 'email', 'estimated_docs_returned': 1}]
```

### PROFILE - Measure Performance
```python
# Time actual query execution
perf = db.users.profile('find', {'role': 'admin'})

print(f"Found {perf['docs_found']} docs in {perf['execution_time_ms']:.2f}ms")
# Output: Found 15 docs in 3.45ms

print(f"Memory change: {perf['memory_delta_bytes']} bytes")
# Output: Memory change: 4096 bytes
```

### Identify Slow Queries
```python
# Create index for frequent queries
db.users.create_index('email', unique=True)

# Check if it helps
before = db.users.profile('find', {'email': 'alice@example.com'})
print(f"With index: {before['execution_time_ms']:.2f}ms")

# Without index (full scan)
after = db.users.profile('find', {'name': 'Alice'})
print(f"Without index: {after['execution_time_ms']:.2f}ms")
```

---

## 4️⃣ Default Values in Schema

### Using Pydantic
```python
from pydantic import BaseModel, Field
from datetime import datetime

class User(BaseModel):
    name: str
    email: str
    role: str = Field(default="user")  # Default value
    is_active: bool = Field(default=True)
    tags: list = Field(default_factory=list)  # Dynamic default
    created_at: datetime = Field(default_factory=datetime.now)

# Register schema
db.users.schema = User

# Insert with partial data
db.users.insert({
    'name': 'Alice',
    'email': 'alice@example.com'
    # role, is_active, tags, created_at auto-filled
})

# Result in database
result = db.users.find_one({'name': 'Alice'})
print(result)
# {
#   '_id': '<uuid>',
#   'name': 'Alice',
#   'email': 'alice@example.com',
#   'role': 'user',           # ← auto-filled
#   'is_active': True,         # ← auto-filled
#   'tags': [],                # ← auto-filled
#   'created_at': '2026-03-28T...'  # ← auto-filled
# }
```

### Without Pydantic
```python
# Manual approach (if not using Pydantic)
doc = {'name': 'Bob', 'email': 'bob@example.com'}
if 'role' not in doc:
    doc['role'] = 'user'
db.users.insert(doc)
```

---

## 🔧 Combining All Features

### Real-world Example: User Management System

```python
from pydantic import BaseModel, Field
from datetime import datetime
from hvpdb import HVPDB

# 1. Define schema with defaults
class User(BaseModel):
    name: str
    email: str
    role: str = Field(default="user")
    is_active: bool = Field(default=True)
    created_at: datetime = Field(default_factory=datetime.now)

db = HVPDB("users_db.hvp", password="secure_password")
db.users.schema = User

# 2. Setup hooks for auditing
def log_user_creation(doc):
    print(f"📝 New user registered: {doc['email']}")
    # Could also send email, webhook, etc.

def log_user_deletion(doc):
    print(f"🗑️  User deleted: {doc['email']}")

db.users.register_hook('post_insert', log_user_creation)
db.users.register_hook('post_delete', log_user_deletion)

# 3. Create indexes
db.users.create_index('email', unique=True)
db.users.create_index('role')

# 4. Insert users
db.users.insert({'name': 'Alice', 'email': 'alice@example.com'})
# Output: 📝 New user registered: alice@example.com
# (role='user', is_active=True auto-filled)

# 5. Query analysis
admin_plan = db.users.explain({'role': 'admin'})
print(f"Admin query uses index: {admin_plan['has_indexes']}")

# 6. Profile operations
perf = db.users.profile('find', {'role': 'admin'})
print(f"Found {perf['docs_found']} admins in {perf['execution_time_ms']:.2f}ms")

# 7. GraphQL API available at /graphql
# POST http://localhost:2321/graphql with:
# { groupDocs(groupName: "users", queryJson: "{}") }
```

---

## 📚 Documentation Links

- **Full reference**: `hvpdb_definitive_reference.md`
- **API docs**: Check docstrings in `hvpdb/core.py`
- **Changelog**: `CHANGELOG.md`

---

## ⚡ Tips & Tricks

### Tip 1: Chain Multiple Hooks
```python
def validate_email(doc):
    if '@' not in doc['email']:
        raise ValueError("Invalid email")

def send_welcome_email(doc):
    # Send email...
    pass

db.users.register_hook('pre_insert', validate_email)
db.users.register_hook('post_insert', send_welcome_email)
```

### Tip 2: Profile During Optimization
```python
# Before optimization
before = db.users.profile('find', {'name': 'Alice'})

# Create index
db.users.create_index('name')

# After optimization
after = db.users.profile('find', {'name': 'Alice'})

speedup = before['execution_time_ms'] / after['execution_time_ms']
print(f"Index speedup: {speedup:.2f}x")
```

### Tip 3: Use GraphQL for Frontend Integration
```python
# Python backend exposes database via GraphQL
# Frontend can query without Python dependency
```

---

**Version**: HVPDB v1.0.8  
**Last Updated**: 2026-03-28

