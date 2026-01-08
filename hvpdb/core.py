from .storage import HVPStorage
import uuid
import time
import os
import contextvars
from typing import Dict, Any, List, Optional, Union

class HVPGroup:
    """
    Represents a Collection/Table in HVPDB.
    Handles CRUD operations and Indexing.
    """
    def __init__(self, storage: HVPStorage, name: str, db_instance=None):
        self.storage = storage
        self.name = name
        self.db = db_instance
        # Indexes: {field_name: {value: [doc_id, ...]}}
        self.indexes: Dict[str, Dict[Any, List[str]]] = {}
        # Unique Indexes: {field_name: {value: doc_id}} (Optimization: Map value to ID)
        self.unique_indexes: Dict[str, Dict[Any, str]] = {}
        
        # Ensure group exists
        if name not in self.storage.data["groups"]:
            self.storage.data["groups"][name] = {}
            
        # Load Index Definitions (Persistence)
        if "_indexes" not in self.storage.data:
            self.storage.data["_indexes"] = {}
            
        self._rebuild_indexes()

    def _rebuild_indexes(self):
        """Rebuilds in-memory indexes from stored definitions."""
        # Clear existing indexes to ensure they match new data
        self.indexes = {}
        self.unique_indexes = {}

        if "_indexes" not in self.storage.data:
            return

        if self.name not in self.storage.data["_indexes"]:
            return

        index_defs = self.storage.data["_indexes"][self.name]
        for field, unique in index_defs.items():
            if unique:
                self.create_index(field, unique=True, persist=False)
            else:
                self.create_index(field, unique=False, persist=False)

    def create_index(self, field: str, unique: bool = False, persist: bool = True):
        """Creates an index on a specific field."""
        if unique:
            if field in self.unique_indexes: return
            self.unique_indexes[field] = {}
            
            # Populate and validate unique index
            for doc_id, doc in self.storage.data["groups"][self.name].items():
                val = doc.get(field)
                if val is not None:
                    if val in self.unique_indexes[field]:
                         raise ValueError(f"Cannot create unique index on '{field}': Duplicate value '{val}' found.")
                    self.unique_indexes[field][val] = doc_id
        else:
            if field in self.indexes: return
            
            self.indexes[field] = {}
            # Populate index
            for doc_id, doc in self.storage.data["groups"][self.name].items():
                val = doc.get(field)
                if val is not None:
                    if val not in self.indexes[field]:
                        self.indexes[field][val] = []
                    self.indexes[field][val].append(doc_id)
        
        if persist:
            if self.name not in self.storage.data["_indexes"]:
                self.storage.data["_indexes"][self.name] = {}
            self.storage.data["_indexes"][self.name][field] = unique
            self.storage._dirty = True

    def _update_index(self, doc_id: str, old_doc: Optional[dict], new_doc: Optional[dict]):
        """Internal: Updates in-memory indexes when data changes."""
        # 1. Check Unique Constraints first (Fail Fast)
        if new_doc:
            for field, unique_map in self.unique_indexes.items():
                new_val = new_doc.get(field)
                old_val = old_doc.get(field) if old_doc else None
                
                # Only check if value is not None and (it's a new insert OR value changed)
                if new_val is not None and new_val != old_val:
                    if new_val in unique_map:
                        raise ValueError(f"Duplicate key error: Field '{field}' must be unique. Value '{new_val}' already exists.")

        # 2. Update Standard Indexes
        for field, index_map in self.indexes.items():
            # Remove old value
            old_val = old_doc.get(field) if old_doc else None
            if old_val is not None and old_val in index_map:
                if doc_id in index_map[old_val]:
                    index_map[old_val].remove(doc_id)
                    if not index_map[old_val]:
                        del index_map[old_val]
            
            # Add new value
            new_val = new_doc.get(field) if new_doc else None
            if new_val is not None:
                if new_val not in index_map:
                    index_map[new_val] = []
                index_map[new_val].append(doc_id)
                
        # 3. Update Unique Indexes
        for field, unique_map in self.unique_indexes.items():
            # Remove old
            old_val = old_doc.get(field) if old_doc else None
            if old_val is not None and old_val in unique_map:
                 # Verify it points to this doc (sanity check)
                 if unique_map[old_val] == doc_id:
                     del unique_map[old_val]
                
            # Add new
            new_val = new_doc.get(field) if new_doc else None
            if new_val is not None:
                unique_map[new_val] = doc_id

    def find(self, query: dict = None) -> List[dict]:
        """Finds documents matching the query (Optimized)."""
        if self.name not in self.storage.data["groups"]:
            return []
            
        group_data = self.storage.data["groups"][self.name]
        if not query:
            return list(group_data.values())
            
        results = []
        
        # --- Advanced Query Optimizer ---
        
        # 1. Fast Path: Unique Index Lookup
        # If any query field matches a unique index, we can get the doc immediately.
        for key, value in query.items():
            if key in self.unique_indexes:
                 unique_map = self.unique_indexes[key]
                 if value in unique_map:
                     doc_id = unique_map[value]
                     if doc_id in group_data:
                         doc = group_data[doc_id]
                         # Verify remaining fields
                         match = True
                         for k, v in query.items():
                             if doc.get(k) != v:
                                 match = False
                                 break
                         if match:
                             return [doc]
                     return [] 
                 else:
                     return [] # Unique field queried but value not found -> 0 results
        
        # 2. Intersection Path: Standard Indexes
        # Collect candidate sets from all available indexes
        indexed_matches = []
        for key, value in query.items():
            if key in self.indexes:
                if value in self.indexes[key]:
                    indexed_matches.append(set(self.indexes[key][value]))
                else:
                    return [] # Indexed field queried but value not found -> 0 results

        candidates = None
        if indexed_matches:
            # Intersection of all available index hits (Drastically reduces search space)
            candidates = set.intersection(*indexed_matches)
        
        # 3. Execution Phase
        if candidates is not None:
             # Scan only candidates
             for doc_id in candidates:
                if doc_id in group_data:
                    doc = group_data[doc_id]
                    # Verify unindexed fields
                    match = True
                    for k, v in query.items():
                        if doc.get(k) != v:
                            match = False
                            break
                    if match:
                        results.append(doc)
        else:
            # Full Scan (Fallback)
            for doc in group_data.values():
                match = True
                for k, v in query.items():
                    if doc.get(k) != v:
                        match = False
                        break
                if match:
                    results.append(doc)
                
        return results

    def find_one(self, query: dict = None) -> Optional[dict]:
        """Finds a single document."""
        results = self.find(query)
        return results[0] if results else None

    def _insert_mem(self, data: dict):
        """Internal: Apply insert to memory and indexes."""
        # Check constraints & Update indexes (will raise if duplicate)
        self._update_index(data["_id"], None, data)
        self.storage.data["groups"][self.name][data["_id"]] = data
        self.storage._dirty = True

    def insert(self, data: dict) -> dict:
        """Inserts a new document."""
        if "_id" not in data:
            data["_id"] = str(uuid.uuid4())
        
        data["_created_at"] = time.time()
        
        # Check for active transaction from DB context
        txn_id = None
        is_implicit = True
        
        if self.db and self.db.current_txn:
            txn_id = self.db.current_txn
            is_implicit = False
        else:
            txn_id = self.storage.begin_txn()
            
        try:
            # 1. Validate & Update Memory (Optimistic)
            self._insert_mem(data)
            
            # 2. Write to WAL (Durability) with Txn ID
            self.storage.append_log("insert", self.name, data["_id"], data, txn_id=txn_id)
            
            # 3. Commit (Only if we started the transaction)
            if is_implicit:
                self.storage.commit_txn(txn_id)
            return data
        except Exception:
            # Rollback Memory: Remove the inserted document
            if data["_id"] in self.storage.data["groups"][self.name]:
                self._delete_mem(data["_id"], data)
                
            # Rollback Txn (Only if we started it)
            if is_implicit:
                self.storage.rollback_txn(txn_id)
            raise

    def _update_mem(self, doc_id: str, update_data: dict, old_doc: dict):
        """Internal: Apply update to memory."""
        new_state = old_doc.copy()
        new_state.update(update_data)
        
        # Update index (raises if unique violation)
        self._update_index(doc_id, old_doc, new_state)
        
        # Apply update
        doc = self.storage.data["groups"][self.name][doc_id]
        doc.update(update_data)
        doc["_updated_at"] = time.time()
        self.storage._dirty = True
        return doc
    
    def _restore_mem(self, doc_id: str, old_doc: dict):
        """Internal: Restore document state (used for memory rollback)."""
        current_doc = self.storage.data["groups"][self.name].get(doc_id)
        # Revert index
        self._update_index(doc_id, current_doc, old_doc)
        # Revert data
        self.storage.data["groups"][self.name][doc_id] = old_doc
        self.storage._dirty = True

    def update(self, query: dict, update_data: dict) -> int:
        docs = self.find(query)
        if not docs:
            return 0

        updated_count = 0
        txn_id = self.storage.begin_txn()
        
        # Track modified documents for memory rollback
        modified_log = [] # List of (doc_id, old_doc)
        
        try:
            for doc in docs:
                old_doc = doc.copy() # Capture Before Image
                
                # 1. Update Memory
                updated_doc = self._update_mem(doc["_id"], update_data, old_doc)
                modified_log.append((doc["_id"], old_doc))
                
                # 2. WAL (Append with Before Image)
                self.storage.append_log("update", self.name, doc["_id"], updated_doc, txn_id=txn_id, before_image=old_doc)
                
                updated_count += 1
            
            self.storage.commit_txn(txn_id)
            return updated_count
        except Exception:
            # Rollback Memory
            for doc_id, old_doc in reversed(modified_log):
                self._restore_mem(doc_id, old_doc)
                
            self.storage.rollback_txn(txn_id)
            raise
    
    def _delete_mem(self, doc_id: str, doc: dict):
        """Internal: Apply delete to memory."""
        self._update_index(doc_id, doc, None)
        del self.storage.data["groups"][self.name][doc_id]
        self.storage._dirty = True

    def delete(self, query: dict) -> int:
        docs = self.find(query)
        if not docs:
            return 0
            
        deleted_count = 0
        txn_id = self.storage.begin_txn()
        
        # Track deleted documents for memory rollback
        deleted_log = [] # List of (doc_id, full_doc)
        
        try:
            for doc in docs:
                doc_copy = doc.copy() # Capture Before Image
                
                # 1. Memory
                self._delete_mem(doc["_id"], doc)
                deleted_log.append((doc["_id"], doc_copy))
                
                # 2. WAL
                self.storage.append_log("delete", self.name, doc["_id"], doc_copy, txn_id=txn_id, before_image=doc_copy)
                
                deleted_count += 1
            
            self.storage.commit_txn(txn_id)
            return deleted_count
        except Exception:
            # Rollback Memory: Restore deleted documents
            for doc_id, doc_data in reversed(deleted_log):
                # We can reuse _insert_mem but we need to bypass _created_at override?
                # _insert_mem doesn't override _created_at if present in data.
                # However, _insert_mem assumes data["_id"] exists.
                self._insert_mem(doc_data)
                
            self.storage.rollback_txn(txn_id)
            raise
    
    def count(self, query: dict = None) -> int:
        return len(self.find(query))

    def append(self, op: str, data: dict):
        """Internal: Append operation to WAL via Storage."""
        if hasattr(self.storage, 'append_log'):
            # Extract doc_id if present
            doc_id = data.get("_id") if data else None
            self.storage.append_log(op, self.name, doc_id, data)

    def get_audit_trail(self, doc_id: str = None, limit: int = 100) -> List[dict]:
        """
        Retrieves the audit trail (history) of changes.
        If doc_id is provided, filters by document ID.
        """
        return self.storage.read_audit_log(self.name, doc_id, limit)

import hashlib
import secrets
import difflib

class HVPDB:
    """
    Main Database Interface for HVPDB.
    Supports Single File Mode and Cluster Mode (Directory).
    """
    def __init__(self, filepath_or_uri: str, password: str = None):
        # Path Normalization Logic (New Structure: ./hvp/{name}/{name}.hvp)
        raw_path = filepath_or_uri
        self.is_cluster = False
        
        # 1. Parse Name
        basename = os.path.basename(raw_path)
        if basename.endswith(".hvp"):
            db_name = basename[:-4]
        elif basename.endswith(".hvdb"):
            db_name = basename[:-5]
            self.is_cluster = True
        else:
            db_name = basename
            
        # 2. Determine Real Path
        # If raw_path is just a name or relative path without dirs, force structure
        # If user provided explicit directory (e.g. /tmp/db), we might respect it?
        # User requirement: "design storage format... ./hvp/{file_name}/..."
        # We will enforce this for local files.
        
        if "://" in raw_path:
             # URI handling (keep as is for now, assuming storage handles it)
             self.filepath = raw_path
        else:
             base_dir = "hvp"
             # If it's a cluster (.hvdb), it's a directory.
             if raw_path.endswith(".hvdb") or (os.path.exists(raw_path) and os.path.isdir(raw_path)):
                 self.is_cluster = True
                 target_dir = os.path.join(base_dir, db_name)
                 self.filepath = target_dir
             else:
                 # Single file mode
                 target_dir = os.path.join(base_dir, db_name)
                 self.filepath = os.path.join(target_dir, f"{db_name}.hvp")
             
             # Create Directory Structure
             if not os.path.exists(target_dir):
                 os.makedirs(target_dir, exist_ok=True)

        self.password = password
        # Thread-safe user context per instance (ContextVar ensures thread/async safety)
        self._user_ctx = contextvars.ContextVar(f"user_{uuid.uuid4()}", default=None)
        # Thread-safe transaction context
        self._txn_ctx = contextvars.ContextVar(f"txn_{uuid.uuid4()}", default=None)
        self._groups = {} 
        
        if self.is_cluster:
            self.storage = None 
        else:
            self.storage = HVPStorage(self.filepath, self.password)
            self.storage.load()
            
            # Initialize Users System
        if "users" not in self.storage.data:
            self.storage.data["users"] = {}
            # Create root user if not exists
            self._create_root_user()

        # REBUILD INDEXES (Fix: Ensure indexes match loaded data)
        # Iterate over all loaded groups and ensure indexes are consistent
        for grp_name in self.storage.data.get("groups", {}):
            if grp_name not in self._groups:
                # We instantiate HVPGroup to trigger _rebuild_indexes
                self.group(grp_name)

        # Load Plugins
        self.plugins = {}
        self.load_plugins()

    @property
    def current_user(self):
        """Thread-safe access to current authenticated user."""
        return self._user_ctx.get()

    @current_user.setter
    def current_user(self, value):
        self._user_ctx.set(value)

    @property
    def current_txn(self):
        """Thread-safe access to current transaction ID."""
        return self._txn_ctx.get()

    @property
    def help(self):
        """Prints a quick usage guide."""
        msg = """
        HVPDB Quick Guide 🚀
        --------------------
        1. Manage Groups (Collections):
           - g = db.users              # Magic access! (or db.group('users'))
           - db.get_all_groups()       # List groups

        2. CRUD Operations:
           - g.insert({"name": "Alice"}) 
           - g.find({"name": "Alice"})
           - g.update({"name": "Alice"}, {"role": "admin"})
           - g.delete({"name": "Alice"})
           
        3. Indexing:
           - g.create_index("email", unique=True)
           
        4. Plugins (e.g., Permissions):
           - perms = db.plugins.get('perms')
           - if perms: perms.list_users()
        
        5. Transactions/Save:
           - db.commit()               # Save to disk
           - db.close()                # Save & Close
        """
        print(msg)

    def __getattr__(self, name: str):
        """
        Magic attribute access for Groups.
        Allows db.users instead of db.group('users').
        """
        # Avoid infinite recursion for internal attributes
        if name.startswith("_"):
            raise AttributeError(f"'HVPDB' object has no attribute '{name}'")
            
        # Treat unknown attributes as Groups
        return self.group(name)

    def load_plugins(self):
        """Loads plugins via entry points."""
        try:
            from importlib.metadata import entry_points
            # Python 3.10+ style
            eps = entry_points(group='hvpdb.plugins')
        except TypeError:
            # Fallback for older Python versions
            try:
                from importlib.metadata import entry_points
                eps = entry_points().get('hvpdb.plugins', [])
            except ImportError:
                 return # No plugin support if importlib missing

        for ep in eps:
            try:
                plugin_cls = ep.load()
                # Initialize plugin with DB instance
                self.plugins[ep.name] = plugin_cls(self)
            except Exception as e:
                print(f"Failed to load plugin {ep.name}: {e}")

    def _create_root_user(self):
        """Creates the default root user."""
        if "root" not in self.storage.data["users"]:
            # Default root has no password initially or inherits DB password?
            # For simplicity, we set a default or empty password that must be changed.
            # However, since we are encrypted, access to this function implies we have the DB key.
            # We will store 'root' with role 'admin'.
            self.storage.data["users"]["root"] = {
                "role": "admin",
                "groups": ["*"],
                "created_at": time.time()
            }
            self.storage._dirty = True

    def hash_user_password(self, password: str) -> str:
        """Public API for hashing user passwords."""
        return self._hash_password(password)

    def _hash_password(self, password: str) -> str:
        """Secure hashing using Argon2id (if available) or Scrypt."""
        try:
            from argon2 import PasswordHasher
            ph = PasswordHasher()
            return ph.hash(password)
        except ImportError:
            # Fallback to Scrypt (Standard Library)
            salt = secrets.token_bytes(16)
            key = hashlib.scrypt(password.encode(), salt=salt, n=16384, r=8, p=1, dklen=32)
            return f"scrypt${salt.hex()}${key.hex()}"

    def _verify_password(self, stored_hash: str, password: str) -> bool:
        if not stored_hash: return False
        
        try:
            if stored_hash.startswith("scrypt$"):
                _, salt_hex, key_hex = stored_hash.split("$")
                salt = bytes.fromhex(salt_hex)
                check_key = hashlib.scrypt(password.encode(), salt=salt, n=16384, r=8, p=1, dklen=32)
                return secrets.compare_digest(check_key.hex(), key_hex)
            else:
                # Assume Argon2 or legacy SHA256 (for migration)
                if "$" in stored_hash and not stored_hash.startswith("$argon2"):
                     # Legacy SHA256 fallback (to avoid locking out old users if any)
                     salt, hash_val = stored_hash.split("$")
                     if len(salt) == 16: # Hex(8) = 16 chars
                         verify_hash = hashlib.sha256((salt + password).encode()).hexdigest()
                         return secrets.compare_digest(hash_val, verify_hash)

                from argon2 import PasswordHasher
                ph = PasswordHasher()
                return ph.verify(stored_hash, password)
        except Exception:
            return False

    def authenticate(self, username: str, password: str) -> bool:
        """Authenticates a user and sets the current session context."""
        user = self.storage.data["users"].get(username)
        if not user:
            return False
            
        # Handle case where password_hash might be missing (e.g. initial root)
        stored_hash = user.get("password_hash")
        if not stored_hash:
            return False
            
        if self._verify_password(stored_hash, password):
            # In a real app, you'd return a token. Here we return True.
            self.current_user = username # Set current user context
            return True
        return False

    def check_permission(self, username: str, group_name: str) -> bool:
        """Checks if a user has access to a group."""
        if username not in self.storage.data["users"]:
            return False
        
        user = self.storage.data["users"][username]
        if user["role"] == "admin":
            return True
            
        return group_name in user["groups"] or "*" in user["groups"]

    def group(self, name: str) -> HVPGroup:
        """Gets or creates a group (collection)."""
        # Security: Validate group name to prevent Path Traversal
        if not name or any(c in name for c in r'\/:*?"<>|'):
            raise ValueError(f"Invalid group name: '{name}'. Cannot contain special characters.")
            
        if name in self._groups:
            return self._groups[name]
            
        if self.is_cluster:
            # Secure path joining is handled, but validation above prevents '..' injection
            group_path = os.path.join(self.filepath, f"{name}.hvp")
            storage = HVPStorage(group_path, self.password)
            storage.load()
            
            if "groups" not in storage.data:
                storage.data["groups"] = {}
            
            g = HVPGroup(storage, name, self)
            self._groups[name] = g
            return g
        else:
            if name not in self._groups:
                self._groups[name] = HVPGroup(self.storage, name, self)
            return self._groups[name]

    def get_all_groups(self) -> List[str]:
        if self.is_cluster:
            groups = []
            if os.path.exists(self.filepath):
                for f in os.listdir(self.filepath):
                    if f.endswith(".hvp"):
                        groups.append(f[:-4])
            return sorted(groups)
        else:
            return list(self.storage.data.get("groups", {}).keys())

    def commit(self):
        """Saves all changes to disk."""
        if self.is_cluster:
            for _, grp in self._groups.items():
                if grp.storage._dirty:
                    grp.storage.save()
        else:
            if self.storage._dirty:
                self.storage.save()

    def refresh(self):
        """Refreshes data from disk (discards unsaved changes if any? No, checks dirty)."""
        if self.is_cluster:
            for _, grp in self._groups.items():
                grp.storage.refresh()
        else:
            self.storage.refresh()
            # Rebuild indexes if data changed?
            # load() replaces self.storage.data, but indexes are in HVPGroup.
            # HVPGroup indexes need to be rebuilt!
            for grp_name in self._groups:
                self._groups[grp_name]._rebuild_indexes()

    def close(self):
        """Commits changes and securely clears sensitive keys from memory."""
        self.commit()
        
        # Security Hygiene: Clear keys
        if self.storage and self.storage.security:
            self.storage.security.clear_key()
            
        if self.is_cluster:
            for grp in self._groups.values():
                if grp.storage and grp.storage.security:
                    grp.storage.security.clear_key()

    def begin(self):
        """Starts a new atomic transaction."""
        from .transaction import HVPTransaction
        return HVPTransaction(self)
