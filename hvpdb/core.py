import contextvars
import hashlib
import os
import secrets
import time
import uuid
import warnings
from typing import Any, Dict, List, Optional

try:
    from pydantic import BaseModel, ValidationError
except ImportError:
    BaseModel = None
    ValidationError = None

try:
    from argon2 import PasswordHasher
except ImportError:
    PasswordHasher = None

try:
    from importlib.metadata import entry_points
except ImportError:
    entry_points = None

from .storage import HVPStorage


class HVPGroup:
    """
    Represents a logical grouping of documents within HVPDB.
    
    Provides CRUD operations, indexing, and querying capabilities for 
    a specific namespace in the database.
    """

    def __init__(self, storage: HVPStorage, name: str, db_instance=None, schema=None):
        """
        Initialize a group context.
        
        Args:
            storage: The underlying HVPStorage instance.
            name: Name of the group.
            db_instance: Reference to the parent HVPDB.
            schema: Optional Pydantic schema for validation.
        """
        self.storage = storage
        self.name = name
        self.db = db_instance
        self.schema = schema
        self.indexes = {}
        self.unique_indexes = {}
        if name not in self.storage.data['groups']:
            self.storage.data['groups'][name] = {}
        if '_indexes' not in self.storage.data:
            self.storage.data['_indexes'] = {}
        self._rebuild_indexes()

    def find_one(self, query: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Find a single document matching the query.
        
        Args:
            query: Dictionary of field-value pairs to match.
            
        Returns:
            The first matching document or None.
        """
        if '_id' in query and len(query) == 1:
            return self.storage.data['groups'][self.name].get(query['_id'])
        for field, val in query.items():
            if field in self.unique_indexes and len(query) == 1:
                doc_id = self.unique_indexes[field].get(val)
                if doc_id:
                    return self.storage.data['groups'][self.name].get(doc_id)
                return None
        results = self.find(query, limit=1)
        return results[0] if results else None

    def _rebuild_indexes(self):
        """Reconstruct all indexes for this group from storage metadata."""
        self.indexes = {}
        self.unique_indexes = {}
        if '_indexes' not in self.storage.data:
            return
        if self.name not in self.storage.data['_indexes']:
            return
        defs = self.storage.data['_indexes'][self.name]
        for field, unique in defs.items():
            self.create_index(field, unique=unique, persist=False)

    def create_index(self, field: str, unique: bool=False, persist: bool=True):
        """
        Create an index on a specific field.
        
        Args:
            field: Field name to index.
            unique: If True, enforces uniqueness on the field.
            persist: If True, saves index definition to storage.
        """
        if unique:
            if field in self.unique_indexes:
                return
            self.unique_indexes[field] = {}
            for doc_id, doc in self.storage.data['groups'][self.name].items():
                val = doc.get(field)
                if val is not None:
                    if val in self.unique_indexes[field]:
                        raise ValueError(f"Duplicate value '{val}' for unique index '{field}'")
                    self.unique_indexes[field][val] = doc_id
        else:
            if field in self.indexes:
                return
            self.indexes[field] = {}
            for doc_id, doc in self.storage.data['groups'][self.name].items():
                val = doc.get(field)
                if val is not None:
                    if val not in self.indexes[field]:
                        self.indexes[field][val] = []
                    self.indexes[field][val].append(doc_id)
        if persist:
            if self.name not in self.storage.data['_indexes']:
                self.storage.data['_indexes'][self.name] = {}
            self.storage.data['_indexes'][self.name][field] = unique
            self.storage._dirty = True

    def _update_index(self, doc_id: str, old_doc: Optional[dict], new_doc: Optional[dict]):
        """
        Update indexes when a document is created, updated, or deleted.
        
        Args:
            doc_id: The unique ID of the document.
            old_doc: Previous document state (None for new inserts).
            new_doc: New document state (None for deletions).
            
        Raises:
            ValueError: If a unique constraint is violated.
        """
        if new_doc:
            for field, unique_map in self.unique_indexes.items():
                new_val = new_doc.get(field)
                old_val = old_doc.get(field) if old_doc else None
                if new_val is not None and new_val != old_val:
                    if new_val in unique_map:
                        raise ValueError(f"Duplicate key '{field}': '{new_val}' exists.")
        
        # Remove old values from indexes
        for field, idx_map in self.indexes.items():
            old_val = old_doc.get(field) if old_doc else None
            if old_val is not None and old_val in idx_map:
                if doc_id in idx_map[old_val]:
                    idx_map[old_val].remove(doc_id)
                    if not idx_map[old_val]:
                        del idx_map[old_val]
        
        # Add new values to indexes
        for field, idx_map in self.indexes.items():
            new_val = new_doc.get(field) if new_doc else None
            if new_val is not None:
                if new_val not in idx_map:
                    idx_map[new_val] = []
                idx_map[new_val].append(doc_id)
        
        # Update unique indexes
        for field, unique_map in self.unique_indexes.items():
            old_val = old_doc.get(field) if old_doc else None
            if old_val is not None and old_val in unique_map:
                if unique_map[old_val] == doc_id:
                    del unique_map[old_val]
            new_val = new_doc.get(field) if new_doc else None
            if new_val is not None:
                unique_map[new_val] = doc_id

    def find(self, query: Optional[dict]=None, limit: int=0) -> List[dict]:
        """
        Find documents matching a query.
        
        Args:
            query: Dictionary of criteria (field=value).
            limit: Maximum number of results to return (0 for all).
            
        Returns:
            List of matching documents.
        """
        res = list(self.find_iter(query))
        if limit > 0:
            return res[:limit]
        return res

    def find_iter(self, query: Optional[dict]=None):
        """
        Iterate over documents matching a query.
        
        Uses indexes for performance if available.
        
        Args:
            query: Dictionary of criteria (field=value).
            
        Yields:
            Matching documents.
        """
        if self.name not in self.storage.data['groups']:
            return iter([])
        gdata = self.storage.data['groups'][self.name]
        if not query:
            yield from gdata.values()
            return
        
        # Try unique index first (most efficient)
        query = query or {}
        for key, value in query.items():
            if key in self.unique_indexes:
                umap = self.unique_indexes[key]
                if value in umap:
                    doc_id = umap[value]
                    if doc_id in gdata:
                        doc = gdata[doc_id]
                        # Verify other fields match
                        match = True
                        for k, v in query.items():
                            if doc.get(k) != v:
                                match = False
                                break
                        if match:
                            yield doc
                        return
                    else:
                        return
        
        # Try standard indexes
        idx_matches = []
        for key, value in query.items():
            if key in self.indexes:
                if value in self.indexes[key]:
                    idx_matches.append(set(self.indexes[key][value]))
                else:
                    return # No intersection possible
        
        candidates = None
        if idx_matches:
            candidates = set.intersection(*idx_matches)
        
        if candidates is not None:
            for doc_id in candidates:
                if doc_id in gdata:
                    doc = gdata[doc_id]
                    match = True
                    for k, v in query.items():
                        if doc.get(k) != v:
                            match = False
                            break
                    if match:
                        yield doc
        else:
            # Full scan fallback
            for doc in gdata.values():
                match = True
                for k, v in query.items():
                    if doc.get(k) != v:
                        match = False
                        break
                if match:
                    yield doc

    def get_all(self) -> List[dict]:
        """
        Retrieve all documents in the group.
        
        Returns:
            List of all documents.
        """
        return list(self.storage.data['groups'][self.name].values())

    def get_all_iter(self):
        """
        Iterate over all documents in the group.
        
        Yields:
            Documents.
        """
        return self.storage.data['groups'][self.name].values()

    def _insert_mem(self, data: dict):
        """Internal: Add document to in-memory storage and update indexes."""
        doc_id = data['_id']
        group_data = self.storage.data['groups'][self.name]
        
        # Idempotency check: prevent duplicate inserts on WAL replay
        if doc_id in group_data:
            existing = group_data[doc_id]
            if existing != data:
                warnings.warn(f"Duplicate insert for {doc_id} with different data during replay/insert")
            return

        self._update_index(doc_id, None, data)
        group_data[doc_id] = data
        self.storage._dirty = True

    def insert(self, data: dict, external_txn_id: Optional[str]=None) -> dict:
        """
        Insert a new document into the group.
        
        Args:
            data: Document content.
            external_txn_id: Optional ID of an active transaction.
            
        Returns:
            The inserted document with generated metadata.
        """
        if self.schema and BaseModel and ValidationError:
            try:
                # Validate against Pydantic schema
                if isinstance(data, dict):
                    model = self.schema(**data)
                    data = model.model_dump()
            except ValidationError as e:
                raise ValueError(f"Schema Validation Error: {e}")

        if '_id' not in data:
            data['_id'] = str(uuid.uuid4())
        data['_created_at'] = time.time()
        
        txn_id = None
        is_implicit = True
        if external_txn_id:
            txn_id = external_txn_id
            is_implicit = False
        elif self.db and self.db.current_txn:
            txn_id = self.db.current_txn
            is_implicit = False
        else:
            txn_id = self.storage.begin_txn()
            
        try:
            self._insert_mem(data)
            self.storage.append_log('insert', self.name, str(data['_id']), data, txn_id=txn_id)
            if is_implicit:
                self.storage.commit_txn(txn_id)
            return data
        except Exception as e:
            if data['_id'] in self.storage.data['groups'][self.name]:
                self._delete_mem(data['_id'], data)
            if is_implicit:
                self.storage.rollback_txn(txn_id)
            warnings.warn(f"Insert failed, rolled back: {e}")
            raise

    def _update_mem(self, doc_id: str, update_data: dict, old_doc: dict):
        """Internal: Update document in-memory and refresh indexes."""
        new_state = old_doc.copy()
        new_state.update(update_data)

        if self.schema and BaseModel and ValidationError:
            try:
                self.schema(**new_state)
            except ValidationError as e:
                raise ValueError(f"Schema Validation Error on Update: {e}")

        self._update_index(doc_id, old_doc, new_state)
        doc = self.storage.data['groups'][self.name][doc_id]
        doc.update(update_data)
        doc['_updated_at'] = time.time()
        self.storage._dirty = True
        return doc

    def _restore_mem(self, doc_id: str, old_doc: dict):
        """Internal: Restore document to a previous state (used for rollbacks)."""
        cur = self.storage.data['groups'][self.name].get(doc_id)
        self._update_index(doc_id, cur, old_doc)
        self.storage.data['groups'][self.name][doc_id] = old_doc
        self.storage._dirty = True

    def update(self, query: dict, update_data: dict, external_txn_id: Optional[str]=None) -> int:
        """
        Update documents matching a query.
        
        Args:
            query: Criteria to match documents.
            update_data: Fields and values to update.
            external_txn_id: Optional active transaction ID.
            
        Returns:
            Number of documents updated.
        """
        docs = self.find(query)
        if not docs:
            return 0
        cnt = 0
        txn_id = None
        is_implicit = True
        if external_txn_id:
            txn_id = external_txn_id
            is_implicit = False
        elif self.db and self.db.current_txn:
            txn_id = self.db.current_txn
            is_implicit = False
        else:
            txn_id = self.storage.begin_txn()
        mod_log = []
        try:
            for doc in docs:
                old_doc = doc.copy()
                updated_doc = self._update_mem(doc['_id'], update_data, old_doc)
                mod_log.append((doc['_id'], old_doc))
                self.storage.append_log('update', self.name, doc['_id'], updated_doc, txn_id=txn_id, before_image=old_doc)
                cnt += 1
            if is_implicit:
                self.storage.commit_txn(txn_id)
            return cnt
        except Exception as e:
            for doc_id, old_doc in reversed(mod_log):
                self._restore_mem(doc_id, old_doc)
            if is_implicit:
                self.storage.rollback_txn(txn_id)
            warnings.warn(f"Update failed, rolled back {len(mod_log)} docs: {e}")
            raise

    def _delete_mem(self, doc_id: str, doc: dict):
        """Internal: Remove document from in-memory storage and update indexes."""
        self._update_index(doc_id, doc, None)
        del self.storage.data['groups'][self.name][doc_id]
        self.storage._dirty = True

    def delete(self, query: dict, external_txn_id: Optional[str]=None) -> int:
        """
        Delete documents matching a query.
        
        Args:
            query: Criteria to match documents.
            external_txn_id: Optional active transaction ID.
            
        Returns:
            Number of documents deleted.
        """
        docs = self.find(query)
        if not docs:
            return 0
        cnt = 0
        txn_id = None
        is_implicit = True
        if external_txn_id:
            txn_id = external_txn_id
            is_implicit = False
        elif self.db and self.db.current_txn:
            txn_id = self.db.current_txn
            is_implicit = False
        else:
            txn_id = self.storage.begin_txn()
        del_log = []
        try:
            for doc in docs:
                doc_copy = doc.copy()
                self._delete_mem(doc['_id'], doc)
                del_log.append((doc['_id'], doc_copy))
                self.storage.append_log('delete', self.name, doc['_id'], doc_copy, txn_id=txn_id, before_image=doc_copy)
                cnt += 1
            if is_implicit:
                self.storage.commit_txn(txn_id)
            return cnt
        except Exception as e:
            for doc_id, doc_data in reversed(del_log):
                self._insert_mem(doc_data)
            if is_implicit:
                self.storage.rollback_txn(txn_id)
            warnings.warn(f"Delete failed, rolled back {len(del_log)} docs: {e}")
            raise

    def count(self, query: Optional[dict]=None) -> int:
        """
        Count documents matching a query.
        
        Args:
            query: Criteria to match (None for all).
            
        Returns:
            Match count.
        """
        if query is None:
            return len(self.storage.data['groups'][self.name])
        return sum(1 for _ in self.find_iter(query))

    def append(self, op: str, data: dict):
        """
        Directly append an operation to the WAL (low-level).
        
        Args:
            op: Operation type ('insert', 'update', 'delete').
            data: Document data.
        """
        doc_id = data.get('_id') if data else ''
        self.storage.append_log(op, self.name, str(doc_id), data)

    def get_audit_trail(self, doc_id: Optional[str]=None, limit: int=100) -> List[Dict[str, Any]]:
        """
        Retrieve history of changes for this group or a specific document.
        
        Args:
            doc_id: Optional document ID to filter by.
            limit: Maximum number of entries.
            
        Returns:
            List of historical operations.
        """
        return self.storage.read_audit_log(self.name, doc_id, limit)

    def set(self, key: str, value: Any):
        """Set a key-value pair."""
        if 'kv' not in self.storage.data:
            self.storage.data['kv'] = {}
        key = str(key)
        self.storage.data['kv'][key] = value
        self.storage._dirty = True
        self.storage.append_log('set', 'kv', key, {'value': value})

    def delete_key(self, key: str):
        """Delete a key-value pair."""
        key = str(key)
        if 'kv' in self.storage.data and key in self.storage.data['kv']:
            del self.storage.data['kv'][key]
            self.storage._dirty = True
            self.storage.append_log('delete', 'kv', key, {})

    def get(self, key: str, default: Any=None) -> Any:
        """Get a value by key."""
        return self.storage.data.get('kv', {}).get(str(key), default)

class HVPDB:
    """
    Main database engine for HVPDB.
    
    Handles both single-file and cluster-mode database operations,
    transaction management, and plugin lifecycle.
    """

    _CLUSTER_META_FILENAME = '__hvpdb_meta__.hvp'
    _CLUSTER_META_GROUP_NAME = '__hvpdb_meta__'

    def __init__(self, path: str, password: Optional[str]=None, durable: bool=True):
        """
        Initialize the database engine.
        
        Args:
            path: File path or hvp:// URI.
            password: Authentication password.
            durable: Whether to use durable storage (WAL).
        """
        raw = path
        self.is_cluster = False
        base = os.path.basename(raw)
        if base.endswith('.hvp'):
            name = base[:-4]
        elif base.endswith('.hvdb'):
            name = base[:-5]
            self.is_cluster = True
        else:
            name = base
        if '://' in raw:
            self.filepath = raw
        elif os.path.isabs(raw) or os.path.dirname(raw):
            self.filepath = raw
            tdir = os.path.dirname(raw)
            if tdir and (not os.path.exists(tdir)):
                os.makedirs(tdir, exist_ok=True)
        else:
            bdir = 'hvp'
            if raw.endswith('.hvdb') or (os.path.exists(raw) and os.path.isdir(raw)):
                self.is_cluster = True
                tdir = os.path.join(bdir, name)
                self.filepath = tdir
            else:
                tdir = os.path.join(bdir, name)
                self.filepath = os.path.join(tdir, f'{name}.hvp')
            if not os.path.exists(tdir):
                os.makedirs(tdir, exist_ok=True)
        if self.is_cluster and '://' not in raw:
            if not os.path.exists(self.filepath):
                os.makedirs(self.filepath, exist_ok=True)
            elif not os.path.isdir(self.filepath):
                raise ValueError(f"Cluster path must be a directory: '{self.filepath}'")

        self.password = password
        if not self.password:
            from .utils import get_db_password
            self.password = get_db_password()
            if not self.password:
                 raise ValueError("Password required. Set HVPDB_PASSWORD or pass 'password' argument.")

        self.durable = durable
        self._user_ctx: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(f'user_{uuid.uuid4()}', default=None)
        self._txn_ctx: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(f'txn_{uuid.uuid4()}', default=None)
        self._groups = {}
        if self.is_cluster:
            meta_path = os.path.join(self.filepath, self._CLUSTER_META_FILENAME)
            self.storage = HVPStorage(meta_path, self.password, durable=self.durable)
        else:
            self.storage = HVPStorage(self.filepath, self.password, durable=self.durable)
        
        self.storage.load()
        if 'users' not in self.storage.data:
            self.storage.data['users'] = {}
            self._create_root_user()
        for grp in self.storage.data.get('groups', {}):
            if grp not in self._groups:
                self.group(grp)
        self.plugins = {}
        self.load_plugins()

    @property
    def current_user(self) -> Optional[str]:
        """Get the currently authenticated username."""
        return self._user_ctx.get()

    @current_user.setter
    def current_user(self, value: Optional[str]):
        """Set the currently authenticated username."""
        self._user_ctx.set(value)

    @property
    def current_txn(self) -> Optional[str]:
        """Get the ID of the current active transaction, if any."""
        return self._txn_ctx.get()

    @property
    def help(self):
        """Display help information about HVPDB."""
        from .hvpshell import HVPShell
        shell = HVPShell(self)
        shell.do_help('')

    def __getattr__(self, name: str) -> HVPGroup:
        """
        Dynamic access to groups as attributes.
        
        Example: db.my_group instead of db.group('my_group')
        """
        if name.startswith('_'):
            raise AttributeError(f"'HVPDB' object has no attribute '{name}'")
        try:
            return self.group(name)
        except ValueError as e:
            raise AttributeError(str(e)) from None

    def load_plugins(self):
        """Load HVPDB plugins registered via entry points."""
        if entry_points is None:
            return
        
        try:
            # try modern entry_points API (Python 3.10+)
            eps = entry_points(group='hvpdb.plugins')
        except TypeError:
            # fallback for older API
            try:
                eps = entry_points().get('hvpdb.plugins', [])
            except Exception as e:
                warnings.warn(f"Failed to discover plugins using older entry_points API: {e}")
                return

        for ep in eps:
            try:
                cls = ep.load()
                if isinstance(cls, type):
                    self.plugins[ep.name] = cls(self)
            except Exception as e:
                warnings.warn(f"Plugin '{ep.name}' failed to load: {e}")

    def _create_root_user(self):
        """Initialize the root administrator user if it doesn't exist."""
        if 'root' not in self.storage.data['users']:
            self.storage.data['users']['root'] = {'role': 'admin', 'groups': ['*'], 'created_at': time.time()}
            self.storage._dirty = True

    def hash_user_password(self, password: str) -> str:
        """
        Securely hash a user password.
        
        Args:
            password: Raw password string.
            
        Returns:
            Hashed password string (scrypt).
        """
        salt = secrets.token_bytes(16)
        # 2026 Recommended Parameters for scrypt: N=65536 (2^16), r=8, p=1
        key = hashlib.scrypt(password.encode(), salt=salt, n=65536, r=8, p=1, dklen=32)
        return f'scrypt${salt.hex()}${key.hex()}'

    def _verify_password(self, stored: str, password: str) -> bool:
        """
        Verify a password against its stored hash.
        
        Args:
            stored: The stored hash string.
            password: The raw password to verify.
            
        Returns:
            True if password matches, False otherwise.
        """
        # Timing attack mitigation: always perform some work
        if not stored:
            # Perform dummy check
            secrets.compare_digest('dummy', 'dummy')
            return False
            
        try:
            if stored.startswith('scrypt$'):
                _, salt_hex, key_hex = stored.split('$')
                salt = bytes.fromhex(salt_hex)
                # Try with 65536 first (new default), then 16384 (old)
                for n in (65536, 16384):
                    check = hashlib.scrypt(password.encode(), salt=salt, n=n, r=8, p=1, dklen=32)
                    if secrets.compare_digest(check.hex(), key_hex):
                        return True
                return False
            
            if '$' in stored and not stored.startswith('$argon2'):
                parts = stored.split('$')
                if len(parts) == 2:
                    salt, val = parts
                    if len(salt) == 16:
                        vhash = hashlib.sha256((salt + password).encode()).hexdigest()
                        return secrets.compare_digest(val, vhash)
            
            if PasswordHasher:
                return PasswordHasher().verify(stored, password)
        except Exception as e:
            warnings.warn(f"Password verification error: {e}")
        return False

    def authenticate(self, username: str, password: str) -> bool:
        """
        Authenticate a user by username and password.
        
        Args:
            username: User to authenticate.
            password: Raw password.
            
        Returns:
            True if authentication succeeded.
        """
        user = self.storage.data['users'].get(username)
        if not user:
            return False
        stored = user.get('password_hash')
        if not stored:
            return False
        if self._verify_password(stored, password):
            self.current_user = username
            return True
        return False

    def check_permission(self, username: str, group_name: str) -> bool:
        """
        Check if a user has access to a specific group.
        
        Args:
            username: Username to check.
            group_name: Group name to check access for.
            
        Returns:
            True if access is granted, False otherwise.
        """
        if username not in self.storage.data['users']:
            return False
        user = self.storage.data['users'][username]
        if user['role'] == 'admin':
            return True
        return group_name in user['groups'] or '*' in user['groups']

    def group(self, name: str, schema=None) -> HVPGroup:
        """
        Get or create a group context.
        
        Args:
            name: Group name.
            schema: Optional validation schema.
            
        Returns:
            HVPGroup instance.
        """
        if not name or any((c in name for c in '\\/:*?"<>|')):
            raise ValueError(f"Invalid group: '{name}'")
        if '..' in name or name.startswith('.') or name.endswith('.'):
            raise ValueError(f"Invalid group: '{name}' (Path Traversal Protection)")
        if self.is_cluster and name == self._CLUSTER_META_GROUP_NAME:
            raise ValueError(f"Invalid group: '{name}'")
        if name in self._groups:
            grp = self._groups[name]
            if schema:
                grp.schema = schema
            return grp
        if self.is_cluster:
            path = os.path.join(self.filepath, f'{name}.hvp')
            s = HVPStorage(path, self.password, durable=self.durable)
            s.load()
            if 'groups' not in s.data:
                s.data['groups'] = {}
            g = HVPGroup(s, name, self, schema=schema)
            self._groups[name] = g
            return g
        else:
            if name not in self._groups:
                self._groups[name] = HVPGroup(self.storage, name, self, schema=schema)
            return self._groups[name]

    def get_all_groups(self) -> List[str]:
        """
        List all available groups in the database.
        
        Returns:
            Sorted list of group names.
        """
        if self.is_cluster:
            gs = []
            if os.path.exists(self.filepath):
                for f in os.listdir(self.filepath):
                    if f.endswith('.hvp'):
                        if f == self._CLUSTER_META_FILENAME:
                            continue
                        gs.append(f[:-4])
            return sorted(gs)
        else:
            return list(self.storage.data.get('groups', {}).keys())

    def commit(self):
        """Persist all pending changes from all groups to disk."""
        if self.is_cluster:
            if self.storage and getattr(self.storage, '_dirty', False):
                self.storage.save()
            for _, grp in self._groups.items():
                if grp.storage._dirty:
                    grp.storage.save()
        elif self.storage._dirty:
            self.storage.save()

    def refresh(self, force: bool=False):
        """
        Reload database data from disk.
        
        Args:
            force: If True, discards unsaved in-memory changes.
        """
        self.storage.refresh(force=force)
        if self.is_cluster:
            for _, grp in self._groups.items():
                grp.storage.refresh(force=force)
        else:
            for grp in self._groups:
                self._groups[grp]._rebuild_indexes()

    def close(self):
        """
        Commit pending changes and release all storage resources.
        
        Closes WAL files and clears security keys from memory.
        """
        self.commit()
        
        def _cleanup_storage(s):
            if s:
                if hasattr(s, 'wal') and s.wal:
                    s.wal.close()
                if s.security:
                    s.security.clear_key()

        _cleanup_storage(self.storage)
        for grp in self._groups.values():
            _cleanup_storage(grp.storage)

    def begin(self):
        """
        Start a new transaction context.
        
        Returns:
            HVPTransaction context manager.
            
        Raises:
            RuntimeError: If called in cluster mode (unsupported).
        """
        if self.is_cluster:
            raise RuntimeError('Transactions not supported in cluster mode.')
        from .transaction import HVPTransaction
        return HVPTransaction(self)

    def change_password(self, new_password: str, auth_type: Optional[str] = None):
        """
        Update the database encryption password.
        
        This will re-encrypt all storage files with the new password.
        
        Args:
            new_password: The new master password.
            auth_type: Optional new authentication type ('password', 'access_key').
                       If None, preserves the current type.
        """
        self.password = new_password
        
        def _reencrypt(s, pwd, atype):
            # Preserve existing KDF params (e.g. cost settings) but generate new salt
            old_params = s.security.get_kdf_params() if s.security else {}
            
            s.password = pwd
            s.security = None # Force re-init with new password
            
            # Re-initialize security with preserved params (except auth_type if changed)
            # We need to temporarily hook into _init_security or manually init
            # Since s.save() calls _init_security(), we need a way to pass params there.
            # Best way: Initialize it right here.
            
            # Prepare new params
            new_params = old_params.copy()
            if atype:
                new_params['auth_type'] = atype
            elif 'auth_type' not in new_params:
                new_params['auth_type'] = 'password'
                
            from .security import HVPSecurity
            s.security = HVPSecurity(pwd, kdf_params=new_params)
            
            s._dirty = True
            s.save()

        _reencrypt(self.storage, new_password, auth_type)
        if self.is_cluster:
            for grp in self._groups.values():
                _reencrypt(grp.storage, new_password, auth_type)

    def set(self, key: str, value: Any):
        """
        Set a key-value pair in the global store.
        
        Args:
            key: Key name.
            value: Value to store.
        """
        key = str(key)
        if self.is_cluster:
            # Propagate to all groups (simple broadcast)
            for grp in self._groups.values():
                grp.set(key, value)
            return
        
        if 'kv' not in self.storage.data:
            self.storage.data['kv'] = {}
        
        self.storage.data['kv'][key] = value
        self.storage._dirty = True
        self.storage.append_log('set', 'kv', key, {'value': value})

    def get(self, key: str, default: Any=None) -> Any:
        """
        Retrieve a value from the global store.
        
        Args:
            key: Key name.
            default: Value if key not found.
            
        Returns:
            Value or default.
        """
        return self.storage.data.get('kv', {}).get(str(key), default)
