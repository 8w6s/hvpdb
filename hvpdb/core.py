import contextlib
import contextvars
import functools
import hashlib
import json
import os
import re
import secrets
import threading
import time
import uuid
import warnings
import zlib
from typing import Any, Dict, List, Optional, Type

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
from .uri import HVPURI
from .transaction import HVPTransaction


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
        self._computed_fields = {}
        # Hooks/Triggers system (v1.0.8+)
        self._hooks = {
            'pre_insert': [],
            'post_insert': [],
            'pre_update': [],
            'post_update': [],
            'pre_delete': [],
            'post_delete': []
        }
        if name not in self.storage.data['groups']:
            self.storage.data['groups'][name] = {}
        
        # Phase 11: Register reload callback to ensure cache consistency 
        # when other processes update the disk.
        self.storage.register_reload_callback(self._invalidate_cache)
        if '_indexes' not in self.storage.data:
            self.storage.data['_indexes'] = {}
        self._rebuild_indexes()

    def __del__(self):
        """Cleanup: Unregister reload callback when group is garbage collected."""
        try:
            if hasattr(self, 'storage') and hasattr(self, '_invalidate_cache'):
                self.storage.unregister_reload_callback(self._invalidate_cache)
        except Exception:
            pass  # Ignore errors during cleanup

    def find_one(self, query: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Find a single document matching the query.
        """
        now = time.time()
        def _check(d):
            if not d: return None
            # Skip expired
            if d.get("_expires_at") and d["_expires_at"] < now:
                return None
            # Skip soft-deleted
            if d.get("_deleted") and not (query and query.get("_deleted")):
                return None
            return d

        if '_id' in query and len(query) == 1:
            # Fix: Stale Read & Race Condition prevention
            # Check reload and use lock for direct access
            if self.db:
                # Need to check if file changed before accessing memory
                # This requires lock to prevent race during reload
                with self.db._thread_lock:
                    self.storage.check_reload()
                    return _check(self.storage.data['groups'][self.name].get(query['_id']))
            else:
                 return _check(self.storage.data['groups'][self.name].get(query['_id']))
        for field, val in query.items():
            if field in self.unique_indexes and len(query) == 1:
                # Fix: Stale Read prevention for unique index
                if self.db:
                    with self.db._thread_lock:
                        self.storage.check_reload()
                
                doc_id = self.unique_indexes[field].get(val)
                if doc_id:
                    return _check(self.storage.data['groups'][self.name].get(doc_id))
                return None
        results = self.find(query, limit=1)
        return results[0] if results else None

    def _rebuild_indexes(self):
        """Reconstruct all indexes for this group from storage metadata."""
        new_indexes = {}
        new_unique_indexes = {}
        
        if '_indexes' in self.storage.data and self.name in self.storage.data['_indexes']:
            defs = self.storage.data['_indexes'][self.name]
            for field_str, unique in defs.items():
                field = field_str
                if isinstance(field_str, str) and field_str.startswith('[') and field_str.endswith(']'):
                    try:
                        field_val = json.loads(field_str)
                        if isinstance(field_val, list):
                            field = tuple(field_val)
                    except Exception:
                        pass
                self.create_index(field, unique=unique, persist=False, 
                                 _target_indexes=new_indexes, 
                                 _target_unique_indexes=new_unique_indexes)
        
        # Atomic swap to prevent readers from seeing empty indexes during rebuild
        self.indexes = new_indexes
        self.unique_indexes = new_unique_indexes
        self._invalidate_cache()

    def create_index(self, field: Any, unique: bool=False, persist: bool=True, 
                     condition: Optional[dict]=None, _target_indexes: dict=None, 
                     _target_unique_indexes: dict=None):
        """
        Create an index on a specific field or a composite index on multiple fields.
        
        Args:
            unique: If True, enforces uniqueness on the field(s).
            persist: If True, saves index definition to storage.
            condition: Optional query dictionary. Only docs matching this match are indexed.
            _target_indexes: Internal use for atomic rebuild.
            _target_unique_indexes: Internal use for atomic rebuild.
        """
        target_idxs = _target_indexes if _target_indexes is not None else self.indexes
        target_uniques = _target_unique_indexes if _target_unique_indexes is not None else self.unique_indexes
        # Ensure field is hashable (str or tuple)
        if isinstance(field, list):
            field = tuple(field)

        if condition:
            if self.name not in self.storage.data.get('_partial_indexes', {}):
                 if '_partial_indexes' not in self.storage.data:
                     self.storage.data['_partial_indexes'] = {}
                 if self.name not in self.storage.data['_partial_indexes']:
                     self.storage.data['_partial_indexes'][self.name] = {}
            self.storage.data['_partial_indexes'][self.name][str(field)] = condition
            self.storage._dirty = True

        if unique:
            if field in target_uniques:
                return
            target_uniques[field] = {}
            for doc_id, doc in self.storage.data['groups'][self.name].items():
                val = self._get_field_val(field, doc)
                if val is not None:
                    if val in target_uniques[field]:
                        raise ValueError(f"Duplicate value '{val}' for unique index '{field}'")
                    target_uniques[field][val] = doc_id
        else:
            if field in target_idxs:
                return
            target_idxs[field] = {}
            for doc_id, doc in self.storage.data['groups'][self.name].items():
                val = self._get_field_val(field, doc)
                if val is not None:
                    if val not in target_idxs[field]:
                        target_idxs[field][val] = []
                    target_idxs[field][val].append(doc_id)

        if persist:
            if '_indexes' not in self.storage.data:
                self.storage.data['_indexes'] = {}
            if self.name not in self.storage.data['_indexes']:
                self.storage.data['_indexes'][self.name] = {}
            
            key = field
            if isinstance(field, tuple):
                try:
                    key = json.dumps(list(field))
                except (TypeError, ValueError):
                    key = str(field)
            
            self.storage.data['_indexes'][self.name][key] = unique
            self.storage._dirty = True

    def _get_field_val(self, field: Any, doc: Optional[dict]):
        """Internal: Extract value for a field/composite-field, respecting partial index conditions."""
        if not doc: return None
        # Check partial index condition
        condition = self.storage.data.get('_partial_indexes', {}).get(self.name, {}).get(str(field))
        if condition:
            if not self._matches_query(doc, condition):
                return None

        if isinstance(field, tuple):
            vals = []
            for sub_f in field:
                v = doc.get(sub_f)
                if v is None: return None
                vals.append(v)
            return tuple(vals)
        return doc.get(field)

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
                new_val = self._get_field_val(field, new_doc)
                old_val = self._get_field_val(field, old_doc)
                if new_val is not None and new_val != old_val:
                    if new_val in unique_map:
                        raise ValueError(f"Duplicate key '{field}': '{new_val}' exists.")
        
        # Update standard indexes
        for field, idx_map in self.indexes.items():
            new_val = self._get_field_val(field, new_doc)
            old_val = self._get_field_val(field, old_doc)
            
            if old_val is not None and old_val in idx_map:
                if doc_id in idx_map[old_val]:
                    idx_map[old_val].remove(doc_id)
                    if not idx_map[old_val]:
                        del idx_map[old_val]
            
            if new_val is not None:
                if new_val not in idx_map:
                    idx_map[new_val] = []
                if doc_id not in idx_map[new_val]:
                    idx_map[new_val].append(doc_id)

        # Update unique indexes
        for field, unique_map in self.unique_indexes.items():
            old_val = self._get_field_val(field, old_doc)
            new_val = self._get_field_val(field, new_doc)
            
            if old_val is not None and old_val in unique_map:
                if unique_map[old_val] == doc_id:
                    del unique_map[old_val]
            
            if new_val is not None:
                unique_map[new_val] = doc_id

    def _matches_query(self, doc: dict, query: dict) -> bool:
        """Internal: Check if a document matches a query (including operators)."""
        for k, v in query.items():
            if k == "$or" and isinstance(v, list):
                if not any(self._matches_query(doc, q) for q in v):
                    return False
                continue
            if k == "$and" and isinstance(v, list):
                if not all(self._matches_query(doc, q) for q in v):
                    return False
                continue
            if k == "$not" and isinstance(v, dict):
                if self._matches_query(doc, v):
                    return False
                continue

            doc_val = doc.get(k)
            if isinstance(v, dict):
                # Handle operators
                for op, val in v.items():
                    if op == "$regex":
                        if not isinstance(val, (str, bytes)) or doc_val is None:
                            return False
                        try:
                            if not re.search(str(val), str(doc_val)):
                                return False
                        except re.error:
                            return False
                    elif op == "$gt":
                        if doc_val is None or not (doc_val > val): return False
                    elif op == "$lt":
                        if doc_val is None or not (doc_val < val): return False
                    elif op == "$gte":
                        if doc_val is None or not (doc_val >= val): return False
                    elif op == "$lte":
                        if doc_val is None or not (doc_val <= val): return False
                    elif op == "$in":
                        if not isinstance(val, (list, tuple, set)) or doc_val not in val: return False
                    elif op == "$nin":
                        if not isinstance(val, (list, tuple, set)) or doc_val in val: return False
                    elif op == "$ne":
                        if doc_val == val: return False
                    elif op == "$exists":
                        exists = k in doc
                        if bool(val) != exists: return False
                    elif op == "$not" and isinstance(val, dict):
                        # Nested $not: {"field": {"$not": {"$gt": 5}}}
                        temp_query = {k: val}
                        if self._matches_query(doc, temp_query):
                            return False
            else:
                # Exact match
                if doc_val != v:
                    return False
        return True

    def _invalidate_cache(self):
        """Invalidate the query cache for this group."""
        if hasattr(self, "_query_cache"):
            self._query_cache.clear()

    def find(self, query: Optional[dict]=None, limit: int=0, skip: int=0) -> List[dict]:
        """
        Find documents matching a query (cached).
        """
        # Ensure data is fresh before checking cache
        self.storage.check_reload()
        if getattr(self.storage, '_defunct', False):
            raise OSError(f"Group '{self.name}' is defunct (deleted on disk).")
        
        # Create a stable string key for query
        query_json = json.dumps(query, sort_keys=True) if query else ""
        
        if not hasattr(self, "_query_cache"):
            self._query_cache = {}
        
        lock = self.db._thread_lock if self.db else contextlib.nullcontext()
        with lock:
            if query_json in self._query_cache:
                res = self._query_cache[query_json]
            else:
                res = list(self.find_iter(query))
                # Limit cache size to prevent OOM
                if len(self._query_cache) > 1000:
                    self._query_cache.clear()
                self._query_cache[query_json] = res
        
        # Filter expired documents from result (whether cached or fresh)
        # fresh find_iter already does it, but cached result might strictly contain expired ones now
        now = time.time()
        
        include_expired = query.get("_include_expired", False) if query else False
        
        if not include_expired:
            res = [d for d in res if not (d.get("_expires_at") and d["_expires_at"] < now)]

        if skip > 0:
            res = res[skip:]
        if limit > 0:
            return res[:limit]
        return res

    def resolve_ref(self, ref: dict) -> Optional[dict]:
        """
        Resolve a Data Reference (DBRef).
        Format: {"$ref": "group_name", "$id": "doc_id"}
        """
        if not isinstance(ref, dict) or "$ref" not in ref or "$id" not in ref:
            return None
        return self.db.group(ref["$ref"]).find_one({"_id": ref["$id"]})

    def soft_delete(self, query: dict) -> int:
        """Mark documents matching query as deleted (soft delete)."""
        return self.update(query, {"_deleted": True})

    def compact(self) -> int:
        """
        Permanently remove all soft-deleted documents from storage.
        This operation is irreversible.
        
        Returns:
            Number of documents permanently removed.
        """
        lock = self.db._thread_lock if self.db else contextlib.nullcontext()
        with lock:
            self.storage.check_reload()
            group_data = self.storage.data['groups'].get(self.name, {})
            to_remove = [doc_id for doc_id, doc in group_data.items() if doc.get("_deleted")]
            
            if not to_remove:
                return 0
                
            for doc_id in to_remove:
                del group_data[doc_id]
            
            self.storage._dirty = True
            self._rebuild_indexes()
            self._invalidate_cache()
            return len(to_remove)

    def undelete(self, query: dict) -> int:
        """Restore soft-deleted documents."""
        # Ensure we can find the deleted documents
        q = query.copy()
        if '_deleted' not in q:
            q['_deleted'] = True
        return self.update(q, {"_deleted": False})

    def bulk_insert(self, docs: List[dict]) -> List[dict]:
        """
        Efficiently insert multiple documents.
        Uses a transaction in single-file mode for atomicity.
        Falls back to iterative inserts in cluster mode.
        """
        if self.db.is_cluster:
            for d in docs:
                self.insert(d)
            return docs
        
        with self.db.begin() as txn:
            for d in docs:
                getattr(txn, self.name).insert(d)
        return docs

    def bulk_update(self, query: dict, update_data: dict) -> int:
        """
        Update multiple documents matching a query.
        Uses a transaction in single-file mode for atomicity.
        Falls back to iterative updates in cluster mode.
        """
        if self.db.is_cluster:
            return self.update(query, update_data) # update() already handles iteration
        
        with self.db.begin() as txn:
            return getattr(txn, self.name).update(query, update_data)

    def bulk_delete(self, query: dict) -> int:
        """
        Delete multiple documents matching a query.
        Uses a transaction in single-file mode for atomicity.
        Falls back to iterative deletions in cluster mode.
        """
        if self.db.is_cluster:
            return self.delete(query) # delete() already handles iteration
            
        with self.db.begin() as txn:
            return getattr(txn, self.name).delete(query)

    def find_iter(self, query: Optional[dict]=None):
        """
        Iterate over documents matching a query.
        
        Uses indexes for performance if available.
        Thread-safe: Snapshots results while holding lock.
        
        Args:
            query: Dictionary of criteria (field=value).
            
        Yields:
            Matching documents.
        """
        if self.name not in self.storage.data['groups']:
            return iter([])
        
        # Fix: Race Condition - Check reload MUST be inside lock
        # self.storage.check_reload() <- Moved inside lock below

        lock = self.db._thread_lock if self.db else contextlib.nullcontext()
        results = []
        
        with lock:
            # Fix: Race Condition prevention
            self.storage.check_reload()
            if getattr(self.storage, '_defunct', False):
                raise OSError(f"Group '{self.name}' is defunct (deleted on disk).")

            gdata = self.storage.data['groups'][self.name]
            now = time.time()
            
            # Support _include_expired logic
            # Use copy to avoid side effect on user's query object
            q = query.copy() if query else {}
            include_expired = q.pop("_include_expired", False)

            if not q:
                # Filter expired and soft-deleted
                all_docs = [
                    d for d in gdata.values() 
                    if (include_expired or not (d.get("_expires_at") and d["_expires_at"] < now))
                    and not d.get("_deleted")
                ]
                yield from all_docs
                return
            
            query = q # Use the cleaned query copy for further processing
            
            if "$or" in query or "$and" in query:
                # Top-level logic: skip indexing optimization for now for simplicity/safety
                for doc in gdata.values():
                    if not include_expired and doc.get("_expires_at") and doc["_expires_at"] < now:
                        continue
                    if doc.get("_deleted") and not query.get("_deleted"):
                        continue
                    if self._matches_query(doc, query):
                        results.append(doc)
                yield from results
                return

            query = query or {}
            
            # 1. Try exact match on Composite/Unique Indexes first (Most Efficient)
            # Check if query fields match any composite index
            # Strategy: Iterate over indexes, check if query contains all fields of the index key
            
            best_candidate_set = None
            
            # Helper to extract value tuple from query
            def get_query_val(idx_key):
                if isinstance(idx_key, tuple):
                    vals = []
                    for k in idx_key:
                        if k not in query: return None
                        vals.append(query[k])
                    return tuple(vals)
                return query.get(idx_key)

            # Check Unique Indexes
            unique_match_found = False
            for idx_key, umap in self.unique_indexes.items():
                val = get_query_val(idx_key)
                if val is not None:
                    if val in umap:
                        doc_id = umap[val]
                        if doc_id in gdata:
                            doc = gdata[doc_id]
                            # Skip expired docs
                            if not include_expired and doc.get("_expires_at") and doc["_expires_at"] < now:
                                unique_match_found = True # Match technically found but ignored
                                break
                            # Skip soft-deleted docs unless explicitly asked
                            if doc.get("_deleted") and not (query and query.get("_deleted")):
                                unique_match_found = True # Match technically found but ignored
                                break
                            # Verify all fields (including complex operators) match
                            if self._matches_query(doc, query):
                                results.append(doc)
                            unique_match_found = True
                            break # Unique match found (or mismatch confirmed), we are done
                        else:
                            unique_match_found = True
                            break # Index points to missing doc
                    else:
                        unique_match_found = True
                        break # Unique key not found, result is empty
            
            if unique_match_found:
                pass # Already handled above
            else:
                # Check Standard Indexes (including Composite)
                idx_matches = []
                for idx_key, idx_map in self.indexes.items():
                    val = get_query_val(idx_key)
                    if val is not None:
                        # Query covers this index
                        if val in idx_map:
                            idx_matches.append(set(idx_map[val]))
                        else:
                            idx_matches = None # Intersection with empty set is empty
                            results = [] # clear results
                            break
                
                if idx_matches is not None:
                    if idx_matches:
                        best_candidate_set = set.intersection(*idx_matches)
                    
                    if best_candidate_set is not None:
                        for doc_id in best_candidate_set:
                            if doc_id in gdata:
                                doc = gdata[doc_id]
                                # Skip expired docs
                                if not include_expired and doc.get("_expires_at") and doc["_expires_at"] < now:
                                    continue
                                # Skip soft-deleted docs unless explicitly asked
                                if doc.get("_deleted") and not (query and query.get("_deleted")):
                                    continue
                                if self._matches_query(doc, query):
                                    results.append(doc)
                    else:
                        # Full scan fallback
                        for doc in gdata.values():
                            # Skip expired docs
                            if not include_expired and doc.get("_expires_at") and doc["_expires_at"] < now:
                                continue
                            # Skip soft-deleted docs unless explicitly asked
                            if doc.get("_deleted") and not (query and query.get("_deleted")):
                                continue
                            if self._matches_query(doc, query):
                                results.append(doc)
                else:
                     pass # idx_matches was explicitly set to None (empty result)

        yield from results

    def get_all(self) -> List[dict]:
        """
        Retrieve all documents in the group.
        
        Returns:
            List of all documents.
        """
        return self.find({})

    def get_all_iter(self):
        """
        Iterate over all documents in the group.
        
        Yields:
            Documents.
        """
        return self.find_iter({})

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

        if self._computed_fields:
            for field, func in self._computed_fields.items():
                data[field] = func(data)

        self._update_index(doc_id, None, data)
        group_data[doc_id] = data
        self.storage._dirty = True
        self._invalidate_cache()

    def set_computed_field(self, name: str, func: callable):
        """Register a computed field function."""
        self._computed_fields[name] = func
        self._invalidate_cache()

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
        
        # Apply default values from schema if defined
        if self.schema and hasattr(self.schema, '__fields__'):
            for field_name, field_info in self.schema.__fields__.items():
                if field_name not in data and hasattr(field_info, 'default'):
                    default_val = field_info.default
                    if default_val is not None:
                        data[field_name] = default_val
                    elif hasattr(field_info, 'default_factory') and field_info.default_factory:
                        data[field_name] = field_info.default_factory()
        
        # TTL Support
        if "ttl" in data:
            data["_expires_at"] = time.time() + data.pop("ttl")
        
        lock = self.db._thread_lock if self.db else contextlib.nullcontext()
        with lock:
            self.storage.check_reload()
            if getattr(self.storage, '_defunct', False):
                 raise OSError(f"Group '{self.name}' is defunct (deleted on disk).")
            self._invalidate_cache()
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
                # Execute pre-insert hooks
                self._execute_hook('pre_insert', data)
                
                self._insert_mem(data)
                self.storage.append_log('insert', self.name, str(data['_id']), data, txn_id=txn_id)
                if is_implicit:
                    self.storage.commit_txn(txn_id)
                self._invalidate_cache()
                # Execute post-insert hooks
                self._execute_hook('post_insert', data)
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
        self._invalidate_cache()

    def register_hook(self, hook_type: str, callback):
        """
        Register lifecycle callback for document events.
        
        Hooks allow users to inject custom logic into CRUD operations without
        modifying core database code. Each hook type corresponds to a specific
        lifecycle point (before/after insert, update, delete operations).
        
        The callback signature varies by hook type:
        - pre_insert/post_insert: callback(doc)
        - pre_delete/post_delete: callback(doc) 
        - pre_update/post_update: callback(old_doc, new_doc)
        
        We maintain a registry (_hooks dict) mapping hook type -> list of
        callbacks. This allows multiple handlers per event (chain pattern).

        Args:
            hook_type: str - Hook type identifier (pre_insert, post_insert, etc)
            callback: func - Callable to execute when hook is triggered
            
        Raises:
            ValueError: If hook_type is not registered in the _hooks dict
        """
        if hook_type not in self._hooks:
            raise ValueError(f"Unknown hook type: {hook_type}. Valid types: {list(self._hooks.keys())}")
        self._hooks[hook_type].append(callback)

    def unregister_hook(self, hook_type: str, callback):
        """
        Remove registered lifecycle callback.
        
        Safely removes a callback from the hook registry. We search the hook list
        and remove only the first matching callback reference. Silent on not-found
        to allow cleanup code to be idempotent (remove callback multiple times).
        
        This is critical for long-running applications where hooks may be
        registered during object initialization but need cleanup during shutdown.

        Args:
            hook_type: str - Hook type identifier
            callback: func - Callback reference to remove
        """
        if hook_type in self._hooks:
            try:
                self._hooks[hook_type].remove(callback)
            except ValueError:
                pass  # Callback was not registered, safe to ignore

    def _execute_hook(self, hook_type: str, *args):
        """
        Internal: Trigger all callbacks for a hook type.
        
        Executes each callback in the hook registry sequentially. Individual
        callback failures are caught and logged but do NOT propagate - this
        ensures one failing hook doesn't block the database operation or
        subsequent hooks.
        
        We use warnings.warn() rather than raising to give admins visibility
        into hook failures without breaking CRUD operations. Failed hooks
        should be logged/monitored separately from core DB errors.

        Args:
            hook_type: str - Hook type identifier
            *args: Variable arguments passed to each callback
            
        Note:
            Hook execution failures are logged as warnings, not raised.
            This maintains database consistency even with buggy user code.
        """
        if hook_type not in self._hooks:
            return
        for callback in self._hooks[hook_type]:
            try:
                callback(*args)
            except Exception as e:
                warnings.warn(f"Hook {hook_type} failed: {e}")


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
        lock = self.db._thread_lock if self.db else contextlib.nullcontext()
        with lock:
            self.storage.check_reload()
            if getattr(self.storage, '_defunct', False):
                raise OSError(f"Group '{self.name}' is defunct (deleted on disk).")
            self._invalidate_cache()
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
                self._invalidate_cache()
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
        self._invalidate_cache()

    def delete(self, query: dict, external_txn_id: Optional[str]=None) -> int:
        """
        Delete documents matching a query.
        
        Args:
            query: Criteria to match documents.
            external_txn_id: Optional active transaction ID.
            
        Returns:
            Number of documents deleted.
        """
        lock = self.db._thread_lock if self.db else contextlib.nullcontext()
        with lock:
            self.storage.check_reload()
            if getattr(self.storage, '_defunct', False):
                raise OSError(f"Group '{self.name}' is defunct (deleted on disk).")
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
                self._invalidate_cache()
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
        """Get a value by key (respects reloads)."""
        self.storage.check_reload()
        return self.storage.data.get('kv', {}).get(str(key), default)

    def explain(self, query: dict) -> dict:
        """
        explain() - Analyze and explain query execution plan.
        
        Provides detailed breakdown of how a query will be executed:

        Execution Strategies:
        - full_scan:    No indexes applicable, scan all documents
        - unique_index: Document found via unique key lookup (1 result max)
        - index_scan:   Standard index lookup used to filter documents
        
        Returned plan dict contains:
        - query:                 Input query dict
        - execution_strategy:    One of [full_scan, unique_index, index_scan]
        - index_usage:           List of indexes used (field, type, estimates)
        - estimated_docs_scanned: Total docs to scan with current strategy
        - has_indexes:           Boolean, true if any indexes exist on group
        - explain_time_ms:       Execution time of explain() itself (planning cost)

        Args:
            query: dict - Query criteria to analyze

        Returns:
            dict - Execution plan with detailed metrics
            
        Example:
            plan = group.explain({'name': 'John'})
            if plan['execution_strategy'] == 'unique_index':
                print(f"Fast: O(1) lookup, returning ~1 doc")
            elif plan['execution_strategy'] == 'full_scan':
                print(f"Slow: Full scan of {plan['estimated_docs_scanned']} docs")
        """
        start_time = time.time()
        
        plan = {
            'query': query,
            'index_usage': [],
            'execution_strategy': 'full_scan',
            'estimated_docs_scanned': len(self.storage.data['groups'][self.name]),
            'has_indexes': len(self.indexes) > 0 or len(self.unique_indexes) > 0
        }
        
        def get_query_val(idx_key):
            """Extract query value(s) for a given index key(s)."""
            if isinstance(idx_key, tuple):
                vals = []
                for k in idx_key:
                    if k not in query: return None
                    vals.append(query[k])
                return tuple(vals)
            return query.get(idx_key)
        
        # Check unique indexes first (best case: O(1) lookup)
        for idx_key, umap in self.unique_indexes.items():
            val = get_query_val(idx_key)
            if val is not None:
                plan['execution_strategy'] = 'unique_index'
                plan['index_usage'].append({'type': 'unique', 'field': idx_key, 'estimated_docs_returned': 1})
                break
        
        # Check standard indexes if no unique index matched
        if plan['execution_strategy'] == 'full_scan':
            for idx_key, idx_map in self.indexes.items():
                val = get_query_val(idx_key)
                if val is not None:
                    plan['execution_strategy'] = 'index_scan'
                    est_docs = len(idx_map.get(val, []))
                    plan['index_usage'].append({'type': 'standard', 'field': idx_key, 'estimated_docs_returned': est_docs})
        
        elapsed = (time.time() - start_time) * 1000
        plan['explain_time_ms'] = elapsed
        
        return plan

    def profile(self, operation: str = 'find', query: dict = None) -> dict:
        """
        Profile database operation performance metrics.
        
        Database profiling is essential for identifying performance bottlenecks
        in production workloads. This method measures four dimensions:

        1. Wall-clock time: How long the operation took in milliseconds.
           Includes query planning, index lookups, document filtering, etc.
        
        2. Memory delta: Change in memory occupied by the group storage.
           Positive delta indicates new documents/data, negative indicates
           deletions. Helps detect memory bloat or leak patterns.
        
        3. Operation result: Count of documents affected (find->found,
           insert->status, update->updated, delete->deleted).
        
        4. Success status: Boolean indicating whether the operation completed
           successfully. Failures include the exception message for debugging.
        
        Supported operations:
        - find: Query documents, returns count of matches found
        - insert: Insert single document (query param is the document)
        - update: Bulk update (query is {'query': {...}, 'data': {...}})
        - delete: Bulk delete matching documents (query is delete criteria)

        Args:
            operation: str - Operation type: find, insert, update, delete
            query: dict - Operation-specific parameters (default: {})

        Returns:
            dict with keys:
            - operation: The operation type executed
            - start_time: Unix timestamp when profiling started
            - query: The input parameters
            - docs_found/updated/deleted: Count (operation-dependent)
            - execution_time_ms: Wall-clock duration in milliseconds
            - memory_delta_bytes: Memory change during operation
            - success: Boolean, true if operation completed
            - error: Exception message if success=false
        """
        import sys
        
        query = query or {}
        result = {
            'operation': operation,
            'start_time': time.time(),
            'query': query,
        }
        
        try:
            # Measure baseline memory before operation. sys.getsizeof() gives
            # the size of the group storage structure. We use this as a proxy
            # for total memory usage because deep measurement is expensive.
            start_mem = sys.getsizeof(self.storage.data['groups'][self.name])
            op_start = time.time()
            
            # Execute the requested operation. Each branch updates the result
            # dict with operation-specific metrics (docs affected count).
            if operation == 'find':
                res = list(self.find_iter(query))
                result['docs_found'] = len(res)
            elif operation == 'insert':
                self.insert(query)
                result['status'] = 'inserted'
            elif operation == 'update':
                upd_query = query.get('query', {})
                upd_data = query.get('data', {})
                count = self.update(upd_query, upd_data)
                result['docs_updated'] = count
            elif operation == 'delete':
                count = self.delete(query)
                result['docs_deleted'] = count
            else:
                raise ValueError(f"Unknown operation: {operation}")
            
            # Measure post-operation state. Calculate elapsed time in
            # milliseconds and memory delta.
            op_time = (time.time() - op_start) * 1000
            end_mem = sys.getsizeof(self.storage.data['groups'][self.name])
            
            result['execution_time_ms'] = op_time
            result['memory_delta_bytes'] = end_mem - start_mem
            result['success'] = True
            
        except Exception as e:
            result['error'] = str(e)
            result['success'] = False
        
        return result


class HVPDB:
    """
    Main database engine for HVPDB.
    
    Handles both single-file and cluster-mode database operations,
    transaction management, and plugin lifecycle.
    """

    _CLUSTER_META_FILENAME = '__hvpdb_meta__.hvp'
    _CLUSTER_META_GROUP_NAME = '__hvpdb_meta__'

    def __init__(self, path: str, password: Optional[str]=None, durable: bool=True, concurrent: bool=False, wal_checksum_type: int=0):
        """
        Initialize the database engine.
        
        Args:
            path: File path or hvp:// URI.
            password: Authentication password.
            durable: Whether to use durable storage (WAL).
            concurrent: Whether to enable high-concurrency mode (requires plugin).
            wal_checksum_type: 0 for CRC32, 1 for SHA-256.
        """
        self.is_cluster = False
        self._ttl_thread = None
        self._stop_ttl = threading.Event()
        self.durable = durable
        self.concurrent = concurrent
        self.wal_checksum_type = wal_checksum_type
        
        self._load_storage(path, password)
        
        if durable and not self.is_cluster:
            self._start_ttl_reaper()

    def _start_ttl_reaper(self):
        """Start a background thread to purge expired documents."""
        def reaper():
            while not self._stop_ttl.wait(60): # Run every 60 seconds
                try:
                    now = time.time()
                    for group_name in self.get_all_groups():
                        group = self.group(group_name)
                        while True:
                            # Process in batches of 500 to prevent memory pressure
                            query = {"_expires_at": {"$lt": now}, "_include_expired": True}
                            expired_docs = group.find(query, limit=500)
                            if not expired_docs:
                                break
                            
                            ids_to_del = [d["_id"] for d in expired_docs]
                            group.delete({"_id": {"$in": ids_to_del}, "_include_expired": True})
                            
                            if len(ids_to_del) < 500:
                                break # Done with this group
                except Exception as e:
                    warnings.warn(f"TTL Reaper Error: {e}")
        
        self._ttl_thread = threading.Thread(target=reaper, daemon=True)
        self._ttl_thread.start()

    def _load_storage(self, path: str, password: Optional[str]=None):
        """
        Internal method to load storage based on path and password.
        This logic was moved from __init__ to allow for re-initialization
        or separate loading concerns.
        
        Args:
            path: File path or hvp:// URI.
            password: Authentication password.
        """
        raw = path
        # self.is_cluster is already initialized in __init__
        
        if path.startswith('hvp://') or path.startswith('hvpdb://'):
            from .uri import HVPURI
            self.uri = HVPURI.parse(path)
            raw = self.uri.cluster or '.' 
            password = password or self.uri.password
            self.is_cluster = self.uri.options.get('cluster', 'false').lower() == 'true'
            self.database = self.uri.database
        else:
            self.uri = None
            self.database = None
        
        base = os.path.basename(raw)
        if base.endswith('.hvp'):
            name = base[:-4]
        elif base.endswith('.hvdb'):
            name = base[:-5]
            self.is_cluster = True
        else:
            name = base
            
        # Determine filepath
        if self.uri and self.uri.cluster == 'local':
            # Local URI: use the database part as the path
            self.filepath = self.database
            if self.is_cluster and not (self.filepath.endswith('.hvdb') or os.path.isdir(self.filepath)):
                 # If cluster but points to a name, we might want to keep it as is
                 pass
            tdir = os.path.dirname(self.filepath)
            if tdir and (not os.path.exists(tdir)):
                os.makedirs(tdir, exist_ok=True)
        elif '://' in raw and 'local' not in raw and 'host=local' not in raw.lower():
            # Remote/Protocol-based URI (future use)
            self.filepath = raw
        elif os.path.isabs(raw) or os.path.dirname(raw):
            # Normalize path to prevent traversal/obfuscation
            self.filepath = os.path.normpath(raw)
            tdir = os.path.dirname(self.filepath)
            if tdir and (not os.path.exists(tdir)):
                os.makedirs(tdir, exist_ok=True)
        else:
            # Relative path logic for simple names (e.g. HVPDB("mydb"))
            bdir = 'hvp'
            db_name = name.replace(':', '_').replace('/', '_').replace('\\', '_')
            tdir = os.path.join(bdir, db_name)
            self.filepath = os.path.join(tdir, 'cluster') if self.is_cluster else os.path.join(tdir, f'{db_name}.hvp')
            
            if not os.path.exists(tdir):
                os.makedirs(tdir, exist_ok=True)

        if self.is_cluster and '://' not in self.filepath:
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


        # self.durable is already initialized in __init__
        self._thread_lock = threading.RLock()
        self._user_ctx: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(f'user_{uuid.uuid4()}', default=None)
        self._txn_ctx: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(f'txn_{uuid.uuid4()}', default=None)
        self._groups = {}
        if self.is_cluster:
            meta_path = os.path.join(self.filepath, self._CLUSTER_META_FILENAME)
            self.storage = HVPStorage(meta_path, self.password, durable=self.durable, wal_checksum_type=self.wal_checksum_type)
        else:
            self.storage = HVPStorage(self.filepath, self.password, durable=self.durable, wal_checksum_type=self.wal_checksum_type)
        
        self.storage.register_reload_callback(self._handle_storage_reload)
        
        self.storage.load()
        if 'users' not in self.storage.data:
            self.storage.data['users'] = {}
            self._create_root_user()
        for grp in self.storage.data.get('groups', {}):
            if grp not in self._groups:
                self.group(grp)
        self.plugins = {}
        self.load_plugins()

    def _handle_storage_reload(self):
        """Callback triggered when storage reloads data from disk."""
        with self._thread_lock:
            for group in self._groups.values():
                group._rebuild_indexes()

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

    def transaction(self) -> HVPTransaction:
        """
        Start a new transaction context.
        
        Returns:
            An HVPTransaction instance.
        """
        return HVPTransaction(self)

    def close(self):
        """Close the database and all its groups."""
        with self._thread_lock:
            if hasattr(self, 'storage') and self.storage:
                self.storage.wal.close()
            for group in self._groups.values():
                if hasattr(group, 'storage') and group.storage:
                    group.storage.wal.close()
            # Stop TTL thread
            self._stop_ttl.set()

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

    def _get_group_path(self, name: str) -> str:
        """Internal: Resolve the physical file path for a group, considering sharding."""
        if not self.is_cluster:
            return self.filepath
            
        shards = []
        if self.uri:
            shards = list(self.uri.shards) if self.uri.shards else []
            if not shards and 'shards' in self.uri.options:
                shards = self.uri.options['shards'].split(',')
            
        if shards:
            # Deterministic hashing for sharding
            shard_idx = zlib.crc32(name.encode()) % len(shards)
            shard_host = shards[shard_idx]
            shard_dir_name = f"shard_{shard_idx}_{shard_host.replace(':', '_')}"
            shard_dir = os.path.join(self.filepath, shard_dir_name)
            # Ensure shard directory exists
            if not os.path.exists(shard_dir):
                try:
                    os.makedirs(shard_dir, exist_ok=True)
                except OSError:
                    pass
            return os.path.join(shard_dir, f"{name}.hvp")
        else:
            return os.path.join(self.filepath, f"{name}.hvp")

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
            # Lazy Loading in cluster mode
            # Initialize storage for this specific group file
            group_path = self._get_group_path(name)
            group_storage = HVPStorage(group_path, self.password, durable=self.durable, wal_checksum_type=self.wal_checksum_type)
            group_storage.load()
            group = HVPGroup(group_storage, name, db_instance=self, schema=schema)
            # Register reload callback so index is rebuilt when storage reloads from disk
            group_storage.register_reload_callback(group._rebuild_indexes)
        else:
            # Single file mode: all groups share the main storage
            group = HVPGroup(self.storage, name, db_instance=self, schema=schema)
            
        self._groups[name] = group
        return group

    def drop_group(self, name: str):
        """
        Permanently delete a group and all its data.
        
        In Cluster Mode, this deletes the underlying file (O(1) operation),
        similar to dropping a partition in other databases.
        
        Args:
            name: Group name to drop.
        """
        if name not in self._groups and (not self.is_cluster or name not in self.get_all_groups()):
            # If not loaded and not in list, assume it doesn't exist
            return

        if self.is_cluster:
            # Ensure group is loaded so we can close it properly
            if name in self._groups:
                grp = self._groups[name]
                # Close storage handles (WAL, etc.)
                if hasattr(grp.storage, 'wal') and grp.storage.wal:
                    grp.storage.wal.close()
                if grp.storage.security:
                    grp.storage.security.clear_key()
                del self._groups[name]

            # Delete files
            base_path = self._get_group_path(name)
            if base_path.endswith('.hvp'):
                base_path = base_path[:-4] # Get base without .hvp
                
            for ext in ['.hvp', '.hvp.log', '.hvp.writelock', '.hvp.lock']:
                p = base_path + ext
                if os.path.exists(p):
                    try:
                        os.remove(p)
                    except OSError as e:
                        warnings.warn(f"Failed to delete {p}: {e}")
        else:
            # Single file mode: delete from memory dict
            if name in self.storage.data['groups']:
                del self.storage.data['groups'][name]
                # Also clean up indexes
                if '_indexes' in self.storage.data and name in self.storage.data['_indexes']:
                    del self.storage.data['_indexes'][name]
                
                self.storage._dirty = True
                self.storage.append_log('drop_group', name, '', {})
            
            if name in self._groups:
                del self._groups[name]

    def get_all_groups(self) -> List[str]:
        """
        List all available groups in the database.
        
        Returns:
            Sorted list of group names.
        """
        if self.is_cluster:
            gs = set()
            if os.path.exists(self.filepath):
                # Scan main directory
                for f in os.listdir(self.filepath):
                    if f.endswith('.hvp') or f.endswith('.hvp.log'):
                        name = f[:-4] if f.endswith('.hvp') else f[:-8]
                        if name != self._CLUSTER_META_FILENAME[:-4]:
                            gs.add(name)
                    # Scan shard subdirectories
                    elif f.startswith('shard_') and os.path.isdir(os.path.join(self.filepath, f)):
                        shard_dir = os.path.join(self.filepath, f)
                        for sf in os.listdir(shard_dir):
                            if sf.endswith('.hvp') or sf.endswith('.hvp.log'):
                                name = sf[:-4] if sf.endswith('.hvp') else sf[:-8]
                                gs.add(name)
            return sorted(list(gs))
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

    def backup(self, path: str):
        """
        Create a point-in-time backup of the database.
        In Cluster Mode, this creates a copy of the entire database directory.
        """
        if self.is_cluster:
            import shutil
            # Ensure all in-memory changes are flushed across all groups
            self.commit()
            
            # Remove destination if it exists (copytree requires it doesn't exist or we handle it)
            if os.path.exists(path):
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
            
            shutil.copytree(self.filepath, path)
        else:
            self.storage.create_snapshot(path)

    def compact(self) -> int:
        """
        Perform compaction across all groups in the database.
        Permanently removes soft-deleted documents.
        """
        total = 0
        if self.is_cluster:
            # Compact all loaded groups
            for name in list(self._groups.keys()):
                total += self.group(name).compact()
            
            # Also scan for unloaded group files in the cluster directory
            for entry in os.scandir(self.filepath):
                if entry.is_file() and entry.name.endswith('.hvp'):
                    g_name = entry.name[:-4]
                    if g_name not in self._groups:
                        # Load, compact, and save
                        try:
                            g = self.group(g_name)
                            total += g.compact()
                            g.storage.save()
                        except Exception as e:
                            warnings.warn(f"Failed to compact unloaded group '{g_name}': {e}")
        else:
            # Single file mode: All groups are in self.storage
            # We need to iterate over all groups in storage
            for g_name in list(self.storage.data.get('groups', {}).keys()):
                total += self.group(g_name).compact()
            
        if total > 0:
            self.commit()
        return total

    def repair(self) -> bool:
        """
        Attempt to repair the database storage files.
        """
        return self.storage.repair()

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
        if self._ttl_thread:
            self._stop_ttl.set()
            self._ttl_thread.join(timeout=5) # Give it a moment to stop
            if self._ttl_thread.is_alive():
                warnings.warn("TTL reaper thread did not terminate gracefully.")

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
        Update the database encryption password with atomic rollback support.
        """
        old_password = self.password
        success_stack = [] # (storage, old_pwd, old_atype)
        
        def _get_atype(s):
            return s.security.get_kdf_params().get('auth_type', 'password') if s.security else 'password'

        def _reencrypt(s, pwd, atype):
            # Force checkpoint to ensure WAL is clean before key change
            s.checkpoint()
            
            # Capture state for local rollback if save fails
            old_s_pwd = s.password
            old_s_sec = s.security
            
            s.password = pwd
            s.security = None 
            
            old_params = old_s_sec.get_kdf_params() if old_s_sec else {}
            new_params = old_params.copy()
            if atype:
                new_params['auth_type'] = atype
            elif 'auth_type' not in new_params:
                new_params['auth_type'] = 'password'
                
            from .security import HVPSecurity
            s.security = HVPSecurity(pwd, kdf_params=new_params)
            if hasattr(s, 'wal') and s.wal:
                s.wal.security = s.security
            
            try:
                s._dirty = True
                s.save()
            except Exception:
                # Local rollback: restore memory state to match disk state
                s.password = old_s_pwd
                s.security = old_s_sec
                if hasattr(s, 'wal') and s.wal:
                    s.wal.security = s.security
                raise

        try:
            # 1. Main storage
            old_atype = _get_atype(self.storage)
            _reencrypt(self.storage, new_password, auth_type)
            success_stack.append((self.storage, old_password, old_atype))
            
            # 2. Group storages
            if self.is_cluster:
                paths_to_check = []
                for item in os.listdir(self.filepath):
                    p = os.path.join(self.filepath, item)
                    if item.endswith('.hvp') and item != self._CLUSTER_META_FILENAME:
                        paths_to_check.append((item[:-4], p))
                    elif item.startswith('shard_') and os.path.isdir(p):
                        for sitem in os.listdir(p):
                            if sitem.endswith('.hvp'):
                                paths_to_check.append((sitem[:-4], os.path.join(p, sitem)))
                
                for group_name, full_path in paths_to_check:
                    target_s = None
                    if group_name in self._groups:
                        target_s = self._groups[group_name].storage
                    else:
                        target_s = HVPStorage(full_path, old_password, durable=self.durable)
                        target_s.load()
                    
                    g_old_atype = _get_atype(target_s)
                    _reencrypt(target_s, new_password, auth_type)
                    success_stack.append((target_s, old_password, g_old_atype))
            
            # Success: update master password
            self.password = new_password
            
        except Exception as e:
            # Failure: Rollback
            errors = [str(e)]
            while success_stack:
                s, old_pwd, old_atype = success_stack.pop()
                try:
                    _reencrypt(s, old_pwd, old_atype)
                except Exception as re:
                    errors.append(f"Rollback failed for {s.filepath}: {re}")
            
            raise RuntimeError(f"Password rotation failed. Rollback attempted. Errors: {'; '.join(errors)}")
        
        # Old password reference will be cleared when the method exits

    def set(self, key: str, value: Any):
        """
        Set a key-value pair in the global store.
        
        Args:
            key: Key name.
            value: Value to store.
        """
        key = str(key)
        if self.is_cluster:
            # Only store KV in the meta storage to ensure a single source of truth
            self.storage.data.setdefault('kv', {})[key] = value
            self.storage._dirty = True
            self.storage.append_log('set', 'kv', key, {'value': value})
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
        self.storage.check_reload()
        return self.storage.data.get('kv', {}).get(str(key), default)
