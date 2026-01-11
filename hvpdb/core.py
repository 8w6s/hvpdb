from .storage import HVPStorage
import uuid
import time
import os
import contextvars
from typing import Dict, Any, List, Optional, Union
import hashlib
import secrets
import difflib

class HVPGroup:

    def __init__(self, storage: HVPStorage, name: str, db_instance=None):
        self.storage = storage
        self.name = name
        self.db = db_instance
        self.indexes = {}
        self.unique_indexes = {}
        if name not in self.storage.data['groups']:
            self.storage.data['groups'][name] = {}
        if '_indexes' not in self.storage.data:
            self.storage.data['_indexes'] = {}
        self._rebuild_indexes()

    def find_one(self, query: Dict[str, Any]) -> Optional[Dict[str, Any]]:
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
        if new_doc:
            for field, unique_map in self.unique_indexes.items():
                new_val = new_doc.get(field)
                old_val = old_doc.get(field) if old_doc else None
                if new_val is not None and new_val != old_val:
                    if new_val in unique_map:
                        raise ValueError(f"Duplicate key '{field}': '{new_val}' exists.")
        for field, idx_map in self.indexes.items():
            old_val = old_doc.get(field) if old_doc else None
            if old_val is not None and old_val in idx_map:
                if doc_id in idx_map[old_val]:
                    idx_map[old_val].remove(doc_id)
                    if not idx_map[old_val]:
                        del idx_map[old_val]
            new_val = new_doc.get(field) if new_doc else None
            if new_val is not None:
                if new_val not in idx_map:
                    idx_map[new_val] = []
                idx_map[new_val].append(doc_id)
        for field, unique_map in self.unique_indexes.items():
            old_val = old_doc.get(field) if old_doc else None
            if old_val is not None and old_val in unique_map:
                if unique_map[old_val] == doc_id:
                    del unique_map[old_val]
            new_val = new_doc.get(field) if new_doc else None
            if new_val is not None:
                unique_map[new_val] = doc_id

    def find(self, query: dict=None, limit: int=0) -> List[dict]:
        res = list(self.find_iter(query))
        if limit > 0:
            return res[:limit]
        return res

    def find_iter(self, query: dict=None):
        if self.name not in self.storage.data['groups']:
            return iter([])
        gdata = self.storage.data['groups'][self.name]
        if not query:
            yield from gdata.values()
            return
        for key, value in query.items():
            if key in self.unique_indexes:
                umap = self.unique_indexes[key]
                if value in umap:
                    doc_id = umap[value]
                    if doc_id in gdata:
                        doc = gdata[doc_id]
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
        idx_matches = []
        for key, value in query.items():
            if key in self.indexes:
                if value in self.indexes[key]:
                    idx_matches.append(set(self.indexes[key][value]))
                else:
                    return
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
            for doc in gdata.values():
                match = True
                for k, v in query.items():
                    if doc.get(k) != v:
                        match = False
                        break
                if match:
                    yield doc

    def get_all(self):
        return list(self.storage.data['groups'][self.name].values())

    def get_all_iter(self):
        return self.storage.data['groups'][self.name].values()

    def _insert_mem(self, data: dict):
        self._update_index(data['_id'], None, data)
        self.storage.data['groups'][self.name][data['_id']] = data
        self.storage._dirty = True

    def insert(self, data: dict) -> dict:
        if '_id' not in data:
            data['_id'] = str(uuid.uuid4())
        data['_created_at'] = time.time()
        txn_id = None
        is_implicit = True
        if self.db and self.db.current_txn:
            txn_id = self.db.current_txn
            is_implicit = False
        else:
            txn_id = self.storage.begin_txn()
        try:
            self._insert_mem(data)
            self.storage.append_log('insert', self.name, data['_id'], data, txn_id=txn_id)
            if is_implicit:
                self.storage.commit_txn(txn_id)
            return data
        except Exception:
            if data['_id'] in self.storage.data['groups'][self.name]:
                self._delete_mem(data['_id'], data)
            if is_implicit:
                self.storage.rollback_txn(txn_id)
            raise

    def _update_mem(self, doc_id: str, update_data: dict, old_doc: dict):
        new_state = old_doc.copy()
        new_state.update(update_data)
        self._update_index(doc_id, old_doc, new_state)
        doc = self.storage.data['groups'][self.name][doc_id]
        doc.update(update_data)
        doc['_updated_at'] = time.time()
        self.storage._dirty = True
        return doc

    def _restore_mem(self, doc_id: str, old_doc: dict):
        cur = self.storage.data['groups'][self.name].get(doc_id)
        self._update_index(doc_id, cur, old_doc)
        self.storage.data['groups'][self.name][doc_id] = old_doc
        self.storage._dirty = True

    def update(self, query: dict, update_data: dict) -> int:
        docs = self.find(query)
        if not docs:
            return 0
        cnt = 0
        txn_id = self.storage.begin_txn()
        mod_log = []
        try:
            for doc in docs:
                old_doc = doc.copy()
                updated_doc = self._update_mem(doc['_id'], update_data, old_doc)
                mod_log.append((doc['_id'], old_doc))
                self.storage.append_log('update', self.name, doc['_id'], updated_doc, txn_id=txn_id, before_image=old_doc)
                cnt += 1
            self.storage.commit_txn(txn_id)
            return cnt
        except Exception:
            for doc_id, old_doc in reversed(mod_log):
                self._restore_mem(doc_id, old_doc)
            self.storage.rollback_txn(txn_id)
            raise

    def _delete_mem(self, doc_id: str, doc: dict):
        self._update_index(doc_id, doc, None)
        del self.storage.data['groups'][self.name][doc_id]
        self.storage._dirty = True

    def delete(self, query: dict) -> int:
        docs = self.find(query)
        if not docs:
            return 0
        cnt = 0
        txn_id = self.storage.begin_txn()
        del_log = []
        try:
            for doc in docs:
                doc_copy = doc.copy()
                self._delete_mem(doc['_id'], doc)
                del_log.append((doc['_id'], doc_copy))
                self.storage.append_log('delete', self.name, doc['_id'], doc_copy, txn_id=txn_id, before_image=doc_copy)
                cnt += 1
            self.storage.commit_txn(txn_id)
            return cnt
        except Exception:
            for doc_id, doc_data in reversed(del_log):
                self._insert_mem(doc_data)
            self.storage.rollback_txn(txn_id)
            raise

    def count(self, query: dict=None) -> int:
        return len(self.find(query))

    def append(self, op: str, data: dict):
        if hasattr(self.storage, 'append_log'):
            doc_id = data.get('_id') if data else None
            self.storage.append_log(op, self.name, doc_id, data)

    def get_audit_trail(self, doc_id: str=None, limit: int=100) -> List[dict]:
        return self.storage.read_audit_log(self.name, doc_id, limit)

class HVPDB:

    def __init__(self, path: str, password: str=None, durable: bool=True):
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
        self.password = password
        self.durable = durable
        self._user_ctx = contextvars.ContextVar(f'user_{uuid.uuid4()}', default=None)
        self._txn_ctx = contextvars.ContextVar(f'txn_{uuid.uuid4()}', default=None)
        self._groups = {}
        if self.is_cluster:
            self.storage = None
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
    def current_user(self):
        return self._user_ctx.get()

    @current_user.setter
    def current_user(self, value):
        self._user_ctx.set(value)

    @property
    def current_txn(self):
        return self._txn_ctx.get()

    @property
    def help(self):
        print('HVPDB - The Database.')

    def __getattr__(self, name: str):
        if name.startswith('_'):
            raise AttributeError(f"'HVPDB' object has no attribute '{name}'")
        return self.group(name)

    def load_plugins(self):
        try:
            from importlib.metadata import entry_points
            eps = entry_points(group='hvpdb.plugins')
        except TypeError:
            try:
                from importlib.metadata import entry_points
                eps = entry_points().get('hvpdb.plugins', [])
            except ImportError:
                return
        for ep in eps:
            try:
                cls = ep.load()
                if isinstance(cls, type):
                    self.plugins[ep.name] = cls(self)
            except Exception:
                pass

    def _create_root_user(self):
        if 'root' not in self.storage.data['users']:
            self.storage.data['users']['root'] = {'role': 'admin', 'groups': ['*'], 'created_at': time.time()}
            self.storage._dirty = True

    def hash_user_password(self, password: str) -> str:
        return self._hash_password(password)

    def _hash_password(self, password: str) -> str:
        try:
            from argon2 import PasswordHasher
            ph = PasswordHasher()
            return ph.hash(password)
        except ImportError:
            salt = secrets.token_bytes(16)
            key = hashlib.scrypt(password.encode(), salt=salt, n=16384, r=8, p=1, dklen=32)
            return f'scrypt${salt.hex()}${key.hex()}'

    def _verify_password(self, stored: str, password: str) -> bool:
        if not stored:
            return False
        try:
            if stored.startswith('scrypt$'):
                _, salt_hex, key_hex = stored.split('$')
                salt = bytes.fromhex(salt_hex)
                check = hashlib.scrypt(password.encode(), salt=salt, n=16384, r=8, p=1, dklen=32)
                return secrets.compare_digest(check.hex(), key_hex)
            else:
                if '$' in stored and (not stored.startswith('$argon2')):
                    salt, val = stored.split('$')
                    if len(salt) == 16:
                        vhash = hashlib.sha256((salt + password).encode()).hexdigest()
                        return secrets.compare_digest(val, vhash)
                from argon2 import PasswordHasher
                return PasswordHasher().verify(stored, password)
        except Exception:
            return False

    def authenticate(self, username: str, password: str) -> bool:
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
        if username not in self.storage.data['users']:
            return False
        user = self.storage.data['users'][username]
        if user['role'] == 'admin':
            return True
        return group_name in user['groups'] or '*' in user['groups']

    def group(self, name: str) -> HVPGroup:
        if not name or any((c in name for c in '\\/:*?"<>|')):
            raise ValueError(f"Invalid group: '{name}'")
        if name in self._groups:
            return self._groups[name]
        if self.is_cluster:
            path = os.path.join(self.filepath, f'{name}.hvp')
            s = HVPStorage(path, self.password, durable=self.durable)
            s.load()
            if 'groups' not in s.data:
                s.data['groups'] = {}
            g = HVPGroup(s, name, self)
            self._groups[name] = g
            return g
        else:
            if name not in self._groups:
                self._groups[name] = HVPGroup(self.storage, name, self)
            return self._groups[name]

    def get_all_groups(self) -> List[str]:
        if self.is_cluster:
            gs = []
            if os.path.exists(self.filepath):
                for f in os.listdir(self.filepath):
                    if f.endswith('.hvp'):
                        gs.append(f[:-4])
            return sorted(gs)
        else:
            return list(self.storage.data.get('groups', {}).keys())

    def commit(self):
        if self.is_cluster:
            for _, grp in self._groups.items():
                if grp.storage._dirty:
                    grp.storage.save()
        elif self.storage._dirty:
            self.storage.save()

    def refresh(self, force: bool=False):
        if self.is_cluster:
            for _, grp in self._groups.items():
                grp.storage.refresh(force=force)
        else:
            self.storage.refresh(force=force)
            for grp in self._groups:
                self._groups[grp]._rebuild_indexes()

    def close(self):
        self.commit()
        if self.storage:
            if hasattr(self.storage, 'wal'):
                self.storage.wal.close()
            if self.storage.security:
                self.storage.security.clear_key()
        if self.is_cluster:
            for grp in self._groups.values():
                if grp.storage:
                    if hasattr(grp.storage, 'wal'):
                        grp.storage.wal.close()
                    if grp.storage.security:
                        grp.storage.security.clear_key()

    def begin(self):
        from .transaction import HVPTransaction
        return HVPTransaction(self)

    def change_password(self, new_password: str):
        if self.is_cluster:
            self.password = new_password
            for grp in self._groups.values():
                grp.storage.password = new_password
                grp.storage.security = None
                grp.storage._dirty = True
                grp.storage.save()
        else:
            self.password = new_password
            self.storage.password = new_password
            self.storage.security = None
            self.storage._dirty = True
            self.storage.save()