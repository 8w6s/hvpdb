import struct
import zstandard as zstd
import msgpack
import os
import stat
import portalocker
import time
import uuid
from typing import Optional, Dict, Any
from .security import HVPSecurity
from .uri import HVPURI
from .wal import HVPWAL
from .concurrency import HVPLockManager
from .exceptions import AuthError
HEADER = b'HVPDB'
VERSION = 2

class HVPStorage:

    def __init__(self, filepath_or_uri: str, password: str=None, durable: bool=True):
        self.connection_info = None
        self.filepath = ''
        self.password = password
        self.durable = durable
        if filepath_or_uri.startswith('hvp://'):
            self.connection_info = HVPURI.parse(filepath_or_uri)
            self.filepath = self.connection_info.cluster
            uri_pass = self.connection_info.password
            if uri_pass:
                if self.password:
                    pass
                else:
                    self.password = uri_pass
            self.password = self.password or uri_pass
            if not self.filepath.endswith('.hvp'):
                self.filepath += '.hvp'
        else:
            self.filepath = filepath_or_uri
            if not self.filepath.endswith('.hvp') and (not self.filepath.endswith('.hvdb')):
                self.filepath += '.hvp'
        if not self.password:
            raise ValueError('Auth Error: Password required.')
        db_dir = os.path.dirname(os.path.abspath(self.filepath))
        if os.path.exists(db_dir):
            try:
                os.chmod(db_dir, 448)
            except OSError:
                pass
        self.log_path = self.filepath + '.log'
        if not os.path.exists(self.log_path):
            with open(self.log_path, 'wb') as f:
                pass
            os.chmod(self.log_path, 384)
        else:
            try:
                os.chmod(self.log_path, 384)
            except OSError:
                pass
        self.security: Optional[HVPSecurity] = None
        self.data: Dict[str, Any] = {'groups': {}}
        self._dirty = False
        self._last_sequence = 0
        self.cctx = zstd.ZstdCompressor(level=3)
        self.dctx = zstd.ZstdDecompressor()
        self.wal = HVPWAL(self.log_path, self.security)
        self.lock_manager = HVPLockManager(self.filepath)
        self._txn_buffers = {}

    def _init_security(self, salt: Optional[bytes]=None, kdf_params: Optional[dict]=None):
        if not self.security:
            self.security = HVPSecurity(self.password, salt, kdf_params)
            self.wal.security = self.security
            self.wal.ensure_header(self.security.get_salt(), self.security.get_kdf_params())

    def refresh(self, force: bool=False):
        if self._dirty and (not force):
            raise RuntimeError('Cannot refresh with unsaved changes.')
        self.load()

    def load(self):
        with self.lock_manager.reader_lock():
            if not os.path.exists(self.filepath):
                self.data = {'groups': {}}
                self._last_sequence = 0
                salt, kdf_params = HVPWAL.read_header(self.log_path)
                if salt:
                    self._init_security(salt, kdf_params)
                else:
                    self._init_security()
            else:
                with open(self.filepath, 'rb') as f:
                    try:
                        header = f.read(5)
                        if header != HEADER:
                            raise ValueError('Invalid Header')
                        version = int.from_bytes(f.read(2), 'big')
                        if version == 1:
                            salt = f.read(16)
                            nonce = f.read(12)
                            ciphertext = f.read()
                            self._init_security(salt)
                            compressed_data = self.security.decrypt(nonce, ciphertext)
                        elif version == 2:
                            salt = f.read(16)
                            kdf_len = int.from_bytes(f.read(2), 'big')
                            kdf_bytes = f.read(kdf_len)
                            kdf_params = msgpack.unpackb(kdf_bytes)
                            nonce = f.read(12)
                            ciphertext = f.read()
                            self._init_security(salt, kdf_params)
                            aad = HEADER + version.to_bytes(2, 'big') + salt + kdf_len.to_bytes(2, 'big') + kdf_bytes
                            compressed_data = self.security.decrypt(nonce, ciphertext, associated_data=aad)
                        else:
                            raise ValueError(f'Unsupported Version: {version}')
                        packed_data = self.dctx.decompress(compressed_data)
                        self.data = msgpack.unpackb(packed_data, raw=False)
                        self._last_sequence = self.data.get('seq', 0)
                    except Exception as e:
                        raise ValueError(f'Decryption Failed: {e}')
        self._replay_wal()

    def _replay_wal(self):
        replayed_count = self.wal.replay(self._last_sequence, self._apply_entry)
        if replayed_count > 0:
            self._dirty = True

    def _apply_entry(self, entry: dict):
        entry_type = entry.get('type', 'DATA')
        if entry_type not in ('DATA', 'legacy'):
            return
        op = entry.get('op')
        group_name = entry.get('g')
        doc_id = entry.get('id')
        data = entry.get('d')
        seq = entry.get('seq', 0)
        if seq > self._last_sequence:
            self._last_sequence = seq
        if not group_name:
            return
        if group_name not in self.data['groups']:
            self.data['groups'][group_name] = {}
        group_data = self.data['groups'][group_name]
        if op == 'insert' or op == 'update':
            if doc_id and data:
                group_data[doc_id] = data
        elif op == 'delete':
            if doc_id and doc_id in group_data:
                del group_data[doc_id]
        elif data and '_id' in data:
            group_data[data['_id']] = data

    def save(self):
        with self.lock_manager.writer_lock():
            self._init_security()
            self.data['seq'] = self._last_sequence
            packed_data = msgpack.packb(self.data, use_bin_type=True)
            compressed_data = self.cctx.compress(packed_data)
            salt = self.security.get_salt()
            kdf_params = self.security.get_kdf_params()
            kdf_bytes = msgpack.packb(kdf_params)
            kdf_len = len(kdf_bytes)
            aad = HEADER + VERSION.to_bytes(2, 'big') + salt + kdf_len.to_bytes(2, 'big') + kdf_bytes
            nonce, ciphertext = self.security.encrypt(compressed_data, associated_data=aad)
            temp_path = self.filepath + '.tmp'
            fd = os.open(temp_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 384)
            with os.fdopen(fd, 'wb') as f:
                portalocker.lock(f, portalocker.LOCK_EX)
                try:
                    f.write(HEADER)
                    f.write(VERSION.to_bytes(2, 'big'))
                    f.write(salt)
                    f.write(kdf_len.to_bytes(2, 'big'))
                    f.write(kdf_bytes)
                    f.write(nonce)
                    f.write(ciphertext)
                    f.flush()
                    try:
                        os.fsync(f.fileno())
                    except OSError:
                        pass
                finally:
                    portalocker.unlock(f)
            with self.lock_manager.critical_swap_lock():
                retries = 5
                while retries > 0:
                    try:
                        os.replace(temp_path, self.filepath)
                        break
                    except OSError:
                        retries -= 1
                        if retries == 0:
                            raise
                        time.sleep(0.1)
                self.wal.truncate()
            self._dirty = False

    def begin_txn(self) -> str:
        txn_id = self.wal.begin_transaction()
        self._init_security()
        self._last_sequence += 1
        self._txn_buffers[txn_id] = []
        entry = {'seq': self._last_sequence, 'txn': txn_id, 'type': 'BEGIN', 'ts': time.time()}
        self._txn_buffers[txn_id].append(entry)
        return txn_id

    def commit_txn(self, txn_id: str):
        self._init_security()
        self._last_sequence += 1
        entry = {'seq': self._last_sequence, 'txn': txn_id, 'type': 'COMMIT', 'ts': time.time()}
        if txn_id in self._txn_buffers:
            self._txn_buffers[txn_id].append(entry)
            self.wal.write_batch(self._txn_buffers[txn_id], sync=self.durable)
            del self._txn_buffers[txn_id]
        else:
            self.wal.log_commit(self._last_sequence, txn_id)

    def rollback_txn(self, txn_id: str):
        self._init_security()
        self._last_sequence += 1
        if txn_id in self._txn_buffers:
            del self._txn_buffers[txn_id]
        self.wal.log_rollback(self._last_sequence, txn_id)

    def append_log(self, op: str, group_name: str, doc_id: str, data: dict, txn_id: str=None, before_image: dict=None):
        self._init_security()
        self._last_sequence += 1
        if not txn_id:
            txn_id = str(uuid.uuid4())
        entry = {'seq': self._last_sequence, 'txn': txn_id, 'type': 'DATA', 'op': op, 'g': group_name, 'id': doc_id, 'd': data, 'b': before_image, 'ts': time.time()}
        if txn_id in self._txn_buffers:
            self._txn_buffers[txn_id].append(entry)
        else:
            self.wal.append(self._last_sequence, op, group_name, doc_id, data, txn_id, before_image, sync=self.durable)

    def append_batch_log(self, operations: list, txn_id: str=None):
        self._init_security()
        is_implicit = False
        if not txn_id:
            txn_id = self.begin_txn()
            is_implicit = True
        try:
            for op_data in operations:
                self.append_log(op=op_data.get('op'), group_name=op_data.get('g'), doc_id=op_data.get('id'), data=op_data.get('d'), txn_id=txn_id, before_image=op_data.get('b'))
            if is_implicit:
                self.commit_txn(txn_id)
        except Exception:
            if is_implicit:
                self.rollback_txn(txn_id)
            raise

    def read_audit_log(self, group_name: str, doc_id: str=None, limit: int=100) -> list:
        results = []

        def collector(entry):
            if entry.get('g') == group_name:
                if doc_id is None or entry.get('id') == doc_id:
                    results.append(entry)
        self.wal.replay(0, collector)
        return sorted(results, key=lambda x: x.get('ts', 0), reverse=True)[:limit]