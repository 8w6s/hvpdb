import errno
import os
import shutil
import time
import warnings
from typing import Any, Dict, List, Optional, cast

import msgpack
import portalocker
import zstandard as zstd

from .concurrency import HVPLockManager
from .security import HVPSecurity
from .uri import HVPURI
from .wal import HVPWAL
from .utils import acquire_interruptible_lock

HEADER = b'HVPDB'
VERSION = 2

class HVPStorage:
    """
    Storage engine for HVPDB handling file I/O, encryption, and compression.
    
    Implements durable storage with Write-Ahead Logging (WAL) and 
    thread-safe access via lock management.
    """

    def __init__(self, filepath_or_uri: str, password: Optional[str]=None, durable: bool=True):
        """
        Initialize the storage engine.
        
        Args:
            filepath_or_uri: Path to the database file or cluster URI.
            password: Password for encryption/authentication.
            durable: If True, uses WAL for crash consistency.
        """
        self.connection_info = None
        self.filepath = ''
        self.password = password
        self.durable = durable
        
        if filepath_or_uri.startswith('hvp://'):
            self.connection_info = HVPURI.parse(filepath_or_uri)
            self.filepath = self.connection_info.cluster or ''
            uri_pass = self.connection_info.password
            if uri_pass and not self.password:
                self.password = uri_pass
            # Standardize file extension
            if not self.filepath.endswith('.hvp'):
                self.filepath += '.hvp'
        else:
            self.filepath = filepath_or_uri
            if not self.filepath.endswith('.hvp') and not self.filepath.endswith('.hvdb'):
                self.filepath += '.hvp'
        
        if not self.password:
            raise ValueError('Auth Error: Password required.')
        
        # Ensure database directory exists and has safe permissions
        db_dir = os.path.dirname(os.path.abspath(self.filepath))
        if os.path.exists(db_dir):
            try:
                os.chmod(db_dir, 0o700) # Only owner can read/write/exec
            except OSError:
                pass
        
        self.log_path = self.filepath + '.log'
        # Initialize WAL log file
        if not os.path.exists(self.log_path):
            with open(self.log_path, 'wb'):
                pass
        
        try:
            os.chmod(self.log_path, 0o600) # Only owner can read/write
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
        
        # Auto-Checkpoint Config
        self.wal_checkpoint_threshold = 10 * 1024 * 1024  # 10 MB

    def _init_security(self, salt: Optional[bytes]=None, kdf_params: Optional[dict]=None):
        """
        Initialize the security layer with the provided or existing password.
        
        Args:
            salt: Optional salt for key derivation.
            kdf_params: Optional KDF parameters.
        """
        if not self.security:
            if self.password is None:
                raise ValueError("Password required for security initialization")
            self.security = HVPSecurity(self.password, salt, kdf_params)
            self.wal.security = self.security
            self.wal.ensure_header(self.security.get_salt(), self.security.get_kdf_params())

    def refresh(self, force: bool=False):
        """
        Reload the storage from disk.
        
        Args:
            force: If True, allows reloading even if there are unsaved changes.
        """
        if self._dirty and (not force):
            raise RuntimeError('Cannot refresh with unsaved changes.')
        self.load()

    def load(self):
        """
        Read and decrypt the database file from disk.
        
        This method uses a reader lock to ensure thread-safe access and 
        replays the WAL to apply any pending operations.
        """
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
                            assert self.security is not None
                            compressed_data = self.security.decrypt(nonce, ciphertext)
                        elif version == 2:
                            salt = f.read(16)
                            kdf_len = int.from_bytes(f.read(2), 'big')
                            kdf_bytes = f.read(kdf_len)
                            kdf_params = msgpack.unpackb(kdf_bytes)
                            nonce = f.read(12)
                            ciphertext = f.read()
                            self._init_security(salt, kdf_params)
                            assert self.security is not None
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
        """
        Apply all logged operations since the last save point.
        
        This synchronizes the in-memory state with any crash-recovery 
        data found in the WAL log.
        """
        replayed_count = self.wal.replay(self._last_sequence, self._apply_entry)
        if replayed_count > 0:
            self._dirty = True

    def _apply_entry(self, entry: dict):
        """
        Apply a single WAL entry to the in-memory data.
        
        Args:
            entry: The WAL log entry to apply.
        """
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
        """Compress, encrypt, and atomically save the database to disk."""
        with self.lock_manager.writer_lock():
            self._init_security()
            if self.security is None:
                raise RuntimeError("Security context not initialized")
            self.data['seq'] = self._last_sequence
            packed_data = cast(bytes, msgpack.packb(self.data, use_bin_type=True))
            compressed_data = self.cctx.compress(packed_data)
            salt = self.security.get_salt()
            kdf_params = self.security.get_kdf_params()
            kdf_bytes = cast(bytes, msgpack.packb(kdf_params))
            kdf_len = len(kdf_bytes)
            aad = HEADER + VERSION.to_bytes(2, 'big') + salt + kdf_len.to_bytes(2, 'big') + kdf_bytes
            nonce, ciphertext = self.security.encrypt(compressed_data, associated_data=aad)
            temp_path = self.filepath + '.tmp'
            try:
                # Try to open with secure permissions (0o600)
                fd = os.open(temp_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            except OSError as e:
                # Termux/Android shared storage often doesn't support setting permissions (ENOSYS/EPERM)
                # 38=ENOSYS, 1=EPERM, 95=EOPNOTSUPP
                if e.errno in (errno.ENOSYS, errno.EPERM, getattr(errno, 'EOPNOTSUPP', 95)) or 'not implemented' in str(e).lower():
                     print(f"Warning: Could not set secure permissions on {temp_path}. File may be readable by other users.")
                     fd = os.open(temp_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
                else:
                    raise
            with os.fdopen(fd, 'wb') as f:
                try:
                    acquire_interruptible_lock(f)
                except (OSError, portalocker.LockException):
                    pass
                try:
                    f.write(HEADER)
                    f.write(VERSION.to_bytes(2, 'big'))
                    f.write(salt)
                    f.write(kdf_len.to_bytes(2, 'big'))
                    f.write(kdf_bytes)
                    f.write(nonce)
                    f.write(ciphertext)
                    f.flush()  # Ensure data is pushed to OS buffer
                    os.fsync(f.fileno())  # Ensure data is physically written to disk
                finally:
                    try:
                        portalocker.unlock(f)
                    except OSError:
                        pass
            with self.lock_manager.critical_swap_lock():
                retries = 5
                while retries > 0:
                    try:
                        os.replace(temp_path, self.filepath)
                        
                        # Write Barrier: Ensure directory metadata entry is persisted
                        # This guarantees the file replacement is atomic and durable
                        if hasattr(os, 'open') and hasattr(os, 'fsync'):
                            try:
                                dir_fd = os.open(os.path.dirname(os.path.abspath(self.filepath)), os.O_RDONLY)
                                try:
                                    os.fsync(dir_fd)
                                finally:
                                    os.close(dir_fd)
                            except (OSError, ValueError):
                                # Directory fsync might not be supported on all platforms/filesystems
                                pass
                        
                        break
                    except OSError as e:
                        # Handle Termux/FUSE limitations (ENOSYS/EPERM/EXDEV)
                        if e.errno in (errno.ENOSYS, errno.EPERM, errno.EXDEV, getattr(errno, 'EOPNOTSUPP', 95)) or 'not implemented' in str(e).lower():
                            try:
                                if os.path.exists(self.filepath):
                                    os.remove(self.filepath)
                                os.rename(temp_path, self.filepath)
                                break
                            except OSError:
                                # If rename also fails, try shutil.move as last resort
                                try:
                                    shutil.move(temp_path, self.filepath)
                                    break
                                except OSError:
                                    pass  # Retry logic will handle this

                        retries -= 1
                        if retries == 0:
                            raise
                        time.sleep(0.1)
                self.wal.truncate()
            self._dirty = False

    def commit(self):
        """
        Persist in-memory changes to the WAL and optionally to the main file.
        """
        if not self.durable:
            return
        
        # Check WAL size for auto-checkpoint
        try:
            if os.path.exists(self.log_path) and os.path.getsize(self.log_path) > self.wal_checkpoint_threshold:
                self.save()
                return
        except OSError:
            pass

        if self._dirty:
            # In durable mode, we rely on WAL. 
            # Changes are already in memory and in WAL (via append).
            # We only force a full save (checkpoint) if explicitly requested or auto-triggered.
            pass

    def begin_txn(self) -> str:
        """
        Start a new transaction and return its ID.
        
        Returns:
            The unique transaction ID.
        """
        txn_id = self.wal.begin_transaction()
        self._init_security()
        self._last_sequence += 1
        self._txn_buffers[txn_id] = []
        entry = {'seq': self._last_sequence, 'txn': txn_id, 'type': 'BEGIN', 'ts': time.time()}
        self._txn_buffers[txn_id].append(entry)
        return txn_id

    def commit_txn(self, txn_id: str):
        """
        Commit a transaction and sync its operations to the WAL.
        
        Args:
            txn_id: The ID of the transaction to commit.
        """
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
        """
        Roll back a transaction and discard its operations.
        
        Args:
            txn_id: The ID of the transaction to roll back.
        """
        self._init_security()
        self._last_sequence += 1
        if txn_id in self._txn_buffers:
            del self._txn_buffers[txn_id]
        self.wal.log_rollback(self._last_sequence, txn_id)

    def append_log(self, op: str, group_name: str, doc_id: str, data: dict, txn_id: Optional[str]=None, before_image: Optional[dict]=None):
        """
        Append an operation to the WAL.
        
        Args:
            op: Operation type ('insert', 'update', 'delete').
            group_name: Name of the data group.
            doc_id: ID of the affected document.
            data: New document data.
            txn_id: Optional transaction ID.
            before_image: Optional document state before the operation.
        """
        self._init_security()
        self._last_sequence += 1
        entry = {'seq': self._last_sequence, 'txn': txn_id, 'type': 'DATA', 'op': op, 'g': group_name, 'id': doc_id, 'd': data, 'b': before_image, 'ts': time.time()}
        if txn_id and txn_id in self._txn_buffers:
            self._txn_buffers[txn_id].append(entry)
        else:
            self.wal.append(self._last_sequence, op, group_name, doc_id, data, txn_id, before_image, sync=self.durable)

    def append_batch_log(self, operations: list, txn_id: Optional[str]=None):
        """
        Append multiple operations to the WAL in a single batch.
        
        Args:
            operations: List of operation dictionaries.
            txn_id: Optional transaction ID.
        """
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
        except Exception as e:
            if is_implicit:
                self.rollback_txn(txn_id)
            warnings.warn(f"Batch log append failed, rolled back: {e}")
            raise

    def read_audit_log(self, group_name: str, doc_id: Optional[str]=None, limit: int=100) -> list:
        """
        Read audit history for a group or specific document.
        
        Args:
            group_name: Name of the group to query.
            doc_id: Optional document ID to filter by.
            limit: Maximum number of entries to return.
            
        Returns:
            List of audit log entries.
        """
        results = []

        def collector(entry):
            if entry.get('g') == group_name:
                if doc_id is None or entry.get('id') == doc_id:
                    results.append(entry)
        self.wal.replay(0, collector)
        return sorted(results, key=lambda x: x.get('ts', 0), reverse=True)[:limit]
