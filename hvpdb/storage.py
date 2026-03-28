import errno
import os
import shutil
import time
import warnings
import uuid
from typing import Any, Dict, Optional, cast

import msgpack
import portalocker
import zstandard as zstd

from .concurrency import HVPLockManager
from .security import HVPSecurity
from .uri import HVPURI
from .wal import HVPWAL
from .utils import acquire_interruptible_lock, default_serializer

HEADER = b'HVPDB'
VERSION = 3
COMPRESSION_ZSTD = 1

class HVPStorage:
    """
    Storage engine for HVPDB handling file I/O, encryption, and compression.
    
    Implements durable storage with Write-Ahead Logging (WAL) and 
    thread-safe access via lock management.
    """

    def __init__(self, filepath_or_uri: str, password: Optional[str]=None, durable: bool=True, wal_checksum_type: int=0):
        """
        Initialize the storage engine.
        
        Args:
            filepath_or_uri: Path to the database file or cluster URI.
            password: Password for encryption/authentication.
            durable: If True, uses WAL for crash consistency.
            wal_checksum_type: 0 for CRC32, 1 for SHA-256.
        """
        self.connection_info = None
        self.filepath = ''
        self.password = password
        self.durable = durable
        self.wal_checksum_type = wal_checksum_type
        
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
        # Do not pre-create log file here, let HVPWAL handle it atomically via ensure_header
        
        try:
            if os.path.exists(self.log_path):
                os.chmod(self.log_path, 0o600) # Only owner can read/write
        except OSError:
            pass

        self.security: Optional[HVPSecurity] = None
        self.data: Dict[str, Any] = {'groups': {}}
        self._dirty = False
        self._last_sequence = 0
        self.cctx = zstd.ZstdCompressor(level=3)
        self.dctx = zstd.ZstdDecompressor()
        self._defunct = False
        self.wal = HVPWAL(self.log_path, self.security, checksum_type=wal_checksum_type)
        self.lock_manager = HVPLockManager(self.filepath)
        self._txn_buffers = {}
        
        # Auto-Checkpoint Config
        self.wal_checkpoint_threshold = 10 * 1024 * 1024  # 10 MB
        self._last_mtime_ns = 0
        self._last_size = 0
        self._last_ino = 0
        self._last_log_mtime_ns = 0
        self._last_log_size = 0
        self._last_log_ino = 0
        self._session_id = os.urandom(4).hex() # Phase 13: Unique ID for this writer session
        self.on_reload = []
        self.compression_level = 3
        self.compression_type = COMPRESSION_ZSTD

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
            
            # Atomic header check/init
            salt_from_wal, kdf_from_wal, _ = HVPWAL.read_header(self.log_path)
            if salt_from_wal:
                salt, kdf_params = salt_from_wal, kdf_from_wal
                
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

    def _update_file_stats(self):
        """Update file stats (mtime_ns, size, inode) for change detection."""
        if os.path.exists(self.filepath):
            try:
                st = os.stat(self.filepath)
                self._last_mtime_ns = st.st_mtime_ns
                self._last_size = st.st_size
                self._last_ino = st.st_ino
            except OSError:
                pass
        
        if os.path.exists(self.log_path):
            try:
                st = os.stat(self.log_path)
                self._last_log_mtime_ns = st.st_mtime_ns
                self._last_log_size = st.st_size
                self._last_log_ino = st.st_ino
            except OSError:
                pass
        else:
            self._last_log_mtime_ns = 0
            self._last_log_size = 0
            self._last_log_ino = 0
            self._last_log_mtime = 0
            self._last_log_size = 0

    def check_reload(self):
        """Check for disk changes using mtime_ns, size, and inode."""
        if not os.path.exists(self.filepath):
            if self._last_mtime_ns != 0 and not self._defunct and not os.path.exists(self.log_path):
                # Group was dropped by another process
                self._defunct = True
                self.data['groups'] = {}
                for callback in self.on_reload:
                    try: callback()
                    except: pass
            return
        
        try:
            st = os.stat(self.filepath)
            log_st = os.stat(self.log_path) if os.path.exists(self.log_path) else None
            
            changed = (st.st_mtime_ns != self._last_mtime_ns or 
                       st.st_size != self._last_size or 
                       st.st_ino != self._last_ino)
            
            if not changed and log_st:
                changed = (log_st.st_mtime_ns != self._last_log_mtime_ns or 
                           log_st.st_size != self._last_log_size or 
                           log_st.st_ino != self._last_log_ino)
            
            if changed:
                self.load()
        except OSError:
            pass

    def register_reload_callback(self, callback):
        """Register a function to be called after a successful reload/WAL replay."""
        self.on_reload.append(callback)

    def unregister_reload_callback(self, callback):
        """Unregister a reload callback (prevents memory leaks when objects are deleted)."""
        try:
            self.on_reload.remove(callback)
        except ValueError:
            pass  # Callback was not registered

    def load(self):
        """
        Read and decrypt the database file from disk.
        
        This method uses a reader lock to ensure thread-safe access and 
        replays the WAL to apply any pending operations.
        """
        with self.lock_manager.writer_lock():
            if self._defunct:
                 raise OSError(f"Database file {self.filepath} has been deleted/defunct.")
            if not os.path.exists(self.filepath):
                self.data = {'groups': {}}
                self._last_sequence = 0
                salt, kdf_params, _ = HVPWAL.read_header(self.log_path)
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
                        elif version >= 2:
                            salt = f.read(16)
                            kdf_len = int.from_bytes(f.read(2), 'big')
                            kdf_bytes = f.read(kdf_len)
                            kdf_params = msgpack.unpackb(kdf_bytes)
                            
                            comp_type = COMPRESSION_ZSTD
                            if version >= 3:
                                comp_type = int.from_bytes(f.read(1), 'big')
                                
                            nonce = f.read(12)
                            ciphertext = f.read()
                            self._init_security(salt, kdf_params)
                            assert self.security is not None
                            
                            # Build AAD
                            prefix = HEADER + version.to_bytes(2, 'big') + salt + kdf_len.to_bytes(2, 'big') + kdf_bytes
                            if version >= 3:
                                prefix += comp_type.to_bytes(1, 'big')
                            
                            compressed_data = self.security.decrypt(nonce, ciphertext, associated_data=prefix)
                        else:
                            raise ValueError(f'Unsupported Version: {version}')
                        packed_data = self.dctx.decompress(compressed_data)
                        self.data = msgpack.unpackb(packed_data, raw=False)
                        self._last_sequence = self.data.get('seq', 0)
                    except Exception as e:
                        raise ValueError(f'Decryption Failed: {e}')
            # Always update stats even if file didn't exist (might have WAL)
            self._update_file_stats()
        self._replay_wal()
        
        # Notify listeners that data has been updated
        for callback in self.on_reload:
            try:
                callback()
            except Exception as e:
                warnings.warn(f"Reload callback failed: {e}")

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
        if entry_type not in ('DATA', 'legacy', 'drop_group'):
            return
        op = entry.get('op')
        group_name = entry.get('g')
        doc_id = entry.get('id')
        data = entry.get('d')
        seq = entry.get('seq', 0)
        if seq <= self._last_sequence and seq != 0:
            return
        self._last_sequence = seq

        if op == 'drop_group' or entry_type == 'drop_group':
            if group_name in self.data['groups']:
                del self.data['groups'][group_name]
            return
        if group_name == 'kv':
            if 'kv' not in self.data:
                self.data['kv'] = {}
            if op == 'set':
                self.data['kv'][doc_id] = data.get('value') if isinstance(data, dict) else data
            elif op == 'delete':
                if doc_id in self.data['kv']:
                    del self.data['kv'][doc_id]
            return

        if group_name not in self.data['groups']:
            self.data['groups'][group_name] = {}
        group_data = self.data['groups'][group_name]
        
        if op == 'insert' or op == 'update' or op == 'set':
            if doc_id and data:
                group_data[doc_id] = data
        elif op == 'delete':
            if doc_id and doc_id in group_data:
                del group_data[doc_id]
        elif data and '_id' in data:
            group_data[data['_id']] = data

    def save(self):
        """Compress, encrypt, and atomically save the database to disk (Manual trigger)."""
        if self._defunct:
            raise OSError(f"Cannot save: Database file {self.filepath} has been deleted/defunct.")
        self.checkpoint()

    def checkpoint(self):
        """
        Perform a full checkpoint: Serialize memory -> Disk, Truncate WAL.
        Acquires writer_lock to ensure exclusivity.
        """
        if self._defunct:
            raise OSError(f"Cannot checkpoint: Database file {self.filepath} has been deleted/defunct.")
        with self.lock_manager.writer_lock():
            # Hold WAL lock during the entire checkpoint to prevent 
            # concurrent writes from being lost during truncate.
            with self.wal.exclusive_lock():
                print(f"DEBUG: STG performing checkpoint (truncating WAL) for {self.filepath}")
                self._save_internal()

    def create_snapshot(self, snapshot_path: str):
        """
        Create a point-in-time snapshot of the database.
        Wraps both save and copy in a single writer lock to prevent TOCTOU.
        """
        if not snapshot_path.endswith('.hvp'):
            snapshot_path += '.hvp'
            
        with self.lock_manager.writer_lock():
            # Flush memory to main file
            self._save_internal()
            if os.path.exists(self.filepath):
                shutil.copy2(self.filepath, snapshot_path)

    def repair(self):
        """
        Attempt to repair corrupted storage or WAL.
        """
        with self.lock_manager.writer_lock():
            print("[REPAIR] Attempting database repair...")
            try:
                self.load() 
                self._save_internal()
                self.wal.truncate()
                print("[REPAIR] Repair completed successfully.")
                return True
            except Exception as e:
                print(f"[REPAIR] Repair failed: {e}")
                return False

    def _save_internal(self):
        """Internal save implementation. Expects writer lock to be held."""
        self._init_security()
        if self.security is None:
            raise RuntimeError("Security context not initialized")
        self.data['seq'] = self._last_sequence
        packed_data = cast(bytes, msgpack.packb(self.data, use_bin_type=True, default=default_serializer))
        compressed_data = self.cctx.compress(packed_data)
        salt = self.security.get_salt()
        kdf_params = self.security.get_kdf_params()
        kdf_bytes = cast(bytes, msgpack.packb(kdf_params))
        kdf_len = len(kdf_bytes)
        aad = HEADER + VERSION.to_bytes(2, 'big') + salt + kdf_len.to_bytes(2, 'big') + kdf_bytes + self.compression_type.to_bytes(1, 'big')
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
                f.write(self.compression_type.to_bytes(1, 'big'))
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
            retries = 20
            while retries > 0:
                try:
                    if hasattr(os, 'replace'):
                        os.replace(temp_path, self.filepath)
                    else:
                        if os.path.exists(self.filepath):
                            os.remove(self.filepath)
                        os.rename(temp_path, self.filepath)
                    
                    # Write Barrier: Ensure directory metadata entry is persisted
                    # This guarantees the file replacement is atomic and durable
                    if os.name != 'nt' and hasattr(os, 'open') and hasattr(os, 'fsync'):
                        try:
                            dir_fd = os.open(os.path.dirname(os.path.abspath(self.filepath)), os.O_RDONLY)
                            try:
                                os.fsync(dir_fd)
                            finally:
                                os.close(dir_fd)
                        except (OSError, ValueError):
                            pass
                    
                    self._update_file_stats()
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

    def _check_auto_checkpoint(self):
        """Internal: Check if WAL size exceeds threshold and trigger checkpoint."""
        if not self.durable:
            return
        
        # Check WAL size for auto-checkpoint
        try:
            if os.path.exists(self.log_path):
                sz = os.path.getsize(self.log_path)
                if sz > self.wal_checkpoint_threshold:
                    print(f"DEBUG: STG auto-checkpoint triggered (size {sz} > threshold {self.wal_checkpoint_threshold})")
                    self.checkpoint()
        except OSError:
            pass

    def commit(self):
        """
        Persist in-memory changes to the WAL and optionally to the main file.
        """
        self._check_auto_checkpoint()

        if self._dirty:
            # In durable mode, we rely on WAL. 
            # Changes are already in memory and in WAL (via append).
            # We only force a full save (checkpoint) if explicitly requested or auto-triggered.
            pass

    def begin_txn(self) -> str:
        """
        Start a new transaction and return its ID.
        """
        self._init_security()
        
        # Phase 13: Hold exclusive lock for atomic sequence increment
        with self.wal.exclusive_lock():
            self.check_reload()
            txn_id = self.wal.begin_transaction()
            self._last_sequence += 1
            self._txn_buffers[txn_id] = []
            print(f"DEBUG: Storage begin_txn {txn_id}, last_seq {self._last_sequence}")
            entry = {'seq': self._last_sequence, 'sid': self._session_id, 'txn': txn_id, 'type': 'BEGIN', 'ts': time.time()}
            self._txn_buffers[txn_id].append(entry)
            return txn_id

    def commit_txn(self, txn_id: str, check_auto_checkpoint: bool = True):
        """
        Commit a transaction and sync its operations to the WAL.
        """
        self._init_security()
        
        # Phase 13: Hold exclusive lock for atomic sequence increment
        with self.wal.exclusive_lock():
            self.check_reload()
            self._last_sequence += 1
            entry = {'seq': self._last_sequence, 'sid': self._session_id, 'txn': txn_id, 'type': 'COMMIT', 'ts': time.time()}
            
            if txn_id in self._txn_buffers:
                self._txn_buffers[txn_id].append(entry)
                self.wal.write_batch(self._txn_buffers[txn_id], sync=self.durable)
                del self._txn_buffers[txn_id]
            else:
                self.wal.log_commit(self._last_sequence, txn_id, sid=self._session_id)
            
        # Trigger auto-checkpoint check after transaction commit
        if check_auto_checkpoint:
            self._check_auto_checkpoint()

    def rollback_txn(self, txn_id: str):
        """
        Roll back a transaction.
        """
        self._init_security()
        
        # Phase 13: Hold exclusive lock for atomic sequence increment
        with self.wal.exclusive_lock():
            self.check_reload()
            self._last_sequence += 1
            
            # Restore RAM state from buffer
            if txn_id in self._txn_buffers:
                # Iterate backwards to undo changes
                for entry in reversed(self._txn_buffers[txn_id]):
                    if entry.get('type') != 'DATA':
                        continue
                
                    op = entry.get('op')
                    group_name = entry.get('g')
                    doc_id = entry.get('id')
                    before_image = entry.get('b')
                    
                    if not group_name or not doc_id:
                        continue
                        
                    if group_name not in self.data['groups']:
                        continue
                        
                    group_data = self.data['groups'][group_name]
                    
                    # Undo based on operation
                    if op == 'insert':
                        # Undo insert = delete
                        if doc_id in group_data:
                            del group_data[doc_id]
                    elif op == 'update':
                        # Undo update = restore before_image
                        if before_image:
                            group_data[doc_id] = before_image
                    elif op == 'delete':
                        # Undo delete = insert before_image
                        if before_image:
                            group_data[doc_id] = before_image
            
            # Clear buffer
            if txn_id in self._txn_buffers:
                del self._txn_buffers[txn_id]
            self._dirty = True # Mark dirty because we modified RAM (reverted)
            
            self.wal.log_rollback(self._last_sequence, txn_id, sid=self._session_id)

    def append_log(self, op: str, group_name: str, doc_id: str, data: dict, txn_id: Optional[str]=None, before_image: Optional[dict]=None):
        """
        Append an operation to the WAL.
        """
        self._init_security()
        
        # Phase 13: Hold exclusive lock to ensure atomic sequence increment 
        # and prevent collisions between processes.
        with self.wal.exclusive_lock():
            self.check_reload()
            self._last_sequence += 1
            entry = {'seq': self._last_sequence, 'sid': self._session_id, 'txn': txn_id, 'type': 'DATA', 'op': op, 'g': group_name, 'id': doc_id, 'd': data, 'b': before_image, 'ts': time.time()}
            
            if txn_id and txn_id in self._txn_buffers:
                self._txn_buffers[txn_id].append(entry)
            else:
                self.wal.append(self._last_sequence, op, group_name, doc_id, data, txn_id, before_image, sync=self.durable, sid=self._session_id)

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
        # Strategy: Scan backwards to quickly find matching entries for the group/doc
        # We scan more than 'limit' in WAL since some might not match the filters
        scan_limit = limit * 10 if group_name else limit
        
        try:
            raw_entries = self.wal.replay_reverse(scan_limit)
            for entry in raw_entries:
                if not group_name or entry.get('g') == group_name:
                    if doc_id is None or entry.get('id') == doc_id:
                        results.append(entry)
                        if len(results) >= limit:
                            break
            if results:
                return results
        except Exception:
             pass # Fallback to standard replay
             
        from collections import deque
        results_q = deque(maxlen=limit)
        def collector(entry):
            if not group_name or entry.get('g') == group_name:
                if doc_id is None or entry.get('id') == doc_id:
                    results_q.append(entry)
        self.wal.replay(0, collector)
        res = list(results_q)
        res.reverse()
        return res
