import struct
import zstandard as zstd
import msgpack
import os
import portalocker
import time
import uuid
from typing import Optional, Dict, Any
from .security import HVPSecurity
from .uri import HVPURI
from .wal import HVPWAL
from .concurrency import HVPLockManager

# Magic Header for File Recognition
HEADER = b'HVPDB' 
VERSION = 2

class HVPStorage:
    """
    Manages the physical storage layer of HVPDB.
    Handles File I/O, Compression (Zstd), Encryption (AES-GCM), and Concurrency.
    """

    def __init__(self, filepath_or_uri: str, password: str = None):
        self.connection_info = None
        self.filepath = ""
        self.password = password

        if filepath_or_uri.startswith("hvp://"):
            self.connection_info = HVPURI.parse(filepath_or_uri)
            self.filepath = self.connection_info.cluster
            self.password = self.connection_info.password
            
            # Normalize extension
            if not self.filepath.endswith(".hvp"):
                self.filepath += ".hvp"
        else:
            self.filepath = filepath_or_uri
            # Smart extension handling
            if not self.filepath.endswith(".hvp") and not self.filepath.endswith(".hvdb"):
                self.filepath += ".hvp"

        if not self.password:
            raise ValueError("Authentication Error: Password is required for encryption.")

        self.log_path = self.filepath + ".log"
        self.security: Optional[HVPSecurity] = None
        self.data: Dict[str, Any] = {"groups": {}} 
        self._dirty = False 
        self._last_sequence = 0 # Global Monotonic Sequence Number
        
        # Initialize Zstandard Contexts
        self.cctx = zstd.ZstdCompressor(level=3) 
        self.dctx = zstd.ZstdDecompressor()
        
        # Initialize WAL Manager
        self.wal = HVPWAL(self.log_path, self.security)
        
        # Initialize Lock Manager
        self.lock_manager = HVPLockManager(self.filepath)
        
        # Transaction Buffer: {txn_id: [entries]}
        self._txn_buffers = {}

    def _init_security(self, salt: Optional[bytes] = None, kdf_params: Optional[dict] = None):
        """Initializes the security layer (key derivation)."""
        if not self.security:
            self.security = HVPSecurity(self.password, salt, kdf_params)
            # Update WAL security context
            self.wal.security = self.security

    def refresh(self):
        """Reloads data from disk to pick up external changes."""
        if self._dirty:
            raise RuntimeError("Cannot refresh with unsaved changes. Commit first.")
        
        # Check if file exists/changed before reloading?
        # For now, unconditional reload is safer to ensure consistency.
        self.load()

    def load(self):
        """Loads and decrypts data from the storage file."""
        # Acquire Shared Lock for Reading
        with self.lock_manager.reader_lock():
            if not os.path.exists(self.filepath):
                # Cold start: Return empty state
                self.data = {"groups": {}}
                
                # CRITICAL FIX: Persist Header/Salt immediately!
                self._init_security()
                # We can't save here inside a reader lock if save() requires writer lock.
                # But cold start is special. We upgrade to save.
                # However, recursion is tricky. Let's just init security and defer save or force it.
                # Actually, save() calls writer_lock, which is separate file. It's fine.
                # But to be safe, we should release reader lock before saving?
                # No, reader lock is on .lock file (SH). Writer lock is on .writelock (EX).
                # Save also needs .lock (EX) for swap. SH and EX conflict!
                # So we must NOT call self.save() inside self.load() if locks conflict.
                return

            with open(self.filepath, 'rb') as f:
                # portalocker.lock(f, portalocker.LOCK_SH) # Handled by HVPLockManager
                try:
                    header = f.read(5)
                    if header != HEADER: 
                        raise ValueError("File Error: Invalid HVPDB Header. Is this a database file?")
                    
                    version = int.from_bytes(f.read(2), 'big')
                    
                    if version == 1:
                        # Legacy Format (v1)
                        salt = f.read(16)
                        nonce = f.read(12)
                        ciphertext = f.read()
                        
                        self._init_security(salt)
                        # v1: No AAD
                        compressed_data = self.security.decrypt(nonce, ciphertext)
                    
                    elif version == 2:
                        # Secure Format (v2) - With KDF Params & AAD
                        salt = f.read(16)
                        kdf_len = int.from_bytes(f.read(2), 'big')
                        kdf_bytes = f.read(kdf_len)
                        kdf_params = msgpack.unpackb(kdf_bytes)
                        
                        nonce = f.read(12)
                        ciphertext = f.read()
                        
                        self._init_security(salt, kdf_params)
                        
                        # Reconstruct AAD for integrity check
                        aad = HEADER + version.to_bytes(2, 'big') + salt + kdf_len.to_bytes(2, 'big') + kdf_bytes
                        compressed_data = self.security.decrypt(nonce, ciphertext, associated_data=aad)
                        
                    else:
                        raise ValueError(f"Unsupported DB Version: {version}")

                    packed_data = self.dctx.decompress(compressed_data)
                    self.data = msgpack.unpackb(packed_data, raw=False)
                    
                    # Restore sequence number from snapshot data if available, else 0
                    self._last_sequence = self.data.get("seq", 0)
                except Exception as e:
                    raise ValueError(f"Decryption Failed: Incorrect password, corrupted file, or tampering detected. ({e})")

        # REPLAY WAL (Source of Truth)
        # Apply any transactions in WAL that are newer than the snapshot (seq > _last_sequence)
        self._replay_wal()

    def _replay_wal(self):
        """Replays WAL entries to restore state to the latest point in time."""
        replayed_count = self.wal.replay(self._last_sequence, self._apply_entry)
        
        if replayed_count > 0:
            self._dirty = True
            # Optional: Checkpoint if replay log is too large?
            # For now, we leave it dirty and let user commit.

    def _apply_entry(self, entry: dict):
        """Applies a single WAL entry to the in-memory state."""
        op = entry.get("op")
        group_name = entry.get("g")
        doc_id = entry.get("id")
        data = entry.get("d")
        seq = entry.get("seq", 0)
        
        # Update sequence to match WAL
        if seq > self._last_sequence:
            self._last_sequence = seq
        
        if not group_name: return
        
        if group_name not in self.data["groups"]:
            self.data["groups"][group_name] = {}
            
        group_data = self.data["groups"][group_name]
        
        if op == "insert" or op == "update":
            if doc_id and data:
                group_data[doc_id] = data
        elif op == "delete":
            if doc_id and doc_id in group_data:
                del group_data[doc_id]
        else:
            # Legacy WAL format support (v1 had no 'op', just data insert/update)
            # If no op, assume insert/update if data exists
            if data and "_id" in data:
                 group_data[data["_id"]] = data

    def save(self):
        """Encrypts and commits the current state to disk (Atomic Write)."""
        # 1. Acquire Writer Lock (Serialize writers, allow readers)
        with self.lock_manager.writer_lock():
            self._init_security()
            
            # Persist sequence number in snapshot
            self.data["seq"] = self._last_sequence
            
            packed_data = msgpack.packb(self.data, use_bin_type=True)
            compressed_data = self.cctx.compress(packed_data)
            
            # Prepare Header Metadata (v2)
            salt = self.security.get_salt()
            kdf_params = self.security.get_kdf_params()
            kdf_bytes = msgpack.packb(kdf_params)
            kdf_len = len(kdf_bytes)
            
            # Construct AAD
            aad = HEADER + VERSION.to_bytes(2, 'big') + salt + kdf_len.to_bytes(2, 'big') + kdf_bytes
            
            # Encrypt with AAD
            nonce, ciphertext = self.security.encrypt(compressed_data, associated_data=aad)
            
            temp_path = self.filepath + ".tmp"
            with open(temp_path, 'wb') as f:
                # We already hold writer_lock, but locking temp file is good practice for extra safety
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
                    os.fsync(f.fileno()) 
                finally:
                    portalocker.unlock(f)
                
            # 2. Acquire Critical Swap Lock (Block readers briefly)
            with self.lock_manager.critical_swap_lock():
                # Robust replace for Windows
                retries = 5
                while retries > 0:
                    try:
                        if os.path.exists(self.filepath):
                            os.replace(temp_path, self.filepath)
                        else:
                            os.rename(temp_path, self.filepath)
                        break
                    except PermissionError:
                        retries -= 1
                        if retries == 0:
                            raise
                        time.sleep(0.1)
                    except OSError:
                        # Other OS errors
                        retries -= 1
                        if retries == 0:
                            raise
                        time.sleep(0.1)
                
                # CHECKPOINT COMPLETE: Truncate WAL
                # We truncate while holding the lock so readers don't read partial WAL + new DB
                self.wal.truncate()
            
            self._dirty = False

    def begin_txn(self) -> str:
        """Starts a new transaction and logs BEGIN."""
        txn_id = self.wal.begin_transaction()
        self._init_security()
        self._last_sequence += 1
        
        # Start Buffering
        self._txn_buffers[txn_id] = []
        
        # Log BEGIN (Buffered)
        entry = {
            "seq": self._last_sequence,
            "txn": txn_id,
            "type": "BEGIN",
            "ts": time.time()
        }
        self._txn_buffers[txn_id].append(entry)
        
        return txn_id

    def commit_txn(self, txn_id: str):
        """Commits a transaction."""
        self._init_security()
        self._last_sequence += 1
        
        # Log COMMIT (Buffered)
        entry = {
            "seq": self._last_sequence,
            "txn": txn_id,
            "type": "COMMIT",
            "ts": time.time()
        }
        
        if txn_id in self._txn_buffers:
            self._txn_buffers[txn_id].append(entry)
            # FLUSH BUFFER TO DISK (Atomic Batch Write)
            self.wal.write_batch(self._txn_buffers[txn_id])
            del self._txn_buffers[txn_id]
        else:
            # Fallback if somehow not buffered (should not happen)
            self.wal.log_commit(self._last_sequence, txn_id)

    def rollback_txn(self, txn_id: str):
        """Rolls back a transaction."""
        self._init_security()
        self._last_sequence += 1
        
        # Discard Buffer
        if txn_id in self._txn_buffers:
            del self._txn_buffers[txn_id]
        
        # Log ROLLBACK (Directly to WAL, though technically optional if we discard buffer)
        # But good for audit.
        self.wal.log_rollback(self._last_sequence, txn_id)

    def append_log(self, op: str, group_name: str, doc_id: str, data: dict, txn_id: str = None, before_image: dict = None):
        """Appends a transaction to the Write-Ahead Log (WAL)."""
        self._init_security()
        
        # Increment sequence
        self._last_sequence += 1
        
        # Prepare Entry
        if not txn_id:
            txn_id = str(uuid.uuid4())
            
        entry = {
            "seq": self._last_sequence,
            "txn": txn_id,
            "type": "DATA",
            "op": op,
            "g": group_name,
            "id": doc_id,
            "d": data,
            "b": before_image,
            "ts": time.time()
        }
        
        # Check if buffering
        if txn_id in self._txn_buffers:
            self._txn_buffers[txn_id].append(entry)
        else:
            # Direct Write (Slow Path)
            # Delegate to WAL Manager
            self.wal.append(self._last_sequence, op, group_name, doc_id, data, txn_id, before_image)

    def append_batch_log(self, operations: list, txn_id: str = None):
        """Appends a batch of transactions to the WAL."""
        self._init_security()
        
        # Use explicit transaction if provided, else create one
        is_implicit = False
        if not txn_id:
            txn_id = self.begin_txn()
            is_implicit = True
            
        # Log BEGIN if implicit (if explicit, caller handled BEGIN)
        if is_implicit:
            self._last_sequence += 1
            self.wal.log_begin(self._last_sequence, txn_id)
        
        for op_data in operations:
            self._last_sequence += 1
            # op_data should be dict with keys: op, g, id, d, b
            self.wal.append(
                self._last_sequence, 
                op_data.get("op"), 
                op_data.get("g"), 
                op_data.get("id"), 
                op_data.get("d"), 
                txn_id,
                op_data.get("b")
            )
            
        # Log COMMIT if implicit
        if is_implicit:
            self._last_sequence += 1
            self.wal.log_commit(self._last_sequence, txn_id)

    def read_audit_log(self, group_name: str, doc_id: str = None, limit: int = 100) -> list:
        """Reads and decrypts the WAL to reconstruct history (Legacy Audit Support)."""
        # Note: This is inefficient for audit as it scans the whole WAL.
        # Future: Move audit to a separate indexed log.
        results = []
        
        def collector(entry):
            if entry.get("g") == group_name:
                if doc_id is None or entry.get("id") == doc_id:
                    results.append(entry)
                    
        self.wal.replay(0, collector) # Replay from beginning
        
        return sorted(results, key=lambda x: x.get("ts", 0), reverse=True)[:limit]
