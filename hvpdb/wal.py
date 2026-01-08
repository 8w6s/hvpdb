import os
import time
import struct
import zlib
import msgpack
import zstandard as zstd
import portalocker
import uuid
from typing import Optional, List, Dict, Callable

class HVPWAL:
    """
    Write-Ahead Log Manager for HVPDB (WAL v2).
    Handles append-only logging with Transaction Support (ACID).
    
    Entry Structure (v2):
    {
        "seq": int,             # Global Sequence Number
        "txn": str,             # Transaction ID (UUID)
        "type": str,            # 'BEGIN', 'COMMIT', 'ROLLBACK', 'DATA'
        "op": str,              # 'insert', 'update', 'delete' (Only for DATA)
        "g": str,               # Group Name
        "id": str,              # Document ID
        "d": dict,              # Data (After Image)
        "b": dict,              # Before Image (For Undo/Rollback)
        "ts": float             # Timestamp
    }
    """
    def __init__(self, log_path: str, security_context, compression_level: int = 3):
        self.log_path = log_path
        self.security = security_context
        self.cctx = zstd.ZstdCompressor(level=compression_level)
        self.dctx = zstd.ZstdDecompressor()

    def write_batch(self, entries: List[dict]):
        """Writes multiple entries with a single fsync."""
        if not entries: return
        
        with open(self.log_path, 'ab') as f:
            portalocker.lock(f, portalocker.LOCK_EX)
            try:
                for entry in entries:
                    # Logic duplicated from _write_entry to avoid repeated file open/lock
                    packed = msgpack.packb(entry, use_bin_type=True)
                    compressed = self.cctx.compress(packed)
                    nonce, ciphertext = self.security.encrypt_chunk(compressed)
                    payload = nonce + ciphertext
                    crc = zlib.crc32(payload)
                    length = len(ciphertext)
                    
                    f.write(struct.pack('>I', crc))
                    f.write(struct.pack('>I', length))
                    f.write(nonce)
                    f.write(ciphertext)
                
                f.flush()
                os.fsync(f.fileno())
            finally:
                portalocker.unlock(f)

    def _write_entry(self, entry: dict):
        """Internal: Encrypts and writes a serialized entry to disk."""
        # Serialize & Compress
        packed = msgpack.packb(entry, use_bin_type=True)
        compressed = self.cctx.compress(packed)
        
        # Encrypt
        nonce, ciphertext = self.security.encrypt_chunk(compressed)
        
        # Calculate Checksum (CRC32 of Nonce + Ciphertext)
        payload = nonce + ciphertext
        crc = zlib.crc32(payload)
        
        length = len(ciphertext)
        
        # Write to disk (Atomic Append)
        with open(self.log_path, 'ab') as f:
            portalocker.lock(f, portalocker.LOCK_EX)
            try:
                # Structure: CRC(4) | LEN(4) | NONCE(12) | DATA(...)
                f.write(struct.pack('>I', crc))         # 4 bytes
                f.write(struct.pack('>I', length))      # 4 bytes
                f.write(nonce)                          # 12 bytes
                f.write(ciphertext)                     # N bytes
                f.flush()
                # fsync for durability
                os.fsync(f.fileno())
            finally:
                portalocker.unlock(f)
        
    def begin_transaction(self) -> str:
        """Starts a new transaction and returns its ID."""
        return str(uuid.uuid4())

    def log_begin(self, sequence: int, txn_id: str):
        """Logs a BEGIN transaction marker."""
        if not self.security:
            raise ValueError("WAL Security context not initialized")
        
        entry = {
            "seq": sequence,
            "txn": txn_id,
            "type": "BEGIN",
            "ts": time.time()
        }
        self._write_entry(entry)

    def log_commit(self, sequence: int, txn_id: str):
        """Logs a COMMIT transaction marker."""
        if not self.security:
            raise ValueError("WAL Security context not initialized")
            
        entry = {
            "seq": sequence,
            "txn": txn_id,
            "type": "COMMIT",
            "ts": time.time()
        }
        self._write_entry(entry)

    def log_rollback(self, sequence: int, txn_id: str):
        """Logs a ROLLBACK transaction marker."""
        if not self.security:
            raise ValueError("WAL Security context not initialized")
            
        entry = {
            "seq": sequence,
            "txn": txn_id,
            "type": "ROLLBACK",
            "ts": time.time()
        }
        self._write_entry(entry)

    def append(self, sequence: int, op: str, group: str, doc_id: str, data: dict, txn_id: str = None, before_image: dict = None):
        """Appends a single data operation to the WAL."""
        if not self.security:
            raise ValueError("WAL Security context not initialized")

        # Auto-generate txn_id if not provided (Implicit Transaction)
        # Note: In strict mode, caller should always provide txn_id.
        if not txn_id:
            txn_id = str(uuid.uuid4())

        entry = {
            "seq": sequence,
            "txn": txn_id,
            "type": "DATA",
            "op": op,
            "g": group,
            "id": doc_id,
            "d": data,
            "b": before_image,
            "ts": time.time()
        }
        self._write_entry(entry)

    def append_batch(self, sequence: int, operations: List[dict], txn_id: str):
        """Appends a batch of operations. Deprecated in favor of explicit BEGIN/COMMIT blocks but kept for compatibility."""
        # For V2, we should map this to a proper transaction block if possible, 
        # but for now we just write a 'batch' op entry.
        # Ideally, core should loop and call append() inside a txn.
        if not self.security:
            raise ValueError("WAL Security context not initialized")

        entry = {
            "seq": sequence,
            "txn": txn_id,
            "type": "DATA",
            "op": "batch",
            "d": operations,
            "ts": time.time()
        }
        self._write_entry(entry)

    def replay(self, last_sequence: int, apply_callback: Callable[[dict], None]) -> int:
        """
        Replays the log with Transaction Isolation.
        Only applies operations from COMMITTED transactions.
        Uncommitted or Rolled-back transactions are ignored (Crash Recovery).
        """
        if not os.path.exists(self.log_path):
            return 0
            
        replayed_count = 0
        
        # Buffer for uncommitted transactions: {txn_id: [entries]}
        txn_buffer: Dict[str, List[dict]] = {}
        
        # Set of committed transaction IDs (to handle cases where we see data after commit? unlikely in WAL)
        # Actually, WAL is strictly ordered. We just need to buffer until we see COMMIT.
        
        with open(self.log_path, 'rb') as f:
            portalocker.lock(f, portalocker.LOCK_SH)
            try:
                while True:
                    # 1. Read Header (CRC + Len) = 8 bytes
                    header = f.read(8)
                    if not header or len(header) < 8:
                        break
                        
                    stored_crc, length = struct.unpack('>II', header)
                    
                    # 2. Read Payload (Nonce + Data)
                    payload_len = 12 + length
                    payload = f.read(payload_len)
                    
                    if len(payload) != payload_len:
                        print("Warning: WAL truncated/corrupted entry detected.")
                        break
                        
                    # 3. Verify Checksum
                    computed_crc = zlib.crc32(payload)
                    if computed_crc != stored_crc:
                        print("Warning: WAL CRC mismatch! Entry skipped/corrupted.")
                        break
                        
                    # 4. Decrypt & Decompress
                    nonce = payload[:12]
                    ciphertext = payload[12:]
                    
                    try:
                        compressed = self.security.decrypt(nonce, ciphertext)
                        packed = self.dctx.decompress(compressed)
                        entry = msgpack.unpackb(packed, raw=False)
                        
                        seq = entry.get("seq", 0)
                        
                        # Only process if newer than snapshot
                        if seq > last_sequence:
                            entry_type = entry.get("type", "DATA") # Default to DATA for V1 compat
                            txn_id = entry.get("txn")
                            
                            # V1 Compatibility: No txn_id, treat as auto-commit
                            if not txn_id:
                                # Legacy V1 entry
                                apply_callback(entry)
                                replayed_count += 1
                                continue

                            if entry_type == "BEGIN":
                                txn_buffer[txn_id] = []
                                
                            elif entry_type == "DATA":
                                # If we missed the BEGIN (e.g. log rotation?), start buffering anyway
                                if txn_id not in txn_buffer:
                                    txn_buffer[txn_id] = []
                                txn_buffer[txn_id].append(entry)
                                
                            elif entry_type == "COMMIT":
                                if txn_id in txn_buffer:
                                    # Apply all buffered operations
                                    for buffered_entry in txn_buffer[txn_id]:
                                        apply_callback(buffered_entry)
                                        replayed_count += 1
                                    # Clear buffer
                                    del txn_buffer[txn_id]
                                    
                            elif entry_type == "ROLLBACK":
                                if txn_id in txn_buffer:
                                    # Discard buffer
                                    del txn_buffer[txn_id]
                            
                    except Exception as e:
                        print(f"Warning: WAL Entry Decryption failed: {e}")
                        break
                        
            finally:
                portalocker.unlock(f)
                
        return replayed_count

    def truncate(self):
        """Clears the WAL file (typically after a successful checkpoint)."""
        with open(self.log_path, 'wb') as f:
            portalocker.lock(f, portalocker.LOCK_EX)
            try:
                pass 
            finally:
                portalocker.unlock(f)
