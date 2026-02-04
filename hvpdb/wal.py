import os
import struct
import time
import uuid
import warnings
import zlib
from typing import Callable, Dict, List, Optional, Tuple, cast

import msgpack
import portalocker
import zstandard as zstd

from .exceptions import ConsistencyError
from .utils import default_serializer, acquire_interruptible_lock

WAL_MAGIC = b'HVPWAL'
WAL_VERSION = 2
MAX_ENTRY_SIZE = 64 * 1024 * 1024

class HVPWAL:
    """
    Write-Ahead Log (WAL) for HVPDB.
    
    Provides crash consistency by logging all operations before they 
    are applied to the main database file. Supports encryption, 
    compression, and transaction grouping.
    """

    def __init__(self, log_path: str, security_context, compression_level: int=3):
        """
        Initialize the WAL manager.
        
        Args:
            log_path: Path to the .log file.
            security_context: HVPSecurity instance for encryption.
            compression_level: Zstd compression level.
        """
        self.log_path = log_path
        self.security = security_context
        self.cctx = zstd.ZstdCompressor(level=compression_level)
        self.dctx = zstd.ZstdDecompressor()
        self._file_handle = None

    def _open_log(self):
        """Open the log file with an exclusive lock for writing."""
        if self._file_handle is None:
            self._file_handle = open(self.log_path, 'ab')
            try:
                portalocker.lock(self._file_handle, portalocker.LOCK_EX)
            except OSError:
                # Some filesystems (e.g. some Docker mounts) do not support locking
                warnings.warn('WAL file locking not supported; continuing without lock.')

    def close(self):
        """Unlock and close the log file handle."""
        if self._file_handle:
            try:
                try:
                    portalocker.unlock(self._file_handle)
                except (OSError, portalocker.exceptions.LockException):
                    warnings.warn('WAL unlock failed; continuing to close file.')
                self._file_handle.close()
            except OSError:
                warnings.warn('WAL close failed.')
            self._file_handle = None

    @staticmethod
    def read_header(log_path: str) -> Tuple[Optional[bytes], Optional[dict]]:
        """
        Read the WAL header from a log file without opening it for writing.
        
        Args:
            log_path: Path to the log file.
            
        Returns:
            Tuple of (salt, kdf_params) or (None, None) if invalid.
        """
        if not os.path.exists(log_path):
            return (None, None)
        with open(log_path, 'rb') as f:
            try:
                magic = f.read(6)
                if magic != WAL_MAGIC:
                    return (None, None)
                version = int.from_bytes(f.read(2), 'big')
                if version != WAL_VERSION:
                    return (None, None)
                salt = f.read(16)
                kdf_len = int.from_bytes(f.read(2), 'big')
                kdf_bytes = f.read(kdf_len)
                kdf_params = msgpack.unpackb(kdf_bytes)
                return (salt, kdf_params)
            except Exception as e:
                warnings.warn(f"Failed to read WAL header from {log_path}: {e}")
                return (None, None)

    def ensure_header(self, salt: bytes, kdf_params: dict):
        """
        Ensure the WAL header is written to the log file.
        
        Args:
            salt: Encryption salt.
            kdf_params: KDF parameters.
        """
        if os.path.exists(self.log_path) and os.path.getsize(self.log_path) > 0:
            return
        self._open_log()
        if self._file_handle is None:
            raise RuntimeError("Failed to open WAL file")
        f = self._file_handle
        if f.tell() == 0:
            try:
                os.chmod(self.log_path, 0o600)
            except OSError as e:
                warnings.warn(f"Failed to set WAL file permissions: {e}")
            f.write(WAL_MAGIC)
            f.write(WAL_VERSION.to_bytes(2, 'big'))
            f.write(salt)
            kdf_bytes = cast(bytes, msgpack.packb(kdf_params))
            f.write(len(kdf_bytes).to_bytes(2, 'big'))
            f.write(kdf_bytes)
            f.flush()
            os.fsync(f.fileno())

    def _write_entry(self, entry: dict, sync: bool=True):
        """
        Internal: Encrypt, compress, and write a single entry to the log.
        
        Args:
            entry: Dictionary containing the operation.
            sync: If True, performs fsync to ensure data is on disk.
        """
        if self.security:
            self.ensure_header(self.security.get_salt(), self.security.get_kdf_params())
        packed = cast(bytes, msgpack.packb(entry, use_bin_type=True, default=default_serializer))
        compressed = self.cctx.compress(packed)
        nonce, ciphertext = self.security.encrypt_chunk(compressed)
        payload = nonce + ciphertext
        crc = zlib.crc32(payload)
        length = len(ciphertext)
        self._open_log()
        if self._file_handle is None:
            raise RuntimeError("Failed to open WAL file")
        f = self._file_handle
        f.write(struct.pack('>I', crc))
        f.write(struct.pack('>I', length))
        f.write(nonce)
        f.write(ciphertext)
        if sync:
            f.flush()
            os.fsync(f.fileno())

    def write_batch(self, entries: List[dict], sync: bool=True):
        """
        Encrypt and write a batch of entries to the log in one go.
        
        Args:
            entries: List of operation dictionaries.
            sync: If True, performs fsync.
        """
        if not entries:
            return
        if not self.security:
            raise ValueError("WAL Security context not initialized")
            
        self.ensure_header(self.security.get_salt(), self.security.get_kdf_params())
        self._open_log()
        if self._file_handle is None:
            raise RuntimeError("Failed to open WAL file")
        f = self._file_handle
        for entry in entries:
            packed = cast(bytes, msgpack.packb(entry, use_bin_type=True, default=default_serializer))
            compressed = self.cctx.compress(packed)
            nonce, ciphertext = self.security.encrypt_chunk(compressed)
            payload = nonce + ciphertext
            crc = zlib.crc32(payload)
            length = len(ciphertext)
            f.write(struct.pack('>I', crc))
            f.write(struct.pack('>I', length))
            f.write(nonce)
            f.write(ciphertext)
        if sync:
            f.flush()
            os.fsync(f.fileno())

    def begin_transaction(self) -> str:
        """Generate a unique transaction ID."""
        return str(uuid.uuid4())

    def log_begin(self, sequence: int, txn_id: str):
        """Log the start of a transaction."""
        if not self.security:
            raise ValueError('WAL Security context not initialized')
        entry = {'seq': sequence, 'txn': txn_id, 'type': 'BEGIN', 'ts': time.time()}
        self._write_entry(entry)

    def log_commit(self, sequence: int, txn_id: str):
        """Log the commitment of a transaction."""
        if not self.security:
            raise ValueError('WAL Security context not initialized')
        entry = {'seq': sequence, 'txn': txn_id, 'type': 'COMMIT', 'ts': time.time()}
        self._write_entry(entry)

    def log_rollback(self, sequence: int, txn_id: str):
        """Log the rollback of a transaction."""
        if not self.security:
            raise ValueError('WAL Security context not initialized')
        entry = {'seq': sequence, 'txn': txn_id, 'type': 'ROLLBACK', 'ts': time.time()}
        self._write_entry(entry)

    def append(self, sequence: int, op: str, group: str, doc_id: str, data: dict, txn_id: Optional[str]=None, before_image: Optional[dict]=None, sync: bool=True):
        """
        Append a data operation to the log.
        
        Args:
            sequence: Monotonic operation sequence number.
            op: Operation type ('insert', 'update', 'delete').
            group: Target group name.
            doc_id: Document unique ID.
            data: New document data.
            txn_id: Optional transaction ID.
            before_image: Optional document state before the operation.
            sync: If True, performs fsync.
        """
        if not self.security:
            raise ValueError('WAL Security context not initialized')
        entry = {'seq': sequence, 'txn': txn_id, 'type': 'DATA', 'op': op, 'g': group, 'id': doc_id, 'd': data, 'b': before_image, 'ts': time.time()}
        self._write_entry(entry, sync=sync)

    def append_batch(self, sequence: int, operations: List[dict], txn_id: str):
        """Append a batch of operations to the log."""
        if not self.security:
            raise ValueError('WAL Security context not initialized')
        entry = {'seq': sequence, 'txn': txn_id, 'type': 'DATA', 'op': 'batch', 'd': operations, 'ts': time.time()}
        self._write_entry(entry)

    def replay(self, last_sequence: int, apply_callback: Callable[[dict], None]) -> int:
        """
        Replay the WAL and apply operations since the last known sequence.
        
        Args:
            last_sequence: The last sequence number successfully applied.
            apply_callback: Function to call for each replayed operation.
            
        Returns:
            Number of operations replayed.
        """
        if not os.path.exists(self.log_path):
            return 0
        if self._file_handle:
            self.close()
        replayed_count = 0
        txn_buffer: Dict[str, List[dict]] = {}
        corrupt_entries = 0
        with open(self.log_path, 'rb') as f:
            try:
                portalocker.lock(f, portalocker.LOCK_SH)
            except OSError:
                pass
            try:
                header_magic = f.read(6)
                if header_magic == WAL_MAGIC:
                    version = int.from_bytes(f.read(2), 'big')
                    if version != WAL_VERSION:
                        raise ConsistencyError(f'WAL Version Mismatch: Expected {WAL_VERSION}, got {version}.')
                    f.read(16)
                    kdf_len = int.from_bytes(f.read(2), 'big')
                    f.read(kdf_len)
                else:
                    f.seek(0)
                while True:
                    header = f.read(8)
                    if not header or len(header) < 8:
                        break
                    stored_crc, length = struct.unpack('>II', header)
                    if length == 0 or length > MAX_ENTRY_SIZE:
                        raise ConsistencyError(f'WAL corruption detected: Entry size {length} invalid.')
                    payload_len = 12 + length
                    payload = f.read(payload_len)
                    if len(payload) != payload_len:
                        warnings.warn('WAL truncated at end. Stopping replay.')
                        break
                    computed_crc = zlib.crc32(payload) & 4294967295
                    stored_crc &= 4294967295
                    if computed_crc != stored_crc:
                        warnings.warn('WAL CRC mismatch (corruption or partial write). Stopping replay.')
                        corrupt_entries += 1
                        if corrupt_entries >= 3:
                            break
                        continue
                    nonce = payload[:12]
                    ciphertext = payload[12:]
                    try:
                        if hasattr(self.security, 'decrypt_chunk'):
                            compressed = self.security.decrypt_chunk(nonce, ciphertext)
                        else:
                            compressed = self.security.decrypt(nonce, ciphertext)
                        packed = self.dctx.decompress(compressed)
                        entry = msgpack.unpackb(packed, raw=False)
                        seq = entry.get('seq', 0)
                        if seq > last_sequence:
                            entry_type = entry.get('type', 'DATA')
                            txn_id = entry.get('txn')
                            if not txn_id:
                                apply_callback(entry)
                                replayed_count += 1
                                continue
                            if entry_type == 'BEGIN':
                                txn_buffer[txn_id] = []
                            elif entry_type == 'DATA':
                                if txn_id not in txn_buffer:
                                    txn_buffer[txn_id] = []
                                txn_buffer[txn_id].append(entry)
                            elif entry_type == 'COMMIT':
                                if txn_id in txn_buffer:
                                    for buffered_entry in txn_buffer[txn_id]:
                                        apply_callback(buffered_entry)
                                        replayed_count += 1
                                    del txn_buffer[txn_id]
                            elif entry_type == 'ROLLBACK':
                                if txn_id in txn_buffer:
                                    del txn_buffer[txn_id]
                    except Exception as e:
                        warnings.warn(f'WAL Entry Decryption failed: {e}')
                        corrupt_entries += 1
                        if corrupt_entries >= 3:
                            break
                        continue
            finally:
                try:
                    portalocker.unlock(f)
                except OSError:
                    pass
        return replayed_count

    def truncate(self):
        """Clear the WAL file and reset it with a fresh header."""
        if self._file_handle:
            f = self._file_handle
            f.seek(0)
            f.truncate(0)
            self._write_header_to_handle(f)
        else:
            with open(self.log_path, 'a+b') as f:
                try:
                    acquire_interruptible_lock(f)
                except (OSError, portalocker.LockException):
                    pass
                try:
                    f.seek(0)
                    f.truncate(0)
                    self._write_header_to_handle(f)
                finally:
                    try:
                        portalocker.unlock(f)
                    except OSError:
                        pass
            return

    def _write_header_to_handle(self, f):
        """Internal: Write the WAL header to an open file handle."""
        if os.name != 'nt':  # Skip chmod on Windows to avoid noisy warnings
            try:
                os.chmod(self.log_path, 0o600)
            except OSError as e:
                warnings.warn(f"Failed to set WAL file permissions: {e}")
        if self.security:
            f.write(WAL_MAGIC)
            f.write(WAL_VERSION.to_bytes(2, 'big'))
            f.write(self.security.get_salt())
            kdf_bytes = cast(bytes, msgpack.packb(self.security.get_kdf_params()))
            f.write(len(kdf_bytes).to_bytes(2, 'big'))
            f.write(kdf_bytes)
        f.flush()
        os.fsync(f.fileno())
