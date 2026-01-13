import os
import time
import struct
import zlib
import msgpack
import zstandard as zstd
import portalocker
import uuid
import stat
from typing import Optional, List, Dict, Callable, Tuple
import warnings
WAL_MAGIC = b'HVPWAL'
WAL_VERSION = 2
MAX_ENTRY_SIZE = 64 * 1024 * 1024

class HVPWAL:

    def __init__(self, log_path: str, security_context, compression_level: int=3):
        self.log_path = log_path
        self.security = security_context
        self.cctx = zstd.ZstdCompressor(level=compression_level)
        self.dctx = zstd.ZstdDecompressor()
        self._file_handle = None

    def _open_log(self):
        if self._file_handle is None:
            self._file_handle = open(self.log_path, 'ab')
            try:
                portalocker.lock(self._file_handle, portalocker.LOCK_EX)
            except OSError:
                # Some filesystems (e.g. some Docker mounts) do not support locking
                pass

    def close(self):
        if self._file_handle:
            try:
                portalocker.unlock(self._file_handle)
                self._file_handle.close()
            except:
                pass
            self._file_handle = None

    @staticmethod
    def read_header(log_path: str) -> Tuple[Optional[bytes], Optional[dict]]:
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
            except Exception:
                return (None, None)

    def ensure_header(self, salt: bytes, kdf_params: dict):
        if os.path.exists(self.log_path) and os.path.getsize(self.log_path) > 0:
            return
        self._open_log()
        f = self._file_handle
        if f.tell() == 0:
            try:
                os.chmod(self.log_path, 384)
            except OSError:
                pass
            f.write(WAL_MAGIC)
            f.write(WAL_VERSION.to_bytes(2, 'big'))
            f.write(salt)
            kdf_bytes = msgpack.packb(kdf_params)
            f.write(len(kdf_bytes).to_bytes(2, 'big'))
            f.write(kdf_bytes)
            f.flush()
            os.fsync(f.fileno())

    def _write_entry(self, entry: dict, sync: bool=True):
        if self.security:
            self.ensure_header(self.security.get_salt(), self.security.get_kdf_params())
        packed = msgpack.packb(entry, use_bin_type=True)
        compressed = self.cctx.compress(packed)
        nonce, ciphertext = self.security.encrypt_chunk(compressed)
        payload = nonce + ciphertext
        crc = zlib.crc32(payload)
        length = len(ciphertext)
        self._open_log()
        f = self._file_handle
        f.write(struct.pack('>I', crc))
        f.write(struct.pack('>I', length))
        f.write(nonce)
        f.write(ciphertext)
        if sync:
            f.flush()
            os.fsync(f.fileno())

    def write_batch(self, entries: List[dict], sync: bool=True):
        if not entries:
            return
        if self.security:
            self.ensure_header(self.security.get_salt(), self.security.get_kdf_params())
        self._open_log()
        f = self._file_handle
        for entry in entries:
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
        if sync:
            f.flush()
            os.fsync(f.fileno())

    def begin_transaction(self) -> str:
        return str(uuid.uuid4())

    def log_begin(self, sequence: int, txn_id: str):
        if not self.security:
            raise ValueError('WAL Security context not initialized')
        entry = {'seq': sequence, 'txn': txn_id, 'type': 'BEGIN', 'ts': time.time()}
        self._write_entry(entry)

    def log_commit(self, sequence: int, txn_id: str):
        if not self.security:
            raise ValueError('WAL Security context not initialized')
        entry = {'seq': sequence, 'txn': txn_id, 'type': 'COMMIT', 'ts': time.time()}
        self._write_entry(entry)

    def log_rollback(self, sequence: int, txn_id: str):
        if not self.security:
            raise ValueError('WAL Security context not initialized')
        entry = {'seq': sequence, 'txn': txn_id, 'type': 'ROLLBACK', 'ts': time.time()}
        self._write_entry(entry)

    def append(self, sequence: int, op: str, group: str, doc_id: str, data: dict, txn_id: str=None, before_image: dict=None, sync: bool=True):
        if not self.security:
            raise ValueError('WAL Security context not initialized')
        if not txn_id:
            txn_id = str(uuid.uuid4())
        entry = {'seq': sequence, 'txn': txn_id, 'type': 'DATA', 'op': op, 'g': group, 'id': doc_id, 'd': data, 'b': before_image, 'ts': time.time()}
        self._write_entry(entry, sync=sync)

    def append_batch(self, sequence: int, operations: List[dict], txn_id: str):
        if not self.security:
            raise ValueError('WAL Security context not initialized')
        entry = {'seq': sequence, 'txn': txn_id, 'type': 'DATA', 'op': 'batch', 'd': operations, 'ts': time.time()}
        self._write_entry(entry)

    def replay(self, last_sequence: int, apply_callback: Callable[[dict], None]) -> int:
        if not os.path.exists(self.log_path):
            return 0
        if self._file_handle:
            self.close()
        replayed_count = 0
        txn_buffer: Dict[str, List[dict]] = {}
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
                        warnings.warn(f'WAL Version Mismatch: Expected {WAL_VERSION}, got {version}. Treating as corrupt/legacy.')
                        return 0
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
                        warnings.warn(f'WAL corruption detected: Entry size {length} invalid. Stopping replay.')
                        break
                    payload_len = 12 + length
                    payload = f.read(payload_len)
                    if len(payload) != payload_len:
                        warnings.warn('WAL truncated at end. Stopping replay.')
                        break
                    computed_crc = zlib.crc32(payload) & 4294967295
                    stored_crc &= 4294967295
                    if computed_crc != stored_crc:
                        warnings.warn('WAL CRC mismatch (corruption or partial write). Stopping replay.')
                        break
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
                        break
            finally:
                try:
                    portalocker.unlock(f)
                except OSError:
                    pass
        return replayed_count

    def truncate(self):
        if self._file_handle:
            f = self._file_handle
            f.seek(0)
            f.truncate(0)
        else:
            with open(self.log_path, 'a+b') as f:
                try:
                    portalocker.lock(f, portalocker.LOCK_EX)
                except OSError:
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
        self._write_header_to_handle(f)

    def _write_header_to_handle(self, f):
        try:
            os.chmod(self.log_path, 384)
        except OSError:
            pass
        if self.security:
            f.write(WAL_MAGIC)
            f.write(WAL_VERSION.to_bytes(2, 'big'))
            f.write(self.security.get_salt())
            kdf_bytes = msgpack.packb(self.security.get_kdf_params())
            f.write(len(kdf_bytes).to_bytes(2, 'big'))
            f.write(kdf_bytes)
            f.flush()
            os.fsync(f.fileno())
