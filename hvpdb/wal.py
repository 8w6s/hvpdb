import hashlib
import os
import struct
import time
import uuid
import threading
import random
import contextlib
import warnings
import zlib
from typing import Callable, Dict, List, Optional, Tuple, cast

import msgpack
import portalocker
import zstandard as zstd

from .exceptions import ConsistencyError
from .utils import default_serializer, acquire_interruptible_lock

WAL_MAGIC = b'HVPWAL'
WAL_VERSION = 4
ENTRY_MAGIC = b'\x17'
MAX_ENTRY_SIZE = 64 * 1024 * 1024

CHECKSUM_CRC32 = 0
CHECKSUM_SHA256 = 1

class HVPWAL:
    """
    Write-Ahead Log (WAL) with robust locking and re-entrancy support.
    """

    def __init__(self, log_path: str, security_context, compression_level: int=3, checksum_type: int=CHECKSUM_CRC32):
        self.log_path = log_path
        self.security = security_context
        self.cctx = zstd.ZstdCompressor(level=compression_level)
        self.dctx = zstd.ZstdDecompressor()
        self._file_handle = None
        self._lock = threading.RLock()
        self._lock_count = 0
        self.checksum_type = checksum_type
        self._header_checksum_type = CHECKSUM_CRC32

    def _open_log_internal(self, mode: str = 'a+b'):
        """Internal: Open and lock the log with retries and non-blocking strategy."""
    def _open_log_internal(self):
        with self._lock:
            if self._file_handle is None or self._file_handle.closed:
                # Mode a+b allows reading and appending.
                # It's better than 'rb' followed by 'ab' because it stays open.
                for attempt in range(300):
                    try:
                        f = open(self.log_path, 'a+b')
                        # On Windows, portalocker.lock with LOCK_EX handles both locking and sharing correctly
                        portalocker.lock(f, portalocker.LOCK_EX | portalocker.LOCK_NB)
                        self._file_handle = f
                        return
                    except (PermissionError, portalocker.exceptions.LockException, OSError):
                        try:
                            if 'f' in locals() and f: f.close()
                        except: pass
                        time.sleep(0.01 + random.random() * 0.04)
                raise OSError(f"Could not acquire exclusive lock on WAL (TIMEOUT after 300 attempts): {self.log_path}")

    @contextlib.contextmanager
    def exclusive_lock(self):
        """Re-entrant exclusive lock."""
        with self._lock:
            self._lock_count += 1
            try:
                self._open_log_internal()
                yield self._file_handle
            finally:
                self._lock_count -= 1
                # DO NOT close here. Let close() handle it. 
                # Keeping it open avoids race conditions on Windows.

    def close(self):
        with self._lock:
            self._lock_count = 0
            self._close_internal()

    def _close_internal(self):
        if self._file_handle:
            try:
                try:
                    portalocker.unlock(self._file_handle)
                except Exception as unlock_err:
                    warnings.warn(f"Failed to unlock WAL file: {unlock_err}")
                self._file_handle.close()
            except Exception as close_err:
                warnings.warn(f"Failed to close WAL file handle: {close_err}")
            finally:
                self._file_handle = None

    @staticmethod
    def read_header(path: str) -> Tuple[Optional[bytes], Optional[dict], int]:
        if not os.path.exists(path) or os.path.getsize(path) == 0: return (None, None, CHECKSUM_CRC32)
        for _ in range(10):
            try:
                with open(path, 'rb') as f:
                    try:
                        portalocker.lock(f, portalocker.LOCK_SH | portalocker.LOCK_NB)
                    except (OSError, portalocker.exceptions.LockException):
                        # Lock failed, but we can still read - it's a shared lock attempt
                        pass
                    magic = f.read(len(WAL_MAGIC))
                    if magic != WAL_MAGIC: return (None, None, CHECKSUM_CRC32)
                    version = int.from_bytes(f.read(2), 'big')
                    salt = f.read(16)
                    k_len_bytes = f.read(2)
                    if len(k_len_bytes) < 2: return (None, None, CHECKSUM_CRC32)
                    k_len = int.from_bytes(k_len_bytes, 'big')
                    k_bytes = f.read(k_len)
                    kp = msgpack.unpackb(k_bytes)
                    ctype = CHECKSUM_CRC32
                    if version >= 4:
                        ctype_raw = f.read(1)
                        if ctype_raw: ctype = int.from_bytes(ctype_raw, 'big')
                    return (salt, kp, ctype)
            except (PermissionError, portalocker.exceptions.LockException): time.sleep(0.05)
            except Exception: break
        return (None, None, CHECKSUM_CRC32)

    def ensure_header(self, salt: bytes, kdf_params: dict):
        """Ensure WAL has a proper header with the given salt/params."""
        with self.exclusive_lock() as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            if size > 0:
                # Header exists, verify salt consistency
                f.seek(0)
                magic = f.read(len(WAL_MAGIC))
                if magic != WAL_MAGIC:
                    print(f"DEBUG: WAL Header Magic Mismatch! Truncating. Expected {WAL_MAGIC}, got {magic}")
                    f.seek(0); f.truncate(0)
                else:
                    v = int.from_bytes(f.read(2), 'big')
                    s = f.read(16)
                    if s != salt:
                        print(f"DEBUG: WAL Salt Mismatch! Truncating. File has {s.hex()}, new is {salt.hex()}")
                        f.seek(0); f.truncate(0)
                    else:
                        # print("DEBUG: WAL Header matches. Keeping file.")
                        return
            
            # Write new header
            if os.name != 'nt':
                try: os.chmod(self.log_path, 0o600)
                except: pass
            f.write(WAL_MAGIC)
            f.write(WAL_VERSION.to_bytes(2, 'big'))
            f.write(salt)
            kb = msgpack.packb(kdf_params)
            f.write(len(kb).to_bytes(2, 'big'))
            f.write(kb)
            f.write(self.checksum_type.to_bytes(1, 'big'))
            self._header_checksum_type = self.checksum_type
            f.flush()
            os.fsync(f.fileno())

    def _write_entry(self, entry: dict, sync: bool=True):
        if not self.security:
            raise RuntimeError("WAL security context not initialized. Cannot write encrypted entry.")
        with self.exclusive_lock() as f:
            self.ensure_header(self.security.get_salt(), self.security.get_kdf_params())
            
            packed = msgpack.packb(entry, use_bin_type=True, default=default_serializer)
            compressed = self.cctx.compress(packed)
            nonce = os.urandom(8) + struct.pack('>I', entry.get('seq', 0))
            
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
            ct = AESGCM(self.security._key).encrypt(nonce, compressed, None)
            l = len(ct)
            payload = ENTRY_MAGIC + struct.pack('>I', l) + nonce + ct
            
            if self._header_checksum_type == CHECKSUM_SHA256: 
                cs = hashlib.sha256(payload).digest()
            else: 
                cs = struct.pack('>I', zlib.crc32(payload))
                
            f.seek(0, os.SEEK_END)
            f.write(ENTRY_MAGIC); f.write(cs); f.write(struct.pack('>I', l)); f.write(nonce); f.write(ct); f.write(struct.pack('>I', l))
            if sync: 
                f.flush(); os.fsync(f.fileno())

    def write_batch(self, entries: List[dict], sync: bool=True):
        if not entries: return
        if not self.security:
            raise RuntimeError("WAL security context not initialized. Cannot write encrypted batch.")
        with self.exclusive_lock() as f:
            self.ensure_header(self.security.get_salt(), self.security.get_kdf_params())
            
            f.seek(0, os.SEEK_END)
            for entry in entries:
                pk = msgpack.packb(entry, use_bin_type=True, default=default_serializer)
                ct = self.cctx.compress(pk)
                nonce = os.urandom(8) + struct.pack('>I', entry.get('seq', 0))
                from cryptography.hazmat.primitives.ciphers.aead import AESGCM
                ctx = AESGCM(self.security._key).encrypt(nonce, ct, None)
                l = len(ctx)
                payload = ENTRY_MAGIC + struct.pack('>I', l) + nonce + ctx
                if self._header_checksum_type == CHECKSUM_SHA256: 
                    cs = hashlib.sha256(payload).digest()
                else: 
                    cs = struct.pack('>I', zlib.crc32(payload))
                f.write(ENTRY_MAGIC); f.write(cs); f.write(struct.pack('>I', l)); f.write(nonce); f.write(ctx); f.write(struct.pack('>I', l))
            if sync: 
                f.flush(); os.fsync(f.fileno())

    def begin_transaction(self) -> str: return str(uuid.uuid4())
    def log_begin(self, s: int, tx: str, sid: str=None): self._write_entry({'seq':s,'sid':sid,'txn':tx,'type':'BEGIN','ts':time.time()})
    def log_commit(self, s: int, tx: str, sid: str=None): self._write_entry({'seq':s,'sid':sid,'txn':tx,'type':'COMMIT','ts':time.time()})
    def log_rollback(self, s: int, tx: str, sid: str=None): self._write_entry({'seq':s,'sid':sid,'txn':tx,'type':'ROLLBACK','ts':time.time()})
    def append(self, s: int, op: str, g: str, id: str, d: dict, tx: str=None, b: dict=None, sync: bool=True, sid: str=None):
        self._write_entry({'seq':s,'sid':sid,'txn':tx,'type':'DATA','op':op,'g':g,'id':id,'d':d,'b':b,'ts':time.time()}, sync=sync)

    def replay(self, last_sequence: int, apply_callback: Callable[[dict], None]) -> int:
        if not os.path.exists(self.log_path): return 0
        replayed_count, txn_buffer, f_handle, own_handle = 0, {}, None, False
        try:
            if self._file_handle:
                f_handle = self._file_handle
                f_handle.seek(0)
            else:
                for _ in range(20):
                    try:
                        f_handle = open(self.log_path, 'rb')
                        try: portalocker.lock(f_handle, portalocker.LOCK_SH | portalocker.LOCK_NB)
                        except: pass
                        own_handle = True; break
                    except: time.sleep(0.1)
                if not f_handle: return 0

            magic = f_handle.read(6)
            if magic != WAL_MAGIC: return 0
            version = int.from_bytes(f_handle.read(2), 'big')
            f_handle.read(16); kl = int.from_bytes(f_handle.read(2), 'big'); f_handle.read(kl)
            ct = CHECKSUM_CRC32
            if version >= 4:
                ctr = f_handle.read(1)
                if ctr: ct = int.from_bytes(ctr, 'big')
            cl = 32 if ct == CHECKSUM_SHA256 else 4
            
            while True:
                m = f_handle.read(1)
                if not m or m != ENTRY_MAGIC: break
                csr, lr = f_handle.read(cl), f_handle.read(4)
                if len(lr) < 4: break
                dl = struct.unpack('>I', lr)[0]; nonce = f_handle.read(12); ctx = f_handle.read(dl); f_handle.read(4)
                if len(ctx) < dl: break
                valid = (hashlib.sha256(m+lr+nonce+ctx).digest() == csr) if ct == CHECKSUM_SHA256 else (struct.pack('>I', zlib.crc32(m+lr+nonce+ctx)) == csr)
                if not valid: break
                try:
                    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
                    decrypted = AESGCM(self.security._key).decrypt(nonce, ctx, None)
                    entry = msgpack.unpackb(decrypted, raw=False)
                    if entry.get('seq', 0) > last_sequence:
                        et, tx = entry.get('type', 'DATA'), entry.get('txn')
                        if not tx: apply_callback(entry); replayed_count += 1
                        elif et == 'BEGIN': txn_buffer[tx] = []
                        elif et == 'DATA':
                            if tx not in txn_buffer: txn_buffer[tx] = []
                            txn_buffer[tx].append(entry)
                        elif et == 'COMMIT':
                            if tx in txn_buffer:
                                for be in txn_buffer[tx]: apply_callback(be); replayed_count += 1
                                del txn_buffer[tx]
                        elif et == 'ROLLBACK':
                            if tx in txn_buffer: del txn_buffer[tx]
                except Exception as replay_err:
                    warnings.warn(f"Failed to replay WAL entry: {replay_err}")
                    break
        finally:
            if own_handle and f_handle:
                try:
                    portalocker.unlock(f_handle)
                    f_handle.close()
                except Exception as cleanup_err:
                    warnings.warn(f"Failed to cleanup WAL file handle during replay: {cleanup_err}")
        return replayed_count

    def truncate(self):
        with self.exclusive_lock() as f:
            f.seek(0); f.truncate(0); self._write_header_to_handle(f)

    def _write_header_to_handle(self, f):
        if os.name != 'nt':
            try:
                os.chmod(self.log_path, 0o600)
            except OSError as chmod_err:
                warnings.warn(f"Failed to set WAL file permissions: {chmod_err}")
        if self.security:
            f.write(WAL_MAGIC); f.write(WAL_VERSION.to_bytes(2, 'big')); f.write(self.security.get_salt())
            kb = msgpack.packb(self.security.get_kdf_params())
            f.write(len(kb).to_bytes(2, 'big')); f.write(kb)
            f.write(self.checksum_type.to_bytes(1, 'big'))
            self._header_checksum_type = self.checksum_type
        f.flush(); os.fsync(f.fileno())

    def replay_reverse(self, limit: int=100) -> List[dict]:
        results = []
        salt, kp, ct = self.read_header(self.log_path)
        if salt is None:
            return []
            
        # Re-calculate header size exactly by reading it
        header_offset = 0
        try:
            with open(self.log_path, 'rb') as f:
                f.read(len(WAL_MAGIC)) # MAGIC
                f.read(2) # VERSION
                f.read(16) # SALT
                k_len_bytes = f.read(2)
                if len(k_len_bytes) == 2:
                    k_len = int.from_bytes(k_len_bytes, 'big')
                    f.read(k_len)
                    header_offset = f.tell()
                    # Version 4+ has checksum type byte
                    # Check version
                    f.seek(len(WAL_MAGIC))
                    v = int.from_bytes(f.read(2), 'big')
                    if v >= 4:
                        header_offset += 1
        except Exception as offset_err:
            warnings.warn(f"Failed to calculate WAL header offset for reverse replay: {offset_err}")
            return []
            
        cl = 32 if ct == CHECKSUM_SHA256 else 4
        oh = 1 + cl + 4 + 12 + 4
        
        with self._lock:
            try:
                # Use shared lock to avoid conflicts during read
                with open(self.log_path, 'rb') as f:
                    try:
                        portalocker.lock(f, portalocker.LOCK_SH | portalocker.LOCK_NB)
                    except (OSError, portalocker.exceptions.LockException):
                        # Continue even if we can't lock - it's a shared read attempt
                        pass
                    f.seek(0, os.SEEK_END); pos = f.tell()
                    # Start scanning from the end
                    while len(results) < limit and pos > header_offset:
                        if pos < (header_offset + oh): break
                        
                        # Peek at trailer length (last 4 bytes of entry)
                        f.seek(pos-4); fr = f.read(4)
                        if len(fr) < 4: 
                            pos -= 1; continue
                        el = struct.unpack('>I', fr)[0]
                        if el > MAX_ENTRY_SIZE or el == 0:
                            pos -= 1; continue
                            
                        tl = el + oh
                        if pos < (header_offset + tl):
                            pos -= 1; continue
                            
                        # Potential entry start pos
                        sp = pos - tl
                        f.seek(sp)
                        m = f.read(1)
                        if not m or m[0] != ENTRY_MAGIC: 
                            pos -= 1; continue
                            
                        # Found magic, read full entry
                        # We are at sp+1
                        cr = f.read(cl)
                        lr_bytes = f.read(4)
                        if len(lr_bytes) < 4: 
                            pos -= 1; continue
                        lr = struct.unpack('>I', lr_bytes)[0]
                        if lr != el:
                            pos -= 1; continue
                            
                        nonce = f.read(12)
                        ct_bin = f.read(el)
                        
                        # Verify integrity
                        valid = False
                        if ct == CHECKSUM_SHA256:
                            valid = (hashlib.sha256(m+lr_bytes+nonce+ct_bin).digest() == cr)
                        else:
                            valid = (struct.pack('>I', zlib.crc32(m+lr_bytes+nonce+ct_bin)) == cr)
                            
                        if valid:
                            try:
                                from cryptography.hazmat.primitives.ciphers.aead import AESGCM
                                decrypted = AESGCM(self.security._key).decrypt(nonce, ct_bin, None)
                                entry = msgpack.unpackb(decrypted, raw=False)
                                results.append(entry)
                                pos = sp # Move pointer to before this entry
                                continue
                            except Exception as e:
                                pass
                        pos -= 1 # Scan backwards one byte if not valid
            except Exception as e:
                warnings.warn(f"Reverse replay failed: {e}")
        return results
