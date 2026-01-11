import os
import gc
from typing import Optional, Tuple
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

class HVPSecurity:

    def __init__(self, password: str, salt: Optional[bytes]=None, kdf_params: Optional[dict]=None):
        self._password = password.encode('utf-8')
        self.salt = salt if salt else os.urandom(16)
        self.kdf_params = kdf_params if kdf_params else {'time_cost': 4, 'memory_cost': 102400, 'parallelism': 4}
        self._key = self._derive_key()
        if self._password:
            pass
        del self._password
        self._password = None
        gc.collect()

    def rotate_key(self, new_password: str) -> bool:
        try:
            self._password = new_password.encode('utf-8')
            self.salt = os.urandom(16)
            self._key = self._derive_key()
            del self._password
            self._password = None
            gc.collect()
            return True
        except Exception:
            return False

    def _derive_key(self) -> bytes:
        from argon2.low_level import hash_secret_raw, Type, ARGON2_VERSION
        return hash_secret_raw(secret=self._password, salt=self.salt, time_cost=self.kdf_params['time_cost'], memory_cost=self.kdf_params['memory_cost'], parallelism=self.kdf_params['parallelism'], hash_len=32, type=Type.ID, version=ARGON2_VERSION)

    def encrypt(self, plaintext: bytes, associated_data: Optional[bytes]=None) -> Tuple[bytes, bytes]:
        if not self._key:
            raise RuntimeError('Key has been cleared from memory.')
        aesgcm = AESGCM(self._key)
        nonce = os.urandom(12)
        ciphertext = aesgcm.encrypt(nonce, plaintext, associated_data)
        return (nonce, ciphertext)

    def decrypt(self, nonce: bytes, ciphertext: bytes, associated_data: Optional[bytes]=None) -> bytes:
        if not self._key:
            raise RuntimeError('Key has been cleared from memory.')
        aesgcm = AESGCM(self._key)
        return aesgcm.decrypt(nonce, ciphertext, associated_data)

    def decrypt_chunk(self, nonce: bytes, ciphertext: bytes, associated_data: Optional[bytes]=None) -> bytes:
        return self.decrypt(nonce, ciphertext, associated_data)

    def encrypt_chunk(self, chunk: bytes, associated_data: Optional[bytes]=None) -> Tuple[bytes, bytes]:
        return self.encrypt(chunk, associated_data)

    def get_salt(self) -> bytes:
        return self.salt

    def get_kdf_params(self) -> dict:
        return self.kdf_params

    def clear_key(self):
        if hasattr(self, '_key') and self._key:
            del self._key
            self._key = None
            gc.collect()