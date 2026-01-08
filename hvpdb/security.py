import os
import gc
from typing import Optional, Tuple
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

class HVPSecurity:
    """
    Handles cryptographic operations for HVPDB using AES-GCM-256 and Argon2id.
    Ensures zero-knowledge privacy by deriving keys from user passwords.
    """

    def __init__(self, password: str, salt: Optional[bytes] = None, kdf_params: Optional[dict] = None):
        """
        Initialize security context.
        
        Args:
            password (str): The master password for the database.
            salt (bytes, optional): A 16-byte salt. If None, a new one is generated.
            kdf_params (dict, optional): Argon2id parameters (time_cost, memory_cost, parallelism).
        """
        self._password = password.encode('utf-8')
        self.salt = salt if salt else os.urandom(16)
        
        # Default KDF Parameters (OWASP Recommended)
        self.kdf_params = kdf_params if kdf_params else {
            "time_cost": 3,
            "memory_cost": 65536, # 64MB
            "parallelism": 4
        }
        
        self._key = self._derive_key()
        
        # Memory Hygiene: Clear password from RAM immediately
        del self._password
        self._password = None
        gc.collect()

    def _derive_key(self) -> bytes:
        """Derives a 32-byte AES key using Argon2id (High Memory/CPU Cost)."""
        from argon2.low_level import hash_secret_raw, Type, ARGON2_VERSION
        
        return hash_secret_raw(
            secret=self._password,
            salt=self.salt,
            time_cost=self.kdf_params["time_cost"],
            memory_cost=self.kdf_params["memory_cost"],
            parallelism=self.kdf_params["parallelism"],
            hash_len=32,
            type=Type.ID,
            version=ARGON2_VERSION
        )

    def encrypt(self, plaintext: bytes, associated_data: Optional[bytes] = None) -> Tuple[bytes, bytes]:
        """
        Encrypts data using AES-GCM-256.
        
        Args:
            plaintext (bytes): Data to encrypt.
            associated_data (bytes, optional): AAD for integrity check (prevents replay/swap).
            
        Returns:
            Tuple[bytes, bytes]: (Nonce, Ciphertext)
        """
        if not self._key:
            raise RuntimeError("Key has been cleared from memory.")
            
        aesgcm = AESGCM(self._key)
        nonce = os.urandom(12) # NIST recommended 96-bit nonce
        ciphertext = aesgcm.encrypt(nonce, plaintext, associated_data)
        return nonce, ciphertext

    def decrypt(self, nonce: bytes, ciphertext: bytes, associated_data: Optional[bytes] = None) -> bytes:
        """Decrypts data using AES-GCM-256."""
        if not self._key:
            raise RuntimeError("Key has been cleared from memory.")

        aesgcm = AESGCM(self._key)
        return aesgcm.decrypt(nonce, ciphertext, associated_data)
    
    def encrypt_chunk(self, chunk: bytes, associated_data: Optional[bytes] = None) -> Tuple[bytes, bytes]:
        """Alias for encrypt, used in streaming contexts."""
        return self.encrypt(chunk, associated_data)

    def get_salt(self) -> bytes:
        return self.salt
        
    def get_kdf_params(self) -> dict:
        return self.kdf_params
        
    def clear_key(self):
        """Wipes the encryption key from memory."""
        if hasattr(self, '_key') and self._key:
            # Best effort to clear memory (Python strings/bytes are immutable so we just del ref)
            del self._key
            self._key = None
            gc.collect()
