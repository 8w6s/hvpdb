import os
import warnings
from typing import Optional, Tuple

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


class HVPSecurity:
    """
    Security manager for HVPDB handling encryption and key derivation.
    
    Uses Argon2 for key derivation and AES-GCM for authenticated encryption.
    Designed to be memory-safe by clearing sensitive keys when possible.
    """

    def __init__(self, password: str, salt: Optional[bytes]=None, kdf_params: Optional[dict]=None):
        """
        Initialize the security context.
        
        Args:
            password: Raw password for key derivation.
            salt: Optional 16-byte salt (generated if missing).
            kdf_params: Optional Argon2 parameters.
        """
        self._password = password.encode('utf-8')
        self.salt = salt if salt else os.urandom(16)
        self.kdf_params = kdf_params if kdf_params else {'time_cost': 4, 'memory_cost': 102400, 'parallelism': 4}
        self._key = self._derive_key()
        
        # Clear password from memory immediately after derivation
        self._password = None

    def rotate_key(self, new_password: str) -> bool:
        """
        Update the security context with a new password.
        
        Args:
            new_password: The new password to use.
            
        Returns:
            True if rotation succeeded.
        """
        try:
            self._password = new_password.encode('utf-8')
            self.salt = os.urandom(16)
            self._key = self._derive_key()
            self._password = None
            return True
        except Exception as e:
            warnings.warn(f"Key rotation failed: {e}")
            return False

    def _derive_key(self) -> bytes:
        """
        Derive a 32-byte key from the password using Argon2id.
        
        Returns:
            The derived raw key bytes.
        """
        if self._password is None:
            raise ValueError("Password is required for key derivation")
            
        from argon2.low_level import ARGON2_VERSION, Type, hash_secret_raw
        return hash_secret_raw(
            secret=self._password, 
            salt=self.salt, 
            time_cost=self.kdf_params['time_cost'], 
            memory_cost=self.kdf_params['memory_cost'], 
            parallelism=self.kdf_params['parallelism'], 
            hash_len=32, 
            type=Type.ID, 
            version=ARGON2_VERSION
        )

    def encrypt(self, plaintext: bytes, associated_data: Optional[bytes]=None) -> Tuple[bytes, bytes]:
        """
        Encrypt data using AES-GCM.
        
        Args:
            plaintext: Data to encrypt.
            associated_data: Optional authenticated data (not encrypted).
            
        Returns:
            Tuple of (nonce, ciphertext).
        """
        if not self._key:
            raise RuntimeError('Key has been cleared from memory.')
        aesgcm = AESGCM(self._key)
        nonce = os.urandom(12)
        ciphertext = aesgcm.encrypt(nonce, plaintext, associated_data)
        return (nonce, ciphertext)

    def decrypt(self, nonce: bytes, ciphertext: bytes, associated_data: Optional[bytes]=None) -> bytes:
        """
        Decrypt data using AES-GCM.
        
        Args:
            nonce: 12-byte nonce used during encryption.
            ciphertext: Encrypted data.
            associated_data: Authenticated data to verify.
            
        Returns:
            Decrypted plaintext.
        """
        if not self._key:
            raise RuntimeError('Key has been cleared from memory.')
        aesgcm = AESGCM(self._key)
        return aesgcm.decrypt(nonce, ciphertext, associated_data)

    def decrypt_chunk(self, nonce: bytes, ciphertext: bytes, associated_data: Optional[bytes]=None) -> bytes:
        """Alias for decrypt (used by WAL)."""
        return self.decrypt(nonce, ciphertext, associated_data)

    def encrypt_chunk(self, chunk: bytes, associated_data: Optional[bytes]=None) -> Tuple[bytes, bytes]:
        """Alias for encrypt (used by WAL)."""
        return self.encrypt(chunk, associated_data)

    def get_salt(self) -> bytes:
        """Get the current salt."""
        return self.salt

    def get_kdf_params(self) -> dict:
        """Get the current KDF parameters."""
        return self.kdf_params

    def clear_key(self):
        """Securely clear the derived key from memory."""
        if hasattr(self, '_key') and self._key:
            self._key = None