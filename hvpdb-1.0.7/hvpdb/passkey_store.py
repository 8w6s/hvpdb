
import json
import os
from typing import Dict, List, Optional
from fido2.utils import websafe_encode, websafe_decode
from cryptography.fernet import Fernet

PASSKEY_STORE_FILE = "hvpdb_passkeys.json"

class PasskeyStore:
    def __init__(self, filename=PASSKEY_STORE_FILE):
        self.filename = filename
        self.key_file = filename + '.key'
        self._init_key()
        self.data: Dict[str, List[Dict]] = self._load()

    def _init_key(self):
        if os.path.exists(self.key_file):
            with open(self.key_file, 'rb') as f:
                self.key = f.read()
        else:
            self.key = Fernet.generate_key()
            with open(self.key_file, 'wb') as f:
                f.write(self.key)
        self.cipher = Fernet(self.key)

    def _load(self) -> Dict[str, List[Dict]]:
        if not os.path.exists(self.filename):
            return {}
        try:
            with open(self.filename, 'r') as f:
                return json.load(f)
        except Exception:
            return {}

    def _save(self):
        with open(self.filename, 'w') as f:
            json.dump(self.data, f, indent=2)

    def add_passkey(self, username: str, credential_id: bytes, public_key: bytes, sign_count: int = 0, secret: Optional[str] = None):
        if username not in self.data:
            self.data[username] = []
        
        encrypted_secret = None
        if secret:
            encrypted_secret = self.cipher.encrypt(secret.encode()).decode('utf-8')

        # Check if credential_id already exists
        encoded_id = websafe_encode(credential_id)
        for pk in self.data[username]:
            if pk['credential_id'] == encoded_id:
                pk['public_key'] = websafe_encode(public_key)
                pk['sign_count'] = sign_count
                if encrypted_secret:
                    pk['secret'] = encrypted_secret
                self._save()
                return

        self.data[username].append({
            'credential_id': encoded_id,
            'public_key': websafe_encode(public_key),
            'sign_count': sign_count,
            'secret': encrypted_secret
        })
        self._save()

    def get_passkeys(self, username: str) -> List[Dict]:
        return self.data.get(username, [])
    
    def get_secret(self, username: str, credential_id: bytes) -> Optional[str]:
        encoded_id = websafe_encode(credential_id)
        if username in self.data:
            for pk in self.data[username]:
                if pk['credential_id'] == encoded_id:
                    enc_secret = pk.get('secret')
                    if enc_secret:
                        try:
                            return self.cipher.decrypt(enc_secret.encode('utf-8')).decode('utf-8')
                        except Exception:
                            return None
        return None

    def update_sign_count(self, username: str, credential_id: bytes, new_count: int):
        encoded_id = websafe_encode(credential_id)
        if username in self.data:
            for pk in self.data[username]:
                if pk['credential_id'] == encoded_id:
                    pk['sign_count'] = new_count
                    self._save()
                    return
