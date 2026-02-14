import os
import sys
from unittest.mock import MagicMock, patch
from typer.testing import CliRunner
from hvpdb.cli import app
import platform

# Mock fido2 structures
class MockAttestationObject:
    class AuthData:
        class CredentialData:
            credential_id = b'mock_cred_id'
            public_key = b'mock_public_key'
        counter = 1
        credential_data = CredentialData()
    auth_data = AuthData()

class MockResult:
    attestation_object = MockAttestationObject()

def run_demo():
    runner = CliRunner()
    
    print("🎥 DEMO: HVPDB Passkey Integration (Simulated)")
    print("==============================================")
    
    # 1. Init DB
    print("\n[1] Initializing Database...")
    result = runner.invoke(app, ["init", "demo.hvp", "password123"])
    print(result.stdout)
    
    # 2. Generate Passkey (Mocked Windows Hello)
    print("\n[2] Generating Passkey for 'admin'...")
    # We must patch where it is defined because cli.py imports it inside the function
    with patch('hvpdb.fido_native.create_passkey_windows', return_value=MockResult()) as mock_create:
        # Also mock platform to ensure we hit the native path logic if auto-detect fails
        with patch('platform.system', return_value='Windows'):
             # We also need to patch PasskeyStore to avoid file permission issues or real writes if needed
             with patch('hvpdb.passkey_store.PasskeyStore') as MockStore: # Patch at definition too
                 mock_store_instance = MockStore.return_value
                 mock_store_instance.filename = "mock_passkeys.json"
                 
                 result = runner.invoke(app, ["gen-passkey", "admin", "--native"])
                 print(result.stdout)
                 
                 # Verify store was called
                 # Note: If cli.py imports PasskeyStore inside function, it might pick up the patch if we patch 'hvpdb.passkey_store.PasskeyStore'
                 # because cli.py does "from .passkey_store import PasskeyStore"
                 if mock_store_instance.add_passkey.call_count > 0:
                     print("✅ Passkey generated and stored (Mocked).")
                 else:
                     print("⚠️  Store not called (Check imports)")

    # 3. Configure DB to use Passkey (Mocked)
    print("\n[3] Configuring DB to use Passkey Auth...")
    # This interacts with real DB file created in step 1
    result = runner.invoke(app, ["config", "demo.hvp", "--auth-type", "passkey", "--password", "password123"])
    print(result.stdout)
    
    # 4. Login with Passkey (Mocked)
    print("\n[4] Logging in with Passkey...")
    with patch('hvpdb.fido_native.authenticate_passkey_windows', return_value=True):
        with patch('hvpdb.passkey_store.PasskeyStore') as MockStore:
            mock_store_instance = MockStore.return_value
            mock_store_instance.get_passkeys.return_value = [{'cred_id': b'mock', 'public_key': b'mock'}]
            
            with patch('platform.system', return_value='Windows'):
                result = runner.invoke(app, ["login-passkey", "admin", "--native"])
                print(result.stdout)

if __name__ == "__main__":
    run_demo()
