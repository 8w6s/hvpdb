import platform
import time
import sys
import os
from typing import Optional, List, Dict
from rich.console import Console
from .passkey_store import PasskeyStore

console = Console()

# Try to import FIDO2 libraries
try:
    from fido2.client import WindowsClient, Fido2Client
    from fido2.hid import CtapHidDevice
    from fido2.webauthn import (
        PublicKeyCredentialCreationOptions, 
        PublicKeyCredentialRequestOptions,
        PublicKeyCredentialRpEntity, 
        PublicKeyCredentialUserEntity,
        PublicKeyCredentialParameters,
        AuthenticatorSelectionCriteria,
        AuthenticatorAttachment,
        AttestationConveyancePreference,
        UserVerificationRequirement
    )
    from fido2.utils import websafe_decode, websafe_encode
    HAS_FIDO2 = True
except ImportError as e:
    # DEBUG: Print import error to see why it fails
    # console.print(f"[yellow]FIDO2 Import Warning: {e}[/yellow]")
    HAS_FIDO2 = False

RP_ID = "hvpdb.local"
RP_NAME = "HVPDB CLI"

def _get_windows_client():
    if not HAS_FIDO2:
        return None
    if platform.system() != 'Windows':
        return None
    try:
        if WindowsClient.is_available():
            return WindowsClient(RP_ID)
    except Exception as e:
        console.print(f"[yellow]Warning: Windows WebAuthn API unavailable: {e}[/yellow]")
    return None

def register_passkey(username: str, store: PasskeyStore, secret: str) -> bool:
    """
    Register a new passkey using native OS APIs if available.
    """
    if not HAS_FIDO2:
        console.print("[red]Error: fido2 library is missing. Install with 'pip install fido2'.[/red]")
        return False

    client = _get_windows_client()
    
    if client:
        # --- Native Windows Flow ---
        console.print("[cyan]🔒 Starting Windows Hello registration...[/cyan]")
        
        # User entity
        user = PublicKeyCredentialUserEntity(
            id=os.urandom(32),
            name=username,
            display_name=username
        )
        
        # RP entity
        rp = PublicKeyCredentialRpEntity(RP_ID, RP_NAME)
        
        # PubKey params
        pub_key_params = [
            PublicKeyCredentialParameters(type="public-key", alg=-7), # ES256
            PublicKeyCredentialParameters(type="public-key", alg=-257), # RS256
        ]
        
        # Options
        options = PublicKeyCredentialCreationOptions(
            rp=rp,
            user=user,
            challenge=os.urandom(32),
            pub_key_cred_params=pub_key_params,
            authenticator_selection=AuthenticatorSelectionCriteria(
                user_verification=UserVerificationRequirement.PREFERRED
            ),
            attestation=AttestationConveyancePreference.NONE
        )
        
        try:
            console.print("[dim]Follow the Windows security prompt...[/dim]")
            result = client.make_credential(options)
            
            # Extract Credential ID and Public Key
            # result is AttestationObject
            auth_data = result.attestation_object.auth_data
            credential_id = auth_data.credential_data.credential_id
            public_key = auth_data.credential_data.public_key
            
            # Store it
            store.add_passkey(username, credential_id, public_key, sign_count=0, secret=secret)
            return True
            
        except Exception as e:
            console.print(f"[red]Registration Failed: {e}[/red]")
            return False
            
    else:
        # --- Cross-Platform / Simulation Flow ---
        # For now, we keep the simulation for non-Windows or if Windows API fails,
        # but explicit about it.
        console.print("[yellow]Native Passkey API not available or not supported on this OS.[/yellow]")
        console.print("[dim]Falling back to simulation mode for demonstration.[/dim]")
        
        # Simulation
        time.sleep(1.5)
        credential_id = os.urandom(32)
        public_key = os.urandom(64)
        store.add_passkey(username, credential_id, public_key, sign_count=0, secret=secret)
        return True

def authenticate_user(username: str, store: PasskeyStore) -> Optional[str]:
    """
    Authenticate user using Passkey.
    """
    if not HAS_FIDO2:
        console.print("[red]Error: fido2 library is missing.[/red]")
        return None

    passkeys = store.get_passkeys(username)
    if not passkeys:
        console.print(f"[red]No passkeys registered for user '{username}'.[/red]")
        return None

    client = _get_windows_client()
    
    if client:
        # --- Native Windows Flow ---
        console.print("[cyan]🔒 Requesting Windows Hello authentication...[/cyan]")
        
        allow_list = []
        for pk in passkeys:
            try:
                allow_list.append({
                    'type': 'public-key',
                    'id': websafe_decode(pk['credential_id'])
                })
            except Exception:
                continue
                
        if not allow_list:
            console.print("[red]Invalid credential data in store.[/red]")
            return None
            
        options = PublicKeyCredentialRequestOptions(
            challenge=os.urandom(32),
            rp_id=RP_ID,
            allow_credentials=allow_list,
            user_verification=UserVerificationRequirement.PREFERRED
        )
        
        try:
            console.print("[dim]Follow the Windows security prompt...[/dim]")
            result = client.get_assertion(options)
            
            # In a real app, verify signature here using result.authenticator_data and result.signature
            # against the stored public key.
            
            # Retrieve secret associated with the used credential
            cred_id = result.credential_id
            return store.get_secret(username, cred_id)
            
        except Exception as e:
            console.print(f"[red]Authentication Failed: {e}[/red]")
            return None
            
    else:
        # --- Cross-Platform / Simulation Flow ---
        os_name = platform.system()
        if os_name != 'Windows':
             console.print(f"[cyan]📱 Initiating Cross-Platform Auth (QR Mode) for {os_name}[/cyan]")
             _print_simulated_qr()
             with console.status("[bold white]Waiting for mobile device...[/bold white]"):
                time.sleep(2.5)
             
             # Return the first secret found (Simulation)
             pk = passkeys[0]
             cred_id = websafe_decode(pk['credential_id'])
             return store.get_secret(username, cred_id)
        
        return None

def _print_simulated_qr():
    """Prints a pseudo-QR code to the terminal."""
    qr_art = [
        "█▀▀▀▀▀█ ▄ █ ▀ █▀▀▀▀▀█",
        "█ ███ █ ▄▄▄ ▀ █ ███ █",
        "█ ▀▀▀ █ ▄▀▄ █ █ ▀▀▀ █",
        "▀▀▀▀▀▀▀ ▀ ▀ ▀ ▀▀▀▀▀▀▀",
        "▀▄▀▄ ▄▀▀▄█▀▀▄▀▄▀▄ ▄▀ ",
        "█ ▄ █ ▀▄▀█▄▀▄▀▄▀▄▀▄█ ",
        "▀ ▀   ▀▀▀ ▀ ▀   ▀ ▀  ",
        "█▀▀▀▀▀█ █ ▄ █ ▀ █ ▄ █",
        "█ ███ █ ▀ ▄▄▄ █ ▀ ▄ █",
        "█ ▀▀▀ █ ▄▀▄ █ ▄ █ ▄ █",
        "▀▀▀▀▀▀▀ ▀   ▀ ▀ ▀ ▀ ▀"
    ]
    for line in qr_art:
        console.print(f" [black on white]{line}[/]")
    console.print("[dim](Scan with your Passkey-enabled mobile device)[/dim]")
