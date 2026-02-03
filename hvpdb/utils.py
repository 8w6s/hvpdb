import os
import time
import warnings
from typing import Optional

import portalocker


def is_termux() -> bool:
    """Check if the current environment is Termux."""
    return 'com.termux' in os.environ.get('PREFIX', '') or os.environ.get('TERMUX_VERSION') is not None


def acquire_interruptible_lock(file_handle, timeout: float = 30.0, check_interval: float = 0.1):
    """
    Acquire an exclusive lock on the file handle in an interruptible way (Ctrl+C safe).
    
    Args:
        file_handle: The open file handle to lock.
        timeout: Maximum time to wait for the lock in seconds.
        check_interval: Time to sleep between attempts.
        
    Raises:
        portalocker.LockException: If lock cannot be acquired within timeout.
    """
    start_time = time.time()
    while True:
        try:
            portalocker.lock(file_handle, portalocker.LOCK_EX | portalocker.LOCK_NB)
            return
        except (portalocker.LockException, OSError):
            if time.time() - start_time > timeout:
                raise portalocker.LockException("Timeout acquiring lock")
            time.sleep(check_interval)

def redact_target(target: str) -> str:
    """
    Redact passwords from connection URIs for safe logging.
    
    Args:
        target: The target string or URI.
        
    Returns:
        Redacted string.
    """
    if not target or '://' not in target:
        return target or ''
    try:
        from urllib.parse import urlparse
        parsed = urlparse(target)
        if parsed.password:
            return target.replace(parsed.password, '***')
        return target
    except Exception as e:
        warnings.warn(f"URI redaction fallback triggered for {target}: {e}")
        scheme_sep = target.find('://')
        if scheme_sep == -1:
            return target
        at = target.find('@', scheme_sep + 3)
        if at == -1:
            return target
        userinfo = target[scheme_sep + 3 : at]
        colon = userinfo.find(':')
        if colon == -1:
            return target
        safe_userinfo = userinfo[: colon + 1] + '***'
        return target[: scheme_sep + 3] + safe_userinfo + target[at:]

def normalize_target(target: str) -> str:
    """
    Ensure the database target has a valid extension.
    
    Args:
        target: Database filename or path.
        
    Returns:
        Normalized path string.
    """
    if not target or target.startswith('hvp://'):
        return target
    if not target.endswith('.hvp') and not target.endswith('.hvdb'):
        return target + '.hvp'
    return target

def get_db_password() -> Optional[str]:
    """Retrieve the database password from environment variables."""
    return os.environ.get('HVPDB_PASSWORD')

def connect_db(target: str, password: Optional[str]=None):
    """
    Helper to quickly connect to an HVPDB instance.
    
    Args:
        target: Database target.
        password: Optional password (falls back to env var).
        
    Returns:
        HVPDB instance.
    """
    from .core import HVPDB
    target = normalize_target(target)
    if password is None:
        password = get_db_password()
    return HVPDB(target, password)
