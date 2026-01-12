import os
import sys
from typing import Optional
from .core import HVPDB

def is_termux() -> bool:
    return 'com.termux' in os.environ.get('PREFIX', '') or os.environ.get('TERMUX_VERSION') is not None

def redact_target(target: str) -> str:
    if not target:
        return ''
    if '://' not in target:
        return target
    try:
        from urllib.parse import urlparse
        parsed = urlparse(target)
        if parsed.password:
            return target.replace(parsed.password, '***')
        return target
    except:
        return target

def normalize_target(target: str) -> str:
    if not target:
        return target
    if target.startswith('hvp://'):
        return target
    if not target.endswith('.hvp') and (not target.endswith('.hvdb')):
        return target + '.hvp'
    return target

def get_db_password() -> Optional[str]:
    return os.environ.get('HVPDB_PASSWORD')

def connect_db(target: str, password: str=None) -> HVPDB:
    target = normalize_target(target)
    if password is None:
        password = get_db_password()
    return HVPDB(target, password)