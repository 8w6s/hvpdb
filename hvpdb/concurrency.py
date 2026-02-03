import os
import time
import warnings
from contextlib import contextmanager

import portalocker

from .utils import is_termux


class HVPLockManager:
    """
    Manager for file-based locking to ensure thread and process safety.
    
    Provides reader, writer, and critical swap locks using portalocker.
    Handles environment-specific limitations (e.g., Termux).
    """

    def __init__(self, db_path: str):
        """
        Initialize the lock manager.
        
        Args:
            db_path: Base path of the database file.
        """
        self.db_path = db_path
        self.lock_path = db_path + '.lock'
        self.write_lock_path = db_path + '.writelock'
        self.is_termux = is_termux()

    @contextmanager
    def reader_lock(self):
        """
        Acquire a shared reader lock.

        Yields:
            None
        """
        if not os.path.exists(self.lock_path):
            try:
                # Use secure permissions (0o600) for lock files
                with open(self.lock_path, 'w') as f:
                    pass
                if os.name != 'nt':
                    os.chmod(self.lock_path, 0o600)
            except OSError as e:
                warnings.warn(f"Failed to set lock file permissions: {e}")
        
        try:
            f = open(self.lock_path, 'r+')
        except OSError as e:
            warnings.warn(f"Failed to open lock file: {e}")
            # Fallback for read-only filesystems
            yield
            return

        try:
            # Interruptible lock acquisition
            while True:
                try:
                    portalocker.lock(f, portalocker.LOCK_SH | portalocker.LOCK_NB)
                    break
                except (OSError, portalocker.exceptions.LockException) as e:
                    # If locking is not supported (e.g. Termux), fail fast
                    if self.is_termux:
                        break
                    # Otherwise wait and retry to emulate blocking lock but allow signals
                    time.sleep(0.1)
            yield
        finally:
            try:
                portalocker.unlock(f)
            except (OSError, portalocker.exceptions.LockException) as e:
                warnings.warn(f"Failed to unlock: {e}")
            f.close()

    @contextmanager
    def writer_lock(self):
        """
        Acquire an exclusive writer lock.

        Yields:
            None
        """
        if not os.path.exists(self.write_lock_path):
            try:
                with open(self.write_lock_path, 'w') as f:
                    pass
                if os.name != 'nt':
                    os.chmod(self.write_lock_path, 0o600)
            except OSError as e:
                warnings.warn(f"Failed to set write lock file permissions: {e}")
        
        try:
            f = open(self.write_lock_path, 'r+')
        except OSError as e:
            warnings.warn(f"Failed to open write lock file: {e}")
            yield
            return

        try:
            # Interruptible lock acquisition
            while True:
                try:
                    portalocker.lock(f, portalocker.LOCK_EX | portalocker.LOCK_NB)
                    break
                except (OSError, portalocker.exceptions.LockException) as e:
                    if self.is_termux:
                        break
                    time.sleep(0.1)
            yield
        finally:
            try:
                portalocker.unlock(f)
            except (OSError, portalocker.exceptions.LockException) as e:
                warnings.warn(f"Failed to unlock: {e}")
            f.close()

    @contextmanager
    def critical_swap_lock(self):
        """
        Acquire an exclusive lock during critical file swaps.

        Uses the main lock file to ensure no readers are active during the swap.

        Yields:
            None
        """
        if not os.path.exists(self.lock_path):
            try:
                with open(self.lock_path, 'w') as f:
                    pass
                if os.name != 'nt':
                    os.chmod(self.lock_path, 0o600)
            except OSError:
                pass
        
        try:
            f = open(self.lock_path, 'r+')
        except OSError:
            yield
            return

        try:
            try:
                portalocker.lock(f, portalocker.LOCK_EX)
            except (OSError, portalocker.exceptions.LockException):
                pass
            yield
        finally:
            try:
                portalocker.unlock(f)
            except (OSError, portalocker.exceptions.LockException):
                pass
            f.close()