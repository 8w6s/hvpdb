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
        try:
             fd = os.open(self.lock_path, os.O_RDWR | os.O_CREAT, 0o600)
             f = os.fdopen(fd, 'r+')
        except OSError as e:
             # Fail fast if we can't even open the lock file
             raise RuntimeError(f"Failed to open lock file {self.lock_path}: {e}")

        try:
            # Interruptible lock acquisition with exponential backoff
            delay = 0.01
            while True:
                try:
                    portalocker.lock(f, portalocker.LOCK_SH | portalocker.LOCK_NB)
                    break
                except (OSError, portalocker.exceptions.LockException) as e:
                    # If locking is not supported (e.g. Termux), fail fast
                    if self.is_termux:
                        warnings.warn(f"Locking not supported on this platform: {e}")
                        break
                    # Otherwise wait and retry to emulate blocking lock but allow signals
                    time.sleep(delay)
                    delay = min(delay * 2, 0.5) # Cap at 0.5s
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
        try:
             fd = os.open(self.write_lock_path, os.O_RDWR | os.O_CREAT, 0o600)
             f = os.fdopen(fd, 'r+')
        except OSError as e:
             raise RuntimeError(f"Failed to open write lock file {self.write_lock_path}: {e}")

        try:
            # Interruptible lock acquisition with exponential backoff
            delay = 0.01
            while True:
                try:
                    portalocker.lock(f, portalocker.LOCK_EX | portalocker.LOCK_NB)
                    break
                except (OSError, portalocker.exceptions.LockException) as e:
                    if self.is_termux:
                        break
                    time.sleep(delay)
                    delay = min(delay * 2, 0.5)
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
        try:
            fd = os.open(self.lock_path, os.O_WRONLY | os.O_CREAT, 0o600)
            os.close(fd)
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