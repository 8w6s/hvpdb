import os
import portalocker
from contextlib import contextmanager

from .utils import is_termux

class HVPLockManager:

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.lock_path = db_path + '.lock'
        self.write_lock_path = db_path + '.writelock'
        self.is_termux = is_termux()

    @contextmanager
    def reader_lock(self):
        if not os.path.exists(self.lock_path):
            try:
                with open(self.lock_path, 'w') as f:
                    pass
            except OSError:
                pass
        
        # In Termux (especially on SD card), file locking might be flaky.
        # We try to lock, but if it fails with specific errors, we might have to skip it or warn.
        try:
            f = open(self.lock_path, 'r+')
        except OSError:
            # Fallback if we can't open r+ (maybe read-only fs?)
            # Just yield without lock in worst case
            yield
            return

        try:
            try:
                portalocker.lock(f, portalocker.LOCK_SH)
            except OSError:
                if not self.is_termux:
                    # On normal systems, locking failure is real issue. 
                    # On Termux, it might just be the filesystem.
                    pass
            yield
        finally:
            try:
                portalocker.unlock(f)
            except OSError:
                pass
            f.close()

    @contextmanager
    def writer_lock(self):
        if not os.path.exists(self.write_lock_path):
            try:
                with open(self.write_lock_path, 'w') as f:
                    pass
            except OSError:
                pass
        
        try:
            f = open(self.write_lock_path, 'r+')
        except OSError:
            yield
            return

        try:
            try:
                portalocker.lock(f, portalocker.LOCK_EX)
            except OSError:
                if not self.is_termux:
                    pass
            yield
        finally:
            try:
                portalocker.unlock(f)
            except OSError:
                pass
            f.close()

    @contextmanager
    def critical_swap_lock(self):
        if not os.path.exists(self.lock_path):
            try:
                with open(self.lock_path, 'w') as f:
                    pass
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
            except OSError:
                pass
            yield
        finally:
            try:
                portalocker.unlock(f)
            except OSError:
                pass
            f.close()