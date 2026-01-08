import os
import portalocker
from contextlib import contextmanager

class HVPLockManager:
    """
    Manages concurrency locks for HVPDB.
    Implements a separate lock-file strategy to allow atomic file replacements on Windows.
    """
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.lock_path = db_path + ".lock"
        self.write_lock_path = db_path + ".writelock"

    @contextmanager
    def reader_lock(self):
        """
        Acquire Shared Lock (SH) on the main lock file.
        Allows multiple readers, blocks main writer (checkpoint).
        """
        # Ensure lock file exists
        if not os.path.exists(self.lock_path):
            with open(self.lock_path, 'w') as f: pass

        f = open(self.lock_path, 'r+')
        try:
            portalocker.lock(f, portalocker.LOCK_SH)
            yield
        finally:
            portalocker.unlock(f)
            f.close()

    @contextmanager
    def writer_lock(self):
        """
        Acquire Exclusive Lock (EX) on the write lock file.
        Ensures only ONE writer can perform heavy operations (like compaction) at a time.
        Does NOT block readers (until the swap phase).
        """
        if not os.path.exists(self.write_lock_path):
            with open(self.write_lock_path, 'w') as f: pass
            
        f = open(self.write_lock_path, 'r+')
        try:
            portalocker.lock(f, portalocker.LOCK_EX)
            yield
        finally:
            portalocker.unlock(f)
            f.close()

    @contextmanager
    def critical_swap_lock(self):
        """
        Acquire Exclusive Lock (EX) on the main lock file.
        BLOCKS ALL READERS. Use this only for very short durations (file rename/swap).
        """
        if not os.path.exists(self.lock_path):
            with open(self.lock_path, 'w') as f: pass

        f = open(self.lock_path, 'r+')
        try:
            portalocker.lock(f, portalocker.LOCK_EX)
            yield
        finally:
            portalocker.unlock(f)
            f.close()
