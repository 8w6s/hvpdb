import os
import portalocker
from contextlib import contextmanager

class HVPLockManager:

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.lock_path = db_path + '.lock'
        self.write_lock_path = db_path + '.writelock'

    @contextmanager
    def reader_lock(self):
        if not os.path.exists(self.lock_path):
            try:
                with open(self.lock_path, 'w') as f:
                    pass
            except OSError:
                pass
        f = open(self.lock_path, 'r+')
        try:
            try:
                portalocker.lock(f, portalocker.LOCK_SH)
            except OSError:
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
        f = open(self.write_lock_path, 'r+')
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

    @contextmanager
    def critical_swap_lock(self):
        if not os.path.exists(self.lock_path):
            try:
                with open(self.lock_path, 'w') as f:
                    pass
            except OSError:
                pass
        f = open(self.lock_path, 'r+')
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