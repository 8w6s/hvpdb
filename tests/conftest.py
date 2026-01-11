import pytest
import os
from hvpdb.security import HVPSecurity
from hvpdb.core import HVPDB
original_init = HVPSecurity.__init__

def fast_init(self, password: str, salt=None, kdf_params=None):
    if kdf_params is None:
        kdf_params = {'time_cost': 1, 'memory_cost': 8, 'parallelism': 1}
    original_init(self, password, salt, kdf_params)
HVPSecurity.__init__ = fast_init

@pytest.fixture
def db(tmp_path):
    db_path = tmp_path / 'test.hvp'
    if os.path.exists(db_path):
        os.remove(db_path)
    _db = HVPDB(str(db_path), 'test_password')
    yield _db
    _db.close()