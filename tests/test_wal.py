import os
import time
from hvpdb.core import HVPDB

def test_wal_replay(tmp_path):
    """Test that uncommitted changes in WAL are recovered on next open."""
    db_path = tmp_path / "wal_test.hvp"
    
    # 1. Open and insert data, but DO NOT commit/save
    db = HVPDB(str(db_path), "pass")
    grp = db.group("logs")
    grp.insert({"msg": "I am in WAL"})
    
    # Verify it's in memory
    assert len(grp.find({})) == 1
    
    # Force close without save (simulate crash or just closing)
    if db.storage.wal._file_handle:
        db.storage.wal._file_handle.close()
    del db
    
    # 2. Reopen
    # The new instance should detect the existing .log file and replay it
    db2 = HVPDB(str(db_path), "pass")
    grp2 = db2.group("logs")
    
    docs = grp2.find({})
    assert len(docs) == 1
    assert docs[0]["msg"] == "I am in WAL"
    
    # Now commit properly
    db2.commit()
    db2.close()
    
    # Let's verify data persists after commit
    db3 = HVPDB(str(db_path), "pass")
    assert len(db3.group("logs").find({})) == 1
    db3.close()

def test_wal_complex_types(tmp_path):
    """Test that WAL correctly handles complex types during replay."""
    db_path = tmp_path / "wal_complex.hvp"
    
    db = HVPDB(str(db_path), "pass")
    grp = db.group("data")
    
    # Insert set, datetime
    import datetime
    now = datetime.datetime.now()
    grp.insert({"tags": {"a", "b"}, "time": now})
    
    # Crash
    if db.storage.wal._file_handle:
        db.storage.wal._file_handle.close()
    del db
    
    # Replay
    db2 = HVPDB(str(db_path), "pass")
    doc = db2.group("data").find_one({})
    
    assert isinstance(doc["tags"], list)
    assert set(doc["tags"]) == {"a", "b"}
    assert isinstance(doc["time"], str)
    assert doc["time"] == now.isoformat()
    
    db2.close()
