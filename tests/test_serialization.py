import datetime
import uuid
from hvpdb.core import HVPDB

def test_serialize_set(db):
    """Test that Python sets are converted to lists."""
    grp = db.group('types')
    data = {'tags': {'a', 'b', 'c'}}
    doc = grp.insert(data)
    
    # Save and reload to force serialization cycle
    db.commit()
    db.close()
    
    # Reopen
    db2 = HVPDB(db.filepath, db.password)
    loaded_doc = db2.group('types').find_one({'_id': doc['_id']})
    
    assert isinstance(loaded_doc['tags'], list)
    assert set(loaded_doc['tags']) == {'a', 'b', 'c'}
    db2.close()

def test_serialize_datetime(db):
    """Test that datetime objects are converted to ISO strings."""
    grp = db.group('types')
    now = datetime.datetime.now()
    data = {'created': now}
    doc = grp.insert(data)
    
    db.commit()
    db.close()
    
    db2 = HVPDB(db.filepath, db.password)
    loaded_doc = db2.group('types').find_one({'_id': doc['_id']})
    
    assert isinstance(loaded_doc['created'], str)
    # Basic check if it looks like ISO format (or whatever default_serializer does)
    assert loaded_doc['created'] == now.isoformat()
    db2.close()

def test_serialize_uuid(db):
    """Test that UUID objects are converted to strings."""
    grp = db.group('types')
    uid = uuid.uuid4()
    data = {'uid': uid}
    doc = grp.insert(data)
    
    db.commit()
    db.close()
    
    db2 = HVPDB(db.filepath, db.password)
    loaded_doc = db2.group('types').find_one({'_id': doc['_id']})
    
    assert isinstance(loaded_doc['uid'], str)
    assert loaded_doc['uid'] == str(uid)
    db2.close()

def test_nested_complex_types(db):
    """Test nested complex types."""
    grp = db.group('types')
    uid = uuid.uuid4()
    now = datetime.datetime.now()
    data = {
        'meta': {
            'ids': {uid},
            'timestamps': [now, now]
        }
    }
    doc = grp.insert(data)
    
    db.commit()
    db.close()
    
    db2 = HVPDB(db.filepath, db.password)
    loaded_doc = db2.group('types').find_one({'_id': doc['_id']})
    
    assert isinstance(loaded_doc['meta']['ids'], list)
    assert loaded_doc['meta']['ids'][0] == str(uid)
    assert loaded_doc['meta']['timestamps'][0] == now.isoformat()
    db2.close()
