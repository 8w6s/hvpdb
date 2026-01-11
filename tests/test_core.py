import pytest
import os
import time
from hvpdb.core import HVPDB

def test_init_creates_file_explicitly(tmp_path):
    db_path = tmp_path / 'new.hvp'
    db = HVPDB(str(db_path), 'pass')
    assert not os.path.exists(str(db_path))
    db.storage.save()
    assert os.path.exists(str(db_path))
    db.close()

def test_crud_insert_find(db):
    grp = db.group('users')
    doc = {'name': 'Alice', 'role': 'dev'}
    res = grp.insert(doc)
    assert '_id' in res
    assert res['name'] == 'Alice'
    found = grp.find_one({'name': 'Alice'})
    assert found is not None
    assert found['_id'] == res['_id']
    grp.insert({'name': 'Bob', 'role': 'dev'})
    devs = grp.find({'role': 'dev'})
    assert len(devs) == 2

def test_crud_update(db):
    grp = db.group('items')
    item = grp.insert({'name': 'Laptop', 'price': 1000})
    count = grp.update({'name': 'Laptop'}, {'price': 900})
    assert count == 1
    updated = grp.find_one({'_id': item['_id']})
    assert updated['price'] == 900

def test_crud_delete(db):
    grp = db.group('trash')
    item = grp.insert({'junk': True})
    count = grp.delete({'junk': True})
    assert count == 1
    assert grp.find_one({'_id': item['_id']}) is None

def test_transaction_commit(db):
    grp = db.group('bank')
    with db.begin():
        grp.insert({'account': 'A', 'balance': 100})
        grp.insert({'account': 'B', 'balance': 200})
    assert len(grp.find({})) == 2

def test_transaction_rollback(db):
    grp = db.group('bank')
    grp.insert({'account': 'Initial', 'balance': 0})
    try:
        with db.begin():
            grp.insert({'account': 'Bad', 'balance': -100})
            raise RuntimeError('Simulation Crash')
    except RuntimeError:
        pass
    all_accs = grp.find({})
    assert len(all_accs) == 1
    assert all_accs[0]['account'] == 'Initial'

def test_persistence(tmp_path):
    path = str(tmp_path / 'persist.hvp')
    db1 = HVPDB(path, 'pass')
    db1.group('test').insert({'msg': 'hello'})
    db1.storage.save()
    db1.close()
    db2 = HVPDB(path, 'pass')
    docs = db2.group('test').find({})
    assert len(docs) == 1
    assert docs[0]['msg'] == 'hello'
    db2.close()