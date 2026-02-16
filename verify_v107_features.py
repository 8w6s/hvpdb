
import os
import shutil
import time
import uuid
from hvpdb import HVPDB

TEST_DB = "./test_v107.hvp"
BACKUP_DB = "./test_v107_backup.hvp"

def setup():
    for ext in ["", ".log", ".writelock"]:
        if os.path.exists(TEST_DB + ext):
            os.remove(TEST_DB + ext)
    if os.path.exists(BACKUP_DB):
        os.remove(BACKUP_DB)
    
    # Initialize DB
    db = HVPDB(TEST_DB, "password")
    return db

def test_bulk_ops(db):
    print("\n--- Testing Bulk Operations ---")
    users = db.group("users")
    
    # Bulk Insert
    docs = [{"name": f"User{i}", "role": "admin" if i % 2 == 0 else "user", "age": 20 + i} for i in range(10)]
    users.bulk_insert(docs)
    print(f"Bulk inserted {len(docs)} documents.")
    assert users.count() == 10
    
    # Bulk Update
    updated_count = users.bulk_update({"role": "admin"}, {"is_active": True})
    print(f"Bulk updated {updated_count} admins to be active.")
    assert updated_count == 5
    
    # Verify update
    admin_check = users.find_one({"role": "admin"})
    assert admin_check["is_active"] is True
    
    # Bulk Delete
    deleted_count = users.bulk_delete({"role": "user"})
    print(f"Bulk deleted {deleted_count} users.")
    assert deleted_count == 5
    assert users.count() == 5

def test_regex_pagination(db):
    print("\n--- Testing Regex & Pagination ---")
    products = db.group("products")
    
    data = [
        {"name": "Apple iPhone 13", "category": "phone"},
        {"name": "Samsung Galaxy S22", "category": "phone"},
        {"name": "Apple MacBook Pro", "category": "laptop"},
        {"name": "Dell XPS 13", "category": "laptop"},
        {"name": "Apple iPad", "category": "tablet"},
    ]
    products.bulk_insert(data)
    
    # Regex
    apple_products = products.find({"name": {"$regex": "^Apple"}})
    print(f"Regex '^Apple' found: {[p['name'] for p in apple_products]}")
    assert len(apple_products) == 3
    
    # Pagination
    page1 = products.find(query={}, limit=2, skip=0)
    page2 = products.find(query={}, limit=2, skip=2)
    print(f"Page 1: {[p['name'] for p in page1]}")
    print(f"Page 2: {[p['name'] for p in page2]}")
    assert len(page1) == 2
    assert len(page2) == 2
    assert page1[0]["_id"] != page2[0]["_id"]

def test_soft_delete(db):
    print("\n--- Testing Soft Delete ---")
    notes = db.group("notes")
    notes.insert({"title": "Secret Note", "content": "hidden"})
    notes.insert({"title": "Public Note", "content": "visible"})
    
    # Soft delete
    notes.soft_delete({"title": "Secret Note"})
    print("Soft deleted 'Secret Note'.")
    
    # Normal find should ignore it
    visible = notes.find({})
    print(f"Visible notes: {[n['title'] for n in visible]}")
    assert len(visible) == 1
    assert visible[0]["title"] == "Public Note"
    
    # Find deleted explicitly
    deleted = notes.find({"_deleted": True})
    print(f"Deleted notes found explicitly: {[n['title'] for n in deleted]}")
    assert len(deleted) == 1
    
    # Undelete
    notes.undelete({"title": "Secret Note"})
    print("Undeleted 'Secret Note'.")
    assert len(notes.find({})) == 2

def test_ttl(db):
    print("\n--- Testing TTL (Time-To-Live) ---")
    cache = db.group("cache")
    
    # Insert with TTL (1 second for quick test)
    # Note: Logic sets _expires_at = now + ttl
    cache.insert({"key": "temp_data", "value": 123, "ttl": 1})
    cache.insert({"key": "perm_data", "value": 456}) # No TTL
    
    print(f"Inserted TTL doc (1s) and permanent doc.")
    
    # Immediately visible?
    assert len(cache.find({})) == 2
    
    print("Waiting 1.5 seconds...")
    time.sleep(1.5)
    
    # Should be filtered out by find() logic even before reaper runs
    visible = cache.find({})
    print(f"Visible after wait: {[d['key'] for d in visible]}")
    assert len(visible) == 1
    assert visible[0]["key"] == "perm_data"

def test_computed_fields(db):
    print("\n--- Testing Computed Fields ---")
    orders = db.group("orders")
    
    # Define computed field
    orders.set_computed_field("total", lambda d: d.get("price", 0) * d.get("qty", 1))
    
    # Insert
    doc = orders.insert({"item": "Book", "price": 20, "qty": 3})
    print(f"Inserted doc: {doc}")
    
    assert "total" in doc
    assert doc["total"] == 60

def test_dbref(db):
    print("\n--- Testing DBRef ---")
    users = db.group("users")
    user = users.find_one({"role": "admin"}) # Should exist from bulk test
    
    posts = db.group("posts")
    post = posts.insert({
        "title": "Announcement",
        "author": {"$ref": "users", "$id": user["_id"]}
    })
    
    print(f"Created post with ref: {post}")
    
    # Resolve
    resolved_user = posts.resolve_ref(post["author"])
    print(f"Resolved author: {resolved_user['name']}")
    assert resolved_user["_id"] == user["_id"]

def test_backup_restore(db):
    print("\n--- Testing Backup ---")
    db.backup(BACKUP_DB)
    print(f"Backup created at {BACKUP_DB}")
    
    assert os.path.exists(BACKUP_DB)
    
    # Verify backup by loading it
    db2 = HVPDB(BACKUP_DB, "password")
    assert db2.group("users").count() == 5
    print("Backup verification successful: Loaded and counted users.")

def main():
    try:
        db = setup()
        test_bulk_ops(db)
        test_regex_pagination(db)
        test_soft_delete(db)
        test_ttl(db)
        test_computed_fields(db)
        test_dbref(db)
        test_backup_restore(db)
        
        print("\n[PASS] All v1.0.7 Feature Tests PASSED!")
    except Exception as e:
        print(f"\n[FAIL] Test FAILED: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Cleanup
        if os.path.exists(TEST_DB):
            pass # Keep for inspection if needed, or remove
        if os.path.exists(TEST_DB + ".log"):
            pass

if __name__ == "__main__":
    main()
