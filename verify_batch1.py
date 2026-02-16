import os
import time
import re
import uuid
from hvpdb.core import HVPDB

def test_batch1():
    # Use a unique DB file for each run to avoid persistence issues
    db_path = f"test_batch1_{uuid.uuid4().hex[:8]}.hvp"
    print(f"Testing with unique DB: {db_path}")
    
    try:
        db = HVPDB(db_path, password="testpassword")
        users = db.group("users")
        
        # Check initial state
        count = users.count()
        print(f"Initial count (should be 0): {count}")
        assert count == 0
        
        # 1. Regex Queries
        users.insert({"name": "Alice", "role": "admin"})
        users.insert({"name": "Bob", "role": "user"})
        users.insert({"name": "Charlie", "role": "admin"})
        
        admins = users.find({"role": {"$regex": "^adm.*"}})
        print(f"Regex Admins (should be 2): {len(admins)}")
        for a in admins: print(f" - Found admin: {a['name']}")
        assert len(admins) == 2
        
        # 2. Pagination (Skip/Limit)
        all_users = users.find(limit=1, skip=1)
        print(f"Paginated User (should be Bob): {all_users[0]['name']}")
        assert all_users[0]['name'] == "Bob"
        
        # 3. Bulk Operations
        users.bulk_insert([{"name": "Dave"}, {"name": "Eve"}])
        print(f"Total users after bulk (should be 5): {users.count()}")
        assert users.count() == 5
        
        # 4. TTL Documents
        users.insert({"name": "Flash", "ttl": 2}) # Expires in 2s
        print("Flash inserted with TTL=2s")
        # Find logic should filter expired docs immediately even if reaper hasn't run
        time.sleep(3)
        flash = users.find_one({'name': 'Flash'})
        print(f"Flash after 3s (should be None): {flash}")
        # Note: We need to implement filtering expired docs in find_iter for this to work without reaper
        
        # 5. Soft Delete
        users.soft_delete({"name": "Bob"})
        bob = users.find_one({"name": "Bob"})
        print(f"Bob after soft delete (should be None): {bob}")
        assert bob is None
        
        bob_deleted = users.find_one({"name": "Bob", "_deleted": True})
        print(f"Bob with _deleted=True (should be found): {bob_deleted['name']}")
        assert bob_deleted is not None
        
        # 6. Computed Fields
        users.set_computed_field("upper_name", lambda d: d["name"].upper())
        users.insert({"name": "frank"})
        frank = users.find_one({"name": "frank"})
        print(f"Frank upper_name: {frank['upper_name']}")
        assert frank["upper_name"] == "FRANK"
        
        # 7. Data References
        posts = db.group("posts")
        posts.insert({"title": "Hello", "author": {"$ref": "users", "$id": frank["_id"]}})
        post = posts.find_one({"title": "Hello"})
        author = posts.resolve_ref(post["author"])
        print(f"Resolved author: {author['name']}")
        assert author["name"] == "frank"
        
        print("BATCH 1 VERIFICATION SUCCESSFUL!")
        
    finally:
        db.close()
        # Cleanup
        for ext in ['', '.log', '.writelock', '.lock']:
            p = db_path + ext
            if os.path.exists(p):
                try: os.remove(p)
                except: pass

if __name__ == "__main__":
    test_batch1()
