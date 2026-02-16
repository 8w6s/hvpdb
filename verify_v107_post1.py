import time
import os
import shutil
from hvpdb import HVPDB

TEST_DB = "./test_post1.hvp"

def setup():
    if os.path.exists(TEST_DB):
        try:
            os.remove(TEST_DB)
        except: pass
    if os.path.exists(TEST_DB + ".log"):
        try:
            os.remove(TEST_DB + ".log")
        except: pass
    if os.path.exists(TEST_DB + ".writelock"):
        try:
            os.remove(TEST_DB + ".writelock")
        except: pass
    
    # Clean up hvp directory if exists (legacy clutter)
    if os.path.exists("hvp") and os.path.isdir("hvp"):
        shutil.rmtree("hvp", ignore_errors=True)

    return HVPDB(TEST_DB, "password")

def test_ttl_reaper_logic():
    print("--- Testing TTL Reaper Logic ---")
    db = setup()
    cache = db.group("cache")
    
    # 1. Insert expired doc
    cache.insert({"_id": "expired_doc", "val": 1, "ttl": 0.1})
    time.sleep(0.5) # Ensure it is expired
    
    # 2. Verify normal find hides it
    docs = list(cache.find({}))
    assert len(docs) == 0, "Expired doc should be hidden by default"
    print("[PASS] Normal find hides expired doc")
    
    # 3. Verify _include_expired finds it
    docs = list(cache.find({"_include_expired": True}))
    assert len(docs) == 1, "_include_expired should find expired doc"
    assert docs[0]["_id"] == "expired_doc"
    print("[PASS] _include_expired finds expired doc")
    
    # 4. Trigger Reaper Logic (Manually simulate what core.py does)
    now = time.time()
    query = {"_expires_at": {"$lt": now}, "_include_expired": True}
    expired_ids = [d["_id"] for d in cache.find(query) if "_expires_at" in d]
    
    assert "expired_doc" in expired_ids
    print(f"[PASS] Reaper query found {len(expired_ids)} expired docs")
    
    # Reaper Delete
    count = cache.delete({"_id": {"$in": expired_ids}, "_include_expired": True})
    assert count == 1, "Should delete 1 doc"
    
    # 5. Verify physical deletion
    docs = list(cache.find({"_include_expired": True}))
    assert len(docs) == 0, "Doc should be physically deleted"
    print("[PASS] Reaper delete successfully removed doc")

def main():
    try:
        test_ttl_reaper_logic()
        print("\n[PASS] All v1.0.7.post1 checks passed!")
    except Exception as e:
        print(f"\n[FAIL] Test failed: {e}")
        import traceback
        traceback.print_exc()
        exit(1)

if __name__ == "__main__":
    main()
