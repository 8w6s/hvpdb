import os
from hvpdb import HVPDB

DB_NAME = "checkpoint_crash_db"
PASSWORD = "123"

print("Opening DB after checkpoint crash...")

db = HVPDB(DB_NAME, password=PASSWORD)
logs = db.group("logs")

records = list(logs.find({}))
count = len(records)

print("Recovered records:", count)

db.close()

# In file sizes để kiểm tra consistency
db_folder = f"hvp/{DB_NAME}"
data_file = os.path.join(db_folder, f"{DB_NAME}.hvp")
wal_file = os.path.join(db_folder, f"{DB_NAME}.hvp.log")

if os.path.exists(data_file):
    print("Data file size:", os.path.getsize(data_file), "bytes")

if os.path.exists(wal_file):
    print("WAL file size:", os.path.getsize(wal_file), "bytes")
