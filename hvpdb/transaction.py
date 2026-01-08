import uuid
import time

class HVPTransactionGroup:
    """Wrapper for Group within a Transaction context."""
    def __init__(self, tx, real_group):
        self.tx = tx
        self.real_group = real_group

    def insert(self, data: dict):
        # Generate ID early
        if "_id" not in data:
            data["_id"] = str(uuid.uuid4())
        data["_created_at"] = time.time()
        
        # Buffer
        self.tx.add_op("insert", self.real_group.name, data["_id"], data)
        return data

    def update(self, query: dict, update_data: dict) -> int:
        # NOTE: This only finds COMMITTED data (Read Committed Isolation)
        docs = self.real_group.find(query)
        count = 0
        for doc in docs:
            new_doc = doc.copy()
            new_doc.update(update_data)
            new_doc["_updated_at"] = time.time()
            
            self.tx.add_op("update", self.real_group.name, doc["_id"], new_doc)
            count += 1
        return count

    def delete(self, query: dict) -> int:
        docs = self.real_group.find(query)
        count = 0
        for doc in docs:
            self.tx.add_op("delete", self.real_group.name, doc["_id"], doc)
            count += 1
        return count
        
    def find(self, query: dict = None):
        return self.real_group.find(query)
    
    def find_one(self, query: dict):
        return self.real_group.find_one(query)

class HVPTransaction:
    """Manages a buffered atomic transaction."""
    def __init__(self, db):
        self.db = db
        self.ops = []
        self._committed = False
        self._txn_id = None
        self._token = None # ContextVar token

    def __enter__(self):
        # Start transaction on DB
        self._txn_id = self.db.storage.begin_txn()
        # Set Context
        self._token = self.db._txn_ctx.set(self._txn_id)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type:
                self.rollback()
            else:
                self.commit()
        finally:
            # Reset Context
            if self._token:
                self.db._txn_ctx.reset(self._token)
                self._token = None

    def group(self, name):
        return HVPTransactionGroup(self, self.db.group(name))
        
    def __getattr__(self, name):
        return self.group(name)

    def add_op(self, op, group, doc_id, data):
        self.ops.append({"op": op, "g": group, "id": doc_id, "d": data})

    def commit(self):
        if self._committed:
            raise ValueError("Transaction already committed")
            
        # 1. If we have buffered ops (Legacy mode or manual add_op)
        if self.ops:
            # We are in hybrid mode.
            # If we are in __enter__, we have a txn_id.
            # We should push these ops to the storage buffer.
            if self._txn_id:
                 for op in self.ops:
                     self.db.storage.append_log(op["op"], op["g"], op["id"], op["d"], txn_id=self._txn_id)
            else:
                 # Legacy: One-shot commit without explicit begin (implicit)
                 # This calls append_batch_log -> wal.append loop (Slow)
                 # We should optimize this too, but for now focus on Context Mode.
                 self.db.storage.append_batch_log(self.ops)
        
        # 2. Commit the Storage Transaction (if active)
        if self._txn_id:
            self.db.storage.commit_txn(self._txn_id)
            
        # 3. Apply buffered ops to Memory (Only needed if we used manual add_op)
        # If we used direct group.insert(), memory was already updated!
        for op in self.ops:
            grp = self.db.group(op["g"])
            try:
                if op["op"] == "insert":
                    grp._insert_mem(op["d"])
                elif op["op"] == "update":
                    current_doc = grp.storage.data["groups"][op["g"]].get(op["id"])
                    if current_doc:
                        grp._update_mem(op["id"], op["d"], current_doc)
                elif op["op"] == "delete":
                    current_doc = grp.storage.data["groups"][op["g"]].get(op["id"])
                    if current_doc:
                        grp._delete_mem(op["id"], current_doc)
            except ValueError as e:
                print(f"Critical Error applying transaction to memory: {e}")
                
        self._committed = True
        self.ops = []

    def rollback(self):
        if self._txn_id:
            self.db.storage.rollback_txn(self._txn_id)
            # Rollback memory logic is tricky if direct insert was used.
            # HVPGroup.insert catches exception and rolls back memory.
            # But if we used with txn:, and an error occurred in the middle?
            # The exception propagates, causing __exit__ -> rollback.
            # But memory changes from successful inserts BEFORE the error remain in memory!
            # We need to track memory changes in the Transaction object if we want to rollback memory!
            # Or simpler: reload storage on rollback?
            # HVPDB.refresh()!
            self.db.refresh()
            
        self.ops = []
        self._committed = True
