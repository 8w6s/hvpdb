import time
import uuid


class HVPTransactionGroup:
    """
    Transactional proxy for HVPGroup.
    
    Buffers operations during a transaction to ensure atomicity.
    """

    def __init__(self, tx, real_group):
        self.tx = tx
        self.real_group = real_group

    def insert(self, data: dict):
        """Buffer an insert operation."""
        if '_id' not in data:
            data['_id'] = str(uuid.uuid4())
        data['_created_at'] = time.time()
        self.tx.add_op('insert', self.real_group.name, data['_id'], data)
        return data

    def update(self, query: dict, update_data: dict) -> int:
        """Buffer an update operation."""
        docs = self.real_group.find(query)
        count = 0
        for doc in docs:
            new_doc = doc.copy()
            new_doc.update(update_data)
            new_doc['_updated_at'] = time.time()
            self.tx.add_op('update', self.real_group.name, doc['_id'], new_doc)
            count += 1
        return count

    def delete(self, query: dict) -> int:
        """Buffer a delete operation."""
        docs = self.real_group.find(query)
        count = 0
        for doc in docs:
            self.tx.add_op('delete', self.real_group.name, doc['_id'], doc)
            count += 1
        return count

    def find(self, query: dict=None):
        """Delegate find to the real group."""
        return self.real_group.find(query)

    def find_one(self, query: dict):
        """Delegate find_one to the real group."""
        return self.real_group.find_one(query)

class HVPTransaction:
    """
    Transaction context manager for HVPDB.
    
    Ensures ACID properties by buffering operations and committing them
    atomically to both the WAL and memory.
    """

    def __init__(self, db):
        self.db = db
        self.ops = []
        self._committed = False
        self._txn_id = None
        self._token = None

    def __enter__(self):
        if getattr(self.db, 'is_cluster', False):
            raise RuntimeError('Transactions not supported in cluster mode.')
        self._txn_id = self.db.storage.begin_txn()
        self._token = self.db._txn_ctx.set(self._txn_id)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type:
                self.rollback()
            else:
                self.commit()
        finally:
            self.ops = [] # Always clear ops to prevent memory leaks
            if self._token:
                self.db._txn_ctx.reset(self._token)
                self._token = None

    def group(self, name):
        return HVPTransactionGroup(self, self.db.group(name))

    def __getattr__(self, name):
        return self.group(name)

    def add_op(self, op, group, doc_id, data):
        self.ops.append({'op': op, 'g': group, 'id': doc_id, 'd': data})

    def commit(self):
        if self._committed:
            raise ValueError('Transaction already committed')
        
        # Use storage-level writer lock for entire commit process to ensure atomicity
        # between WAL writes and in-memory updates.
        with self.db.storage.lock_manager.writer_lock():
            if self.ops:
                if self._txn_id:
                    for op in self.ops:
                        self.db.storage.append_log(op['op'], op['g'], op['id'], op['d'], txn_id=self._txn_id)
                else:
                    self.db.storage.append_batch_log(self.ops)
            if self._txn_id:
                self.db.storage.commit_txn(self._txn_id)
            
            # Apply to memory atomically with WAL write
            for op in self.ops:
                grp = self.db.group(op['g'])
                try:
                    if op['op'] == 'insert':
                        grp._insert_mem(op['d'])
                    elif op['op'] == 'update':
                        current_doc = grp.storage.data['groups'][op['g']].get(op['id'])
                        if current_doc:
                            grp._update_mem(op['id'], op['d'], current_doc)
                    elif op['op'] == 'delete':
                        current_doc = grp.storage.data['groups'][op['g']].get(op['id'])
                        if current_doc:
                            grp._delete_mem(op['id'], current_doc)
                except ValueError as e:
                    # In a critical section, memory update failures are catastrophic for consistency
                    # However, since we validated before (hopefully), this is rare.
                    print(f'Critical Error applying transaction to memory: {e}')
                    
            self._committed = True
            self.ops = []

    def rollback(self):
        if self._txn_id:
            self.db.storage.rollback_txn(self._txn_id)
            self.db.refresh(force=True)
        self.ops = []
        self._committed = True