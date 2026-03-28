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
        # Apply computed fields before buffering
        if self.real_group._computed_fields:
            for field, func in self.real_group._computed_fields.items():
                data[field] = func(data)
        self.tx.add_op('insert', self.real_group.name, data['_id'], data)
        return data

    def update(self, query: dict, update_data: dict) -> int:
        """Buffer an update operation."""
        docs = self.find(query)
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
        docs = self.find(query)
        count = 0
        for doc in docs:
            self.tx.add_op('delete', self.real_group.name, doc['_id'], doc)
            count += 1
        return count

    def find(self, query: dict=None):
        """Find including uncommitted changes in current transaction."""
        # Start with committed data
        committed_docs = list(self.real_group.find(query))
        
        # Map for easy lookup and mutation
        result_map = {doc['_id']: doc for doc in committed_docs}
        
        # Apply pending ops from this transaction
        for op in self.tx.ops:
            if op['g'] != self.real_group.name:
                continue
                
            op_type = op['op']
            doc_id = op['id']
            data = op['d']
            
            if op_type == 'insert' or op_type == 'update':
                if self.real_group._matches_query(data, query):
                    result_map[doc_id] = data
                elif doc_id in result_map:
                    del result_map[doc_id]
            elif op_type == 'delete':
                if doc_id in result_map:
                    del result_map[doc_id]
                    
        return list(result_map.values())

    def find_one(self, query: dict):
        """Find one doc including uncommitted changes."""
        res = self.find(query)
        return res[0] if res else None

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
        # Phase 13: Allow transactions in Cluster Mode but with caution.
        # Since HVPTransaction uses self.db.storage for locking, it's safe 
        # for single-file mode. In cluster mode, the user must ensure 
        # they only touch groups within the same shard if they want atomicity.
        # FOR NOW: We relax the check but warn if it's cluster.
        if getattr(self.db, 'is_cluster', False):
             pass # Allowed, but atomicity is per-shard
        
        # Prevent nested transactions
        if self.db.current_txn:
            raise RuntimeError(f'Nested transactions are not supported. Current transaction: {self.db.current_txn}')
            
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
        
        try:
            # Use storage-level writer lock for entire commit process to ensure atomicity
            # between WAL writes and in-memory updates.
            with self.db.storage.lock_manager.writer_lock():
                # Validate operations before committing to WAL
                pending_ids = set()
                pending_uniques = {} # {field: set(values)}
                
                for op in self.ops:
                    grp = self.db.group(op['g'])
                    if op['op'] == 'insert':
                        # Check ID conflict
                        if op['id'] in grp.storage.data['groups'][op['g']] or op['id'] in pending_ids:
                            raise ValueError(f"Transaction Aborted: Duplicate ID {op['id']}")
                        pending_ids.add(op['id'])
                        
                        # Check unique index conflict
                        for field, unique_map in grp.unique_indexes.items():
                            val = op['d'].get(field)
                            if val is not None:
                                if field not in pending_uniques: pending_uniques[field] = set()
                                if val in unique_map or val in pending_uniques[field]:
                                    raise ValueError(f"Transaction Aborted: Duplicate unique key '{field}': '{val}'")
                                pending_uniques[field].add(val)

                if self.ops:
                    if self._txn_id:
                        for op in self.ops:
                            self.db.storage.append_log(op['op'], op['g'], op['id'], op['d'], txn_id=self._txn_id)
                    else:
                        self.db.storage.append_batch_log(self.ops)
                if self._txn_id:
                    # Don't trigger checkpoint inside lock to avoid deadlock
                    self.db.storage.commit_txn(self._txn_id, check_auto_checkpoint=False)
                
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
                        # We must force a refresh to resync memory with WAL
                        print(f'Critical Error applying transaction to memory: {e}. Forcing refresh.')
                        self.db.refresh(force=True)
                        raise
                        
                self._committed = True
                self.ops = []

            # Check for auto-checkpoint after releasing the lock
            self.db.storage._check_auto_checkpoint()
            
        except Exception:
            # Ensure cleanup on failure
            self.rollback()
            raise

    def rollback(self):
        if self._txn_id:
            self.db.storage.rollback_txn(self._txn_id)
            # No refresh(force=True) needed anymore
        self.ops = []
        self._committed = True