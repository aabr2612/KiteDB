from threading import Lock
from typing import List, Dict
from src.config import logger
from src.core.exceptions import TransactionError

class Transaction:
    def __init__(self, db: 'Database'):
        self.db = db
        self.lock = Lock()
        self.active = False
        self.ops: List[Dict] = []

    def begin(self):
        with self.lock:
            if self.active:
                raise TransactionError("Transaction already active")
            self.active = True
            self.ops.clear()
            logger.info("Transaction begun")

    def log(self, op: Dict):
        with self.lock:
            if not self.active:
                raise TransactionError("No active transaction")
            self.ops.append(op)
            logger.debug(f"Logged operation: {op}")

    def commit(self) -> None:
        with self.lock:
            if not self.active:
                raise TransactionError("No active transaction")
            try:
                for op in self.ops:
                    coll_name = op['collection']
                    action = op['action']
                    params = op['params']
                    if action == 'add':
                        coll = self.db.get_collection(coll_name)
                        coll.insert(params[0], apply_transaction=True)
                    elif action == 'update':
                        coll = self.db.get_collection(coll_name)
                        coll.update(params[0], params[1], apply_transaction=True)
                    elif action == 'delete':
                        coll = self.db.get_collection(coll_name)
                        coll.delete(params[0], apply_transaction=True)
                    elif action == 'drop':
                        self.db.drop_collection(coll_name)
                self.active = False
                self.ops.clear()
                logger.info("Transaction committed")
            except Exception as e:
                logger.error(f"Commit failed: {e}")
                self.rollback()
                raise TransactionError(f"Commit failed: {e}")

    def rollback(self):
        with self.lock:
            if not self.active:
                logger.warning("Rollback called on inactive transaction")
                return
            self.active = False
            self.ops.clear()
            logger.info("Transaction rolled back")