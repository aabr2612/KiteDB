from threading import Lock
from typing import List, Dict
from src.logging.logger import logger
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
                raise TransactionError("Already active")
            self.active = True
            self.ops.clear()
            logger.info("Transaction begun")

    def log(self, op: Dict):
        with self.lock:
            if not self.active:
                raise TransactionError("No active txn")
            self.ops.append(op)
            logger.debug(f"Logged op: {op}")

    def commit(self) -> None:
        with self.lock:
            if not self.active:
                raise TransactionError("No active txn")
            try:
                for op in self.ops:
                    coll = self.db.get_collection(op['collection'])
                    getattr(coll, op['action'])(*op['params'])
                self.active = False
                self.ops.clear()
                logger.info("Transaction committed")
            except Exception as e:
                logger.error(f"Commit failed: {e}")
                self.rollback()
                raise

    def rollback(self):
        with self.lock:
            self.active = False
            self.ops.clear()
            logger.info("Transaction rolled back")