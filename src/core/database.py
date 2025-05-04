from src.storage.storage_engine import StorageEngine
from src.index.index_manager import IndexManager
from src.core.collection import Collection
from src.core.transaction import Transaction
from src.logging.logger import logger
from src.core.exceptions import KiteDBError

class Database:
    def __init__(self, name: str):
        self.name = name
        self.storage = StorageEngine(name)
        self.collections = {}
        self.indexes = {}
        self.transaction = None
        self._load()

    def _load(self):
        try:
            data = self.storage.load()
            self.collections = data.get('collections', {})
            for coll, docs in self.collections.items():
                idx = IndexManager()
                for i, doc in enumerate(docs):
                    idx.build(doc, i)
                self.indexes[coll] = idx
            logger.info(f"Database '{self.name}' loaded")
        except Exception as e:
            logger.error(f"Load DB failed: {e}")
            raise KiteDBError(e)

    def save(self):
        try:
            self.storage.save({'collections': self.collections})
            logger.info(f"Database '{self.name}' saved")
        except Exception as e:
            logger.error(f"Save DB failed: {e}")
            raise KiteDBError(e)

    def create_collection(self, name: str, schema: dict = None):
        if name in self.collections:
            raise KiteDBError(f"Collection '{name}' exists")
        self.collections[name] = []
        self.indexes[name] = IndexManager()
        self.save()
        logger.info(f"Collection '{name}' created")

    def get_collection(self, name: str):
        if name not in self.collections:
            raise KiteDBError(f"No collection '{name}'")
        return Collection(self, name)

    def begin_transaction(self):
        if self.transaction and self.transaction.active:
            raise KiteDBError("Transaction already active")
        self.transaction = Transaction(self)
        return self.transaction