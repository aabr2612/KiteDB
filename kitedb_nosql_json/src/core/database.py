from src.storage.storage_engine import StorageEngine
from src.index.index_manager import IndexManager
from src.core.collection import Collection
from src.core.transaction import Transaction
from src.config import logger
from src.core.exceptions import KiteDBError
from threading import Lock
import re


class Database:
    def __init__(self, name: str):
        self.name = name
        self.storage = StorageEngine(name)
        self.collections = {}
        self.schemas = {}
        self.indexes = {}
        self.transaction = None
        self._lock = Lock()
        self._load()

    def _load(self):
        """Loads database data from storage and initializes collections and indexes"""
        try:
            data = self.storage.load()
            self.collections = data.get("collections", {})
            self.schemas = data.get("schemas", {})
            for coll, docs in self.collections.items():
                idx = IndexManager()
                for i, doc in enumerate(docs):
                    idx.build(doc, i)
                self.indexes[coll] = idx
            logger.info(f"Database '{self.name}' loaded from '{self.storage.db_path}'")
        except Exception as e:
            logger.error(f"Load database '{self.name}' failed: {e}")
            raise KiteDBError(f"Failed to load database: {e}")

    def save(self):
        """Saves the current state of collections and schemas to storage."""
        try:
            self.storage.save(
                {"collections": self.collections, "schemas": self.schemas}
            )
            logger.info(f"Database '{self.name}' saved")
        except Exception as e:
            logger.error(f"Save database '{self.name}' failed: {e}")
            raise KiteDBError(f"Failed to save database: {e}")

    def create_collection(self, name: str, schema: dict = None):
        """Creates a new collection with an optional schema, validating the name."""
        if not re.match(r"^[a-zA-Z0-9_-]+$", name):
            raise KiteDBError(
                "Collection name must contain only letters, numbers, underscores, or hyphens"
            )
        if name in self.collections:
            raise KiteDBError(f"Collection '{name}' already exists")
        self.collections[name] = []
        self.indexes[name] = IndexManager()
        if schema:
            self.schemas[name] = schema
        self.save()
        logger.info(f"Collection '{name}' created in '{self.name}'")

    def drop_collection(self, name: str):
        """Drops a specified collection, logging the action if a transaction is active."""
        if name not in self.collections:
            raise KiteDBError(f"Collection '{name}' not found")
        with self._lock:
            if self.transaction and self.transaction.active:
                self.transaction.log(
                    {"collection": name, "action": "drop", "params": []}
                )
                logger.debug(f"Logged drop collection '{name}'")
                return "logged"
            self.collections.pop(name, None)
            self.indexes.pop(name, None)
            self.schemas.pop(name, None)
            self.save()
            logger.info(f"Collection '{name}' dropped from '{self.name}'")

    def get_collection(self, name: str) -> Collection:
        """Retrieves a collection object by name for further operations."""
        if name not in self.collections:
            raise KiteDBError(f"Collection '{name}' not found")
        return Collection(self, name)

    def begin_transaction(self):
        """Initiates a new transaction, ensuring no existing transaction is active."""
        with self._lock:
            if self.transaction and self.transaction.active:
                raise KiteDBError("Transaction already active")
            self.transaction = Transaction(self)
            self.transaction.begin()