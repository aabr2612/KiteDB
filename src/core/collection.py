from threading import Lock
from typing import Dict, List, Any
from src.logging.logger import logger
from src.core.exceptions import ValidationError
from src.query.query_parser import QueryParser

class Collection:
    def __init__(self, db: 'Database', name: str):
        self.db = db
        self.name = name
        self.lock = Lock()

    def insert(self, doc: Dict[str, Any], apply_transaction: bool = False) -> Any:
        with self.lock:
            if not isinstance(doc, dict):
                raise ValidationError("Document must be a dictionary")
            try:
                if self.db.transaction and self.db.transaction.active and not apply_transaction:
                    self.db.transaction.log({'collection': self.name, 'action': 'insert', 'params': [doc]})
                    logger.debug(f"Logged insert in '{self.name}': {doc}")
                    return "logged"
                else:
                    docs = self.db.collections[self.name]
                    doc_id = len(docs)
                    docs.append(doc)
                    self.db.indexes[self.name].add_bulk(doc, doc_id)
                    self.db.save()
                    logger.info(f"Inserted into '{self.name}': {doc} with ID {doc_id}")
                    return doc_id
            except Exception as e:
                logger.error(f"Insert failed in '{self.name}': {e}")
                raise KiteDBError(f"Insert operation failed: {e}")

    def find(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        with self.lock:
            try:
                results = []
                for doc in self.db.collections[self.name]:
                    if self._match(doc, query):
                        results.append(doc.copy())
                logger.info(f"Find in '{self.name}' with query {query} returned {len(results)} docs")
                return results
            except Exception as e:
                logger.error(f"Find failed in '{self.name}': {e}")
                raise KiteDBError(f"Find operation failed: {e}")

    def update(self, query: Dict[str, Any], update: Dict[str, Any], apply_transaction: bool = False) -> Any:
        with self.lock:
            if not isinstance(update, dict):
                raise ValidationError("Update must be a dictionary")
            try:
                if self.db.transaction and self.db.transaction.active and not apply_transaction:
                    self.db.transaction.log({'collection': self.name, 'action': 'update', 'params': [query, update]})
                    logger.debug(f"Logged update in '{self.name}': query={query}, update={update}")
                    return "logged"
                else:
                    count = 0
                    docs = self.db.collections[self.name]
                    for idx, doc in enumerate(docs):
                        if self._match(doc, query):
                            old = doc.copy()
                            doc.update(update)
                            self.db.indexes[self.name].reindex(old, doc, idx)
                            count += 1
                    if count:
                        self.db.save()
                    logger.info(f"Updated {count} docs in '{self.name}' with query {query}")
                    return count
            except Exception as e:
                logger.error(f"Update failed in '{self.name}': {e}")
                raise KiteDBError(f"Update operation failed: {e}")

    def delete(self, query: Dict[str, Any], apply_transaction: bool = False) -> Any:
        with self.lock:
            try:
                if self.db.transaction and self.db.transaction.active and not apply_transaction:
                    self.db.transaction.log({'collection': self.name, 'action': 'delete', 'params': [query]})
                    logger.debug(f"Logged delete in '{self.name}': query={query}")
                    return "logged"
                else:
                    count = 0
                    new_docs = []
                    docs = self.db.collections[self.name]
                    for idx, doc in enumerate(docs):
                        if self._match(doc, query):
                            self.db.indexes[self.name].remove_bulk(doc, idx)
                            count += 1
                        else:
                            new_docs.append(doc)
                    if count:
                        self.db.collections[self.name] = new_docs
                        self.db.save()
                    logger.info(f"Deleted {count} docs from '{self.name}' with query {query}")
                    return count
            except Exception as e:
                logger.error(f"Delete failed in '{self.name}': {e}")
                raise KiteDBError(f"Delete operation failed: {e}")

    def _match(self, doc: Dict[str, Any], query: Dict[str, Any]) -> bool:
        return QueryParser.match(doc, query)