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

    def insert(self, doc: Dict[str, Any]) -> int:
        with self.lock:
            if not isinstance(doc, dict):
                raise ValidationError("Document must be a dict")
            docs = self.db.collections[self.name]
            doc_id = len(docs)
            docs.append(doc)
            self.db.indexes[self.name].add_bulk(doc, doc_id)
            self.db.save()
            logger.info(f"Inserted into '{self.name}': {doc}")
            return doc_id

    def find(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        with self.lock:
            results = []
            for doc in self.db.collections[self.name]:
                if self._match(doc, query):
                    results.append(doc)
            logger.info(f"Find in '{self.name}' returned {len(results)} docs")
            return results

    def update(self, query: Dict[str, Any], update: Dict[str, Any]) -> int:
        with self.lock:
            count = 0
            for idx, doc in enumerate(self.db.collections[self.name]):
                if self._match(doc, query):
                    old = doc.copy()
                    doc.update(update)
                    self.db.indexes[self.name].reindex(old, doc, idx)
                    count += 1
            if count:
                self.db.save()
            logger.info(f"Updated {count} docs in '{self.name}'")
            return count

    def delete(self, query: Dict[str, Any]) -> int:
        with self.lock:
            count = 0
            new_docs = []
            for idx, doc in enumerate(self.db.collections[self.name]):
                if self._match(doc, query):
                    self.db.indexes[self.name].remove_bulk(doc, idx)
                    count += 1
                else:
                    new_docs.append(doc)
            if count:
                self.db.collections[self.name] = new_docs
                self.db.save()
            logger.info(f"Deleted {count} docs from '{self.name}'")
            return count

    def _match(self, doc: Dict[str, Any], query: Dict[str, Any]) -> bool:
        # implement nested fields and operators
        return QueryParser.match(doc, query)