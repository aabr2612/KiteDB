from typing import Dict, Any, List
from src.logging.logger import logger

class IndexManager:
    def __init__(self):
        self.index: Dict[str, Dict[Any, List[int]]] = {}

    def build(self, doc: Dict[str, Any], doc_id: int):
        for k, v in doc.items():
            self.add(k, v, doc_id)

    def add(self, field: str, value: Any, doc_id: int):
        self.index.setdefault(field, {}).setdefault(value, []).append(doc_id)
        logger.debug(f"Index add: {field}={value} @ {doc_id}")

    def add_bulk(self, doc: Dict[str, Any], doc_id: int):
        self.build(doc, doc_id)

    def remove_bulk(self, doc: Dict[str, Any], doc_id: int):
        for k, v in doc.items(): self.remove(k, v, doc_id)

    def remove(self, field: str, value: Any, doc_id: int):
        lst = self.index.get(field, {}).get(value, [])
        if doc_id in lst: lst.remove(doc_id)
        logger.debug(f"Index remove: {field}={value} @ {doc_id}")

    def reindex(self, old: Dict[str, Any], new: Dict[str, Any], doc_id: int):
        self.remove_bulk(old, doc_id)
        self.add_bulk(new, doc_id)

    def query(self, field: str, value: Any) -> List[int]:
        return self.index.get(field, {}).get(value, [])