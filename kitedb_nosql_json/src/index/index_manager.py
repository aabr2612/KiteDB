from typing import Dict, Any, List
from src.config import logger

class BTreeNode:
    def __init__(self):
        self.keys = []
        self.values = []
        self.children = []

class IndexManager:
    def __init__(self):
        self.index: Dict[str, BTreeNode] = {}
        self._size = 0

    def build(self, doc: Dict[str, Any], doc_id: int):
        for k, v in doc.items():
            self.add(k, v, doc_id)

    def add(self, field: str, value: Any, doc_id: int):
        if field not in self.index:
            self.index[field] = BTreeNode()
        node = self.index[field]
        if value not in node.keys:
            node.keys.append(value)
            node.values.append([doc_id])
            self._size += 1
        else:
            idx = node.keys.index(value)
            if doc_id not in node.values[idx]:
                node.values[idx].append(doc_id)
                self._size += 1
        if self._size > 100000:
            logger.warning(f"Index size for field '{field}' exceeds 100,000 entries; consider persistent indexing")
        logger.debug(f"Index add: {field}={value} @ {doc_id}")

    def add_bulk(self, doc: Dict[str, Any], doc_id: int):
        self.build(doc, doc_id)

    def remove_bulk(self, doc: Dict[str, Any], doc_id: int):
        for k, v in doc.items():
            self.remove(k, v, doc_id)

    def remove(self, field: str, value: Any, doc_id: int):
        node = self.index.get(field)
        if node and value in node.keys:
            idx = node.keys.index(value)
            if doc_id in node.values[idx]:
                node.values[idx].remove(doc_id)
                self._size -= 1
                if not node.values[idx]:
                    node.keys.pop(idx)
                    node.values.pop(idx)
                    self._size -= 1
        logger.debug(f"Index remove: {field}={value} @ {doc_id}")

    def reindex(self, old: Dict[str, Any], new: Dict[str, Any], doc_id: int):
        self.remove_bulk(old, doc_id)
        self.add_bulk(new, doc_id)

    def query(self, field: str, value: Any) -> List[int]:
        node = self.index.get(field)
        if node and value in node.keys:
            return node.values[node.keys.index(value)]
        return []