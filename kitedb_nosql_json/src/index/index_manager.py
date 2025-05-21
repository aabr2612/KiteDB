from typing import Dict, Any, List
from src.config import logger


class BTreeNode:
    def __init__(self):
        """Initialize a BTreeNode with empty lists for keys, values, and children."""
        self.keys = []
        self.values = []
        self.children = []


class IndexManager:
    def __init__(self):
        """Initialize the IndexManager with an empty index dictionary and size counter."""
        self.index: Dict[str, BTreeNode] = {}  # Dictionary to store field-to-node mappings
        self._size = 0  # Counter for total number of index entries

    def build(self, doc: Dict[str, Any], doc_id: int):
        """Build an index for all fields in a document by adding each field-value pair."""
        # Iterate through each key-value pair in the document and add to the index
        for k, v in doc.items():
            self.add(k, v, doc_id)

    def add(self, field: str, value: Any, doc_id: int):
        """Add a field-value pair with a document ID to the index."""
        # Create a new BTreeNode for the field if it doesn't exist
        if field not in self.index:
            self.index[field] = BTreeNode()
        node = self.index[field]
        # Add the value and doc_id if the value is not already in the node's keys
        if value not in node.keys:
            node.keys.append(value)
            node.values.append([doc_id])
            self._size += 1
        else:
            # If value exists, append doc_id to the existing value's list if not already present
            idx = node.keys.index(value)
            if doc_id not in node.values[idx]:
                node.values[idx].append(doc_id)
                self._size += 1
        # Warn if index size exceeds threshold to suggest persistent indexing
        if self._size > 100000:
            logger.warning(
                f"Index size for field '{field}' exceeds 100,000 entries; consider persistent indexing"
            )
        logger.debug(f"Index add: {field}={value} @ {doc_id}")

    def add_bulk(self, doc: Dict[str, Any], doc_id: int):
        """Add all field-value pairs of a document to the index in bulk."""
        # Delegate to build method to process all fields in the document
        self.build(doc, doc_id)

    def remove_bulk(self, doc: Dict[str, Any], doc_id: int):
        """Remove all field-value pairs of a document from the index."""
        # Iterate through each key-value pair in the document and remove from the index
        for k, v in doc.items():
            self.remove(k, v, doc_id)

    def remove(self, field: str, value: Any, doc_id: int):
        """Remove a specific field-value pair with a document ID from the index."""
        # Get the node for the specified field, if it exists
        node = self.index.get(field)
        if node and value in node.keys:
            # Find the index of the value in the node's keys
            idx = node.keys.index(value)
            # Remove the doc_id from the value's list if present
            if doc_id in node.values[idx]:
                node.values[idx].remove(doc_id)
                self._size -= 1
                # If the value's list is empty, remove the key and value from the node
                if not node.values[idx]:
                    node.keys.pop(idx)
                    node.values.pop(idx)
                    self._size -= 1
        logger.debug(f"Index remove: {field}={value} @ {doc_id}")

    def reindex(self, old: Dict[str, Any], new: Dict[str, Any], doc_id: int):
        """Reindex a document by removing old field-value pairs and adding new ones."""
        # Remove old document data and add new document data to update the index
        self.remove_bulk(old, doc_id)
        self.add_bulk(new, doc_id)

    def query(self, field: str, value: Any) -> List[int]:
        """Query the index for document IDs associated with a field-value pair."""
        # Get the node for the specified field
        node = self.index.get(field)
        # Return the list of document IDs if the value exists in the node's keys
        if node and value in node.keys:
            return node.values[node.keys.index(value)]
        # Return empty list if field or value not found
        return []