from threading import Lock
from typing import Dict, List, Any
from src.config import logger
from src.core.exceptions import ValidationError, KiteDBError
from src.query.query_parser import QueryParser


class Collection:
    def __init__(self, db: "Database", name: str):
        self.db = db
        self.name = name
        self.lock = Lock()

    def _map_type(self, type_name: str) -> type:
        """Map string type names to Python types.

        Args:
            type_name (str): The name of the type to map.

        Returns:
            type: The corresponding Python type.

        Raises:
            ValidationError: If the type_name is not found in the type map.
        """
        type_map = {
            "str": str,
            "int": int,
            "float": float,
            "bool": bool,
            "list": list,
            "dict": dict,
        }
        if type_name not in type_map:
            raise ValidationError(f"Unknown type in schema: {type_name}")
        return type_map[type_name]

    def validate_schema(self, doc: Dict[str, Any]):
        """Validate a document against the collection's schema.

        Args:
            doc (Dict[str, Any]): The document to validate.

        Raises:
            ValidationError: If the document does not match the schema requirements.
        """
        schema = self.db.schemas.get(self.name, {})
        if not schema:
            return
        required_fields = schema.get("fields", {})
        for field, type_name in required_fields.items():
            if field not in doc:
                raise ValidationError(f"Missing required field: {field}")
            field_type = self._map_type(type_name)
            if not isinstance(doc[field], field_type):
                raise ValidationError(
                    f"Field {field} must be of type {type_name}, got {type(doc[field]).__name__}"
                )
        for field in doc:
            if field not in required_fields:
                raise ValidationError(f"Unexpected field in document: {field}")

    def insert(self, docs: Any, apply_transaction: bool = False) -> Any:
        """Insert one or more documents into the collection.

        Args:
            docs (Any): A single document or a list of documents to insert.
            apply_transaction (bool): If True, applies the insert directly, bypassing transaction logging.

        Returns:
            Any: List of inserted document IDs or "logged" if transaction is active.

        Raises:
            ValidationError: If a document is not a dictionary or fails schema validation.
            KiteDBError: If the insert operation fails.
        """
        with self.lock:
            documents = docs if isinstance(docs, list) else [docs]
            for doc in documents:
                if not isinstance(doc, dict):
                    raise ValidationError("Document must be a dictionary")
                self.validate_schema(doc)

            try:
                if (
                    self.db.transaction
                    and self.db.transaction.active
                    and not apply_transaction
                ):
                    self.db.transaction.log(
                        {
                            "collection": self.name,
                            "action": "add",
                            "params": [documents],
                        }
                    )
                    logger.debug(
                        f"Logged insert in '{self.name}': {len(documents)} documents"
                    )
                    return "logged"
                else:
                    docs_list = self.db.collections[self.name]
                    start_id = len(docs_list)
                    for i, doc in enumerate(documents):
                        docs_list.append(doc)
                        self.db.indexes[self.name].add_bulk(doc, start_id + i)
                    self.db.save()
                    logger.info(
                        f"Inserted {len(documents)} documents into '{self.name}'"
                    )
                    return list(range(start_id, start_id + len(documents)))
            except Exception as e:
                logger.error(f"Insert failed in '{self.name}': {e}")
                raise KiteDBError(f"Insert operation failed: {e}")

    def find(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Find documents in the collection matching the given query.

        Args:
            query (Dict[str, Any]): The query to filter documents.

        Returns:
            List[Dict[str, Any]]: List of matching documents.

        Raises:
            KiteDBError: If the find operation fails.
        """
        with self.lock:
            try:
                results = []
                if not query:
                    for doc in self.db.collections[self.name]:
                        results.append(doc.copy())
                    logger.info(
                        f"Find in '{self.name}' with empty query returned {len(results)} docs"
                    )
                    return results
                if len(query) == 1 and not any(k.startswith("$") for k in query.keys()):
                    field = next(iter(query))
                    value = query[field]
                    if isinstance(value, dict) and len(value) == 1 and "$eq" in value:
                        doc_ids = self.db.indexes[self.name].query(field, value["$eq"])
                        for doc_id in doc_ids:
                            results.append(
                                self.db.collections[self.name][doc_id].copy()
                            )
                        logger.info(
                            f"Find in '{self.name}' with indexed query {query} returned {len(results)} docs"
                        )
                        return results
                for doc in self.db.collections[self.name]:
                    if self._match(doc, query):
                        results.append(doc.copy())
                logger.info(
                    f"Find in '{self.name}' with query {query} returned {len(results)} docs"
                )
                return results
            except Exception as e:
                logger.error(f"Find failed in '{self.name}': {e}")
                raise KiteDBError(f"Find operation failed: {e}")

    def update(
        self, query: Dict[str, Any], updates: Any, apply_transaction: bool = False
    ) -> Any:
        """Update documents in the collection that match the query.

        Args:
            query (Dict[str, Any]): The query to select documents to update.
            updates (Any): A single update or list of updates to apply.
            apply_transaction (bool): If True, applies the update directly, bypassing transaction logging.

        Returns:
            Any: Number of updated documents or "logged" if transaction is active.

        Raises:
            ValidationError: If an update is not a dictionary or fails schema validation.
            KiteDBError: If the update operation fails.
        """
        with self.lock:
            update_list = updates if isinstance(updates, list) else [updates]
            for update in update_list:
                if not isinstance(update, dict):
                    raise ValidationError("Update must be a dictionary")
                self.validate_schema(update)

            try:
                if (
                    self.db.transaction
                    and self.db.transaction.active
                    and not apply_transaction
                ):
                    self.db.transaction.log(
                        {
                            "collection": self.name,
                            "action": "update",
                            "params": [query, update_list],
                        }
                    )
                    logger.debug(
                        f"Logged update in '{self.name}': query={query}, updates={len(update_list)}"
                    )
                    return "logged"
                else:
                    count = 0
                    docs = self.db.collections[self.name]
                    for idx, doc in enumerate(docs):
                        if self._match(doc, query):
                            old = doc.copy()
                            for update in update_list:
                                doc.update(update)
                            self.db.indexes[self.name].reindex(old, doc, idx)
                            count += 1
                    if count:
                        self.db.save()
                    logger.info(
                        f"Updated {count} docs in '{self.name}' with query {query}"
                    )
                    return count
            except Exception as e:
                logger.error(f"Update failed in '{self.name}': {e}")
                raise KiteDBError(f"Update operation failed: {e}")

    def delete(self, query: Dict[str, Any], apply_transaction: bool = False) -> Any:
        """Delete documents from the collection that match the query.

        Args:
            query (Dict[str, Any]): The query to select documents to delete.
            apply_transaction (bool): If True, applies the delete directly, bypassing transaction logging.

        Returns:
            Any: Number of deleted documents or "logged" if transaction is active.

        Raises:
            KiteDBError: If the delete operation fails.
        """
        with self.lock:
            try:
                if (
                    self.db.transaction
                    and self.db.transaction.active
                    and not apply_transaction
                ):
                    self.db.transaction.log(
                        {"collection": self.name, "action": "delete", "params": [query]}
                    )
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
                    logger.info(
                        f"Deleted {count} docs from '{self.name}' with query {query}"
                    )
                    return count
            except Exception as e:
                logger.error(f"Delete failed in '{self.name}': {e}")
                raise KiteDBError(f"Delete operation failed: {e}")

    def _match(self, doc: Dict[str, Any], query: Dict[str, Any]) -> bool:
        """Check if a document matches the given query.

        Args:
            doc (Dict[str, Any]): The document to check.
            query (Dict[str, Any]): The query to match against.

        Returns:
            bool: True if the document matches the query, False otherwise.
        """
        return QueryParser.match(doc, query)