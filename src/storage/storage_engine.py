import os
import json
from typing import Dict, Any
from src.config import DATA_ROOT, CHUNK_SIZE
from src.logging.logger import logger
from src.core.exceptions import StorageError

class StorageEngine:
    def __init__(self, db_name: str):
        self.db_name = db_name
        self.db_path = os.path.join(DATA_ROOT, db_name)
        os.makedirs(self.db_path, exist_ok=True)
        logger.debug(f"StorageEngine for '{db_name}' at '{self.db_path}'")

    def _chunk_path(self, chunk_id: int) -> str:
        return os.path.join(self.db_path, f"chunk_{chunk_id}.json")

    def load(self) -> Dict[str, Any]:
        data = {"collections": {}}
        chunk = 0
        while True:
            path = self._chunk_path(chunk)
            if not os.path.isfile(path): break
            try:
                with open(path) as f:
                    part = json.load(f)
                for coll, docs in part.get("collections", {}).items():
                    data["collections"].setdefault(coll, []).extend(docs)
                logger.debug(f"Loaded chunk {chunk}")
            except Exception as e:
                logger.error(f"Failed to load chunk {path}: {e}")
                raise StorageError(f"Load error: {e}")
            chunk += 1
        return data

    def save(self, data: Dict[str, Any]) -> None:
        collections = data.get("collections", {})
        docs = list(collections.items())
        for i in range(0, len(docs), CHUNK_SIZE):
            chunk_id = i // CHUNK_SIZE
            part = dict(docs[i:i+CHUNK_SIZE])
            path = self._chunk_path(chunk_id)
            try:
                with open(path, 'w') as f:
                    json.dump({"collections": part}, f, indent=2)
                logger.info(f"Saved chunk {chunk_id}")
            except Exception as e:
                logger.error(f"Failed to save chunk {path}: {e}")
                raise StorageError(f"Save error: {e}")