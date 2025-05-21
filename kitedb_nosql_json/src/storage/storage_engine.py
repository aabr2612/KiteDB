import os
import pickle
from typing import Dict, Any
from src.config import config
from src.config import logger
from src.core.exceptions import StorageError
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
import shutil


class StorageEngine:
    def __init__(self, db_name: str):
        """Initialize the StorageEngine with a database name and set up encryption."""
        self.db_name = db_name
        self.db_path = os.path.join(config.get("storage.data_root"), db_name)
        os.makedirs(self.db_path, exist_ok=True)
        self.key = config.get("storage.encryption_key").encode()
        if len(self.key) not in (16, 24, 32):
            raise StorageError("Encryption key must be 16, 24, or 32 bytes")
        logger.debug(f"StorageEngine for '{db_name}' at '{self.db_path}'")

    def _chunk_path(self, chunk_id: int) -> str:
        """Generate the file path for a specific data chunk."""
        return os.path.join(self.db_path, f"chunk_{chunk_id}.bin")

    def _encrypt(self, data: bytes) -> bytes:
        """Encrypt the provided data using AES-CBC with padding."""
        cipher = AES.new(self.key, AES.MODE_CBC)
        ct_bytes = cipher.encrypt(pad(data, AES.block_size))
        return cipher.iv + ct_bytes

    def _decrypt(self, data: bytes) -> bytes:
        """Decrypt the provided data using AES-CBC with padding."""
        if len(data) < 16:
            raise StorageError("Invalid encrypted data: too short")
        iv = data[:16]
        ct = data[16:]
        cipher = AES.new(self.key, AES.MODE_CBC, iv=iv)
        try:
            pt = unpad(cipher.decrypt(ct), AES.block_size)
            return pt
        except ValueError as e:
            raise StorageError(f"Decryption failed: {e}")

    def load(self) -> Dict[str, Any]:
        """Load and decrypt all data chunks from disk into a dictionary."""
        data = {"collections": {}, "schemas": {}}
        chunk = 0
        while True:
            path = self._chunk_path(chunk)
            if not os.path.isfile(path):
                break
            try:
                with open(path, "rb") as f:
                    encrypted = f.read()
                decrypted = self._decrypt(encrypted)
                part = pickle.loads(decrypted)
                for coll, docs in part.get("collections", {}).items():
                    data["collections"].setdefault(coll, []).extend(docs)
                data["schemas"].update(part.get("schemas", {}))
                logger.debug(f"Loaded chunk {chunk}")
            except Exception as e:
                logger.error(f"Failed to load chunk {path}: {e}")
                raise StorageError(f"Load error: {e}")
            chunk += 1
        return data

    def save(self, data: Dict[str, Any]) -> None:
        """Save the provided data to disk in encrypted chunks, checking disk space."""
        total, used, free = shutil.disk_usage(self.db_path)
        estimated_size = len(pickle.dumps(data)) * 1.5
        if free < estimated_size:
            raise StorageError("Insufficient disk space to save data")

        collections = data.get("collections", {})
        schemas = data.get("schemas", {})
        docs = list(collections.items())
        chunk_size = config.get("storage.chunk_size")
        for i in range(0, len(docs), chunk_size):
            chunk_id = i // chunk_size
            part = dict(docs[i : i + chunk_size])
            chunk_data = {"collections": part, "schemas": schemas}
            path = self._chunk_path(chunk_id)
            try:
                serialized = pickle.dumps(chunk_data)
                encrypted = self._encrypt(serialized)
                with open(path, "wb") as f:
                    f.write(encrypted)
                logger.info(f"Saved chunk {chunk_id}")
            except Exception as e:
                logger.error(f"Failed to save chunk {path}: {e}")
                raise StorageError(f"Save error: {e}")