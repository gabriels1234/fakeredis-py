import os
import json
import tempfile
from typing import Dict, Any, Optional
from .json_storage_mixin import JsonStorageMixin  # Assuming you save the mixin in storage_mixin.py

class JsonStorage(JsonStorageMixin):
    """
    Storage implementation that persists Redis data to a single JSON file per DB.
    Suitable for small datasets or quick tests.
    """

    def __init__(self, storage_dir: Optional[str] = None):
        super().__init__(storage_dir or os.path.join(tempfile.gettempdir(), "fakeredis"))

    def get_db_path(self, server_id: str, db_num: int) -> str:
        server_dir = os.path.join(self.storage_dir, server_id)
        os.makedirs(server_dir, exist_ok=True)
        return os.path.join(server_dir, f"db{db_num}.json")

    def load(self, server_id: str, db_num: int) -> Dict[bytes, Any]:
        path = self.get_db_path(server_id, db_num)
        if not os.path.exists(path):
            self.save(server_id, db_num, {})
            return {}

        with self._get_lock(server_id, db_num):
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                result = {}
                for k, v in data.items():
                    key = k.encode('utf-8') if isinstance(k, str) else k
                    result[key] = self._deserialize_value(v)
                return result
            except (json.JSONDecodeError, FileNotFoundError):
                return {}

    def save(self, server_id: str, db_num: int, data: Dict[bytes, Any]) -> None:
        path = self.get_db_path(server_id, db_num)
        serializable_data = {}

        for k, v in data.items():
            if isinstance(k, bytes):
                k = k.decode('utf-8', errors='replace')
            serializable_data[k] = self._make_serializable(v)

        with self._get_lock(server_id, db_num):
            with open(path, 'w') as f:
                json.dump(serializable_data, f)
