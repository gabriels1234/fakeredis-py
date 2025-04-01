import os
import json
import fnmatch
import time
from typing import Dict, Any, Optional, Generator
from .storage_mixin import StorageMixin

class Json2Storage(StorageMixin):
    def __init__(self, storage_dir: Optional[str] = None):
        super().__init__(storage_dir or "/tmp/fakeredis2")

    def _file_path(self, server_id: str, db_num: int, key: bytes) -> str:
        return os.path.join(self._db_path(server_id, db_num), self._encode_key(key) + ".json")

    def save_key(self, server_id: str, db_num: int, key: bytes, value: Any, expireat: Optional[float] = None):
        path = self._file_path(server_id, db_num, key)
        item = {
            '__type': 'item',
            'value': self._make_serializable(value),
            'expireat': expireat
        }
        with self._get_lock(server_id, db_num):
            with open(path, 'w') as f:
                json.dump(item, f)

    def load_key(self, server_id: str, db_num: int, key: bytes) -> Optional[Any]:
        path = self._file_path(server_id, db_num, key)
        if not os.path.exists(path):
            return None
        with self._get_lock(server_id, db_num):
            try:
                with open(path, 'r') as f:
                    obj = json.load(f)
                if self.is_expired(obj.get('expireat')):
                    os.remove(path)
                    return None
                return self._deserialize_value(obj['value'])
            except Exception:
                return None

    def del_key(self, server_id: str, db_num: int, key: bytes) -> bool:
        path = self._file_path(server_id, db_num, key)
        if os.path.exists(path):
            os.remove(path)
            return True
        return False

    def ttl(self, server_id: str, db_num: int, key: bytes) -> int:
        path = self._file_path(server_id, db_num, key)
        if not os.path.exists(path):
            return -2
        with self._get_lock(server_id, db_num):
            try:
                with open(path, 'r') as f:
                    obj = json.load(f)
                return self.ttl_seconds(obj.get('expireat'))
            except Exception:
                return -2

    def get_all_keys(self, server_id: str, db_num: int) -> Generator[bytes, None, None]:
        db_dir = self._db_path(server_id, db_num)
        for fname in os.listdir(db_dir):
            try:
                key = self._decode_key(fname[:-5])
                if self.load_key(server_id, db_num, key) is not None:
                    yield key
            except Exception:
                continue

    def scan_keys(self, server_id: str, db_num: int, pattern: str) -> Generator[bytes, None, None]:
        db_dir = self._db_path(server_id, db_num)
        for fname in os.listdir(db_dir):
            try:
                key = self._decode_key(fname[:-5])
                if fnmatch.fnmatch(key.decode('utf-8', errors='ignore'), pattern):
                    if self.load_key(server_id, db_num, key) is not None:
                        yield key
            except Exception:
                continue

    def flushdb(self, server_id: str, db_num: int):
        db_dir = self._db_path(server_id, db_num)
        for fname in os.listdir(db_dir):
            os.remove(os.path.join(db_dir, fname))

    def flushall(self):
        for server_id in os.listdir(self.storage_dir):
            server_path = os.path.join(self.storage_dir, server_id)
            if os.path.isdir(server_path):
                for db in os.listdir(server_path):
                    db_path = os.path.join(server_path, db)
                    for fname in os.listdir(db_path):
                        os.remove(os.path.join(db_path, fname))
