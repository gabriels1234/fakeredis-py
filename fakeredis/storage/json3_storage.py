import os
import json
import fnmatch
import time
import threading
from typing import Optional, Generator, Any
from .storage_mixin import StorageMixin

class Json3Storage(StorageMixin):
    def __init__(self, storage_dir: Optional[str] = None):
        super().__init__(storage_dir or "/tmp/fakeredis3")
        self._start_cleanup_thread()

    def _file_path(self, server_id: str, db_num: int, key: bytes) -> str:
        return os.path.join(self._db_path(server_id, db_num), self._encode_key(key) + ".mjson")

    def save_key(self, server_id: str, db_num: int, key: bytes, value: Any, expireat: Optional[float] = None):
        path = self._file_path(server_id, db_num, key)
        value_json = json.dumps(self._make_serializable(value))
        expire_line = str(expireat if expireat is not None else "inf")
        with self._get_lock(server_id, db_num):
            with open(path, "w") as f:
                f.write(expire_line + "\n")
                f.write(value_json)

    def load_key(self, server_id: str, db_num: int, key: bytes) -> Optional[Any]:
        path = self._file_path(server_id, db_num, key)
        if not os.path.exists(path):
            return None
        with self._get_lock(server_id, db_num):
            try:
                with open(path, "r") as f:
                    expire_line = f.readline().strip()
                    expireat = float(expire_line) if expire_line != "inf" else None
                    if self.is_expired(expireat):
                        os.remove(path)
                        return None
                    return self._deserialize_value(json.load(f))
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
                with open(path, "r") as f:
                    expire_line = f.readline().strip()
                    expireat = float(expire_line) if expire_line != "inf" else None
                    return self.ttl_seconds(expireat)
            except Exception:
                return -2

    def get_all_keys(self, server_id: str, db_num: int) -> Generator[bytes, None, None]:
        db_dir = self._db_path(server_id, db_num)
        for fname in os.listdir(db_dir):
            try:
                key = self._decode_key(fname[:-6])  # strip .mjson
                if self.load_key(server_id, db_num, key) is not None:
                    yield key
            except Exception:
                continue

    def scan_keys(self, server_id: str, db_num: int, pattern: str) -> Generator[bytes, None, None]:
        db_dir = self._db_path(server_id, db_num)
        for fname in os.listdir(db_dir):
            try:
                key = self._decode_key(fname[:-6])
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

    def _start_cleanup_thread(self, interval: float = 10.0):
        def cleanup_loop():
            while True:
                self._cleanup_expired()
                time.sleep(interval)

        thread = threading.Thread(target=cleanup_loop, daemon=True)
        thread.start()

    def _cleanup_expired(self):
        now = time.time()
        for server_id in os.listdir(self.storage_dir):
            server_path = os.path.join(self.storage_dir, server_id)
            if not os.path.isdir(server_path):
                continue
            for db in os.listdir(server_path):
                db_path = os.path.join(server_path, db)
                for fname in os.listdir(db_path):
                    full = os.path.join(db_path, fname)
                    try:
                        with open(full, "r") as f:
                            expire_line = f.readline().strip()
                            expireat = float(expire_line) if expire_line != "inf" else None
                            if self.is_expired(expireat):
                                os.remove(full)
                    except Exception:
                        continue
