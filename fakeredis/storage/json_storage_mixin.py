import os
import base64
import threading
import time
import json
from typing import Optional, Dict, Any

class JsonStorageMixin:
    def __init__(self, storage_dir: str):
        self.storage_dir = storage_dir
        os.makedirs(storage_dir, exist_ok=True)
        self.locks: Dict[str, threading.Lock] = {}

    def _get_lock(self, server_id: str, db_num: int) -> threading.Lock:
        key = f"{server_id}:{db_num}"
        if key not in self.locks:
            self.locks[key] = threading.Lock()
        return self.locks[key]

    def _encode_key(self, key: bytes) -> str:
        return base64.urlsafe_b64encode(key).decode()

    def _decode_key(self, s: str) -> bytes:
        return base64.urlsafe_b64decode(s)

    def _db_path(self, server_id: str, db_num: int) -> str:
        path = os.path.join(self.storage_dir, server_id, f"db{db_num}")
        os.makedirs(path, exist_ok=True)
        return path

    def _file_path(self, server_id: str, db_num: int, key: bytes, ext: str = ".json") -> str:
        return os.path.join(self._db_path(server_id, db_num), self._encode_key(key) + ext)

    def is_expired(self, expireat: Optional[float]) -> bool:
        return expireat is not None and expireat < time.time()

    def ttl_seconds(self, expireat: Optional[float]) -> int:
        if expireat is None:
            return -1
        now = time.time()
        return int(expireat - now) if expireat > now else -2

    def _make_serializable(self, value: Any) -> Any:
        from fakeredis._commands import Item
        if hasattr(value, '__class__') and value.__class__.__name__ == 'Item':
            return {
                '__type': 'item',
                'value': self._make_serializable(value.value),
                'expireat': value.expireat
            }
        if hasattr(value, 'getall'):
            return {'__type': 'hash', 'data': {
                (k.decode('utf-8') if isinstance(k, bytes) else k):
                (v.decode('utf-8') if isinstance(v, bytes) else v)
                for k, v in value.getall().items()
            }}
        if isinstance(value, bytes):
            return {'__type': 'bytes', 'data': value.decode('utf-8')}
        if isinstance(value, list):
            return {'__type': 'list', 'data': [self._make_serializable(v) for v in value]}
        if hasattr(value, 'members'):
            return {'__type': 'set', 'data': [self._make_serializable(m) for m in value.members()]}
        if hasattr(value, 'items'):
            return {'__type': 'zset', 'data': [
                (self._make_serializable(k), s) for k, s in value.items()
            ]}
        return value

    def _deserialize_value(self, value: Any) -> Any:
        from fakeredis._commands import Item
        if isinstance(value, dict) and '__type' in value:
            t = value['__type']
            if t == 'item':
                item = Item(self._deserialize_value(value['value']))
                item.expireat = value.get('expireat')
                return item
            if t == 'bytes':
                return value['data'].encode('utf-8')
            if t == 'list':
                return [self._deserialize_value(v) for v in value['data']]
            if t == 'set':
                from fakeredis.model import ExpiringMembersSet
                s = ExpiringMembersSet()
                for m in value['data']:
                    s.add(self._deserialize_value(m))
                return s
            if t == 'hash':
                from fakeredis.model import Hash
                h = Hash()
                for k, v in value['data'].items():
                    h[k.encode('utf-8')] = v.encode('utf-8')
                return h
            if t == 'zset':
                from fakeredis.model import ZSet
                z = ZSet()
                for k, score in value['data']:
                    z[self._deserialize_value(k)] = score
                return z
        return value
