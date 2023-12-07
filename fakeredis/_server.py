import inspect
import logging
import queue
import threading
import time
import uuid
import warnings
import weakref
from collections import defaultdict
from typing import Dict, Tuple, Any, List
from abc import ABC, abstractmethod
import redis

from fakeredis._fakesocket import FakeSocket
from fakeredis._helpers import (
    Database,
    PersistentWithLockingWithPostgresDatabase,
    PersistentWithLockingWithSQLiteDatabase,
    PersistentWithLockingDatabase,
    FileLock,
    FakeSelector,
)
from . import _msgs as msgs

LOGGER = logging.getLogger("fakeredis")


def _create_version(v) -> Tuple[int]:
    if isinstance(v, tuple):
        return v  # type: ignore
    if isinstance(v, int):
        return (v,)
    if isinstance(v, str):
        v = v.split(".")
        return tuple(int(x) for x in v)  # type: ignore
    return v


class AbstractServer(ABC):
    @abstractmethod
    def __init__(self, version: Tuple[int]):
        """Initialize the server with a version."""
        pass

    @abstractmethod
    def get_server(self, key: str, version: Tuple[int]) -> "BaseServer":
        """Retrieve or create a server instance."""
        pass


class BaseServer(AbstractServer):
    def __init__(self, version: Tuple[int] = (7,)):
        super().__init__(version)
        self.lock = threading.Lock()
        # Maps channel/pattern to weak set of sockets
        self.subscribers: Dict[bytes, weakref.WeakSet] = defaultdict(weakref.WeakSet)
        self.psubscribers: Dict[bytes, weakref.WeakSet] = defaultdict(weakref.WeakSet)
        self.ssubscribers: Dict[bytes, weakref.WeakSet] = defaultdict(weakref.WeakSet)
        self.lastsave = int(time.time())
        self.connected = True
        # List of weakrefs to sockets that are being closed lazily
        self.closed_sockets: List[Any] = []
        self.version = _create_version(version)


class FakeServer(BaseServer):
    _servers_map: Dict[str, "FakeServer"] = dict()

    def __init__(self, version: Tuple[int] = (7,)):
        super().__init__(version)
        self.dbs: Dict[int, Database] = defaultdict(lambda: Database(self.lock))

    @staticmethod
    def get_server(key, version: Tuple[int]):
        return FakeServer._servers_map.setdefault(key, FakeServer(version=version))


class DBServer(BaseServer):
    _servers_map: Dict[str, "DBServer"] = dict()

    def __init__(
        self,
        conn,
        version: Tuple[int] = (7,),
        table_name: str = "kv_store",
        db_class=PersistentWithLockingWithPostgresDatabase,
    ):
        super().__init__(version)
        self.conn = conn
        self.table_name = table_name
        self.dbs: Dict[int, db_class] = defaultdict(lambda: db_class(self.conn, self.table_name))

    @staticmethod
    def get_server(
        key: str, conn, version: Tuple[int], table_name: str = "kv_store", db_class=PersistentWithLockingWithPostgresDatabase
    ) -> "DBServer":
        return DBServer._servers_map.setdefault(key, DBServer(conn, version=version, table_name=table_name, db_class=db_class))


class FileServer(BaseServer):
    _servers_map: Dict[str, "FileServer"] = dict()

    def __init__(self, version: Tuple[int] = (7,), file_path: str = "redis.json"):
        super().__init__(version)
        self.lock = threading.Lock()
        self.file_path = file_path
        self.dbs: Dict[int, PersistentWithLockingDatabase] = defaultdict(
            lambda: PersistentWithLockingDatabase(FileLock(self.file_path), self.file_path)
        )
        # Maps channel/pattern to weak set of sockets
        self.subscribers: Dict[bytes, weakref.WeakSet] = defaultdict(weakref.WeakSet)
        self.psubscribers: Dict[bytes, weakref.WeakSet] = defaultdict(weakref.WeakSet)
        self.ssubscribers: Dict[bytes, weakref.WeakSet] = defaultdict(weakref.WeakSet)
        self.lastsave = int(time.time())
        self.connected = True
        # List of weakrefs to sockets that are being closed lazily
        self.closed_sockets: List[Any] = []
        self.version = _create_version(version)

    @staticmethod
    def get_server(key: str, version: Tuple[int], file_path: str = "redis.json") -> "FileServer":
        return FileServer._servers_map.setdefault(key, FileServer(version=version, file_path=file_path))

    # Implementation of other methods...


class FakeBaseConnectionMixin:
    def __init__(self, *args, **kwargs):
        self.client_name = None
        self._sock = None
        self._selector = None
        self._server = kwargs.pop("server", None)
        path = kwargs.pop("path", None)
        version = kwargs.pop("version", (7, 0))
        connected = kwargs.pop("connected", True)
        if self._server is None:
            if path:
                self.server_key = path
            else:
                host, port = kwargs.get("host"), kwargs.get("port")
                self.server_key = f"{host}:{port}"
            self.server_key += f":v{version}"
            self._server = FakeServer.get_server(self.server_key, version=version)
            self._server.connected = connected
        super().__init__(*args, **kwargs)


class FakeConnection(FakeBaseConnectionMixin, redis.Connection):
    def connect(self):
        super().connect()
        # The selector is set in redis.Connection.connect() after _connect() is called
        self._selector = FakeSelector(self._sock)

    def _connect(self):
        if not self._server.connected:
            raise redis.ConnectionError(msgs.CONNECTION_ERROR_MSG)
        return FakeSocket(self._server, db=self.db)

    def can_read(self, timeout=0):
        if not self._server.connected:
            return True
        if not self._sock:
            self.connect()
        # We use check_can_read rather than can_read, because on redis-py<3.2,
        # FakeSelector inherits from a stub BaseSelector which doesn't
        # implement can_read. Normally can_read provides retries on EINTR,
        # but that's not necessary for the implementation of
        # FakeSelector.check_can_read.
        return self._selector and self._selector.check_can_read(timeout)

    def _decode(self, response):
        if isinstance(response, list):
            return [self._decode(item) for item in response]
        elif isinstance(response, bytes):
            return self.encoder.decode(
                response,
            )
        else:
            return response

    def read_response(self, **kwargs):
        if not self._sock:
            raise redis.ConnectionError(msgs.CONNECTION_ERROR_MSG)
        if not self._server.connected:
            try:
                response = self._sock.responses.get_nowait()
            except queue.Empty:
                if kwargs.get("disconnect_on_error", True):
                    self.disconnect()
                raise redis.ConnectionError(msgs.CONNECTION_ERROR_MSG)
        else:
            response = self._sock.responses.get()
        if isinstance(response, redis.ResponseError):
            raise response
        if kwargs.get("disable_decoding", False):
            return response
        else:
            return self._decode(response)

    def repr_pieces(self):
        pieces = [("server", self._server), ("db", self.db)]
        if self.client_name:
            pieces.append(("client_name", self.client_name))
        return pieces

    def __str__(self):
        return self.server_key


class FakeRedisMixin:
    def __init__(self, *args, server=None, version=(7,), **kwargs):
        # Interpret the positional and keyword arguments according to the
        # version of redis in use.
        parameters = list(inspect.signature(redis.Redis.__init__).parameters.values())[1:]
        # Convert args => kwargs
        kwargs.update({parameters[i].name: args[i] for i in range(len(args))})
        kwargs.setdefault("host", uuid.uuid4().hex)
        kwds = {
            p.name: kwargs.get(p.name, p.default) for ind, p in enumerate(parameters) if p.default != inspect.Parameter.empty
        }
        if not kwds.get("connection_pool", None):
            charset = kwds.get("charset", None)
            errors = kwds.get("errors", None)
            # Adapted from redis-py
            if charset is not None:
                warnings.warn(DeprecationWarning('"charset" is deprecated. Use "encoding" instead'))
                kwds["encoding"] = charset
            if errors is not None:
                warnings.warn(DeprecationWarning('"errors" is deprecated. Use "encoding_errors" instead'))
                kwds["encoding_errors"] = errors
            conn_pool_args = {
                "host",
                "port",
                "db",
                # Ignoring because AUTH is not implemented
                # 'username',
                # 'password',
                "socket_timeout",
                "encoding",
                "encoding_errors",
                "decode_responses",
                "retry_on_timeout",
                "max_connections",
                "health_check_interval",
                "client_name",
                "connected",
            }
            connection_kwargs = {
                "connection_class": FakeConnection,
                "server": server,
                "version": version,
            }
            connection_kwargs.update({arg: kwds[arg] for arg in conn_pool_args if arg in kwds})
            kwds["connection_pool"] = redis.connection.ConnectionPool(**connection_kwargs)
        kwds.pop("server", None)
        kwds.pop("connected", None)
        kwds.pop("version", None)
        super().__init__(**kwds)

    @classmethod
    def from_url(cls, *args, **kwargs):
        pool = redis.ConnectionPool.from_url(*args, **kwargs)
        # Now override how it creates connections
        pool.connection_class = FakeConnection
        # Using username and password fails since AUTH is not implemented.
        # https://github.com/cunla/fakeredis-py/issues/9
        pool.connection_kwargs.pop("username", None)
        pool.connection_kwargs.pop("password", None)
        return cls(connection_pool=pool)


class FakeStrictRedis(FakeRedisMixin, redis.StrictRedis):
    pass


class FakeRedis(FakeRedisMixin, redis.Redis):
    pass

class SQLiteRedis(FakeRedisMixin, redis.Redis):
    def __init__(self, *args, server=None, version=(7, ), **kwargs):
        server = server or DBServer.get_server("sqlite", None, version=version, db_class=PersistentWithLockingWithSQLiteDatabase)
        super().__init__(*args, server=server, version=version, **kwargs)