import logging
import threading
import time
import weakref
from collections import defaultdict
from typing import Dict, Tuple, Any, List, Optional, Union
import uuid

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal


from fakeredis.model import AccessControlList
from fakeredis._helpers import Database, FakeSelector

LOGGER = logging.getLogger("fakeredis")

VersionType = Union[Tuple[int, ...], int, str]

ServerType = Literal["redis", "dragonfly", "valkey"]


def _create_version(v: VersionType) -> Tuple[int, ...]:
    if isinstance(v, tuple):
        return v
    if isinstance(v, int):
        return (v,)
    if isinstance(v, str):
        v_split = v.split(".")
        return tuple(int(x) for x in v_split)
    return v


def _version_to_str(v: VersionType) -> str:
    if isinstance(v, tuple):
        return ".".join(str(x) for x in v)
    return str(v)


class FakeServer:
    _servers_map: Dict[str, "FakeServer"] = dict()
    _storage = None  # Global storage instance
    _shared_server_id = None  # For persistence between instances
    
    @classmethod
    def configure_storage(cls, storage_dir=None):
        """
        Configure storage for all FakeServer instances
        
        Args:
            storage_dir: Directory to store JSON files, if None uses a temp directory
        """
        from fakeredis.storage.json_storage import JSONStorage
        cls._storage = JSONStorage(storage_dir)
        # Set a shared server ID for persistence if not already set
        if cls._shared_server_id is None:
            cls._shared_server_id = str(uuid.uuid4())

    def __init__(
        self,
        version: VersionType = (7,),
        server_type: ServerType = "redis",
        config: Dict[bytes, bytes] = None,
    ) -> None:
        """Initialize a new FakeServer instance.
        :param version: The version of the server (e.g. 6, 7.4, "7.4.1", can also be a tuple)
        :param server_type: The type of server (redis, dragonfly, valkey)
        :param config: A dictionary of configuration options.

        Configuration options:
        - `requirepass`: The password required to authenticate to the server.
        - `aclfile`: The path to the ACL file.
        """
        # Use shared server ID for persistent storage if available
        if self._storage and self._shared_server_id:
            self.server_id = self._shared_server_id
        else:
            # Generate a unique server ID if not using persistent storage
            self.server_id = str(uuid.uuid4())
        self.lock = threading.Lock()
        self.dbs: Dict[int, Database] = {}  # Don't use defaultdict for persistence
        # Maps channel/pattern to a weak set of sockets
        self.subscribers: Dict[bytes, weakref.WeakSet[Any]] = defaultdict(weakref.WeakSet)
        self.psubscribers: Dict[bytes, weakref.WeakSet[Any]] = defaultdict(weakref.WeakSet)
        self.ssubscribers: Dict[bytes, weakref.WeakSet[Any]] = defaultdict(weakref.WeakSet)
        self.lastsave: int = int(time.time())
        self.connected = True
        # List of weakrefs to sockets that are being closed lazily
        self.closed_sockets: List[Any] = []
        self.version: Tuple[int, ...] = _create_version(version)
        if server_type not in ("redis", "dragonfly", "valkey"):
            raise ValueError(f"Unsupported server type: {server_type}")
        self.server_type: str = server_type
        self.config: Dict[bytes, bytes] = config or dict()
        self.acl: AccessControlList = AccessControlList()

    def get_db(self, db_num: int) -> Database:
        """Get or create a database with specified number"""
        if db_num not in self.dbs:
            self.dbs[db_num] = Database(
                self.lock, 
                storage=self._storage,
                server_id=self.server_id,
                db_num=db_num
            )
            
            # Always initialize db 0 if we have storage enabled
            if self._storage and db_num != 0 and 0 not in self.dbs:
                self.get_db(0)  # Initialize db 0
                
        return self.dbs[db_num]

    @staticmethod
    def get_server(key: str, version: VersionType, server_type: ServerType) -> "FakeServer":
        if key not in FakeServer._servers_map:
            FakeServer._servers_map[key] = FakeServer(version=version, server_type=server_type)
        return FakeServer._servers_map[key]


class FakeBaseConnectionMixin(object):
    def __init__(
        self, *args: Any, version: VersionType = (7, 0), server_type: ServerType = "redis", **kwargs: Any
    ) -> None:
        self.client_name: Optional[str] = None
        self.server_key: str
        self._sock = None
        self._selector: Optional[FakeSelector] = None
        self._server = kwargs.pop("server", None)
        self._lua_modules = kwargs.pop("lua_modules", set())
        path = kwargs.pop("path", None)
        connected = kwargs.pop("connected", True)
        if self._server is None:
            if path:
                self.server_key = path
            else:
                host, port = kwargs.get("host"), kwargs.get("port")
                self.server_key = f"{host}:{port}"
            self.server_key += f":{server_type}:v{_version_to_str(version)[0]}"
            self._server = FakeServer.get_server(self.server_key, server_type=server_type, version=version)
            self._server.connected = connected
        super().__init__(*args, **kwargs)
