import threading
from src.configs.clickhouse_conf import ClickhouseConf
from src.services.clickhouse import ClickHouseService
from src.configs.influx_conf import InfluxConf
from src.services.influx import InfluxService


class _ServiceProperty:
    """Descriptor to allow class-level property access for singleton service."""
    def __init__(self, get_service_func):
        self.get_service_func = get_service_func

    def __get__(self, obj, objtype=None):
        # When accessed as ClassName.service, obj is None
        # When accessed as instance.service, obj is the instance
        return self.get_service_func()


class ClickHouse:
    """Singleton wrapper for ClickHouse service with lazy initialization."""
    _instance = None
    _lock = threading.Lock()
    conf = ClickhouseConf()

    @classmethod
    def get_service(cls) -> ClickHouseService:
        """Get the singleton ClickHouse service instance, initializing if needed."""
        if cls._instance is None:
            with cls._lock:
                # Double-check locking pattern
                if cls._instance is None:
                    cls._instance = ClickHouseService()
                    cls._instance.connect()
        return cls._instance

    # Class-level property - allows ClickHouse.service access
    service = _ServiceProperty(lambda: ClickHouse.get_service())


class Influx:
    """Singleton wrapper for InfluxDB service with lazy initialization."""
    _instance = None
    _lock = threading.Lock()
    conf = InfluxConf()

    @classmethod
    def get_service(cls) -> InfluxService:
        """Get the singleton InfluxDB service instance, initializing if needed."""
        if cls._instance is None:
            with cls._lock:
                # Double-check locking pattern
                if cls._instance is None:
                    cls._instance = InfluxService()
                    cls._instance.connect()
        return cls._instance

    # Class-level property - allows Influx.service access
    service = _ServiceProperty(lambda: Influx.get_service())
