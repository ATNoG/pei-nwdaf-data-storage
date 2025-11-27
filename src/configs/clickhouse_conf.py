import logging
import os

from src.configs.conf import Conf

logger = logging.getLogger("Config")

class ClickhouseConf(Conf):
    host:     str
    port:     int
    user:     str
    password: str

    _instance = None
    _loaded = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not self._loaded:
            self.load_env()

    @classmethod
    def load_env(cls, file: str = ".env") -> None:

        cls.host     = os.getenv("CLICKHOUSE_HOST", "localhost")
        cls.port     = int(os.getenv("CLICKHOUSE_PORT", "9000"))
        cls.user     = os.getenv("CLICKHOUSE_USER", "default")
        cls.password = os.getenv("CLICKHOUSE_PASSWORD", "")

        cls._loaded = True
        logger.info("ClickHouse configuration loaded")

    @classmethod
    def get(cls) -> dict:
        if not cls._loaded:
            cls.load_env()

        return {
            "host": cls.host,
            "port": cls.port,
            "user": cls.user,
            "password": cls.password,
        }
