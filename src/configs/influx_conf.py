import logging
import os
from dotenv import load_dotenv

from src.configs.conf import Conf

logger = logging.getLogger("Config")

class InfluxConf(Conf):
    url:    str
    token:  str
    org:    str
    bucket: str

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
        load_dotenv(file)

        cls.url    = os.getenv("INFLUX_URL", "http://localhost:8086")
        cls.token  = os.getenv("INFLUX_TOKEN", "")
        cls.org    = os.getenv("INFLUX_ORG", "myorg")
        cls.bucket = os.getenv("INFLUX_BUCKET", "mybucket")

        cls._loaded = True
        logger.info("InfluxDB configuration loaded")

    @classmethod
    def get(cls) -> dict:
        if not cls._loaded:
            cls.load_env()

        return {
            "url": cls.url,
            "token": cls.token,
            "org": cls.org,
            "bucket": cls.bucket,
        }
