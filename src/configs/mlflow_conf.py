import logging
import os

from src.configs.conf import Conf

logger = logging.getLogger("Config")

class MLflowConf(Conf):
    tracking_uri: str
    experiment_name: str

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

        cls.tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
        cls.experiment_name = os.getenv("MLFLOW_EXPERIMENT_NAME", "default")

        cls._loaded = True
        logger.info("MLflow configuration loaded")

    @classmethod
    def get(cls) -> dict:
        if not cls._loaded:
            cls.load_env()

        return {
            "tracking_uri": cls.tracking_uri,
            "experiment_name": cls.experiment_name,
        }