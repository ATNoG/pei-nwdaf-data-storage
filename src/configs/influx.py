import logging
import dotenv
import os

from src.configs.conf import Conf

logger = logging.getLogger("Config")

class InfluxConf(Conf):
    url:    str
    token:  str
    org:    str
    bucket: str
