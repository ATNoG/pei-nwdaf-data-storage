import logging
import os
from pathlib import Path
from typing import Any

import yaml

from src.configs.conf import Conf

logger = logging.getLogger("Config")


class SchemaConf(Conf):
    core_fields: dict[str, Any] = {}
    extra_fields: dict[str, Any] = {}

    @classmethod
    def _read_yml(cls, file_path: Path):
        """Process yml file and save its content on target_variable"""

        target_variable = None
        with open(file_path, "r") as f:
            target_variable = yaml.safe_load(f)

        return target_variable

    @classmethod
    def load_yml(cls) -> None:

        core_fields_path_str: str = os.getenv(
            "CORE_FIELDS_PATH", "confs/core_fields.yml"
        )
        extra_fields_path_str: str = os.getenv(
            "EXTRA_FIELDS_PATH", "confs/extra_fields.yml"
        )

        core_fields_path: Path = Path(core_fields_path_str)
        extra_fields_path: Path = Path(extra_fields_path_str)

        if core_fields_path.exists():
            cls.core_fields = cls._read_yml(core_fields_path)
            logger.info(f"Loaded {len(cls.core_fields)} core fields")
        else:
            logger.warning(f"[{core_fields_path.absolute().resolve()}] not found")

        if extra_fields_path.exists():
            cls.extra_fields = cls._read_yml(extra_fields_path)
            logger.info(f"Loaded {len(cls.extra_fields)} extra fields")
        else:
            logger.warning(f"[{extra_fields_path.absolute().resolve()}] not found")

    @classmethod
    def load(cls) -> None:
        cls.load_yml()

    @classmethod
    def get(cls) -> dict:
        """Get all schema configuration"""
        return {
            "core_fields": cls.core_fields,
            "extra_fields": cls.extra_fields,
        }

    @classmethod
    def get_extra_fields(cls) -> dict[str, Any]:
        """Get set of all allowed field names"""
        return cls.extra_fields

    @classmethod
    def get_core_fields(cls) -> dict[str, Any]:
        """Get set of required core field names"""
        return cls.core_fields
