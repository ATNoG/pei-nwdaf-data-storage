import logging
import os
from datetime import datetime
from pathlib import Path

import yaml

from src.configs.conf import Conf

logger = logging.getLogger("Config")


class SchemaConf(Conf):
    core_fields: dict[str, type] = {}
    extra_fields: dict[str, type] = {}

    _TYPE_MAP = {
        "float": float,
        "integer": int,
        "int": int,
        "string": str,
        "str": str,
        "datetime": datetime,
        "bool": bool,
        "boolean": bool,
    }

    @classmethod
    def _read_yml(cls, file_path: Path):
        """Process yml file and save its content on target_variable"""

        target_variable = None
        with open(file_path, "r") as f:
            target_variable = yaml.safe_load(f)

        return target_variable

    @classmethod
    def _parse_types(cls, fields_dict: dict[str, str]) -> dict[str, type]:
        parsed = {}
        for field_name, type_string in fields_dict.items():
            type_string = type_string.lower().strip()
            if type_string in cls._TYPE_MAP:
                parsed[field_name] = cls._TYPE_MAP[type_string]
            else:
                logger.warning(
                    f"Unknown type [{type_string}] for field [{field_name}], defaulting to string"
                )
                parsed[field_name] = str

        return parsed

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
            raw_core_fields = cls._read_yml(core_fields_path)
            cls.core_fields = cls._parse_types(raw_core_fields)
            logger.info(f"Loaded {len(cls.core_fields)} core fields")
        else:
            logger.warning(f"[{core_fields_path.absolute().resolve()}] not found")

        if extra_fields_path.exists():
            raw_extra_fields = cls._read_yml(extra_fields_path)
            cls.extra_fields = cls._parse_types(raw_extra_fields)
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
