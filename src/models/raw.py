from datetime import datetime, timezone

from influxdb_client.client.write.point import Point

from src.configs.schema_conf import SchemaConf

RAW_MEASUREMENT: str = "raw"


class Raw:
    def __init__(self, **data) -> None:

        core_fields = SchemaConf.get_core_fields()
        self.tags = SchemaConf.get_tags()

        # validate core fields existence
        for field_name in core_fields.keys():
            if field_name not in data:
                raise ValueError(f"Missing core field: {field_name}")

        filtered_data = {k: v for k, v in data.items()}
        self.data = self._validate_types(filtered_data)

        # Do not change the following if statement if timestamp is not ensured to be present in data
        # The current approach allows core features to fe configurable but influxdb always needs timestamp
        if "timestamp" not in self.data:
            self.data["timestamp"] = datetime.now(timezone.utc)

    def _validate_types(self, data: dict) -> dict:
        core_fields = SchemaConf.get_core_fields()

        validated = {}
        for key, value in data.items():
            if value is None or key not in core_fields:
                validated[key] = value
                continue

            expected_type = core_fields.get(key)
            if expected_type:
                validated[key] = self._cast_type(key, value, expected_type)
            else:
                # this isn't reached if method is not called without proper order
                validated[key] = value

        return validated

    def _cast_type(self, field_name: str, value, expected_type: type):
        """Cast value to type"""
        if expected_type is datetime:
            if isinstance(value, datetime):
                return value

            # Handle Unix timestamp (int/float)
            if isinstance(value, (int, float)):
                return datetime.fromtimestamp(value, tz=timezone.utc)

            # Handle ISO format string
            return datetime.fromisoformat(str(value))

        try:
            return expected_type(value)
        except (ValueError, TypeError) as e:
            raise ValueError(
                f"Field '{field_name}' expected {expected_type.__name__}, got {type(value).__name__}: {e}"
            )

    def to_point(self) -> Point:
        timestamp = self.data["timestamp"]
        point = Point(RAW_MEASUREMENT).time(timestamp)

        # fill point
        for key, value in self.data.items():
            if key != "timestamp" and value is not None:
                if key in self.tags:
                    point.tag(key, str(value))
                elif isinstance(value, (int, float, bool)):
                    point.field(key, float(value))
                else:
                    point.field(key, value)

        return point

    def model_dump(self, **kwargs):
        return self.data.copy()
