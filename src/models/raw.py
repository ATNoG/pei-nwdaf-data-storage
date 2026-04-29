import json
from datetime import datetime, timezone

from influxdb_client.client.write.point import Point

RAW_MEASUREMENT: str = "raw"


class Raw:
    def __init__(self, timestamp, tags: dict, event: str, metrics: dict) -> None:
        if isinstance(timestamp, (int, float)):
            self.timestamp = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        else:
            self.timestamp = timestamp
        self.tags = tags
        self.event = event
        self.metrics = metrics

    def to_point(self) -> Point:
        point = Point(RAW_MEASUREMENT).time(self.timestamp)
        point.tag("event", self.event)
        for k, v in self.tags.items():
            point.tag(k, str(v))
        for k, v in self.metrics.items():
            if isinstance(v, (int, float)):
                point.field(k, float(v))
            elif isinstance(v, (dict, list)):
                point.field(k, json.dumps(v))
            elif v is not None:
                point.field(k, str(v))
        return point
