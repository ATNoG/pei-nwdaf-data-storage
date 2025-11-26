from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime, timezone
from influxdb_client.client.write.point import Point
from typing import Optional

RAW:str = "raw"

class Raw(BaseModel):
    # Temporal
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    # Signal Quality (prediction targets)
    datarate: Optional[float] = None
    mean_latency: Optional[float] = None
    rsrp: Optional[float] = None  # Signal power
    sinr: Optional[float] = None  # Interference + noise
    rsrq: Optional[float] = None  # Signal quality

    # Iperf
    direction: Optional[str] = None

    # Cell Data
    network: Optional[str] = None
    cqi: Optional[float] = None  # Cell quality
    cell_index: Optional[int] = None
    primary_bandwidth: Optional[float] = None  # Download
    ul_bandwidth: Optional[float] = None  # Upload

    model_config = ConfigDict(
        json_encoders={datetime: lambda v: v.isoformat()},
        from_attributes=True
    )

    def to_point(self) -> Point:
        """Convert to InfluxDB Point for writing"""
        point = Point(RAW).time(self.timestamp)

        # Tags (indexed, used for filtering/grouping)
        tags = ["cell_index", "network"]

        # Add tags and fields
        for key, value in self.model_dump(exclude={"timestamp"}).items():
            if value is not None:
                if key in tags:
                    point.tag(key, str(value))
                else:
                    point.field(key, value)
        return point
