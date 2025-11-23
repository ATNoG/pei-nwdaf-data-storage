from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime, timezone
from influxdb_client.client.write.point import Point

RAW:str = "raw"

class Raw(BaseModel):
    timestamp:datetime = Field(
        default_factory= lambda: datetime.now(timezone.utc)
    )

    # TODO: add fields and config to json
    model_config = ConfigDict(
        json_encoders={datetime: lambda v: v.isoformat()},
        from_attributes=True
    )

    def to_point(self) -> Point:
        """Convert to InfluxDB Point for writing"""
        point = Point(RAW).time(self.timestamp)

        # add remaining values to point
        for key,value in self.model_dump(exclude={"timestamp"}).items():
            if value is not None:
                point.field(key,value)
        return point
