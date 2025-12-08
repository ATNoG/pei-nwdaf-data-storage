from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime, timezone
from typing import Optional

PROCESSED_LATENCY: str = "processed_latency"


class ProcessedLatency(BaseModel):
    # temporal info
    window_start_time: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    window_end_time: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    window_duration_seconds: float

    # Cell metadata
    cell_index: int
    network: str

    # RSRP
    rsrp_mean: Optional[float] = None
    rsrp_max: Optional[float] = None
    rsrp_min: Optional[float] = None
    rsrp_std: Optional[float] = None

    # SINR
    sinr_mean: Optional[float] = None
    sinr_max: Optional[float] = None
    sinr_min: Optional[float] = None
    sinr_std: Optional[float] = None

    # RSRQ
    rsrq_mean: Optional[float] = None
    rsrq_max: Optional[float] = None
    rsrq_min: Optional[float] = None
    rsrq_std: Optional[float] = None

    # Latency
    latency_mean: Optional[float] = None
    latency_max: Optional[float] = None
    latency_min: Optional[float] = None
    latency_std: Optional[float] = None

    # CQI
    cqi_mean: Optional[float] = None
    cqi_max: Optional[float] = None
    cqi_min: Optional[float] = None
    cqi_std: Optional[float] = None

    # Bandwidth
    primary_bandwidth: Optional[float] = None
    ul_bandwidth: Optional[float] = None

    sample_count: int = Field(ge=1)

    model_config = ConfigDict(
        extra="ignore",
        json_encoders={datetime: lambda v: v.isoformat()},
        from_attributes=True
    )

    def to_dict(self) -> dict:
        """Convert to dictionary for ClickHouse insertion"""
        # Use mode='python' to keep datetime objects as datetime, not strings
        return self.model_dump(mode='python')
