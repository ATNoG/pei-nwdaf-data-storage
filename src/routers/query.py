
"""
Endpoints for query data
"""

from fastapi import APIRouter, HTTPException
from src.configs.clickhouse_conf import ClickhouseConf
from src.configs.influx_conf import InfluxConf
from src.services.clickhouse import ClickHouseService
from src.services.influx import InfluxService
from src.models.raw import Raw

router =    APIRouter()

class Influx():
    conf =      InfluxConf()
    service =   InfluxService()

class ClickHouse():
    conf =      ClickhouseConf()
    service =   ClickHouseService()

@router.get("/raw/", response_model=list[Raw])
def get_data(param: str, batch_number: int = 0, batch_size: int = 100):
    return Influx.service.get_data(batch_number=batch_number, batch_size=batch_size)

#TODO: queries processed
