
"""
Endpoints for influx db queries
"""

from fastapi import APIRouter, HTTPException
from src.configs.influx import InfluxConf
from src.services.influx import InfluxService
from src.models.raw import Raw

conf =      InfluxConf()
router =    APIRouter()
service =   InfluxService()
RESOURCE_NAMES = ["raw"]

@router.get("/{param}/", response_model=list[Raw])
def get_data(param: str):
    if param not in RESOURCE_NAMES:
        raise HTTPException(status_code=404, detail="Resource not found")

    #TODO: query data using batch_size and batch_number
