from fastapi import APIRouter
from src.routers.v1 import latency

v1_router = APIRouter()

v1_router.include_router(latency.router, prefix="/processed", tags=["v1", "latency"])
