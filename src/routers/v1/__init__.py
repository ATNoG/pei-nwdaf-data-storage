from fastapi import APIRouter
from src.routers.v1.latency_router import router as latencyR
from src.routers.v1.raw_router import router as rawR

v1_router = APIRouter()

v1_router.include_router(latencyR, prefix="/processed", tags=["v1", "latency"])
v1_router.include_router(rawR, prefix="/raw", tags=["v1", "latency"])
