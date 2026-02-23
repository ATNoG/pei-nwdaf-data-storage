from fastapi import APIRouter

from src.routers.v1.cells_router import router as cellR
from src.routers.v1.processed import router as latencyR
from src.routers.v1.raw_router import router as rawR

v1_router = APIRouter()
v1_router.include_router(latencyR, prefix="/processed", tags=["v1", "data"])
v1_router.include_router(rawR, prefix="/raw", tags=["v1", "data"])
v1_router.include_router(cellR, prefix="/cell", tags=["v1", "cell"])
