from fastapi import APIRouter

from src.routers.v1.processed import router as latencyR
from src.routers.v1.raw_router import router as rawR
from src.routers.v1.policy import router as policyR
from src.routers.v1.decisions import router as decisionsR

v1_router = APIRouter()
v1_router.include_router(latencyR, prefix="/processed", tags=["v1", "data"])
v1_router.include_router(rawR, prefix="/raw", tags=["v1", "data"])
v1_router.include_router(policyR, prefix="/policy", tags=["v1", "policy"])
v1_router.include_router(decisionsR, prefix="/decisions", tags=["v1", "decisions"])
