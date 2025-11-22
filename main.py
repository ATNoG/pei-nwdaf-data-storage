from contextlib import asynccontextmanager
from fastapi import FastAPI
from src.routers.query import router, Influx, ClickHouse

@asynccontextmanager
async def lifespan(app: FastAPI):
    Influx.service.connect()
    ClickHouse.service.connect()
    yield
    Influx.service.client.close()
    ClickHouse.service.client.close()

app = FastAPI(lifespan=lifespan)

app.include_router(router, prefix="/data", tags=["data"])
