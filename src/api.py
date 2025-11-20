from fastapi import FastAPI
from src.routers.influx import router

app = FastAPI()

# include routers
app.include_router(router, prefix="/influx", tags=["influx"])
