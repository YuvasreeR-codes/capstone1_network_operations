from fastapi import FastAPI

from fastapi.middleware.cors import CORSMiddleware
from api.routers.network import router as network_router
from api.routers.grid import router as grid_router
from api.routers import alerts
from api.routers import features
from api.routers import prediction
from api.routers import operational
app = FastAPI(
    title="Network Operations Analytics API",
    description=(
        "FastAPI service for telecom network "
        "analytics and NOC KPIs."
    ),
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "*"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(network_router)
app.include_router(grid_router)
app.include_router(alerts.router)
app.include_router(features.router)
app.include_router(prediction.router)
app.include_router(operational.router)

@app.get("/")
def root():

    return {
        "message": "Network Analytics API is running"
    }