"""API v1 router configuration."""

from fastapi import APIRouter

from app.api.v1.endpoints import dashboard, data, tables, files, sop, dimensions

api_router = APIRouter()

api_router.include_router(
    sop.router,
    prefix="/sop",
    tags=["S&OP"],
)

api_router.include_router(
    dimensions.router,
    prefix="/dimensions",
    tags=["Dimensions"],
)

api_router.include_router(
    dashboard.router,
    prefix="/dashboards",
    tags=["Dashboards"],
)

api_router.include_router(
    data.router,
    prefix="/data",
    tags=["Data"],
)

api_router.include_router(
    tables.router,
    prefix="/tables",
    tags=["Tables"],
)

api_router.include_router(
    files.router,
    prefix="/files",
    tags=["Files"],
)
