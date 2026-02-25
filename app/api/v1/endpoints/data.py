"""Data query and aggregation endpoints."""

from fastapi import APIRouter, Depends

from app.core.database import ClickHouseDatabase, get_database
from app.schemas.data import (
    AggregationRequest,
    AggregationResponse,
    ChartDataRequest,
    ChartDataResponse,
    DataQueryRequest,
    DataQueryResponse,
    ExportRequest,
    ExportResponse,
)
from app.services.data import DataService
from app.core.minio_client import MinIOClient, get_minio_client

router = APIRouter()


def get_data_service(
    db: ClickHouseDatabase = Depends(get_database),
    minio: MinIOClient = Depends(get_minio_client),
) -> DataService:
    """Get data service instance."""
    return DataService(db, minio)


@router.post("/query", response_model=DataQueryResponse)
async def query_data(
    request: DataQueryRequest,
    service: DataService = Depends(get_data_service),
) -> DataQueryResponse:
    """
    Query data with filtering, sorting, and pagination.
    
    This endpoint allows querying data from a data source or table
    with support for:
    - Column selection
    - Filtering with various operators
    - Multi-column sorting
    - Pagination
    """
    return await service.query_data(request)


@router.post("/aggregate", response_model=AggregationResponse)
async def aggregate_data(
    request: AggregationRequest,
    service: DataService = Depends(get_data_service),
) -> AggregationResponse:
    """
    Aggregate data with grouping and aggregation functions.
    
    Supports various aggregation functions like COUNT, SUM, AVG,
    MIN, MAX, etc. with optional grouping by columns and time-based
    grouping.
    """
    return await service.aggregate_data(request)


@router.post("/chart", response_model=ChartDataResponse)
async def get_chart_data(
    request: ChartDataRequest,
    service: DataService = Depends(get_data_service),
) -> ChartDataResponse:
    """
    Get data formatted for chart visualization.
    
    Returns data in a format optimized for rendering different
    chart types including line, bar, pie, area, and scatter charts.
    """
    return await service.get_chart_data(request)


@router.post("/export", response_model=ExportResponse)
async def export_data(
    request: ExportRequest,
    service: DataService = Depends(get_data_service),
) -> ExportResponse:
    """
    Export data to a file (CSV, JSON, or Parquet).
    
    Returns a presigned URL for downloading the exported file.
    The file is stored in MinIO and the URL expires after a
    configured time period.
    """
    return await service.export_data(request)
