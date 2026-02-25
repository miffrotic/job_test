"""S&OP data endpoints."""

from fastapi import APIRouter, Depends, HTTPException, status

from app.core.database import ClickHouseDatabase, get_database
from app.core.minio_client import MinIOClient, get_minio_client
from app.schemas.sop import (
    SopAggregationRequest,
    SopAggregationResponse,
    SopChartRequest,
    SopChartResponse,
    SopDataResponse,
    SopExportRequest,
    SopExportResponse,
    SopFilterParams,
    SopFiltersResponse,
    SopQueryRequest,
)
from app.services.sop import SopService

router = APIRouter()


def get_sop_service(
    db: ClickHouseDatabase = Depends(get_database),
    minio: MinIOClient = Depends(get_minio_client),
) -> SopService:
    """Get S&OP service instance."""
    return SopService(db, minio)


@router.post("/query", response_model=SopDataResponse)
async def query_sop_data(
    request: SopQueryRequest,
    service: SopService = Depends(get_sop_service),
) -> SopDataResponse:
    """
    Query S&OP data with filtering, sorting, and pagination.
    
    This endpoint is used to populate the main S&OP table view.
    Supports filtering by:
    - Years and months
    - Territories and macroregions
    - Categories
    - Financial codes (finkod)
    - Drivers and priznaks
    - Units of measure
    
    Also supports full-text search across text columns.
    """
    try:
        return await service.query_data(request)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to query S&OP data: {str(e)}",
        )


@router.get("/filters", response_model=SopFiltersResponse)
async def get_sop_filter_options(
    service: SopService = Depends(get_sop_service),
) -> SopFiltersResponse:
    """
    Get available filter options for S&OP page.
    
    Returns all unique values for each filter dimension with counts.
    Used to populate filter dropdowns in the UI.
    """
    try:
        return await service.get_filter_options()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get filter options: {str(e)}",
        )


@router.post("/filters/scoped", response_model=SopFiltersResponse)
async def get_scoped_filter_options(
    current_filters: SopFilterParams,
    service: SopService = Depends(get_sop_service),
) -> SopFiltersResponse:
    """
    Get filter options scoped by current filter selection.
    
    Returns available values for each filter dimension, limited to
    records that match the currently applied filters. This enables
    cascading/dependent filter dropdowns.
    
    For example, if a territory is selected, only macroregions within
    that territory will be returned in the macroregions list.
    """
    try:
        return await service.get_filter_options(current_filters)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get scoped filter options: {str(e)}",
        )


@router.post("/aggregate", response_model=SopAggregationResponse)
async def aggregate_sop_data(
    request: SopAggregationRequest,
    service: SopService = Depends(get_sop_service),
) -> SopAggregationResponse:
    """
    Aggregate S&OP data with grouping.
    
    Supports aggregation functions:
    - sum: Sum of values
    - avg: Average value
    - count: Count of records
    - min: Minimum value
    - max: Maximum value
    
    Can group by any combination of dimensions and supports
    time-based grouping by month, quarter, or year.
    """
    try:
        return await service.aggregate_data(request)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to aggregate S&OP data: {str(e)}",
        )


@router.post("/chart", response_model=SopChartResponse)
async def get_sop_chart_data(
    request: SopChartRequest,
    service: SopService = Depends(get_sop_service),
) -> SopChartResponse:
    """
    Get S&OP data formatted for chart visualization.
    
    Supports chart types:
    - line: Line chart
    - bar: Bar chart
    - pie: Pie chart
    
    Can create multiple series by specifying series_by parameter.
    For example, to show revenue by month with separate lines per territory:
    - x_axis: "month"
    - y_axis: "value"
    - series_by: "territory"
    """
    try:
        return await service.get_chart_data(request)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get chart data: {str(e)}",
        )


@router.post("/export", response_model=SopExportResponse)
async def export_sop_data(
    request: SopExportRequest,
    service: SopService = Depends(get_sop_service),
) -> SopExportResponse:
    """
    Export S&OP data to file.
    
    Supported formats:
    - csv: Comma-separated values
    - json: JSON format
    - xlsx: Excel format (coming soon)
    
    Returns a presigned URL for downloading the exported file.
    The URL expires after 24 hours.
    """
    try:
        return await service.export_data(request)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to export S&OP data: {str(e)}",
        )


# Quick access endpoints for specific data views

@router.get("/summary/by-territory", response_model=SopAggregationResponse)
async def get_summary_by_territory(
    year: int | None = None,
    unit: str = "Выручка",
    service: SopService = Depends(get_sop_service),
) -> SopAggregationResponse:
    """Get total values summarized by territory."""
    filters = SopFilterParams(
        years=[year] if year else None,
        units=[unit],
    )
    request = SopAggregationRequest(
        filters=filters,
        group_by=["territory"],
        aggregations=["sum"],
        value_column="value",
    )
    return await service.aggregate_data(request)


@router.get("/summary/by-month", response_model=SopAggregationResponse)
async def get_summary_by_month(
    year: int | None = None,
    territory: str | None = None,
    unit: str = "Выручка",
    service: SopService = Depends(get_sop_service),
) -> SopAggregationResponse:
    """Get total values summarized by month."""
    filters = SopFilterParams(
        years=[year] if year else None,
        territories=[territory] if territory else None,
        units=[unit],
    )
    request = SopAggregationRequest(
        filters=filters,
        group_by=["year", "month"],
        aggregations=["sum"],
        value_column="value",
    )
    return await service.aggregate_data(request)


@router.get("/summary/by-category", response_model=SopAggregationResponse)
async def get_summary_by_category(
    year: int | None = None,
    territory: str | None = None,
    unit: str = "Выручка",
    service: SopService = Depends(get_sop_service),
) -> SopAggregationResponse:
    """Get total values summarized by category."""
    filters = SopFilterParams(
        years=[year] if year else None,
        territories=[territory] if territory else None,
        units=[unit],
    )
    request = SopAggregationRequest(
        filters=filters,
        group_by=["category_code", "category_name"],
        aggregations=["sum"],
        value_column="value",
    )
    return await service.aggregate_data(request)


@router.get("/summary/by-driver", response_model=SopAggregationResponse)
async def get_summary_by_driver(
    year: int | None = None,
    territory: str | None = None,
    priznak: str | None = None,
    unit: str = "Выручка",
    service: SopService = Depends(get_sop_service),
) -> SopAggregationResponse:
    """Get total values summarized by driver."""
    filters = SopFilterParams(
        years=[year] if year else None,
        territories=[territory] if territory else None,
        priznaks=[priznak] if priznak else None,
        units=[unit],
    )
    request = SopAggregationRequest(
        filters=filters,
        group_by=["driver"],
        aggregations=["sum"],
        value_column="value",
    )
    return await service.aggregate_data(request)
