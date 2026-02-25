"""Pydantic schemas package."""

from app.schemas.base import (
    BaseSchema,
    PaginatedResponse,
    FilterParams,
    SortParams,
    TableQueryParams,
)
from app.schemas.dashboard import (
    DashboardCreate,
    DashboardResponse,
    DashboardUpdate,
    WidgetCreate,
    WidgetResponse,
)
from app.schemas.data import (
    DataSourceCreate,
    DataSourceResponse,
    DataQueryRequest,
    DataQueryResponse,
    AggregationRequest,
    ChartDataRequest,
    ChartDataResponse,
)
from app.schemas.sop import (
    SopFilterParams,
    SopQueryRequest,
    SopDataResponse,
    SopAggregationRequest,
    SopAggregationResponse,
    SopChartRequest,
    SopChartResponse,
    SopFiltersResponse,
    SopExportRequest,
    SopExportResponse,
)

__all__ = [
    # Base
    "BaseSchema",
    "PaginatedResponse",
    "FilterParams",
    "SortParams",
    "TableQueryParams",
    # Dashboard
    "DashboardCreate",
    "DashboardResponse",
    "DashboardUpdate",
    "WidgetCreate",
    "WidgetResponse",
    # Data
    "DataSourceCreate",
    "DataSourceResponse",
    "DataQueryRequest",
    "DataQueryResponse",
    "AggregationRequest",
    "ChartDataRequest",
    "ChartDataResponse",
    # S&OP
    "SopFilterParams",
    "SopQueryRequest",
    "SopDataResponse",
    "SopAggregationRequest",
    "SopAggregationResponse",
    "SopChartRequest",
    "SopChartResponse",
    "SopFiltersResponse",
    "SopExportRequest",
    "SopExportResponse",
]
