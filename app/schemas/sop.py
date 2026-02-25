"""S&OP related Pydantic schemas."""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Optional
from uuid import UUID

from pydantic import Field

from app.schemas.base import BaseSchema, FilterParams, PaginatedResponse, SortParams


# Enums for filter options
class PriznakEnum(str, Enum):
    """Priznak (indicator) options."""
    PROMO = "Промо"
    REGULAR = "Регуляр"


class UnitEnum(str, Enum):
    """Unit of measure options."""
    REVENUE = "Выручка"
    RTO = "РТО"
    PIECES = "шт"


class DriverEnum(str, Enum):
    """Driver options."""
    OPENING = "Открытия"
    PROMO = "Промо"
    REGULAR = "Регуляр"
    BUDGET = "Бюджет"
    ED = "ЭД"
    BUDGET_KD = "Бюджет КД"
    INPUT = "Вводные"
    MARKETING = "Маркетинг"
    PROGNOZ_KD = "Прогноз КД"


# Filter schemas
class SopFilterParams(BaseSchema):
    """Filter parameters for S&OP data."""
    
    # Time filters
    years: list[int] | None = Field(None, description="Filter by years")
    months: list[int] | None = Field(None, description="Filter by months (1-12)")
    
    # Classification filters
    priznaks: list[str] | None = Field(None, description="Filter by priznak (Промо, Регуляр)")
    drivers: list[str] | None = Field(None, description="Filter by driver")
    
    # Financial code filters
    finkod_codes: list[str] | None = Field(None, description="Filter by finkod codes (AA, C1...)")
    finkod_groups: list[str] | None = Field(None, description="Filter by finkod groups (A, C, D...)")
    
    # Category filters
    category_codes: list[str] | None = Field(None, description="Filter by category codes")
    
    # Geographic filters
    territories: list[str] | None = Field(None, description="Filter by territories")
    macroregion_codes: list[str] | None = Field(None, description="Filter by macroregion codes")
    
    # Unit filter
    units: list[str] | None = Field(None, description="Filter by units (Выручка, РТО, шт)")
    
    # Search
    search: str | None = Field(None, description="Full-text search")


class SopSortParams(BaseSchema):
    """Sort parameters for S&OP data."""
    
    field: str = Field("year", description="Field to sort by")
    order: str = Field("desc", description="Sort order: asc or desc")


class SopQueryRequest(BaseSchema):
    """Request schema for querying S&OP data."""
    
    filters: SopFilterParams | None = Field(None, description="Filter parameters")
    sort: list[SopSortParams] = Field(
        default_factory=lambda: [SopSortParams(field="year", order="desc")],
        description="Sort parameters"
    )
    page: int = Field(1, ge=1, description="Page number")
    page_size: int = Field(50, ge=1, le=10000, description="Items per page")
    
    # Column selection
    columns: list[str] | None = Field(None, description="Columns to return (None for all)")


# Response schemas
class SopRowResponse(BaseSchema):
    """Single S&OP data row response."""
    
    id: UUID | None = Field(None, description="Record ID")
    
    # Model
    model: str = Field(..., description="Model type")
    
    # Classification
    priznak: str = Field(..., description="Priznak")
    driver: str = Field(..., description="Driver")
    
    # Financial code
    finkod_code: str = Field(..., description="Financial code")
    finkod_name: str = Field(..., description="Financial code name")
    
    # Category
    category_code: str = Field(..., description="Category code")
    category_name: str | None = Field(None, description="Category name")
    
    # Geography
    macroregion_code: str = Field(..., description="Macroregion code")
    macroregion_name: str = Field(..., description="Macroregion name")
    territory: str = Field(..., description="Territory")
    
    # Time
    year: int = Field(..., description="Year")
    month: int = Field(..., description="Month")
    
    # Measure
    unit: str = Field(..., description="Unit of measure")
    value: Decimal = Field(..., description="Value")


class SopDataResponse(BaseSchema):
    """Response schema for S&OP data query."""
    
    columns: list[dict[str, str]] = Field(..., description="Column definitions")
    data: list[dict[str, Any]] = Field(..., description="Data rows")
    total: int = Field(..., description="Total rows matching filters")
    page: int = Field(..., description="Current page")
    page_size: int = Field(..., description="Page size")
    pages: int = Field(..., description="Total pages")
    query_time_ms: float = Field(..., description="Query execution time in ms")


# Dimension response schemas
class DimensionValue(BaseSchema):
    """Single dimension value for filter options."""
    
    code: str = Field(..., description="Value code")
    name: str = Field(..., description="Display name")
    count: int | None = Field(None, description="Count of records with this value")
    parent_code: str | None = Field(None, description="Parent code for hierarchical dimensions")


class SopFiltersResponse(BaseSchema):
    """Available filter options for S&OP page."""
    
    years: list[int] = Field(..., description="Available years")
    territories: list[DimensionValue] = Field(..., description="Available territories")
    macroregions: list[DimensionValue] = Field(..., description="Available macroregions")
    categories: list[DimensionValue] = Field(..., description="Available categories")
    finkod_codes: list[DimensionValue] = Field(..., description="Available financial codes")
    drivers: list[DimensionValue] = Field(..., description="Available drivers")
    priznaks: list[DimensionValue] = Field(..., description="Available priznaks")
    units: list[DimensionValue] = Field(..., description="Available units")


# Aggregation schemas  
class SopAggregationRequest(BaseSchema):
    """Request for S&OP data aggregation."""
    
    filters: SopFilterParams | None = Field(None, description="Filter parameters")
    
    # Aggregation settings
    group_by: list[str] = Field(..., description="Columns to group by")
    aggregations: list[str] = Field(
        default_factory=lambda: ["sum"],
        description="Aggregation functions (sum, avg, count, min, max)"
    )
    value_column: str = Field("value", description="Column to aggregate")
    
    # Time grouping
    time_granularity: str | None = Field(
        None, 
        description="Time grouping: month, quarter, year"
    )
    
    limit: int = Field(1000, description="Max rows to return")


class SopAggregationResponse(BaseSchema):
    """Response for S&OP aggregation."""
    
    columns: list[str] = Field(..., description="Result columns")
    data: list[dict[str, Any]] = Field(..., description="Aggregated data")
    total_rows: int = Field(..., description="Number of result rows")
    query_time_ms: float = Field(..., description="Query time in ms")


# Chart data schemas
class SopChartRequest(BaseSchema):
    """Request for S&OP chart data."""
    
    filters: SopFilterParams | None = Field(None, description="Filter parameters")
    
    chart_type: str = Field("line", description="Chart type: line, bar, pie")
    x_axis: str = Field(..., description="X-axis field (e.g., month, territory)")
    y_axis: str = Field("value", description="Y-axis field")
    series_by: str | None = Field(None, description="Field for multiple series")
    
    aggregation: str = Field("sum", description="Aggregation function")
    
    limit: int = Field(100, description="Max data points")


class SopChartSeries(BaseSchema):
    """Single chart series."""
    
    name: str = Field(..., description="Series name")
    data: list[dict[str, Any]] = Field(..., description="Series data points")


class SopChartResponse(BaseSchema):
    """Response for S&OP chart data."""
    
    chart_type: str = Field(..., description="Chart type")
    x_axis_label: str = Field(..., description="X-axis label")
    y_axis_label: str = Field(..., description="Y-axis label")
    series: list[SopChartSeries] = Field(..., description="Chart series")
    total_points: int = Field(..., description="Total data points")
    query_time_ms: float = Field(..., description="Query time in ms")


# Export schema
class SopExportRequest(BaseSchema):
    """Request for S&OP data export."""
    
    filters: SopFilterParams | None = Field(None, description="Filter parameters")
    columns: list[str] | None = Field(None, description="Columns to export")
    format: str = Field("xlsx", description="Export format: xlsx, csv, json")
    limit: int | None = Field(None, description="Max rows to export")


class SopExportResponse(BaseSchema):
    """Response for S&OP export."""
    
    file_url: str = Field(..., description="Download URL")
    file_name: str = Field(..., description="File name")
    file_size: int = Field(..., description="File size in bytes")
    row_count: int = Field(..., description="Exported rows")
    expires_at: datetime = Field(..., description="URL expiration time")
