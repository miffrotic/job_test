"""Data-related Pydantic schemas."""

from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import Field

from app.schemas.base import BaseSchema, FilterParams, SortParams


class AggregationFunction(str, Enum):
    """Supported aggregation functions."""

    COUNT = "count"
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    DISTINCT_COUNT = "uniqExact"
    MEDIAN = "median"
    PERCENTILE_90 = "quantile(0.9)"
    PERCENTILE_95 = "quantile(0.95)"
    PERCENTILE_99 = "quantile(0.99)"


class ChartType(str, Enum):
    """Chart type enumeration."""

    LINE = "line"
    BAR = "bar"
    PIE = "pie"
    AREA = "area"
    SCATTER = "scatter"
    HEATMAP = "heatmap"
    TIMELINE = "timeline"


class TimeGranularity(str, Enum):
    """Time granularity for aggregations."""

    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"


class DataSourceType(str, Enum):
    """Data source type enumeration."""

    TABLE = "table"
    QUERY = "query"
    EXTERNAL = "external"


class DataSourceBase(BaseSchema):
    """Base data source schema."""

    name: str = Field(..., min_length=1, max_length=255, description="Data source name")
    description: str | None = Field(None, max_length=1000, description="Description")
    source_type: DataSourceType = Field(..., description="Type of data source")
    table_name: str | None = Field(None, description="Table name for TABLE type")
    query: str | None = Field(None, description="SQL query for QUERY type")
    connection_config: dict[str, Any] | None = Field(
        None, description="Connection config for EXTERNAL type"
    )


class DataSourceCreate(DataSourceBase):
    """Schema for creating a data source."""

    pass


class DataSourceUpdate(BaseSchema):
    """Schema for updating a data source."""

    name: str | None = Field(None, min_length=1, max_length=255)
    description: str | None = Field(None, max_length=1000)
    table_name: str | None = None
    query: str | None = None
    connection_config: dict[str, Any] | None = None


class DataSourceResponse(DataSourceBase):
    """Data source response schema."""

    id: UUID = Field(..., description="Data source ID")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime | None = Field(None, description="Last update timestamp")
    columns: list[dict[str, str]] | None = Field(
        None, description="Available columns with types"
    )


class ColumnAggregation(BaseSchema):
    """Column aggregation configuration."""

    column: str = Field(..., description="Column name")
    function: AggregationFunction = Field(..., description="Aggregation function")
    alias: str | None = Field(None, description="Result column alias")


class DataQueryRequest(BaseSchema):
    """Request schema for querying data."""

    data_source_id: UUID | None = Field(None, description="Data source ID")
    table_name: str | None = Field(None, description="Direct table name")
    columns: list[str] | None = Field(None, description="Columns to select")
    filters: FilterParams | None = Field(None, description="Filter parameters")
    sort: list[SortParams] = Field(default_factory=list, description="Sort options")
    page: int = Field(1, ge=1, description="Page number")
    page_size: int = Field(50, ge=1, le=10000, description="Items per page")


class DataQueryResponse(BaseSchema):
    """Response schema for data queries."""

    columns: list[dict[str, str]] = Field(..., description="Column definitions")
    data: list[dict[str, Any]] = Field(..., description="Query results")
    total: int = Field(..., description="Total number of rows")
    page: int = Field(..., description="Current page")
    page_size: int = Field(..., description="Page size")
    pages: int = Field(..., description="Total pages")
    query_time_ms: float = Field(..., description="Query execution time in ms")


class AggregationRequest(BaseSchema):
    """Request schema for data aggregation."""

    data_source_id: UUID | None = Field(None, description="Data source ID")
    table_name: str | None = Field(None, description="Direct table name")
    aggregations: list[ColumnAggregation] = Field(
        ..., min_length=1, description="Aggregations to perform"
    )
    group_by: list[str] = Field(default_factory=list, description="Group by columns")
    filters: FilterParams | None = Field(None, description="Filter parameters")
    time_column: str | None = Field(None, description="Time column for time-based grouping")
    time_granularity: TimeGranularity | None = Field(
        None, description="Time grouping granularity"
    )
    sort: list[SortParams] = Field(default_factory=list, description="Sort options")
    limit: int = Field(1000, ge=1, le=100000, description="Max rows to return")


class AggregationResponse(BaseSchema):
    """Response schema for aggregations."""

    columns: list[str] = Field(..., description="Result column names")
    data: list[dict[str, Any]] = Field(..., description="Aggregation results")
    total_rows: int = Field(..., description="Number of result rows")
    query_time_ms: float = Field(..., description="Query execution time in ms")


class ChartDataRequest(BaseSchema):
    """Request schema for chart data."""

    data_source_id: UUID | None = Field(None, description="Data source ID")
    table_name: str | None = Field(None, description="Direct table name")
    chart_type: ChartType = Field(..., description="Type of chart")

    # For line/bar/area charts
    x_column: str = Field(..., description="X-axis column")
    y_columns: list[str] = Field(..., min_length=1, description="Y-axis columns")
    y_aggregations: list[AggregationFunction] | None = Field(
        None, description="Y-axis aggregation functions"
    )

    # For pie charts
    label_column: str | None = Field(None, description="Label column for pie charts")
    value_column: str | None = Field(None, description="Value column for pie charts")

    # Common options
    filters: FilterParams | None = Field(None, description="Filter parameters")
    group_by: list[str] | None = Field(None, description="Additional group by columns")
    time_granularity: TimeGranularity | None = Field(
        None, description="Time grouping for time-series data"
    )
    limit: int = Field(1000, ge=1, le=100000, description="Max data points")


class ChartSeries(BaseSchema):
    """Single chart series data."""

    name: str = Field(..., description="Series name")
    data: list[dict[str, Any]] = Field(..., description="Series data points")
    color: str | None = Field(None, description="Series color")


class ChartDataResponse(BaseSchema):
    """Response schema for chart data."""

    chart_type: ChartType = Field(..., description="Chart type")
    series: list[ChartSeries] = Field(..., description="Chart series data")
    x_axis_label: str | None = Field(None, description="X-axis label")
    y_axis_label: str | None = Field(None, description="Y-axis label")
    total_points: int = Field(..., description="Total data points")
    query_time_ms: float = Field(..., description="Query execution time in ms")


class TableMetadata(BaseSchema):
    """Table metadata schema."""

    table_name: str = Field(..., description="Table name")
    database: str = Field(..., description="Database name")
    engine: str = Field(..., description="Table engine")
    total_rows: int = Field(..., description="Approximate row count")
    total_bytes: int = Field(..., description="Table size in bytes")
    columns: list[dict[str, str]] = Field(..., description="Column definitions")


class ExportRequest(BaseSchema):
    """Request schema for data export."""

    data_source_id: UUID | None = Field(None, description="Data source ID")
    table_name: str | None = Field(None, description="Direct table name")
    format: str = Field("csv", description="Export format: csv, json, parquet")
    columns: list[str] | None = Field(None, description="Columns to export")
    filters: FilterParams | None = Field(None, description="Filter parameters")
    limit: int | None = Field(None, description="Max rows to export")


class ExportResponse(BaseSchema):
    """Response schema for data export."""

    file_url: str = Field(..., description="URL to download the exported file")
    file_name: str = Field(..., description="File name")
    file_size: int = Field(..., description="File size in bytes")
    row_count: int = Field(..., description="Number of exported rows")
    expires_at: datetime = Field(..., description="URL expiration time")
