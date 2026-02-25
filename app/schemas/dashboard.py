"""Dashboard-related Pydantic schemas."""

from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import Field

from app.schemas.base import BaseSchema


class WidgetType(str, Enum):
    """Dashboard widget types."""

    TABLE = "table"
    LINE_CHART = "line_chart"
    BAR_CHART = "bar_chart"
    PIE_CHART = "pie_chart"
    AREA_CHART = "area_chart"
    SCATTER_CHART = "scatter_chart"
    METRIC_CARD = "metric_card"
    HEATMAP = "heatmap"


class WidgetPosition(BaseSchema):
    """Widget position and size on dashboard grid."""

    x: int = Field(..., ge=0, description="X position in grid")
    y: int = Field(..., ge=0, description="Y position in grid")
    width: int = Field(..., ge=1, description="Widget width in grid units")
    height: int = Field(..., ge=1, description="Widget height in grid units")


class WidgetConfig(BaseSchema):
    """Widget configuration settings."""

    data_source_id: UUID | None = Field(None, description="Data source ID")
    query: str | None = Field(None, description="Custom query for data")
    columns: list[str] | None = Field(None, description="Columns to display")
    aggregations: dict[str, str] | None = Field(
        None, description="Column aggregations (column: function)"
    )
    group_by: list[str] | None = Field(None, description="Group by columns")
    chart_config: dict[str, Any] | None = Field(
        None, description="Chart-specific configuration"
    )
    refresh_interval: int | None = Field(
        None, ge=0, description="Auto-refresh interval in seconds"
    )
    filters: dict[str, Any] | None = Field(
        None, description="Default widget filters"
    )


class WidgetBase(BaseSchema):
    """Base widget schema."""

    title: str = Field(..., min_length=1, max_length=255, description="Widget title")
    widget_type: WidgetType = Field(..., description="Type of widget")
    position: WidgetPosition = Field(..., description="Widget position")
    config: WidgetConfig = Field(..., description="Widget configuration")


class WidgetCreate(WidgetBase):
    """Schema for creating a widget."""

    dashboard_id: UUID = Field(..., description="Parent dashboard ID")


class WidgetUpdate(BaseSchema):
    """Schema for updating a widget."""

    title: str | None = Field(None, min_length=1, max_length=255)
    widget_type: WidgetType | None = None
    position: WidgetPosition | None = None
    config: WidgetConfig | None = None


class WidgetResponse(WidgetBase):
    """Widget response schema."""

    id: UUID = Field(..., description="Widget ID")
    dashboard_id: UUID = Field(..., description="Parent dashboard ID")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime | None = Field(None, description="Last update timestamp")


class DashboardBase(BaseSchema):
    """Base dashboard schema."""

    name: str = Field(..., min_length=1, max_length=255, description="Dashboard name")
    description: str | None = Field(None, max_length=1000, description="Description")
    is_public: bool = Field(False, description="Public visibility")
    tags: list[str] = Field(default_factory=list, description="Dashboard tags")


class DashboardCreate(DashboardBase):
    """Schema for creating a dashboard."""

    pass


class DashboardUpdate(BaseSchema):
    """Schema for updating a dashboard."""

    name: str | None = Field(None, min_length=1, max_length=255)
    description: str | None = Field(None, max_length=1000)
    is_public: bool | None = None
    tags: list[str] | None = None


class DashboardResponse(DashboardBase):
    """Dashboard response schema."""

    id: UUID = Field(..., description="Dashboard ID")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime | None = Field(None, description="Last update timestamp")
    widgets: list[WidgetResponse] = Field(
        default_factory=list, description="Dashboard widgets"
    )


class DashboardListResponse(DashboardBase):
    """Dashboard list item response (without widgets)."""

    id: UUID = Field(..., description="Dashboard ID")
    created_at: datetime = Field(..., description="Creation timestamp")
    widget_count: int = Field(0, description="Number of widgets")
