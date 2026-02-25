"""Dashboard management endpoints."""

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status

from app.core.database import ClickHouseDatabase, get_database
from app.schemas.dashboard import (
    DashboardCreate,
    DashboardListResponse,
    DashboardResponse,
    DashboardUpdate,
    WidgetCreate,
    WidgetResponse,
    WidgetUpdate,
)
from app.schemas.base import PaginatedResponse, SuccessResponse
from app.services.dashboard import DashboardService

router = APIRouter()


def get_dashboard_service(
    db: ClickHouseDatabase = Depends(get_database),
) -> DashboardService:
    """Get dashboard service instance."""
    return DashboardService(db)


@router.get("", response_model=PaginatedResponse[DashboardListResponse])
async def list_dashboards(
    page: int = 1,
    page_size: int = 20,
    search: str | None = None,
    tags: list[str] | None = None,
    service: DashboardService = Depends(get_dashboard_service),
) -> PaginatedResponse[DashboardListResponse]:
    """List all dashboards with pagination and filtering."""
    return await service.list_dashboards(
        page=page,
        page_size=page_size,
        search=search,
        tags=tags,
    )


@router.post("", response_model=DashboardResponse, status_code=status.HTTP_201_CREATED)
async def create_dashboard(
    dashboard: DashboardCreate,
    service: DashboardService = Depends(get_dashboard_service),
) -> DashboardResponse:
    """Create a new dashboard."""
    return await service.create_dashboard(dashboard)


@router.get("/{dashboard_id}", response_model=DashboardResponse)
async def get_dashboard(
    dashboard_id: UUID,
    service: DashboardService = Depends(get_dashboard_service),
) -> DashboardResponse:
    """Get a specific dashboard by ID."""
    dashboard = await service.get_dashboard(dashboard_id)
    if not dashboard:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dashboard {dashboard_id} not found",
        )
    return dashboard


@router.put("/{dashboard_id}", response_model=DashboardResponse)
async def update_dashboard(
    dashboard_id: UUID,
    dashboard_update: DashboardUpdate,
    service: DashboardService = Depends(get_dashboard_service),
) -> DashboardResponse:
    """Update a dashboard."""
    dashboard = await service.update_dashboard(dashboard_id, dashboard_update)
    if not dashboard:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dashboard {dashboard_id} not found",
        )
    return dashboard


@router.delete("/{dashboard_id}", response_model=SuccessResponse)
async def delete_dashboard(
    dashboard_id: UUID,
    service: DashboardService = Depends(get_dashboard_service),
) -> SuccessResponse:
    """Delete a dashboard."""
    deleted = await service.delete_dashboard(dashboard_id)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dashboard {dashboard_id} not found",
        )
    return SuccessResponse(message=f"Dashboard {dashboard_id} deleted successfully")


# Widget endpoints
@router.post(
    "/{dashboard_id}/widgets",
    response_model=WidgetResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_widget(
    dashboard_id: UUID,
    widget: WidgetCreate,
    service: DashboardService = Depends(get_dashboard_service),
) -> WidgetResponse:
    """Add a widget to a dashboard."""
    # Ensure dashboard_id matches
    widget.dashboard_id = dashboard_id
    return await service.create_widget(widget)


@router.get("/{dashboard_id}/widgets", response_model=list[WidgetResponse])
async def list_widgets(
    dashboard_id: UUID,
    service: DashboardService = Depends(get_dashboard_service),
) -> list[WidgetResponse]:
    """List all widgets in a dashboard."""
    return await service.list_widgets(dashboard_id)


@router.put("/{dashboard_id}/widgets/{widget_id}", response_model=WidgetResponse)
async def update_widget(
    dashboard_id: UUID,
    widget_id: UUID,
    widget_update: WidgetUpdate,
    service: DashboardService = Depends(get_dashboard_service),
) -> WidgetResponse:
    """Update a widget."""
    widget = await service.update_widget(widget_id, widget_update)
    if not widget:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Widget {widget_id} not found",
        )
    return widget


@router.delete("/{dashboard_id}/widgets/{widget_id}", response_model=SuccessResponse)
async def delete_widget(
    dashboard_id: UUID,
    widget_id: UUID,
    service: DashboardService = Depends(get_dashboard_service),
) -> SuccessResponse:
    """Delete a widget from a dashboard."""
    deleted = await service.delete_widget(widget_id)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Widget {widget_id} not found",
        )
    return SuccessResponse(message=f"Widget {widget_id} deleted successfully")
