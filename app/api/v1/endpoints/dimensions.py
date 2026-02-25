"""Dimension (reference data) endpoints."""

from fastapi import APIRouter, Depends

from app.core.database import ClickHouseDatabase, get_database
from app.services.dimensions import DimensionService

router = APIRouter()


def get_dimension_service(
    db: ClickHouseDatabase = Depends(get_database),
) -> DimensionService:
    """Get dimension service instance."""
    return DimensionService(db)


@router.get("/territories")
async def get_territories(
    include_inactive: bool = False,
    service: DimensionService = Depends(get_dimension_service),
) -> list[dict]:
    """Get all territories."""
    return await service.get_territories(include_inactive=include_inactive)


@router.get("/macroregions")
async def get_macroregions(
    territory_code: str | None = None,
    include_inactive: bool = False,
    service: DimensionService = Depends(get_dimension_service),
) -> list[dict]:
    """Get macroregions, optionally filtered by territory."""
    return await service.get_macroregions(
        territory_code=territory_code,
        include_inactive=include_inactive,
    )


@router.get("/categories")
async def get_categories(
    parent_code: str | None = None,
    include_inactive: bool = False,
    service: DimensionService = Depends(get_dimension_service),
) -> list[dict]:
    """Get categories, optionally filtered by parent."""
    return await service.get_categories(
        parent_code=parent_code,
        include_inactive=include_inactive,
    )


@router.get("/finkods")
async def get_finkods(
    group_code: str | None = None,
    include_inactive: bool = False,
    service: DimensionService = Depends(get_dimension_service),
) -> list[dict]:
    """Get financial codes, optionally filtered by group."""
    return await service.get_finkods(
        group_code=group_code,
        include_inactive=include_inactive,
    )


@router.get("/drivers")
async def get_drivers(
    include_inactive: bool = False,
    service: DimensionService = Depends(get_dimension_service),
) -> list[dict]:
    """Get all drivers."""
    return await service.get_drivers(include_inactive=include_inactive)


@router.get("/priznaks")
async def get_priznaks(
    include_inactive: bool = False,
    service: DimensionService = Depends(get_dimension_service),
) -> list[dict]:
    """Get all priznaks (indicators)."""
    return await service.get_priznaks(include_inactive=include_inactive)


@router.get("/units")
async def get_units(
    include_inactive: bool = False,
    service: DimensionService = Depends(get_dimension_service),
) -> list[dict]:
    """Get all units of measure."""
    return await service.get_units(include_inactive=include_inactive)


@router.get("/years")
async def get_available_years(
    service: DimensionService = Depends(get_dimension_service),
) -> list[int]:
    """Get available years from S&OP data."""
    return await service.get_available_years()


@router.get("/months")
async def get_months() -> list[dict]:
    """Get months (static list)."""
    return [
        {"code": 1, "name": "Январь"},
        {"code": 2, "name": "Февраль"},
        {"code": 3, "name": "Март"},
        {"code": 4, "name": "Апрель"},
        {"code": 5, "name": "Май"},
        {"code": 6, "name": "Июнь"},
        {"code": 7, "name": "Июль"},
        {"code": 8, "name": "Август"},
        {"code": 9, "name": "Сентябрь"},
        {"code": 10, "name": "Октябрь"},
        {"code": 11, "name": "Ноябрь"},
        {"code": 12, "name": "Декабрь"},
    ]
