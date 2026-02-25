"""Table and data source management endpoints."""

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status

from app.core.database import ClickHouseDatabase, get_database
from app.schemas.data import (
    DataSourceCreate,
    DataSourceResponse,
    DataSourceUpdate,
    TableMetadata,
)
from app.schemas.base import PaginatedResponse, SuccessResponse
from app.services.tables import TableService

router = APIRouter()


def get_table_service(
    db: ClickHouseDatabase = Depends(get_database),
) -> TableService:
    """Get table service instance."""
    return TableService(db)


@router.get("", response_model=list[TableMetadata])
async def list_tables(
    database: str | None = None,
    service: TableService = Depends(get_table_service),
) -> list[TableMetadata]:
    """
    List all available tables in the database.
    
    Returns metadata for each table including column definitions,
    row count, and table engine.
    """
    return await service.list_tables(database=database)


@router.get("/{table_name}/metadata", response_model=TableMetadata)
async def get_table_metadata(
    table_name: str,
    database: str | None = None,
    service: TableService = Depends(get_table_service),
) -> TableMetadata:
    """
    Get detailed metadata for a specific table.
    
    Returns column definitions, row count, table size, and engine type.
    """
    metadata = await service.get_table_metadata(table_name, database=database)
    if not metadata:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table {table_name} not found",
        )
    return metadata


@router.get("/{table_name}/columns", response_model=list[dict])
async def get_table_columns(
    table_name: str,
    database: str | None = None,
    service: TableService = Depends(get_table_service),
) -> list[dict]:
    """
    Get column definitions for a table.
    
    Returns list of columns with name, type, and other properties.
    """
    return await service.get_table_columns(table_name, database=database)


@router.get("/{table_name}/sample", response_model=dict)
async def get_table_sample(
    table_name: str,
    limit: int = 100,
    service: TableService = Depends(get_table_service),
) -> dict:
    """
    Get sample data from a table.
    
    Returns a limited number of rows for preview purposes.
    """
    return await service.get_table_sample(table_name, limit=limit)


# Data Sources endpoints
@router.post(
    "/sources",
    response_model=DataSourceResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_data_source(
    data_source: DataSourceCreate,
    service: TableService = Depends(get_table_service),
) -> DataSourceResponse:
    """
    Create a new data source.
    
    Data sources can be:
    - TABLE: Reference to an existing ClickHouse table
    - QUERY: Custom SQL query
    - EXTERNAL: External data connection
    """
    return await service.create_data_source(data_source)


@router.get("/sources", response_model=PaginatedResponse[DataSourceResponse])
async def list_data_sources(
    page: int = 1,
    page_size: int = 20,
    service: TableService = Depends(get_table_service),
) -> PaginatedResponse[DataSourceResponse]:
    """List all data sources with pagination."""
    return await service.list_data_sources(page=page, page_size=page_size)


@router.get("/sources/{source_id}", response_model=DataSourceResponse)
async def get_data_source(
    source_id: UUID,
    service: TableService = Depends(get_table_service),
) -> DataSourceResponse:
    """Get a specific data source by ID."""
    source = await service.get_data_source(source_id)
    if not source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Data source {source_id} not found",
        )
    return source


@router.put("/sources/{source_id}", response_model=DataSourceResponse)
async def update_data_source(
    source_id: UUID,
    source_update: DataSourceUpdate,
    service: TableService = Depends(get_table_service),
) -> DataSourceResponse:
    """Update a data source."""
    source = await service.update_data_source(source_id, source_update)
    if not source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Data source {source_id} not found",
        )
    return source


@router.delete("/sources/{source_id}", response_model=SuccessResponse)
async def delete_data_source(
    source_id: UUID,
    service: TableService = Depends(get_table_service),
) -> SuccessResponse:
    """Delete a data source."""
    deleted = await service.delete_data_source(source_id)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Data source {source_id} not found",
        )
    return SuccessResponse(message=f"Data source {source_id} deleted successfully")
