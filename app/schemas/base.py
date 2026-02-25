"""Base Pydantic schemas and common types."""

from datetime import datetime
from enum import Enum
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field


class BaseSchema(BaseModel):
    """Base schema with common configuration."""

    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True,
        use_enum_values=True,
    )


T = TypeVar("T")


class PaginatedResponse(BaseSchema, Generic[T]):
    """Generic paginated response schema."""

    items: list[T]
    total: int = Field(..., description="Total number of items")
    page: int = Field(..., ge=1, description="Current page number")
    page_size: int = Field(..., ge=1, le=1000, description="Items per page")
    pages: int = Field(..., ge=0, description="Total number of pages")

    @classmethod
    def create(
        cls,
        items: list[T],
        total: int,
        page: int,
        page_size: int,
    ) -> "PaginatedResponse[T]":
        """Create paginated response from items."""
        pages = (total + page_size - 1) // page_size if page_size > 0 else 0
        return cls(
            items=items,
            total=total,
            page=page,
            page_size=page_size,
            pages=pages,
        )


class SortOrder(str, Enum):
    """Sort order enumeration."""

    ASC = "asc"
    DESC = "desc"


class FilterOperator(str, Enum):
    """Filter operator enumeration."""

    EQ = "eq"  # Equal
    NEQ = "neq"  # Not equal
    GT = "gt"  # Greater than
    GTE = "gte"  # Greater than or equal
    LT = "lt"  # Less than
    LTE = "lte"  # Less than or equal
    IN = "in"  # In list
    NOT_IN = "not_in"  # Not in list
    LIKE = "like"  # Like pattern
    ILIKE = "ilike"  # Case-insensitive like
    BETWEEN = "between"  # Between two values
    IS_NULL = "is_null"  # Is null
    IS_NOT_NULL = "is_not_null"  # Is not null


class FilterCondition(BaseSchema):
    """Single filter condition."""

    field: str = Field(..., description="Field name to filter on")
    operator: FilterOperator = Field(..., description="Filter operator")
    value: Any = Field(None, description="Filter value")
    values: list[Any] | None = Field(
        None, description="Multiple values for IN/BETWEEN operators"
    )


class FilterParams(BaseSchema):
    """Filter parameters for queries."""

    conditions: list[FilterCondition] = Field(
        default_factory=list, description="List of filter conditions"
    )
    logic: str = Field("AND", description="Logic operator: AND or OR")


class SortParams(BaseSchema):
    """Sort parameters for queries."""

    field: str = Field(..., description="Field name to sort by")
    order: SortOrder = Field(SortOrder.ASC, description="Sort order")


class TableQueryParams(BaseSchema):
    """Combined query parameters for table data."""

    page: int = Field(1, ge=1, description="Page number")
    page_size: int = Field(50, ge=1, le=1000, description="Items per page")
    filters: FilterParams | None = Field(None, description="Filter parameters")
    sort: list[SortParams] = Field(
        default_factory=list, description="Sort parameters"
    )
    search: str | None = Field(None, description="Full-text search query")
    columns: list[str] | None = Field(
        None, description="Columns to select (None for all)"
    )


class TimestampMixin(BaseSchema):
    """Mixin for timestamp fields."""

    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime | None = Field(None, description="Last update timestamp")


class ErrorResponse(BaseSchema):
    """Error response schema."""

    error: str = Field(..., description="Error type")
    message: str = Field(..., description="Error message")
    details: dict[str, Any] | None = Field(None, description="Additional error details")


class SuccessResponse(BaseSchema):
    """Generic success response."""

    success: bool = Field(True, description="Operation success status")
    message: str = Field(..., description="Success message")
    data: dict[str, Any] | None = Field(None, description="Additional data")
