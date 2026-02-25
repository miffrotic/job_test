"""
SQLAlchemy ORM models for ClickHouse tables.

Uses clickhouse-sqlalchemy for ClickHouse-specific types and engines.
"""

from app.models.base import Base, metadata
from app.models.sop import SopFact
from app.models.dimensions import (
    DimTerritory,
    DimMacroregion,
    DimCategory,
    DimFinkod,
    DimDriver,
    DimPriznak,
    DimUnit,
)
from app.models.dashboard import Dashboard, Widget
from app.models.data_source import DataSource

__all__ = [
    "Base",
    "metadata",
    # S&OP
    "SopFact",
    # Dimensions
    "DimTerritory",
    "DimMacroregion",
    "DimCategory",
    "DimFinkod",
    "DimDriver",
    "DimPriznak",
    "DimUnit",
    # Dashboard
    "Dashboard",
    "Widget",
    # Data sources
    "DataSource",
]
