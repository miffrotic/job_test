"""Dimension (reference) tables SQLAlchemy models."""

from datetime import datetime

from clickhouse_sqlalchemy import types as ch_types, engines
from sqlalchemy import Column

from app.models.base import Base


class DimTerritory(Base):
    """Territory dimension table."""

    __tablename__ = "dim_territory"

    code = Column(ch_types.String, primary_key=True)
    name = Column(ch_types.String, nullable=False)
    sort_order = Column(ch_types.UInt32, nullable=False, default=0)
    is_active = Column(ch_types.UInt8, nullable=False, default=1)
    created_at = Column(ch_types.DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        engines.ReplacingMergeTree(
            order_by=(code,),
            primary_key=(code,),
        ),
    )


class DimMacroregion(Base):
    """Macroregion dimension table."""

    __tablename__ = "dim_macroregion"

    code = Column(ch_types.String, primary_key=True)
    name = Column(ch_types.String, nullable=False)
    territory_code = Column(ch_types.String, nullable=False)
    sort_order = Column(ch_types.UInt32, nullable=False, default=0)
    is_active = Column(ch_types.UInt8, nullable=False, default=1)
    created_at = Column(ch_types.DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        engines.ReplacingMergeTree(
            order_by=(territory_code, code),
            primary_key=(code,),
        ),
    )


class DimCategory(Base):
    """Product category dimension table."""

    __tablename__ = "dim_category"

    code = Column(ch_types.String, primary_key=True)
    name = Column(ch_types.String, nullable=False)
    parent_code = Column(ch_types.String, nullable=False, default="")
    level = Column(ch_types.UInt8, nullable=False, default=1)
    sort_order = Column(ch_types.UInt32, nullable=False, default=0)
    is_active = Column(ch_types.UInt8, nullable=False, default=1)
    created_at = Column(ch_types.DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        engines.ReplacingMergeTree(
            order_by=(code,),
            primary_key=(code,),
        ),
    )


class DimFinkod(Base):
    """Financial code dimension table."""

    __tablename__ = "dim_finkod"

    code = Column(ch_types.String, primary_key=True)
    name = Column(ch_types.String, nullable=False)
    group_code = Column(ch_types.String, nullable=False, default="")  # Группа: A, C, D, F, G
    sort_order = Column(ch_types.UInt32, nullable=False, default=0)
    is_active = Column(ch_types.UInt8, nullable=False, default=1)
    created_at = Column(ch_types.DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        engines.ReplacingMergeTree(
            order_by=(code,),
            primary_key=(code,),
        ),
    )


class DimDriver(Base):
    """Driver dimension table."""

    __tablename__ = "dim_driver"

    code = Column(ch_types.String, primary_key=True)
    name = Column(ch_types.String, nullable=False)
    sort_order = Column(ch_types.UInt32, nullable=False, default=0)
    is_active = Column(ch_types.UInt8, nullable=False, default=1)
    created_at = Column(ch_types.DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        engines.ReplacingMergeTree(
            order_by=(code,),
            primary_key=(code,),
        ),
    )


class DimPriznak(Base):
    """Priznak (Sign/Indicator) dimension table."""

    __tablename__ = "dim_priznak"

    code = Column(ch_types.String, primary_key=True)
    name = Column(ch_types.String, nullable=False)
    sort_order = Column(ch_types.UInt32, nullable=False, default=0)
    is_active = Column(ch_types.UInt8, nullable=False, default=1)
    created_at = Column(ch_types.DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        engines.ReplacingMergeTree(
            order_by=(code,),
            primary_key=(code,),
        ),
    )


class DimUnit(Base):
    """Unit of measure dimension table."""

    __tablename__ = "dim_unit"

    code = Column(ch_types.String, primary_key=True)
    name = Column(ch_types.String, nullable=False)
    sort_order = Column(ch_types.UInt32, nullable=False, default=0)
    is_active = Column(ch_types.UInt8, nullable=False, default=1)
    created_at = Column(ch_types.DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        engines.ReplacingMergeTree(
            order_by=(code,),
            primary_key=(code,),
        ),
    )
