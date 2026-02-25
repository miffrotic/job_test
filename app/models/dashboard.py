"""Dashboard and Widget SQLAlchemy models."""

from datetime import datetime
from uuid import uuid4

from clickhouse_sqlalchemy import types as ch_types, engines
from sqlalchemy import Column

from app.models.base import Base


class Dashboard(Base):
    """Dashboard table for storing dashboard configurations."""

    __tablename__ = "dashboards"

    id = Column(ch_types.UUID, primary_key=True, default=uuid4)
    name = Column(ch_types.String, nullable=False)
    description = Column(ch_types.String, nullable=False, default="")
    is_public = Column(ch_types.UInt8, nullable=False, default=0)
    tags = Column(ch_types.Array(ch_types.String), nullable=False, default=[])
    created_at = Column(ch_types.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(ch_types.DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        engines.MergeTree(
            order_by=(created_at, id),
            primary_key=(id,),
        ),
    )


class Widget(Base):
    """Widget table for storing widget configurations."""

    __tablename__ = "widgets"

    id = Column(ch_types.UUID, primary_key=True, default=uuid4)
    dashboard_id = Column(ch_types.UUID, nullable=False)
    title = Column(ch_types.String, nullable=False)
    widget_type = Column(ch_types.String, nullable=False)  # table, line_chart, bar_chart, etc.
    position = Column(ch_types.String, nullable=False)  # JSON as string: {x, y, width, height}
    config = Column(ch_types.String, nullable=False)    # JSON as string: widget configuration
    created_at = Column(ch_types.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(ch_types.DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        engines.MergeTree(
            order_by=(dashboard_id, created_at, id),
            primary_key=(id,),
        ),
    )
