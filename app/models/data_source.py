"""Data source SQLAlchemy model."""

from datetime import datetime
from uuid import uuid4

from clickhouse_sqlalchemy import types as ch_types, engines
from sqlalchemy import Column

from app.models.base import Base


class DataSource(Base):
    """Data source table for storing data source configurations."""

    __tablename__ = "data_sources"

    id = Column(ch_types.UUID, primary_key=True, default=uuid4)
    name = Column(ch_types.String, nullable=False)
    description = Column(ch_types.String, nullable=False, default="")
    source_type = Column(ch_types.String, nullable=False)  # table, query, external
    table_name = Column(ch_types.String, nullable=False, default="")
    query = Column(ch_types.String, nullable=False, default="")
    connection_config = Column(ch_types.String, nullable=False, default="")  # JSON as string
    created_at = Column(ch_types.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(ch_types.DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        engines.MergeTree(
            order_by=(created_at, id),
            primary_key=(id,),
        ),
    )
