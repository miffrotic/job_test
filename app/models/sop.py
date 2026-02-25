"""S&OP fact table SQLAlchemy model."""

from datetime import datetime
from decimal import Decimal
from uuid import uuid4

from clickhouse_sqlalchemy import types as ch_types, engines
from sqlalchemy import Column, Index
from sqlalchemy.dialects import registry

from app.models.base import Base


class SopFact(Base):
    """
    S&OP (Sales & Operations Planning) fact table.
    
    Contains forecast/actual data with multiple dimensions for filtering.
    Partitioned by year-month for efficient time-based queries.
    """

    __tablename__ = "sop_facts"

    # Primary identifier
    id = Column(ch_types.UUID, primary_key=True, default=uuid4)

    # Time dimensions
    year = Column(ch_types.UInt16, nullable=False)
    month = Column(ch_types.UInt8, nullable=False)

    # Model type
    model = Column(ch_types.String, nullable=False, default="S&OP")

    # Classification dimensions
    priznak = Column(ch_types.String, nullable=False)  # Признак: Промо, Регуляр
    driver = Column(ch_types.String, nullable=False)   # Драйвер: Открытия, Промо, etc.

    # Financial code dimensions
    finkod_code = Column(ch_types.String, nullable=False)  # Код: AA, C1, C2...
    finkod_name = Column(ch_types.String, nullable=False)  # Полное название

    # Category dimensions
    category_code = Column(ch_types.String, nullable=False)  # Код категории
    category_name = Column(ch_types.String, nullable=False, default="")  # Название категории

    # Geographic dimensions
    territory = Column(ch_types.String, nullable=False)  # Территория
    macroregion_code = Column(ch_types.String, nullable=False)  # Код макрорегиона
    macroregion_name = Column(ch_types.String, nullable=False)  # Название макрорегиона

    # Measure dimension
    unit = Column(ch_types.String, nullable=False)  # ЕИ: Выручка, РТО, шт

    # Fact value
    value = Column(ch_types.Decimal(18, 2), nullable=False)  # Значение

    # Metadata
    created_at = Column(ch_types.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(ch_types.DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        # ClickHouse MergeTree engine with partitioning
        engines.MergeTree(
            partition_by="toYYYYMM(toDate(concat(toString(year), '-', lpad(toString(month), 2, '0'), '-01')))",
            order_by=(year, month, territory, macroregion_code, category_code, finkod_code, driver, priznak, unit),
            primary_key=(year, month, territory, macroregion_code),
        ),
        # Bloom filter indexes for efficient filtering
        Index("idx_territory", territory, clickhouse_type="bloom_filter"),
        Index("idx_macroregion", macroregion_code, clickhouse_type="bloom_filter"),
        Index("idx_category", category_code, clickhouse_type="bloom_filter"),
        Index("idx_finkod", finkod_code, clickhouse_type="bloom_filter"),
        Index("idx_driver", driver, clickhouse_type="bloom_filter"),
        Index("idx_priznak", priznak, clickhouse_type="bloom_filter"),
        Index("idx_unit", unit, clickhouse_type="bloom_filter"),
    )
