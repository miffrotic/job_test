"""Table service for managing tables and data sources."""

from datetime import datetime
from uuid import UUID, uuid4

from app.core.database import ClickHouseDatabase
from app.schemas.data import (
    DataSourceCreate,
    DataSourceResponse,
    DataSourceType,
    DataSourceUpdate,
    TableMetadata,
)
from app.schemas.base import PaginatedResponse


class TableService:
    """Service for table and data source operations."""

    def __init__(self, db: ClickHouseDatabase) -> None:
        self.db = db

    async def list_tables(self, database: str | None = None) -> list[TableMetadata]:
        """List all tables in the database."""
        db_filter = f"AND database = '{database}'" if database else ""

        rows = await self.db.fetch_all(
            f"""
            SELECT 
                database,
                name as table_name,
                engine,
                total_rows,
                total_bytes
            FROM system.tables
            WHERE database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
            {db_filter}
            ORDER BY database, name
            """
        )

        tables = []
        for row in rows:
            columns = await self.get_table_columns(row["table_name"], row["database"])
            tables.append(
                TableMetadata(
                    table_name=row["table_name"],
                    database=row["database"],
                    engine=row["engine"],
                    total_rows=row["total_rows"] or 0,
                    total_bytes=row["total_bytes"] or 0,
                    columns=columns,
                )
            )

        return tables

    async def get_table_metadata(
        self, table_name: str, database: str | None = None
    ) -> TableMetadata | None:
        """Get metadata for a specific table."""
        db_filter = f"AND database = '{database}'" if database else ""

        row = await self.db.fetch_one(
            f"""
            SELECT 
                database,
                name as table_name,
                engine,
                total_rows,
                total_bytes
            FROM system.tables
            WHERE name = %(table_name)s
            {db_filter}
            """,
            {"table_name": table_name},
        )

        if not row:
            return None

        columns = await self.get_table_columns(table_name, row["database"])

        return TableMetadata(
            table_name=row["table_name"],
            database=row["database"],
            engine=row["engine"],
            total_rows=row["total_rows"] or 0,
            total_bytes=row["total_bytes"] or 0,
            columns=columns,
        )

    async def get_table_columns(
        self, table_name: str, database: str | None = None
    ) -> list[dict]:
        """Get column definitions for a table."""
        db_filter = f"AND database = '{database}'" if database else ""

        rows = await self.db.fetch_all(
            f"""
            SELECT 
                name,
                type,
                default_kind,
                default_expression,
                comment
            FROM system.columns
            WHERE table = %(table_name)s
            {db_filter}
            ORDER BY position
            """,
            {"table_name": table_name},
        )

        return [
            {
                "name": row["name"],
                "type": row["type"],
                "default_kind": row.get("default_kind", ""),
                "default_expression": row.get("default_expression", ""),
                "comment": row.get("comment", ""),
            }
            for row in rows
        ]

    async def get_table_sample(self, table_name: str, limit: int = 100) -> dict:
        """Get sample data from a table."""
        # Get columns first
        columns = await self.get_table_columns(table_name)

        # Sanitize table name
        sanitized_table = "".join(c for c in table_name if c.isalnum() or c == "_")

        # Get sample data
        data = await self.db.fetch_all(
            f"SELECT * FROM `{sanitized_table}` LIMIT %(limit)s",
            {"limit": limit},
        )

        return {
            "columns": columns,
            "data": data,
            "row_count": len(data),
        }

    # Data Sources CRUD
    async def create_data_source(self, data: DataSourceCreate) -> DataSourceResponse:
        """Create a new data source."""
        source_id = uuid4()
        now = datetime.utcnow()

        await self.db.execute(
            """
            INSERT INTO data_sources (
                id, name, description, source_type, table_name, 
                query, connection_config, created_at, updated_at
            )
            VALUES (
                %(id)s, %(name)s, %(description)s, %(source_type)s, %(table_name)s,
                %(query)s, %(connection_config)s, %(created_at)s, %(updated_at)s
            )
            """,
            {
                "id": str(source_id),
                "name": data.name,
                "description": data.description or "",
                "source_type": data.source_type.value,
                "table_name": data.table_name or "",
                "query": data.query or "",
                "connection_config": str(data.connection_config or {}),
                "created_at": now,
                "updated_at": now,
            },
        )

        # Get column info if table type
        columns = None
        if data.source_type == DataSourceType.TABLE and data.table_name:
            columns = await self.get_table_columns(data.table_name)

        return DataSourceResponse(
            id=source_id,
            name=data.name,
            description=data.description,
            source_type=data.source_type,
            table_name=data.table_name,
            query=data.query,
            connection_config=data.connection_config,
            created_at=now,
            updated_at=now,
            columns=columns,
        )

    async def get_data_source(self, source_id: UUID) -> DataSourceResponse | None:
        """Get a data source by ID."""
        row = await self.db.fetch_one(
            "SELECT * FROM data_sources WHERE id = %(id)s",
            {"id": str(source_id)},
        )

        if not row:
            return None

        # Get column info if table type
        columns = None
        if row["source_type"] == DataSourceType.TABLE.value and row["table_name"]:
            columns = await self.get_table_columns(row["table_name"])

        return DataSourceResponse(
            id=UUID(row["id"]),
            name=row["name"],
            description=row["description"],
            source_type=row["source_type"],
            table_name=row["table_name"],
            query=row["query"],
            connection_config=eval(row["connection_config"]) if row["connection_config"] else None,
            created_at=row["created_at"],
            updated_at=row.get("updated_at"),
            columns=columns,
        )

    async def list_data_sources(
        self, page: int = 1, page_size: int = 20
    ) -> PaginatedResponse[DataSourceResponse]:
        """List data sources with pagination."""
        offset = (page - 1) * page_size

        # Get total count
        count_result = await self.db.fetch_one(
            "SELECT count() as total FROM data_sources"
        )
        total = count_result["total"] if count_result else 0

        # Get data sources
        rows = await self.db.fetch_all(
            """
            SELECT * FROM data_sources
            ORDER BY created_at DESC
            LIMIT %(limit)s OFFSET %(offset)s
            """,
            {"limit": page_size, "offset": offset},
        )

        items = []
        for row in rows:
            columns = None
            if row["source_type"] == DataSourceType.TABLE.value and row["table_name"]:
                columns = await self.get_table_columns(row["table_name"])

            items.append(
                DataSourceResponse(
                    id=UUID(row["id"]),
                    name=row["name"],
                    description=row["description"],
                    source_type=row["source_type"],
                    table_name=row["table_name"],
                    query=row["query"],
                    connection_config=eval(row["connection_config"]) if row["connection_config"] else None,
                    created_at=row["created_at"],
                    updated_at=row.get("updated_at"),
                    columns=columns,
                )
            )

        return PaginatedResponse.create(
            items=items,
            total=total,
            page=page,
            page_size=page_size,
        )

    async def update_data_source(
        self, source_id: UUID, data: DataSourceUpdate
    ) -> DataSourceResponse | None:
        """Update a data source."""
        existing = await self.get_data_source(source_id)
        if not existing:
            return None

        update_fields = []
        params = {"id": str(source_id), "updated_at": datetime.utcnow()}

        if data.name is not None:
            update_fields.append("name = %(name)s")
            params["name"] = data.name
        if data.description is not None:
            update_fields.append("description = %(description)s")
            params["description"] = data.description
        if data.table_name is not None:
            update_fields.append("table_name = %(table_name)s")
            params["table_name"] = data.table_name
        if data.query is not None:
            update_fields.append("query = %(query)s")
            params["query"] = data.query
        if data.connection_config is not None:
            update_fields.append("connection_config = %(connection_config)s")
            params["connection_config"] = str(data.connection_config)

        update_fields.append("updated_at = %(updated_at)s")

        if update_fields:
            await self.db.execute(
                f"""
                ALTER TABLE data_sources UPDATE {', '.join(update_fields)}
                WHERE id = %(id)s
                """,
                params,
            )

        return await self.get_data_source(source_id)

    async def delete_data_source(self, source_id: UUID) -> bool:
        """Delete a data source."""
        existing = await self.get_data_source(source_id)
        if not existing:
            return False

        await self.db.execute(
            "ALTER TABLE data_sources DELETE WHERE id = %(id)s",
            {"id": str(source_id)},
        )

        return True
