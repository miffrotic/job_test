"""ClickHouse database connection and session management."""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from app.core.config import settings


class ClickHouseDatabase:
    """ClickHouse async database manager."""

    def __init__(self) -> None:
        self._client: AsyncClient | None = None
        self._engine = None
        self._session_factory = None

    @property
    def sqlalchemy_url(self) -> str:
        """Build SQLAlchemy connection URL for ClickHouse."""
        password = settings.CLICKHOUSE_PASSWORD or ""
        return (
            f"clickhouse+native://{settings.CLICKHOUSE_USER}:{password}"
            f"@{settings.CLICKHOUSE_HOST}:{settings.CLICKHOUSE_PORT}"
            f"/{settings.CLICKHOUSE_DATABASE}"
        )

    @property
    def engine(self):
        """Get or create SQLAlchemy engine."""
        if self._engine is None:
            self._engine = create_engine(self.sqlalchemy_url, echo=settings.DEBUG)
        return self._engine

    @property
    def session_factory(self):
        """Get or create session factory."""
        if self._session_factory is None:
            self._session_factory = sessionmaker(bind=self.engine)
        return self._session_factory

    def get_session(self) -> Session:
        """Create a new SQLAlchemy session."""
        return self.session_factory()

    async def connect(self) -> None:
        """Create async ClickHouse client connection."""
        self._client = await clickhouse_connect.get_async_client(
            host=settings.CLICKHOUSE_HOST,
            port=settings.CLICKHOUSE_HTTP_PORT,
            username=settings.CLICKHOUSE_USER,
            password=settings.CLICKHOUSE_PASSWORD,
            database=settings.CLICKHOUSE_DATABASE,
        )

    async def disconnect(self) -> None:
        """Close ClickHouse client connection."""
        if self._client:
            self._client.close()
            self._client = None

    @property
    def client(self) -> AsyncClient:
        """Get the async client instance."""
        if self._client is None:
            raise RuntimeError("Database not connected. Call connect() first.")
        return self._client

    async def execute(self, query: str, parameters: dict | None = None) -> None:
        """Execute a query without returning results."""
        await self.client.command(query, parameters=parameters or {})

    async def fetch_all(
        self, query: str, parameters: dict | None = None
    ) -> list[dict]:
        """Fetch all rows as list of dictionaries."""
        result = await self.client.query(query, parameters=parameters or {})
        columns = result.column_names
        return [dict(zip(columns, row)) for row in result.result_rows]

    async def fetch_one(
        self, query: str, parameters: dict | None = None
    ) -> dict | None:
        """Fetch a single row as dictionary."""
        rows = await self.fetch_all(query, parameters)
        return rows[0] if rows else None

    async def insert(
        self,
        table: str,
        data: list[dict],
        column_names: list[str] | None = None,
    ) -> None:
        """Insert data into a table."""
        if not data:
            return

        if column_names is None:
            column_names = list(data[0].keys())

        rows = [[row.get(col) for col in column_names] for row in data]
        await self.client.insert(table, rows, column_names=column_names)


# Global database instance
database = ClickHouseDatabase()


@asynccontextmanager
async def get_db() -> AsyncGenerator[ClickHouseDatabase, None]:
    """Dependency for getting database connection."""
    yield database


async def get_database() -> ClickHouseDatabase:
    """FastAPI dependency for database access."""
    return database
