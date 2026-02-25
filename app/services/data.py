"""Data service for querying and aggregating data."""

import csv
import io
import json
import time
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from app.core.database import ClickHouseDatabase
from app.core.minio_client import MinIOClient
from app.schemas.data import (
    AggregationRequest,
    AggregationResponse,
    ChartDataRequest,
    ChartDataResponse,
    ChartSeries,
    DataQueryRequest,
    DataQueryResponse,
    ExportRequest,
    ExportResponse,
    TimeGranularity,
)
from app.schemas.base import FilterCondition, FilterOperator, FilterParams


class DataService:
    """Service for data operations."""

    def __init__(self, db: ClickHouseDatabase, minio: MinIOClient) -> None:
        self.db = db
        self.minio = minio

    def _build_filter_clause(
        self, filters: FilterParams | None, param_prefix: str = "f"
    ) -> tuple[str, dict]:
        """Build SQL WHERE clause from filter parameters."""
        if not filters or not filters.conditions:
            return "", {}

        clauses = []
        params = {}

        for i, condition in enumerate(filters.conditions):
            param_name = f"{param_prefix}_{i}"
            clause = self._build_condition(condition, param_name, params)
            if clause:
                clauses.append(clause)

        if not clauses:
            return "", {}

        logic = " AND " if filters.logic.upper() == "AND" else " OR "
        return f"({logic.join(clauses)})", params

    def _build_condition(
        self, condition: FilterCondition, param_name: str, params: dict
    ) -> str:
        """Build a single filter condition."""
        field = self._sanitize_identifier(condition.field)
        op = condition.operator

        if op == FilterOperator.EQ:
            params[param_name] = condition.value
            return f"{field} = %({param_name})s"
        elif op == FilterOperator.NEQ:
            params[param_name] = condition.value
            return f"{field} != %({param_name})s"
        elif op == FilterOperator.GT:
            params[param_name] = condition.value
            return f"{field} > %({param_name})s"
        elif op == FilterOperator.GTE:
            params[param_name] = condition.value
            return f"{field} >= %({param_name})s"
        elif op == FilterOperator.LT:
            params[param_name] = condition.value
            return f"{field} < %({param_name})s"
        elif op == FilterOperator.LTE:
            params[param_name] = condition.value
            return f"{field} <= %({param_name})s"
        elif op == FilterOperator.IN:
            params[param_name] = condition.values or [condition.value]
            return f"{field} IN %({param_name})s"
        elif op == FilterOperator.NOT_IN:
            params[param_name] = condition.values or [condition.value]
            return f"{field} NOT IN %({param_name})s"
        elif op == FilterOperator.LIKE:
            params[param_name] = condition.value
            return f"{field} LIKE %({param_name})s"
        elif op == FilterOperator.ILIKE:
            params[param_name] = condition.value
            return f"lower({field}) LIKE lower(%({param_name})s)"
        elif op == FilterOperator.BETWEEN:
            if condition.values and len(condition.values) >= 2:
                params[f"{param_name}_min"] = condition.values[0]
                params[f"{param_name}_max"] = condition.values[1]
                return f"{field} BETWEEN %({param_name}_min)s AND %({param_name}_max)s"
        elif op == FilterOperator.IS_NULL:
            return f"{field} IS NULL"
        elif op == FilterOperator.IS_NOT_NULL:
            return f"{field} IS NOT NULL"

        return ""

    def _sanitize_identifier(self, identifier: str) -> str:
        """Sanitize SQL identifier to prevent injection."""
        # Allow only alphanumeric characters and underscores
        sanitized = "".join(c for c in identifier if c.isalnum() or c == "_")
        return f"`{sanitized}`"

    def _get_time_function(self, granularity: TimeGranularity, column: str) -> str:
        """Get ClickHouse time truncation function."""
        col = self._sanitize_identifier(column)
        functions = {
            TimeGranularity.MINUTE: f"toStartOfMinute({col})",
            TimeGranularity.HOUR: f"toStartOfHour({col})",
            TimeGranularity.DAY: f"toStartOfDay({col})",
            TimeGranularity.WEEK: f"toStartOfWeek({col})",
            TimeGranularity.MONTH: f"toStartOfMonth({col})",
            TimeGranularity.QUARTER: f"toStartOfQuarter({col})",
            TimeGranularity.YEAR: f"toStartOfYear({col})",
        }
        return functions.get(granularity, col)

    async def query_data(self, request: DataQueryRequest) -> DataQueryResponse:
        """Query data with filtering, sorting, and pagination."""
        start_time = time.time()

        # Get table name
        table_name = await self._resolve_table_name(request.data_source_id, request.table_name)

        # Build SELECT columns
        if request.columns:
            columns = ", ".join(self._sanitize_identifier(c) for c in request.columns)
        else:
            columns = "*"

        # Build WHERE clause
        where_clause, filter_params = self._build_filter_clause(request.filters)
        where_sql = f"WHERE {where_clause}" if where_clause else ""

        # Build ORDER BY clause
        order_sql = ""
        if request.sort:
            order_parts = [
                f"{self._sanitize_identifier(s.field)} {s.order.value.upper()}"
                for s in request.sort
            ]
            order_sql = f"ORDER BY {', '.join(order_parts)}"

        # Calculate offset
        offset = (request.page - 1) * request.page_size

        # Build query
        query = f"""
            SELECT {columns}
            FROM {self._sanitize_identifier(table_name)}
            {where_sql}
            {order_sql}
            LIMIT %(limit)s OFFSET %(offset)s
        """

        params = {**filter_params, "limit": request.page_size, "offset": offset}

        # Execute query
        data = await self.db.fetch_all(query, params)

        # Get total count
        count_query = f"""
            SELECT count() as total
            FROM {self._sanitize_identifier(table_name)}
            {where_sql}
        """
        count_result = await self.db.fetch_one(count_query, filter_params)
        total = count_result["total"] if count_result else 0

        # Get column types
        columns_info = await self._get_column_info(table_name)

        query_time_ms = (time.time() - start_time) * 1000

        return DataQueryResponse(
            columns=columns_info,
            data=data,
            total=total,
            page=request.page,
            page_size=request.page_size,
            pages=(total + request.page_size - 1) // request.page_size,
            query_time_ms=round(query_time_ms, 2),
        )

    async def aggregate_data(self, request: AggregationRequest) -> AggregationResponse:
        """Aggregate data with grouping."""
        start_time = time.time()

        # Get table name
        table_name = await self._resolve_table_name(request.data_source_id, request.table_name)

        # Build SELECT with aggregations
        select_parts = []
        result_columns = []

        # Add group by columns
        group_by_cols = []
        if request.time_column and request.time_granularity:
            time_expr = self._get_time_function(request.time_granularity, request.time_column)
            select_parts.append(f"{time_expr} as time_bucket")
            result_columns.append("time_bucket")
            group_by_cols.append("time_bucket")

        for col in request.group_by:
            sanitized = self._sanitize_identifier(col)
            select_parts.append(sanitized)
            result_columns.append(col)
            group_by_cols.append(sanitized)

        # Add aggregations
        for agg in request.aggregations:
            col = self._sanitize_identifier(agg.column)
            alias = agg.alias or f"{agg.function.value}_{agg.column}"
            select_parts.append(f"{agg.function.value}({col}) as `{alias}`")
            result_columns.append(alias)

        # Build WHERE clause
        where_clause, filter_params = self._build_filter_clause(request.filters)
        where_sql = f"WHERE {where_clause}" if where_clause else ""

        # Build GROUP BY clause
        group_by_sql = ""
        if group_by_cols:
            group_by_sql = f"GROUP BY {', '.join(group_by_cols)}"

        # Build ORDER BY clause
        order_sql = ""
        if request.sort:
            order_parts = [
                f"{self._sanitize_identifier(s.field)} {s.order.value.upper()}"
                for s in request.sort
            ]
            order_sql = f"ORDER BY {', '.join(order_parts)}"
        elif group_by_cols:
            order_sql = f"ORDER BY {group_by_cols[0]}"

        # Build query
        query = f"""
            SELECT {', '.join(select_parts)}
            FROM {self._sanitize_identifier(table_name)}
            {where_sql}
            {group_by_sql}
            {order_sql}
            LIMIT %(limit)s
        """

        params = {**filter_params, "limit": request.limit}

        # Execute query
        data = await self.db.fetch_all(query, params)

        query_time_ms = (time.time() - start_time) * 1000

        return AggregationResponse(
            columns=result_columns,
            data=data,
            total_rows=len(data),
            query_time_ms=round(query_time_ms, 2),
        )

    async def get_chart_data(self, request: ChartDataRequest) -> ChartDataResponse:
        """Get data formatted for chart visualization."""
        start_time = time.time()

        # Get table name
        table_name = await self._resolve_table_name(request.data_source_id, request.table_name)

        series = []
        x_col = self._sanitize_identifier(request.x_column)

        # Build WHERE clause
        where_clause, filter_params = self._build_filter_clause(request.filters)
        where_sql = f"WHERE {where_clause}" if where_clause else ""

        # Handle time granularity for x-axis
        if request.time_granularity:
            x_expr = self._get_time_function(request.time_granularity, request.x_column)
        else:
            x_expr = x_col

        # Build query for each y-column
        for i, y_column in enumerate(request.y_columns):
            y_col = self._sanitize_identifier(y_column)
            
            # Determine aggregation
            agg_func = "avg"
            if request.y_aggregations and i < len(request.y_aggregations):
                agg_func = request.y_aggregations[i].value

            query = f"""
                SELECT {x_expr} as x, {agg_func}({y_col}) as y
                FROM {self._sanitize_identifier(table_name)}
                {where_sql}
                GROUP BY x
                ORDER BY x
                LIMIT %(limit)s
            """

            params = {**filter_params, "limit": request.limit}
            data = await self.db.fetch_all(query, params)

            series.append(
                ChartSeries(
                    name=y_column,
                    data=[{"x": row["x"], "y": row["y"]} for row in data],
                )
            )

        query_time_ms = (time.time() - start_time) * 1000
        total_points = sum(len(s.data) for s in series)

        return ChartDataResponse(
            chart_type=request.chart_type,
            series=series,
            x_axis_label=request.x_column,
            y_axis_label=request.y_columns[0] if len(request.y_columns) == 1 else None,
            total_points=total_points,
            query_time_ms=round(query_time_ms, 2),
        )

    async def export_data(self, request: ExportRequest) -> ExportResponse:
        """Export data to a file."""
        # Get table name
        table_name = await self._resolve_table_name(request.data_source_id, request.table_name)

        # Build SELECT columns
        if request.columns:
            columns = ", ".join(self._sanitize_identifier(c) for c in request.columns)
        else:
            columns = "*"

        # Build WHERE clause
        where_clause, filter_params = self._build_filter_clause(request.filters)
        where_sql = f"WHERE {where_clause}" if where_clause else ""

        # Build limit clause
        limit_sql = f"LIMIT {request.limit}" if request.limit else ""

        # Build query
        query = f"""
            SELECT {columns}
            FROM {self._sanitize_identifier(table_name)}
            {where_sql}
            {limit_sql}
        """

        # Execute query
        data = await self.db.fetch_all(query, filter_params)

        # Generate file content
        file_name = f"export_{uuid4().hex[:8]}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        if request.format == "csv":
            file_content = self._generate_csv(data)
            file_name += ".csv"
            content_type = "text/csv"
        elif request.format == "json":
            file_content = json.dumps(data, default=str, indent=2).encode()
            file_name += ".json"
            content_type = "application/json"
        else:
            # Default to CSV
            file_content = self._generate_csv(data)
            file_name += ".csv"
            content_type = "text/csv"

        # Upload to MinIO
        object_name = f"exports/{file_name}"
        self.minio.upload_file(
            file_data=file_content,
            object_name=object_name,
            content_type=content_type,
        )

        # Generate presigned URL
        expires_hours = 24
        url = self.minio.get_presigned_url(object_name, expires_hours=expires_hours)

        return ExportResponse(
            file_url=url,
            file_name=file_name,
            file_size=len(file_content),
            row_count=len(data),
            expires_at=datetime.utcnow() + timedelta(hours=expires_hours),
        )

    def _generate_csv(self, data: list[dict]) -> bytes:
        """Generate CSV content from data."""
        if not data:
            return b""

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

        return output.getvalue().encode("utf-8")

    async def _resolve_table_name(
        self, data_source_id: UUID | None, table_name: str | None
    ) -> str:
        """Resolve table name from data source ID or direct table name."""
        if table_name:
            return table_name

        if data_source_id:
            source = await self.db.fetch_one(
                "SELECT table_name, query FROM data_sources WHERE id = %(id)s",
                {"id": str(data_source_id)},
            )
            if source:
                return source.get("table_name") or f"({source.get('query')})"

        raise ValueError("Either data_source_id or table_name must be provided")

    async def _get_column_info(self, table_name: str) -> list[dict[str, str]]:
        """Get column information for a table."""
        rows = await self.db.fetch_all(
            """
            SELECT name, type
            FROM system.columns
            WHERE table = %(table)s
            """,
            {"table": table_name},
        )

        return [{"name": row["name"], "type": row["type"]} for row in rows]
