"""S&OP service for querying and aggregating S&OP data."""

import csv
import io
import json
import time
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import uuid4

from app.core.database import ClickHouseDatabase
from app.core.minio_client import MinIOClient
from app.schemas.sop import (
    DimensionValue,
    SopAggregationRequest,
    SopAggregationResponse,
    SopChartRequest,
    SopChartResponse,
    SopChartSeries,
    SopDataResponse,
    SopExportRequest,
    SopExportResponse,
    SopFilterParams,
    SopFiltersResponse,
    SopQueryRequest,
)


class SopService:
    """Service for S&OP data operations."""

    # Table and column names
    FACT_TABLE = "sop_facts"
    
    # Column mappings for display
    COLUMN_LABELS = {
        "model": "Модель",
        "priznak": "Признак",
        "driver": "Драйвер",
        "finkod_code": "Финкод Код",
        "finkod_name": "Финкод код_назв",
        "category_code": "Категория",
        "category_name": "Категория название",
        "macroregion_code": "Макрорегион код",
        "macroregion_name": "Макрорегион название",
        "territory": "Территория название",
        "year": "Год",
        "month": "Месяц",
        "unit": "ЕИ",
        "value": "Прогноз",
    }

    def __init__(self, db: ClickHouseDatabase, minio: MinIOClient | None = None) -> None:
        self.db = db
        self.minio = minio

    def _build_where_clause(
        self, filters: SopFilterParams | None
    ) -> tuple[str, dict]:
        """Build WHERE clause from filter parameters."""
        if not filters:
            return "", {}

        conditions = []
        params = {}

        # Year filter
        if filters.years:
            conditions.append("year IN %(years)s")
            params["years"] = filters.years

        # Month filter
        if filters.months:
            conditions.append("month IN %(months)s")
            params["months"] = filters.months

        # Priznak filter
        if filters.priznaks:
            conditions.append("priznak IN %(priznaks)s")
            params["priznaks"] = filters.priznaks

        # Driver filter
        if filters.drivers:
            conditions.append("driver IN %(drivers)s")
            params["drivers"] = filters.drivers

        # Finkod code filter
        if filters.finkod_codes:
            conditions.append("finkod_code IN %(finkod_codes)s")
            params["finkod_codes"] = filters.finkod_codes

        # Finkod group filter (first letter of code)
        if filters.finkod_groups:
            group_conditions = [
                f"startsWith(finkod_code, '{g}')" for g in filters.finkod_groups
            ]
            conditions.append(f"({' OR '.join(group_conditions)})")

        # Category filter
        if filters.category_codes:
            conditions.append("category_code IN %(category_codes)s")
            params["category_codes"] = filters.category_codes

        # Territory filter
        if filters.territories:
            conditions.append("territory IN %(territories)s")
            params["territories"] = filters.territories

        # Macroregion filter
        if filters.macroregion_codes:
            conditions.append("macroregion_code IN %(macroregion_codes)s")
            params["macroregion_codes"] = filters.macroregion_codes

        # Unit filter
        if filters.units:
            conditions.append("unit IN %(units)s")
            params["units"] = filters.units

        # Full-text search
        if filters.search:
            search_pattern = f"%{filters.search}%"
            conditions.append(
                """(
                    finkod_name ILIKE %(search)s OR
                    category_name ILIKE %(search)s OR
                    macroregion_name ILIKE %(search)s OR
                    territory ILIKE %(search)s
                )"""
            )
            params["search"] = search_pattern

        if not conditions:
            return "", {}

        return "WHERE " + " AND ".join(conditions), params

    def _sanitize_column(self, column: str) -> str:
        """Sanitize column name."""
        allowed = set(self.COLUMN_LABELS.keys()) | {"id", "created_at", "updated_at"}
        if column in allowed:
            return f"`{column}`"
        return "`id`"  # Default to safe column

    async def query_data(self, request: SopQueryRequest) -> SopDataResponse:
        """Query S&OP data with filtering, sorting, and pagination."""
        start_time = time.time()

        # Build WHERE clause
        where_clause, params = self._build_where_clause(request.filters)

        # Build SELECT columns
        if request.columns:
            columns = ", ".join(self._sanitize_column(c) for c in request.columns)
        else:
            columns = ", ".join(f"`{c}`" for c in self.COLUMN_LABELS.keys())

        # Build ORDER BY
        order_parts = []
        for sort in request.sort:
            col = self._sanitize_column(sort.field)
            direction = "DESC" if sort.order.lower() == "desc" else "ASC"
            order_parts.append(f"{col} {direction}")
        
        order_by = f"ORDER BY {', '.join(order_parts)}" if order_parts else "ORDER BY year DESC, month ASC"

        # Calculate offset
        offset = (request.page - 1) * request.page_size
        params["limit"] = request.page_size
        params["offset"] = offset

        # Execute main query
        query = f"""
            SELECT {columns}
            FROM {self.FACT_TABLE}
            {where_clause}
            {order_by}
            LIMIT %(limit)s OFFSET %(offset)s
        """
        data = await self.db.fetch_all(query, params)

        # Get total count
        count_query = f"""
            SELECT count() as total
            FROM {self.FACT_TABLE}
            {where_clause}
        """
        # Remove pagination params for count query
        count_params = {k: v for k, v in params.items() if k not in ("limit", "offset")}
        count_result = await self.db.fetch_one(count_query, count_params)
        total = count_result["total"] if count_result else 0

        # Build column definitions
        column_defs = [
            {"name": col, "label": self.COLUMN_LABELS.get(col, col)}
            for col in (request.columns or list(self.COLUMN_LABELS.keys()))
        ]

        query_time_ms = (time.time() - start_time) * 1000

        return SopDataResponse(
            columns=column_defs,
            data=data,
            total=total,
            page=request.page,
            page_size=request.page_size,
            pages=(total + request.page_size - 1) // request.page_size if request.page_size > 0 else 0,
            query_time_ms=round(query_time_ms, 2),
        )

    async def get_filter_options(
        self, current_filters: SopFilterParams | None = None
    ) -> SopFiltersResponse:
        """Get available filter options, optionally scoped by current filters."""
        where_clause, params = self._build_where_clause(current_filters)

        # Get years
        years_query = f"""
            SELECT DISTINCT year
            FROM {self.FACT_TABLE}
            {where_clause}
            ORDER BY year DESC
        """
        years_result = await self.db.fetch_all(years_query, params)
        years = [row["year"] for row in years_result]

        # Get territories with counts
        territories_query = f"""
            SELECT territory as code, territory as name, count() as count
            FROM {self.FACT_TABLE}
            {where_clause}
            GROUP BY territory
            ORDER BY territory
        """
        territories_result = await self.db.fetch_all(territories_query, params)
        territories = [
            DimensionValue(code=row["code"], name=row["name"], count=row["count"])
            for row in territories_result
        ]

        # Get macroregions
        macroregions_query = f"""
            SELECT 
                macroregion_code as code, 
                macroregion_name as name, 
                territory as parent_code,
                count() as count
            FROM {self.FACT_TABLE}
            {where_clause}
            GROUP BY macroregion_code, macroregion_name, territory
            ORDER BY territory, macroregion_name
        """
        macroregions_result = await self.db.fetch_all(macroregions_query, params)
        macroregions = [
            DimensionValue(
                code=row["code"],
                name=row["name"],
                parent_code=row["parent_code"],
                count=row["count"],
            )
            for row in macroregions_result
        ]

        # Get categories
        categories_query = f"""
            SELECT category_code as code, category_name as name, count() as count
            FROM {self.FACT_TABLE}
            {where_clause}
            GROUP BY category_code, category_name
            ORDER BY category_name
        """
        categories_result = await self.db.fetch_all(categories_query, params)
        categories = [
            DimensionValue(code=row["code"], name=row["name"], count=row["count"])
            for row in categories_result
        ]

        # Get finkod codes
        finkods_query = f"""
            SELECT finkod_code as code, finkod_code as name, count() as count
            FROM {self.FACT_TABLE}
            {where_clause}
            GROUP BY finkod_code
            ORDER BY finkod_code
        """
        finkods_result = await self.db.fetch_all(finkods_query, params)
        finkods = [
            DimensionValue(code=row["code"], name=row["name"], count=row["count"])
            for row in finkods_result
        ]

        # Get drivers
        drivers_query = f"""
            SELECT driver as code, driver as name, count() as count
            FROM {self.FACT_TABLE}
            {where_clause}
            GROUP BY driver
            ORDER BY driver
        """
        drivers_result = await self.db.fetch_all(drivers_query, params)
        drivers = [
            DimensionValue(code=row["code"], name=row["name"], count=row["count"])
            for row in drivers_result
        ]

        # Get priznaks
        priznaks_query = f"""
            SELECT priznak as code, priznak as name, count() as count
            FROM {self.FACT_TABLE}
            {where_clause}
            GROUP BY priznak
            ORDER BY priznak
        """
        priznaks_result = await self.db.fetch_all(priznaks_query, params)
        priznaks = [
            DimensionValue(code=row["code"], name=row["name"], count=row["count"])
            for row in priznaks_result
        ]

        # Get units
        units_query = f"""
            SELECT unit as code, unit as name, count() as count
            FROM {self.FACT_TABLE}
            {where_clause}
            GROUP BY unit
            ORDER BY unit
        """
        units_result = await self.db.fetch_all(units_query, params)
        units = [
            DimensionValue(code=row["code"], name=row["name"], count=row["count"])
            for row in units_result
        ]

        return SopFiltersResponse(
            years=years,
            territories=territories,
            macroregions=macroregions,
            categories=categories,
            finkod_codes=finkods,
            drivers=drivers,
            priznaks=priznaks,
            units=units,
        )

    async def aggregate_data(
        self, request: SopAggregationRequest
    ) -> SopAggregationResponse:
        """Aggregate S&OP data with grouping."""
        start_time = time.time()

        where_clause, params = self._build_where_clause(request.filters)

        # Build GROUP BY columns
        group_cols = [self._sanitize_column(c) for c in request.group_by]

        # Add time grouping if specified
        if request.time_granularity == "quarter":
            group_cols.append("toQuarter(toDate(concat(toString(year), '-', toString(month), '-01'))) as quarter")
        elif request.time_granularity == "year":
            # year is already available
            pass

        # Build aggregations
        agg_expressions = []
        value_col = self._sanitize_column(request.value_column)
        
        for agg in request.aggregations:
            if agg == "sum":
                agg_expressions.append(f"sum({value_col}) as sum_value")
            elif agg == "avg":
                agg_expressions.append(f"avg({value_col}) as avg_value")
            elif agg == "count":
                agg_expressions.append("count() as count_value")
            elif agg == "min":
                agg_expressions.append(f"min({value_col}) as min_value")
            elif agg == "max":
                agg_expressions.append(f"max({value_col}) as max_value")

        select_parts = group_cols + agg_expressions
        group_by_clause = f"GROUP BY {', '.join(request.group_by)}" if request.group_by else ""

        params["limit"] = request.limit

        query = f"""
            SELECT {', '.join(select_parts)}
            FROM {self.FACT_TABLE}
            {where_clause}
            {group_by_clause}
            ORDER BY {request.group_by[0] if request.group_by else '1'}
            LIMIT %(limit)s
        """

        data = await self.db.fetch_all(query, params)
        columns = request.group_by + [f"{agg}_value" for agg in request.aggregations]

        query_time_ms = (time.time() - start_time) * 1000

        return SopAggregationResponse(
            columns=columns,
            data=data,
            total_rows=len(data),
            query_time_ms=round(query_time_ms, 2),
        )

    async def get_chart_data(self, request: SopChartRequest) -> SopChartResponse:
        """Get chart data for S&OP visualization."""
        start_time = time.time()

        where_clause, params = self._build_where_clause(request.filters)
        params["limit"] = request.limit

        x_col = self._sanitize_column(request.x_axis)
        y_col = self._sanitize_column(request.y_axis)

        # Determine aggregation function
        agg_func = request.aggregation
        if agg_func not in ("sum", "avg", "count", "min", "max"):
            agg_func = "sum"

        if request.series_by:
            # Multiple series
            series_col = self._sanitize_column(request.series_by)
            query = f"""
                SELECT 
                    {x_col} as x,
                    {series_col} as series,
                    {agg_func}({y_col}) as y
                FROM {self.FACT_TABLE}
                {where_clause}
                GROUP BY {request.x_axis}, {request.series_by}
                ORDER BY {request.x_axis}, {request.series_by}
                LIMIT %(limit)s
            """
            data = await self.db.fetch_all(query, params)

            # Group by series
            series_data: dict[str, list] = {}
            for row in data:
                series_name = str(row["series"])
                if series_name not in series_data:
                    series_data[series_name] = []
                series_data[series_name].append({"x": row["x"], "y": row["y"]})

            series = [
                SopChartSeries(name=name, data=points)
                for name, points in series_data.items()
            ]
        else:
            # Single series
            query = f"""
                SELECT 
                    {x_col} as x,
                    {agg_func}({y_col}) as y
                FROM {self.FACT_TABLE}
                {where_clause}
                GROUP BY {request.x_axis}
                ORDER BY {request.x_axis}
                LIMIT %(limit)s
            """
            data = await self.db.fetch_all(query, params)
            series = [
                SopChartSeries(
                    name=self.COLUMN_LABELS.get(request.y_axis, request.y_axis),
                    data=[{"x": row["x"], "y": row["y"]} for row in data],
                )
            ]

        total_points = sum(len(s.data) for s in series)
        query_time_ms = (time.time() - start_time) * 1000

        return SopChartResponse(
            chart_type=request.chart_type,
            x_axis_label=self.COLUMN_LABELS.get(request.x_axis, request.x_axis),
            y_axis_label=self.COLUMN_LABELS.get(request.y_axis, request.y_axis),
            series=series,
            total_points=total_points,
            query_time_ms=round(query_time_ms, 2),
        )

    async def export_data(self, request: SopExportRequest) -> SopExportResponse:
        """Export S&OP data to file."""
        if not self.minio:
            raise ValueError("MinIO client not configured for export")

        where_clause, params = self._build_where_clause(request.filters)

        # Build columns
        if request.columns:
            columns = ", ".join(self._sanitize_column(c) for c in request.columns)
        else:
            columns = ", ".join(f"`{c}`" for c in self.COLUMN_LABELS.keys())

        limit_clause = f"LIMIT {request.limit}" if request.limit else ""

        query = f"""
            SELECT {columns}
            FROM {self.FACT_TABLE}
            {where_clause}
            ORDER BY year DESC, month ASC
            {limit_clause}
        """

        data = await self.db.fetch_all(query, params)

        # Generate file
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        
        if request.format == "csv":
            file_content = self._generate_csv(data)
            file_name = f"sop_export_{timestamp}.csv"
            content_type = "text/csv"
        elif request.format == "json":
            file_content = json.dumps(data, default=str, ensure_ascii=False, indent=2).encode("utf-8")
            file_name = f"sop_export_{timestamp}.json"
            content_type = "application/json"
        else:
            # Default CSV
            file_content = self._generate_csv(data)
            file_name = f"sop_export_{timestamp}.csv"
            content_type = "text/csv"

        # Upload to MinIO
        object_name = f"exports/sop/{file_name}"
        self.minio.upload_file(
            file_data=file_content,
            object_name=object_name,
            content_type=content_type,
        )

        # Generate presigned URL
        expires_hours = 24
        url = self.minio.get_presigned_url(object_name, expires_hours=expires_hours)

        return SopExportResponse(
            file_url=url,
            file_name=file_name,
            file_size=len(file_content),
            row_count=len(data),
            expires_at=datetime.utcnow() + timedelta(hours=expires_hours),
        )

    def _generate_csv(self, data: list[dict]) -> bytes:
        """Generate CSV content."""
        if not data:
            return b""

        output = io.StringIO()
        
        # Use display labels as headers
        fieldnames = list(data[0].keys())
        headers = {f: self.COLUMN_LABELS.get(f, f) for f in fieldnames}
        
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        
        # Write header with labels
        writer.writerow(headers)
        writer.writerows(data)

        return output.getvalue().encode("utf-8")
