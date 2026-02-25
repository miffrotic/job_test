"""Dimension service for reference data."""

from app.core.database import ClickHouseDatabase


class DimensionService:
    """Service for dimension (reference) data operations."""

    def __init__(self, db: ClickHouseDatabase) -> None:
        self.db = db

    async def get_territories(self, include_inactive: bool = False) -> list[dict]:
        """Get all territories."""
        where = "" if include_inactive else "WHERE is_active = 1"
        query = f"""
            SELECT code, name, sort_order
            FROM dim_territory
            {where}
            ORDER BY sort_order, name
        """
        return await self.db.fetch_all(query)

    async def get_macroregions(
        self,
        territory_code: str | None = None,
        include_inactive: bool = False,
    ) -> list[dict]:
        """Get macroregions, optionally filtered by territory."""
        conditions = []
        params = {}

        if not include_inactive:
            conditions.append("is_active = 1")

        if territory_code:
            conditions.append("territory_code = %(territory_code)s")
            params["territory_code"] = territory_code

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        query = f"""
            SELECT code, name, territory_code, sort_order
            FROM dim_macroregion
            {where}
            ORDER BY territory_code, sort_order, name
        """
        return await self.db.fetch_all(query, params)

    async def get_categories(
        self,
        parent_code: str | None = None,
        include_inactive: bool = False,
    ) -> list[dict]:
        """Get categories, optionally filtered by parent."""
        conditions = []
        params = {}

        if not include_inactive:
            conditions.append("is_active = 1")

        if parent_code:
            conditions.append("parent_code = %(parent_code)s")
            params["parent_code"] = parent_code

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        query = f"""
            SELECT code, name, parent_code, level, sort_order
            FROM dim_category
            {where}
            ORDER BY sort_order, name
        """
        return await self.db.fetch_all(query, params)

    async def get_finkods(
        self,
        group_code: str | None = None,
        include_inactive: bool = False,
    ) -> list[dict]:
        """Get financial codes, optionally filtered by group."""
        conditions = []
        params = {}

        if not include_inactive:
            conditions.append("is_active = 1")

        if group_code:
            conditions.append("group_code = %(group_code)s")
            params["group_code"] = group_code

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        query = f"""
            SELECT code, name, group_code, sort_order
            FROM dim_finkod
            {where}
            ORDER BY sort_order, code
        """
        return await self.db.fetch_all(query, params)

    async def get_drivers(self, include_inactive: bool = False) -> list[dict]:
        """Get all drivers."""
        where = "" if include_inactive else "WHERE is_active = 1"
        query = f"""
            SELECT code, name, sort_order
            FROM dim_driver
            {where}
            ORDER BY sort_order, name
        """
        return await self.db.fetch_all(query)

    async def get_priznaks(self, include_inactive: bool = False) -> list[dict]:
        """Get all priznaks."""
        where = "" if include_inactive else "WHERE is_active = 1"
        query = f"""
            SELECT code, name, sort_order
            FROM dim_priznak
            {where}
            ORDER BY sort_order, name
        """
        return await self.db.fetch_all(query)

    async def get_units(self, include_inactive: bool = False) -> list[dict]:
        """Get all units of measure."""
        where = "" if include_inactive else "WHERE is_active = 1"
        query = f"""
            SELECT code, name, sort_order
            FROM dim_unit
            {where}
            ORDER BY sort_order, name
        """
        return await self.db.fetch_all(query)

    async def get_available_years(self) -> list[int]:
        """Get available years from S&OP facts."""
        query = """
            SELECT DISTINCT year
            FROM sop_facts
            ORDER BY year DESC
        """
        result = await self.db.fetch_all(query)
        return [row["year"] for row in result]

    async def get_finkod_groups(self) -> list[dict]:
        """Get unique finkod groups."""
        query = """
            SELECT DISTINCT group_code as code, group_code as name
            FROM dim_finkod
            WHERE group_code != ''
            ORDER BY group_code
        """
        return await self.db.fetch_all(query)
