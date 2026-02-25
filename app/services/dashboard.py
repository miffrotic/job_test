"""Dashboard service for managing dashboards and widgets."""

import json
from datetime import datetime
from uuid import UUID, uuid4

from app.core.database import ClickHouseDatabase
from app.schemas.dashboard import (
    DashboardCreate,
    DashboardListResponse,
    DashboardResponse,
    DashboardUpdate,
    WidgetCreate,
    WidgetResponse,
    WidgetUpdate,
)
from app.schemas.base import PaginatedResponse


class DashboardService:
    """Service for dashboard operations."""

    def __init__(self, db: ClickHouseDatabase) -> None:
        self.db = db

    async def create_dashboard(self, data: DashboardCreate) -> DashboardResponse:
        """Create a new dashboard."""
        dashboard_id = uuid4()
        now = datetime.utcnow()

        await self.db.execute(
            """
            INSERT INTO dashboards (id, name, description, is_public, tags, created_at, updated_at)
            VALUES (%(id)s, %(name)s, %(description)s, %(is_public)s, %(tags)s, %(created_at)s, %(updated_at)s)
            """,
            {
                "id": str(dashboard_id),
                "name": data.name,
                "description": data.description or "",
                "is_public": data.is_public,
                "tags": data.tags,
                "created_at": now,
                "updated_at": now,
            },
        )

        return DashboardResponse(
            id=dashboard_id,
            name=data.name,
            description=data.description,
            is_public=data.is_public,
            tags=data.tags,
            created_at=now,
            updated_at=now,
            widgets=[],
        )

    async def get_dashboard(self, dashboard_id: UUID) -> DashboardResponse | None:
        """Get a dashboard by ID with its widgets."""
        row = await self.db.fetch_one(
            "SELECT * FROM dashboards WHERE id = %(id)s",
            {"id": str(dashboard_id)},
        )

        if not row:
            return None

        widgets = await self.list_widgets(dashboard_id)

        return DashboardResponse(
            id=UUID(row["id"]),
            name=row["name"],
            description=row["description"],
            is_public=row["is_public"],
            tags=row["tags"],
            created_at=row["created_at"],
            updated_at=row.get("updated_at"),
            widgets=widgets,
        )

    async def list_dashboards(
        self,
        page: int = 1,
        page_size: int = 20,
        search: str | None = None,
        tags: list[str] | None = None,
    ) -> PaginatedResponse[DashboardListResponse]:
        """List dashboards with pagination and filtering."""
        offset = (page - 1) * page_size

        # Build query conditions
        conditions = []
        params: dict = {"limit": page_size, "offset": offset}

        if search:
            conditions.append("(name ILIKE %(search)s OR description ILIKE %(search)s)")
            params["search"] = f"%{search}%"

        if tags:
            conditions.append("hasAny(tags, %(tags)s)")
            params["tags"] = tags

        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

        # Get total count
        count_result = await self.db.fetch_one(
            f"SELECT count() as total FROM dashboards {where_clause}",
            params,
        )
        total = count_result["total"] if count_result else 0

        # Get dashboards with widget count
        rows = await self.db.fetch_all(
            f"""
            SELECT d.*, 
                   (SELECT count() FROM widgets WHERE dashboard_id = d.id) as widget_count
            FROM dashboards d
            {where_clause}
            ORDER BY created_at DESC
            LIMIT %(limit)s OFFSET %(offset)s
            """,
            params,
        )

        items = [
            DashboardListResponse(
                id=UUID(row["id"]),
                name=row["name"],
                description=row["description"],
                is_public=row["is_public"],
                tags=row["tags"],
                created_at=row["created_at"],
                widget_count=row["widget_count"],
            )
            for row in rows
        ]

        return PaginatedResponse.create(
            items=items,
            total=total,
            page=page,
            page_size=page_size,
        )

    async def update_dashboard(
        self, dashboard_id: UUID, data: DashboardUpdate
    ) -> DashboardResponse | None:
        """Update a dashboard."""
        existing = await self.get_dashboard(dashboard_id)
        if not existing:
            return None

        update_fields = []
        params = {"id": str(dashboard_id), "updated_at": datetime.utcnow()}

        if data.name is not None:
            update_fields.append("name = %(name)s")
            params["name"] = data.name
        if data.description is not None:
            update_fields.append("description = %(description)s")
            params["description"] = data.description
        if data.is_public is not None:
            update_fields.append("is_public = %(is_public)s")
            params["is_public"] = data.is_public
        if data.tags is not None:
            update_fields.append("tags = %(tags)s")
            params["tags"] = data.tags

        update_fields.append("updated_at = %(updated_at)s")

        if update_fields:
            await self.db.execute(
                f"""
                ALTER TABLE dashboards UPDATE {', '.join(update_fields)}
                WHERE id = %(id)s
                """,
                params,
            )

        return await self.get_dashboard(dashboard_id)

    async def delete_dashboard(self, dashboard_id: UUID) -> bool:
        """Delete a dashboard and its widgets."""
        existing = await self.get_dashboard(dashboard_id)
        if not existing:
            return False

        # Delete widgets first
        await self.db.execute(
            "ALTER TABLE widgets DELETE WHERE dashboard_id = %(dashboard_id)s",
            {"dashboard_id": str(dashboard_id)},
        )

        # Delete dashboard
        await self.db.execute(
            "ALTER TABLE dashboards DELETE WHERE id = %(id)s",
            {"id": str(dashboard_id)},
        )

        return True

    # Widget operations
    async def create_widget(self, data: WidgetCreate) -> WidgetResponse:
        """Create a new widget."""
        widget_id = uuid4()
        now = datetime.utcnow()

        await self.db.execute(
            """
            INSERT INTO widgets (id, dashboard_id, title, widget_type, position, config, created_at, updated_at)
            VALUES (%(id)s, %(dashboard_id)s, %(title)s, %(widget_type)s, %(position)s, %(config)s, %(created_at)s, %(updated_at)s)
            """,
            {
                "id": str(widget_id),
                "dashboard_id": str(data.dashboard_id),
                "title": data.title,
                "widget_type": data.widget_type.value,
                "position": json.dumps(data.position.model_dump()),
                "config": json.dumps(data.config.model_dump()),
                "created_at": now,
                "updated_at": now,
            },
        )

        return WidgetResponse(
            id=widget_id,
            dashboard_id=data.dashboard_id,
            title=data.title,
            widget_type=data.widget_type,
            position=data.position,
            config=data.config,
            created_at=now,
            updated_at=now,
        )

    async def list_widgets(self, dashboard_id: UUID) -> list[WidgetResponse]:
        """List all widgets in a dashboard."""
        rows = await self.db.fetch_all(
            "SELECT * FROM widgets WHERE dashboard_id = %(dashboard_id)s ORDER BY created_at",
            {"dashboard_id": str(dashboard_id)},
        )

        widgets = []
        for row in rows:
            position = json.loads(row["position"]) if isinstance(row["position"], str) else row["position"]
            config = json.loads(row["config"]) if isinstance(row["config"], str) else row["config"]
            
            widgets.append(
                WidgetResponse(
                    id=UUID(row["id"]),
                    dashboard_id=UUID(row["dashboard_id"]),
                    title=row["title"],
                    widget_type=row["widget_type"],
                    position=position,
                    config=config,
                    created_at=row["created_at"],
                    updated_at=row.get("updated_at"),
                )
            )

        return widgets

    async def update_widget(
        self, widget_id: UUID, data: WidgetUpdate
    ) -> WidgetResponse | None:
        """Update a widget."""
        row = await self.db.fetch_one(
            "SELECT * FROM widgets WHERE id = %(id)s",
            {"id": str(widget_id)},
        )

        if not row:
            return None

        update_fields = []
        params = {"id": str(widget_id), "updated_at": datetime.utcnow()}

        if data.title is not None:
            update_fields.append("title = %(title)s")
            params["title"] = data.title
        if data.widget_type is not None:
            update_fields.append("widget_type = %(widget_type)s")
            params["widget_type"] = data.widget_type.value
        if data.position is not None:
            update_fields.append("position = %(position)s")
            params["position"] = json.dumps(data.position.model_dump())
        if data.config is not None:
            update_fields.append("config = %(config)s")
            params["config"] = json.dumps(data.config.model_dump())

        update_fields.append("updated_at = %(updated_at)s")

        if update_fields:
            await self.db.execute(
                f"""
                ALTER TABLE widgets UPDATE {', '.join(update_fields)}
                WHERE id = %(id)s
                """,
                params,
            )

        # Return updated widget
        updated_row = await self.db.fetch_one(
            "SELECT * FROM widgets WHERE id = %(id)s",
            {"id": str(widget_id)},
        )

        if not updated_row:
            return None

        position = json.loads(updated_row["position"]) if isinstance(updated_row["position"], str) else updated_row["position"]
        config = json.loads(updated_row["config"]) if isinstance(updated_row["config"], str) else updated_row["config"]

        return WidgetResponse(
            id=UUID(updated_row["id"]),
            dashboard_id=UUID(updated_row["dashboard_id"]),
            title=updated_row["title"],
            widget_type=updated_row["widget_type"],
            position=position,
            config=config,
            created_at=updated_row["created_at"],
            updated_at=updated_row.get("updated_at"),
        )

    async def delete_widget(self, widget_id: UUID) -> bool:
        """Delete a widget."""
        row = await self.db.fetch_one(
            "SELECT id FROM widgets WHERE id = %(id)s",
            {"id": str(widget_id)},
        )

        if not row:
            return False

        await self.db.execute(
            "ALTER TABLE widgets DELETE WHERE id = %(id)s",
            {"id": str(widget_id)},
        )

        return True
